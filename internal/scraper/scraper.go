package scraper

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"proxychecker/internal/config"
	"proxychecker/internal/storage"
)

type Scraper struct {
	Storage *storage.Storage
}

func New(s *storage.Storage) *Scraper {
	return &Scraper{Storage: s}
}

// RunLoop 启动采集循环
func (s *Scraper) RunLoop() {
	fmt.Println("[采集] 采集器模块已就绪")
	for {
		// 分布式锁
        lockID := s.Storage.AcquireLock("scraper")
		if lockID != "" {
			s.RunOnce()
			s.Storage.ReleaseLock("scraper", lockID)
		} else {
			// fmt.Println("[锁] Scraper lock held by another node")
		}
		time.Sleep(config.IntervalScraper)
	}
}

func (s *Scraper) RunOnce() {
	fmt.Println("[采集] 开始执行采集任务...")
	start := time.Now()
	totalFound := 0
	totalNew := 0

	var wg sync.WaitGroup
	var mu sync.Mutex

	// 1. URL 源
	urls := config.ProxySourcesURL
	// 加载额外文件源 (如果存在)
	if _, err := os.Stat(config.ProxyURLFile); err == nil {
		content, _ := os.ReadFile(config.ProxyURLFile)
		lines := strings.Split(string(content), "\n")
		for _, l := range lines {
			if strings.TrimSpace(l) != "" && !strings.HasPrefix(l, "#") {
				urls = append(urls, strings.TrimSpace(l))
			}
		}
	}

	for _, u := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			found, newC := s.scrapeURL(url)
			mu.Lock()
			totalFound += found
			totalNew += newC
			mu.Unlock()
		}(u)
	}

	// 2. 本地文件源 (txt)
	for _, path := range config.ProxySourcesTxt {
		found, newC := s.scrapeFile(path)
		totalFound += found
		totalNew += newC
	}

	wg.Wait()
	fmt.Printf("[采集] 采集完成: 发现 %d | 新入库 %d | 耗时 %v\n", totalFound, totalNew, time.Since(start))
}

func (s *Scraper) scrapeURL(url string) (int, int) {
	transport := &http.Transport{
		MaxIdleConns:        100,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   config.ScraperTimeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		// fmt.Printf("[错误] 采集失败 %s: %v\n", url, err)
		return 0, 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, 0
	}

	body, _ := io.ReadAll(resp.Body)
	proxies := s.extractProxies(string(body))
	if len(proxies) > 0 {
		_, newCount := s.Storage.AddProxies(proxies)
		fmt.Printf("  [源] %s -> 提取 %d | 新增 %d\n", url, len(proxies), newCount)
		return len(proxies), newCount
	}
	return 0, 0
}

func (s *Scraper) scrapeFile(path string) (int, int) {
	content, err := os.ReadFile(path)
	if err != nil {
		return 0, 0
	}
	proxies := s.extractProxies(string(content))
	if len(proxies) > 0 {
		_, newCount := s.Storage.AddProxies(proxies)
		fmt.Printf("  [文件] %s -> 提取 %d | 新增 %d\n", path, len(proxies), newCount)
		return len(proxies), newCount
	}
	return 0, 0
}

func (s *Scraper) extractProxies(text string) []string {
	// 正则匹配候选 IP:Port
	re := regexp.MustCompile(`(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[:\s\t]+(\d+)`)
	matches := re.FindAllStringSubmatch(text, -1)

	result := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) == 3 {
			// 用 net.ParseIP 二次校验，排除 999.999.999.999 等非法格式
			if net.ParseIP(m[1]) != nil {
				result = append(result, fmt.Sprintf("%s:%s", m[1], m[2]))
			}
		}
	}
	return result
}
