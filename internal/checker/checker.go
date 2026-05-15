package checker

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"proxychecker/internal/config"
	"proxychecker/internal/storage"
)

type Checker struct {
	Storage *storage.Storage
}

func New(s *storage.Storage) *Checker {
	return &Checker{Storage: s}
}

var checkCounter uint64

// RunWorker 启动工作协程池
func (c *Checker) RunWorker(concurrency int) {
	fmt.Printf("[Work] 检测服务已启动，并发线程: %d\n", concurrency)
	
	// Create a buffered channel of semaphores
	sem := make(chan struct{}, concurrency)

	for {
		// 1. 获取一批待测代理
		proxies := c.Storage.PopPending(concurrency) 
		if len(proxies) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, proxy := range proxies {
			sem <- struct{}{} // 获取信号量
			go func(p string) {
				defer func() { <-sem }() // 释放信号量
                // 防止单个协程 Panic 导致程序崩溃
                defer func() {
                    if r := recover(); r != nil {
                        fmt.Printf("[错误] Worker Panic: %v\n", r)
                    }
                }()
				c.checkOne(p)
                
                // 原子增加计数并打印进度
                newVal := atomic.AddUint64(&checkCounter, 1)
                if config.LogIntervalChecker > 0 && newVal % uint64(config.LogIntervalChecker) == 0 {
                     fmt.Printf("[进度] 已检测 %d 个代理...\n", newVal)
                }
			}(proxy)
		}
	}
}

// checkOne 验证单个代理
// 流程：check_url2(可用性) → check_url1(归属国，仅当 enable_checker_type1=true)
func (c *Checker) checkOne(proxy string) {
	if !c.Storage.CheckQualityGate(proxy) {
		return
	}

	protocols := []string{"socks5", "socks4"}
	if config.EnableCheckHTTP {
		protocols = append([]string{"http"}, protocols...)
	}

	// Step 1 可用性验证 URL：优先 check_url2，无则用 check_url1，再则 Legacy
	verifyURL := config.CheckURL2
	verifyKeyword := config.CheckKeyword2
	if verifyURL == "" {
		verifyURL = config.CheckURL1
	}
	if verifyURL == "" {
		verifyURL = config.CheckURL
		verifyKeyword = config.CheckKeyword
	}
	if verifyURL == "" {
		return
	}

	// Step 2 归属国提取：check_url1（Type1），仅当 enable_checker_type1=true 且 URL 不同于验证 URL
	// 提取失败则整个代理判负（check_url1 也是必须通过的环节）
	hasCountryStep := config.EnableCheckerType1 && config.CheckURL1 != "" && config.CheckURL1 != verifyURL

	success := false

	for _, proto := range protocols {
		// Step 1: 可用性检测
		if !c.testProtocol(proxy, proto, verifyURL, verifyKeyword, false) {
			c.Storage.RemoveFromActive(proxy, proto)
			if proto == "http" {
				c.Storage.RemoveFromActive(proxy, "https")
			}
			continue
		}

		// Step 2: 归属国提取（必须通过）
		if hasCountryStep && !c.testProtocol(proxy, proto, config.CheckURL1, "", true) {
			c.Storage.RemoveFromActive(proxy, proto)
			if proto == "http" {
				c.Storage.RemoveFromActive(proxy, "https")
			}
			continue
		}

		storageProto := proto
		if proto == "http" && strings.HasPrefix(verifyURL, "https://") {
			storageProto = "https"
		}
		c.Storage.AddToActive(proxy, storageProto)
		c.Storage.RecordSuccess(proxy)
		success = true
	}

	if !success {
		c.Storage.RecordFailure(proxy)
	}
}

// testProtocol 尝试使用指定代理和协议发起请求
func (c *Checker) testProtocol(proxyStr, protocol, checkURL, checkKeyword string, isType1 bool) bool {
	// Format: ip:port
	proxyURLStr := fmt.Sprintf("%s://%s", protocol, proxyStr)
	proxyURL, err := url.Parse(proxyURLStr)
	if err != nil {
		return false
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: (&net.Dialer{
			Timeout:   config.CheckTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
        TLSClientConfig: &tls.Config{InsecureSkipVerify: config.InsecureSkipVerify},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   config.CheckTimeout,
	}

	req, err := http.NewRequest("GET", checkURL, nil)
    if err != nil {
        if config.DebugMode {
            fmt.Printf("[调试] 创建请求失败: %v\n", err)
        }
        return false
    }
	req.Header.Set("User-Agent", config.UserAgent)
    // 模拟真实浏览器行为
    req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
    req.Header.Set("Accept-Language", "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7")
    req.Header.Set("Cache-Control", "no-cache")
    req.Header.Set("Connection", "keep-alive")
    req.Header.Set("Upgrade-Insecure-Requests", "1")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
        if config.DebugMode {
            fmt.Printf("[调试] 检测失败 %s (%s): %v\n", proxyStr, protocol, err)
        }
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 204 {
        // Read Body
        bodyBytes, err := io.ReadAll(resp.Body)
        if err != nil {
            return false
        }
        bodyStr := string(bodyBytes)
        
		// Keyword Check
		if checkKeyword != "" {
			if !strings.Contains(bodyStr, checkKeyword) {
                if config.DebugMode {
				    fmt.Printf("[调试] 关键字未找到 %s (%s) 期望: %s\n", proxyStr, protocol, checkKeyword)
                }
				return false
			}
		}
        
        // Type 1 Special: Country Extraction + 匿名检测
        country := ""
        if isType1 {
            // 提取 IP（ip.me 页面显示的出口 IP）
            ipRe := regexp.MustCompile(`<td><code>(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})</code></td>`)
            seenIP := ""
            if ipMatch := ipRe.FindStringSubmatch(bodyStr); len(ipMatch) >= 2 {
                seenIP = ipMatch[1]
            }
            // 提取国家代码
            re := regexp.MustCompile(`<td><code>([A-Z]{2})</code></td>`)
            matches := re.FindStringSubmatch(bodyStr)
            if len(matches) >= 2 {
                country = matches[1]
            }

            if country == "" {
                if config.DebugMode {
                    fmt.Printf("[Type1] 无法从 HTML 提取国家代码，判定失败: %s\n", proxyStr)
                }
                return false
            }

            c.Storage.UpdateProxyCountry(proxyStr, country)
            if config.DebugMode {
                fmt.Printf("[Type1] Extracted Country %s -> %s\n", proxyStr, country)
            }

            // 匿名检测：代理 IP ≠ 出口 IP → 匿名
            proxyIP := proxyStr
            if idx := strings.Index(proxyStr, ":"); idx != -1 {
                proxyIP = proxyStr[:idx]
            }
            if seenIP != "" && seenIP != proxyIP {
                c.Storage.MarkAnonymous(proxyStr)
                if config.DebugMode {
                    fmt.Printf("[匿名] %s 出口 IP=%s (代理 IP=%s)\n", proxyStr, seenIP, proxyIP)
                }
            }
        }


        
        duration := time.Since(start)
        // [Log] Success (Always print as requested)
        // 修正协议显示：如果是 HTTP 协议但检测目标是 HTTPS，则显示 https
        displayProto := protocol
        if protocol == "http" && strings.HasPrefix(checkURL, "https://") {
            displayProto = "https"
        }
        
        countryInfo := ""
        if country != "" {
            countryInfo = fmt.Sprintf(" | %s", country)
        }
        fmt.Printf("[检测成功] %s (%s) | 耗时: %v%s\n", proxyStr, displayProto, duration, countryInfo)

		return true
	}
    if config.DebugMode {
        fmt.Printf("[调试] 状态码无效 %s (%s): %d\n", proxyStr, protocol, resp.StatusCode)
    }
	return false
}
