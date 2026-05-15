package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"runtime"
	"net"

	"proxychecker/internal/api"
	"proxychecker/internal/checker"
	"proxychecker/internal/config"
	"proxychecker/internal/scraper"
	"proxychecker/internal/storage"
	"proxychecker/internal/rotator"
)

func main() {
	// 0. 加载配置 (默认读取 "config.yaml")
	config.Load("config.yaml")

	// 禁用 Windows 快速编辑模式防止假死
	disableQuickEdit()
	fmt.Println("[系统] Antigravity 代理检测系统 (Go重构版) 正在启动...")
	// [Fix] Customize DNS if specified in config (Linux Only, 带系统 DNS 兜底)
	if len(config.DNSServerList) > 0 && runtime.GOOS == "linux" {
		fmt.Printf("[网络] (Linux) 使用自定义 DNS 服务器列表: %v\n", config.DNSServerList)
		savedResolver := net.DefaultResolver
		net.DefaultResolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: 3 * time.Second}
				for _, dnsServer := range config.DNSServerList {
					conn, err := d.DialContext(ctx, "udp", dnsServer)
					if err == nil {
						return conn, nil
					}
				}
				// 自定义 DNS 全部失败，降级到系统 DNS
				fmt.Printf("[网络] 自定义 DNS 不可用，降级到系统 DNS\n")
				if savedResolver != nil && savedResolver.Dial != nil {
					return savedResolver.Dial(ctx, network, address)
				}
				// 直接使用系统默认 TCP 连接（不经过自定义 DNS）
				return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
			},
		}
	}

	// 交互式启动：允许用户覆盖默认开关
	// 默认值来自 config.yaml，此处允许按需修改。
	fmt.Println("\n================ 功能开关配置 ================")
	config.EnableChecker = promptYesNo("是否开启 [检测器] (验证IP有效性)?", config.EnableChecker)
	config.EnableScraper = promptYesNo("是否开启 [采集器] (自动抓取IP)?", config.EnableScraper)
	config.EnableAPI = promptYesNo("是否开启 [API服务] (提供接口)?", config.EnableAPI)
	config.EnableRotator = promptYesNo("是否开启 [代理轮换] (提供SOCKS5/HTTP服务)?", config.EnableRotator)
	fmt.Println("==============================================")

	// 1. 初始化存储 (依赖配置)
	store := storage.New()
	pong, err := store.Client.Ping(context.Background()).Result()
	if err != nil {
		fmt.Printf("[错误] Redis 连接失败: %v\n", err)
		fmt.Println("按回车键退出...")
		fmt.Scanln()
		os.Exit(1)
	}
	fmt.Printf("[成功] Redis 连接成功: %s\n", pong)

	// 2. 初始化各组件
	scr := scraper.New(store)
	chk := checker.New(store)

	// API 模块较重且依赖 "web" 目录，仅在启用时初始化
	var srv *api.Server
	if config.EnableAPI {
		srv = api.New(store)
	}

	// 3. 启动后台任务

	// 采集器循环 (协程)
	if config.EnableScraper {
		go scr.RunLoop()
	} else {
		fmt.Println("[系统] 采集器模块已禁用")
	}

	// 检测器工作池 (协程池)
	// chk.RunWorker 是阻塞的，所以需在协程中运行
	if config.EnableChecker {
		go chk.RunWorker(config.Concurrency)
	} else {
		fmt.Println("[系统] 检测器模块已禁用")
	}

	// 队列监控/回填 (在采集器或检测器启用时运行)
	if config.EnableScraper || config.EnableChecker {
		go runMonitor(store)
	}

	// 状态日志记录器
	go runStatusLogger(store)

	// 过期清理任务 (仅在采集器启用时运行，避免多设备冲突)
	if config.ProxyLeaseTime > 0 && config.EnableScraper {
		go runCleaner(store)
	}

	// 4. 启动 API 服务 (主协程阻塞或独立协程)
	// 在协程中运行 API，以便在主线程处理优雅停机
	if config.EnableAPI && srv != nil {
		go func() {
			if err := srv.Run(); err != nil {
				log.Fatalf("[错误] API 服务启动错误: %v", err)
			}
		}()
	} else {
		fmt.Println("[系统] API 服务已禁用")
	}

	// 5. 启动 Rotator 服务
	if config.EnableRotator && config.RotatorPort != "" {
		rt := rotator.New(store)
		go func() {
			if err := rt.Run(); err != nil {
				fmt.Printf("[错误] Rotator 启动失败: %v\n", err)
			}
		}()
	} else {
		fmt.Println("[系统] 代理轮换服务已禁用")
	}

	// 5. 等待退出信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n[退出] 正在停止系统...")
}

func runMonitor(s *storage.Storage) {
	interval := config.IntervalThreshold
	if interval <= 0 {
		interval = 10 * time.Second
	}
	fmt.Printf("[监控] 队列监控任务已就绪 (间隔 %v)\n", interval)

	// 启动时立即检查一次
	checkAndRefillQueue(s)

	for {
		time.Sleep(interval)
		checkAndRefillQueue(s)
	}
}

// checkAndRefillQueue 检查队列并回填
func checkAndRefillQueue(s *storage.Storage) {
	pending := s.PendingCount()
	target := int64(config.QueueTargetSize)

	// 调试: 显示当前队列状态
	if config.DebugMode {
		fmt.Printf("[监控] 当前队列: %d, 目标: %d, 阈值: %d\n", pending, target, target/2)
	}

	if pending < target/2 {
		diff := int(target - pending)
		if diff > 0 {
			lockID := s.AcquireLock("recheck_monitor")
			if lockID != "" {
				fmt.Printf("[监控] 队列不足 (%d/%d), 正在回填 %d...\n", pending, target, diff)
				// 从 proxy:all_seen 顺序获取代理回填队列 (使用游标循环遍历)
				proxies, startPos, endPos := s.GetProxiesSequentially(diff)
				if len(proxies) > 0 {
					// [Fix] 过滤已在活跃池中的代理，避免重复检测和误移除
					proxies = s.FilterOutActive(proxies)
					if len(proxies) > 0 {
						s.PushToTail(proxies)
						fmt.Printf("[监控] 已回填 %d 个 (数据库第 %d 行 > 第 %d 行)\n", len(proxies), startPos+1, endPos)
					} else {
						fmt.Println("[监控] 所有候选代理已在活跃池中，跳过回填")
					}
				} else {
					fmt.Println("[监控] 警告: proxy:all_seen 为空，无法回填队列")
				}
				s.ReleaseLock("recheck_monitor", lockID)
			} else {
				fmt.Println("[监控] 获取锁失败，跳过本轮回填")
			}
		}
	}
}

func runStatusLogger(s *storage.Storage) {
	for {
		time.Sleep(config.LogIntervalWorker)
		stats := s.GetAllStats()
		pending := s.PendingCount()

		fmt.Printf("\n-------[系统状态] %s-------\n", time.Now().Format("15:04:05"))
		fmt.Printf(" > 待测队列: %d\n", pending)
		fmt.Printf(" > H= %d | HS= %d | S4= %d | S5= %d\n",
			stats["http"], stats["https"], stats["socks4"], stats["socks5"])
		fmt.Printf(" > 数据库总量: %d / 存活总量: %d\n", stats["db_total"], stats["total_active"])
		fmt.Println("---------------------------------")
	}
}

func runCleaner(s *storage.Storage) {
	fmt.Println("[系统] 代理过期清理任务已启动")
	ticker := time.NewTicker(60 * time.Second) // 检查频率: 1分钟
	defer ticker.Stop()

	for range ticker.C {
		// PruneExpired 内部接受 duration 参数
		// 实际调用清理逻辑
		s.PruneExpired(config.ProxyLeaseTime)
	}
}

func promptYesNo(question string, defaultValue bool) bool {
	promptStr := "[y/N]"
	if defaultValue {
		promptStr = "[Y/n]"
	}

	defaultStr := "N"
	if defaultValue {
		defaultStr = "Y"
	}

	fmt.Printf("%s %s (默认: %s): ", question, promptStr, defaultStr)

	var input string
	fmt.Scanln(&input)

	input = strings.TrimSpace(strings.ToUpper(input))
	if input == "" {
		return defaultValue
	}

	if input == "Y" || input == "YES" || input == "1" {
		return true
	}
	return false
}
