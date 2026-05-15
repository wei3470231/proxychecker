package storage

import (
	"context"
	"fmt"
	"math/rand"
    "net"
    "runtime"
	"strconv"
	"strings"
    "time"

	"github.com/redis/go-redis/v9"
	"proxychecker/internal/config"
	"proxychecker/internal/model"
)


var Ctx = context.Background()

type Storage struct {
	Client *redis.Client
}

// RebuildCountryIndexes 重建国家索引，修复旧版遗留的脏数据
// 旧版 UpdateProxyCountry 不清理旧索引，导致同IP出现在多个 proxy:country:* 集合中
// 使用哨兵 key proxy:index:rebuilt 标记，仅在首次或版本变更时执行
func (s *Storage) RebuildCountryIndexes() {
	// 检查是否已重建过 (哨兵 key)
	done, _ := s.Client.Exists(Ctx, "proxy:index:rebuilt").Result()
	if done > 0 {
		return
	}

	fmt.Println("[维护] 正在重建国家索引 (首次启动或升级后执行)...")

	// 1. 获取所有国家索引键并删除
	iter := s.Client.Scan(Ctx, 0, "proxy:country:*", 1000).Iterator()
	delKeys := make([]string, 0)
	for iter.Next(Ctx) {
		delKeys = append(delKeys, iter.Val())
	}
	if len(delKeys) > 0 {
		s.Client.Del(Ctx, delKeys...).Result()
	}

	// 2. 从 proxy:stats:country 哈希重建干净的国家索引
	cursor := uint64(0)
	rebuilt := 0
	skipped := 0
	for {
		keys, nextCursor, err := s.Client.HScan(Ctx, "proxy:stats:country", cursor, "", 500).Result()
		if err != nil {
			fmt.Printf("[维护] 重建索引失败: %v\n", err)
			break
		}
		for i := 0; i < len(keys); i += 2 {
			proxy := keys[i]
			country := strings.ToUpper(keys[i+1])
			if country != "" && len(country) <= 10 {
				// 排除明显非国家代码的值（如国家名"CHINA"）
				isAlpha := true
				for _, ch := range country {
					if ch < 'A' || ch > 'Z' {
						isAlpha = false
						break
					}
				}
				if isAlpha {
					s.Client.SAdd(Ctx, "proxy:country:"+country, proxy)
					rebuilt++
				} else {
					skipped++
				}
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// 3. 标记重建完成
	s.Client.Set(Ctx, "proxy:index:rebuilt", "1", 0)

	fmt.Printf("[维护] 国家索引重建完成: %d 条已索引, %d 条跳过(非代码)\n", rebuilt, skipped)
}

func New() *Storage {
    fmt.Printf("[DEBUG] Storage Init RedisURL: '%s'\n", config.RedisURL)
	opt, err := redis.ParseURL(config.RedisURL)
	if err != nil {
		fmt.Printf("❌ Redis URL Error: %v\n", err)
		opt = &redis.Options{Addr: "localhost:6379"}
	}
    
    // [核心] 应用配置中的 Redis 超时时间
    if config.RedisConnectTimeout > 0 {
        opt.DialTimeout = config.RedisConnectTimeout
    }
    
    // [Linux DNS Fix] 使用自定义 DNS 解析器 (支持故障转移 + 系统 DNS 兜底)
    if len(config.DNSServerList) > 0 && runtime.GOOS == "linux" {
        fmt.Printf("[Redis] 使用自定义 DNS 列表: %v\n", config.DNSServerList)
        opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
            host, port, _ := net.SplitHostPort(addr)
            var ips []string

            // 尝试自定义 DNS
            customResolver := &net.Resolver{
                PreferGo: true,
                Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
                    d := net.Dialer{Timeout: 3 * time.Second}
                    var lastErr error
                    for _, dnsServer := range config.DNSServerList {
                        conn, err := d.DialContext(ctx, "udp", dnsServer)
                        if err == nil {
                            return conn, nil
                        }
                        lastErr = err
                    }
                    return nil, lastErr
                },
            }
            ips, err := customResolver.LookupHost(ctx, host)
            if err != nil {
                fmt.Printf("[Redis] 自定义 DNS 失败, 尝试系统 DNS...\n")
                ips, err = (&net.Resolver{}).LookupHost(ctx, host)
            }

            if err != nil || len(ips) == 0 {
                // 终极兜底：直接按原始地址连接，不经过自定义 DNS
                fmt.Printf("[Redis] DNS 全部失败, 尝试直连 %s\n", host)
                d := net.Dialer{
                    Timeout:  opt.DialTimeout,
                    Resolver: &net.Resolver{}, // 不走 main.go 改过的 DefaultResolver
                }
                return d.DialContext(ctx, network, addr)
            }

            d := net.Dialer{Timeout: opt.DialTimeout}
            return d.DialContext(ctx, network, net.JoinHostPort(ips[0], port))
        }
    }
    
	client := redis.NewClient(opt)
	store := &Storage{Client: client}
	// 启动时重建国家索引，修复旧版 UpdateProxyCountry 遗留的脏数据
	store.RebuildCountryIndexes()
	return store
}



// AddProxies 将新代理添加到待处理队列
func (s *Storage) AddProxies(proxies []string) (int, int) {
	if len(proxies) == 0 {
		return 0, 0
	}

	total := len(proxies)
	newCount := 0
	pipe := s.Client.Pipeline()
	// SADD 返回 1 表示新增，0 表示已存在
	cmds := make([]*redis.IntCmd, total)
	for i, proxy := range proxies {
		cmds[i] = pipe.SAdd(Ctx, "proxy:all_seen", proxy)
	}
	_, err := pipe.Exec(Ctx)
	if err != nil {
		fmt.Printf("❌ Redis pipeline error (SADD): %v\n", err)
		return total, 0 
	}

	newProxies := make([]interface{}, 0, total)
	for i, cmd := range cmds {
		if cmd.Val() == 1 {
			newCount++
			newProxies = append(newProxies, proxies[i])
		}
	}

	if len(newProxies) > 0 {
		for i := 0; i < len(newProxies); i += config.BatchSizeStorageAdd {
			end := i + config.BatchSizeStorageAdd
			if end > len(newProxies) {
				end = len(newProxies)
			}
			s.Client.RPush(Ctx, "proxy:pending", newProxies[i:end]...)
		}
	}

	return total, newCount
}

// AcquireLock 尝试获取分布式锁（带重试）。成功返回 lockID，否则返回空字符串。
func (s *Storage) AcquireLock(lockName string) string {
	key := "lock:" + lockName
	val := fmt.Sprintf("%d", time.Now().UnixNano())
	interval := config.LockRetryInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	// 最多重试 5 次
	for i := 0; i < 5; i++ {
		success, err := s.Client.SetNX(Ctx, key, val, config.DistributedLockTTL).Result()
		if err == nil && success {
			return val
		}
		time.Sleep(interval)
	}
	return ""
}

// ReleaseLock 安全释放锁 (利用 Lua 脚本校验 Value)
func (s *Storage) ReleaseLock(lockName, val string) {
    if val == "" {
        return
    }
	key := "lock:" + lockName
    // Lua script to safely check and delete
    script := `
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    `
	if result, err := s.Client.Eval(Ctx, script, []string{key}, val).Int(); err != nil || result == 0 {
        if config.DebugMode {
            fmt.Printf("[Lock] 释放锁 %s 失败 (可能已过期或被其他节点获取)\n", lockName)
        }
    }
}

// GetProxiesSequentially 从 proxy:all_seen 顺序获取代理用于回填
// 使用 Redis 存储的游标位置实现循环遍历整个数据库
// 返回: 代理列表, 起始位置, 结束位置
func (s *Storage) GetProxiesSequentially(limit int) ([]string, int64, int64) {
    const cursorKey = "proxy:refill:cursor"
    const offsetKey = "proxy:refill:offset"
    const dataKey = "proxy:all_seen"
    
    // 获取数据库总量
    totalCount, _ := s.Client.SCard(Ctx, dataKey).Result()
    
    // 获取当前游标位置和偏移量
    cursor, _ := s.Client.Get(Ctx, cursorKey).Uint64()
    startOffset, _ := s.Client.Get(Ctx, offsetKey).Int64()
    
    var proxies []string
    remaining := limit
    loopCount := 0 // 防止无限循环
    
    for remaining > 0 && loopCount < 2 {
        // 使用 SSCAN 扫描数据库
        keys, nextCursor, err := s.Client.SScan(Ctx, dataKey, cursor, "", int64(remaining)).Result()
        if err != nil {
            break
        }
        
        proxies = append(proxies, keys...)
        remaining -= len(keys)
        cursor = nextCursor
        
        // 如果游标归零，说明已遍历完一轮
        if cursor == 0 {
            loopCount++
            if remaining > 0 && loopCount < 2 {
                // 还需要更多代理，继续从头开始
                fmt.Printf("[监控] 数据库遍历完成第 %d 轮，从头开始新一轮\n", loopCount)
                startOffset = 0 // 重置偏移量
            }
        }
    }
    
    // 计算结束位置
    endOffset := startOffset + int64(len(proxies))
    if endOffset > totalCount {
        endOffset = totalCount
    }
    
    // 保存新的游标位置和偏移量
    s.Client.Set(Ctx, cursorKey, cursor, 0)
    if cursor == 0 {
        s.Client.Set(Ctx, offsetKey, 0, 0) // 遍历完成，重置偏移量
    } else {
        s.Client.Set(Ctx, offsetKey, endOffset, 0)
    }
    
    return proxies, startOffset, endOffset
}

// PushToTail
func (s *Storage) PushToTail(proxies []string) {
    if len(proxies) == 0 {
        return
    }
    interfaceProxies := make([]interface{}, len(proxies))
    for i, v := range proxies {
        interfaceProxies[i] = v
    }
    s.Client.RPush(Ctx, "proxy:pending", interfaceProxies...)
}

// FilterOutActive 过滤掉已在任意活跃池中的代理，避免重复检测
func (s *Storage) FilterOutActive(proxies []string) []string {
    if len(proxies) == 0 {
        return proxies
    }
    // 获取所有活跃代理集合 (去重)
    activeSet := make(map[string]struct{})
    for _, proto := range []string{"http", "https", "socks4", "socks5"} {
        ips, _ := s.Client.SMembers(Ctx, "proxy:active:"+proto).Result()
        for _, ip := range ips {
            activeSet[ip] = struct{}{}
        }
    }
    if len(activeSet) == 0 {
        return proxies
    }
    result := make([]string, 0, len(proxies))
    for _, proxy := range proxies {
        if _, exists := activeSet[proxy]; !exists {
            result = append(result, proxy)
        }
    }
    return result
}

// PendingCount 获取队列积压数量
func (s *Storage) PendingCount() int64 {
	val, _ := s.Client.LLen(Ctx, "proxy:pending").Result()
	return val
}

// GetAllStats 获取所有统计数据
func (s *Storage) GetAllStats() map[string]int {
	stats := make(map[string]int)
	
	protocols := []string{"http", "https", "socks4", "socks5"}
	totalActive := 0
	
	for _, proto := range protocols {
		count, _ := s.Client.SCard(Ctx, "proxy:active:"+proto).Result()
		stats[proto] = int(count)
		totalActive += int(count)
	}

	allTotal, _ := s.Client.SCard(Ctx, "proxy:all_seen").Result()
	
	stats["total_active"] = totalActive
	stats["db_total"] = int(allTotal)
	
	return stats
}

// --- 检测器辅助逻辑 ---

// AddToActive 将验证通过的代理加入活跃集合
func (s *Storage) AddToActive(proxy string, protocol string) {
    key := "proxy:active:" + protocol
    
    // 使用 Pipeline 同时更新 Set 和 ZSet (用于过期清理)
    pipe := s.Client.Pipeline()
    pipe.SAdd(Ctx, key, proxy)
    // Score = 当前时间戳
    pipe.ZAdd(Ctx, key+":score", redis.Z{Score: float64(time.Now().Unix()), Member: proxy})
    pipe.SAdd(Ctx, "proxy:history:"+protocol, proxy)
    
    _, err := pipe.Exec(Ctx)
    if err != nil {
         fmt.Printf("[Redis错误] ADD Active %s: %v\n", key, err)
         return
    }
    
    // Verification
    // count, _ := s.Client.SCard(Ctx, key).Result()
    // fmt.Printf("[Redis] +入库成功 %s (%s) | 当前池数量: %d\n", proxy, protocol, count)
}

// RemoveFromActive 从活跃集合移除代理
func (s *Storage) RemoveFromActive(proxy, protocol string) {
    key := "proxy:active:" + protocol
    // 从 Set 和 ZSet 移除
    pipe := s.Client.Pipeline()
    pipe.SRem(Ctx, key, proxy)
    pipe.ZRem(Ctx, key+":score", proxy)
    pipe.Exec(Ctx)
    
    // 检查代理是否还在其他 active 池中
    isStillActive := false
    for _, proto := range []string{"http", "https", "socks4", "socks5"} {
        if proto == protocol { continue }
        if isMember, _ := s.Client.SIsMember(Ctx, "proxy:active:"+proto, proxy).Result(); isMember {
            isStillActive = true
            break
        }
    }
    
    // 如果没有任何协议存活，才清理国家全局索引
    if !isStillActive {
        country, _ := s.Client.HGet(Ctx, "proxy:stats:country", proxy).Result()
        if country != "" {
            s.Client.SRem(Ctx, "proxy:country:"+strings.ToUpper(country), proxy)
        }
    }
}

// RecordSuccess / Failure
func (s *Storage) RecordSuccess(proxy string) {
    s.Client.HIncrBy(Ctx, "proxy:stats:success", proxy, 1)
    s.Client.HIncrBy(Ctx, "proxy:stats:total", proxy, 1)
}

func (s *Storage) RecordFailure(proxy string) {
    s.Client.HIncrBy(Ctx, "proxy:stats:fail", proxy, 1)
    s.Client.HIncrBy(Ctx, "proxy:stats:total", proxy, 1)
}

// MarkAnonymous 标记代理为匿名（代理 IP ≠ 出口 IP）
func (s *Storage) MarkAnonymous(proxy string) {
	s.Client.SAdd(Ctx, "proxy:anonymous", proxy)
}

// IsAnonymous 检查代理是否匿名
func (s *Storage) IsAnonymous(proxy string) bool {
	ok, _ := s.Client.SIsMember(Ctx, "proxy:anonymous", proxy).Result()
	return ok
}

// UpdateProxyCountry 存储代理归属地信息
// [Fix] 代理归属国变更时，自动从旧的国家索引中移除，防止同IP出现在多个国家索引中
func (s *Storage) UpdateProxyCountry(proxy, country string) {
    if country == "" {
        return
    }
    country = strings.ToUpper(country)

    // 先读取旧国家，如果变更则清理旧索引
    oldCountry, _ := s.Client.HGet(Ctx, "proxy:stats:country", proxy).Result()
    oldCountry = strings.ToUpper(oldCountry)

    if oldCountry != "" && oldCountry != country {
        // 代理所属国家已变更，从旧索引移除
        s.Client.SRem(Ctx, "proxy:country:"+oldCountry, proxy)
        if config.DebugMode {
            fmt.Printf("[GEO] 国家变更 %s: %s -> %s\n", proxy, oldCountry, country)
        }
    }

    // 更新哈希 + 建立新索引
    s.Client.HSet(Ctx, "proxy:stats:country", proxy, country)
    s.Client.SAdd(Ctx, "proxy:country:"+country, proxy)
}

// GetRandomActiveProxyByCountry 随机获取指定国家的活跃代理
// 首先求取活跃池和国家池的交集，保证 100% 抽取到活跃代理
// 返回: proxy, protocol, error
func (s *Storage) GetRandomActiveProxyByCountry(country string) (string, string, error) {
    country = strings.ToUpper(country)

    protocols := []string{"http", "socks5", "https", "socks4"}
    // 随机打乱协议顺序
    for i := range protocols {
        j := rand.Intn(len(protocols))
        protocols[i], protocols[j] = protocols[j], protocols[i]
    }
    
    // 支持 "ALL" -> 即随机从活跃池选取
    if country == "ALL" {
        for _, proto := range protocols {
             key := "proxy:active:" + proto
             proxy, err := s.Client.SRandMember(Ctx, key).Result()
             if err == nil && proxy != "" {
                 return proxy, proto, nil
             }
        }
        return "", "", fmt.Errorf("没有找到活跃代理")
    }

    countryKey := "proxy:country:" + country
    
    // 使用 SINTER 精确定位同时存活且属于指定国家的代理
    type activeProxy struct {
        proxy string
        proto string
    }
    var availableProxies []activeProxy

    for _, proto := range protocols {
        activeKey := "proxy:active:" + proto
        // SInter 求交集：取出既在活跃池，又在国家索引池的 IP
        intersectIPs, err := s.Client.SInter(Ctx, activeKey, countryKey).Result()
        if err == nil && len(intersectIPs) > 0 {
            for _, ip := range intersectIPs {
                availableProxies = append(availableProxies, activeProxy{proxy: ip, proto: proto})
            }
        }
    }
    
    if len(availableProxies) > 0 {
        // 随机选择一个
        idx := rand.Intn(len(availableProxies))
        return availableProxies[idx].proxy, availableProxies[idx].proto, nil
    }

    return "", "", fmt.Errorf("指定国家 %s 无可用活跃代理", country)
}

// RemoveProxyFromAll 从所有集合和索引中彻底移除代理
func (s *Storage) RemoveProxyFromAll(proxy string) {
    // fmt.Printf("[Storage] 正在移除代理: %s\n", proxy) // Debug Log
    pipe := s.Client.Pipeline()
    protocols := []string{"http", "https", "socks4", "socks5"}
    
    for _, proto := range protocols {
        pipe.SRem(Ctx, "proxy:active:"+proto, proxy)
        pipe.ZRem(Ctx, "proxy:active:"+proto+":score", proxy)
    }
    
    // 同时尝试从国家索引移除
    // 由于不确定其国家，可能需要查表
    country, _ := s.Client.HGet(Ctx, "proxy:stats:country", proxy).Result()
    if country != "" {
        pipe.SRem(Ctx, "proxy:country:"+strings.ToUpper(country), proxy)
    }
    
    _, err := pipe.Exec(Ctx)
    if err != nil {
        fmt.Printf("[Storage] 移除代理失败 %s: %v\n", proxy, err)
    } else {
        if config.DebugMode {
            fmt.Printf("[Storage] 已销毁代理: %s\n", proxy)
        }
    }
}

// CheckQualityGate 质量门槛检查
func (s *Storage) CheckQualityGate(proxy string) bool {
    if !config.FilterQualityGateEnabled {
        return true
    }
    total, _ := s.Client.HGet(Ctx, "proxy:stats:total", proxy).Int()
    if total < config.FilterMinTotalChecks {
        return true
    }
    success, _ := s.Client.HGet(Ctx, "proxy:stats:success", proxy).Int()
    if success < config.FilterMinSuccessCount {
        return false // Block
    }
    return true
}

// PopPending 获取一批待测代理
func (s *Storage) PopPending(count int) []string {
    // Redis 5.0 (Windows) 不支持 LPOP key count，需要使用 Pipeline 模拟批量弹出
    pipe := s.Client.Pipeline()
    cmds := make([]*redis.StringCmd, count)
    for i := 0; i < count; i++ {
        cmds[i] = pipe.LPop(Ctx, "proxy:pending")
    }
    _, _ = pipe.Exec(Ctx) // 忽略 pipeline error，因为队列空时会有 redis.Nil 错误

    var res []string
    for _, cmd := range cmds {
        if val, err := cmd.Result(); err == nil {
            res = append(res, val)
        }
    }
    return res
}

// PruneExpired 清理超过租约时间的代理 (同时清理国家索引和统计哈希)
func (s *Storage) PruneExpired(leaseDuration time.Duration) {
    cutoff := time.Now().Add(-leaseDuration).Unix()
    protocols := []string{"http", "https", "socks4", "socks5"}

    for _, proto := range protocols {
        key := "proxy:active:" + proto
        scoreKey := key + ":score"

        // 1. 查找过期成员
        // ZRangeByScore 获取 score <= cutoff 的成员
        expired, err := s.Client.ZRangeByScore(Ctx, scoreKey, &redis.ZRangeBy{
            Min: "-inf",
            Max: fmt.Sprintf("%d", cutoff),
        }).Result()

        if err != nil || len(expired) == 0 {
            continue
        }

        // 2. Remove them + 清理国家索引和统计哈希
        pipe := s.Client.Pipeline()
        pipe.SRem(Ctx, key,  stringSliceToInterface(expired)...)
        pipe.ZRemRangeByScore(Ctx, scoreKey, "-inf", fmt.Sprintf("%d", cutoff))

        // [Fix] 同时清理国家索引和统计哈希，防止无限膨胀
        for _, proxy := range expired {
            country, _ := s.Client.HGet(Ctx, "proxy:stats:country", proxy).Result()
            if country != "" {
                pipe.SRem(Ctx, "proxy:country:"+strings.ToUpper(country), proxy)
            }
            pipe.HDel(Ctx, "proxy:stats:country", proxy)
            pipe.HDel(Ctx, "proxy:stats:success", proxy)
            pipe.HDel(Ctx, "proxy:stats:fail", proxy)
            pipe.HDel(Ctx, "proxy:stats:total", proxy)
        }

        _, errExec := pipe.Exec(Ctx)

        if errExec == nil {
            fmt.Printf("[清理] 已过期剔除 %d 个 %s 代理 (租约超过 %.0fs)\n", len(expired), proto, leaseDuration.Seconds())
        }
    }
}

func stringSliceToInterface(s []string) []interface{} {
    i := make([]interface{}, len(s))
    for idx, v := range s {
        i[idx] = v
    }
    return i
}

// GetProxies API获取代理接口 (含 Fallback 逻辑).
func (s *Storage) GetProxies(protocol, country string, count int, fallback bool, exclude bool, anonymous bool) []model.Proxy {
    if protocol == "" {
        protocol = "http"
    }

    // 1. 优先获取 Active
    key := "proxy:active:" + protocol
    var activeIPs []string

    // 国家过滤相关
    var countryKey string
    hasCountryFilter := false

    // [新增] 国家过滤逻辑
    // [Fix] "ALL" 表示无国家过滤，与空字符串行为一致
    countryUp := strings.ToUpper(country)
    if country != "" && countryUp != "ALL" {
        country = countryUp
        countryKey = "proxy:country:" + country
        hasCountryFilter = true

        if exclude {
            // [Fix] 使用 Lua 脚本原子执行 SINTER + SREM，避免竞态
            // KEYS[1]=active, KEYS[2]=country, KEYS[3]=history, ARGV[1]=count
            script := `
            math.randomseed(redis.call("TIME")[1])
            local intersect = redis.call("SINTER", KEYS[1], KEYS[2])
            if #intersect == 0 then return {} end
            local count = tonumber(ARGV[1])
            if count > #intersect then count = #intersect end
            local result = {}
            for i = 1, count do
                local idx = math.random(#intersect)
                table.insert(result, intersect[idx])
                table.remove(intersect, idx)
            end
            for _, ip in ipairs(result) do
                redis.call("SREM", KEYS[1], ip)
                -- [Fix] 不移除国家索引 KEYS[2]，否则 GetTopCountries 统计膨胀
                redis.call("SREM", KEYS[3], ip)
            end
            return result`
            activeIPs, _ = s.Client.Eval(Ctx, script,
                []string{key, countryKey, "proxy:history:" + protocol},
                count).StringSlice()
        } else {
            // 只读：使用 SINTER 获取交集
            intersectIPs, _ := s.Client.SInter(Ctx, key, countryKey).Result()
            if len(intersectIPs) > 0 {
                rand.Shuffle(len(intersectIPs), func(i, j int) {
                    intersectIPs[i], intersectIPs[j] = intersectIPs[j], intersectIPs[i]
                })
                if len(intersectIPs) > count {
                    activeIPs = intersectIPs[:count]
                } else {
                    activeIPs = intersectIPs
                }
            }
        }
    } else {
        // 无国家过滤
        if exclude {
            // [原子性] 使用 SPOP 原子弹出，避免并发冲突
            activeIPs, _ = s.Client.SPopN(Ctx, key, int64(count)).Result()
        } else {
            // [只读] 使用 SRANDMEMBER 随机获取
            activeIPs, _ = s.Client.SRandMemberN(Ctx, key, int64(count)).Result()
        }
    }

    finalIPs := activeIPs

    // 2. Fallback: 若 Active 不足，从 History 补充
    // [Fix] Fallback 必须尊重国家过滤，否则指定 US 却返回全球代理
    if fallback && len(finalIPs) < count {
        needed := count - len(finalIPs)
        historyKey := "proxy:history:" + protocol

        // 去重合并
        seen := make(map[string]bool)
        for _, ip := range finalIPs {
            seen[ip] = true
        }

        if hasCountryFilter {
            // [Fix] 带国家过滤的 Fallback: 用 SINTER 限定同一国家的历史代理
            histIPs, _ := s.Client.SInter(Ctx, historyKey, countryKey).Result()
            rand.Shuffle(len(histIPs), func(i, j int) {
                histIPs[i], histIPs[j] = histIPs[j], histIPs[i]
            })
            for _, ip := range histIPs {
                if !seen[ip] {
                    finalIPs = append(finalIPs, ip)
                    seen[ip] = true
                    if len(finalIPs) >= count {
                        break
                    }
                }
            }
        } else {
            // 无国家过滤: 从全局历史随机取
            histIPs, _ := s.Client.SRandMemberN(Ctx, historyKey, int64(needed*2)).Result()
            for _, ip := range histIPs {
                if !seen[ip] {
                    finalIPs = append(finalIPs, ip)
                    seen[ip] = true
                    if len(finalIPs) >= count {
                        break
                    }
                }
            }
        }
    }
    
    // 3. Exclude 逻辑补充
    // 如果 exclude=true 且有部分数据来自 History (History 未使用 SPOP)，需要手动清除
    if exclude && len(finalIPs) > len(activeIPs) {
         pipe := s.Client.Pipeline()
         for _, ip := range finalIPs {
             // 确保清理干净 (History + Active)
             pipe.SRem(Ctx, key, ip)           // Active (Redundant but safe)
             pipe.SRem(Ctx, "proxy:history:"+protocol, ip) // History
         }
         pipe.Exec(Ctx)
    }

    // [Fix] 如果国家索引不同步，回退到全协议活跃池
    if hasCountryFilter && len(finalIPs) == 0 {
        allProtocols := []string{"http", "https", "socks4", "socks5"}
        for _, proto := range allProtocols {
            pkey := "proxy:active:" + proto
            activeIPs, _ = s.Client.SInter(Ctx, pkey, countryKey).Result()
            if len(activeIPs) > 0 {
                rand.Shuffle(len(activeIPs), func(i, j int) {
                    activeIPs[i], activeIPs[j] = activeIPs[j], activeIPs[i]
                })
                if len(activeIPs) > count {
                    activeIPs = activeIPs[:count]
                }
                break
            }
        }
        finalIPs = activeIPs
    }

    // [Filter] 匿名代理过滤
    if anonymous && len(finalIPs) > 0 {
        filtered := make([]string, 0, len(finalIPs))
        for _, ip := range finalIPs {
            if ok, _ := s.Client.SIsMember(Ctx, "proxy:anonymous", ip).Result(); ok {
                filtered = append(filtered, ip)
            }
        }
        finalIPs = filtered
    }

    // 转换为模型
    result := make([]model.Proxy, 0, len(finalIPs))
    
    // 批量获取国家信息
    countryMap := make(map[string]string)
    if len(finalIPs) > 0 {
        countries, _ := s.Client.HMGet(Ctx, "proxy:stats:country", finalIPs...).Result()
        for i, ip := range finalIPs {
            if countries[i] != nil {
                countryMap[ip] = countries[i].(string)
            }
        }
    }
    
    for _, ipStr := range finalIPs {
         parts := strings.Split(ipStr, ":")
         if len(parts) == 2 {
             port, _ := strconv.Atoi(parts[1])
             result = append(result, model.Proxy{
                 IP:          parts[0],
                 Port:        port,
                 Protocol:    protocol,
                 CountryCode: countryMap[ipStr], // 国家代码
             })
         }
    }
    return result
}

// GetTopCountries 返回 IP 数量前 N 的国家列表
func (s *Storage) GetTopCountries(limit int) []model.CountryStat {
	// 1. 获取所有 Active 代理IP (去重)
    // 统计必须反映"当前在线"状态，而非历史状态
	protocols := []string{"http", "https", "socks4", "socks5"}
	activeIPs := make(map[string]struct{})

	for _, proto := range protocols {
		ips, err := s.Client.SMembers(Ctx, "proxy:active:"+proto).Result()
		if err == nil {
			for _, ip := range ips {
				activeIPs[ip] = struct{}{}
			}
		}
	}
    
    if len(activeIPs) == 0 {
		return []model.CountryStat{}
	}
    
    uniqueIPList := make([]string, 0, len(activeIPs))
    for ip := range activeIPs {
        uniqueIPList = append(uniqueIPList, ip)
    }

	// 2. 解析 Active IP 的国家信息
    countryCounts := make(map[string]int)
    batchSize := 500
    
    for i := 0; i < len(uniqueIPList); i += batchSize {
        end := i + batchSize
        if end > len(uniqueIPList) {
            end = len(uniqueIPList)
        }
        batchIPs := uniqueIPList[i:end]
        
        // HMGET 批量获取
        results, err := s.Client.HMGet(Ctx, "proxy:stats:country", batchIPs...).Result()
        if err != nil {
            fmt.Printf("[统计] 获取国家信息失败: %v\n", err)
            continue
        }
        
        for _, res := range results {
            if country, ok := res.(string); ok && country != "" {
                 countryCounts[country]++
            }
        }
    }
    
    // 3. Convert to Slice
    var stats []model.CountryStat
    for code, count := range countryCounts {
         stats = append(stats, model.CountryStat{
             CountryCode: code,
             Count:       count,
         })
    }

	// 4. Sort Descending
	for i := 0; i < len(stats)-1; i++ {
		for j := i + 1; j < len(stats); j++ {
			if stats[j].Count > stats[i].Count {
				stats[i], stats[j] = stats[j], stats[i]
			}
		}
	}

	// 5. Limit
	if len(stats) > limit {
		stats = stats[:limit]
	}

	return stats
}
