# Antigravity Proxy Checker

高性能代理 IP 采集、检测、分发系统。Go 语言构建，SOCKS5/HTTP/HTTPS/SOCKS4 全协议代理池管理。

## 功能模块

| 模块 | 说明 |
|------|------|
| **Scraper** | 从在线订阅源/本地文件抓取代理，正则 + net.ParseIP 双校验 |
| **Checker** | 双重检测：可用性(URL+Keyword) + 归属国提取(Type1)。协程池并发，质量门控 |
| **API Server** | RESTful 提取代理，国家/匿名过滤，Fallback 回退，格式/显示控制 |
| **Rotator** | 原生 SOCKS5/HTTP CONNECT，用户名=国家代码路由出口 |
| **Monitor** | 队列水位维护，FIFO 回填（跳过活跃代理免重复） |
| **Web UI** | 协议分布、Top5 排行、CPU/MEM、API 调试器、链接生成器、5 主题、手机适配 |

## 架构

```
订阅源/文件 → Scraper → proxy:all_seen → proxy:pending (FIFO)
proxy:pending → Checker → proxy:active:{proto} + 匿名标记 + 归属国
proxy:active   → API     → GET /api/get?country=US&anonymous=true
proxy:active   → Rotator → SOCKS5 出口路由
pending<阈值   → Monitor → proxy:all_seen 回填 (跳过活跃)
```

## 快速开始

- Redis 必须运行，默认 `localhost:6379`
- Go 1.20+ 编译

```bash
go build -o build\proxychecker.exe .
```

Android (Termux)：复制二进制 + config.yaml + web/ 到 $HOME，`chmod +x` 运行。Android 上需设 `insecure_skip_verify: true`（无 root CA）。

## API

### GET /api/get

| 参数 | 默认 | 说明 |
|:---|:---|:---|
| `protocol` | http | http, https, socks4, socks5 |
| `country` | 全部 | US, CN, JP... 或 ALL |
| `num` | 1 | 1-5000 |
| `format` | json | json / txt |
| `fallback` | false | 活跃不足时从历史补 |
| `exclude` | false | 提取后是否移除 |
| `show_country` | false | txt 是否附 `\|CN` |
| `anonymous` | false | 仅匿名代理 |

```bash
curl "http://IP:16378/api/get?protocol=socks5&country=US&num=10&format=txt&anonymous=true"
```

### GET /api/stats · /api/system · /api/ip?ip=x.x.x.x

## Rotator 代理

```
SOCKS5: your-server:16377
用户名: US（国家代码大写）
密码:   config.yaml 的 password
```

## 配置精简参考

```yaml
# ── Redis ──
redis_url: "redis://:{{redis_password}}@{{base_domain}}:{{base_port_redis}}/1"
redis_password: "pass"

# ── 关键配置 ──
insecure_skip_verify: false   # Android 必须 true
concurrency: 100
enable_check_http: false       # 关 HTTP，仅检测 socks5/socks4
enable_checker_type1: true     # check_url1 提取归属国
check_url1: "https://ip.me"
check_url2: "https://docs.github.com"
check_keyword2: "/site-policy/github-terms"

# ── Rotator ──
enable_rotator: true
rotator_port: "16377"
```

完整配置见 `config.yaml`。

## 常见问题

**网页和 API 数据不一致** → `redis-cli -n 1 DEL proxy:index:rebuilt` 后重启

**Android 检测为 0** → `insecure_skip_verify: true`

**Android DNS 超时** → redis_url 用 IP 直连

**提取不足** → `&fallback=true`

## 维护

| 脚本 | 用途 |
|------|------|
| `clear_data.bat` | 清空检测数据，保留 IP 矿池 |
| `backup_ips.bat` | 备份 proxy:all_seen |
| `restore_ips.bat` | 恢复 IP 到 proxy:all_seen |

## Redis 结构

| Key | 类型 | 说明 |
|:---|:---|:---|
| `proxy:all_seen` | Set | 所有发现的代理 |
| `proxy:pending` | List | 待测队列 (FIFO) |
| `proxy:active:{proto}` | Set | 活跃代理 |
| `proxy:history:{proto}` | Set | 历史代理 (Fallback) |
| `proxy:country:{CODE}` | Set | 国家索引 |
| `proxy:anonymous` | Set | 匿名代理 |
| `proxy:stats:country` | Hash | IP→国家 |
| `proxy:stats:success/fail/total` | Hash | 统计 |
| `proxy:index:rebuilt` | String | 重建哨兵 |
