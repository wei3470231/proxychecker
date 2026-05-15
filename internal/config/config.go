package config

import (
	"fmt"
	"os"
	"log"
	"time"
	"bytes"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	BaseDomain     string `yaml:"base_domain"`
	BasePortWebAPI string `yaml:"base_port_webapi"`
	BasePortRedis  string `yaml:"base_port_redis"`

	Password string `yaml:"password"`
	Username string `yaml:"username"`

	RedisURL            string `yaml:"redis_url"`
	RedisPassword       string `yaml:"redis_password"`
	RedisConnectTimeout int    `yaml:"redis_connect_timeout"`
	DistributedLockTTL  int    `yaml:"distributed_lock_ttl"`
	LockRetryInterval   int    `yaml:"lock_retry_interval"`

	Concurrency         int `yaml:"concurrency"`
	QueueTargetSize     int `yaml:"queue_target_size"`
	BatchSizeStorageAdd int `yaml:"batch_size_storage_add"`

	CheckTimeout   int    `yaml:"check_timeout"`
	ScraperTimeout int    `yaml:"scraper_timeout"`
	UserAgent      string `yaml:"user_agent"`

	CheckURL           string `yaml:"check_url"`
	CheckKeyword       string `yaml:"check_keyword"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`

	EnableCheckerType1 bool   `yaml:"enable_checker_type1"`
	CheckURL1          string `yaml:"check_url1"`
	CheckKeyword1      string `yaml:"check_keyword1"`

	CheckURL2     string `yaml:"check_url2"`
	CheckKeyword2 string `yaml:"check_keyword2"`

	DNSServer     string   `yaml:"dns_server"`
	DNSServerList []string `yaml:"dns_server_list"`

	ProxyLeaseTime     int `yaml:"proxy_lease_time"`
	IntervalThreshold  int `yaml:"interval_threshold"`
	IntervalScraper    int `yaml:"interval_scraper"`
	LogIntervalWorker  int `yaml:"log_interval_worker"`
	LogIntervalChecker int `yaml:"log_interval_checker"`

	ServerHost         string `yaml:"server_host"`
	ServerPort         string `yaml:"server_port"`
	APIExcludeAfterGet bool   `yaml:"api_exclude_after_get"`

	RotatorPort        string   `yaml:"rotator_port"`
	RotatorRetryCount  int      `yaml:"rotator_retry_count"`
	RotatorDialTimeout int      `yaml:"rotator_dial_timeout"`
	RotatorBypassList  []string `yaml:"rotator_bypass_list"`

	FilterQualityGateEnabled bool `yaml:"filter_quality_gate_enabled"`
	FilterMinTotalChecks     int  `yaml:"filter_min_total_checks"`
	FilterMinSuccessCount    int  `yaml:"filter_min_success_count"`

	EnableScraper    bool `yaml:"enable_scraper"`
	EnableChecker    bool `yaml:"enable_checker"`
	EnableAPI        bool `yaml:"enable_api"`
	EnableRotator    bool `yaml:"enable_rotator"`
	EnableCheckHTTP  bool `yaml:"enable_check_http"`
	DebugMode        bool `yaml:"debug_mode"`

	ProxySourcesURL []string `yaml:"proxy_sources_url"`
	ProxyURLFile    string   `yaml:"proxy_url_file"`
	ProxySourcesTxt []string `yaml:"proxy_sources_txt"`
}

// Global Variables
var (
	Password   string
	Username   string

	RedisURL            string
	RedisPassword       string
	RedisConnectTimeout time.Duration
	DistributedLockTTL  time.Duration
	LockRetryInterval   time.Duration

	Concurrency         int
	QueueTargetSize     int
	BatchSizeStorageAdd int

	CheckTimeout   time.Duration
	ScraperTimeout time.Duration
	UserAgent      string

	CheckURL           string
	CheckKeyword       string
	InsecureSkipVerify bool

	EnableCheckerType1 bool
	CheckURL1          string
	CheckKeyword1      string
	CheckURL2          string
	CheckKeyword2      string
	DNSServerList      []string
	DebugMode          bool

	EnableScraper   bool
	EnableChecker   bool
	EnableAPI       bool
	EnableRotator   bool
	EnableCheckHTTP bool

	RotatorRetryCount  int
	RotatorDialTimeout int
	RotatorBypassList  []string

	ProxyLeaseTime     time.Duration
	IntervalThreshold  time.Duration
	IntervalScraper    time.Duration
	LogIntervalWorker  time.Duration
	LogIntervalChecker int

	ServerHost         string
	ServerPort         string
	APIExcludeAfterGet bool
	RotatorPort        string

	FilterQualityGateEnabled bool
	FilterMinTotalChecks     int
	FilterMinSuccessCount    int

	ProxySourcesURL []string
	ProxyURLFile    string
	ProxySourcesTxt []string
)

func Load(path string) {
	fmt.Println("Config Load Start")
	if path == "" {
		path = "config.yaml"
	}

	data, err := os.ReadFile(path)
	if len(data) >= 3 && bytes.Equal(data[:3], []byte("\xef\xbb\xbf")) {
		data = data[3:]
	}

	if err != nil {
		fmt.Printf("Config not found: %v, using defaults\n", err)
		RedisURL = "redis://@127.0.0.1:6379/1"
		Concurrency = 10
		return
	}

	var cfg Config
	if err = yaml.Unmarshal(data, &cfg); err != nil {
		log.Printf("YAML parse error: %v\n", err)
	}

	// Fallback base_domain from raw text
	if cfg.BaseDomain == "" {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "base_domain:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					cfg.BaseDomain = strings.Trim(parts[1], " \"'\r\n")
					break
				}
			}
		}
	}

	// Variable substitution
	if cfg.BaseDomain != "" {
		repl := func(s string) string {
			s = strings.ReplaceAll(s, "{{base_domain}}", cfg.BaseDomain)
			return strings.ReplaceAll(s, "{{domain}}", cfg.BaseDomain)
		}
		cfg.RedisURL = repl(cfg.RedisURL)
			cfg.CheckURL = repl(cfg.CheckURL)
	}
	if cfg.BasePortRedis != "" {
		cfg.RedisURL = strings.ReplaceAll(cfg.RedisURL, "{{base_port_redis}}", cfg.BasePortRedis)
	}
	if cfg.BasePortWebAPI != "" {
		cfg.ServerPort = strings.ReplaceAll(cfg.ServerPort, "{{base_port_webapi}}", cfg.BasePortWebAPI)
		}
	if cfg.RedisPassword != "" {
		cfg.RedisURL = strings.ReplaceAll(cfg.RedisURL, "{{redis_password}}", cfg.RedisPassword)
	}

	// Masked log
	maskedRedis := cfg.RedisURL
	if parts := strings.SplitN(cfg.RedisURL, "@", 2); len(parts) == 2 {
		maskedRedis = "redis://*****@" + parts[1]
	}
	fmt.Printf("Redis URL: %s\n", maskedRedis)

	// Map to globals
	RedisURL = cfg.RedisURL
	RedisPassword = cfg.RedisPassword
	RedisConnectTimeout = time.Duration(cfg.RedisConnectTimeout) * time.Second
	DistributedLockTTL = time.Duration(cfg.DistributedLockTTL) * time.Second
	LockRetryInterval = time.Duration(cfg.LockRetryInterval) * time.Millisecond

	Concurrency = cfg.Concurrency
	QueueTargetSize = cfg.QueueTargetSize
	BatchSizeStorageAdd = cfg.BatchSizeStorageAdd

	CheckTimeout = time.Duration(cfg.CheckTimeout) * time.Second
	ScraperTimeout = time.Duration(cfg.ScraperTimeout) * time.Second
	UserAgent = cfg.UserAgent

	CheckURL = cfg.CheckURL
	CheckKeyword = cfg.CheckKeyword
	InsecureSkipVerify = cfg.InsecureSkipVerify

	EnableCheckerType1 = cfg.EnableCheckerType1
	CheckURL1 = cfg.CheckURL1
	CheckKeyword1 = cfg.CheckKeyword1
	CheckURL2 = cfg.CheckURL2
	CheckKeyword2 = cfg.CheckKeyword2

	if cfg.BaseDomain != "" {
		repl := func(s string) string {
			s = strings.ReplaceAll(s, "{{base_domain}}", cfg.BaseDomain)
			return strings.ReplaceAll(s, "{{domain}}", cfg.BaseDomain)
		}
		CheckURL1 = repl(CheckURL1)
		CheckURL2 = repl(CheckURL2)
	}

	if len(cfg.DNSServerList) > 0 {
		DNSServerList = cfg.DNSServerList
	} else if cfg.DNSServer != "" {
		DNSServerList = []string{cfg.DNSServer}
	}

	DebugMode = cfg.DebugMode
	Password = cfg.Password
	Username = cfg.Username

	EnableScraper = cfg.EnableScraper
	EnableChecker = cfg.EnableChecker
	EnableAPI = cfg.EnableAPI
	EnableRotator = cfg.EnableRotator
	EnableCheckHTTP = cfg.EnableCheckHTTP

	ProxyLeaseTime = time.Duration(cfg.ProxyLeaseTime) * time.Minute
	IntervalThreshold = time.Duration(cfg.IntervalThreshold) * time.Minute
	IntervalScraper = time.Duration(cfg.IntervalScraper) * time.Second
	LogIntervalWorker = time.Duration(cfg.LogIntervalWorker) * time.Second
	LogIntervalChecker = cfg.LogIntervalChecker

	ServerHost = cfg.ServerHost
	ServerPort = cfg.ServerPort
	APIExcludeAfterGet = cfg.APIExcludeAfterGet

	RotatorPort = cfg.RotatorPort
	RotatorRetryCount = cfg.RotatorRetryCount
	RotatorDialTimeout = cfg.RotatorDialTimeout
	RotatorBypassList = cfg.RotatorBypassList

	if RotatorRetryCount <= 0 {
		RotatorRetryCount = 5
	}
	if RotatorDialTimeout <= 0 {
		RotatorDialTimeout = 3000
	}

	FilterQualityGateEnabled = cfg.FilterQualityGateEnabled
	FilterMinTotalChecks = cfg.FilterMinTotalChecks
	FilterMinSuccessCount = cfg.FilterMinSuccessCount

	ProxySourcesURL = cfg.ProxySourcesURL
	ProxyURLFile = cfg.ProxyURLFile
	ProxySourcesTxt = cfg.ProxySourcesTxt

	fmt.Println("[配置] 成功加载配置文件 config.yaml")
}
