package api

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"proxychecker/internal/config"
	"proxychecker/internal/storage"
)

type Server struct {
	Storage *storage.Storage
	Engine  *gin.Engine
}

func New(s *storage.Storage) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	srv := &Server{Storage: s, Engine: r}
	srv.setupRoutes()
	return srv
}

func (s *Server) setupRoutes() {
	s.Engine.Use(s.authMiddleware())

	webDir := "web"
	if exePath, err := os.Executable(); err == nil {
		webDir = filepath.Join(filepath.Dir(exePath), "web")
	}
	if _, err := os.Stat(webDir); os.IsNotExist(err) {
		webDir = "web"
	}

	s.Engine.Static("/static", webDir)
	s.Engine.LoadHTMLGlob(filepath.Join(webDir, "*.html"))
	s.Engine.GET("/", func(c *gin.Context) { c.File(filepath.Join(webDir, "index.html")) })

	api := s.Engine.Group("/api")
	{
		api.GET("/stats", s.handleStats)
		api.GET("/system", s.handleSystemStats)
		api.GET("/get", s.handleGetProxy)
		api.GET("/ip", s.handleIP)
	}
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if config.Password == "" {
			c.Next()
			return
		}
		if c.Query("password") == config.Password {
			c.Next()
			return
		}
		user, pass, hasAuth := c.Request.BasicAuth()
		if hasAuth && pass == config.Password && (config.Username == "" || user == config.Username) {
			c.Next()
			return
		}
		if c.Request.URL.Path == "/" || strings.HasPrefix(c.Request.URL.Path, "/api") {
			c.Header("WWW-Authenticate", `Basic realm="Restricted"`)
		}
		c.AbortWithStatus(401)
	}
}

func (s *Server) handleIP(c *gin.Context) {
	ipStr := c.Query("ip")
	if ipStr == "" {
		ipStr = c.ClientIP()
	}
	c.JSON(200, gin.H{"ip": ipStr, "country_code": "", "country_name": "", "city": "", "time_zone": ""})
}

func isValidProto(p string) bool {
	switch p {
	case "http", "https", "socks4", "socks5":
		return true
	}
	return false
}

func (s *Server) Run() error {
	addr := fmt.Sprintf("%s:%s", config.ServerHost, config.ServerPort)
	fmt.Printf("[API] API 服务运行于 http://%s\n", addr)
	return s.Engine.Run(addr)
}

func (s *Server) handleStats(c *gin.Context) {
	stats := s.Storage.GetAllStats()
	topCountries := s.Storage.GetTopCountries(5)
	response := make(map[string]interface{})
	for k, v := range stats {
		response[k] = v
	}
	response["top_countries"] = topCountries
	c.JSON(200, response)
}

type GetProxyParams struct {
	Protocol string `form:"protocol"`
	Country  string `form:"country"`
	Num      int    `form:"num,default=1"`
	Format   string `form:"format,default=json"`
	Sep      string `form:"sep,default=\r\n"`
	// Exclude manually handled
	Fallback    bool `form:"fallback,default=false"`
	ShowCountry bool `form:"show_country,default=false"`
	Anonymous   bool `form:"anonymous,default=false"`
}

func (s *Server) handleGetProxy(c *gin.Context) {
	var params GetProxyParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.String(404, "Not Found")
		return
	}
	if params.Protocol != "" && !isValidProto(params.Protocol) {
		c.String(404, "Not Found")
		return
	}
	if len(params.Country) > 10 {
		c.String(404, "Not Found")
		return
	}
	for _, ch := range params.Country {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
			c.String(404, "Not Found")
			return
		}
	}
	if params.Format != "json" && params.Format != "txt" {
		c.String(404, "Not Found")
		return
	}
	if params.Num <= 0 || params.Num > 5000 {
		c.String(404, "Not Found")
		return
	}

	shouldExclude := config.APIExcludeAfterGet
	if params.Format == "txt" {
		shouldExclude = false
	}
	if ep := c.Query("exclude"); ep != "" {
		shouldExclude = ep == "true" || ep == "1"
	}

	proxies := s.Storage.GetProxies(params.Protocol, params.Country, params.Num, params.Fallback, shouldExclude, params.Anonymous)
	if len(proxies) == 0 {
		c.String(200, "ip不足")
		return
	}

	if params.Format == "txt" {
		sep := strings.ReplaceAll(params.Sep, "\\n", "\n")
		sep = strings.ReplaceAll(sep, "\\r", "\r")
		sep = strings.ReplaceAll(sep, "\\t", "\t")
		var lines []string
		for _, p := range proxies {
			line := fmt.Sprintf("%s:%d", p.IP, p.Port)
			if params.ShowCountry {
				if p.CountryCode != "" {
					line += "|" + p.CountryCode
				} else if p.Country != "" {
					line += "|" + p.Country
				}
			}
			lines = append(lines, line)
		}
		c.String(200, strings.Join(lines, sep))
		return
	}

	if params.Num == 1 {
		c.JSON(200, proxies[0])
	} else {
		c.JSON(200, proxies)
	}
}

func (s *Server) handleSystemStats(c *gin.Context) {
	var cpuUsage, memUsage float64
	if p, err := cpu.Percent(0, false); err == nil && len(p) > 0 {
		cpuUsage = p[0]
	}
	if v, err := mem.VirtualMemory(); err == nil {
		memUsage = v.UsedPercent
	}
	c.JSON(200, gin.H{"cpu": cpuUsage, "mem": memUsage})
}
