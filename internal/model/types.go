package model

type Proxy struct {
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Protocol    string `json:"protocol"` // http, https, socks4, socks5
	Country     string `json:"country,omitempty"`
	CountryCode string `json:"country_code,omitempty"`
	City        string `json:"city,omitempty"`
	Latency     int    `json:"latency,omitempty"` // ms
	LastCheck   int64  `json:"last_check,omitempty"`
	Source      string `json:"source,omitempty"`
}

type CountryStat struct {
    CountryCode string `json:"country_code"`
    Count       int    `json:"count"`
}
