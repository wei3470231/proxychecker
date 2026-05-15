package rotator

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
	"strconv"

	"proxychecker/internal/config"
	"proxychecker/internal/storage"
)

type Server struct {
	Storage *storage.Storage
}

func New(s *storage.Storage) *Server {
	return &Server{Storage: s}
}

func (s *Server) Run() error {
	if config.RotatorPort == "" {
		return nil
	}

	listener, err := net.Listen("tcp", "0.0.0.0:"+config.RotatorPort)
	if err != nil {
		return err
	}
	fmt.Printf("[Rotator] 轮换代理服务已启动，监听端口: %s (SOCKS5/HTTPS)\n", config.RotatorPort)

	// 简单连接数限制（防DoS）
	sem := make(chan struct{}, 500)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] 接收连接错误: %v\n", err)
			}
			continue
		}

		select {
		case sem <- struct{}{}:
			go func(c net.Conn) {
				defer func() { <-sem }()
				s.handleConn(c)
			}(conn)
		default:
			// 超出连接上限，拒绝
			conn.Close()
			if config.DebugMode {
				fmt.Printf("[Rotator] 连接数超限，拒绝: %s\n", conn.RemoteAddr())
			}
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()

	// 设置 TCP KeepAlive 防止僵死连接
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// 设置读取超时 (握手阶段)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 预读 1 字节判断协议
	reader := bufio.NewReader(conn)
	b, err := reader.Peek(1)
	if err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] 连接 %s 预读失败: %v\n", remoteAddr, err)
		}
		return
	}


	var targetCountry string
	var targetHost string
	var targetPort int
	var socks5Success []byte // SOCKS5 成功响应（延迟到出口代理连通后发送）

	if b[0] == 0x05 {
		// SOCKS5
		targetCountry, targetHost, targetPort, socks5Success = s.handleSocks5(conn, reader)
	} else {
		// HTTP
		targetCountry, targetHost, targetPort = s.handleHTTP(conn, reader)
	}

	if config.DebugMode {
		fmt.Printf("[Rotator] 用户:%s 目标:%s:%d\n", targetCountry, targetHost, targetPort)
	}

	if targetCountry == "" || targetHost == "" {
		if config.DebugMode {
			fmt.Printf("[Rotator] 连接 %s 握手失败，关闭。\n", remoteAddr)
		}
		return
	}

	// 白名单直连检查
	if matchBypass(targetHost, config.RotatorBypassList) {

		// [Fix] SOCKS5 客户端在等待成功响应，必须先发送
		if socks5Success != nil {
			conn.Write(socks5Success)
		}

		conn.SetReadDeadline(time.Time{})

		outConn, err := net.DialTimeout("tcp", net.JoinHostPort(targetHost, strconv.Itoa(targetPort)), 5*time.Second)
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] 直连失败: %v\n", err)
			}
			return
		}
		defer outConn.Close()
		relayConn(conn, outConn, reader)
		return
	}

	// Reset Deadline for piping
	conn.SetReadDeadline(time.Time{})

	// Retry Loop
	var outConn net.Conn
	var exitProxyStr, exitProto, realCountry string

	maxRetries := config.RotatorRetryCount
	dialTimeout := time.Duration(config.RotatorDialTimeout) * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		var err error
		exitProxyStr, exitProto, err = s.Storage.GetRandomActiveProxyByCountry(targetCountry)
		if err != nil {
			if i == maxRetries-1 {
				if config.DebugMode {
					fmt.Printf("[轮换] 国家 %s 活跃代理不足，无法提供服务，断开连接\n", targetCountry)
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		realCountry, _ = s.Storage.Client.HGet(context.Background(), "proxy:stats:country", exitProxyStr).Result()
		if realCountry == "" {
			realCountry = "Unknown"
		}

		outConn, err = net.DialTimeout("tcp", exitProxyStr, dialTimeout)
		if err != nil {
			if config.DebugMode {
				fmt.Printf("远程代理 %s 连接失败: %v. 正在移除并重试...\n", exitProxyStr, err)
			}
			s.Storage.RemoveProxyFromAll(exitProxyStr)
			continue
		}
		break
	}

	if outConn == nil {
		if config.DebugMode {
			fmt.Printf("所有重试均失败，无法连接目标: %s\n", targetHost)
		}
		return
	}
	defer outConn.Close()

	// [Fix] SOCKS5 成功响应延迟到这里发送（出口代理已连通）
	if socks5Success != nil {
		conn.Write(socks5Success)
	}

	// 处理上游握手
	switch exitProto {
	case "http", "https":
		fmt.Fprintf(outConn, "CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n\r\n", targetHost, targetPort, targetHost, targetPort)

		respReader := bufio.NewReader(outConn)
		line, err := respReader.ReadString('\n')
		if err != nil || !strings.Contains(line, "200") {
			if config.DebugMode {
				fmt.Printf("出口代理 %s HTTP 握手失败: %s\n", exitProxyStr, line)
			}
			return
		}
		for {
			l, err := respReader.ReadString('\n')
			if err != nil || l == "\r\n" {
				break
			}
		}

		if config.DebugMode {
			fmt.Printf("分配%s代理: %s | 真实归属: %s | 协议: %s | 目标: %s:%d | 来源: %s\n", targetCountry, exitProxyStr, realCountry, exitProto, targetHost, targetPort, remoteAddr)
		}

		relayConnBuffered(conn, respReader, outConn, reader)

	case "socks5":
		// 握手
		outConn.Write([]byte{0x05, 0x01, 0x00})
		buf := make([]byte, 2)
		if _, err := io.ReadFull(outConn, buf); err != nil || buf[0] != 0x05 || buf[1] != 0x00 {
			if config.DebugMode {
				fmt.Printf("Socks5 握手失败，代理: %s\n", exitProxyStr)
			}
			return
		}

		req := []byte{0x05, 0x01, 0x00}
		ip := net.ParseIP(targetHost)
		if ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				req = append(req, 0x01)
				req = append(req, ip4...)
			} else {
				req = append(req, 0x04)
				req = append(req, ip.To16()...)
			}
		} else {
			req = append(req, 0x03)
			req = append(req, byte(len(targetHost)))
			req = append(req, []byte(targetHost)...)
		}

		portBytes := []byte{byte(targetPort >> 8), byte(targetPort & 0xff)}
		req = append(req, portBytes...)
		outConn.Write(req)

		head := make([]byte, 4)
		if _, err := io.ReadFull(outConn, head); err != nil || head[1] != 0x00 {
			return
		}

		atyp := head[3]
		if atyp == 0x01 {
			io.CopyN(io.Discard, outConn, 4+2)
		} else if atyp == 0x03 {
			lenB := make([]byte, 1)
			io.ReadFull(outConn, lenB)
			io.CopyN(io.Discard, outConn, int64(lenB[0])+2)
		} else if atyp == 0x04 {
			io.CopyN(io.Discard, outConn, 16+2)
		}

		if config.DebugMode {
			fmt.Printf("分配%s代理: %s | 真实归属: %s | 协议: %s | 目标: %s:%d | 来源: %s\n", targetCountry, exitProxyStr, realCountry, exitProto, targetHost, targetPort, remoteAddr)
		}

		relayConn(conn, outConn, reader)

	case "socks4":
		ip := net.ParseIP(targetHost)
		if ip == nil || ip.To4() == nil {
			return
		}

		req := []byte{0x04, 0x01}
		req = append(req, byte(targetPort>>8), byte(targetPort&0xff))
		req = append(req, ip.To4()...)
		req = append(req, []byte("moo")...)
		req = append(req, 0x00)
		outConn.Write(req)

		rep := make([]byte, 8)
		if _, err := io.ReadFull(outConn, rep); err != nil || rep[1] != 0x5A {
			return
		}

		fmt.Printf("分配%s代理: %s | 真实归属: %s | 协议: %s | 目标: %s:%d | 来源: %s\n", targetCountry, exitProxyStr, realCountry, exitProto, targetHost, targetPort, remoteAddr)

		relayConn(conn, outConn, reader)

	default:
		if config.DebugMode {
			fmt.Printf("分配%s代理: %s | 真实归属: %s | 协议: %s | 目标: %s:%d | 来源: %s\n", targetCountry, exitProxyStr, realCountry, exitProto, targetHost, targetPort, remoteAddr)
		}
		relayConn(conn, outConn, reader)
	}
}

func (s *Server) handleSocks5(conn net.Conn, reader *bufio.Reader) (string, string, int, []byte) {
	// 1. 协商
	_, err := reader.ReadByte() // Ver
	if err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 读取版本失败: %v\n", err)
		}
		return "", "", 0, nil
	}

	nmethods, err := reader.ReadByte()
	if err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 读取方法数失败: %v\n", err)
		}
		return "", "", 0, nil
	}

	ms := make([]byte, int(nmethods))
	_, err = io.ReadFull(reader, ms)
	if err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 读取方法失败: %v\n", err)
		}
		return "", "", 0, nil
	}

	// [Fix] 根据是否配置密码选择认证方式
	var user string
	if config.Password == "" {
		conn.Write([]byte{0x05, 0x00})
		user = ""
	} else {
		conn.Write([]byte{0x05, 0x02})

		_, err = reader.ReadByte()
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证版本失败: %v\n", err)
			}
			return "", "", 0, nil
		}

		ulen, err := reader.ReadByte()
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证用户长度失败: %v\n", err)
			}
			return "", "", 0, nil
		}

		u := make([]byte, int(ulen))
		if _, err := io.ReadFull(reader, u); err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证读取用户失败: %v\n", err)
			}
			return "", "", 0, nil
		}

		plen, err := reader.ReadByte()
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证密码长度失败: %v\n", err)
			}
			return "", "", 0, nil
		}

		p := make([]byte, int(plen))
		if _, err := io.ReadFull(reader, p); err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证读取密码失败: %v\n", err)
			}
			return "", "", 0, nil
		}

		// 校验密码 (用户名作为国家代码，不在此校验)
		if string(p) != config.Password {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 认证失败. 密码错误.\n")
			}
			conn.Write([]byte{0x01, 0x01})
			return "", "", 0, nil
		}

		conn.Write([]byte{0x01, 0x00})

		user = string(u)
	}
	country := strings.Split(user, ":")[0]

	// 3. 请求
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 请求头失败: %v\n", err)
		}
		return "", "", 0, nil
	}

	cmd := header[1]
	atyp := header[3]

	var host string
	if atyp == 0x03 {
		dlen, err := reader.ReadByte()
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 请求读取域名长度失败: %v\n", err)
			}
			return "", "", 0, nil
		}
		d := make([]byte, int(dlen))
		if _, err := io.ReadFull(reader, d); err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 请求读取域名失败: %v\n", err)
			}
			return "", "", 0, nil
		}
		host = string(d)
	} else if atyp == 0x01 {
		ip := make([]byte, 4)
		if _, err := io.ReadFull(reader, ip); err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 请求读取IPv4失败: %v\n", err)
			}
			return "", "", 0, nil
		}
		host = net.IP(ip).String()
	} else if atyp == 0x04 {
		ip := make([]byte, 16)
		if _, err := io.ReadFull(reader, ip); err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] S5 请求读取IPv6失败: %v\n", err)
			}
			return "", "", 0, nil
		}
		host = net.IP(ip).String()
	} else {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 请求未知 ATYP: 0x%02x\n", atyp)
		}
		return "", "", 0, nil
	}

	pb := make([]byte, 2)
	if _, err := io.ReadFull(reader, pb); err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 请求读取端口失败: %v\n", err)
		}
		return "", "", 0, nil
	}
	port := int(pb[0])<<8 | int(pb[1])

	// [Fix] 延迟发送成功响应，等出口代理连通后由 handleConn 发送
	var successResp []byte
	if cmd == 0x01 {
		successResp = []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	} else {
		if config.DebugMode {
			fmt.Printf("[Rotator] S5 请求未知 CMD: 0x%02x\n", cmd)
		}
		return "", "", 0, nil
	}

	return country, host, port, successResp
}

func (s *Server) handleHTTP(conn net.Conn, reader *bufio.Reader) (string, string, int) {
	line, err := reader.ReadString('\n')
	if err != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] HTTP 读取请求行失败: %v\n", err)
		}
		return "", "", 0
	}

	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		if config.DebugMode {
			fmt.Printf("[Rotator] HTTP 请求行无效: %s\n", line)
		}
		return "", "", 0
	}

	method := parts[0]
	target := parts[1]

	var user string
	for {
		l, err := reader.ReadString('\n')
		if err != nil {
			if config.DebugMode {
				fmt.Printf("[Rotator] HTTP 读取 Header 失败: %v\n", err)
			}
			break
		}
		if l == "\r\n" {
			break
		}

		if strings.HasPrefix(l, "Proxy-Authorization: Basic ") {
			auth := strings.TrimSpace(strings.TrimPrefix(l, "Proxy-Authorization: Basic "))
			creds, decodeErr := base64.StdEncoding.DecodeString(auth)
			if decodeErr != nil {
				if config.DebugMode {
					fmt.Printf("[Rotator] HTTP Base64 解码失败: %v\n", decodeErr)
				}
				continue
			}
			if idx := strings.Index(string(creds), ":"); idx != -1 {
				user = string(creds)[:idx]
				pass := string(creds)[idx+1:]
				if config.Password != "" && pass != config.Password {
					if config.DebugMode {
						fmt.Printf("[Rotator] HTTP 认证失败: 密码不匹配\n")
					}
					conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"Proxy\"\r\n\r\n"))
					return "", "", 0
				}
			} else {
				if config.DebugMode {
					fmt.Printf("[Rotator] HTTP Basic Auth 格式无效: %s\n", string(creds))
				}
			}
		}
	}

	if config.Password != "" && user == "" {
		conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"Proxy\"\r\n\r\n"))
		return "", "", 0
	}

	if method == "CONNECT" {
		conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	} else {
		if config.DebugMode {
			fmt.Printf("[Rotator] HTTP 不支持的方法: %s\n", method)
		}
		return "", "", 0
	}

	host, portStr, splitErr := net.SplitHostPort(target)
	if splitErr != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] HTTP SplitHostPort 失败 %s: %v\n", target, splitErr)
		}
		return "", "", 0
	}
	port, atoiErr := strconv.Atoi(portStr)
	if atoiErr != nil {
		if config.DebugMode {
			fmt.Printf("[Rotator] HTTP 端口转数字失败 %s: %v\n", portStr, atoiErr)
		}
		return "", "", 0
	}

	country := strings.Split(user, ":")[0]
	if country == "" {
		country = "ALL"
	}

	return country, host, port
}

// relayConn 双向转发 (含 Context 管理 + 闲置超时)
func relayConn(conn, upstream net.Conn, bufReader *bufio.Reader) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}
	if tcp, ok := upstream.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	go func() {
		io.Copy(conn, upstream)
		cancel()
	}()

	go func() {
		io.Copy(upstream, bufReader)
		cancel()
	}()

	<-ctx.Done()
	conn.Close()
	upstream.Close()
}

// relayConnBuffered 双向转发 (HTTP CONNECT 专用，双方均有缓存)
func relayConnBuffered(conn net.Conn, upstreamBuf *bufio.Reader, upstream net.Conn, clientBuf *bufio.Reader) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}
	if tcp, ok := upstream.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(30 * time.Second)
	}

	go func() {
		io.Copy(conn, upstreamBuf)
		cancel()
	}()

	go func() {
		io.Copy(upstream, clientBuf)
		cancel()
	}()

	<-ctx.Done()
	conn.Close()
	upstream.Close()
}

// 辅助函数：通配符匹配 (大小写不敏感)
func matchBypass(host string, patterns []string) bool {
	host = strings.ToLower(host)

	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		pattern = strings.ToLower(pattern)

		// 精确匹配
		if host == pattern {
			return true
		}

		// 通配符匹配: 仅支持前缀 * 和后缀 *，中间 * 视为精确匹配
		if strings.Contains(pattern, "*") {
			if strings.HasSuffix(pattern, "*") && strings.HasPrefix(pattern, "*") {
				// 两边通配符: *xxx*  → 包含匹配
				middle := strings.Trim(pattern, "*")
				if middle != "" && strings.Contains(host, middle) {
					return true
				}
			} else if strings.HasSuffix(pattern, "*") {
				// 后缀通配: xxx* → 前缀匹配
				prefix := strings.TrimSuffix(pattern, "*")
				if strings.HasPrefix(host, prefix) {
					return true
				}
			} else if strings.HasPrefix(pattern, "*") {
				// 前缀通配: *xxx → 后缀匹配
				suffix := strings.TrimPrefix(pattern, "*")
				if strings.HasSuffix(host, suffix) {
					return true
				}
			}
		}
	}
	return false
}
