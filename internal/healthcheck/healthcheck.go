package healthcheck

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

func URL(addr, path string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if ip, err := netip.ParseAddr(host); host == "" || (err == nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + path, nil
}

func Run(addr, path string, stdout, stderr io.Writer) int {
	target, err := URL(addr, path)
	if err != nil {
		fmt.Fprintln(stderr, "probe:", err)
		return 2
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(target)
	if err != nil {
		fmt.Fprintln(stderr, "probe:", err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(stdout, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "probe: status", resp.StatusCode)
		return 1
	}
	return 0
}

func Command(args []string, addr string, stdout, stderr io.Writer) (int, bool) {
	if len(args) < 2 {
		return 0, false
	}
	switch args[1] {
	case "healthcheck":
		return Run(addr, "/healthz", io.Discard, stderr), true
	case "metrics":
		return Run(addr, "/metrics", stdout, stderr), true
	}
	return 0, false
}
