package main

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/supabitapp/supacode-relay/internal/admission"
)

const (
	writeTimeout       = 5 * time.Second
	directoryReadLimit = 16 << 20
	keepAliveIdle      = 15 * time.Second
)

type config struct {
	addr           string
	privateAddr    string
	directoryToken string
	trustedProxies []netip.Prefix
	privatePeers   []netip.Prefix
	admissionRate  float64
	admissionBurst int
	maxConns       int
	maxConnsPerIP  int
	heartbeat      time.Duration
	refreshTimeout time.Duration
	acceptGrace    time.Duration
	dialTimeout    time.Duration
	headerTimeout  time.Duration
	drainTimeout   time.Duration
}

func loadConfig(getenv func(string) string) (config, error) {
	c := config{addr: "127.0.0.1:8080", privateAddr: "127.0.0.1:9090"}
	for _, a := range []struct {
		name string
		dst  *string
	}{{"ROUTER_ADDR", &c.addr}, {"ROUTER_PRIVATE_ADDR", &c.privateAddr}} {
		if v := getenv(a.name); v != "" {
			*a.dst = v
		}
		_, port, err := net.SplitHostPort(*a.dst)
		if err != nil {
			return c, fmt.Errorf("%s: %w", a.name, err)
		}
		if p, err := strconv.Atoi(port); err != nil || p < 0 || p > 65535 {
			return c, fmt.Errorf("%s: invalid port %q", a.name, port)
		}
	}
	if c.addr == c.privateAddr && !strings.HasSuffix(c.addr, ":0") {
		return c, fmt.Errorf("ROUTER_PRIVATE_ADDR: must differ from ROUTER_ADDR")
	}

	token := getenv("ROUTER_DIRECTORY_TOKEN")
	if token == "" {
		if path := getenv("ROUTER_DIRECTORY_TOKEN_FILE"); path != "" {
			b, err := os.ReadFile(path)
			if err != nil {
				return c, fmt.Errorf("ROUTER_DIRECTORY_TOKEN_FILE: %w", err)
			}
			token = strings.TrimSpace(string(b))
		}
	}
	if len(token) < 16 {
		return c, fmt.Errorf("ROUTER_DIRECTORY_TOKEN: at least 16 characters required")
	}
	c.directoryToken = token

	trusted, err := admission.ParsePrefixes(getenv("ROUTER_TRUSTED_PROXIES"))
	if err != nil {
		return c, fmt.Errorf("ROUTER_TRUSTED_PROXIES: %w", err)
	}
	c.trustedProxies = trusted
	peers, err := admission.ParsePrefixes(getenv("ROUTER_PRIVATE_ALLOWED_PEERS"))
	if err != nil {
		return c, fmt.Errorf("ROUTER_PRIVATE_ALLOWED_PEERS: %w", err)
	}
	c.privatePeers = peers

	c.admissionRate = 100
	if v := getenv("ROUTER_ADMISSION_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r <= 0 || r > 1e9 {
			return c, fmt.Errorf("ROUTER_ADMISSION_RATE: must be a positive number, got %q", v)
		}
		c.admissionRate = r
	}
	c.admissionBurst = max(1, int(c.admissionRate))

	ints := []struct {
		name string
		def  int
		dst  *int
	}{
		{"ROUTER_MAX_CONNS", 16384, &c.maxConns},
		{"ROUTER_MAX_CONNS_PER_IP", 512, &c.maxConnsPerIP},
	}
	for _, f := range ints {
		n, err := positiveInt(getenv, f.name, f.def)
		if err != nil {
			return c, err
		}
		*f.dst = n
	}
	durations := []struct {
		name string
		def  int
		dst  *time.Duration
	}{
		{"ROUTER_DIRECTORY_HEARTBEAT_MS", 1000, &c.heartbeat},
		{"ROUTER_REFRESH_TIMEOUT_MS", 500, &c.refreshTimeout},
		{"ROUTER_ACCEPT_GRACE_MS", 30000, &c.acceptGrace},
		{"ROUTER_UPSTREAM_DIAL_TIMEOUT_MS", 2000, &c.dialTimeout},
		{"ROUTER_UPSTREAM_HEADER_TIMEOUT_MS", 3000, &c.headerTimeout},
		{"ROUTER_DRAIN_TIMEOUT_MS", 5000, &c.drainTimeout},
	}
	for _, f := range durations {
		n, err := positiveInt(getenv, f.name, f.def)
		if err != nil {
			return c, err
		}
		*f.dst = time.Duration(n) * time.Millisecond
	}
	return c, nil
}

func positiveInt(getenv func(string) string, name string, def int) (int, error) {
	v := getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: must be a positive integer, got %q", name, v)
	}
	return n, nil
}
