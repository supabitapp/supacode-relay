package main

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

type config struct {
	addr              string
	maxMessageBytes   int
	maxQueueBytes     int
	maxQueueMessages  int
	maxClients        int
	maxClientsPerHost int
	maxPendingPerHost int
	maxHosts          int
	authTimeout       time.Duration
	pairTimeout       time.Duration
	writeTimeout      time.Duration
	heartbeat         time.Duration
	admissionRate     float64
	admissionBurst    int
	trustedProxies    []netip.Prefix
}

func (c config) pongTimeout() time.Duration {
	return 2 * c.heartbeat
}

func loadConfig(getenv func(string) string) (config, error) {
	c := config{addr: "127.0.0.1:8080"}
	if v := getenv("RELAY_ADDR"); v != "" {
		c.addr = v
	}
	if _, port, err := net.SplitHostPort(c.addr); err != nil {
		return c, fmt.Errorf("RELAY_ADDR: %w", err)
	} else if p, err := strconv.Atoi(port); err != nil || p < 0 || p > 65535 {
		return c, fmt.Errorf("RELAY_ADDR: invalid port %q", port)
	}

	ints := []struct {
		name string
		def  int
		dst  *int
	}{
		{"RELAY_MAX_MESSAGE_BYTES", 1 << 20, &c.maxMessageBytes},
		{"RELAY_MAX_QUEUE_BYTES", 4 << 20, &c.maxQueueBytes},
		{"RELAY_MAX_QUEUE_MESSAGES", 256, &c.maxQueueMessages},
		{"RELAY_MAX_CLIENTS", 1024, &c.maxClients},
		{"RELAY_MAX_CLIENTS_PER_HOST", 128, &c.maxClientsPerHost},
		{"RELAY_MAX_PENDING_PER_HOST", 32, &c.maxPendingPerHost},
		{"RELAY_MAX_HOSTS", 1024, &c.maxHosts},
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
		{"RELAY_AUTH_TIMEOUT_MS", 5000, &c.authTimeout},
		{"RELAY_PAIR_TIMEOUT_MS", 5000, &c.pairTimeout},
		{"RELAY_WRITE_TIMEOUT_MS", 5000, &c.writeTimeout},
		{"RELAY_HEARTBEAT_MS", 15000, &c.heartbeat},
	}
	for _, f := range durations {
		n, err := positiveInt(getenv, f.name, f.def)
		if err != nil {
			return c, err
		}
		*f.dst = time.Duration(n) * time.Millisecond
	}

	c.admissionRate = 100
	if v := getenv("RELAY_ADMISSION_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r <= 0 || r > 1e9 {
			return c, fmt.Errorf("RELAY_ADMISSION_RATE: must be a positive number, got %q", v)
		}
		c.admissionRate = r
	}
	c.admissionBurst = max(1, int(c.admissionRate))

	if v := getenv("RELAY_TRUSTED_PROXIES"); v != "" {
		for _, s := range strings.Split(v, ",") {
			p, err := netip.ParsePrefix(strings.TrimSpace(s))
			if err != nil {
				return c, fmt.Errorf("RELAY_TRUSTED_PROXIES: %w", err)
			}
			c.trustedProxies = append(c.trustedProxies, p.Masked())
		}
	}

	if c.maxMessageBytes > c.maxQueueBytes {
		return c, fmt.Errorf("RELAY_MAX_MESSAGE_BYTES (%d) must not exceed RELAY_MAX_QUEUE_BYTES (%d)", c.maxMessageBytes, c.maxQueueBytes)
	}
	if c.maxPendingPerHost > c.maxClientsPerHost {
		return c, fmt.Errorf("RELAY_MAX_PENDING_PER_HOST (%d) must not exceed RELAY_MAX_CLIENTS_PER_HOST (%d)", c.maxPendingPerHost, c.maxClientsPerHost)
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
