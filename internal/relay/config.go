package relay

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr               string
	MaxMessageBytes    int
	IngressBudgetBytes int
	IngressWeight      int
	MaxQueueBytes      int
	MaxQueueMessages   int
	MaxClients         int
	MaxClientsPerHost  int
	MaxPendingPerHost  int
	MaxHosts           int
	AuthTimeout        time.Duration
	PairTimeout        time.Duration
	WriteTimeout       time.Duration
	Heartbeat          time.Duration
	AdmissionRate      float64
	TrustedProxies     []netip.Prefix
}

func (c Config) pongTimeout() time.Duration {
	return 2 * c.Heartbeat
}

func (c Config) admissionBurst() int {
	return max(1, int(c.AdmissionRate))
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{Addr: "127.0.0.1:8080", AdmissionRate: 100}
	if v := getenv("RELAY_ADDR"); v != "" {
		c.Addr = v
	}
	if _, port, err := net.SplitHostPort(c.Addr); err != nil {
		return c, fmt.Errorf("RELAY_ADDR: %w", err)
	} else if p, err := strconv.Atoi(port); err != nil || p < 0 || p > 65535 {
		return c, fmt.Errorf("RELAY_ADDR: invalid port %q", port)
	}

	ints := []struct {
		name string
		def  int
		dst  *int
	}{
		{"RELAY_MAX_MESSAGE_BYTES", (32 << 20) - 14, &c.MaxMessageBytes},
		{"RELAY_MAX_QUEUE_BYTES", 64 << 20, &c.MaxQueueBytes},
		{"RELAY_MAX_QUEUE_MESSAGES", 256, &c.MaxQueueMessages},
		{"RELAY_MAX_CLIENTS", 20000, &c.MaxClients},
		{"RELAY_MAX_CLIENTS_PER_HOST", 20000, &c.MaxClientsPerHost},
		{"RELAY_MAX_PENDING_PER_HOST", 20000, &c.MaxPendingPerHost},
		{"RELAY_MAX_HOSTS", 20000, &c.MaxHosts},
		{"RELAY_INGRESS_BUDGET_BYTES", 512 << 20, &c.IngressBudgetBytes},
		{"RELAY_INGRESS_WEIGHT", 4, &c.IngressWeight},
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
		{"RELAY_AUTH_TIMEOUT_MS", 5000, &c.AuthTimeout},
		{"RELAY_PAIR_TIMEOUT_MS", 5000, &c.PairTimeout},
		{"RELAY_WRITE_TIMEOUT_MS", 5000, &c.WriteTimeout},
		{"RELAY_HEARTBEAT_MS", 15000, &c.Heartbeat},
	}
	for _, f := range durations {
		n, err := positiveInt(getenv, f.name, f.def)
		if err != nil {
			return c, err
		}
		*f.dst = time.Duration(n) * time.Millisecond
	}

	if v := getenv("RELAY_ADMISSION_RATE"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || r <= 0 || r > 1e9 {
			return c, fmt.Errorf("RELAY_ADMISSION_RATE: must be a positive number, got %q", v)
		}
		c.AdmissionRate = r
	}

	if v := getenv("RELAY_TRUSTED_PROXIES"); v != "" {
		for _, s := range strings.Split(v, ",") {
			p, err := netip.ParsePrefix(strings.TrimSpace(s))
			if err != nil {
				return c, fmt.Errorf("RELAY_TRUSTED_PROXIES: %w", err)
			}
			c.TrustedProxies = append(c.TrustedProxies, p.Masked())
		}
	}

	if c.MaxMessageBytes > c.IngressBudgetBytes/c.IngressWeight {
		return c, fmt.Errorf("RELAY_INGRESS_BUDGET_BYTES must admit RELAY_MAX_MESSAGE_BYTES at RELAY_INGRESS_WEIGHT")
	}
	if c.MaxMessageBytes > c.MaxQueueBytes {
		return c, fmt.Errorf("RELAY_MAX_MESSAGE_BYTES (%d) must not exceed RELAY_MAX_QUEUE_BYTES (%d)", c.MaxMessageBytes, c.MaxQueueBytes)
	}
	if c.MaxPendingPerHost > c.MaxClientsPerHost {
		return c, fmt.Errorf("RELAY_MAX_PENDING_PER_HOST (%d) must not exceed RELAY_MAX_CLIENTS_PER_HOST (%d)", c.MaxPendingPerHost, c.MaxClientsPerHost)
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
