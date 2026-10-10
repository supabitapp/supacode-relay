package relay

import (
	"strings"
	"testing"
	"time"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	c, err := LoadConfig(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != "127.0.0.1:8080" || c.MaxMessageBytes != (32<<20)-14 || c.MaxClients != 20000 || c.Heartbeat != 15*time.Second || c.AdmissionRate != 100 || c.admissionBurst() != 100 || c.MaxConnections != 16384 || c.MaxConnectionsPerIP != 512 {
		t.Fatalf("unexpected defaults %+v", c)
	}
	if c.MaxQueueBytes != 1<<20 || c.MaxClientsPerHost != 64 || c.MaxPendingPerHost != 64 || c.WriteTimeout != 5*time.Second || c.DeliveryTimeout != 30*time.Second {
		t.Fatalf("unexpected queue, per-host or timeout defaults %+v", c)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cases := map[string]string{
		"RELAY_ADDR":                  "nonsense",
		"RELAY_MAX_MESSAGE_BYTES":     "abc",
		"RELAY_MAX_QUEUE_BYTES":       "0",
		"RELAY_MAX_QUEUE_MESSAGES":    "-1",
		"RELAY_MAX_CLIENTS":           "1.5",
		"RELAY_MAX_CLIENTS_PER_HOST":  "x",
		"RELAY_MAX_PENDING_PER_HOST":  "999999",
		"RELAY_MAX_CONNS":             "0",
		"RELAY_MAX_CONNS_PER_IP":      "-1",
		"RELAY_MAX_HOSTS":             "0",
		"RELAY_AUTH_TIMEOUT_MS":       "0",
		"RELAY_PAIR_TIMEOUT_MS":       "soon",
		"RELAY_WRITE_TIMEOUT_MS":      "-5",
		"RELAY_DELIVERY_TIMEOUT_MS":   "0",
		"RELAY_HEARTBEAT_MS":          "1e3",
		"RELAY_ADMISSION_RATE":        "0",
		"RELAY_TRUSTED_PROXIES":       "10.0.0.0/99",
		"RELAY_ALLOWED_PEERS":         "nope",
		"RELAY_PRIVATE_ALLOWED_PEERS": "10.0.0.0/33",
		"RELAY_PRIVATE_ADDR":          "9090",
	}
	for name, value := range cases {
		_, err := LoadConfig(env(map[string]string{name: value}))
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s=%q: got %v", name, value, err)
		}
	}
	if _, err := LoadConfig(env(map[string]string{"RELAY_MAX_MESSAGE_BYTES": "9000000", "RELAY_MAX_QUEUE_BYTES": "8192"})); err != nil {
		t.Errorf("message larger than queue rejected: %v", err)
	}
}
