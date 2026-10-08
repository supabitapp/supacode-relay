package relay

import (
	"os"
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
	if c.Addr != "127.0.0.1:8080" || c.MaxMessageBytes != (32<<20)-14 || c.MaxClients != 20000 || c.Heartbeat != 15*time.Second || c.AdmissionRate != 100 || c.admissionBurst() != 100 {
		t.Fatalf("unexpected defaults %+v", c)
	}
	if c.MaxQueueBytes != 1<<20 || c.MaxClientsPerHost != 256 || c.MaxPendingPerHost != 64 || c.WriteTimeout != 5*time.Second || c.DeliveryTimeout != 30*time.Second {
		t.Fatalf("unexpected queue, per-host or timeout defaults %+v", c)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cases := map[string]string{
		"RELAY_ADDR":                   "nonsense",
		"RELAY_MAX_MESSAGE_BYTES":      "abc",
		"RELAY_MAX_QUEUE_BYTES":        "0",
		"RELAY_MAX_QUEUE_MESSAGES":     "-1",
		"RELAY_MAX_CLIENTS":            "1.5",
		"RELAY_MAX_CLIENTS_PER_HOST":   "x",
		"RELAY_MAX_PENDING_PER_HOST":   "999999",
		"RELAY_MAX_HOSTS":              "0",
		"RELAY_AUTH_TIMEOUT_MS":        "0",
		"RELAY_PAIR_TIMEOUT_MS":        "soon",
		"RELAY_WRITE_TIMEOUT_MS":       "-5",
		"RELAY_DELIVERY_TIMEOUT_MS":    "0",
		"RELAY_HEARTBEAT_MS":           "1e3",
		"RELAY_ADMISSION_RATE":         "0",
		"RELAY_TRUSTED_PROXIES":        "10.0.0.0/99",
		"RELAY_ALLOWED_PEERS":          "nope",
		"RELAY_PRIVATE_ALLOWED_PEERS":  "10.0.0.0/33",
		"RELAY_PRIVATE_ADDR":           "9090",
		"RELAY_NODE_ID":                "Node_A",
		"RELAY_ROUTERS":                "ftp://router:9090",
		"RELAY_ADVERTISE_URL":          "https://node-a:8080/path",
		"RELAY_DIRECTORY_TOKEN_FILE":   "/nonexistent/token",
		"RELAY_DIRECTORY_HEARTBEAT_MS": "0",
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

func TestLoadConfigCluster(t *testing.T) {
	base := map[string]string{
		"RELAY_ROUTERS":         "https://relay-directory.int.exe.xyz, http://10.0.0.1:9090/",
		"RELAY_NODE_ID":         "node-a",
		"RELAY_DIRECTORY_TOKEN": "0123456789abcdef",
		"RELAY_ADVERTISE_URL":   "https://relay-node-a.int.exe.xyz/",
	}
	c, err := LoadConfig(env(base))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Clustered() || len(c.Routers) != 2 || c.Routers[0] != "wss://relay-directory.int.exe.xyz/v1/directory" || c.Routers[1] != "ws://10.0.0.1:9090/v1/directory" || c.AdvertiseURL != "https://relay-node-a.int.exe.xyz" {
		t.Fatalf("unexpected cluster config %+v", c)
	}
	for key, want := range map[string]string{"RELAY_NODE_ID": "RELAY_NODE_ID", "RELAY_DIRECTORY_TOKEN": "RELAY_DIRECTORY_TOKEN", "RELAY_ADVERTISE_URL": "RELAY_ADVERTISE_URL"} {
		kv := map[string]string{"RELAY_ADDR": "0.0.0.0:8080"}
		for k, v := range base {
			if k != key {
				kv[k] = v
			}
		}
		if _, err := LoadConfig(env(kv)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s: got %v", key, err)
		}
	}
	short := map[string]string{"RELAY_DIRECTORY_TOKEN": "short"}
	for k, v := range base {
		if _, ok := short[k]; !ok {
			short[k] = v
		}
	}
	if _, err := LoadConfig(env(short)); err == nil || !strings.Contains(err.Error(), "RELAY_DIRECTORY_TOKEN") {
		t.Errorf("short token accepted: %v", err)
	}
	file := t.TempDir() + "/token"
	if err := os.WriteFile(file, []byte("from-a-file-0123456789\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	withFile := map[string]string{"RELAY_DIRECTORY_TOKEN_FILE": file}
	for k, v := range base {
		if k != "RELAY_DIRECTORY_TOKEN" {
			withFile[k] = v
		}
	}
	if c, err := LoadConfig(env(withFile)); err != nil || c.DirectoryToken != "from-a-file-0123456789" {
		t.Errorf("token file: %q %v", c.DirectoryToken, err)
	}
}
