//go:build dockere2e

package docker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const probeIP = "10.231.2.50"

var st *stack

type stack struct {
	file    string
	project string
	port    string
	base    string
	http    string

	mu      sync.Mutex
	env     map[string]string
	secrets map[string]struct{}
	report  map[string]any
	logs    strings.Builder
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	file, err := filepath.Abs("compose.yaml")
	if err != nil {
		panic(err)
	}
	port := getenv("E2E_ROUTER_PORT", "18480")
	st = &stack{
		file:    file,
		project: getenv("E2E_PROJECT", "supacode-relay-e2e"),
		port:    port,
		base:    "ws://127.0.0.1:" + port,
		http:    "http://127.0.0.1:" + port,
		env:     map[string]string{},
		secrets: map[string]struct{}{},
		report:  map[string]any{},
	}
	if os.Getenv("E2E_SKIP_BUILD") == "" {
		if out, err := st.compose("--profile", "tools", "build"); err != nil {
			fmt.Fprintln(os.Stderr, out)
			os.Exit(1)
		}
	}
	_, _ = st.compose("--profile", "extra", "--profile", "tools", "down", "-v", "--remove-orphans")
	if out, err := st.compose("up", "-d", "--wait", "router", "node-a", "node-b", "node-c"); err != nil {
		fmt.Fprintln(os.Stderr, out)
		os.Exit(1)
	}
	code := m.Run()
	if code == 0 {
		code = st.checkLogs()
	}
	st.writeReport()
	if os.Getenv("E2E_KEEP") == "" {
		_, _ = st.compose("--profile", "extra", "--profile", "tools", "down", "-v", "--remove-orphans")
	}
	os.Exit(code)
}

func (s *stack) compose(args ...string) (string, error) {
	cmd := exec.Command("docker", append([]string{"compose", "-p", s.project, "-f", s.file}, args...)...)
	cmd.Env = os.Environ()
	s.mu.Lock()
	for k, v := range s.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	s.mu.Unlock()
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func (s *stack) must(t *testing.T, args ...string) string {
	t.Helper()
	out, err := s.compose(args...)
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
	return out
}

func (s *stack) docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func (s *stack) container(t *testing.T, service string) string {
	t.Helper()
	return strings.TrimSpace(s.must(t, "--profile", "extra", "ps", "-aq", service))
}

func (s *stack) setEnv(kv map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range kv {
		if v == "" {
			delete(s.env, k)
			continue
		}
		s.env[k] = v
	}
}

func (s *stack) baseline(t *testing.T) {
	t.Helper()
	if err := s.collectLogs(); err != nil {
		t.Fatal(err)
	}
	for _, svc := range []string{"node-a", "node-b", "node-c", "router"} {
		_, _ = s.compose("unpause", svc)
	}
	_, _ = s.compose("--profile", "extra", "rm", "-sf", "node-d")
	s.must(t, "up", "-d", "--wait", "router", "node-a", "node-b", "node-c")
	s.waitRouter(t, "baseline with three ready nodes", 30*time.Second, func(m routerMetrics) bool {
		return m.readyNodes() == 3 && len(m.Nodes) == 3
	})
}

type routerNode struct {
	ID             string `json:"id"`
	Ready          bool   `json:"ready"`
	Draining       bool   `json:"draining"`
	Endpoints      int    `json:"endpoints"`
	ControlSockets int    `json:"controlSockets"`
}

type routerMetrics struct {
	Nodes               []routerNode     `json:"nodes"`
	Endpoints           int              `json:"endpoints"`
	DirectoryStreams    int              `json:"directoryStreams"`
	Requests            map[string]int64 `json:"requests"`
	Retries             map[string]int64 `json:"retries"`
	Misses              map[string]int64 `json:"misses"`
	Refreshes           int64            `json:"refreshes"`
	RefreshRounds       int64            `json:"refreshRounds"`
	EvictionsSent       int64            `json:"evictionsSent"`
	UpstreamErrors      int64            `json:"upstreamErrors"`
	RejectedRateLimited int64            `json:"rejectedRateLimited"`
	Goroutines          int              `json:"goroutines"`
}

func (m routerMetrics) readyNodes() int {
	n := 0
	for _, node := range m.Nodes {
		if node.Ready && !node.Draining {
			n++
		}
	}
	return n
}

func (m routerMetrics) node(id string) (routerNode, bool) {
	for _, n := range m.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return routerNode{}, false
}

func (s *stack) routerMetrics(t *testing.T) (routerMetrics, error) {
	t.Helper()
	out, err := s.compose("exec", "-T", "router", "/relay-router", "metrics")
	if err != nil {
		return routerMetrics{}, fmt.Errorf("%v: %s", err, out)
	}
	var m routerMetrics
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		return routerMetrics{}, fmt.Errorf("%v: %s", err, out)
	}
	return m, nil
}

func (s *stack) waitRouter(t *testing.T, desc string, timeout time.Duration, pred func(routerMetrics) bool) routerMetrics {
	t.Helper()
	end := time.Now().Add(timeout)
	var last routerMetrics
	var lastErr error
	for time.Now().Before(end) {
		m, err := s.routerMetrics(t)
		if err == nil && pred(m) {
			return m
		}
		last, lastErr = m, err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %+v %v", desc, last, lastErr)
	return last
}

func (s *stack) nodeMetrics(t *testing.T, node string) map[string]float64 {
	t.Helper()
	out := s.must(t, "--profile", "extra", "exec", "-T", node, "/relay", "metrics")
	raw := map[string]any{}
	if err := json.Unmarshal([]byte(out), &raw); err != nil {
		t.Fatalf("%s metrics: %v %s", node, err, out)
	}
	m := map[string]float64{}
	for k, v := range raw {
		if f, ok := v.(float64); ok {
			m[k] = f
		}
	}
	return m
}

func (s *stack) waitNode(t *testing.T, node, desc string, timeout time.Duration, pred func(map[string]float64) bool) map[string]float64 {
	t.Helper()
	end := time.Now().Add(timeout)
	var m map[string]float64
	for time.Now().Before(end) {
		m = s.nodeMetrics(t, node)
		if pred(m) {
			return m
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s on %s: %v", desc, node, m)
	return m
}

type probeResult struct {
	Status int    `json:"status"`
	Error  string `json:"error"`
	Codes  []int  `json:"codes"`
}

func (s *stack) probe(t *testing.T, args ...string) probeResult {
	t.Helper()
	out := s.must(t, append([]string{"--profile", "tools", "run", "--rm", "-T", "probe"}, args...)...)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var r probeResult
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &r); err != nil {
		t.Fatalf("probe output %q: %v", out, err)
	}
	return r
}

func (s *stack) remember(values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range values {
		if len(v) >= 16 {
			s.secrets[v] = struct{}{}
		}
	}
}

func (s *stack) record(key string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.report[key] = v
}

func (s *stack) writeReport() {
	dir := getenv("E2E_OUT", filepath.Join(os.TempDir(), "supacode-relay-docker-e2e"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	s.mu.Lock()
	b, _ := json.MarshalIndent(s.report, "", "  ")
	s.mu.Unlock()
	path := filepath.Join(dir, "report.json")
	if os.WriteFile(path, b, 0o644) == nil {
		fmt.Fprintln(os.Stderr, "docker e2e report:", path)
	}
}

func (s *stack) collectLogs() error {
	out, err := s.compose("--profile", "extra", "--profile", "tools", "logs", "--no-color", "--no-log-prefix")
	if err != nil {
		return fmt.Errorf("collect logs: %v: %s", err, out)
	}
	s.mu.Lock()
	s.logs.WriteString(out)
	s.mu.Unlock()
	return nil
}

func (s *stack) checkLogs() int {
	if err := s.collectLogs(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	s.mu.Lock()
	out := s.logs.String()
	secrets := make([]string, 0, len(s.secrets))
	for v := range s.secrets {
		secrets = append(secrets, v)
	}
	s.mu.Unlock()
	sort.Strings(secrets)
	start := time.Now()
	var leaks []string
	for _, v := range secrets {
		if strings.Contains(out, v) {
			leaks = append(leaks, v)
		}
	}
	for _, bad := range []string{"panic:", "DATA RACE", "fatal error:"} {
		if strings.Contains(out, bad) {
			leaks = append(leaks, bad)
		}
	}
	s.record("logHygiene", map[string]any{"identifiersChecked": len(secrets), "logBytes": len(out), "leaks": len(leaks), "scanMillis": time.Since(start).Milliseconds()})
	if len(leaks) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: %d identifiers or failures found in container logs, first: %q\n", len(leaks), leaks[0])
		return 1
	}
	fmt.Fprintf(os.Stderr, "log hygiene: %d identifiers absent from %d bytes of container logs\n", len(secrets), len(out))
	return 0
}
