package relay

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"
)

type memorySnapshot struct {
	HeapBytes       uint64
	HeapObjects     uint64
	Goroutines      int
	Hosts           int
	Pairs           int
	Sockets         int
	Controls        int
	ClientSlots     int
	ConnectionSlots int
}

func TestConnectionChurnMemory(t *testing.T) {
	setting := os.Getenv("RELAY_MEMORY_SPIKE_CYCLES")
	if setting == "" {
		t.Skip("set RELAY_MEMORY_SPIKE_CYCLES to run isolated memory measurements")
	}
	cycles, err := strconv.Atoi(setting)
	if err != nil || cycles < 1 || cycles > 10000 {
		t.Fatal("RELAY_MEMORY_SPIKE_CYCLES must be between 1 and 10000")
	}
	base := startMemoryRelay(t)
	socketBase := "ws" + strings.TrimPrefix(base, "http")
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	payload := bytes.Repeat([]byte("memory-churn"), 1024)
	churn := func(count int) {
		for i := range count {
			exerciseLifecycle(t, socketBase, key, byte(i%12), payload)
			waitMemoryResources(t, base)
		}
	}
	churn(24)
	baseline := readMemorySnapshot(t, base)
	t.Logf("baseline: %+v", baseline)
	profileDir := os.Getenv("RELAY_MEMORY_SPIKE_PROFILE_DIR")
	if profileDir != "" {
		saveMemoryProfile(t, base, profileDir, "baseline")
		t.Cleanup(func() { saveMemoryProfile(t, base, profileDir, "final") })
	}
	for batch := 1; batch <= 6; batch++ {
		churn(cycles)
		sample := readMemorySnapshot(t, base)
		t.Logf("batch=%d cycles=%d heap_delta=%d objects_delta=%d goroutines_delta=%d snapshot=%+v", batch, batch*cycles,
			int64(sample.HeapBytes)-int64(baseline.HeapBytes), int64(sample.HeapObjects)-int64(baseline.HeapObjects), sample.Goroutines-baseline.Goroutines, sample)
		if sample.HeapBytes > baseline.HeapBytes+(2<<20) {
			t.Fatal("post-GC relay heap exceeded the 2 MiB retention budget")
		}
		if sample.Goroutines > baseline.Goroutines+4 {
			t.Fatal("relay retained more than four additional goroutines after cleanup")
		}
	}
}

func TestMemoryRelayProcess(t *testing.T) {
	if os.Getenv("SUPACODE_RELAY_MEMORY_CHILD") != "1" {
		t.Skip("isolated relay process for the memory measurements")
	}
	if os.Getenv("RELAY_MEMORY_SPIKE_PROFILE_DIR") != "" {
		runtime.MemProfileRate = 1
	}
	s := newLifecycleServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /__memory", func(w http.ResponseWriter, _ *http.Request) {
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		sample := memorySnapshot{HeapBytes: stats.HeapAlloc, HeapObjects: stats.HeapObjects, Goroutines: runtime.NumGoroutine()}
		s.mu.Lock()
		sample.Hosts, sample.Pairs, sample.Sockets = len(s.hosts), len(s.pairs), len(s.conns)
		sample.Controls, sample.ClientSlots = s.controls, s.clientSlots
		s.mu.Unlock()
		sample.ConnectionSlots, _ = s.gate.Stats()
		writeJSON(w, http.StatusOK, sample)
	})
	if os.Getenv("RELAY_MEMORY_SPIKE_PROFILE_DIR") != "" {
		mux.HandleFunc("GET /__heap", func(w http.ResponseWriter, _ *http.Request) {
			if err := pprof.WriteHeapProfile(w); err != nil {
				t.Error(err)
			}
		})
	}
	mux.Handle("/", s.http.Handler)
	listener := httptest.NewServer(mux)
	defer listener.Close()
	defer closeLifecycleSockets(s)
	fmt.Fprintln(os.Stdout, listener.URL)
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func saveMemoryProfile(t *testing.T, base, dir, name string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	client := http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(base + "/__heap")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	profile, err := os.Create(filepath.Join(dir, name+".pprof"))
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(profile, response.Body)
	closeErr := profile.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("save heap profile: copy=%v close=%v", copyErr, closeErr)
	}
}

func startMemoryRelay(t *testing.T) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestMemoryRelayProcess$", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), "SUPACODE_RELAY_MEMORY_CHILD=1", "GORACE=atexit_sleep_ms=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = stdin.Close()
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("memory relay failed: %v\n%s", err, stderr.String())
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
			t.Error("memory relay did not exit")
		}
	})
	started := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			started <- scanner.Text()
		}
		_, _ = io.Copy(io.Discard, stdout)
	}()
	select {
	case base := <-started:
		if !strings.HasPrefix(base, "http://127.0.0.1:") {
			t.Fatalf("unexpected memory relay startup: %q", base)
		}
		return base
	case <-time.After(5 * time.Second):
		t.Fatal("memory relay did not report its address")
		return ""
	}
}

func waitMemoryResources(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	client := http.Client{Timeout: time.Second}
	for {
		response, err := client.Get(base + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		var metrics map[string]json.RawMessage
		err = json.NewDecoder(response.Body).Decode(&metrics)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		idle := true
		for _, name := range []string{"activeHosts", "activePairs", "pendingPairs", "clientSlots", "closingPairs", "controlConnections", "openSockets", "openConnections"} {
			value, ok := metrics[name]
			var count int
			if !ok || json.Unmarshal(value, &count) != nil {
				t.Fatalf("resource metric %q is missing or not an integer", name)
			}
			idle = idle && count == 0
		}
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay retained resources after churn: %v", metrics)
		}
		time.Sleep(time.Millisecond)
	}
}

func readMemorySnapshot(t *testing.T, base string) memorySnapshot {
	t.Helper()
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(base + "/__memory")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var sample memorySnapshot
	if err := json.NewDecoder(response.Body).Decode(&sample); err != nil {
		t.Fatal(err)
	}
	if sample.Hosts != 0 || sample.Pairs != 0 || sample.Sockets != 0 || sample.Controls != 0 || sample.ClientSlots != 0 || sample.ConnectionSlots != 0 {
		t.Fatalf("memory sample taken before resources were released: %+v", sample)
	}
	return sample
}
