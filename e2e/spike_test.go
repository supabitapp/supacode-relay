package e2e

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func spikeLog(t *testing.T, r *relay, needle string) {
	t.Helper()
	r.stderr.mu.Lock()
	if r.stderr.changed == nil {
		r.stderr.changed = make(chan struct{}, 1)
	}
	notify := r.stderr.changed
	r.stderr.mu.Unlock()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		if strings.Contains(r.stderr.String(), needle) {
			return
		}
		select {
		case <-notify:
		case <-timer.C:
			t.Fatalf("missing log %q", needle)
		}
	}
}

func TestSpikeDirectoryOnlyLoss(t *testing.T) {
	c := startCluster(t, nil)
	target, _ := url.Parse("http://" + c.router.privAddr)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var blocked atomic.Bool
	var mu sync.Mutex
	var streams []net.Conn
	fault := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			w.WriteHeader(503)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	fault.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			mu.Lock()
			streams = append(streams, c)
			mu.Unlock()
		}
	}
	fault.Start()
	defer fault.Close()
	a := c.addNode("a", "RELAY_ROUTERS="+fault.URL)
	c.addNode("b")
	c.waitReady(2)
	key := newKey(t)
	h := serveEcho(t, a.base, key, "host")
	active, res, err := endpoint.Connect(c.router.base, h.ID)
	if err != nil {
		t.Fatalf("initial connect: %v %s", err, status(res))
	}
	defer active.Close()
	if err := exchange(active, "host", "before", 10); err != nil {
		t.Fatal(err)
	}
	blocked.Store(true)
	started := time.Now()
	mu.Lock()
	for _, s := range streams {
		s.Close()
	}
	mu.Unlock()
	spikeLog(t, c.router, "directory.node.left")
	candidate := os.Getenv("SPIKE_DIRECTORY_CANDIDATE") == "1"
	if candidate {
		select {
		case <-h.Done:
		case <-time.After(2 * time.Second):
			t.Fatal("self-fence did not release control")
		}
		replacement := serveEcho(t, c.router.base, key, "replacement")
		ws, res, err := endpoint.Connect(c.router.base, replacement.ID)
		if err != nil {
			t.Fatalf("replacement: %v %s", err, status(res))
		}
		defer ws.Close()
		if err := exchange(ws, "replacement", "recovered", 10); err != nil {
			t.Fatal(err)
		}
		if err := exchange(active, "host", "survives", 10); err != nil {
			t.Fatal(err)
		}
		t.Logf("candidate=true recovery_ms=%d replacement_node=%s old_pair_echoes=10 new_pair_echoes=10", time.Since(started).Milliseconds(), replacement.node(t, c.router.base))
	} else {
		misses := 0
		for range 5 {
			ws, res, err := endpoint.Connect(c.router.base, h.ID)
			if ws != nil {
				ws.Close()
			}
			if err == nil || res.StatusCode != 404 {
				t.Fatalf("expected missing endpoint: %v %s", err, status(res))
			}
			misses++
			if err := exchange(active, "host", fmt.Sprint(misses), 10); err != nil {
				t.Fatal(err)
			}
			select {
			case <-h.Done:
				t.Fatalf("control closed: %v", h.Err)
			case <-time.After(600 * time.Millisecond):
			}
		}
		health, _ := a.get("/healthz")
		t.Logf("candidate=false partition_ms=%d connect_404=%d active_pair_echoes=50 host_controls=%v health_status=%d", time.Since(started).Milliseconds(), misses, a.metrics()["activeHosts"], health)
	}
	blocked.Store(false)
	spikeLog(t, a, "directory.snapshot.queued")
}

func TestSpikeOneOfTwoDirectoryLinks(t *testing.T) {
	c := startCluster(t, nil)
	second := startBinary(t, routerBin, []string{"ROUTER_ADDR=127.0.0.1:0", "ROUTER_PRIVATE_ADDR=127.0.0.1:0", "ROUTER_DIRECTORY_TOKEN=" + directoryToken, "ROUTER_ADMISSION_RATE=100000", "ROUTER_DIRECTORY_HEARTBEAT_MS=200"})
	other := &cluster{t: t, router: second}
	target, _ := url.Parse("http://" + c.router.privAddr)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var blocked atomic.Bool
	var mu sync.Mutex
	var streams []net.Conn
	fault := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			w.WriteHeader(503)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	fault.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			mu.Lock()
			streams = append(streams, conn)
			mu.Unlock()
		}
	}
	fault.Start()
	defer fault.Close()
	a := c.addNode("a", "RELAY_ROUTERS="+fault.URL+",http://"+second.privAddr)
	c.waitReady(1)
	other.waitReady(1)
	h := serveEcho(t, a.base, newKey(t), "two-links")
	initial, _, err := endpoint.Connect(c.router.base, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer initial.Close()
	if err := exchange(initial, "two-links", "before", 10); err != nil {
		t.Fatal(err)
	}
	blocked.Store(true)
	mu.Lock()
	for _, stream := range streams {
		stream.Close()
	}
	mu.Unlock()
	spikeLog(t, c.router, "directory.node.left")
	ws, res, err := endpoint.Connect(c.router.base, h.ID)
	if ws != nil {
		ws.Close()
	}
	if err == nil || res.StatusCode != 404 {
		t.Fatal("missing-router lookup did not fail")
	}
	working, res, err := endpoint.Connect(second.base, h.ID)
	if err != nil {
		t.Fatalf("surviving router failed: %v %s", err, status(res))
	}
	defer working.Close()
	if err := exchange(working, "two-links", "other-router", 20); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.Done:
		t.Fatalf("one missing stream wrongly fenced host: %v", h.Err)
	case <-time.After(600 * time.Millisecond):
	}
	t.Logf("usable_directory_streams=%v failed_router_status=404 surviving_router_echoes=20 host_preserved=true", a.metrics()["directoryStreams"])
}
