//go:build spikeinstrument

package relay

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func spikeDirectoryQueue(s *Server) *queue {
	q := newQueue(16<<20, 65536)
	s.dirStreams[&dirStream{q: q}] = struct{}{}
	return q
}

func spikeApply(t *testing.T, table *directory.Table, q *queue) {
	t.Helper()
	for {
		f, _, ok := q.next()
		if !ok {
			return
		}
		q.release(f)
		var m directory.Message
		if err := json.Unmarshal(f.data, &m); err != nil {
			t.Fatal(err)
		}
		switch m.Type {
		case directory.TypePut:
			table.Put("a", 1, *m.Registration)
		case directory.TypeDel:
			table.Del("a", 1, *m.Registration)
		}
	}
}

func TestSpikeRegistrationOrdering(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{"RELAY_NODE_ID": "a", "RELAY_ROUTERS": "http://127.0.0.1:1", "RELAY_DIRECTORY_TOKEN": "spike-directory-secret"}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard, "node", "a")
	q := spikeDirectoryQueue(s)
	returned := make(chan struct{}, 2)
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { returned <- struct{}{} }()
		s.http.Handler.ServeHTTP(w, r)
	}))
	defer listener.Close()
	base := "ws" + strings.TrimPrefix(listener.URL, "http")
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	hook := func(*host) {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	spikeBeforeRegistrationLock.Store(&hook)
	defer spikeBeforeRegistrationLock.Store(nil)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aReady := make(chan *endpoint.Host, 1)
	errs := make(chan error, 1)
	go func() {
		h, e := endpoint.Register(base, key)
		if e != nil {
			errs <- e
			return
		}
		aReady <- h
	}()
	<-entered
	b, err := endpoint.Register(base, key)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer b.Close()
	close(release)
	var a *endpoint.Host
	select {
	case a = <-aReady:
	case err := <-errs:
		t.Fatal(err)
	}
	<-b.Done
	table := directory.NewTable(0)
	table.Join("a", "http://node", 1, false)
	table.Snapshot("a", 1, nil)
	spikeApply(t, table, q)
	_, reg, _ := table.Lookup(a.ID)
	s.mu.Lock()
	live := s.hosts[a.ID]
	localID, localVersion := live.regID, live.version
	s.mu.Unlock()
	diverged := localID != reg.RegistrationID
	a.Close()
	<-returned
	<-returned
	spikeApply(t, table, q)
	ghosts := table.Len()
	candidate := os.Getenv("SPIKE_REGISTRATION_CANDIDATE") == "1"
	t.Logf("candidate=%v local_version=%d directory_version=%d diverged=%v ghost_entries_after_disconnect=%d", candidate, localVersion, reg.Version, diverged, ghosts)
	if candidate && (diverged || ghosts != 0) {
		t.Fatal("candidate failed registration agreement")
	}
	if !candidate && (!diverged || ghosts != 1) {
		t.Fatal("baseline ordering did not reproduce")
	}
}

func TestSpikeEvictionDeletion(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard, "node", "a")
	h := &host{id: strings.Repeat("a", 64), regID: "old", version: 1, ctrl: &peer{q: newQueue(1024, 16)}, pairs: map[string]*pair{}}
	s.hosts[h.id] = h
	s.cfg.NodeID = "a"
	q := spikeDirectoryQueue(s)
	table := directory.NewTable(0)
	table.Join("a", "http://node", 1, false)
	table.Snapshot("a", 1, []directory.Registration{*s.registration(h)})
	s.evict(directory.Message{Clock: 2, Registration: s.registration(h)})
	s.removeHost(h)
	spikeApply(t, table, q)
	candidate := os.Getenv("SPIKE_REGISTRATION_CANDIDATE") == "1"
	t.Logf("candidate=%v second_router_ghost_entries=%d", candidate, table.Len())
	if candidate && table.Len() != 0 {
		t.Fatal("candidate did not publish eviction deletion")
	}
	if !candidate && table.Len() != 1 {
		t.Fatal("baseline missing-deletion did not reproduce")
	}
}

func TestSpikeClockSkewSupersession(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unobserved", true: "observed"}[observed], func(t *testing.T) {
			baseTime := time.Now()
			servers := map[string]*Server{}
			queues := map[string]*queue{}
			urls := map[string]string{}
			for _, id := range []string{"a", "b"} {
				cfg, err := LoadConfig(env(map[string]string{"RELAY_NODE_ID": id, "RELAY_ROUTERS": "http://127.0.0.1:1", "RELAY_DIRECTORY_TOKEN": "spike-directory-secret"}))
				if err != nil {
					t.Fatal(err)
				}
				s := New(cfg)
				s.events = diagnostics.Logger(io.Discard, "node", id)
				clock := baseTime
				if id == "a" {
					clock = clock.Add(500 * time.Millisecond)
				}
				s.clock.Now = func() time.Time { return clock }
				queues[id] = spikeDirectoryQueue(s)
				servers[id] = s
				listener := httptest.NewServer(s.http.Handler)
				t.Cleanup(listener.Close)
				urls[id] = "ws" + strings.TrimPrefix(listener.URL, "http")
			}
			_, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			a, err := endpoint.Register(urls["a"], key)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			table := directory.NewTable(0)
			for _, id := range []string{"a", "b"} {
				table.Join(id, "http://"+id, 1, false)
				table.Snapshot(id, 1, nil)
			}
			readPut := func(id string) directory.Registration {
				f, _, ok := queues[id].next()
				if !ok {
					t.Fatal("missing publication")
				}
				var m directory.Message
				json.Unmarshal(f.data, &m)
				return *m.Registration
			}
			table.Put("a", 1, readPut("a"))
			if observed {
				servers["b"].clock.Observe(table.Clock())
			}
			b, err := endpoint.Register(urls["b"], key)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			evs, _ := table.Put("b", 1, readPut("b"))
			for _, ev := range evs {
				reg := ev.Registration
				servers[ev.NodeID].evict(directory.Message{Clock: ev.Winner, Registration: &reg})
			}
			_, winner, _ := table.Lookup(a.ID)
			code := 0
			if !observed {
				select {
				case <-b.Done:
				case <-time.After(time.Second):
					t.Fatal("missing false supersession")
				}
				var closeError *websocket.CloseError
				if errors.As(b.Err, &closeError) {
					code = closeError.Code
				}
			}
			t.Logf("observed_latest_clock=%v skew_ms=500 winner=%s newer_host_close_code=%d", observed, winner.NodeID, code)
			if observed && winner.NodeID != "b" {
				t.Fatal("observed clock did not order newer registration")
			}
			if !observed && (winner.NodeID != "a" || code != 4001) {
				t.Fatal("skew scenario did not reproduce")
			}
		})
	}
}
