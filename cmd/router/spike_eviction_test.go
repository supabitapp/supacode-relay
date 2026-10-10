package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

func TestSpikeEvictionSnapshotBurst(t *testing.T) {
	cfg, err := loadConfig(func(k string) string {
		if k == "ROUTER_DIRECTORY_TOKEN" {
			return testToken
		}
		if k == "ROUTER_DIRECTORY_HEARTBEAT_MS" {
			return "10000"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := newRouter(cfg, io.Discard)
	const count = 20000
	newer, older := make([]directory.Registration, count), make([]directory.Registration, count)
	for i := range count {
		id := fmt.Sprintf("%064x", i+1)
		newer[i] = directory.Registration{EndpointID: id, RegistrationID: fmt.Sprint("new", i), Version: 2}
		older[i] = directory.Registration{EndpointID: id, RegistrationID: fmt.Sprint("old", i), Version: 1}
	}
	addNode(t, rt, "b", "http://newer", newer...)
	server := httptest.NewServer(rt.private())
	defer server.Close()
	d := websocket.Dialer{NetDial: func(network, addr string) (net.Conn, error) {
		c, e := net.Dial(network, addr)
		if e == nil {
			c.(*net.TCPConn).SetReadBuffer(1024)
		}
		return c, e
	}}
	ws, _, err := d.Dial("ws"+strings.TrimPrefix(server.URL, "http")+directory.Path, http.Header{"Authorization": {"Bearer " + testToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ws.SetReadLimit(16 << 20)
	if err := ws.WriteJSON(directory.Message{Type: directory.TypeHello, NodeID: "a", Addr: "http://older"}); err != nil {
		t.Fatal(err)
	}
	var welcome directory.Message
	if err := ws.ReadJSON(&welcome); err != nil {
		t.Fatal(err)
	}
	rt.mu.Lock()
	st := rt.streams["a"]
	rt.mu.Unlock()
	started := time.Now()
	ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := ws.WriteJSON(directory.Message{Type: directory.TypeSnapshot, Registrations: older}); err != nil {
		t.Fatal(err)
	}
	pausedUntil := time.NewTimer(200 * time.Millisecond)
	defer pausedUntil.Stop()
	closedBeforeRead := false
	select {
	case <-st.done:
		closedBeforeRead = true
	case <-pausedUntil.C:
	}
	received, frames := 0, 0
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	for received < count {
		var m directory.Message
		if err := ws.ReadJSON(&m); err != nil {
			break
		}
		if m.Type != directory.TypeEvict {
			continue
		}
		frames++
		if m.Registration != nil {
			received++
		} else {
			received += len(m.Registrations)
		}
	}
	candidate := os.Getenv("SPIKE_EVICTION_BATCH_CANDIDATE") == "1"
	t.Logf("candidate=%v snapshot=%d closed_before_receiver_resume=%v eviction_records_received=%d frames=%d elapsed_ms=%d", candidate, count, closedBeforeRead, received, frames, time.Since(started).Milliseconds())
	if candidate && (closedBeforeRead || received != count) {
		t.Fatal("chunked eviction delivery failed")
	}
	if !candidate && !closedBeforeRead {
		t.Fatal("baseline overflow not reproduced")
	}
}
