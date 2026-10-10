package relay

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func FuzzPairLifecycle(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3}, []byte("relay"))
	f.Add([]byte{9, 4, 5, 145}, []byte{0, 255})
	f.Fuzz(func(t *testing.T, operations, payload []byte) {
		if len(operations) == 0 {
			return
		}
		s := newLifecycleServer(t)
		listener := httptest.NewServer(s.http.Handler)
		defer listener.Close()
		defer closeLifecycleSockets(s)
		base := "ws" + strings.TrimPrefix(listener.URL, "http")
		key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
		for _, operation := range operations[:min(len(operations), 4)] {
			exerciseLifecycle(t, base, key, operation, payload[:min(len(payload), 256)])
			waitLifecycleIdle(t, s)
		}
	})
}

func newLifecycleServer(t *testing.T) *Server {
	t.Helper()
	cfg, err := LoadConfig(env(map[string]string{"RELAY_ADMISSION_RATE": "100000"}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard)
	return s
}

func closeLifecycleSockets(s *Server) {
	s.mu.Lock()
	connections := make([]*websocket.Conn, 0, len(s.conns))
	for connection := range s.conns {
		connections = append(connections, connection)
	}
	s.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func exerciseLifecycle(t *testing.T, base string, key ed25519.PrivateKey, operation byte, payload []byte) {
	t.Helper()
	scenario := operation % 6
	earlyMessage := operation&8 != 0
	if scenario == 5 {
		rejectLifecycleAuth(t, base, key)
		return
	}
	h, err := endpoint.Register(base, key)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	client, _, err := endpoint.Connect(base, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	event, err := h.Next(time.Second)
	if err != nil || event.Type != "incoming" {
		t.Fatalf("incoming event: %+v, %v", event, err)
	}
	if scenario == 0 {
		h.Close()
		expectLifecycleClose(t, client, websocket.CloseGoingAway)
		return
	}
	if scenario == 3 && !earlyMessage {
		replaceLifecycleHost(t, h, key, event, client)
		return
	}
	if scenario == 2 {
		expectLifecycleReject(t, base, h.ID, event.ConnectionID, event.Token+"x", http.StatusForbidden)
		expectLifecycleReject(t, base, h.ID+"x", event.ConnectionID, event.Token, http.StatusNotFound)
	}
	typ := websocket.BinaryMessage
	if operation&128 != 0 {
		typ = websocket.TextMessage
	}
	if scenario != 3 && scenario != 4 && earlyMessage {
		sendLifecycleMessage(t, client, typ, payload)
	}
	hostPeer, _, err := h.Accept(event)
	if err != nil {
		t.Fatal(err)
	}
	defer hostPeer.Close()
	if scenario == 3 {
		replaceLifecycleHost(t, h, key, event, client)
		expectLifecycleClose(t, hostPeer, websocket.CloseGoingAway)
		return
	}
	if scenario == 4 {
		interruptLifecycleMessage(t, client, hostPeer, operation)
		return
	}
	if scenario == 2 {
		expectLifecycleReject(t, base, h.ID, event.ConnectionID, event.Token, http.StatusForbidden)
	}
	if !earlyMessage {
		sendLifecycleMessage(t, client, typ, payload)
	}
	expectLifecycleMessage(t, hostPeer, typ, payload)
	sendLifecycleMessage(t, hostPeer, typ, payload)
	expectLifecycleMessage(t, client, typ, payload)
	if operation&16 == 0 {
		_ = client.Close()
		expectLifecycleClose(t, hostPeer, websocket.CloseGoingAway)
	} else {
		_ = hostPeer.Close()
		expectLifecycleClose(t, client, websocket.CloseGoingAway)
	}
}

func rejectLifecycleAuth(t *testing.T, base string, key ed25519.PrivateKey) {
	t.Helper()
	public := key.Public().(ed25519.PublicKey)
	ws, challenge, _, err := endpoint.DialControl(base, endpoint.B64(public))
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	signature := endpoint.SignChallenge(key, endpoint.EndpointID(public), challenge.Nonce+"x")
	if err := ws.WriteJSON(map[string]string{"type": "authenticate", "signature": signature}); err != nil {
		t.Fatal(err)
	}
	expectLifecycleClose(t, ws, websocket.ClosePolicyViolation)
}

func interruptLifecycleMessage(t *testing.T, client, hostPeer *websocket.Conn, operation byte) {
	t.Helper()
	_ = client.SetWriteDeadline(time.Now().Add(time.Second))
	writer, err := client.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte{operation}, 32<<10)); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	_ = hostPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, reader, err := hostPeer.NextReader(); err == nil {
		if _, err := io.Copy(io.Discard, reader); err == nil {
			t.Fatal("interrupted message became a completed message")
		}
	}
}

func sendLifecycleMessage(t *testing.T, ws *websocket.Conn, typ int, payload []byte) {
	t.Helper()
	_ = ws.SetWriteDeadline(time.Now().Add(time.Second))
	if err := ws.WriteMessage(typ, payload); err != nil {
		t.Fatal(err)
	}
}

func replaceLifecycleHost(t *testing.T, old *endpoint.Host, key ed25519.PrivateKey, event endpoint.Event, client *websocket.Conn) {
	t.Helper()
	fresh, err := endpoint.Register(old.Base, key)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	expectLifecycleClose(t, client, websocket.CloseGoingAway)
	expectLifecycleReject(t, old.Base, fresh.ID, event.ConnectionID, event.Token, http.StatusNotFound)
}

func expectLifecycleReject(t *testing.T, base, id, connectionID, token string, status int) {
	t.Helper()
	ws, response, err := endpoint.Accept(base, id, connectionID, token)
	if ws != nil {
		_ = ws.Close()
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != status {
		t.Fatalf("accept should reject with HTTP %d: response=%v, error=%v", status, response, err)
	}
}

func expectLifecycleMessage(t *testing.T, ws *websocket.Conn, typ int, payload []byte) {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	gotType, got, err := ws.ReadMessage()
	if err != nil || gotType != typ || !bytes.Equal(got, payload) {
		t.Fatalf("message changed: type=%d want=%d bytes=%d want=%d error=%v", gotType, typ, len(got), len(payload), err)
	}
}

func expectLifecycleClose(t *testing.T, ws *websocket.Conn, code int) {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := ws.ReadMessage()
	var closed *websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != code {
		t.Fatalf("close should use %d: %v", code, err)
	}
}

func waitLifecycleIdle(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		idle := len(s.hosts) == 0 && len(s.pairs) == 0 && len(s.conns) == 0 && s.controls == 0 && s.clientSlots == 0
		valid := s.active >= 0 && s.pending >= 0 && s.controls >= 0 && s.clientSlots >= s.active+s.pending && len(s.pairs) == s.active+s.pending
		s.mu.Unlock()
		if !valid {
			t.Fatal("pair counters disagree with retained state")
		}
		if total, ips := s.gate.Stats(); idle && total == 0 && ips == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lifecycle cleanup retained hosts, pairs, sockets, or admission slots")
		}
		time.Sleep(time.Millisecond)
	}
}
