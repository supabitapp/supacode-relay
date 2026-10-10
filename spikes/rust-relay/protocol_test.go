package rustspike

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func relayURL(t *testing.T) string {
	t.Helper()
	base := os.Getenv("RELAY_SPIKE_URL")
	if base == "" {
		t.Skip("set RELAY_SPIKE_URL to a running loopback relay")
	}
	return base
}

func register(t *testing.T, base string) *endpoint.Host {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	host, err := endpoint.Register(base, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(host.Close)
	return host
}

func pair(t *testing.T, host *endpoint.Host) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	client, _, err := endpoint.Connect(host.Base, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	incoming, err := host.Next(5 * time.Second)
	if err != nil || incoming.Type != "incoming" {
		t.Fatalf("expected incoming: %v %v", incoming, err)
	}
	accepted, _, err := host.Accept(incoming)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { accepted.Close() })
	return client, accepted
}

func TestMessageTypesOrderingAndEarlyData(t *testing.T) {
	host := register(t, relayURL(t))
	client, _, err := endpoint.Connect(host.Base, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.WriteMessage(websocket.TextMessage, []byte("early data")); err != nil {
		t.Fatal(err)
	}
	incoming, err := host.Next(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	accepted, _, err := host.Accept(incoming)
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	accepted.SetReadDeadline(time.Now().Add(5 * time.Second))
	typ, data, err := accepted.ReadMessage()
	if err != nil || typ != websocket.TextMessage || string(data) != "early data" {
		t.Fatalf("early data: type=%d len=%d err=%v", typ, len(data), err)
	}
	for _, source := range []*websocket.Conn{client, accepted} {
		target := accepted
		if source == accepted {
			target = client
		}
		for _, size := range []int{64, 1024, 65536, 1 << 20} {
			for _, typ := range []int{websocket.TextMessage, websocket.BinaryMessage} {
				payload := bytes.Repeat([]byte("a"), size)
				source.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if err := source.WriteMessage(typ, payload); err != nil {
					t.Fatal(err)
				}
				target.SetReadDeadline(time.Now().Add(5 * time.Second))
				gotType, got, err := target.ReadMessage()
				if err != nil || gotType != typ || !bytes.Equal(got, payload) {
					t.Fatalf("forwarding %d bytes type %d: got type=%d len=%d err=%v", size, typ, gotType, len(got), err)
				}
			}
		}
	}
}

func TestRejectsBadAuthentication(t *testing.T) {
	base := relayURL(t)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ws, _, _, err := endpoint.DialControl(base, endpoint.B64(public))
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.WriteJSON(map[string]string{"type": "authenticate", "signature": endpoint.B64(make([]byte, 64))}); err != nil {
		t.Fatal(err)
	}
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := ws.ReadMessage(); !websocket.IsCloseError(err, 1008) {
		t.Fatalf("expected authentication rejection: %v", err)
	}
}

func TestAcceptTokenCannotBeReusedOrGuessed(t *testing.T) {
	host := register(t, relayURL(t))
	client, _, err := endpoint.Connect(host.Base, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	incoming, err := host.Next(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, response, err := endpoint.Accept(host.Base, host.ID, incoming.ConnectionID, "wrong-token")
	if err == nil || response == nil || response.StatusCode != 403 {
		t.Fatalf("wrong token: response=%v err=%v", response, err)
	}
	accepted, _, err := host.Accept(incoming)
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	if replay, _, err := host.Accept(incoming); err == nil {
		replay.Close()
		t.Fatal("accept token was reusable")
	}
}

func TestNewRegistrationSupersedesOld(t *testing.T) {
	base := relayURL(t)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	old, err := endpoint.Register(base, key)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	newHost, err := endpoint.Register(base, key)
	if err != nil {
		t.Fatal(err)
	}
	defer newHost.Close()
	select {
	case <-old.Done:
		if !websocket.IsCloseError(old.Err, 4001) {
			t.Fatalf("expected superseded close: %v", old.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old registration remained open")
	}
	client, accepted := pair(t, newHost)
	if err := client.WriteMessage(websocket.BinaryMessage, []byte("new owner")); err != nil {
		t.Fatal(err)
	}
	accepted.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, data, err := accepted.ReadMessage(); err != nil || string(data) != "new owner" {
		t.Fatalf("new owner routing: %q %v", data, err)
	}
}

func TestHostOfflineClosesPair(t *testing.T) {
	host := register(t, relayURL(t))
	client, _ := pair(t, host)
	host.Close()
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := client.ReadMessage(); !websocket.IsCloseError(err, 1001) {
		t.Fatalf("expected host offline close: %v", err)
	}
}
