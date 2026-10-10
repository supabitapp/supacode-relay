package e2e

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
)

func awaitDiagnostic(t *testing.T, relay *relay, event string) map[string]any {
	t.Helper()
	return awaitDiagnosticMatch(t, relay, event, func(map[string]any) bool { return true })
}

func awaitDiagnosticMatch(t *testing.T, relay *relay, event string, matches func(map[string]any) bool) map[string]any {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		relay.stderr.mu.Lock()
		if relay.stderr.changed == nil {
			relay.stderr.changed = make(chan struct{}, 1)
		}
		changed := relay.stderr.changed
		text := relay.stderr.buf.String()
		relay.stderr.mu.Unlock()
		scanner := bufio.NewScanner(strings.NewReader(text))
		for scanner.Scan() {
			var record map[string]any
			if json.Unmarshal(scanner.Bytes(), &record) == nil && record["msg"] == event && matches(record) {
				return record
			}
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("missing %s diagnostic:\n%s", event, text)
		}
	}
}

func TestRouterAndNodeLogsShareTheClientTrace(t *testing.T) {
	cluster := startCluster(t, []string{"node-a"})
	node := cluster.nodes["node-a"]
	host := register(t, cluster.router)
	client, incoming := connect(t, cluster.router, host)
	hostPeer := accept(t, host, incoming)
	const payload = "private-cluster-payload"
	send(t, client, message{websocket.BinaryMessage, []byte(payload)})
	expectMessage(t, hostPeer, message{websocket.BinaryMessage, []byte(payload)})
	created := awaitDiagnostic(t, node, "pair.created")
	trace := created["trace_id"]
	selected := awaitDiagnosticMatch(t, cluster.router, "router.route.selected", func(record map[string]any) bool {
		return record["route"] == "connect" && record["trace_id"] == trace
	})
	if selected["selected_node"] != "node-a" || selected["endpoint_tag"] != created["endpoint_tag"] {
		t.Fatalf("trace did not survive routing: node=%v router=%v", created, selected)
	}
	first := awaitDiagnosticMatch(t, cluster.router, "router.byte.first", func(record map[string]any) bool {
		return record["trace_id"] == trace && record["direction"] == "from_client"
	})
	if first["bytes"].(float64) <= 0 {
		t.Fatalf("no first-byte evidence: %v", first)
	}
	client.Close()
	closed := awaitDiagnostic(t, node, "pair.closed")
	if closed["trace_id"] != trace || closed["pair_tag"] != created["pair_tag"] {
		t.Fatalf("connection lost its correlation: %v", closed)
	}
	for _, process := range []*relay{node, cluster.router} {
		for _, secret := range []string{host.ID, incoming.ConnectionID, incoming.Token, directoryToken, payload} {
			if strings.Contains(process.stderr.String(), secret) {
				t.Fatalf("log exposed %q", secret)
			}
		}
	}
	if created["endpoint_tag"] != diagnostics.Tag(host.ID) {
		t.Fatalf("wrong endpoint tag: %v", created)
	}
}

func TestRelayLogsPairProgressAndCloseWithoutPayloads(t *testing.T) {
	r := startRelay(t)
	h := register(t, r)
	client, incoming := connect(t, r, h)
	hostPeer := accept(t, h, incoming)
	const payload = "private-tunnel-payload"
	send(t, client, message{websocket.BinaryMessage, []byte(payload)})
	expectMessage(t, hostPeer, message{websocket.BinaryMessage, []byte(payload)})
	send(t, hostPeer, message{websocket.BinaryMessage, []byte(payload)})
	expectMessage(t, client, message{websocket.BinaryMessage, []byte(payload)})
	first := awaitDiagnostic(t, r, "peer.message.first")
	notification := awaitDiagnostic(t, r, "pair.host.notification")
	if notification["outcome"] != "queued" {
		t.Fatalf("incorrect host notification milestone: %v", notification)
	}
	if first["endpoint_tag"] == "" || first["pair_tag"] == "" || first["trace_id"] == "" {
		t.Fatalf("missing correlation fields: %v", first)
	}
	_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1000, "private-close-reason"), time.Now().Add(deadline))
	_ = client.Close()
	closed := awaitDiagnostic(t, r, "pair.closed")
	if closed["pair_tag"] != first["pair_tag"] || closed["phase"] != "active" ||
		closed["bytes_to_host"] != float64(len(payload)) || closed["bytes_to_client"] != float64(len(payload)) ||
		closed["messages_to_host"] != float64(1) || closed["messages_to_client"] != float64(1) {
		t.Fatalf("incorrect close summary: %v", closed)
	}
	for _, secret := range []string{h.ID, incoming.ConnectionID, incoming.Token, payload, "private-close-reason"} {
		if strings.Contains(r.stderr.String(), secret) {
			t.Fatalf("log exposed %q", secret)
		}
	}
}

func TestRelayLogsWhyAHostDidNotAccept(t *testing.T) {
	r := startRelay(t, "RELAY_PAIR_TIMEOUT_MS=100")
	h := register(t, r)
	client, _ := connect(t, r, h)
	defer client.Close()
	closed := awaitDiagnostic(t, r, "pair.closed")
	if closed["phase"] != "pending" || closed["reason"] != "pair timeout" ||
		closed["forwarded_bytes"] != nil || closed["bytes_to_host"] != float64(0) ||
		closed["close_code"] != float64(websocket.CloseTryAgainLater) {
		t.Fatalf("incorrect pairing failure: %v", closed)
	}
	if closed["elapsed_ms"].(float64) < 90 {
		t.Fatalf("missing timeout timing: %v", closed)
	}
}
