package e2e

import (
	"net/http"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supaterm-relay/internal/endpoint"
)

func TestRelayWritesNothingAboutConnections(t *testing.T) {
	r := startRelay(t, "RELAY_PAIR_TIMEOUT_MS=100")
	h := register(t, r)
	client, host, _ := pairUp(t, r, h)
	payload := message{websocket.BinaryMessage, []byte("private-tunnel-payload")}
	send(t, client, payload)
	expectMessage(t, host, payload)
	send(t, host, payload)
	expectMessage(t, client, payload)
	_ = client.Close()
	nextEvent(t, h, "closed")

	unaccepted, _ := connect(t, r, h)
	defer unaccepted.Close()
	expectClose(t, unaccepted, websocket.CloseTryAgainLater)
	nextEvent(t, h, "closed")
	r.waitIdle()

	_, resp, err := endpoint.Connect(r.base, endpoint.EndpointID(randomBytes(32)))
	expectStatus(t, resp, err, http.StatusNotFound)

	h.Close()
	r.waitMetrics("host removed", func(m map[string]float64) bool { return m["activeHosts"] == 0 })
	if out := r.stderr.String(); out != "" {
		t.Fatalf("relay wrote connection records:\n%s", out)
	}
}
