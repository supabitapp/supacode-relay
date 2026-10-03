package e2e

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func TestHealthMetricsAndRegistration(t *testing.T) {
	r := startRelay(t)
	code, body := r.get("/healthz")
	if code != 200 || strings.TrimSpace(string(body)) != `{"status":"ok"}` {
		t.Fatalf("healthz: %d %s", code, body)
	}
	priv := newKey(t)
	h := registerKey(t, r, priv)
	sum := sha256.Sum256(priv.Public().(ed25519.PublicKey))
	if h.ID != hex.EncodeToString(sum[:]) {
		t.Fatalf("endpointId mismatch: %s", h.ID)
	}
	client, host, id := pairUp(t, r, h)
	secret := message{websocket.TextMessage, []byte("payload-marker-7f3a")}
	send(t, client, secret)
	expectMessage(t, host, secret)
	m := r.metrics()
	for _, k := range []string{"activeHosts", "activePairs", "pendingPairs", "forwardedMessages", "forwardedBytes", "rejectedConnections"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("metric %s missing or not numeric", k)
		}
	}
	if m["activeHosts"] != 1 || m["activePairs"] != 1 || m["forwardedMessages"] < 1 || m["forwardedBytes"] < float64(len(secret.data)) {
		t.Fatalf("unexpected metrics %v", m)
	}
	_, raw := r.get("/metrics")
	for _, s := range []string{"payload-marker", id, endpoint.B64(priv.Public().(ed25519.PublicKey)), h.ID} {
		if strings.Contains(string(raw), s) {
			t.Fatalf("metrics leaked %q", s)
		}
	}
}

func TestRejectsNoncanonicalPublicKeys(t *testing.T) {
	r := startRelay(t)
	pub := newKey(t).Public().(ed25519.PublicKey)
	canonical := endpoint.B64(pub)
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alphabet, canonical[len(canonical)-1])
	trailing := canonical[:len(canonical)-1] + string(alphabet[last|1])
	cases := map[string]string{
		"missing":       "",
		"padded":        canonical + "=",
		"trailing bits": trailing,
		"short":         endpoint.B64(pub[:31]),
		"long":          endpoint.B64(append(append([]byte{}, pub...), 0)),
		"std alphabet":  base64.StdEncoding.EncodeToString(pub),
		"newline":       canonical[:10] + "\n" + canonical[10:],
		"not base64":    strings.Repeat("!", 43),
	}
	for name, key := range cases {
		if key == canonical {
			continue
		}
		_, resp, err := endpoint.Dialer.Dial(r.base+"/v1/control?publicKey="+url.QueryEscape(key), nil)
		if err == nil {
			t.Fatalf("%s: upgraded", name)
		}
		if resp == nil || resp.StatusCode != 400 {
			t.Fatalf("%s: expected 400, got %s", name, status(resp))
		}
	}
	if r.metrics()["rejectedConnections"] < float64(len(cases)-1) {
		t.Fatal("rejections not counted")
	}
}

func TestInvalidAuthentication(t *testing.T) {
	r := startRelay(t)
	priv := newKey(t)
	pub := priv.Public().(ed25519.PublicKey)
	id := endpoint.EndpointID(pub)
	other := newKey(t)
	cases := map[string]func(nonce string) (int, []byte){
		"random signature": func(string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.B64(randomBytes(64))})
		},
		"other key": func(nonce string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(other, id, nonce)})
		},
		"wrong nonce": func(string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(priv, id, endpoint.B64(randomBytes(32)))})
		},
		"trailing newline": func(nonce string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(priv, id, nonce+"\n")})
		},
		"padded signature": func(nonce string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(priv, id, nonce) + "=="})
		},
		"missing signature": func(string) (int, []byte) {
			return websocket.TextMessage, []byte(`{"type":"authenticate"}`)
		},
		"wrong type": func(nonce string) (int, []byte) {
			return websocket.TextMessage, mustJSON(map[string]string{"type": "hello", "signature": endpoint.SignChallenge(priv, id, nonce)})
		},
		"binary frame": func(nonce string) (int, []byte) {
			return websocket.BinaryMessage, mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(priv, id, nonce)})
		},
		"not json": func(string) (int, []byte) { return websocket.TextMessage, []byte("nope") },
	}
	for name, build := range cases {
		ws, ch, resp, err := endpoint.DialControl(r.base, endpoint.B64(pub))
		if err != nil {
			t.Fatalf("%s: %v (%s)", name, err, status(resp))
		}
		if raw, _ := base64.RawURLEncoding.Strict().DecodeString(ch.Nonce); len(raw) != 32 {
			t.Fatalf("%s: nonce is not 32 bytes", name)
		}
		typ, data := build(ch.Nonce)
		if err := ws.WriteMessage(typ, data); err != nil {
			t.Fatal(err)
		}
		expectClose(t, ws, websocket.ClosePolicyViolation)
		ws.Close()
	}
	if m := r.metrics(); m["activeHosts"] != 0 {
		t.Fatalf("host registered after failed auth: %v", m)
	}
	registerKey(t, r, priv)
}

func TestAuthenticationTimeout(t *testing.T) {
	r := startRelay(t, "RELAY_AUTH_TIMEOUT_MS=150")
	ws, _, _, err := endpoint.DialControl(r.base, endpoint.B64(newKey(t).Public().(ed25519.PublicKey)))
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	expectClose(t, ws, websocket.ClosePolicyViolation)
}

func TestStolenEndpointClaim(t *testing.T) {
	r := startRelay(t)
	victim := newKey(t)
	h := registerKey(t, r, victim)
	pub := endpoint.B64(victim.Public().(ed25519.PublicKey))

	ws, ch, _, err := endpoint.DialControl(r.base, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(newKey(t), h.ID, ch.Nonce)}); err != nil {
		t.Fatal(err)
	}
	expectClose(t, ws, websocket.ClosePolicyViolation)

	client, host, _ := pairUp(t, r, h)
	msg := message{websocket.TextMessage, []byte("still mine")}
	send(t, client, msg)
	expectMessage(t, host, msg)
	if m := r.metrics(); m["activeHosts"] != 1 || m["supersededRegistrations"] != 0 {
		t.Fatalf("unexpected metrics %v", m)
	}
}

func TestDuplicateRegistrationNewestWins(t *testing.T) {
	r := startRelay(t)
	priv := newKey(t)
	old := registerKey(t, r, priv)
	oldClient, oldHost, _ := pairUp(t, r, old)
	pendingClient, pendingEv := connect(t, r, old)

	fresh := registerKey(t, r, priv)
	<-old.Done
	var ce *websocket.CloseError
	if !errors.As(old.Err, &ce) || ce.Code != 4001 || ce.Text != "registration superseded" {
		t.Fatalf("old control closed with %v, want 4001 registration superseded", old.Err)
	}
	for _, ws := range []*websocket.Conn{oldClient, oldHost, pendingClient} {
		if ce := expectClose(t, ws, websocket.CloseGoingAway); ce.Text != "host offline" {
			t.Fatalf("unexpected reason %q", ce.Text)
		}
	}
	_, resp, err := fresh.Accept(pendingEv)
	expectStatus(t, resp, err, 404)

	client, host, id := pairUp(t, r, fresh)
	msg := message{websocket.BinaryMessage, []byte("newest registration")}
	send(t, client, msg)
	expectMessage(t, host, msg)
	client.Close()
	if ev := nextEvent(t, fresh, "closed"); ev.ConnectionID != id {
		t.Fatalf("fresh host received a stale event %+v", ev)
	}
	r.waitMetrics("one host, one supersede", func(m map[string]float64) bool {
		return m["activeHosts"] == 1 && m["supersededRegistrations"] == 1 && m["activePairs"] == 0 && m["pendingPairs"] == 0
	})
}

func TestChallengeReplayAndReconnect(t *testing.T) {
	r := startRelay(t)
	priv := newKey(t)
	pub := endpoint.B64(priv.Public().(ed25519.PublicKey))
	id := endpoint.EndpointID(priv.Public().(ed25519.PublicKey))

	first, ch1, _, err := endpoint.DialControl(r.base, pub)
	if err != nil {
		t.Fatal(err)
	}
	captured := mustJSON(map[string]string{"type": "authenticate", "signature": endpoint.SignChallenge(priv, id, ch1.Nonce)})
	if err := first.WriteMessage(websocket.TextMessage, captured); err != nil {
		t.Fatal(err)
	}
	var reg endpoint.Event
	if err := first.ReadJSON(&reg); err != nil || reg.Type != "registered" || reg.EndpointID != id {
		t.Fatalf("registration failed: %v %+v", err, reg)
	}

	if err := first.WriteMessage(websocket.TextMessage, captured); err != nil {
		t.Fatal(err)
	}
	expectClose(t, first, websocket.ClosePolicyViolation)
	first.Close()
	r.waitMetrics("host removed", func(m map[string]float64) bool { return m["activeHosts"] == 0 })

	replay, ch2, _, err := endpoint.DialControl(r.base, pub)
	if err != nil {
		t.Fatal(err)
	}
	if ch2.Nonce == ch1.Nonce {
		t.Fatal("nonce reused")
	}
	if err := replay.WriteMessage(websocket.TextMessage, captured); err != nil {
		t.Fatal(err)
	}
	expectClose(t, replay, websocket.ClosePolicyViolation)
	replay.Close()

	h := registerKey(t, r, priv)
	client, host, _ := pairUp(t, r, h)
	msg := message{websocket.BinaryMessage, []byte{1, 2, 3}}
	send(t, host, msg)
	expectMessage(t, client, msg)
}

func TestReconnectAfterAbruptControlLoss(t *testing.T) {
	r := startRelay(t)
	priv := newKey(t)
	h := registerKey(t, r, priv)
	h.Control.UnderlyingConn().Close()
	r.waitMetrics("host removed", func(m map[string]float64) bool { return m["activeHosts"] == 0 })
	h2 := registerKey(t, r, priv)
	if h2.ID != h.ID {
		t.Fatal("endpoint id changed")
	}
}
