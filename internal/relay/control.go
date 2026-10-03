package relay

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	signaturePrefix  = "supacode-relay-v1\n"
	controlReadLimit = 4096
)

type host struct {
	id      string
	ctrl    *peer
	pairs   map[string]*pair
	pending int
	gone    bool
}

func (h *host) notify(v any) {
	if h.gone {
		return
	}
	if !h.ctrl.q.push(textFrame(v)) {
		h.ctrl.q.finish(closeMsg{websocket.CloseTryAgainLater, "control queue limit exceeded"}, true)
	}
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, false) {
		return
	}
	pub, ok := decodeB64(r.URL.Query().Get("publicKey"), ed25519.PublicKeySize)
	if !ok {
		s.reject(w, http.StatusBadRequest, "invalid publicKey")
		return
	}
	s.mu.Lock()
	if s.controls >= s.cfg.MaxHosts {
		s.mu.Unlock()
		s.reject(w, http.StatusServiceUnavailable, "host capacity reached")
		return
	}
	s.controls++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.controls--
		s.mu.Unlock()
	}()

	ws, ok := s.upgrade(w, r, controlReadLimit)
	if !ok {
		return
	}
	id := endpointID(pub)
	nonce := randomB64(32)
	challenge := textFrame(map[string]string{"type": "challenge", "nonce": nonce})
	_ = ws.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	if err := ws.WriteMessage(challenge.typ, challenge.data); err != nil {
		s.forget(ws)
		return
	}
	_ = ws.SetReadDeadline(time.Now().Add(s.cfg.AuthTimeout))
	if !verifyAuth(ws, pub, []byte(signaturePrefix+id+"\n"+nonce)) {
		s.rejectedConnections.Add(1)
		s.closeNow(ws, closeMsg{websocket.ClosePolicyViolation, "authentication failed"})
		return
	}

	ctrl := s.newPeer(ws, newQueue(s.cfg.MaxQueueBytes, s.cfg.MaxQueueMessages), false)
	h := &host{id: id, ctrl: ctrl, pairs: map[string]*pair{}}
	ctrl.q.push(textFrame(map[string]string{"type": "registered", "endpointId": id}))
	s.mu.Lock()
	if s.hosts[id] != nil {
		s.mu.Unlock()
		s.rejectedConnections.Add(1)
		s.closeNow(ws, closeMsg{websocket.ClosePolicyViolation, "endpoint already registered"})
		return
	}
	s.hosts[id] = h
	s.mu.Unlock()

	go ctrl.writeLoop()
	ctrl.readControl()
	s.removeHost(h)
}

func verifyAuth(ws *websocket.Conn, pub ed25519.PublicKey, msg []byte) bool {
	typ, data, err := ws.ReadMessage()
	if err != nil || typ != websocket.TextMessage {
		return false
	}
	var auth struct {
		Type      string `json:"type"`
		Signature string `json:"signature"`
	}
	if json.Unmarshal(data, &auth) != nil || auth.Type != "authenticate" {
		return false
	}
	sig, ok := decodeB64(auth.Signature, ed25519.SignatureSize)
	return ok && ed25519.Verify(pub, msg, sig)
}

func (s *Server) removeHost(h *host) {
	s.mu.Lock()
	if s.hosts[h.id] == h {
		delete(s.hosts, h.id)
	}
	h.gone = true
	pairs := make([]*pair, 0, len(h.pairs))
	for _, p := range h.pairs {
		pairs = append(pairs, p)
	}
	s.mu.Unlock()
	for _, p := range pairs {
		p.close(closeMsg{websocket.CloseGoingAway, "host offline"}, nil, false)
	}
}

func decodeB64(s string, n int) ([]byte, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || len(b) != n || base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, false
	}
	return b, true
}

func endpointID(pub []byte) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])
}
