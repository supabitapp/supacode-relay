package relay

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

const (
	signaturePrefix  = "supacode-relay-v1\n"
	controlReadLimit = 4096
)

type host struct {
	id          string
	regID       string
	version     uint64
	ctrl        *peer
	pairs       map[string]*pair
	pending     int
	clientSlots int
	gone        bool
	detached    bool
	bytesIn     atomic.Int64
	bytesOut    atomic.Int64
	trace       *diagnostics.Trace
}

func (h *host) notify(v any) string {
	if h.gone {
		return "host_gone"
	}
	outcome := h.ctrl.q.enqueue(textFrame(v))
	if outcome == "queue_full" {
		h.ctrl.q.finish(closeMsg{websocket.CloseTryAgainLater, "control queue limit exceeded"}, true)
	}
	return outcome
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, false) {
		return
	}
	pub, ok := decodeB64(r.URL.Query().Get("publicKey"), ed25519.PublicKeySize)
	if !ok {
		s.reject(w, r, http.StatusBadRequest, "invalid publicKey")
		return
	}
	s.mu.Lock()
	if s.controls >= s.cfg.MaxHosts {
		controls := s.controls
		s.mu.Unlock()
		diagnostics.From(r).Event("control.capacity.reached", "current_hosts", controls, "max_hosts", s.cfg.MaxHosts)
		s.reject(w, r, http.StatusServiceUnavailable, "host capacity reached")
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
	trace := diagnostics.From(r)
	trace.Event("control.challenge.sending", "auth_timeout_ms", s.cfg.AuthTimeout.Milliseconds())
	nonce := randomB64(32)
	challenge := textFrame(map[string]string{"type": "challenge", "nonce": nonce})
	_ = ws.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	if err := ws.WriteMessage(challenge.typ, challenge.data); err != nil {
		trace.Failure("control.challenge.failed", err)
		s.forget(ws)
		return
	}
	trace.Event("control.challenge.sent")
	_ = ws.SetReadDeadline(time.Now().Add(s.cfg.AuthTimeout))
	if accepted, reason := verifyAuth(ws, pub, []byte(signaturePrefix+id+"\n"+nonce)); !accepted {
		s.rejectedConnections.Add(1)
		trace.Event("control.auth.failed", "reason", reason)
		s.closeNow(ws, closeMsg{websocket.ClosePolicyViolation, "authentication failed"})
		return
	}
	trace.Event("control.auth.accepted")

	ctrl := s.newPeer(ws, newQueue(s.cfg.MaxQueueBytes, s.cfg.MaxQueueMessages), nil)
	ctrl.trace = trace.With("peer", "control")
	h := &host{id: id, regID: randomB64(16), version: s.clock.Next(), ctrl: ctrl, pairs: map[string]*pair{}, trace: trace}
	ctrl.q.push(textFrame(map[string]string{"type": "registered", "endpointId": id}))
	s.mu.Lock()
	if s.cfg.Clustered() && s.draining.Load() {
		s.mu.Unlock()
		s.closeNow(ws, closeMsg{websocket.CloseGoingAway, "relay draining"})
		return
	}
	old := s.hosts[id]
	s.hosts[id] = h
	s.publishLocked(directory.Message{Type: directory.TypePut, Registration: s.registration(h)})
	s.mu.Unlock()
	trace.Event("host.registered", "superseded_previous", old != nil, "registration_version", h.version)
	if old != nil {
		old.trace.Event("host.superseded", "replacement_trace_id", trace.ID())
		s.superseded.Add(1)
		old.ctrl.q.finish(closeMsg{directory.CloseSuperseded, directory.SupersededText}, true)
	}

	go ctrl.writeLoop()
	defer func() { <-ctrl.written }()
	ctrl.readControl()
	s.removeHost(h)
}

func verifyAuth(ws *websocket.Conn, pub ed25519.PublicKey, msg []byte) (bool, string) {
	typ, data, err := ws.ReadMessage()
	if err != nil {
		return false, diagnostics.ErrorKind(err)
	}
	if typ != websocket.TextMessage {
		return false, "invalid_frame_type"
	}
	var auth struct {
		Type      string `json:"type"`
		Signature string `json:"signature"`
	}
	if json.Unmarshal(data, &auth) != nil {
		return false, "invalid_auth_json"
	}
	if auth.Type != "authenticate" {
		return false, "invalid_auth_type"
	}
	sig, ok := decodeB64(auth.Signature, ed25519.SignatureSize)
	if !ok {
		return false, "invalid_signature_encoding"
	}
	if !ed25519.Verify(pub, msg, sig) {
		return false, "signature_mismatch"
	}
	return true, ""
}

func (s *Server) registration(h *host) *directory.Registration {
	return &directory.Registration{EndpointID: h.id, NodeID: s.cfg.NodeID, RegistrationID: h.regID, Version: h.version}
}

func (s *Server) removeHost(h *host) {
	s.mu.Lock()
	if s.hosts[h.id] == h {
		delete(s.hosts, h.id)
		s.publishLocked(directory.Message{Type: directory.TypeDel, Registration: s.registration(h)})
	}
	h.gone = true
	if h.detached {
		s.mu.Unlock()
		return
	}
	pairs := make([]*pair, 0, len(h.pairs))
	for _, p := range h.pairs {
		pairs = append(pairs, p)
	}
	s.mu.Unlock()
	h.trace.Event("host.removed", "pairs_closing", len(pairs), "bytes_to_host", h.bytesIn.Load(), "bytes_from_host", h.bytesOut.Load())
	var cleanup sync.WaitGroup
	for _, p := range pairs {
		cleanup.Add(1)
		go func() {
			defer cleanup.Done()
			p.close(closeMsg{websocket.CloseGoingAway, "host offline"})
		}()
	}
	cleanup.Wait()
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
