package endpoint

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

var Dialer = &websocket.Dialer{HandshakeTimeout: 5 * time.Second, ReadBufferSize: 4096, WriteBufferSize: 4096}

type Event struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connectionId"`
	Token        string `json:"token"`
	Nonce        string `json:"nonce"`
	EndpointID   string `json:"endpointId"`
}

type Host struct {
	Base    string
	ID      string
	Control *websocket.Conn
	Events  chan Event
	Done    chan struct{}
	Err     error
}

func EndpointID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])
}

func B64(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func SignChallenge(priv ed25519.PrivateKey, endpointID, nonce string) string {
	return B64(ed25519.Sign(priv, []byte("supacode-relay-v1\n"+endpointID+"\n"+nonce)))
}

func DialControl(base, publicKey string) (*websocket.Conn, Event, *http.Response, error) {
	ws, resp, err := Dialer.Dial(base+"/v1/control?publicKey="+url.QueryEscape(publicKey), nil)
	if err != nil {
		return nil, Event{}, resp, err
	}
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ch Event
	if err := ws.ReadJSON(&ch); err != nil || ch.Type != "challenge" {
		ws.Close()
		return nil, ch, resp, fmt.Errorf("expected challenge: %v", err)
	}
	_ = ws.SetReadDeadline(time.Time{})
	return ws, ch, resp, nil
}

func Register(base string, priv ed25519.PrivateKey) (*Host, error) {
	pub := priv.Public().(ed25519.PublicKey)
	id := EndpointID(pub)
	ws, ch, _, err := DialControl(base, B64(pub))
	if err != nil {
		return nil, err
	}
	if err := ws.WriteJSON(map[string]string{"type": "authenticate", "signature": SignChallenge(priv, id, ch.Nonce)}); err != nil {
		ws.Close()
		return nil, err
	}
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var reg Event
	if err := ws.ReadJSON(&reg); err != nil {
		ws.Close()
		return nil, err
	}
	if reg.Type != "registered" || reg.EndpointID != id {
		ws.Close()
		return nil, fmt.Errorf("unexpected registration reply %q", reg.Type)
	}
	_ = ws.SetReadDeadline(time.Time{})
	h := &Host{Base: base, ID: id, Control: ws, Events: make(chan Event, 1024), Done: make(chan struct{})}
	go h.read()
	return h, nil
}

func (h *Host) read() {
	defer close(h.Done)
	for {
		var ev Event
		if err := h.Control.ReadJSON(&ev); err != nil {
			h.Err = err
			return
		}
		h.Events <- ev
	}
}

func (h *Host) Next(timeout time.Duration) (Event, error) {
	select {
	case ev := <-h.Events:
		return ev, nil
	case <-h.Done:
		return Event{}, fmt.Errorf("control closed: %w", h.Err)
	case <-time.After(timeout):
		return Event{}, errors.New("timed out waiting for control event")
	}
}

func (h *Host) Accept(ev Event) (*websocket.Conn, *http.Response, error) {
	return Accept(h.Base, h.ID, ev.ConnectionID, ev.Token)
}

func Accept(base, endpointID, connectionID, token string) (*websocket.Conn, *http.Response, error) {
	q := url.Values{"endpointId": {endpointID}, "connectionId": {connectionID}, "token": {token}}
	return Dialer.Dial(base+"/v1/accept?"+q.Encode(), nil)
}

func Connect(base, endpointID string) (*websocket.Conn, *http.Response, error) {
	return Dialer.Dial(base+"/v1/connect?endpointId="+url.QueryEscape(endpointID), nil)
}

func (h *Host) Close() {
	h.Control.Close()
	<-h.Done
}
