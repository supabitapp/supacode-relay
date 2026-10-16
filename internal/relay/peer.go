package relay

import (
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supaterm-relay/internal/diagnostics"
)

type peer struct {
	s       *Server
	ws      *websocket.Conn
	q       *queue
	dead    chan struct{}
	written chan struct{}
	cause   atomic.Pointer[closeMsg]
	trace   *diagnostics.Trace
}

func (s *Server) newPeer(ws *websocket.Conn, q *queue) *peer {
	p := &peer{s: s, ws: ws, q: q, dead: make(chan struct{}), written: make(chan struct{})}
	p.extend()
	ws.SetPongHandler(func(string) error {
		p.extend()
		return nil
	})
	return p
}

func (p *peer) extend() {
	_ = p.ws.SetReadDeadline(time.Now().Add(p.s.cfg.pongTimeout()))
}

func (p *peer) fail(c closeMsg) {
	if p.cause.CompareAndSwap(nil, &c) {
		p.trace.Event("peer.failed", "close_code", c.code, "reason", diagnostics.CloseReason(c.reason))
	}
	_ = p.ws.Close()
}

func (p *peer) exit() {
	if p.q != nil {
		p.q.finish(closeMsg{websocket.CloseGoingAway, "peer disconnected"}, true)
	}
	close(p.dead)
}

func (p *peer) ping() bool {
	if err := p.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(p.s.cfg.WriteTimeout)); err != nil {
		p.fail(closeMsg{websocket.CloseGoingAway, "peer unreachable"})
		return false
	}
	return true
}

func (p *peer) writeLoop() {
	defer close(p.written)
	defer p.q.finish(closeMsg{websocket.CloseGoingAway, "writer stopped"}, true)
	defer p.s.forget(p.ws)
	cfg := p.s.cfg
	ticker := time.NewTicker(cfg.Heartbeat)
	defer ticker.Stop()
	for {
		f, final, ok := p.q.next()
		if ok {
			_ = p.ws.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout))
			err := p.ws.WriteMessage(f.typ, f.data)
			p.q.release(f)
			if err != nil {
				p.fail(closeMsg{websocket.CloseTryAgainLater, "peer write timeout"})
				return
			}
			select {
			case <-ticker.C:
				if !p.ping() {
					return
				}
			default:
			}
			continue
		}
		if final != nil {
			p.trace.Event("peer.close.sending", "close_code", final.code, "reason", diagnostics.CloseReason(final.reason))
			_ = p.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(final.code, final.reason), time.Now().Add(cfg.WriteTimeout))
			_ = p.ws.SetReadDeadline(time.Now().Add(closeGrace))
			return
		}
		select {
		case <-p.q.wake:
		case <-ticker.C:
			if !p.ping() {
				return
			}
		case <-p.dead:
			return
		}
	}
}

func (p *peer) readControl() {
	defer p.exit()
	for {
		if typ, data, err := p.ws.ReadMessage(); err != nil {
			p.trace.Failure("control.read.ended", err, "close_code", diagnostics.CloseCode(err))
			return
		} else {
			p.trace.Event("control.message.unexpected", "message_type", typ, "bytes", len(data))
		}
		p.q.finish(closeMsg{websocket.ClosePolicyViolation, "unexpected control message"}, true)
	}
}

func sendableCode(c int) bool {
	return (c >= 1000 && c <= 1003) || (c >= 1007 && c <= 1014) || (c >= 3000 && c <= 4999)
}
