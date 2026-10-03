package relay

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type peer struct {
	s     *Server
	ws    *websocket.Conn
	q     *queue
	data  bool
	dead  chan struct{}
	cause atomic.Pointer[closeMsg]
}

func (s *Server) newPeer(ws *websocket.Conn, q *queue, data bool) *peer {
	p := &peer{s: s, ws: ws, q: q, data: data, dead: make(chan struct{})}
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
	p.cause.CompareAndSwap(nil, &c)
	_ = p.ws.Close()
}

func (p *peer) exit() {
	p.q.finish(closeMsg{websocket.CloseGoingAway, "peer disconnected"}, true)
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
			if p.data {
				p.s.forwardedMessages.Add(1)
				p.s.forwardedBytes.Add(int64(len(f.data)))
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
		if _, _, err := p.ws.ReadMessage(); err != nil {
			return
		}
		p.q.finish(closeMsg{websocket.ClosePolicyViolation, "unexpected control message"}, true)
	}
}

func (p *peer) readData(pr *pair, out *queue) {
	defer p.exit()
	for {
		typ, reader, err := p.ws.NextReader()
		var f frame
		if err == nil {
			f, err = p.s.budget.read(reader, p.s.cfg.MaxMessageBytes)
		}
		if errors.Is(err, errIngressCapacity) {
			pr.close(closeMsg{websocket.CloseTryAgainLater, "ingress capacity exceeded"}, nil, true)
			return
		}
		if err != nil {
			pr.close(p.closeCause(err), p, false)
			return
		}
		p.extend()
		f.typ = typ
		result := out.pushResult(f)
		if !result.accepted() {
			reason := "queue limit exceeded"
			if result == pushBudgetLimit {
				reason = "ingress capacity exceeded"
			}
			pr.close(closeMsg{websocket.CloseTryAgainLater, reason}, nil, true)
			return
		}
	}
}

func (p *peer) closeCause(err error) closeMsg {
	if c := p.cause.Load(); c != nil {
		return *c
	}
	var ce *websocket.CloseError
	switch {
	case errors.As(err, &ce) && ce.Code == websocket.CloseNoStatusReceived:
		return closeMsg{websocket.CloseNormalClosure, ""}
	case errors.As(err, &ce) && sendableCode(ce.Code):
		return closeMsg{ce.Code, ce.Text}
	case errors.Is(err, websocket.ErrReadLimit):
		return closeMsg{websocket.CloseMessageTooBig, "message too big"}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return closeMsg{websocket.CloseGoingAway, "peer timeout"}
	}
	return closeMsg{websocket.CloseGoingAway, "peer disconnected"}
}

func sendableCode(c int) bool {
	return (c >= 1000 && c <= 1003) || (c >= 1007 && c <= 1014) || (c >= 3000 && c <= 4999)
}
