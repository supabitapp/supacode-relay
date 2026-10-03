package relay

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	drainTimeout    = 5 * time.Second
	closeReserve    = 750 * time.Millisecond
	hardStopReserve = 100 * time.Millisecond
)

func (s *Server) Shutdown() {
	start := time.Now()
	hardStop := start.Add(drainTimeout - hardStopReserve)
	closeAt := hardStop.Add(-closeReserve)
	s.draining.Store(true)
	log.Println("relay: draining")
	s.http.SetKeepAlivesEnabled(false)

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for s.pairCount() > 0 && time.Now().Before(closeAt) {
		<-tick.C
	}
	remaining := s.pairCount()

	s.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.conns))
	for ws := range s.conns {
		conns = append(conns, ws)
	}
	s.mu.Unlock()
	msg := websocket.FormatCloseMessage(websocket.CloseGoingAway, "relay shutting down")
	for _, ws := range conns {
		go func() { _ = ws.WriteControl(websocket.CloseMessage, msg, hardStop) }()
	}
	for s.socketCount() > 0 && time.Now().Before(hardStop) {
		<-tick.C
	}
	for _, ws := range conns {
		_ = ws.Close()
	}
	_ = s.http.Close()
	log.Printf("relay: stopped after %s with %d pairs force-closed", time.Since(start).Round(time.Millisecond), remaining)
}

func (s *Server) pairCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending + s.active
}

func (s *Server) socketCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
