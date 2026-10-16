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
	data := make([]*dataConn, 0, len(s.data))
	for conn := range s.data {
		data = append(data, conn)
	}
	s.mu.Unlock()
	shuttingDown := closeMsg{websocket.CloseGoingAway, "relay shutting down"}
	msg := websocket.FormatCloseMessage(shuttingDown.code, shuttingDown.reason)
	for _, ws := range conns {
		go func() { _ = ws.WriteControl(websocket.CloseMessage, msg, hardStop) }()
	}
	for _, conn := range data {
		go func() { _ = conn.sendClose(shuttingDown, hardStop) }()
	}
	for s.socketCount() > 0 && time.Now().Before(hardStop) {
		<-tick.C
	}
	for _, ws := range conns {
		_ = ws.Close()
	}
	for _, conn := range data {
		_ = conn.nc.Close()
	}
	_ = s.http.Close()
	_ = s.private.Close()
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
	return s.openSockets()
}
