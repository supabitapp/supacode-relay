package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	drainTimeout    = 5 * time.Second
	closeReserve    = 750 * time.Millisecond
	hardStopReserve = 100 * time.Millisecond
)

func main() {
	log.SetOutput(os.Stderr)
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: invalid configuration:", err)
		os.Exit(2)
	}
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: listen:", err)
		os.Exit(1)
	}
	s := newServer(cfg)
	srv := &http.Server{Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second, ErrorLog: log.Default()}
	line, _ := json.Marshal(map[string]string{"event": "listening", "address": ln.Addr().String()})
	fmt.Println(string(line))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		log.Println("relay: serve:", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	s.shutdown(srv)
}

func (s *server) shutdown(srv *http.Server) {
	start := time.Now()
	hardStop := start.Add(drainTimeout - hardStopReserve)
	closeAt := hardStop.Add(-closeReserve)
	s.draining.Store(true)
	log.Println("relay: draining")
	srv.SetKeepAlivesEnabled(false)

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
	_ = srv.Close()
	log.Printf("relay: stopped after %s with %d pairs force-closed", time.Since(start).Round(time.Millisecond), remaining)
}

func (s *server) pairCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending + s.active
}

func (s *server) socketCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
