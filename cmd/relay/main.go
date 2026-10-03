package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/supabitapp/supacode-relay/internal/relay"
)

func main() {
	cfg, err := relay.LoadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: invalid configuration:", err)
		os.Exit(2)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: listen:", err)
		os.Exit(1)
	}
	s := relay.New(cfg)
	line, _ := json.Marshal(map[string]string{"event": "listening", "address": ln.Addr().String()})
	fmt.Println(string(line))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ln) }()
	select {
	case err := <-errc:
		log.Println("relay: serve:", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	s.Shutdown()
}
