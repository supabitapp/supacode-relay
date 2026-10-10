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

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/healthcheck"
	"github.com/supabitapp/supacode-relay/internal/relay"
)

func main() {
	if code, handled := diagnostics.Command(os.Args, os.Stdout, os.Stderr); handled {
		os.Exit(code)
	}
	cfg, err := relay.LoadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: invalid configuration:", err)
		os.Exit(2)
	}
	admin := cfg.Addr
	if cfg.PrivateAddr != "" {
		admin = cfg.PrivateAddr
	}
	if code, ok := healthcheck.Command(os.Args, admin, os.Stdout, os.Stderr); ok {
		os.Exit(code)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay: listen:", err)
		os.Exit(1)
	}
	var pln net.Listener
	if cfg.PrivateAddr != "" {
		if pln, err = net.Listen("tcp", cfg.PrivateAddr); err != nil {
			fmt.Fprintln(os.Stderr, "relay: listen private:", err)
			os.Exit(1)
		}
	}
	s := relay.New(cfg)
	announce(ln, "")
	errc := make(chan error, 2)
	if pln != nil {
		announce(pln, "private")
		go func() { errc <- s.ServePrivate(pln) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { errc <- s.Serve(ln) }()
	select {
	case err := <-errc:
		log.Println("relay: serve:", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	s.Shutdown()
}

func announce(ln net.Listener, listener string) {
	ev := map[string]string{"event": "listening", "address": ln.Addr().String()}
	if listener != "" {
		ev["listener"] = listener
	}
	line, _ := json.Marshal(ev)
	fmt.Println(string(line))
}
