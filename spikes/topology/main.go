package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/supabitapp/supacode-relay/internal/relay"
)

func main() {
	if len(os.Args) > 2 && os.Args[1] == "checktls" {
		c := http.Client{Timeout: 8 * time.Second}
		r, err := c.Head(os.Args[2])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer r.Body.Close()
		fmt.Println(r.StatusCode)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "memory" {
		out := map[string]string{}
		for _, name := range []string{"memory.current", "memory.peak", "memory.stat", "cpu.stat"} {
			data, err := os.ReadFile("/sys/fs/cgroup/" + name)
			if err != nil {
				panic(err)
			}
			out[name] = strings.TrimSpace(string(data))
		}
		json.NewEncoder(os.Stdout).Encode(out)
		return
	}
	cfg, err := relay.LoadConfig(os.Getenv)
	if err != nil {
		panic(err)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		panic(err)
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("SPIKE_TLS_CERT"), os.Getenv("SPIKE_TLS_KEY"))
	if err != nil {
		panic(err)
	}
	secure := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	private, err := net.Listen("tcp", cfg.PrivateAddr)
	if err != nil {
		panic(err)
	}
	s := relay.New(cfg)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	s.RunDirectory(ctx)
	errs := make(chan error, 2)
	go func() { errs <- s.ServePrivate(private) }()
	go func() { errs <- s.Serve(secure) }()
	fmt.Println("spike TLS node ready")
	select {
	case err := <-errs:
		panic(err)
	case <-ctx.Done():
		s.Shutdown()
	}
}
