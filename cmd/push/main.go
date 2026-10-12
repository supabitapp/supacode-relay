package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/supabitapp/supacode-relay/internal/push"
)

func run() error {
	config, err := push.ConfigFromEnvironment()
	if err != nil {
		return err
	}
	store, err := push.OpenStore(config.Database)
	if err != nil {
		return err
	}
	defer store.Close()
	sender, err := push.NewAPNsSender(config.TeamID, config.Keys)
	if err != nil {
		return err
	}
	environments := make([]string, 0, len(config.Keys))
	for environment := range config.Keys {
		environments = append(environments, environment)
	}
	gateway := push.NewGateway(store, sender, config.Topics, environments)
	server := &http.Server{Addr: config.Address, Handler: gateway.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("push gateway listening", "address", config.Address)
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("push gateway stopped", "error", err)
		os.Exit(1)
	}
}
