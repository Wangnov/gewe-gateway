package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gewe-gateway/internal/config"
	"gewe-gateway/internal/server"
	"gewe-gateway/internal/store"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	sqliteStore, err := store.Open(cfg.DatabasePath)
	if err != nil {
		slog.Error("open store failed", "err", err)
		os.Exit(1)
	}
	defer sqliteStore.Close()

	api, err := server.New(cfg, sqliteStore)
	if err != nil {
		slog.Error("create server failed", "err", err)
		os.Exit(1)
	}
	defer api.Close()

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("gewe-gateway listening", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server exited", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown failed", "err", err)
		os.Exit(1)
	}
}
