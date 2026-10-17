package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	jobqueue "github.com/azrilpramudia/go-job-queue/pkg"
	"github.com/azrilpramudia/go-job-queue/pkg/httpapi"
)

type config struct {
	address       string
	token         string
	workers       int
	queueCapacity int
	databasePath  string
	maxBackoff    time.Duration
	logLevel      slog.Level
	tlsCert       string
	tlsKey        string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if err := validateAddress(cfg); err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel}))
	slog.SetDefault(logger)

	registry := httpapi.NewRegistry()
	if err := registry.Register("noop", func(context.Context, json.RawMessage) error {
		return nil
	}); err != nil {
		return err
	}

	client, err := jobqueue.NewClientWithOptions(jobqueue.ClientOptions{
		WorkerCount:     cfg.workers,
		QueueCapacity:   cfg.queueCapacity,
		SQLitePath:      cfg.databasePath,
		MaxBackoff:      cfg.maxBackoff,
		RetentionPeriod: 7 * 24 * time.Hour,
		MaxTerminalJobs: 10_000,
		ResolveTask:     registry.Resolve,
		Logger:          logger,
	})
	if err != nil {
		return fmt.Errorf("initialize job client: %w", err)
	}

	handler, err := httpapi.NewHandlerWithOptions(client, registry, httpapi.HandlerOptions{
		BearerToken: cfg.token,
		Logger:      logger,
	})
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = client.CloseWithContext(closeCtx)
		return err
	}
	server := &http.Server{
		Addr:              cfg.address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if cfg.tlsCert != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.tlsCert, cfg.tlsKey)
		if err != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = client.CloseWithContext(closeCtx)
			return fmt.Errorf("load TLS certificate: %w", err)
		}
		server.TLSConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		}
	}
	listener, err := net.Listen("tcp", cfg.address)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := client.CloseWithContext(closeCtx); closeErr != nil {
			logger.Error("close job client after listener failure", "error", closeErr)
		}
		return fmt.Errorf("listen on %s: %w", cfg.address, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		var err error
		if server.TLSConfig != nil {
			err = server.ServeTLS(listener, "", "")
		} else {
			err = server.Serve(listener)
		}
		if !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
		close(serverErrors)
	}()
	logger.Info("HTTP API started", "address", cfg.address, "database", cfg.databasePath)

	select {
	case err := <-serverErrors:
		if err != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if closeErr := client.CloseWithContext(shutdownCtx); closeErr != nil {
				logger.Error("close job client after server failure", "error", closeErr)
			}
			return fmt.Errorf("HTTP server: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful HTTP shutdown failed", "error", err)
			_ = server.Close()
		}
		if err := client.CloseWithContext(shutdownCtx); err != nil {
			logger.Error("job client shutdown deadline reached; canceled cooperative tasks", "error", err)
			return err
		}
		logger.Info("HTTP API stopped cleanly")
		return nil
	}
}

func loadConfig() (config, error) {
	cfg := config{
		address:       envOr("JOB_QUEUE_ADDR", "127.0.0.1:3000"),
		token:         strings.TrimSpace(os.Getenv("JOB_QUEUE_API_TOKEN")),
		databasePath:  envOr("JOB_QUEUE_DB_PATH", "jobs.db"),
		maxBackoff:    30 * time.Second,
		queueCapacity: 10,
		workers:       3,
		tlsCert:       os.Getenv("JOB_QUEUE_TLS_CERT"),
		tlsKey:        os.Getenv("JOB_QUEUE_TLS_KEY"),
	}

	var err error
	if cfg.workers, err = intEnv("JOB_QUEUE_WORKERS", cfg.workers); err != nil {
		return config{}, err
	}
	if cfg.queueCapacity, err = intEnv("JOB_QUEUE_CAPACITY", cfg.queueCapacity); err != nil {
		return config{}, err
	}
	if cfg.maxBackoff, err = durationEnv("JOB_QUEUE_MAX_BACKOFF", cfg.maxBackoff); err != nil {
		return config{}, err
	}
	if cfg.workers <= 0 || cfg.queueCapacity < 0 || cfg.maxBackoff <= 0 {
		return config{}, errors.New("workers and max backoff must be positive; queue capacity cannot be negative")
	}

	switch strings.ToLower(envOr("JOB_QUEUE_LOG_LEVEL", "info")) {
	case "debug":
		cfg.logLevel = slog.LevelDebug
	case "info":
		cfg.logLevel = slog.LevelInfo
	case "warn", "warning":
		cfg.logLevel = slog.LevelWarn
	case "error":
		cfg.logLevel = slog.LevelError
	default:
		return config{}, errors.New("JOB_QUEUE_LOG_LEVEL must be debug, info, warn, or error")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return parsed, nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateAddress(cfg config) error {
	if (cfg.tlsCert == "") != (cfg.tlsKey == "") {
		return errors.New("JOB_QUEUE_TLS_CERT and JOB_QUEUE_TLS_KEY must be configured together")
	}
	if !isLoopbackAddress(cfg.address) {
		if cfg.token == "" {
			return errors.New("JOB_QUEUE_API_TOKEN is required when JOB_QUEUE_ADDR is not loopback")
		}
		if len(cfg.token) < 32 {
			return errors.New("JOB_QUEUE_API_TOKEN must contain at least 32 characters for non-loopback bind")
		}
		if cfg.tlsCert == "" {
			return errors.New("JOB_QUEUE_TLS_CERT and JOB_QUEUE_TLS_KEY are required for non-loopback bind")
		}
	}
	return nil
}
