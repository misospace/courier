// Command metrics-mcp serves Courier's metrics mini-MCP: one read-only tool
// reporting model-server load, scraped from a configured /metrics endpoint.
// It is an optional, per-endpoint service; a lane without it simply runs
// without load context.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	couriermetrics "github.com/misospace/courier/internal/mcp/metrics"
)

type options struct {
	metricsURL    string
	backend       string
	scrapeTimeout time.Duration
}

func main() {
	var opts options
	var addr string
	flag.StringVar(&addr, "addr", ":8080", "Address the MCP server listens on.")
	flag.StringVar(&opts.metricsURL, "metrics-url", "", "URL of the model server's Prometheus /metrics endpoint.")
	flag.StringVar(&opts.backend, "backend", couriermetrics.BackendAuto, "Backend mapping for the metrics endpoint: auto, or vllm.")
	flag.DurationVar(&opts.scrapeTimeout, "scrape-timeout", 5*time.Second, "Timeout for a single metrics scrape.")
	flag.Parse()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, listener, opts, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, listener net.Listener, opts options, logw io.Writer) error {
	if opts.metricsURL == "" {
		return errors.New("-metrics-url is required")
	}
	if _, err := couriermetrics.ValidateMetricsURL(opts.metricsURL); err != nil {
		return err
	}
	mapping, err := couriermetrics.For(opts.backend)
	if err != nil {
		return err
	}
	// http.Client.Timeout <= 0 means no timeout; every scrape must be bounded.
	if opts.scrapeTimeout <= 0 {
		return fmt.Errorf("-scrape-timeout must be positive (got %s)", opts.scrapeTimeout)
	}
	logger := log.New(logw, "metrics-mcp ", log.LstdFlags|log.Lmsgprefix)
	scraper := couriermetrics.NewScraper(&http.Client{Timeout: opts.scrapeTimeout}, mapping)
	server := couriermetrics.NewServer(opts.metricsURL, scraper)
	httpServer := &http.Server{Handler: couriermetrics.HTTPHandler(server)}

	logger.Printf("listening on %s (backend %s, metrics endpoint %s)", listener.Addr(), mapping.Name(), couriermetrics.RedactedURL(opts.metricsURL))
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.Serve(listener) }()
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Printf("shutting down")
		return httpServer.Shutdown(context.Background())
	}
}
