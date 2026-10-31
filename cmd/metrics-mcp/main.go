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
	// shutdownGrace bounds Shutdown before remaining connections are force-closed.
	shutdownGrace time.Duration
	// scrapeTransport overrides the scraper's HTTP transport; tests set it to
	// fail every scrape deterministically without binding a port.
	scrapeTransport http.RoundTripper
}

// HTTP-server timeouts bound inbound reads: an incomplete header or body
// cannot pin a connection, and idle keep-alives expire. Values follow the
// project-wide pattern in cmd/courier-broker and internal/controller, except
// the write budget, which is derived from the scrape timeout so it can never
// cut off a valid bounded tool call (a longer -scrape-timeout is a supported
// configuration).
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 60 * time.Second
	httpIdleTimeout       = 120 * time.Second
	writeBudgetMargin     = 10 * time.Second
)

func main() {
	var opts options
	var addr string
	flag.StringVar(&addr, "addr", ":8080", "Address the MCP server listens on.")
	flag.StringVar(&opts.metricsURL, "metrics-url", "", "URL of the model server's Prometheus /metrics endpoint.")
	flag.StringVar(&opts.backend, "backend", couriermetrics.BackendAuto, "Backend mapping for the metrics endpoint: auto, or vllm.")
	flag.DurationVar(&opts.scrapeTimeout, "scrape-timeout", 5*time.Second, "Timeout for a single metrics scrape.")
	flag.DurationVar(&opts.shutdownGrace, "shutdown-grace", 10*time.Second, "Grace period for in-flight requests on shutdown before remaining connections are force-closed; a non-positive value force-closes immediately.")
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

// newHTTPServer applies the inbound read budgets and derives the write budget
// from the scrape timeout so a tool call that waits for the full configured
// scrape is never cut off mid-response.
func newHTTPServer(handler http.Handler, scrapeTimeout time.Duration) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      scrapeTimeout + writeBudgetMargin,
		IdleTimeout:       httpIdleTimeout,
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
	scrapeClient := &http.Client{Timeout: opts.scrapeTimeout}
	if opts.scrapeTransport != nil {
		scrapeClient.Transport = opts.scrapeTransport
	}
	scraper := couriermetrics.NewScraper(scrapeClient, mapping)
	server := couriermetrics.NewServer(opts.metricsURL, scraper)
	httpServer := newHTTPServer(couriermetrics.HTTPHandler(server), opts.scrapeTimeout)

	logger.Printf("listening on %s (backend %s, metrics endpoint %s)", listener.Addr(), mapping.Name(), couriermetrics.RedactedURL(opts.metricsURL))
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.Serve(listener) }()
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Printf("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.shutdownGrace)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			// Grace budget expired; force-close any remaining connections so
			// a stuck read does not stall process termination.
			return httpServer.Close()
		}
		return nil
	}
}
