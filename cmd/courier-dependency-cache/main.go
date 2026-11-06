// Command courier-dependency-cache is the deployment's read-only, pre-populated
// Go dependency cache for secure workers (HARNESS.md §2, issue #136). It
// serves only administrator-approved immutable module artifacts (.info, .mod,
// .zip) from a local store directory over the read-only subset of the GOPROXY
// protocol: a miss is answered with an actionable approval diagnostic and
// never triggers an upstream lookup, fetch, redirect, or cache fill. The
// binary performs no outbound network I/O of any kind — it has no HTTP
// client — so a compromised worker cannot turn the cache into a proxy or an
// arbitrary destination.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/dependencycache"
)

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-dependency-cache", flag.ContinueOnError)
	var (
		addr           string
		root           string
		maxInfo        int64
		maxMod         int64
		maxZip         int64
		maxListEntries int
		maxListBytes   int64
		rateLimit      float64
		burst          int
		maxIPs         int
		logRequests    bool
	)
	defaults := dependencycache.DefaultLimits()
	flags.StringVar(&addr, "addr", ":8123", "listen address (the chart pins the matching ClusterIP port)")
	flags.StringVar(&root, "root", "", "read-only module store root (<module>/@v/<version>.{info,mod,zip})")
	flags.Int64Var(&maxInfo, "max-info-bytes", defaults.MaxInfoBytes, "largest .info artifact served")
	flags.Int64Var(&maxMod, "max-mod-bytes", defaults.MaxModBytes, "largest .mod artifact served")
	flags.Int64Var(&maxZip, "max-zip-bytes", defaults.MaxZipBytes, "largest .zip artifact served (the go command refuses larger module zips)")
	flags.IntVar(&maxListEntries, "max-list-entries", defaults.MaxListEntries, "bound on the entries scanned per module @v directory (0 uses the built-in bound)")
	flags.Int64Var(&maxListBytes, "max-list-bytes", dependencycache.DefaultMaxListBytes, "bound on a @v/list response body in bytes (0 uses the built-in bound)")
	flags.Float64Var(&rateLimit, "rate", 20, "requests per second per source address (0 disables the limit)")
	flags.IntVar(&burst, "burst", 64, "per-source burst allowance")
	flags.IntVar(&maxIPs, "max-tracked-addrs", 1024, "bound on the rate limiter's address table")
	flags.BoolVar(&logRequests, "log-requests", true, "log every request (false logs denials and misses only; request keys in logs are the documented bounded exfiltration channel)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if root == "" {
		return errors.New("courier-dependency-cache requires --root")
	}
	if burst < 1 {
		return errors.New("courier-dependency-cache --burst must be at least 1")
	}
	if maxIPs < 1 {
		return errors.New("courier-dependency-cache --max-tracked-addrs must be at least 1")
	}

	store, err := dependencycache.NewStoreWithLimits(root, dependencycache.Limits{
		MaxInfoBytes: maxInfo, MaxModBytes: maxMod, MaxZipBytes: maxZip,
		MaxListEntries: maxListEntries,
	})
	if err != nil {
		return err
	}
	handler := dependencycache.NewHandler(store, dependencycache.Options{
		RequestsPerSecond:   rateLimit,
		Burst:               burst,
		MaxTrackedSourceIPs: maxIPs,
		MaxListBytes:        maxListBytes,
		Log: func(format string, args ...any) {
			log.Printf("courier-dependency-cache: "+format, args...)
		},
		LogRequests: logRequests,
	})
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: dependencycache.ReadHeaderTimeout,
		ReadTimeout:       dependencycache.ReadTimeout,
		WriteTimeout:      dependencycache.WriteTimeout,
		IdleTimeout:       dependencycache.IdleTimeout,
		MaxHeaderBytes:    dependencycache.MaxHeaderBytes,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		log.Printf("courier-dependency-cache: serving read-only store %q on %s", root, addr)
		errCh <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("courier-dependency-cache: serve: %w", err)
	}
}
