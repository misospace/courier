// Command courier-console is the standalone Courier operator console: a
// small, server-rendered scaffold for authenticated operators. It is
// deliberately independent of the manager — it never talks to the Kubernetes
// API, and it grows against the (future) operator read API instead. Secrets
// are read from files (mounted Secret volumes), never from environment
// values; all server logic lives in internal/console.
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
	"strings"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/console"
)

const defaultBind = ":8084"

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// serverTimeouts bound request and connection resources: a public-ish
// listener without them lets slow or idle clients pin goroutines and file
// descriptors. ReadTimeout bounds reading a whole request; WriteTimeout
// covers the write side and stays generous around the handler's bounded
// backend probe (backend-timeout defaults to 5s); the console's handlers
// are small form posts and rendered pages.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 90 * time.Second
)

// defaultBackendTimeout is the default bound on each backend probe. The
// test for the write bound pins against this same constant.
const defaultBackendTimeout = 5 * time.Second
const defaultContributeTimeout = 20 * time.Second

func runArgs(args []string) error {
	bind, cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	server, err := console.NewServer(cfg)
	if err != nil {
		return fmt.Errorf("courier-console: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	httpServer := newHTTPServer(bind, server.Handler())
	errCh := make(chan error, 1)
	go func() {
		log.Printf("courier-console listening on %s (auth mode %s)", bind, cfg.Mode)
		errCh <- httpServer.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("courier-console: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("courier-console shutdown: %w", err)
		}
		return nil
	}
}

// newHTTPServer builds the console's bounded listener. It is separate from
// runArgs so the timeout posture is testable without opening a socket.
func newHTTPServer(bind string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              bind,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
}

// parseFlags builds the server configuration from the command line. Secrets
// come from files only: each flag names a file the deployment mounts from a
// Secret. A missing or unreadable file is fatal.
func parseFlags(args []string) (string, console.Config, error) {
	flags := flag.NewFlagSet("courier-console", flag.ContinueOnError)
	var bind, authMode, sessionKeyFile, operatorTokensFile, trustedHeader, backendURL string
	var contributeURL, contributeTokenFile string
	var backendTimeout, contributeTimeout time.Duration
	var cookieSecure bool
	flags.StringVar(&bind, "bind", defaultBind, "listen address")
	flags.StringVar(&authMode, "auth-mode", string(console.AuthModeToken), "authentication mode: token or header")
	flags.StringVar(&sessionKeyFile, "session-key-file", "", "file holding the HMAC session-signing key (required in token mode)")
	flags.StringVar(&operatorTokensFile, "operator-tokens-file", "", "file holding newline-separated accepted operator tokens (required in token mode)")
	flags.StringVar(&trustedHeader, "trusted-header", "", "trusted upstream identity header (required in header mode)")
	flags.StringVar(&backendURL, "backend-url", "", "base URL of the operator read API; empty renders the backend-unavailable states")
	flags.DurationVar(&backendTimeout, "backend-timeout", defaultBackendTimeout, "backend probe timeout")
	flags.StringVar(&contributeURL, "contribute-url", "", "base URL of the contribution admission API")
	flags.StringVar(&contributeTokenFile, "contribute-token-file", "", "file holding the contribution admission bearer token")
	flags.DurationVar(&contributeTimeout, "contribute-timeout", defaultContributeTimeout, "contribution admission timeout")
	flags.BoolVar(&cookieSecure, "cookie-secure", true, "set the Secure attribute on session cookies; false is an explicit opt-out for a console reachable only over plaintext the operator controls")
	if err := flags.Parse(args); err != nil {
		return "", console.Config{}, err
	}
	if (contributeURL == "") != (contributeTokenFile == "") {
		return "", console.Config{}, errors.New("courier-console contribution admission requires both --contribute-url and --contribute-token-file")
	}
	if contributeTimeout <= 0 || contributeTimeout >= writeTimeout {
		return "", console.Config{}, fmt.Errorf("courier-console --contribute-timeout must be greater than zero and less than %s so the console write deadline can return the admission result", writeTimeout)
	}
	cfg := console.Config{
		Mode:              console.AuthMode(authMode),
		TrustedHeader:     trustedHeader,
		BackendURL:        backendURL,
		BackendTimeout:    backendTimeout,
		ContributeURL:     contributeURL,
		ContributeTimeout: contributeTimeout,
		InsecureCookies:   !cookieSecure,
	}
	if contributeTokenFile != "" {
		token, err := readTrimmedFile(contributeTokenFile)
		if err != nil {
			return "", console.Config{}, err
		}
		cfg.ContributeToken = token
	}
	switch cfg.Mode {
	case console.AuthModeToken:
		if sessionKeyFile == "" || operatorTokensFile == "" {
			return "", console.Config{}, errors.New("courier-console token mode requires --session-key-file and --operator-tokens-file")
		}
		sessionKey, err := readTrimmedFile(sessionKeyFile)
		if err != nil {
			return "", console.Config{}, err
		}
		tokens, err := readTokensFile(operatorTokensFile)
		if err != nil {
			return "", console.Config{}, err
		}
		cfg.SessionKey = sessionKey
		cfg.OperatorTokens = tokens
	case console.AuthModeHeader:
		if trustedHeader == "" {
			return "", console.Config{}, errors.New("courier-console header mode requires --trusted-header")
		}
	default:
		return "", console.Config{}, fmt.Errorf("courier-console: unknown --auth-mode %q (want token or header)", authMode)
	}
	return bind, cfg, nil
}

// readTrimmedFile reads the whole file and trims surrounding whitespace, so
// a Secret key's trailing newline never becomes part of the key.
func readTrimmedFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("courier-console: read %s: %w", path, err)
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, fmt.Errorf("courier-console: %s is empty", path)
	}
	return []byte(trimmed), nil
}

// readTokensFile parses newline-separated accepted operator tokens. Blank
// lines and surrounding whitespace are dropped; an empty file is fatal
// because a console that accepts no token can only be dead weight.
func readTokensFile(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("courier-console: read %s: %w", path, err)
	}
	var tokens [][]byte
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tokens = append(tokens, []byte(line))
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("courier-console: %s carries no operator tokens", path)
	}
	return tokens, nil
}
