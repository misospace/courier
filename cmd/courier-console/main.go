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

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-console", flag.ContinueOnError)
	var bind, authMode, sessionKeyFile, operatorTokensFile, trustedHeader, backendURL string
	var backendTimeout time.Duration
	flags.StringVar(&bind, "bind", defaultBind, "listen address")
	flags.StringVar(&authMode, "auth-mode", string(console.AuthModeToken), "authentication mode: token or header")
	flags.StringVar(&sessionKeyFile, "session-key-file", "", "file holding the HMAC session-signing key (required in token mode)")
	flags.StringVar(&operatorTokensFile, "operator-tokens-file", "", "file holding newline-separated accepted operator tokens (required in token mode)")
	flags.StringVar(&trustedHeader, "trusted-header", "", "trusted upstream identity header (required in header mode)")
	flags.StringVar(&backendURL, "backend-url", "", "base URL of the operator read API; empty renders the backend-unavailable states")
	flags.DurationVar(&backendTimeout, "backend-timeout", 5*time.Second, "backend probe timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg := console.Config{
		Mode:           console.AuthMode(authMode),
		TrustedHeader:  trustedHeader,
		BackendURL:     backendURL,
		BackendTimeout: backendTimeout,
	}
	// Secrets come from files only: each flag names a file the deployment
	// mounts from a Secret. A missing or unreadable file is fatal.
	switch cfg.Mode {
	case console.AuthModeToken:
		if sessionKeyFile == "" || operatorTokensFile == "" {
			return errors.New("courier-console token mode requires --session-key-file and --operator-tokens-file")
		}
		sessionKey, err := readTrimmedFile(sessionKeyFile)
		if err != nil {
			return err
		}
		tokens, err := readTokensFile(operatorTokensFile)
		if err != nil {
			return err
		}
		cfg.SessionKey = sessionKey
		cfg.OperatorTokens = tokens
	case console.AuthModeHeader:
		if trustedHeader == "" {
			return errors.New("courier-console header mode requires --trusted-header")
		}
	default:
		return fmt.Errorf("courier-console: unknown --auth-mode %q (want token or header)", authMode)
	}
	server, err := console.NewServer(cfg)
	if err != nil {
		return fmt.Errorf("courier-console: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	httpServer := &http.Server{Addr: bind, Handler: server.Handler()}
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
