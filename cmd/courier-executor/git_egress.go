package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/gitegress"
)

// gitEgressMessage is the single fixed diagnostic the guard prints at
// startup failure. Refusal details stay out of the model-visible pod log.
const gitEgressMessage = "courier: git egress guard unavailable"

// gitEgressConfig is the guard's run-pinned surface, read from the
// environment the pod builder rendered. Every missing or malformed value is
// a startup refusal: the guard serves nothing, the coordinator's git traffic
// fails, and the run terminalizes — there is no path to a half-wired
// credential surface.
type gitEgressConfig struct {
	address      string
	owner        string
	endpoint     string
	namespace    string
	secretName   string
	tokenFile    string
	baseIdentity string
	workIdentity string
	workRemote   string
	baseRemote   string
	workRef      string
}

func decideGitEgress(getenv func(string) string) (gitEgressConfig, error) {
	config := gitEgressConfig{
		address:      strings.TrimSpace(getenv(executor.EnvGitEgressAddress)),
		owner:        strings.TrimSpace(getenv(executor.EnvInstallationOwner)),
		endpoint:     strings.TrimSpace(getenv(executor.EnvInstallationEndpoint)),
		namespace:    strings.TrimSpace(getenv("POD_NAMESPACE")),
		secretName:   strings.TrimSpace(getenv(executor.EnvInstallationSecret)),
		tokenFile:    strings.TrimSpace(getenv(executor.EnvAPITokenFile)),
		baseIdentity: strings.TrimSpace(getenv("COURIER_REPO")),
		workIdentity: strings.TrimSpace(getenv("COURIER_HEAD_REPO")),
		workRemote:   strings.TrimSpace(getenv("COURIER_REPO_URL")),
		baseRemote:   strings.TrimSpace(getenv("COURIER_BASE_REPO_URL")),
	}
	branch := strings.TrimSpace(getenv("COURIER_BRANCH"))
	if config.workIdentity == "" {
		config.workIdentity = config.baseIdentity
	}
	switch {
	case config.address == "":
		return gitEgressConfig{}, errors.New(executor.EnvGitEgressAddress + " is required")
	case config.owner == "" || config.endpoint == "" || config.namespace == "" || config.secretName == "" || config.tokenFile == "":
		return gitEgressConfig{}, errors.New("the installation credential contract is incomplete")
	case config.baseIdentity == "" || config.workIdentity == "":
		return gitEgressConfig{}, errors.New("repository identities are required")
	case config.workRemote == "":
		return gitEgressConfig{}, errors.New("the work repository remote is required")
	case branch == "":
		return gitEgressConfig{}, errors.New("the approved work branch is required")
	}
	config.workRef = "refs/heads/" + branch
	if config.baseRemote == "" {
		config.baseRemote = config.workRemote
	}
	return config, nil
}

// egressCredentials adapts the broker's pinned installation credential
// provider to the guard's narrower provider shape.
type egressCredentials struct {
	provider broker.GitCredentialProvider
}

func (c egressCredentials) Credentials(ctx context.Context, identity string) (gitegress.Credential, error) {
	credentials, err := c.provider.Credentials(ctx, identity)
	if err != nil {
		return gitegress.Credential{}, err
	}
	return gitegress.Credential{Username: credentials.Username, Token: credentials.Token}, nil
}

func gitEgressRemotes(config gitEgressConfig) []gitegress.Remote {
	remotes := []gitegress.Remote{{URL: config.workRemote, Identity: config.workIdentity, Push: true}}
	if !strings.EqualFold(config.baseIdentity, config.workIdentity) {
		remotes = append(remotes, gitegress.Remote{URL: config.baseRemote, Identity: config.baseIdentity})
	}
	return remotes
}

// runGitEgress serves the run's git egress guard until the pod stops. The
// credential document is re-read through the API on every request — the same
// live-read contract the askpass helper and the broker use — so a rotation
// or an expiry takes effect on the next operation, not at restart.
func runGitEgress(ctx context.Context, getenv func(string) string, stderr io.Writer) error {
	config, err := decideGitEgress(getenv)
	if err != nil {
		fmt.Fprintln(stderr, gitEgressMessage)
		return err
	}
	reader, err := installationAPIReader(getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT"), config.tokenFile, getenv(executor.EnvAPICertFile))
	if err != nil {
		fmt.Fprintln(stderr, gitEgressMessage)
		return err
	}
	source, err := broker.NewSecretInstallationTokenSource(reader, config.namespace, config.secretName,
		executor.InstallationTokenDocumentKey, config.owner, config.endpoint, nil)
	if err != nil {
		fmt.Fprintln(stderr, gitEgressMessage)
		return err
	}
	pinned := []string{config.baseIdentity}
	if !strings.EqualFold(config.baseIdentity, config.workIdentity) {
		pinned = append(pinned, config.workIdentity)
	}
	guard, err := gitegress.New(gitegress.Config{
		Remotes:     gitEgressRemotes(config),
		WorkRef:     config.workRef,
		Credentials: egressCredentials{provider: broker.NewInstallationGitCredentials(source, pinned)},
	})
	if err != nil {
		fmt.Fprintln(stderr, gitEgressMessage)
		return err
	}
	server := &http.Server{
		Addr:              config.address,
		Handler:           guard,
		ReadHeaderTimeout: 10 * time.Second,
	}
	listener, err := net.Listen("tcp", config.address)
	if err != nil {
		fmt.Fprintln(stderr, gitEgressMessage)
		return fmt.Errorf("git egress: listen: %w", err)
	}
	serveErr := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()
	shutdown, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	select {
	case err := <-serveErr:
		return err
	case <-shutdown.Done():
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(stopCtx)
	}
}
