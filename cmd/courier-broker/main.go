// Command courier-broker is the trusted per-run broker (HARNESS.md §2-§4). It
// owns the run's forge and git credentials, serves the typed publication API
// to authenticated control over TLS, TokenReviews every control request
// against the live API, patches only the named run's harness-owned status on
// a separate trusted listener, and refuses to serve unless its projected
// provider registration provably matches the persisted publication policy.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	couriergit "github.com/misospace/courier/internal/git"
	couriergithub "github.com/misospace/courier/internal/github"
	"github.com/misospace/courier/internal/topology"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const kubeCACertPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-broker", flag.ContinueOnError)
	var addr, statusAddr, certFile, keyFile, policyFile, scratchDir string
	flags.StringVar(&addr, "addr", brokerAPIAddrDefault, "TLS listener address for the typed API")
	flags.StringVar(&statusAddr, "status-addr", brokerStatusAddrDefault, "TLS listener address for the trusted status API")
	flags.StringVar(&certFile, "tls-cert", "", "per-run broker TLS certificate")
	flags.StringVar(&keyFile, "tls-key", "", "per-run broker TLS private key")
	flags.StringVar(&policyFile, "policy", "", "operator-resolved immutable run policy")
	flags.StringVar(&scratchDir, "scratch-dir", "", "private scratch directory for bundle import")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if addr == "" || statusAddr == "" || certFile == "" || keyFile == "" || policyFile == "" || scratchDir == "" {
		return errors.New("courier-broker requires address, status address, TLS certificate/key, run policy, and scratch directory")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return run(ctx, runConfig{
		addr: addr, statusAddr: statusAddr, certFile: certFile, keyFile: keyFile,
		policyFile: policyFile, scratchDir: scratchDir,
	})
}

type runConfig struct {
	addr, statusAddr, certFile, keyFile, policyFile, scratchDir string
}

const (
	brokerAPIAddrDefault    = ":8443"
	brokerStatusAddrDefault = ":8444"
)

func run(ctx context.Context, config runConfig) error {
	env, err := loadEnv()
	if err != nil {
		return err
	}
	document, err := loadPolicyDocument(config.policyFile)
	if err != nil {
		return err
	}
	if err := verifyProjection(env, document); err != nil {
		return err
	}

	apiConfig, err := apiClientConfig(env)
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(apiConfig)
	if err != nil {
		return fmt.Errorf("courier-broker: build API client: %w", err)
	}
	scheme := runtimeScheme()
	liveClient, err := ctrlclient.New(apiConfig, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("courier-broker: build live reader: %w", err)
	}

	// The run binding is adopted from the broker pod's own owner chain, never
	// from configuration or request state.
	run, err := liveReadRun(ctx, liveClient, env.namespace)
	if err != nil {
		return err
	}
	policy, err := broker.PolicyFromRun(run, document.Provider.Name, document.PublicationPolicy.BaseRepo)
	if err != nil {
		return fmt.Errorf("courier-broker: persisted policy binding failed: %w", err)
	}
	if err := policyMatches(policy, document.PublicationPolicy); err != nil {
		return err
	}

	provider, err := newProvider(env, document)
	if err != nil {
		return err
	}
	baseRepo, err := provider.ResolveRepository(ctx, policy.BaseRepo)
	if err != nil {
		return fmt.Errorf("courier-broker: base repository resolution failed: %w", err)
	}
	workRepo, err := provider.ResolveRepository(ctx, policy.WorkRepo)
	if err != nil {
		return fmt.Errorf("courier-broker: work repository resolution failed: %w", err)
	}
	adapter, err := broker.NewForgeAdapter(policy, provider, provider, baseRepo, workRepo)
	if err != nil {
		return fmt.Errorf("courier-broker: forge adapter rejected the pinned policy: %w", err)
	}

	gitBroker, err := couriergit.NewBroker(ctx, config.scratchDir)
	if err != nil {
		return fmt.Errorf("courier-broker: private git repository unavailable: %w", err)
	}
	pusher, err := broker.NewGitPusher(ctx, broker.GitTransportConfig{
		Policy:      policy,
		Git:         gitBroker,
		Resolver:    newPinnedResolver(policy, document.Provider),
		Credentials: staticCredentials{username: env.gitUsername, token: env.gitToken},
	})
	if err != nil {
		return fmt.Errorf("courier-broker: git transport rejected the pinned policy: %w", err)
	}
	engine, err := broker.NewPolicyEngine(policy, adapter, pusher)
	if err != nil {
		return fmt.Errorf("courier-broker: policy engine rejected the pinned policy: %w", err)
	}

	// The broker's own startup preflight: every credential-bound check runs
	// here, with the broker's credential, before it serves. A broker that
	// cannot write the pinned destinations is a capability-health failure,
	// not a degraded launch.
	if err := verifyCredentialBoundPolicy(ctx, adapter, policy); err != nil {
		return err
	}

	authenticator, err := broker.NewAuthenticator(ctx, liveClient, clientset, env.namespace, env.podName, env.controlSA, env.controlSAUID)
	if err != nil {
		return fmt.Errorf("courier-broker: identity bootstrap failed: %w", err)
	}
	statusWriter, err := broker.NewStatusWriter(liveClient, liveClient, env.namespace, run.Name)
	if err != nil {
		return fmt.Errorf("courier-broker: status writer configuration failed: %w", err)
	}
	trustedStatus, err := broker.NewTrustedStatusHandler(authenticator, statusWriter, lastCommitValidator(adapter, policy))
	if err != nil {
		return fmt.Errorf("courier-broker: trusted status handler rejected: %w", err)
	}
	server, err := broker.NewServer(broker.ServerConfig{
		Policy:        engine,
		Authenticator: authenticator,
		Importer:      gitBroker,
		ScratchDir:    config.scratchDir,
		TLSCertFile:   config.certFile,
		TLSKeyFile:    config.keyFile,
	})
	if err != nil {
		return fmt.Errorf("courier-broker: server rejected its configuration: %w", err)
	}

	errCh := make(chan error, 2)
	go func() {
		errCh <- server.ServeTLS(config.addr, config.certFile, config.keyFile)
	}()
	go func() {
		errCh <- serveTLSHandler(ctx, trustedStatus, config.statusAddr, config.certFile, config.keyFile)
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("courier-broker: serve: %w", err)
	}
}

type brokerEnv struct {
	namespace      string
	podName        string
	controlSA      string
	controlSAUID   string
	providerName   string
	providerType   string
	providerEndpt  string
	providerGitTpl string
	forgeAPIToken  string
	gitUsername    string
	gitToken       string
	apiTokenFile   string
}

func loadEnv() (*brokerEnv, error) {
	out := &brokerEnv{
		namespace:      os.Getenv(topology.EnvPodNamespace),
		podName:        os.Getenv(topology.EnvBrokerPodName),
		controlSA:      os.Getenv(topology.EnvControlSA),
		controlSAUID:   os.Getenv(topology.EnvControlSAUID),
		providerName:   os.Getenv(topology.EnvProviderName),
		providerType:   os.Getenv(topology.EnvProviderType),
		providerEndpt:  os.Getenv(topology.EnvProviderEndpoint),
		providerGitTpl: os.Getenv(topology.EnvProviderGitEndp),
		forgeAPIToken:  os.Getenv(topology.EnvForgeAPIToken),
		gitUsername:    os.Getenv(topology.EnvGitUsername),
		gitToken:       os.Getenv(topology.EnvGitToken),
		apiTokenFile:   os.Getenv(topology.EnvAPITokenFile),
	}
	missing := map[string]string{
		topology.EnvPodNamespace:     out.namespace,
		topology.EnvBrokerPodName:    out.podName,
		topology.EnvControlSA:        out.controlSA,
		topology.EnvControlSAUID:     out.controlSAUID,
		topology.EnvProviderName:     out.providerName,
		topology.EnvProviderType:     out.providerType,
		topology.EnvProviderEndpoint: out.providerEndpt,
		topology.EnvProviderGitEndp:  out.providerGitTpl,
		topology.EnvForgeAPIToken:    out.forgeAPIToken,
		topology.EnvGitToken:         out.gitToken,
		topology.EnvAPITokenFile:     out.apiTokenFile,
	}
	for name, value := range missing {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("courier-broker: environment %s is required", name)
		}
	}
	if out.gitUsername == "" {
		out.gitUsername = topology.DefaultGitUsername
	}
	return out, nil
}

func loadPolicyDocument(path string) (*topology.BrokerPolicyDocument, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("courier-broker: read policy file: %w", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var document topology.BrokerPolicyDocument
	if err := dec.Decode(&document); err != nil {
		return nil, fmt.Errorf("courier-broker: parse policy file: %w", err)
	}
	if document.PublicationPolicy == nil || document.Provider.Name == "" {
		return nil, errors.New("courier-broker: policy file is incomplete")
	}
	return &document, nil
}

// verifyProjection is the selection/enforcement coherence check: the
// broker's frozen projection must be byte-identical to the persisted policy's
// record of the registration, and the digest must recompute exactly.
func verifyProjection(env *brokerEnv, document *topology.BrokerPolicyDocument) error {
	provider := document.Provider
	if env.providerName != provider.Name || env.providerName != document.PublicationPolicy.ProviderConfigRef {
		return errors.New("courier-broker: projected provider name does not match the persisted policy")
	}
	if env.providerEndpt != provider.Endpoint || provider.Endpoint != document.PublicationPolicy.ProviderEndpoint {
		return errors.New("courier-broker: projected provider endpoint does not match the persisted policy")
	}
	if env.providerGitTpl != provider.GitTemplate || provider.GitTemplate == "" {
		return errors.New("courier-broker: projected git endpoint does not match the persisted policy")
	}
	projection := forge.Projection{
		Name:        provider.Name,
		Type:        env.providerType,
		Endpoint:    env.providerEndpt,
		GitTemplate: env.providerGitTpl,
		GitUsername: provider.GitUsername,
		Credentials: provider.Credentials,
	}
	digest := forge.ProjectionDigest(projection)
	if digest != provider.Digest || digest != document.PublicationPolicy.CredentialRefDigest {
		return errors.New("courier-broker: projection digest does not match the persisted policy")
	}
	return nil
}

func apiClientConfig(env *brokerEnv) (*rest.Config, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("courier-broker: KUBERNETES_SERVICE_HOST/PORT are required")
	}
	if _, err := os.Stat(env.apiTokenFile); err != nil {
		return nil, fmt.Errorf("courier-broker: projected API token unavailable: %w", err)
	}
	return &rest.Config{
		Host:            "https://" + net.JoinHostPort(host, port),
		BearerTokenFile: env.apiTokenFile,
		TLSClientConfig: rest.TLSClientConfig{CAFile: kubeCACertPath},
	}, nil
}

// liveReadRun reads the run through the uncached client and follows the
// broker pod's owner chain to it, independently of the policy file.
func liveReadRun(ctx context.Context, reader ctrlclient.Reader, namespace string) (*courier.CoderRun, error) {
	pod := &corev1.Pod{}
	if err := reader.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: os.Getenv(topology.EnvBrokerPodName)}, pod); err != nil {
		return nil, fmt.Errorf("courier-broker: read own pod: %w", err)
	}
	runName, runUID, ok := controllerRunOwner(pod)
	if !ok {
		return nil, errors.New("courier-broker: own pod has no unambiguous CoderRun controller owner")
	}
	run := &courier.CoderRun{}
	if err := reader.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: runName}, run); err != nil {
		return nil, fmt.Errorf("courier-broker: read owner run: %w", err)
	}
	if run.UID != runUID {
		return nil, errors.New("courier-broker: owner run incarnation does not match the pod owner reference")
	}
	return run, nil
}

func controllerRunOwner(pod *corev1.Pod) (string, types.UID, bool) {
	var name string
	var uid types.UID
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.APIVersion != courier.GroupVersion.String() || owner.Kind != "CoderRun" || owner.Name == "" || owner.UID == "" || name != "" {
			return "", "", false
		}
		name, uid = owner.Name, owner.UID
	}
	return name, uid, name != "" && uid != ""
}

// runtimeScheme builds the scheme for the live API clients.
func runtimeScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(courier.AddToScheme(scheme))
	return scheme
}

func policyMatches(resolved broker.Policy, persisted *courier.PublicationPolicy) error {
	if resolved.BaseRepo != persisted.BaseRepo || resolved.BaseRef != persisted.BaseRef ||
		resolved.BaseOID != persisted.BaseOID || resolved.WorkRepo != persisted.WorkRepo ||
		resolved.WorkRef != persisted.WorkRef || resolved.WorkInitiallyAbsent != persisted.WorkInitiallyAbsent ||
		resolved.WorkAnchorOID != persisted.WorkOID || resolved.PRNumber != persisted.PRNumber ||
		resolved.HeadAnchorOID != persisted.HeadAnchorOID {
		return errors.New("courier-broker: live policy binding differs from the persisted policy")
	}
	return nil
}

func newProvider(env *brokerEnv, document *topology.BrokerPolicyDocument) (interface {
	forge.Provider
	forge.RepositoryPolicyProvider
	forge.PullRequestHeadLister
}, error) {
	cfg := forge.ProviderConfig{Name: document.Provider.Name, Endpoint: env.providerEndpt}
	switch env.providerType {
	case "github":
		client, err := couriergithub.New(env.forgeAPIToken)
		if err != nil {
			return nil, fmt.Errorf("courier-broker: forge client rejected its configuration: %w", err)
		}
		return couriergithub.NewProvider(cfg, client), nil
	default:
		// Unsupported providers fail closed rather than silently dropping a
		// check.
		return nil, fmt.Errorf("courier-broker: provider type %q has no registered implementation", env.providerType)
	}
}

// pinnedResolver derives every git endpoint from the projected registration
// and only for the policy's pinned repositories. Request- or model-supplied
// URLs are never endpoints.
type pinnedResolver struct {
	policy   broker.Policy
	template string
}

func newPinnedResolver(policy broker.Policy, provider forge.Projection) *pinnedResolver {
	return &pinnedResolver{policy: policy, template: provider.GitTemplate}
}

func (r *pinnedResolver) Endpoint(_ context.Context, identity string) (string, error) {
	if identity != r.policy.BaseRepo && identity != r.policy.WorkRepo {
		return "", fmt.Errorf("repository %q is not pinned by this run's policy", identity)
	}
	return strings.Replace(r.template, "%s", identity, 1), nil
}

type staticCredentials struct {
	username, token string
}

func (c staticCredentials) Credentials(context.Context, string) (broker.GitCredentials, error) {
	if c.token == "" {
		return broker.GitCredentials{}, errors.New("courier-broker: git credential is unavailable")
	}
	return broker.GitCredentials{Username: c.username, Token: c.token}, nil
}

// verifyCredentialBoundPolicy is the broker startup preflight: write access
// and effective protection under the broker's own credential, for the pinned
// destination in both repositories (once when identical).
func verifyCredentialBoundPolicy(ctx context.Context, adapter *broker.ForgeAdapter, policy broker.Policy) error {
	base, err := adapter.Repository(ctx, policy.BaseRepo, policy.BaseRef)
	if err != nil {
		return fmt.Errorf("courier-broker: base policy read failed: %w", err)
	}
	if !base.Exists {
		return fmt.Errorf("courier-broker: pinned base ref %q of %q does not exist", policy.BaseRef, policy.BaseRepo)
	}
	work, err := adapter.Repository(ctx, policy.WorkRepo, policy.WorkRef)
	if err != nil {
		return fmt.Errorf("courier-broker: work policy read failed: %w", err)
	}
	if work.Default {
		return fmt.Errorf("courier-broker: pinned work ref %q is the default ref of %q and is not a publication destination", policy.WorkRef, policy.WorkRepo)
	}
	if !work.ProtectionKnown || work.Protected {
		return fmt.Errorf("courier-broker: pinned work ref %q of %q is protected or its protection cannot be established", policy.WorkRef, policy.WorkRepo)
	}
	if !work.WriteKnown || !work.Writable {
		return fmt.Errorf("courier-broker: the configured credential cannot write %q; refusing to serve", policy.WorkRepo)
	}
	if policy.Mode == broker.ModeFixPR {
		// The pinned PR must still exist with the pinned full head identity
		// before the broker serves.
		pr, err := adapter.PullRequest(ctx, policy.PRNumber)
		if err != nil {
			return fmt.Errorf("courier-broker: pinned pull request read failed: %w", err)
		}
		if pr.BaseRepo != policy.BaseRepo || pr.BaseRef != policy.BaseRef || pr.HeadRepo != policy.WorkRepo || pr.HeadRef != policy.WorkRef {
			return fmt.Errorf("courier-broker: live pull request identity no longer matches the pinned policy")
		}
	}
	return nil
}

// lastCommitValidator is the trusted status live-world validation: a
// lastCommit confirmation is accepted only while the live work ref still
// carries exactly that OID.
func lastCommitValidator(adapter *broker.ForgeAdapter, policy broker.Policy) broker.StatusValidator {
	return func(ctx context.Context, _ *courier.CoderRun, _ *corev1.Pod, patch broker.HarnessPatch) error {
		if patch.LastCommit == nil || *patch.LastCommit == "" {
			return nil
		}
		state, err := adapter.Repository(ctx, policy.WorkRepo, policy.WorkRef)
		if err != nil {
			return fmt.Errorf("live work ref read failed: %w", err)
		}
		if !state.Exists || state.OID != *patch.LastCommit {
			return errors.New("lastCommit does not match the live work ref tip")
		}
		return nil
	}
}

func serveTLSHandler(ctx context.Context, handler http.Handler, addr, certFile, keyFile string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS13},
		ReadHeaderTimeout: 10 * time.Second,
	}
	err := server.ListenAndServeTLS(certFile, keyFile)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
