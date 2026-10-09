package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/bootstrap"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	couriergithub "github.com/misospace/courier/internal/github"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/dispatch"
	"github.com/misospace/courier/internal/source/manual"
	"github.com/misospace/courier/internal/status"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(courierv1alpha1.AddToScheme(scheme))
}

// stringSliceValue appends each occurrence of a repeatable flag to a slice.
type stringSliceValue []string

func (v *stringSliceValue) String() string {
	return strings.Join(*v, ",")
}

func (v *stringSliceValue) Set(value string) error {
	*v = append(*v, value)
	return nil
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var executorImage string
	var gitRemoteTemplate string
	var gitCredentialSecret string
	var gitTokenKey string
	var githubCredentialSecret string
	var githubTokenKey string
	var executorEnvironmentSecret string
	var opencodeAgent string
	var githubMCPURL string
	var context7MCPURL string
	var metricsMCPURL string
	var dispatchEnabled bool
	var dispatchBaseURL string
	var dispatchAgentName string
	var dispatchQueueLane string
	var dispatchLane string
	var dispatchLaneBindings stringSliceValue
	var dispatchPollInterval time.Duration
	var dispatchHTTPTimeout time.Duration
	var evidenceIntakeKeySecret string
	var evidenceIntakeService string
	var evidenceIntakeBind string
	var secureMode bool
	var forgeProvidersFile string
	var runNamespace string
	var harnessImage string
	var dependencyCacheService string
	var dependencyCachePort int
	var probeImage string
	var bootstrapLaneProfilesFile string
	var modelGatewayURL string
	var modelGatewayKeySecret string
	var modelGatewayKeyName string
	var modelGatewayCIDR string
	var modelGatewayPort int
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	defaults := executor.DefaultPodConfig()
	flag.StringVar(&executorImage, "executor-image", defaults.Image, "Coordinator image containing courier-executor, git, and opencode.")
	flag.StringVar(&gitRemoteTemplate, "git-remote-template", defaults.GitRemoteURL, "Git remote URL template containing one %s repository placeholder.")
	flag.StringVar(&gitCredentialSecret, "git-credential-secret", "courier-github", "Secret containing username and token keys for private git access; empty disables git secret injection.")
	flag.StringVar(&gitTokenKey, "git-token-key", "", "Optional key in the git credential Secret; empty defaults to token.")
	flag.StringVar(&githubCredentialSecret, "github-credential-secret", "", "Optional Secret containing the GitHub API token; empty falls back to --git-credential-secret.")
	flag.StringVar(&githubTokenKey, "github-token-key", "", "Optional key in the GitHub API credential Secret; empty falls back to --git-token-key or token.")
	flag.StringVar(&executorEnvironmentSecret, "executor-environment-secret", "", "Optional Secret exposed as coordinator environment variables for model/provider configuration.")
	flag.StringVar(&opencodeAgent, "opencode-agent", "", "Optional OpenCode agent name used for coordinator runs.")
	flag.StringVar(&githubMCPURL, "github-mcp-url", "", "Optional remote MCP endpoint for GitHub.")
	flag.StringVar(&context7MCPURL, "context7-mcp-url", "", "Optional remote MCP endpoint for Context7.")
	flag.StringVar(&metricsMCPURL, "metrics-mcp-url", "", "Optional remote MCP endpoint for metrics; absence is harmless.")
	flag.BoolVar(&secureMode, "secure-mode", false, "Route runs through the isolated control/broker/worker topology; legacy stays the default and remains explicitly insecure.")
	flag.StringVar(&forgeProvidersFile, "forge-providers-file", "", "Secure mode: provider registry file carrying references only.")
	flag.StringVar(&runNamespace, "run-namespace", "", "Secure mode: the namespace dedicated to run workloads.")
	flag.StringVar(&harnessImage, "harness-image", defaultsHarnessImage, "Secure mode: image carrying courier-control, courier-worker, and courier-broker.")
	flag.StringVar(&dependencyCacheService, "dependency-cache-service", "", "Secure mode: namespace/name of the approved read-only dependency cache Service; empty means workers have no egress.")
	flag.IntVar(&dependencyCachePort, "dependency-cache-port", 0, "Secure mode: cache port override; empty uses the Service's first port.")
	flag.StringVar(&probeImage, "probe-image", defaultsProbeImage, "Secure mode: image for disposable network-probe pods.")
	flag.StringVar(&bootstrapLaneProfilesFile, "bootstrap-lane-profiles-file", "", "Path to a YAML file of deployment-managed bootstrap LaneProfiles; empty disables bootstrap lane management.")
	flag.StringVar(&modelGatewayURL, "model-gateway-url", "", "Secure mode: OpenAI-compatible model gateway API root (for example http://litellm:4000/v1); empty means the native harness fails closed on the model-bindings capability.")
	flag.StringVar(&modelGatewayKeySecret, "model-gateway-key-secret", "", "Secure mode: namespace/name of the deployment's model-gateway key Secret; copied per run into trusted control only.")
	flag.StringVar(&modelGatewayKeyName, "model-gateway-key-name", "key", "Secure mode: key within the model-gateway key Secret.")
	flag.StringVar(&modelGatewayCIDR, "model-gateway-cidr", "", "Secure mode: CIDR of the model gateway for control egress policy; empty means no gateway egress slot.")
	flag.IntVar(&modelGatewayPort, "model-gateway-port", 0, "Secure mode: model gateway port for control egress policy.")
	flag.BoolVar(&dispatchEnabled, "dispatch-enabled", false, "Enable Dispatch source discovery.")
	flag.StringVar(&dispatchBaseURL, "dispatch-base-url", "", "Dispatch base URL.")
	flag.StringVar(&dispatchAgentName, "dispatch-agent-name", "", "Dispatch agent name.")
	flag.StringVar(&dispatchQueueLane, "dispatch-queue-lane", "", "Dispatch queue lane sent to next-task.")
	flag.StringVar(&dispatchLane, "dispatch-lane", "", "LaneProfile assigned to discovered Dispatch work.")
	flag.Var(&dispatchLaneBindings, "dispatch-lane-binding", "Repeatable Dispatch lane binding <queueLane>:<laneProfile>; one discovery runner per binding.")
	flag.DurationVar(&dispatchPollInterval, "dispatch-poll-interval", 30*time.Second, "Dispatch discovery poll interval.")
	flag.DurationVar(&dispatchHTTPTimeout, "dispatch-http-timeout", 30*time.Second, "Dispatch HTTP request timeout.")
	flag.StringVar(&evidenceIntakeKeySecret, "evidence-intake-key-secret", "", "Name of the Secret in the operator namespace holding the evidence intake HMAC key under the \"key\" entry; empty with no --evidence-intake-service disables evidence wiring.")
	flag.StringVar(&evidenceIntakeService, "evidence-intake-service", "", "URL coordinator pods POST failure evidence to; required with --evidence-intake-key-secret.")
	flag.StringVar(&evidenceIntakeBind, "evidence-intake-bind", "", "Address the operator's own evidence-intake server binds to in-process (for example :8082); the chart derives the matching --evidence-intake-service URL. Empty leaves the operator-side intake off, and a non-empty value requires --evidence-intake-key-secret.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "courier.misospace.dev",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Per-lane capacity and suspension gauges are read from the live cluster
	// at scrape time, so they need only the manager's client.
	crmetrics.Registry.MustRegister(controller.NewLaneCollector(mgr.GetClient()))

	bootstrapProfilesFile := strings.TrimSpace(bootstrapLaneProfilesFile)
	if bootstrapProfilesFile != "" {
		podNamespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
		if podNamespace == "" {
			setupLog.Error(fmt.Errorf("POD_NAMESPACE is empty"), "unable to configure bootstrap LaneProfiles")
			os.Exit(1)
		}
		profiles, err := bootstrap.LoadFile(bootstrapProfilesFile)
		if err != nil {
			setupLog.Error(err, "unable to load bootstrap LaneProfiles")
			os.Exit(1)
		}
		if err := mgr.Add(&bootstrap.Reconciler{
			Client:    mgr.GetClient(),
			Namespace: podNamespace,
			Provider:  &bootstrap.FileProvider{Path: bootstrapProfilesFile},
		}); err != nil {
			setupLog.Error(err, "unable to add bootstrap LaneProfile reconciler")
			os.Exit(1)
		}
		setupLog.Info("bootstrap LaneProfiles configured", "profiles", len(profiles), "namespace", podNamespace)
	}

	podConfig := executor.DefaultPodConfig()
	podConfig.Image = executorImage
	podConfig.GitRemoteURL = gitRemoteTemplate
	podConfig.GitCredentialSecret = gitCredentialSecret
	podConfig.GitTokenKey = gitTokenKey
	podConfig.GitHubCredentialSecret = githubCredentialSecret
	podConfig.GitHubTokenKey = githubTokenKey
	podConfig.EnvironmentSecret = executorEnvironmentSecret
	podConfig.OpenCode.Agent = opencodeAgent
	podConfig.GitHubMCPURL = githubMCPURL
	podConfig.Context7MCPURL = context7MCPURL
	podConfig.MetricsMCPURL = metricsMCPURL
	launcher := &controller.CoordinatorLauncher{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Pod:    podConfig,
	}

	evidenceSecretSet := strings.TrimSpace(evidenceIntakeKeySecret) != ""
	evidenceBindSet := strings.TrimSpace(evidenceIntakeBind) != ""
	if err := validateEvidenceFlags(evidenceIntakeKeySecret, evidenceIntakeService, evidenceIntakeBind); err != nil {
		setupLog.Error(err, "unable to configure evidence intake")
		os.Exit(1)
	}
	var evidenceIntake *controller.EvidenceIntake
	if evidenceSecretSet {
		if err := executor.ValidateEvidenceURL(evidenceIntakeService); err != nil {
			setupLog.Error(err, "unable to configure evidence capture")
			os.Exit(1)
		}
		podNamespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
		key, err := loadEvidenceKey(context.Background(), mgr.GetAPIReader(), podNamespace, evidenceIntakeKeySecret)
		if err != nil {
			setupLog.Error(err, "unable to configure evidence capture")
			os.Exit(1)
		}
		launcher.EvidenceURL = strings.TrimSpace(evidenceIntakeService)
		launcher.EvidenceKey = key
		evidenceIntake = &controller.EvidenceIntake{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
			Key:       key,
			Pod:       podConfig,
			Bind:      strings.TrimSpace(evidenceIntakeBind),
		}
		if evidenceBindSet {
			setupLog.Info("evidence intake server enabled", "bind", strings.TrimSpace(evidenceIntakeBind), "service", strings.TrimSpace(evidenceIntakeService))
		} else {
			setupLog.Info("evidence capture wiring configured", "namespace", podNamespace, "secret", strings.TrimSpace(evidenceIntakeKeySecret))
			// Legal but inert: the URL still reaches coordinator pods while no
			// listener answers, so every capture would spend its delivery
			// attempts on nothing. Say so plainly at startup.
			setupLog.Info("evidence intake listener disabled: --evidence-intake-bind is empty, coordinator capture POSTs will be refused", "service", strings.TrimSpace(evidenceIntakeService))
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	githubObserver, err := githubObserver(context.Background(), mgr.GetAPIReader(), os.Getenv("POD_NAMESPACE"), githubCredentialSecret, githubTokenKey, gitCredentialSecret, gitTokenKey)
	if err != nil {
		setupLog.Error(err, "unable to configure GitHub observer")
		os.Exit(1)
	}
	if githubObserver == nil {
		setupLog.Info("GitHub observer disabled; no API credential Secret configured")
	}
	// Structured run events go to stdout as JSON lines alongside
	// controller-runtime's ordinary operational logs; the deployment's log
	// collection stack is the transport. Verbosity is per run: the
	// reconciler marks each event with the run's own spec.debug flag
	// (Event.Verbose), so one shared emitter never turns one run's debug
	// setting into another run's noise.
	runEvents := courierlog.NewEmitter(os.Stdout, courierlog.ParseLevel(os.Getenv("COURIER_LOG_LEVEL")))

	sources := controller.NewSourceRegistry(map[string]source.Adapter{
		"manual": manual.Adapter{},
	})
	if dispatchEnabled {
		if strings.TrimSpace(dispatchBaseURL) == "" || strings.TrimSpace(dispatchAgentName) == "" {
			setupLog.Error(fmt.Errorf("dispatch requires base URL and agent name"), "unable to configure Dispatch source")
			os.Exit(1)
		}
		bindings, err := resolveDispatchBindings(dispatchQueueLane, dispatchLane, dispatchLaneBindings)
		if err != nil {
			setupLog.Error(err, "unable to configure Dispatch source")
			os.Exit(1)
		}
		if len(bindings) == 0 {
			setupLog.Error(fmt.Errorf("dispatch requires base URL, agent name, and at least one queue-lane/LaneProfile binding"), "unable to configure Dispatch source")
			os.Exit(1)
		}
		namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
		if namespace == "" {
			setupLog.Error(fmt.Errorf("POD_NAMESPACE is empty"), "unable to configure Dispatch source")
			os.Exit(1)
		}
		token := strings.TrimSpace(os.Getenv("DISPATCH_AGENT_TOKEN"))
		if token == "" {
			setupLog.Error(fmt.Errorf("DISPATCH_AGENT_TOKEN is empty"), "unable to configure Dispatch source")
			os.Exit(1)
		}
		checker, err := dispatchPRStateChecker(githubObserver)
		if err != nil {
			setupLog.Error(err, "unable to configure Dispatch source: GitHub pull request state lookup is unavailable")
			os.Exit(1)
		}
		// Lifecycle calls are lane-agnostic; per-binding runners do lane-scoped discovery.
		dispatchClient, err := dispatch.NewClient(dispatchBaseURL, dispatchAgentName, token, dispatchHTTPTimeout)
		if err != nil {
			setupLog.Error(err, "unable to configure Dispatch client")
			os.Exit(1)
		}
		dispatchClient.WithPullRequestStateChecker(checker)
		sources.Register("dispatch", dispatch.New(dispatchClient))
		for _, binding := range bindings {
			runnerClient, err := dispatch.NewClientWithLane(dispatchBaseURL, dispatchAgentName, binding.queueLane, token, dispatchHTTPTimeout)
			if err != nil {
				setupLog.Error(err, "unable to configure Dispatch client", "queueLane", binding.queueLane, "laneProfile", binding.laneProfile)
				os.Exit(1)
			}
			runnerClient.WithPullRequestStateChecker(checker)
			runner := source.NewRunner(mgr.GetClient(), dispatch.New(runnerClient), source.RunnerConfig{
				Source:       "dispatch",
				LaneProfile:  binding.laneProfile,
				Namespace:    namespace,
				PollInterval: dispatchPollInterval,
			})
			if err := mgr.Add(runner); err != nil {
				setupLog.Error(err, "unable to add Dispatch discovery runner")
				os.Exit(1)
			}
		}
	}

	reconciler := &controller.CoderRunReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		APIReader:      mgr.GetAPIReader(),
		Launch:         launcher.Launch,
		Sources:        sources,
		StatusWriter:   status.KubePatchWriter{Client: mgr.GetClient()},
		Observer:       githubObserver,
		Events:         runEvents,
		Evidence:       evidenceIntake,
		PRHeadResolver: existingPRHeadResolver(githubObserver),
	}
	if secureMode {
		secure, err := buildSecureControl(secureConfig{
			providersFile:    forgeProvidersFile,
			runNamespace:     runNamespace,
			harnessImage:     harnessImage,
			probeImage:       probeImage,
			cacheService:     dependencyCacheService,
			cachePort:        dependencyCachePort,
			gatewayURL:       modelGatewayURL,
			gatewayKeySecret: modelGatewayKeySecret,
			gatewayKeyName:   modelGatewayKeyName,
			gatewayCIDR:      modelGatewayCIDR,
			gatewayPort:      modelGatewayPort,
			observer:         githubObserver,
			legacyGitSecret:  gitCredentialSecret,
			legacyAPISecret:  githubCredentialSecret,
			operatorNS:       os.Getenv("POD_NAMESPACE"),
			restConfig:       mgr.GetConfig(),
		})
		if err != nil {
			setupLog.Error(err, "unable to configure secure mode")
			os.Exit(1)
		}
		secure.Client = mgr.GetClient()
		secure.Patcher = reconciler
		reconciler.Secure = secure
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CoderRun")
		os.Exit(1)
	}

	// The operator's own intake server runs in-process on every replica (it
	// takes no leader election): it persists the evidence a coordinator
	// POSTs, and the same intake drives the reconciler's evidence lifecycle.
	if evidenceBindSet {
		if err := mgr.Add(evidenceIntake); err != nil {
			setupLog.Error(err, "unable to add evidence intake server")
			os.Exit(1)
		}
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func existingPRHeadResolver(observer controller.WorldObserver) controller.ExistingPRHeadResolver {
	if observer == nil {
		return nil
	}
	resolver, _ := observer.(controller.ExistingPRHeadResolver)
	return resolver
}

// validateEvidenceFlags reports a clear error when the evidence intake flags
// are inconsistent. The key Secret and the service URL come as a pair (one is
// useless without the other), and the operator's own intake listener (bind)
// cannot run without the HMAC key Secret it authenticates with. All three
// empty means evidence wiring is disabled, which is valid.
func validateEvidenceFlags(keySecret, service, bind string) error {
	secretSet := strings.TrimSpace(keySecret) != ""
	serviceSet := strings.TrimSpace(service) != ""
	bindSet := strings.TrimSpace(bind) != ""
	if secretSet != serviceSet {
		return fmt.Errorf("evidence intake requires both --evidence-intake-key-secret and --evidence-intake-service")
	}
	if bindSet && !secretSet {
		return fmt.Errorf("--evidence-intake-bind requires --evidence-intake-key-secret")
	}
	return nil
}

// dispatchBinding pairs one Dispatch queue lane with one Courier LaneProfile.
type dispatchBinding struct {
	queueLane   string
	laneProfile string
}

// resolveDispatchBindings normalizes the single-binding shorthand and the
// repeatable bindings into the final list, rejecting ambiguous input and
// duplicate queue lanes.
func resolveDispatchBindings(queueLane, laneProfile string, repeated []string) ([]dispatchBinding, error) {
	shorthandSet := queueLane != "" || laneProfile != ""
	if len(repeated) > 0 && shorthandSet {
		return nil, fmt.Errorf("dispatch: --dispatch-queue-lane/--dispatch-lane cannot be combined with --dispatch-lane-binding")
	}
	bindings := make([]dispatchBinding, 0, 1+len(repeated))
	if shorthandSet {
		if strings.TrimSpace(queueLane) == "" || strings.TrimSpace(laneProfile) == "" {
			return nil, fmt.Errorf("dispatch: both --dispatch-queue-lane and --dispatch-lane are required for a single binding")
		}
		bindings = append(bindings, dispatchBinding{
			queueLane:   strings.TrimSpace(queueLane),
			laneProfile: strings.TrimSpace(laneProfile),
		})
	}
	seenLanes := make(map[string]struct{}, len(bindings)+len(repeated))
	for _, binding := range bindings {
		seenLanes[binding.queueLane] = struct{}{}
	}
	for _, value := range repeated {
		parts := strings.SplitN(value, ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" || (len(parts) == 2 && strings.Contains(parts[1], ":")) {
			return nil, fmt.Errorf("dispatch: invalid lane binding %q: want <queueLane>:<laneProfile> with no colons in either half", value)
		}
		binding := dispatchBinding{
			queueLane:   strings.TrimSpace(parts[0]),
			laneProfile: strings.TrimSpace(parts[1]),
		}
		if _, ok := seenLanes[binding.queueLane]; ok {
			return nil, fmt.Errorf("dispatch: duplicate queue lane %q", binding.queueLane)
		}
		seenLanes[binding.queueLane] = struct{}{}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func dispatchPRStateChecker(observer controller.WorldObserver) (dispatch.PullRequestStateChecker, error) {
	githubObserver, ok := observer.(couriergithub.Observer)
	if !ok || githubObserver.Client == nil {
		return nil, dispatch.ErrPRStateCheckerNeeded
	}
	return dispatch.PullRequestStateCheckerFunc(func(ctx context.Context, repo string, number int) (dispatch.PullRequestState, error) {
		parts := strings.Split(repo, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return dispatch.PullRequestState{}, fmt.Errorf("invalid repository %q", repo)
		}
		pull, err := githubObserver.Client.GetPullRequest(ctx, parts[0], parts[1], number)
		if err != nil {
			return dispatch.PullRequestState{}, err
		}
		return dispatch.PullRequestState{State: pull.State, Merged: strings.TrimSpace(pull.MergedAt) != ""}, nil
	}), nil
}

func githubObserver(ctx context.Context, reader client.Reader, namespace, configuredSecret, configuredKey, gitSecret, gitKey string) (controller.WorldObserver, error) {
	secretName := strings.TrimSpace(configuredSecret)
	if secretName == "" {
		secretName = strings.TrimSpace(gitSecret)
	}
	if secretName == "" {
		return nil, nil
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("POD_NAMESPACE is required when a GitHub credential Secret is configured")
	}
	secretKey := strings.TrimSpace(configuredKey)
	if secretKey == "" {
		secretKey = strings.TrimSpace(gitKey)
	}
	if secretKey == "" {
		secretKey = "token"
	}
	var secret corev1.Secret
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("GitHub credential Secret %s/%s: %w", namespace, secretName, err)
		}
		return nil, fmt.Errorf("read GitHub credential Secret %s/%s: %w", namespace, secretName, err)
	}
	token, ok := secret.Data[secretKey]
	if !ok || strings.TrimSpace(string(token)) == "" {
		return nil, fmt.Errorf("GitHub credential Secret %s/%s has no non-empty %q key", namespace, secretName, secretKey)
	}
	githubClient, err := couriergithub.New(string(token))
	if err != nil {
		return nil, err
	}
	return couriergithub.Observer{Client: githubClient}, nil
}

// loadEvidenceKey reads the evidence intake HMAC key from the named Secret in
// the operator namespace. It fails closed: the intake and the launcher must
// agree on one key, so a missing or empty key is a startup error rather than a
// run that can never persist evidence. The key value never appears in any
// error.
func loadEvidenceKey(ctx context.Context, reader client.Reader, namespace, name string) ([]byte, error) {
	secretName := strings.TrimSpace(name)
	if secretName == "" {
		return nil, fmt.Errorf("evidence intake key Secret name is required")
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("POD_NAMESPACE is required when an evidence intake key Secret is configured")
	}
	var secret corev1.Secret
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("evidence intake key Secret %s/%s not found: %w", namespace, secretName, err)
		}
		return nil, fmt.Errorf("read evidence intake key Secret %s/%s: %w", namespace, secretName, err)
	}
	key, ok := secret.Data["key"]
	if !ok || strings.TrimSpace(string(key)) == "" {
		return nil, fmt.Errorf("evidence intake key Secret %s/%s has no non-empty %q entry", namespace, secretName, "key")
	}
	return key, nil
}

// secureConfig carries the parsed secure-mode flags into buildSecureControl.
type secureConfig struct {
	providersFile    string
	runNamespace     string
	harnessImage     string
	probeImage       string
	cacheService     string
	cachePort        int
	gatewayURL       string
	gatewayKeySecret string
	gatewayKeyName   string
	gatewayCIDR      string
	gatewayPort      int
	observer         controller.WorldObserver
	legacyGitSecret  string
	legacyAPISecret  string
	operatorNS       string
	restConfig       *rest.Config
}

const (
	defaultsHarnessImage = "ghcr.io/misospace/courier-harness:latest"
	defaultsProbeImage   = "busybox:1.36"
)

// buildSecureControl loads and validates the provider registry, rejects
// registrations that reference a credential exposed to legacy pods (the §9
// taint rule, enforced where the deployment configuration is knowable), and
// assembles the secure control.
func buildSecureControl(config secureConfig) (*controller.SecureControl, error) {
	if strings.TrimSpace(config.providersFile) == "" {
		return nil, fmt.Errorf("secure mode requires --forge-providers-file")
	}
	if strings.TrimSpace(config.runNamespace) == "" {
		return nil, fmt.Errorf("secure mode requires --run-namespace")
	}
	data, err := os.ReadFile(config.providersFile)
	if err != nil {
		return nil, fmt.Errorf("read forge providers file: %w", err)
	}
	registry, err := forge.Load(data, "github")
	if err != nil {
		return nil, err
	}
	tainted := map[string]bool{
		strings.TrimSpace(config.legacyGitSecret): true,
		strings.TrimSpace(config.legacyAPISecret): true,
	}
	for _, registration := range registry.Registrations() {
		for purpose, ref := range registration.Credentials {
			if tainted[ref.SecretName] {
				return nil, fmt.Errorf(
					"provider %q credential purpose %q references Secret %q, which is exposed to legacy coordinator pods: a credential exposed to any legacy pod is tainted and must not back a secure registration",
					registration.Name, purpose, ref.SecretName)
			}
		}
	}
	observer, ok := config.observer.(couriergithub.Observer)
	if !ok || observer.Client == nil {
		return nil, fmt.Errorf("secure mode requires a GitHub observer credential for admission-time provider reads")
	}
	if config.restConfig == nil {
		return nil, fmt.Errorf("secure mode requires an API server connection for access reviews")
	}
	clientset, err := kubernetes.NewForConfig(config.restConfig)
	if err != nil {
		return nil, fmt.Errorf("build access-review client: %w", err)
	}
	harnessImage := strings.TrimSpace(config.harnessImage)
	if harnessImage == "" {
		harnessImage = defaultsHarnessImage
	}
	probeImage := strings.TrimSpace(config.probeImage)
	if probeImage == "" {
		probeImage = defaultsProbeImage
	}
	return &controller.SecureControl{
		Config: controller.SecureConfig{
			RunNamespace:      strings.TrimSpace(config.runNamespace),
			Registry:          registry,
			HarnessImage:      harnessImage,
			ProbeImage:        probeImage,
			CacheService:      strings.TrimSpace(config.cacheService),
			CachePort:         int32(config.cachePort),
			GatewayCIDR:       strings.TrimSpace(config.gatewayCIDR),
			GatewayPort:       int32(config.gatewayPort),
			GatewayURL:        strings.TrimSpace(config.gatewayURL),
			GatewayKeySecret:  strings.TrimSpace(config.gatewayKeySecret),
			GatewayKeyName:    strings.TrimSpace(config.gatewayKeyName),
			LiveProbes:        true,
			Kubernetes:        clientset,
			OperatorNamespace: strings.TrimSpace(config.operatorNS),
			Providers: func(ctx context.Context, registration *forge.Registration) (controller.AdmissionProvider, error) {
				// Admission reads use the operator's own read identity. The
				// registration's credential reference is never resolved here.
				provider := couriergithub.NewProvider(
					forge.ProviderConfig{Name: registration.Name, Endpoint: registration.Endpoint},
					observer.Client)
				return controller.AdmissionProvider{Provider: provider, Policy: provider, HeadLister: provider}, nil
			},
		},
	}, nil
}
