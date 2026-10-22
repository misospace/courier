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
	"github.com/misospace/courier/internal/connector"
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
	var gitAuthorName string
	var gitAuthorEmail string
	var gitCommitterName string
	var gitCommitterEmail string
	var opencodeAgent string
	var githubMCPURL string
	var context7MCPURL string
	var metricsMCPURL string
	var dispatchEnabled bool
	var dispatchPRFixExclusions bool
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
	var doneRetention time.Duration
	var failedRetention time.Duration
	var needsHumanRetention time.Duration
	var terminalAuditKeep int
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
	flag.StringVar(&gitAuthorName, "git-author-name", defaults.Git.AuthorName, "Git author name rendered into operator-created coordinator pods; empty falls back to the explicit default. Separate from any forge authentication identity (#317).")
	flag.StringVar(&gitAuthorEmail, "git-author-email", defaults.Git.AuthorEmail, "Git author email rendered into operator-created coordinator pods; empty falls back to the explicit default. Forge attribution requires an account-associated email (#317).")
	flag.StringVar(&gitCommitterName, "git-committer-name", defaults.Git.CommitterName, "Git committer name rendered into operator-created coordinator pods; empty falls back to the explicit default (#317).")
	flag.StringVar(&gitCommitterEmail, "git-committer-email", defaults.Git.CommitterEmail, "Git committer email rendered into operator-created coordinator pods; empty falls back to the explicit default (#317).")
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
	flag.BoolVar(&dispatchPRFixExclusions, "dispatch-pr-fix-exclusions", false, "Enable Dispatch PR-fix exclusion acknowledgements.")
	flag.StringVar(&dispatchBaseURL, "dispatch-base-url", "", "Dispatch base URL.")
	flag.StringVar(&dispatchAgentName, "dispatch-agent-name", "", "Dispatch agent name.")
	flag.StringVar(&dispatchQueueLane, "dispatch-queue-lane", "", "Dispatch queue lane sent to next-task.")
	flag.StringVar(&dispatchLane, "dispatch-lane", "", "LaneProfile assigned to discovered Dispatch work.")
	flag.Var(&dispatchLaneBindings, "dispatch-lane-binding", "Repeatable Dispatch binding <queueLane>:<laneProfile>[:<agentName>]; one discovery runner per binding.")
	flag.DurationVar(&dispatchPollInterval, "dispatch-poll-interval", 30*time.Second, "Dispatch discovery poll interval.")
	flag.DurationVar(&dispatchHTTPTimeout, "dispatch-http-timeout", 30*time.Second, "Dispatch HTTP request timeout.")
	flag.StringVar(&evidenceIntakeKeySecret, "evidence-intake-key-secret", "", "Name of the Secret in the operator namespace holding the evidence intake HMAC key under the \"key\" entry; empty with no --evidence-intake-service disables evidence wiring.")
	flag.StringVar(&evidenceIntakeService, "evidence-intake-service", "", "URL coordinator pods POST failure evidence to; required with --evidence-intake-key-secret.")
	flag.StringVar(&evidenceIntakeBind, "evidence-intake-bind", "", "Address the operator's own evidence-intake server binds to in-process (for example :8082); the chart derives the matching --evidence-intake-service URL. Empty leaves the operator-side intake off, and a non-empty value requires --evidence-intake-key-secret.")
	flag.DurationVar(&doneRetention, "done-retention", 5*time.Minute, "How long a resolved Done CoderRun is kept before the retention reaper deletes it.")
	flag.DurationVar(&failedRetention, "failed-retention", 168*time.Hour, "How long a Failed CoderRun is kept, once its lifecycle report has settled, before the retention reaper deletes it.")
	flag.DurationVar(&needsHumanRetention, "needs-human-retention", 168*time.Hour, "How long a NeedsHuman CoderRun is kept, once its lifecycle report has settled, before the retention reaper deletes it.")
	flag.IntVar(&terminalAuditKeep, "terminal-audit-keep", 5, "Number of most recent terminal CoderRuns per repository the retention reaper always retains, even past their retention window.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// The pre-start credential Secret reads below use the manager's API
	// reader. An API server that accepts the connection but never answers
	// would otherwise wedge startup forever with no log line, so both reads
	// share one deadline (in the same spirit as --dispatch-http-timeout).
	// A timeout fails closed like every other setup path; it adds no retry.
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), startupReadTimeout)
	defer cancelStartup()

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
	// The oldest retained terminal-run age is likewise read from the live
	// cluster at scrape time, so the retention reaper's observability half
	// needs only the manager's client.
	crmetrics.Registry.MustRegister(controller.NewTerminalAgeCollector(mgr.GetClient()))
	// The terminal-run backlog (held runs and the unsettled lifecycle-report
	// queue they wait on) is likewise read from the live cluster at scrape
	// time.
	crmetrics.Registry.MustRegister(controller.NewTerminalBacklogCollector(mgr.GetClient()))

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
	podConfig.Git = executor.GitIdentity{
		AuthorName:     gitAuthorName,
		AuthorEmail:    gitAuthorEmail,
		CommitterName:  gitCommitterName,
		CommitterEmail: gitCommitterEmail,
	}
	// Fail fast on a malformed deployment identity at startup rather than at
	// the first launch; Build validates the identity again on every pod.
	if err := podConfig.Git.Validate(); err != nil {
		setupLog.Error(err, "unable to configure executor git identity")
		os.Exit(1)
	}
	podConfig.OpenCode.Agent = opencodeAgent
	podConfig.GitHubMCPURL = githubMCPURL
	podConfig.Context7MCPURL = context7MCPURL
	podConfig.MetricsMCPURL = metricsMCPURL
	launcher := &controller.CoordinatorLauncher{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Pod:       podConfig,
	}
	if err := launcher.Validate(); err != nil {
		setupLog.Error(err, "unable to configure coordinator launcher")
		os.Exit(1)
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
		key, err := loadEvidenceKey(startupCtx, mgr.GetAPIReader(), podNamespace, evidenceIntakeKeySecret)
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

	// The provider registry is the source of truth for where forge reads go.
	// Load it once before the observer so the operator's own observation reads
	// route through the same settled registrations as secure admission (#377);
	// a malformed registry fails startup, not a run. Outside secure mode there
	// is no registry and the observer keeps its GitHub.com default client.
	var forgeRegistry *forge.Registry
	if secureMode && strings.TrimSpace(forgeProvidersFile) != "" {
		forgeRegistry, err = loadForgeRegistry(forgeProvidersFile)
		if err != nil {
			setupLog.Error(err, "unable to load forge provider registry")
			os.Exit(1)
		}
	}

	githubObserver, err := githubObserver(startupCtx, mgr.GetAPIReader(), os.Getenv("POD_NAMESPACE"), githubCredentialSecret, githubTokenKey, gitCredentialSecret, gitTokenKey, forgeRegistry)
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

	// Connector diagnostics (docs/connector-diagnostics.md) share the run
	// event emitter for their log half and the manager's metric registry for
	// their metric half. The envelope is generic: core wires it, but the
	// signal vocabularies and capability declarations belong to each adapter.
	diagnostics := connector.NewEmitter(crmetrics.Registry, connector.NewEventLogSink(runEvents))

	sources := controller.NewSourceRegistry(map[string]source.Adapter{
		"manual": manual.New(diagnostics),
	})
	if dispatchEnabled {
		if strings.TrimSpace(dispatchBaseURL) == "" || strings.TrimSpace(dispatchAgentName) == "" {
			setupLog.Error(fmt.Errorf("dispatch requires base URL and agent name"), "unable to configure Dispatch source")
			os.Exit(1)
		}
		bindings, err := resolveDispatchBindings(dispatchAgentName, dispatchQueueLane, dispatchLane, dispatchLaneBindings)
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
		// The bare adapter remains registered for lifecycle compatibility with
		// retained runs created before source-agent routing was introduced.
		legacyClient, err := dispatch.NewClientWithLane(dispatchBaseURL, strings.TrimSpace(dispatchAgentName), "", token, dispatchHTTPTimeout)
		if err != nil {
			setupLog.Error(err, "unable to configure legacy Dispatch client")
			os.Exit(1)
		}
		legacyClient.WithPullRequestStateChecker(checker)
		if dispatchPRFixExclusions {
			legacyClient.WithPRFixExclusions(true)
		}
		sources.Register("dispatch", dispatch.New(legacyClient, dispatch.WithConnectorDiagnostics(diagnostics)))
		// Discovery metrics are process-wide and registered once; each
		// binding's runner emits into them nil-safely via the shared instance.
		discoveryMetrics := source.NewDiscoveryMetrics(crmetrics.Registry)
		for _, binding := range bindings {
			bindingClient, err := dispatch.NewClientWithLane(dispatchBaseURL, binding.agentName, binding.queueLane, token, dispatchHTTPTimeout)
			if err != nil {
				setupLog.Error(err, "unable to configure Dispatch client", "queueLane", binding.queueLane, "laneProfile", binding.laneProfile, "agentName", binding.agentName)
				os.Exit(1)
			}
			bindingClient.WithPullRequestStateChecker(checker)
			if dispatchPRFixExclusions {
				bindingClient.WithPRFixExclusions(true)
			}
			opts := []dispatch.Option{dispatch.WithConnectorDiagnostics(diagnostics)}
			if binding.sourceAgent != "" {
				// Each configured adapter reports under its own bounded
				// identity so independent connectors never share metric
				// series.
				opts = append(opts, dispatch.WithConnectorName("dispatch:"+binding.sourceAgent))
			}
			adapter := dispatch.New(bindingClient, opts...)
			if binding.sourceAgent != "" {
				sources.Register("dispatch:"+binding.sourceAgent, adapter)
			}
			runner := source.NewRunner(mgr.GetClient(), adapter, source.RunnerConfig{
				Source:       "dispatch",
				SourceAgent:  binding.sourceAgent,
				LaneProfile:  binding.laneProfile,
				Namespace:    namespace,
				PollInterval: dispatchPollInterval,
			})
			runner.Metrics = discoveryMetrics
			if err := mgr.Add(runner); err != nil {
				setupLog.Error(err, "unable to add Dispatch discovery runner", "queueLane", binding.queueLane)
				os.Exit(1)
			}
			setupLog.Info("registered Dispatch binding", "queueLane", binding.queueLane, "laneProfile", binding.laneProfile, "agentName", binding.agentName)
		}
	}

	reconciler := &controller.CoderRunReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		APIReader:           mgr.GetAPIReader(),
		Launch:              launcher.Launch,
		Sources:             sources,
		StatusWriter:        status.KubePatchWriter{Client: mgr.GetClient()},
		Observer:            githubObserver,
		PullRequests:        pullRequestReader(githubObserver),
		Events:              runEvents,
		Evidence:            evidenceIntake,
		PRHeadResolver:      existingPRHeadResolver(githubObserver),
		DoneRetention:       doneRetention,
		FailedRetention:     failedRetention,
		NeedsHumanRetention: needsHumanRetention,
		TerminalAuditKeep:   terminalAuditKeep,
	}
	if secureMode {
		secure, err := buildSecureControl(secureConfig{
			providersFile:    forgeProvidersFile,
			registry:         forgeRegistry,
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
		secure.MaxRestarts = reconciler.MaxRestarts
		secure.APIReader = mgr.GetAPIReader()
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

// pullRequestReader gives the reconciler's slow post-review observation the
// forge capability it reads through: the exact pull-request read of the
// configured provider, built on the operator's own read credential. The core
// never receives a forge-specific client or CLI — only the forge.Provider
// seam and its capability diagnostics. The GitHub observer routes each read
// through the endpoint of the registration serving the pull request's
// repository (#377), so the reader is the observer itself.
func pullRequestReader(observer controller.WorldObserver) controller.PullRequestReader {
	githubObserver, ok := observer.(couriergithub.Observer)
	if !ok || githubObserver.Client == nil {
		return nil
	}
	return githubObserver
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
// agentName is the per-binding Dispatch identity. It defaults to the global
// --dispatch-agent-name in single-binding shorthand and is auto-derived per
// binding as <global>-<queueLane> in the list form, so distinct bindings do
// not share an agent identity in Dispatch and the lease-exclusion check
// (`findLeasedIssueIds(agentName).where.agentName != agentName`) actually
// excludes a worker's own in-flight work from its sibling pollers.
type dispatchBinding struct {
	queueLane   string
	laneProfile string
	agentName   string
	sourceAgent string
}

// resolveDispatchBindings normalizes the single-binding shorthand and repeated
// bindings. Multiple bindings retain the historical <base>-<queue> default;
// an explicit third segment lets deployments choose a stable agent identity.
// A one-binding repeated form leaves SourceAgent empty, preserving the legacy
// bare source identity just like the shorthand.
func resolveDispatchBindings(baseAgentName, queueLane, laneProfile string, repeated []string) ([]dispatchBinding, error) {
	shorthandSet := queueLane != "" || laneProfile != ""
	if len(repeated) > 0 && shorthandSet {
		return nil, fmt.Errorf("dispatch: --dispatch-queue-lane/--dispatch-lane cannot be combined with --dispatch-lane-binding")
	}
	bindings := make([]dispatchBinding, 0, 1+len(repeated))
	derive := len(repeated) > 1
	if shorthandSet {
		if strings.TrimSpace(queueLane) == "" || strings.TrimSpace(laneProfile) == "" {
			return nil, fmt.Errorf("dispatch: both --dispatch-queue-lane and --dispatch-lane are required for a single binding")
		}
		bindings = append(bindings, dispatchBinding{queueLane: strings.TrimSpace(queueLane), laneProfile: strings.TrimSpace(laneProfile), agentName: strings.TrimSpace(baseAgentName)})
	}
	seenAgents := make(map[string]struct{}, len(repeated)+len(bindings))
	for _, binding := range bindings {
		seenAgents[binding.agentName] = struct{}{}
	}
	for _, value := range repeated {
		parts := strings.Split(value, ":")
		if (len(parts) != 2 && len(parts) != 3) || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" || (len(parts) == 3 && strings.TrimSpace(parts[2]) == "") {
			return nil, fmt.Errorf("dispatch: invalid lane binding %q: want <queueLane>:<laneProfile>[:<agentName>]", value)
		}
		binding := dispatchBinding{queueLane: strings.TrimSpace(parts[0]), laneProfile: strings.TrimSpace(parts[1])}
		if len(parts) == 3 {
			binding.agentName = strings.TrimSpace(parts[2])
			binding.sourceAgent = binding.agentName
		} else if derive {
			binding.agentName = strings.TrimSpace(baseAgentName) + "-" + binding.queueLane
			binding.sourceAgent = binding.agentName
		} else {
			binding.agentName = strings.TrimSpace(baseAgentName)
		}
		if !validDispatchAgentName(binding.agentName) {
			return nil, fmt.Errorf("dispatch: invalid agent name %q: use letters, digits, dot, underscore, or hyphen", binding.agentName)
		}
		// The composed connector identity "dispatch:<agent>" must fit the
		// adapter's bounded connector label, otherwise startup would panic
		// inside dispatch.New instead of failing fast with a setup error.
		if len("dispatch:"+binding.agentName) > dispatch.MaxConnectorNameLength {
			return nil, fmt.Errorf("dispatch: agent name %q exceeds %d characters", binding.agentName, dispatch.MaxConnectorNameLength-len("dispatch:"))
		}
		if _, ok := seenAgents[binding.agentName]; ok {
			return nil, fmt.Errorf("dispatch: duplicate agent identity %q", binding.agentName)
		}
		seenAgents[binding.agentName] = struct{}{}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func validDispatchAgentName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
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
		// Resolve the destination per repository through the loaded registry,
		// so a Dispatch-tracked pull request on an Enterprise registration is
		// read from that registration's host, never the GitHub.com default (#377).
		client, err := githubObserver.ClientForRepository(repo)
		if err != nil {
			return dispatch.PullRequestState{}, err
		}
		pull, err := client.GetPullRequest(ctx, parts[0], parts[1], number)
		if err != nil {
			return dispatch.PullRequestState{}, err
		}
		return dispatch.PullRequestState{State: pull.State, Merged: strings.TrimSpace(pull.MergedAt) != ""}, nil
	}), nil
}

// githubObserver builds the operator's own forge read identity. The registry
// (nil outside secure mode) is carried onto the observer so every observation
// read — post-review PR state, existing-PR head resolution, Dispatch PR state,
// and merge observation — resolves its destination through the registration
// serving the repository rather than the GitHub.com default (#377).
func githubObserver(ctx context.Context, reader client.Reader, namespace, configuredSecret, configuredKey, gitSecret, gitKey string, registry *forge.Registry) (controller.WorldObserver, error) {
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
	return couriergithub.Observer{Client: githubClient, Registrations: registry}, nil
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
	registry         *forge.Registry
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
	// defaultsProbeImage pins the network-probe image by digest. The probe
	// decides whether a node passes preflight, so a republished tag must not
	// change the code that decides it. The chart's secure.probeImage must
	// carry the same reference; cmd/main_test.go fails the build when the
	// two drift apart or either loses its digest.
	defaultsProbeImage = "busybox:1.38@sha256:fd7dc98638c8e305f4dc34e979f1c0fdfdcaeb0fbf8fcff77ae834b6da3d7e6e"
	// startupReadTimeout bounds the pre-start credential Secret reads so a
	// stalled API server surfaces as a setup error instead of a silent hang.
	startupReadTimeout = 30 * time.Second
)

// loadForgeRegistry reads and validates the deployment's provider registry
// file. The registry is loaded once in main so the same settled configuration
// routes both secure admission and the operator's own observation reads (#377).
func loadForgeRegistry(path string) (*forge.Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read forge providers file: %w", err)
	}
	return forge.Load(data, "github")
}

// buildSecureControl validates the provider registry (preloaded by main, or
// loaded from the providers file for callers that wire it directly), rejects
// registrations that reference a credential exposed to legacy pods (the §9
// taint rule, enforced where the deployment configuration is knowable), and
// assembles the secure control.
func buildSecureControl(config secureConfig) (*controller.SecureControl, error) {
	if config.registry == nil && strings.TrimSpace(config.providersFile) == "" {
		return nil, fmt.Errorf("secure mode requires --forge-providers-file")
	}
	if strings.TrimSpace(config.runNamespace) == "" {
		return nil, fmt.Errorf("secure mode requires --run-namespace")
	}
	registry := config.registry
	if registry == nil {
		var err error
		registry, err = loadForgeRegistry(config.providersFile)
		if err != nil {
			return nil, err
		}
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
				// Each registration's endpoint decides the destination: the
				// client is rebuilt against it rather than reused from the
				// observer's GitHub.com default, so a GitHub Enterprise
				// registration's admission reads never leave for the public
				// API.
				client, err := observer.Client.WithBaseURL(registration.Endpoint)
				if err != nil {
					return controller.AdmissionProvider{}, fmt.Errorf("provider %q endpoint %q is not usable: %w", registration.Name, registration.Endpoint, err)
				}
				provider := couriergithub.NewProvider(
					forge.ProviderConfig{Name: registration.Name, Endpoint: registration.Endpoint},
					client)
				return controller.AdmissionProvider{Provider: provider, Policy: provider, HeadLister: provider}, nil
			},
		},
	}, nil
}
