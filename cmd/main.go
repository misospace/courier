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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/executor"
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
	var dispatchPollInterval time.Duration
	var dispatchHTTPTimeout time.Duration
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
	flag.BoolVar(&dispatchEnabled, "dispatch-enabled", false, "Enable Dispatch source discovery.")
	flag.StringVar(&dispatchBaseURL, "dispatch-base-url", "", "Dispatch base URL.")
	flag.StringVar(&dispatchAgentName, "dispatch-agent-name", "", "Dispatch agent name.")
	flag.StringVar(&dispatchQueueLane, "dispatch-queue-lane", "", "Dispatch queue lane sent to next-task.")
	flag.StringVar(&dispatchLane, "dispatch-lane", "", "LaneProfile assigned to discovered Dispatch work.")
	flag.DurationVar(&dispatchPollInterval, "dispatch-poll-interval", 30*time.Second, "Dispatch discovery poll interval.")
	flag.DurationVar(&dispatchHTTPTimeout, "dispatch-http-timeout", 30*time.Second, "Dispatch HTTP request timeout.")
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
		if strings.TrimSpace(dispatchBaseURL) == "" || strings.TrimSpace(dispatchAgentName) == "" || strings.TrimSpace(dispatchQueueLane) == "" || strings.TrimSpace(dispatchLane) == "" {
			setupLog.Error(fmt.Errorf("dispatch requires base URL, agent name, queue lane, and LaneProfile"), "unable to configure Dispatch source")
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
		dispatchClient, err := dispatch.NewClientWithLane(dispatchBaseURL, dispatchAgentName, dispatchQueueLane, token, dispatchHTTPTimeout)
		if err != nil {
			setupLog.Error(err, "unable to configure Dispatch client")
			os.Exit(1)
		}
		checker, err := dispatchPRStateChecker(githubObserver)
		if err != nil {
			setupLog.Error(err, "unable to configure Dispatch source: GitHub pull request state lookup is unavailable")
			os.Exit(1)
		}
		dispatchClient.WithPullRequestStateChecker(checker)
		dispatchAdapter := dispatch.New(dispatchClient)
		sources.Register("dispatch", dispatchAdapter)
		runner := source.NewRunner(mgr.GetClient(), dispatchAdapter, source.RunnerConfig{
			Source:       "dispatch",
			LaneProfile:  dispatchLane,
			Namespace:    namespace,
			PollInterval: dispatchPollInterval,
		})
		if err := mgr.Add(runner); err != nil {
			setupLog.Error(err, "unable to add Dispatch discovery runner")
			os.Exit(1)
		}
	}

	if err := (&controller.CoderRunReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Launch:         launcher.Launch,
		Sources:        sources,
		StatusWriter:   status.KubePatchWriter{Client: mgr.GetClient()},
		Observer:       githubObserver,
		Events:         runEvents,
		PRHeadResolver: existingPRHeadResolver(githubObserver),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CoderRun")
		os.Exit(1)
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
