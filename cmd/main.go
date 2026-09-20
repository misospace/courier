package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

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
	"github.com/misospace/courier/internal/source"
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
	if err := (&controller.CoderRunReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Launch: launcher.Launch,
		Sources: controller.NewSourceRegistry(map[string]source.Adapter{
			"manual": manual.Adapter{},
		}),
		StatusWriter: status.KubePatchWriter{Client: mgr.GetClient()},
		Observer:     githubObserver,
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
