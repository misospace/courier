package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/legacyobserver"
)

// Observer modes run in separate containers, never in the model's identity.
func observerMode(ctx context.Context, seed bool) error {
	private := filepath.Join(executor.ObserverStatePath, "private")
	if seed {
		// MkdirAll already applies 0700 to the new leaf; an explicit chmod
		// hits EPERM on some volume layouts and adds nothing.
		if err := os.MkdirAll(private, 0700); err != nil {
			return err
		}
	}
	options := legacyobserver.Options{
		Workspace: os.Getenv("COURIER_WORKSPACE"), Scratch: private,
		RemoteURL: os.Getenv("COURIER_REPO_URL"), Branch: os.Getenv("COURIER_BRANCH"),
		BaseBranch: os.Getenv("COURIER_BASE"), BaseRemoteURL: os.Getenv("COURIER_BASE_REPO_URL"),
		Username: os.Getenv("COURIER_GIT_USERNAME"), Token: os.Getenv("COURIER_GIT_TOKEN"),
	}
	provider, err := observerCredentials(os.LookupEnv, os.Getenv)
	if err != nil {
		return err
	}
	options.Credentials = provider
	baselineFile := filepath.Join(private, "baseline")
	if seed {
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		seedOptions := options
		seedOptions.Scratch = filepath.Join(private, "seed-repo")
		baseline, err := legacyobserver.Seed(operation, seedOptions)
		if err != nil {
			return fmt.Errorf("observer: could not pin initial remote head: %w", err)
		}
		if err = os.WriteFile(baselineFile, []byte(baseline), 0600); err != nil {
			return err
		}
		// Kubelet carries the init container's independent baseline to the operator.
		return os.WriteFile(executor.ObserverStatePath+"/seed-termination", []byte(baseline), 0600)
	}
	baseline, err := os.ReadFile(baselineFile)
	if err != nil {
		return errors.New("observer: missing protected baseline")
	}
	options.Baseline = string(baseline)
	options.Scratch = filepath.Join(private, "observation-repo")
	cfg := &rest.Config{Host: "https://" + os.Getenv("KUBERNETES_SERVICE_HOST") + ":" + os.Getenv("KUBERNETES_SERVICE_PORT"), BearerTokenFile: executor.ObserverTokenPath + "/token", TLSClientConfig: rest.TLSClientConfig{CAFile: executor.ObserverTokenPath + "/ca.crt"}, Timeout: 10 * time.Second}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	namespace, name, uid := os.Getenv("POD_NAMESPACE"), os.Getenv("POD_NAME"), os.Getenv("POD_UID")
	var writerID string
	var getErr error
	// The observer starts alongside the writer, before the operator has
	// provisioned its run-scoped RBAC; API blips and authorization
	// propagation at pod start must not forfeit the evidence, so startup
	// failures retry for a bounded window before the observation gives up.
	startupDeadline := time.Now().Add(90 * time.Second)
	for {
		pod, err := kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			getErr = nil
			if string(pod.UID) != uid || !pod.DeletionTimestamp.IsZero() || pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
				return errors.New("observer: pod identity changed")
			}
			for _, s := range pod.Status.ContainerStatuses {
				if s.Name == "coordinator" && s.State.Terminated != nil {
					writerID = s.ContainerID
				}
			}
			if writerID != "" {
				break
			}
		} else {
			getErr = err
			if time.Now().After(startupDeadline) {
				return fmt.Errorf("observer: pod observation failed: %w", getErr)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	operation, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	safety, err := legacyobserver.Observe(operation, options)
	if err != nil {
		safety = executor.WorkspaceSafetyEvidence{State: "unknown"}
	}
	result := executor.ObserverEvidence{Version: 1, RunUID: os.Getenv("COURIER_RUN_UID"), PodUID: uid, Branch: options.Branch, Baseline: options.Baseline, WriterContainerID: writerID, Safety: &safety}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if len(data) > executor.MaxTerminationLineBytes {
		return errors.New("observer: evidence exceeds termination budget")
	}
	return os.WriteFile(os.Getenv("COURIER_TERMINATION_FILE"), data, 0600)
}

func observerCredentials(lookupEnv func(string) (string, bool), getenv func(string) string) (func(context.Context, string) (string, string, error), error) {
	if _, ok := lookupEnv(executor.EnvGitInstallation); !ok {
		return nil, nil
	}
	if getenv(executor.EnvGitInstallation) != "1" {
		return nil, errors.New("observer: installation git credential mode is invalid")
	}
	provider, err := observerInstallationCredentials(getenv)
	if err != nil {
		return nil, err
	}
	return provider, nil
}

// observerInstallationCredentials builds the App-mode credential provider for
// the observer's own projected service identity (#871). It binds exactly the
// two configured remote URLs to their pinned repository identities using the
// same rule as the pod's askpass helper, and re-reads the run's
// installation-token document through the API on every resolution, so
// operator rotation and expiry take effect immediately with no static
// fallback.
func observerInstallationCredentials(getenv func(string) string) (func(context.Context, string) (string, string, error), error) {
	remotes, ok := installationRemotesFrom(getenv)
	if !ok {
		return nil, errors.New("observer: installation git remote binding is incomplete")
	}
	owner := strings.TrimSpace(getenv(executor.EnvInstallationOwner))
	endpoint := strings.TrimSpace(getenv(executor.EnvInstallationEndpoint))
	namespace := strings.TrimSpace(getenv("POD_NAMESPACE"))
	secretName := strings.TrimSpace(getenv(executor.EnvInstallationSecret))
	if owner == "" || endpoint == "" || namespace == "" || secretName == "" {
		return nil, errors.New("observer: installation git credential configuration is incomplete")
	}
	reader, err := installationAPIReader(getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT"),
		executor.ObserverTokenPath+"/token", executor.ObserverTokenPath+"/ca.crt")
	if err != nil {
		return nil, errors.New("observer: installation git API reader unavailable")
	}
	source, err := broker.NewSecretInstallationTokenSource(reader, namespace, secretName,
		executor.InstallationTokenDocumentKey, owner, endpoint, nil)
	if err != nil {
		return nil, errors.New("observer: installation git credential source unavailable")
	}
	credentials := broker.NewInstallationGitCredentials(source, []string{remotes.baseIdentity, remotes.workIdentity})
	return func(ctx context.Context, remoteURL string) (string, string, error) {
		identity, ok := remotes.identityFor(remoteURL)
		if !ok {
			return "", "", errors.New("observer: remote is not a pinned installation repository")
		}
		served, err := credentials.Credentials(ctx, identity)
		if err != nil {
			return "", "", err
		}
		return served.Username, served.Token, nil
	}, nil
}
