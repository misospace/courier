package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

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
		BaseBranch: os.Getenv("COURIER_BASE"), Username: os.Getenv("COURIER_GIT_USERNAME"), Token: os.Getenv("COURIER_GIT_TOKEN"),
	}
	baselineFile := filepath.Join(private, "baseline")
	if seed {
		operation, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		baseline, err := legacyobserver.Seed(operation, options.RemoteURL, options.BaseBranch, options.Branch, filepath.Join(private, "seed-repo"), options.Username, options.Token)
		if err != nil {
			return fmt.Errorf("observer: could not pin initial remote head: %w", err)
		}
		if err = os.WriteFile(baselineFile, []byte(baseline), 0600); err != nil {
			return err
		}
		// Kubelet carries the init container's independent baseline to the operator.
		return os.WriteFile(filepath.Join(private, "seed-termination"), []byte(baseline), 0600)
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
	for {
		pod, err := kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return errors.New("observer: pod observation failed")
		}
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
	return os.WriteFile(filepath.Join(private, "termination"), data, 0600)
}
