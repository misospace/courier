package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

func TestGitGuardTeardown(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		pod           bool
		foreignPod    bool
		labeledPod    bool
		service       bool
		foreignPolicy bool
		coordinator   bool
		pending       bool
	}{
		{name: "terminating guard retains policies", pod: true, labeledPod: true, service: true, pending: true},
		{name: "absent guard removes only ingress policy"},
		{name: "labeled foreign pod fails closed", foreignPod: true, labeledPod: true, pending: true},
		{name: "unlabeled name squatter does not hold the gate", foreignPod: true},
		{name: "foreign ingress policy is skipped", foreignPolicy: true},
		{name: "never launched"},
		{name: "coordinator does not hold guard gate", coordinator: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := &courierv1alpha1.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: "guard-run", Namespace: "runs", UID: types.UID("run-uid")}}
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := courierv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := networkingv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			objects := []client.Object{}
			ownedRef := runOwnerReferences(run)
			guardLabels := map[string]string{
				topology.LabelRun:       topology.RunLabelValue(run.Name),
				executor.LabelComponent: executor.GitGuardComponentName,
			}
			if tc.pod || tc.foreignPod {
				owner := ownedRef
				if tc.foreignPod {
					owner = nil
				}
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name), OwnerReferences: owner}}
				if tc.labeledPod {
					pod.Labels = guardLabels
				}
				if tc.pending {
					pod.Finalizers = []string{"courier.misospace.dev/test-hold"}
				}
				objects = append(objects, pod)
			}
			if tc.service {
				objects = append(objects, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name), OwnerReferences: ownedRef}})
			}
			egress := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name), OwnerReferences: ownedRef}}
			ingressRefs := ownedRef
			if tc.foreignPolicy {
				ingressRefs = nil
			}
			ingress := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: topology.GitGuardPodNetworkPolicyName(run.Name), OwnerReferences: ingressRefs}}
			objects = append(objects, egress, ingress)
			if tc.coordinator {
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Namespace: run.Namespace, Name: run.Name + "-coordinator", OwnerReferences: ownedRef,
					Labels: map[string]string{topology.LabelRun: topology.RunLabelValue(run.Name), executor.LabelComponent: "coordinator"},
				}})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			launcher := &CoordinatorLauncher{Client: c, APIReader: c}
			if tc.pending {
				pod := &corev1.Pod{}
				key := client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}
				if err := c.Get(ctx, key, pod); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			err := launcher.TeardownLegacyGitGuard(ctx, run)
			if tc.pending {
				if !errors.Is(err, ErrGuardTeardownPending) {
					t.Fatalf("teardown error = %v, want pending", err)
				}
				if err := launcher.TeardownLegacyGitGuard(ctx, run); !errors.Is(err, ErrGuardTeardownPending) {
					t.Fatalf("repeated teardown error = %v, want pending", err)
				}
			} else if err != nil {
				t.Fatalf("teardown: %v", err)
			}
			pod := &corev1.Pod{}
			podErr := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, pod)
			if tc.pod || tc.foreignPod {
				if apierrors.IsNotFound(podErr) {
					t.Fatal("guard pod was removed")
				}
				if podErr != nil {
					t.Fatal(podErr)
				}
				if tc.pending && pod.DeletionTimestamp == nil {
					t.Fatal("guard pod lacks deletion timestamp")
				}
			} else if !apierrors.IsNotFound(podErr) {
				t.Fatalf("guard pod error = %v, want NotFound", podErr)
			}
			serviceErr := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}, &corev1.Service{})
			if tc.service && !apierrors.IsNotFound(serviceErr) {
				t.Fatalf("service error = %v, want NotFound", serviceErr)
			}
			for _, policyCase := range []struct {
				name        string
				wantPresent bool
			}{
				{topology.GitGuardNetworkPolicyName(run.Name), true},
				{topology.GitGuardPodNetworkPolicyName(run.Name), tc.pending || tc.foreignPolicy},
			} {
				policy := &networkingv1.NetworkPolicy{}
				err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: policyCase.name}, policy)
				if policyCase.wantPresent && err != nil {
					t.Fatalf("policy %s missing: %v", policyCase.name, err)
				}
				if !policyCase.wantPresent && !apierrors.IsNotFound(err) {
					t.Fatalf("policy %s remains: %v", policyCase.name, err)
				}
			}
			if !tc.pending {
				if err := launcher.TeardownLegacyGitGuard(ctx, run); err != nil {
					t.Fatalf("second teardown: %v", err)
				}
				if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: egress.Name}, &networkingv1.NetworkPolicy{}); err != nil {
					t.Fatalf("coordinator policy not retained: %v", err)
				}
			}
		})
	}
}
