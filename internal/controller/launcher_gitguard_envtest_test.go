package controller

import (
	"context"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
)

// TestEnvtestGitGuardEgressPolicyConvergeIdempotent drives the AlreadyExists
// path of ensureGitGuardEgressPolicy against a real API server (#908): a
// policy the server stored with its own defaulting must compare equal to the
// rendered spec, so a steady-state launch must not rewrite the policy, and a
// drift the deployment wrote must converge exactly once. The stored object's
// resourceVersion is the witness: a converge update always produces a new
// revision, so an unchanged resourceVersion proves no update was issued.
func TestEnvtestGitGuardEgressPolicyConvergeIdempotent(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Create(ctx, envtestLane("gitguard-converge-lane")); err != nil {
		t.Fatal(err)
	}
	run := envtestRun("gitguard-converge", "gitguard-converge-lane")
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	var storedRun courierv1alpha1.CoderRun
	if err := c.Get(ctx, envtestKey(run.Name), &storedRun); err != nil {
		t.Fatal(err)
	}

	cidrs := mustParseCIDRs(t, []string{"140.82.112.0/20"})
	launcher := &CoordinatorLauncher{Client: c, APIReader: c, GitGuardEgressCIDRs: []string{"140.82.112.0/20"}, GitGuardKubernetesAPIIP: "10.96.0.1"}
	policyKey := client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}
	readPolicy := func(t *testing.T) networkingv1.NetworkPolicy {
		t.Helper()
		var policy networkingv1.NetworkPolicy
		if err := c.Get(ctx, policyKey, &policy); err != nil {
			t.Fatal(err)
		}
		return policy
	}

	// First launch: the policy is created through the API server, whose
	// defaulting and storage round-trip shape the spec the next launch reads.
	if err := launcher.ensureGitGuardEgressPolicy(ctx, &storedRun); err != nil {
		t.Fatalf("create ensureGitGuardEgressPolicy: %v", err)
	}
	live := readPolicy(t)
	want := topology.GitGuardEgressPolicy(&storedRun, cidrs, parseGitGuardAPIIP("10.96.0.1"))
	if !apiequality.Semantic.DeepEqual(want.Spec, live.Spec) {
		t.Fatalf("stored spec = %+v, want the rendered spec %+v", live.Spec, want.Spec)
	}
	steady := live.ResourceVersion

	// Second launch: the stored spec must be accepted as the rendered spec,
	// with the policy left untouched.
	if err := launcher.ensureGitGuardEgressPolicy(ctx, &storedRun); err != nil {
		t.Fatalf("steady-state ensureGitGuardEgressPolicy: %v", err)
	}
	still := readPolicy(t)
	if still.ResourceVersion != steady {
		t.Fatalf("steady-state launch rewrote the policy (resourceVersion %q -> %q): the stored spec does not compare equal to the rendered spec after API-server defaulting", steady, still.ResourceVersion)
	}

	// Drift the deployment wrote must converge, in place, exactly once.
	still.Spec.Egress = nil
	if err := c.Update(ctx, &still); err != nil {
		t.Fatal(err)
	}
	drifted := readPolicy(t)
	if err := launcher.ensureGitGuardEgressPolicy(ctx, &storedRun); err != nil {
		t.Fatalf("converge ensureGitGuardEgressPolicy: %v", err)
	}
	converged := readPolicy(t)
	if converged.ResourceVersion == drifted.ResourceVersion {
		t.Fatal("drifted policy was not converged in place")
	}
	if !apiequality.Semantic.DeepEqual(want.Spec, converged.Spec) {
		t.Fatalf("converged spec = %+v, want the rendered spec %+v", converged.Spec, want.Spec)
	}
}
