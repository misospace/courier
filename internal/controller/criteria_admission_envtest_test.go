package controller

import (
	"context"
	"os"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/status"
)

// Secure admission through the real API server (#280): the trusted
// source-issue read and criterion pinning run inside Reconcile, the pinned
// set survives the status subresource round trip (the regenerated CRD schema
// carries the new fields), and the fail-closed outcomes terminalize the run
// NeedsHuman naming why. The fixture is the live miso-gallery#502 body —
// the dogfood failure the contract exists for.
func TestEnvtestSecureAdmissionPinsCriteria(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	fixture, err := os.ReadFile("../../internal/broker/testdata/miso-gallery-502-issue.md")
	if err != nil {
		t.Fatalf("read 502 fixture: %v", err)
	}

	reconcile := func(t *testing.T, name, issueBody string) {
		t.Helper()
		repo := forge.Repository{ID: "repo-1", Canonical: "misospace/miso-gallery", DefaultRef: "main"}
		provider := &fakeAdmissionForge{
			repos: map[string]forge.Repository{"misospace/miso-gallery": repo},
			refs: map[string]forge.RefState{
				"misospace/miso-gallery:main":                                     {Ref: "main", OID: "baseoid", Exists: true},
				"misospace/miso-gallery:courier/misospace/miso-gallery/issue-502": {Ref: "courier/misospace/miso-gallery/issue-502", Exists: false},
			},
			issues: map[string]forge.WorkItem{
				"misospace/miso-gallery#502": {ID: "502", Repo: "misospace/miso-gallery", Title: "Package the tracked assets directory", Body: issueBody, State: "open"},
			},
		}
		// Each admission gets its own lane: a run without a launcher stays
		// Claimed and would otherwise hold the shared lane's only slot.
		run := envtestRun(name, name)
		run.Spec.Repo = "misospace/miso-gallery"
		run.Spec.Ref = 502
		lane := envtestLane(name)
		if err := envtestClient.Create(context.Background(), lane); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
		if err := envtestClient.Create(context.Background(), run); err != nil {
			t.Fatal(err)
		}
		item := &admissionSource{}
		base := &CoderRunReconciler{
			Client:       envtestClient,
			Sources:      NewSourceRegistry(map[string]source.Adapter{"manual": item}),
			StatusWriter: status.KubePatchWriter{Client: envtestClient},
		}
		// Launch stays unset so the admission outcome is observed without
		// provisioning the full secure topology: this test owns admission,
		// not the launcher.
		base.Secure = &SecureControl{
			Client:  envtestClient,
			Patcher: base,
			Config: SecureConfig{
				RunNamespace:      "default",
				Registry:          secureRegistry(),
				HarnessImage:      "harness:test",
				ProbeImage:        "busybox:test",
				LiveProbes:        false,
				OperatorNamespace: "courier-system",
				Providers:         fakeProviderFactory(provider),
				Kubernetes:        fakeAccessReviews(),
			},
		}
		if _, err := base.Reconcile(context.Background(), envtestRequest(name)); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	t.Run("pins miso-gallery 502", func(t *testing.T) {
		reconcile(t, "envtest-502-pin", string(fixture))
		var run courierv1alpha1.CoderRun
		if err := envtestClient.Get(context.Background(), envtestKey("envtest-502-pin"), &run); err != nil {
			t.Fatal(err)
		}
		policy := run.Status.PublicationPolicy
		if policy == nil {
			t.Fatal("admission must persist the publication policy")
		}
		if policy.SourceIssue == nil || policy.SourceIssue.Owner != "misospace" || policy.SourceIssue.Name != "miso-gallery" || policy.SourceIssue.Number != 502 {
			t.Fatalf("source issue pin = %+v, want misospace/miso-gallery#502", policy.SourceIssue)
		}
		expected, err := broker.ExtractCriteria(string(fixture), broker.SourceIssue{Owner: "misospace", Name: "miso-gallery", Number: 502})
		if err != nil {
			t.Fatal(err)
		}
		if policy.AcceptanceCriteriaDigest != expected.Digest {
			t.Fatalf("set digest = %q, want the pure extraction's %q", policy.AcceptanceCriteriaDigest, expected.Digest)
		}
		if len(policy.AcceptanceCriteria) != 3 {
			t.Fatalf("pinned criteria = %+v, want AC-1..AC-3", policy.AcceptanceCriteria)
		}
		// The pin survives the API server: the persisted status round trip
		// proves the regenerated CRD carries the new fields.
		var reread courierv1alpha1.CoderRun
		if err := envtestClient.Get(context.Background(), envtestKey("envtest-502-pin"), &reread); err != nil {
			t.Fatal(err)
		}
		if _, pinned, err := broker.CriterionSetFromPolicy(reread.Status.PublicationPolicy); err != nil || !pinned {
			t.Fatalf("reconstructed pin = (%v, %t)", err, pinned)
		}
	})

	t.Run("empty set for an issue without acceptance criteria", func(t *testing.T) {
		reconcile(t, "envtest-502-empty", "## Problem\nNothing is pinned here.\n")
		var run courierv1alpha1.CoderRun
		if err := envtestClient.Get(context.Background(), envtestKey("envtest-502-empty"), &run); err != nil {
			t.Fatal(err)
		}
		policy := run.Status.PublicationPolicy
		if policy == nil {
			t.Fatal("admission must persist the publication policy")
		}
		if len(policy.AcceptanceCriteria) != 0 || policy.AcceptanceCriteriaDigest == "" {
			t.Fatalf("criteria = %+v digest %q, want an explicit empty pin", policy.AcceptanceCriteria, policy.AcceptanceCriteriaDigest)
		}
	})

	failClosed := map[string]struct {
		body      string
		wantInMsg string
	}{
		"ambiguous sections": {
			body:      "## Acceptance\n- one\n\n## Notes\n\n## Acceptance\n- two\n",
			wantInMsg: "more than one Acceptance section",
		},
		"over-bound set": {
			body:      "## Acceptance\n" + strings.Repeat("- a numbered criterion\n", broker.MaxCriteria+1),
			wantInMsg: "the bound is",
		},
	}
	for name, test := range failClosed {
		t.Run(name, func(t *testing.T) {
			reconcile(t, "envtest-502-"+strings.ReplaceAll(name, " ", "-"), test.body)
			var run courierv1alpha1.CoderRun
			if err := envtestClient.Get(context.Background(), envtestKey("envtest-502-"+strings.ReplaceAll(name, " ", "-")), &run); err != nil {
				t.Fatal(err)
			}
			if run.Status.Phase != courierv1alpha1.PhaseNeedsHuman {
				t.Fatalf("phase = %q, want NeedsHuman", run.Status.Phase)
			}
			if run.Status.PublicationPolicy != nil {
				t.Fatal("a failed admission must not persist a policy")
			}
			var condition *metav1.Condition
			for i := range run.Status.Conditions {
				if run.Status.Conditions[i].Type == "SecurePreflightFailed" {
					condition = &run.Status.Conditions[i]
				}
			}
			if condition == nil {
				t.Fatal("the NeedsHuman terminalization must record SecurePreflightFailed")
			}
			if !strings.Contains(condition.Message, test.wantInMsg) {
				t.Fatalf("condition message = %q, want it to name %q", condition.Message, test.wantInMsg)
			}
		})
	}
}
