package broker

import (
	"testing"

	"github.com/misospace/courier/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func bindingRun(mode v1alpha1.Mode) *v1alpha1.CoderRun {
	policy := &v1alpha1.PublicationPolicy{
		RunUID: "run-uid", ProviderConfigRef: "providers/github",
		BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base-oid",
		WorkRepo: "org/repo", WorkRef: "courier/org/repo/issue-12",
		WorkInitiallyAbsent: true,
	}
	spec := v1alpha1.CoderRunSpec{Mode: mode, Repo: "org/repo", Ref: 12}
	if mode == v1alpha1.ModeFixPR {
		spec.Ref = 42
		policy.WorkRepo = "fork/repo"
		policy.WorkRef = "feature/fix"
		policy.WorkInitiallyAbsent = false
		policy.WorkOID = "head-oid"
		policy.PRNumber = 42
		policy.HeadAnchorOID = "head-oid"
	}
	return &v1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID("run-uid")},
		Spec:       spec,
		Status:     v1alpha1.CoderRunStatus{PublicationPolicy: policy},
	}
}

func TestPolicyFromRunResolveInitiallyAbsent(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkRef != "courier/org/repo/issue-12" || got.WorkRepo != "org/repo" || !got.WorkInitiallyAbsent || got.WorkAnchorOID != "" {
		t.Fatalf("unexpected policy: %+v", got)
	}
}

func TestPolicyFromRunRejectsNonCanonicalSpecRepository(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	for _, canonical := range []string{"", "other/repo"} {
		t.Run(canonical, func(t *testing.T) {
			if _, err := PolicyFromRun(run, "providers/github", canonical); err == nil {
				t.Fatal("expected non-canonical spec repository rejection")
			}
		})
	}
}

func TestPolicyFromRunFixPRUsesPinnedForkHead(t *testing.T) {
	run := bindingRun(v1alpha1.ModeFixPR)
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkRepo != "fork/repo" || got.WorkRef != "feature/fix" || got.WorkAnchorOID != "head-oid" || got.HeadAnchorOID != "head-oid" || got.PRNumber != 42 {
		t.Fatalf("unexpected fork policy: %+v", got)
	}
}

func TestPolicyFromRunRejectsNonCanonicalBaseRepo(t *testing.T) {
	for _, baseRepo := range []string{"", "org/other"} {
		t.Run(baseRepo, func(t *testing.T) {
			run := bindingRun(v1alpha1.ModeResolveIssue)
			run.Status.PublicationPolicy.BaseRepo = baseRepo
			if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
				t.Fatalf("expected base repo %q to be rejected", baseRepo)
			}
		})
	}
}

func TestPolicyFromRunRejectsCrossRunAndWrongProvider(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.CoderRun)
		ref  string
	}{
		{name: "policy belongs to another run", edit: func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.RunUID = "other-run" }, ref: "providers/github"},
		{name: "run incarnation changed", edit: func(r *v1alpha1.CoderRun) { r.UID = types.UID("new-incarnation") }, ref: "providers/github"},
		{name: "wrong-provider", edit: func(*v1alpha1.CoderRun) {}, ref: "providers/other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := bindingRun(v1alpha1.ModeResolveIssue)
			tc.edit(run)
			if _, err := PolicyFromRun(run, tc.ref, "org/repo"); err == nil {
				t.Fatal("expected binding rejection")
			}
		})
	}
}

func TestPolicyFromRunRejectsWrongModeAndIncompleteFixPR(t *testing.T) {
	run := bindingRun(v1alpha1.ModeFixPR)
	run.Spec.Mode = v1alpha1.ModeResolveIssue
	if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
		t.Fatal("expected fix-pr policy to be rejected for resolve-issue")
	}

	run = bindingRun(v1alpha1.ModeFixPR)
	run.Status.PublicationPolicy.HeadAnchorOID = ""
	if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
		t.Fatal("expected incomplete fix-pr head to be rejected")
	}
}

func TestPolicyFromRunIgnoresMutableStatus(t *testing.T) {
	run := bindingRun(v1alpha1.ModeFixPR)
	run.Status.Branch = "stale-or-mutated-branch"
	run.Status.HeadRepo = "stale-or-mutated/repo"
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatalf("mutable status must not affect policy binding: %v", err)
	}
	if got.WorkRepo != "fork/repo" || got.WorkRef != "feature/fix" {
		t.Fatalf("policy did not preserve immutable pins: %+v", got)
	}
}

func TestPolicyFromRunRequiresConsistentResolveInitialTip(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	run.Status.PublicationPolicy.WorkInitiallyAbsent = false
	if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
		t.Fatal("expected existing work ref without an admission tip to be rejected")
	}

	run.Status.PublicationPolicy.WorkInitiallyAbsent = true
	run.Status.PublicationPolicy.WorkOID = "unexpected-tip"
	if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
		t.Fatal("expected initially absent work ref with a tip to be rejected")
	}
}
