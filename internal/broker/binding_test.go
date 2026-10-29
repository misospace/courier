package broker

import (
	"strings"
	"testing"

	"github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
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
	if mode == v1alpha1.ModeContribute {
		spec.Ref = 0
		spec.WorkRepo = "fork/repo"
		spec.WorkItemID = "adhoc-0123456789ab"
		policy.WorkRepo = "fork/repo"
		policy.WorkRef = branch.ResolveGoalBranch("org/repo", spec.WorkItemID)
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

// TestPolicyFromRunPopulatesSourceIssueFromSpec proves the broker binding
// resolves the trusted source-issue identity from the immutable spec. The
// identity feeds the broker's authoritative-linkage check (#248) and is the
// only authority a PR body's closing reference may match. A fix-pr run
// leaves the field zero because the existing PR's identity is already
// pinned through PRNumber and HeadAnchorOID.
func TestPolicyFromRunPopulatesSourceIssueFromSpec(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	want := SourceIssue{Owner: "org", Name: "repo", Number: 12}
	if got.SourceIssue != want {
		t.Fatalf("SourceIssue = %+v, want %+v", got.SourceIssue, want)
	}

	// A fix-pr run must not set the SourceIssue; the broker's
	// CreatePullRequest path is closed to fix-pr, and the
	// existing PR's identity is pinned through PRNumber.
	run = bindingRun(v1alpha1.ModeFixPR)
	got, err = PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	if !got.SourceIssue.IsZero() {
		t.Fatalf("fix-pr SourceIssue = %+v, want zero", got.SourceIssue)
	}
}

// TestPolicyFromRunRejectsBaseRepoWithoutOwnerSlash proves the broker
// binding refuses to derive a source-issue identity from a base
// repository that is not an "owner/name" form. The identity is the
// only authority a PR body may match, so a malformed value must
// fail closed at admission.
func TestPolicyFromRunRejectsBaseRepoWithoutOwnerSlash(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	run.Status.PublicationPolicy.BaseRepo = "no-slash"
	if _, err := PolicyFromRun(run, "providers/github", "no-slash"); err == nil {
		t.Fatal("expected non-owner/name base repository to be rejected")
	}
}

// pinCriteriaOn mirrors admission's mapping of an extracted set onto the
// persisted policy fields (#280), so binding tests exercise the same shape
// the operator writes.
func pinCriteriaOn(t *testing.T, policy *v1alpha1.PublicationPolicy, body string) {
	t.Helper()
	set, err := ExtractCriteria(body, SourceIssue{Owner: "org", Name: "repo", Number: 12})
	if err != nil {
		t.Fatalf("extract criteria: %v", err)
	}
	policy.SourceIssue = &v1alpha1.SourceIssueRef{Owner: "org", Name: "repo", Number: 12}
	for _, criterion := range set.Criteria {
		policy.AcceptanceCriteria = append(policy.AcceptanceCriteria, v1alpha1.Criterion{
			ID: criterion.ID, Text: criterion.Text, Digest: criterion.Digest,
		})
	}
	policy.AcceptanceCriteriaDigest = set.Digest
}

// The binding reconstructs a pinned criterion set and re-verifies it by
// digest; a policy admitted before the criterion contract (no pin at all)
// still binds — the backward-compatibility requirement.
func TestPolicyFromRunCarriesPinnedCriteria(t *testing.T) {
	run := bindingRun(v1alpha1.ModeResolveIssue)
	pinCriteriaOn(t, run.Status.PublicationPolicy, "## Acceptance\n- first criterion\n- second criterion\n")
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	set := got.Criteria
	if set.Issue != (SourceIssue{Owner: "org", Name: "repo", Number: 12}) {
		t.Fatalf("pinned issue = %+v", set.Issue)
	}
	if len(set.Criteria) != 2 || set.Criteria[0].ID != "AC-1" || set.Criteria[1].ID != "AC-2" {
		t.Fatalf("pinned criteria = %+v", set.Criteria)
	}
	if set.Digest == "" || set.Digest != run.Status.PublicationPolicy.AcceptanceCriteriaDigest {
		t.Fatalf("pinned digest = %q", set.Digest)
	}

	// A pre-contract policy carries no pin and still binds.
	legacy := bindingRun(v1alpha1.ModeResolveIssue)
	got, err = PolicyFromRun(legacy, "providers/github", "org/repo")
	if err != nil {
		t.Fatalf("legacy policy binding: %v", err)
	}
	if got.Criteria.Digest != "" || got.Criteria.Issue != (SourceIssue{}) {
		t.Fatalf("legacy policy must carry no criteria pin: %+v", got.Criteria)
	}

	// An explicitly empty pin is carried as a pinned empty set.
	empty := bindingRun(v1alpha1.ModeResolveIssue)
	pinCriteriaOn(t, empty.Status.PublicationPolicy, "No acceptance section here.\n")
	got, err = PolicyFromRun(empty, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Criteria.IsEmpty() || got.Criteria.Digest == "" {
		t.Fatalf("empty pin = %+v, want an empty set with a digest", got.Criteria)
	}
}

// Every corruption of the pin fails closed at the binding: tampered text,
// a stale set digest, a rewritten ordinal, a cross-issue identity, a pin
// without an identity, an identity without a pin, and fix-pr pins.
func TestPolicyFromRunRejectsCorruptCriterionPins(t *testing.T) {
	pinned := func(t *testing.T) *v1alpha1.CoderRun {
		t.Helper()
		run := bindingRun(v1alpha1.ModeResolveIssue)
		pinCriteriaOn(t, run.Status.PublicationPolicy, "## Acceptance\n- first criterion\n- second criterion\n")
		return run
	}

	tamper := map[string]func(*v1alpha1.PublicationPolicy){
		"tampered criterion text": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteria[0].Text = "rewritten by someone else"
		},
		"stale set digest": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteriaDigest = "sha256:" + strings.Repeat("a", 64)
		},
		"rewritten ordinal": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteria[0].ID = "AC-2"
		},
		"dropped criterion": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteria = p.AcceptanceCriteria[1:]
		},
		"cross-issue identity": func(p *v1alpha1.PublicationPolicy) {
			p.SourceIssue.Number = 13
		},
		"pin without identity": func(p *v1alpha1.PublicationPolicy) {
			p.SourceIssue = nil
		},
		"identity without pin": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteria = nil
			p.AcceptanceCriteriaDigest = ""
		},
		"criteria without issue or set digest": func(p *v1alpha1.PublicationPolicy) {
			p.SourceIssue = nil
			p.AcceptanceCriteriaDigest = ""
		},
		"criteria without digest but with identity": func(p *v1alpha1.PublicationPolicy) {
			p.AcceptanceCriteriaDigest = ""
		},
	}
	for name, corrupt := range tamper {
		t.Run(name, func(t *testing.T) {
			run := pinned(t)
			corrupt(run.Status.PublicationPolicy)
			if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
				t.Fatal("a corrupt criterion pin must fail the binding")
			}
		})
	}

	t.Run("fix-pr pins", func(t *testing.T) {
		run := bindingRun(v1alpha1.ModeFixPR)
		pinCriteriaOn(t, run.Status.PublicationPolicy, "## Acceptance\n- stray pin\n")
		if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
			t.Fatal("a fix-pr policy must not carry resolve-issue criterion pins")
		}
	})
}

// A contribute policy binds from the immutable spec's approved fork and the
// goal branch the same pure derivation admission uses, in both admitted tip
// shapes: an absent work ref, and a recorded tip at the resume-adoption
// boundary.
func TestPolicyFromRunContributeBindsForkAndGoalBranch(t *testing.T) {
	absent := bindingRun(v1alpha1.ModeContribute)
	got, err := PolicyFromRun(absent, "providers/github", "org/repo")
	if err != nil {
		t.Fatal(err)
	}
	wantRef := branch.ResolveGoalBranch("org/repo", "adhoc-0123456789ab")
	if wantRef == "" || got.WorkRef != wantRef {
		t.Fatalf("contribute work ref = %q, want the derived goal branch %q", got.WorkRef, wantRef)
	}
	if got.Mode != ModeContribute || got.WorkRepo != "fork/repo" || !got.WorkInitiallyAbsent || got.WorkAnchorOID != "" {
		t.Fatalf("unexpected contribute policy: %+v", got)
	}
	if got.PRNumber != 0 || got.HeadAnchorOID != "" || !got.SourceIssue.IsZero() || got.Criteria.IsPinned() {
		t.Fatalf("contribute policy carries anchors or pins: %+v", got)
	}

	resumed := bindingRun(v1alpha1.ModeContribute)
	resumed.Status.PublicationPolicy.WorkInitiallyAbsent = false
	resumed.Status.PublicationPolicy.WorkOID = "recorded-tip"
	got, err = PolicyFromRun(resumed, "providers/github", "org/repo")
	if err != nil {
		t.Fatalf("resume-adoption shape must bind: %v", err)
	}
	if got.WorkInitiallyAbsent || got.WorkAnchorOID != "recorded-tip" {
		t.Fatalf("unexpected resumed contribute policy: %+v", got)
	}
}

// The persisted fork identity is compared case-folded, because forge
// identities are case-insensitive while the broker holds no provider read:
// the same repository under another casing still binds, and any other
// repository does not.
func TestPolicyFromRunContributeCanonicalizesApprovedFork(t *testing.T) {
	cased := bindingRun(v1alpha1.ModeContribute)
	cased.Spec.WorkRepo = "Contributor/Fork"
	cased.Status.PublicationPolicy.WorkRepo = "contributor/fork"
	if _, err := PolicyFromRun(cased, "providers/github", "org/repo"); err != nil {
		t.Fatalf("case-folded fork identity must bind: %v", err)
	}

	// The binding proves the persisted destination is the spec's approved
	// fork and nothing else; whether that fork is approved at all is the
	// registry's selection, enforced at admission.
	for _, persistedFork := range []string{"someone-else/fork", "contributor/forkish", "fork/repo/extra", "fork", ""} {
		t.Run(persistedFork, func(t *testing.T) {
			run := bindingRun(v1alpha1.ModeContribute)
			run.Status.PublicationPolicy.WorkRepo = persistedFork
			if _, err := PolicyFromRun(run, "providers/github", "org/repo"); err == nil {
				t.Fatalf("persisted work repository %q must not bind as the approved fork", persistedFork)
			}
		})
	}

	malformed := bindingRun(v1alpha1.ModeContribute)
	malformed.Spec.WorkRepo = "fork"
	malformed.Status.PublicationPolicy.WorkRepo = "fork"
	if _, err := PolicyFromRun(malformed, "providers/github", "org/repo"); err == nil {
		t.Fatal("a spec work repository that is not an owner/name must be rejected")
	}
}

// Every malformed contribute shape fails the binding closed with its own
// reason: a spec that is not a contribution, a persisted identity that
// describes different work, pins or anchors a contribution never records, an
// inconsistent work tip, or a policy bound to another run or provider.
func TestPolicyFromRunRejectsMalformedContribute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		edit    func(*v1alpha1.CoderRun)
		wantErr string
	}{
		{
			name:    "spec ref is set",
			edit:    func(r *v1alpha1.CoderRun) { r.Spec.Ref = 7 },
			wantErr: "contribute run spec must carry no ref",
		},
		{
			name:    "spec work repository is empty",
			edit:    func(r *v1alpha1.CoderRun) { r.Spec.WorkRepo = "" },
			wantErr: "requires an approved work repository",
		},
		{
			name:    "spec work item identity is empty",
			edit:    func(r *v1alpha1.CoderRun) { r.Spec.WorkItemID = "" },
			wantErr: "requires a work item identity",
		},
		{
			name:    "persisted work repository is not the approved fork",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.WorkRepo = "other/fork" },
			wantErr: "does not match the approved fork",
		},
		{
			name: "persisted work repository is the base under another casing",
			edit: func(r *v1alpha1.CoderRun) {
				r.Spec.WorkRepo = "ORG/repo"
				r.Status.PublicationPolicy.WorkRepo = "ORG/repo"
			},
			wantErr: "distinct from the base",
		},
		{
			name:    "persisted work ref is not the derived goal branch",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.WorkRef = "courier/org/repo/issue-7" },
			wantErr: "does not match the goal branch",
		},
		{
			name: "persisted work ref was derived over a different base",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.WorkRef = branch.ResolveGoalBranch("other/repo", r.Spec.WorkItemID)
			},
			wantErr: "does not match the goal branch",
		},
		{
			name:    "persisted pr number anchor",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.PRNumber = 3 },
			wantErr: "contribute policy contains fix-pr anchors",
		},
		{
			name:    "persisted head anchor",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.HeadAnchorOID = "head-oid" },
			wantErr: "contribute policy contains fix-pr anchors",
		},
		{
			name: "stray source-issue pin",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.SourceIssue = &v1alpha1.SourceIssueRef{Owner: "org", Name: "repo", Number: 7}
			},
			wantErr: "carries source-issue or criterion pins",
		},
		{
			name: "stray criterion rows",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.AcceptanceCriteria = []v1alpha1.Criterion{{ID: "AC-1", Text: "stray", Digest: "sha256:" + strings.Repeat("a", 64)}}
			},
			wantErr: "carries source-issue or criterion pins",
		},
		{
			name: "stray criterion set digest",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.AcceptanceCriteriaDigest = "sha256:" + strings.Repeat("b", 64)
			},
			wantErr: "carries source-issue or criterion pins",
		},
		{
			name: "absent work ref carries a tip",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.WorkInitiallyAbsent = true
				r.Status.PublicationPolicy.WorkOID = "unexpected-tip"
			},
			wantErr: "inconsistent initial work tip",
		},
		{
			name: "existing work ref carries no tip",
			edit: func(r *v1alpha1.CoderRun) {
				r.Status.PublicationPolicy.WorkInitiallyAbsent = false
			},
			wantErr: "inconsistent initial work tip",
		},
		{
			name:    "policy belongs to another run",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.RunUID = "other-run" },
			wantErr: "run UID does not match live run",
		},
		{
			name:    "run incarnation changed",
			edit:    func(r *v1alpha1.CoderRun) { r.UID = types.UID("new-incarnation") },
			wantErr: "run UID does not match live run",
		},
		{
			name:    "policy names another provider",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.ProviderConfigRef = "providers/other" },
			wantErr: "provider does not match trusted config",
		},
		{
			name:    "persisted base is not canonical",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.BaseRepo = "other/repo" },
			wantErr: "does not match canonical spec repository",
		},
		{
			name:    "incomplete base identity",
			edit:    func(r *v1alpha1.CoderRun) { r.Status.PublicationPolicy.BaseOID = "" },
			wantErr: "base and work identities are incomplete",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := bindingRun(v1alpha1.ModeContribute)
			tc.edit(run)
			_, err := PolicyFromRun(run, "providers/github", "org/repo")
			if err == nil {
				t.Fatalf("expected rejection containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// Mutable status stays out of a contribute binding: the goal branch and fork
// come from the immutable spec and the persisted policy only.
func TestPolicyFromRunContributeIgnoresMutableStatus(t *testing.T) {
	run := bindingRun(v1alpha1.ModeContribute)
	run.Status.Branch = "stale-or-mutated-branch"
	run.Status.HeadRepo = "stale-or-mutated/repo"
	run.Status.HeadSHA = "stale-head-sha"
	got, err := PolicyFromRun(run, "providers/github", "org/repo")
	if err != nil {
		t.Fatalf("mutable status must not affect policy binding: %v", err)
	}
	if got.WorkRepo != "fork/repo" || got.WorkRef != branch.ResolveGoalBranch("org/repo", run.Spec.WorkItemID) {
		t.Fatalf("policy did not preserve immutable pins: %+v", got)
	}
}

// A canonical base the goal-branch derivation cannot name is corruption at
// this boundary too, and each shape says so with its own reason: the binding
// never falls back to a branch the immutable spec did not derive.
func TestPolicyFromRunRejectsContributeBaseWithoutDerivation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		base    string
		workRef string
		wantErr string
	}{
		{
			name:    "base is not an owner/name",
			base:    "no-slash",
			workRef: "courier/no-slash/goal-0123456789ab",
			wantErr: "canonical spec repository",
		},
		{
			name:    "base cannot derive a goal branch",
			base:    "org/re po",
			workRef: "courier/org/re po/goal-0123456789ab",
			wantErr: "does not match the goal branch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := bindingRun(v1alpha1.ModeContribute)
			run.Status.PublicationPolicy.BaseRepo = tc.base
			run.Status.PublicationPolicy.WorkRef = tc.workRef
			_, err := PolicyFromRun(run, "providers/github", tc.base)
			if err == nil {
				t.Fatalf("expected rejection containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}
