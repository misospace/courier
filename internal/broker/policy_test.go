package broker

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

type fakeObserver struct {
	base, work RepositoryState
	pr         PullRequestState
	pulls      []PullRequestState
	getErr     error
	created    CreatePullRequest
	updated    UpdatePullRequest
}

func (f *fakeObserver) Repository(_ context.Context, repo, ref string) (RepositoryState, error) {
	if f.getErr != nil {
		return RepositoryState{}, f.getErr
	}
	if repo == f.base.Repo && ref == f.base.Ref {
		return f.base, nil
	}
	return f.work, nil
}
func (f *fakeObserver) PullRequest(_ context.Context, _ int) (PullRequestState, error) {
	if f.getErr != nil {
		return PullRequestState{}, f.getErr
	}
	return f.pr, nil
}
func (f *fakeObserver) FindPullRequest(_ context.Context, repo, ref string) ([]PullRequestState, error) {
	return append([]PullRequestState(nil), f.pulls...), nil
}
func (f *fakeObserver) CreatePullRequest(_ context.Context, in CreatePullRequest) (int, error) {
	f.created = in
	return 42, nil
}
func (f *fakeObserver) UpdatePullRequest(_ context.Context, _ int, in UpdatePullRequest) error {
	f.updated = in
	return nil
}

type fakePusher struct {
	observer                         *fakeObserver
	ancestor                         bool
	ancestorSeq                      []bool
	ancestorCalls                    int
	pushErr                          error
	pushedRepo, pushedRef, pushedOID string
	afterPush                        func()
}

func (f *fakePusher) IsAncestor(_ context.Context, _, ancestor, descendant string) (bool, error) {
	if f.ancestorSeq != nil {
		answer := f.ancestor
		if f.ancestorCalls < len(f.ancestorSeq) {
			answer = f.ancestorSeq[f.ancestorCalls]
		}
		f.ancestorCalls++
		return answer, nil
	}
	return f.ancestor, nil
}
func (f *fakePusher) Push(_ context.Context, repo, ref, oid string) error {
	f.pushedRepo, f.pushedRef, f.pushedOID = repo, ref, oid
	if f.afterPush != nil {
		f.afterPush()
	}
	return f.pushErr
}

func goodObserver(mode Mode) *fakeObserver {
	f := &fakeObserver{base: RepositoryState{Repo: "org/repo", Ref: "main", Exists: true, OID: "base2", Default: true, Protected: true}, work: RepositoryState{Repo: "org/repo", Ref: "courier/work", Exists: true, OID: "old", ProtectionKnown: true, Writable: true, WriteKnown: true}}
	if mode == ModeFixPR {
		f.pr = PullRequestState{Number: 7, State: "open", BaseRepo: "org/repo", BaseRef: "main", HeadRepo: "org/repo", HeadRef: "courier/work", HeadOID: "old"}
	}
	return f
}
func goodPolicy(mode Mode) Policy {
	p := Policy{RunUID: "uid-1", Mode: mode, Provider: "gh", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base1", WorkRepo: "org/repo", WorkRef: "courier/work", WorkAnchorOID: "old"}
	if mode == ModeResolveIssue {
		p.WorkInitiallyAbsent = false
		p.SourceIssue = SourceIssue{Owner: "org", Name: "repo", Number: 502}
	} else {
		p.PRNumber = 7
		p.HeadAnchorOID = "old"
	}
	return p
}
func engine(t *testing.T, p Policy, o *fakeObserver, w *fakePusher) *PolicyEngine {
	t.Helper()
	w.observer = o
	e, err := NewPolicyEngine(p, o, w)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestNewEngineRejectsSameRefAsBaseAndWork(t *testing.T) {
	p := goodPolicy(ModeResolveIssue)
	p.WorkRef = p.BaseRef
	if _, err := NewPolicyEngine(p, goodObserver(ModeResolveIssue), &fakePusher{}); err == nil {
		t.Fatal("accepted base ref as publication destination")
	}
}

func TestPublishAllowsProtectedDefaultBaseAndUsesPinnedOrdinaryPush(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	w.afterPush = func() { o.work.OID = "new"; o.pr.HeadOID = "new" }
	got, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if got.OID != "new" || w.pushedRepo != "org/repo" || w.pushedRef != "courier/work" || w.pushedOID != "new" {
		t.Fatalf("unexpected publish: %#v %#v", got, w)
	}
}
func TestPublishRejectsRunUIDAndWrongRefOrRepository(t *testing.T) {
	for _, mutate := range []func(*fakeObserver){func(o *fakeObserver) { o.work.Ref = "other" }, func(o *fakeObserver) { o.work.Repo = "fork/repo" }} {
		o := goodObserver(ModeResolveIssue)
		mutate(o)
		w := &fakePusher{ancestor: true}
		e := engine(t, goodPolicy(ModeResolveIssue), o, w)
		_, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"})
		if err == nil {
			t.Fatal("accepted wrong pinned destination")
		}
	}
	o := goodObserver(ModeResolveIssue)
	w := &fakePusher{ancestor: true}
	e := engine(t, goodPolicy(ModeResolveIssue), o, w)
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "forged", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
		t.Fatal("accepted foreign run UID")
	}
}
func TestPublishRejectsProtectedAndUnknownDestination(t *testing.T) {
	for _, change := range []func(*RepositoryState){func(s *RepositoryState) { s.Protected = true }, func(s *RepositoryState) { s.ProtectionKnown = false }, func(s *RepositoryState) { s.WriteKnown = false }, func(s *RepositoryState) { s.Writable = false }, func(s *RepositoryState) { s.Default = true }} {
		o := goodObserver(ModeResolveIssue)
		change(&o.work)
		w := &fakePusher{ancestor: true}
		e := engine(t, goodPolicy(ModeResolveIssue), o, w)
		if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
			t.Fatal("accepted unsafe destination")
		}
		if w.pushedOID != "" {
			t.Fatal("pushed before policy rejection")
		}
	}
}
func TestPublishRejectsForeignInitiallyPresentAndRacedAbsentTips(t *testing.T) {
	p := goodPolicy(ModeResolveIssue)
	o := goodObserver(ModeResolveIssue)
	o.work.OID = "foreign"
	w := &fakePusher{ancestor: true}
	e := engine(t, p, o, w)
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "foreign", ProposedOID: "new"}); err == nil {
		t.Fatal("accepted unadmitted initial work tip")
	}

	p.WorkInitiallyAbsent = true
	p.WorkAnchorOID = ""
	o = goodObserver(ModeResolveIssue)
	o.work.Exists = false
	o.work.OID = ""
	w = &fakePusher{ancestor: true}
	e = engine(t, p, o, w)
	o.work.Exists = true
	o.work.OID = "foreign"
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ProposedOID: "new"}); err == nil {
		t.Fatal("adopted a work ref that raced into existence")
	}
}

func TestPublishEnforcesExpectedTipAndFastForward(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected string
		ancestor bool
	}{{"head-race", "other", true}, {"not-ff", "old", false}} {
		t.Run(tc.name, func(t *testing.T) {
			o := goodObserver(ModeResolveIssue)
			w := &fakePusher{ancestor: tc.ancestor}
			e := engine(t, goodPolicy(ModeResolveIssue), o, w)
			_, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: tc.expected, ProposedOID: "new"})
			if err == nil {
				t.Fatal("accepted race/non-FF")
			}
			if w.pushedOID != "" {
				t.Fatal("pushed rejected candidate")
			}
		})
	}
}
func TestPublishRejectsForkHeadAndPRHeadRace(t *testing.T) {
	for _, mutate := range []func(*fakeObserver){func(o *fakeObserver) { o.pr.HeadRepo = "fork/repo" }, func(o *fakeObserver) { o.pr.HeadOID = "raced" }, func(o *fakeObserver) { o.pr.BaseRef = "release" }} {
		o := goodObserver(ModeFixPR)
		mutate(o)
		w := &fakePusher{ancestor: true}
		e := engine(t, goodPolicy(ModeFixPR), o, w)
		if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
			t.Fatal("accepted changed PR identity/head")
		}
	}
}

// TestRestartDoesNotRecoverFromCallerOIDOrAncestry is the crash half of the
// uncertain-push contract. A previous process may have confirmed this exact OID
// in memory — via a successful push or via an uncertain push confirmed by exact
// live equality — but in-memory confirmation is not durable evidence. A fresh
// broker rejects a caller-supplied OID/ancestry that only the caller asserts.
func TestRestartDoesNotRecoverFromCallerOIDOrAncestry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pushErr error
	}{
		{"after-successful-push", nil},
		{"after-confirmed-uncertain-push", errors.New("transport read-back failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := goodObserver(ModeFixPR)
			w := &fakePusher{ancestor: true, pushErr: tc.pushErr}
			first := engine(t, goodPolicy(ModeFixPR), o, w)
			w.afterPush = func() { o.work.OID = "new"; o.pr.HeadOID = "new" }
			got, err := first.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"})
			if err != nil {
				t.Fatalf("first publish failed: %v", err)
			}
			if first.confirmed != "new" {
				t.Fatalf("first process did not confirm exact tip: %q", first.confirmed)
			}
			if got.AlreadyPublished != (tc.pushErr != nil) {
				t.Fatalf("unexpected AlreadyPublished: %#v", got)
			}

			// A fresh broker process has no durable trusted evidence. The caller
			// can repeat the same OID and ancestry still does not establish
			// ownership.
			restarted := engine(t, goodPolicy(ModeFixPR), o, w)
			if _, err := restarted.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "new", ProposedOID: "next"}); err == nil {
				t.Fatal("accepted foreign descendant after restart")
			}
			if _, err := restarted.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "new", ProposedOID: "new"}); err == nil {
				t.Fatal("accepted caller-proposed OID as restart evidence")
			}
			if restarted.confirmed != "" {
				t.Fatalf("rejected caller OID still advanced confirmation: %q", restarted.confirmed)
			}
		})
	}
}

// TestPublishConfirmsUncertainPushWhenExactOIDIsLive covers the same-call case:
// the transport reports an uncertain push, but the mandatory post-push
// observation proves the exact proposed OID is live and every policy check
// passes. Full postflight confirms publication idempotently rather than failing.
func TestPublishConfirmsUncertainPushWhenExactOIDIsLive(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true, pushErr: errors.New("transport read-back failed")}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	w.afterPush = func() { o.work.OID = "new"; o.pr.HeadOID = "new" }
	got, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"})
	if err != nil {
		t.Fatalf("uncertain push with exact live OID rejected: %v", err)
	}
	if got.OID != "new" || !got.AlreadyPublished {
		t.Fatalf("uncertain push not confirmed idempotently: %#v", got)
	}
	if e.confirmed != "new" {
		t.Fatalf("confirmed tip not advanced to exact live OID: %q", e.confirmed)
	}
	// The confirmed exact tip is now admitted, so a follow-on publication from
	// it proceeds without re-supplying trusted evidence.
	w.pushErr = nil
	w.afterPush = func() { o.work.OID = "next"; o.pr.HeadOID = "next" }
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "new", ProposedOID: "next"}); err != nil {
		t.Fatalf("confirmed tip not admitted for follow-on publication: %v", err)
	}
}

// TestPublishConfirmsUncertainPushForNewResolveRef is the resolve-issue,
// initially-absent variant of the same-call case: the confirmation rule is
// mode-agnostic and must not depend on a pre-existing work ref.
func TestPublishConfirmsUncertainPushForNewResolveRef(t *testing.T) {
	p := goodPolicy(ModeResolveIssue)
	p.WorkInitiallyAbsent = true
	p.WorkAnchorOID = ""
	o := goodObserver(ModeResolveIssue)
	o.work.Exists = false
	o.work.OID = ""
	w := &fakePusher{ancestor: true, pushErr: errors.New("transport read-back failed")}
	e := engine(t, p, o, w)
	w.afterPush = func() { o.work.Exists = true; o.work.OID = "new" }
	got, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ProposedOID: "new"})
	if err != nil {
		t.Fatalf("uncertain push creating a new resolve ref rejected: %v", err)
	}
	if got.OID != "new" || !got.AlreadyPublished {
		t.Fatalf("uncertain push not confirmed idempotently: %#v", got)
	}
	if e.confirmed != "new" {
		t.Fatalf("confirmed tip not advanced to exact live OID: %q", e.confirmed)
	}
}

// TestPublishDropsConfirmationWhenPostPushPRCheckFails covers an abnormal
// postflight: the work ref advances to the proposal but the pinned PR head does
// not follow, so publication must fail and no in-process confirmation may
// survive to be reused as evidence for the next observation.
func TestPublishDropsConfirmationWhenPostPushPRCheckFails(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	w.afterPush = func() { o.work.OID = "new" } // PR head stays at "old"
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
		t.Fatal("accepted post-push PR head mismatch")
	}
	if e.confirmed != "" {
		t.Fatalf("failed postflight left in-process confirmation: %q", e.confirmed)
	}
}

// TestPublishDropsConfirmationWhenPostPushBaseAncestryFails covers the other
// abnormal postflight branch: the exact OID is live and the PR identity is
// intact, but the proposal no longer includes the live base tip. Publication
// must fail and, like the PR-check branch, clear any in-process confirmation.
func TestPublishDropsConfirmationWhenPostPushBaseAncestryFails(t *testing.T) {
	o := goodObserver(ModeResolveIssue)
	// Pre-push base-inclusion and fast-forward checks pass; the post-push
	// base-inclusion recheck fails.
	w := &fakePusher{ancestorSeq: []bool{true, true, false}}
	e := engine(t, goodPolicy(ModeResolveIssue), o, w)
	w.afterPush = func() { o.work.OID = "new" }
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
		t.Fatal("accepted post-push base ancestry failure")
	}
	if e.confirmed != "" {
		t.Fatalf("failed postflight left in-process confirmation: %q", e.confirmed)
	}
}

// TestPublishRejectsUncertainPushWithoutExactLiveOID is the negative half: an
// uncertain push whose exact proposed OID is not live must still fail closed and
// leave no in-process confirmation behind.
func TestPublishRejectsUncertainPushWithoutExactLiveOID(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true, pushErr: errors.New("transport read-back failed")}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	w.afterPush = func() { o.work.OID = "foreign"; o.pr.HeadOID = "foreign" }
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err == nil {
		t.Fatal("confirmed an uncertain push whose exact proposed OID is not live")
	}
	if e.confirmed != "" {
		t.Fatalf("uncertain push advanced in-memory confirmed tip: %q", e.confirmed)
	}
	// The unconfirmed foreign head is not admitted: the next attempt fails.
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "foreign", ProposedOID: "next"}); err == nil {
		t.Fatal("accepted unconfirmed head")
	}
}
func TestUpdateFixPRGuardsPinnedIdentity(t *testing.T) {
	o := goodObserver(ModeFixPR)
	e := engine(t, goodPolicy(ModeFixPR), o, &fakePusher{})
	if err := e.UpdateFixPR(context.Background(), UpdatePullRequest{Title: "reviewed"}); err != nil {
		t.Fatalf("allowed metadata update rejected: %v", err)
	}
	if o.updated.Title != "reviewed" {
		t.Fatal("pinned PR was not updated")
	}
	for _, mutate := range []func(*fakeObserver){
		func(o *fakeObserver) { o.pr.HeadRepo = "foreign/repo" },
		func(o *fakeObserver) { o.work.OID = "foreign" },
	} {
		candidate := goodObserver(ModeFixPR)
		mutate(candidate)
		if err := engine(t, goodPolicy(ModeFixPR), candidate, &fakePusher{}).UpdateFixPR(context.Background(), UpdatePullRequest{Body: "body"}); err == nil {
			t.Fatal("accepted foreign PR identity or head")
		}
	}
	if err := e.UpdateFixPR(context.Background(), UpdatePullRequest{}); err == nil {
		t.Fatal("accepted empty update")
	}
	if err := engine(t, goodPolicy(ModeResolveIssue), goodObserver(ModeResolveIssue), &fakePusher{}).UpdateFixPR(context.Background(), UpdatePullRequest{Title: "bad"}); err == nil {
		t.Fatal("resolve-issue updated a PR")
	}
}

func TestUpdateFixPRAllowsConfirmedHeadAfterPublishAndRejectsForeignTip(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	// A successful publish advances the PR and work tips to "new" and confirms it.
	w.afterPush = func() { o.work.OID = "new"; o.pr.HeadOID = "new" }
	if _, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"}); err != nil {
		t.Fatalf("publish old->new failed: %v", err)
	}
	// Metadata update at the exact new PR/work tip must now succeed.
	if err := e.UpdateFixPR(context.Background(), UpdatePullRequest{Title: "t2", Body: "b2"}); err != nil {
		t.Fatalf("metadata update at confirmed new tip rejected: %v", err)
	}
	if o.updated.Title != "t2" || o.updated.Body != "b2" {
		t.Fatalf("pinned PR was not updated: %#v", o.updated)
	}
	// A foreign tip this run never published is not admitted.
	o.work.OID = "foreign"
	o.pr.HeadOID = "foreign"
	if err := e.UpdateFixPR(context.Background(), UpdatePullRequest{Title: "x"}); err == nil {
		t.Fatal("accepted foreign head for metadata update")
	}
}

func TestNewEngineFailsClosedOnMissingCapabilityAndBadPolicy(t *testing.T) {
	if _, err := NewPolicyEngine(goodPolicy(ModeFixPR), goodObserver(ModeFixPR), nil); err == nil {
		t.Fatal("missing pusher accepted")
	}
	p := goodPolicy(ModeFixPR)
	p.WorkRepo = ""
	if _, err := NewPolicyEngine(p, goodObserver(ModeFixPR), &fakePusher{}); err == nil {
		t.Fatal("incomplete policy accepted")
	}
}
func TestCreatePRGuardsPinnedHeadAndReturnedIdentity(t *testing.T) {
	o := goodObserver(ModeResolveIssue)
	o.work.OID = "candidate"
	w := &fakePusher{}
	e := engine(t, goodPolicy(ModeResolveIssue), o, w)
	// The body must satisfy linkage before the engine ever touches
	// the work ref; "body" by itself is rejected before the head
	// guard runs, so the first error is the linkage rejection. Use
	// a body with valid linkage to exercise the head guard.
	if _, _, err := e.CreatePullRequest(context.Background(), "candidate", "title", "body", false); err == nil {
		t.Fatal("create accepted with missing linkage")
	}
	if _, _, err := e.CreatePullRequest(context.Background(), "candidate", "title", "Closes org/repo#502", false); err == nil {
		t.Fatal("create accepted without exact pinned work tip")
	}
	o.work.Exists = false
	o.work.OID = ""
	o.pulls = []PullRequestState{{Number: 9, HeadRepo: "fork/repo", HeadRef: "courier/work"}}
	if _, _, err := e.CreatePullRequest(context.Background(), "candidate", "title", "Closes org/repo#502", false); err == nil {
		t.Fatal("ignored same-name fork PR")
	}
}

// TestCreatePREnforcesSourceIssueLinkageBeforeForgeWrite is the broker half
// of the resolve-issue linkage contract. The linkage check runs first, so a
// body that names the wrong issue, a different source repository, an
// ambiguous multiple-match body, or no supported closing keyword at all is
// rejected before the broker calls CreatePullRequest on the underlying
// observer. The observer's CreatePullRequest is therefore never reached for
// an invalid body.
func TestCreatePREnforcesSourceIssueLinkageBeforeForgeWrite(t *testing.T) {
	o := goodObserver(ModeResolveIssue)
	o.work.OID = "candidate"
	w := &fakePusher{}
	e := engine(t, goodPolicy(ModeResolveIssue), o, w)
	cases := []struct {
		name string
		body string
	}{
		{"no-closing-reference", "This change ships the icon assets and updates the manifest."},
		{"vague-addresses", "Addresses #502"},
		{"wrong-issue-number", "Closes #999"},
		{"wrong-source-repository", "Closes other/repo#502"},
		{"ambiguous-multiple", "Closes #502. Closes #503."},
		{"substring-fake", "discloses #502"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := e.CreatePullRequest(context.Background(), "candidate", "title", tc.body, false); err == nil {
				t.Fatalf("create accepted body %q without valid linkage", tc.body)
			}
			if o.created.Body != "" {
				t.Fatalf("observer CreatePullRequest called with body %q despite invalid linkage", o.created.Body)
			}
		})
	}
}

func TestCreatePRAcceptsValidLinkage(t *testing.T) {
	o := goodObserver(ModeResolveIssue)
	w := &fakePusher{}
	e := engine(t, goodPolicy(ModeResolveIssue), o, w)
	// For a body that satisfies linkage, the linkage check must
	// not be the cause of any error. The fake observer's identity
	// does not match the pinned policy, so CreatePullRequest will
	// fail at the post-create identity check; that is fine — the
	// test only proves the failure is not a linkage rejection. The
	// observer is reached (o.created is set) once linkage has
	// passed and the head guards have accepted "old" as the
	// admitted anchor, so an empty created body indicates a
	// non-linkage guard rejected before the forge write.
	for _, body := range []string{
		"Closes #502",
		"Fixes #502",
		"Closes org/repo#502",
		"This change ships the icon assets. Closes #502.\n",
	} {
		o.created = CreatePullRequest{}
		_, _, err := e.CreatePullRequest(context.Background(), "old", "title", body, false)
		if err != nil {
			var le *LinkageError
			if errors.As(err, &le) {
				t.Fatalf("body %q rejected by linkage: %v", body, err)
			}
			continue
		}
		if o.created.Body == "" {
			t.Fatalf("body %q: accepted without reaching the observer", body)
		}
	}
}

func TestValidatePolicyRejectsResolveIssueWithoutSourceIssue(t *testing.T) {
	p := goodPolicy(ModeResolveIssue)
	p.SourceIssue = SourceIssue{}
	if _, err := NewPolicyEngine(p, goodObserver(ModeResolveIssue), &fakePusher{}); err == nil {
		t.Fatal("resolve-issue policy without SourceIssue accepted")
	}
}

func TestValidatePolicyRejectsResolveIssueWithMismatchedSourceRepo(t *testing.T) {
	p := goodPolicy(ModeResolveIssue)
	p.SourceIssue = SourceIssue{Owner: "other", Name: "repo", Number: 502}
	if _, err := NewPolicyEngine(p, goodObserver(ModeResolveIssue), &fakePusher{}); err == nil {
		t.Fatal("resolve-issue policy with mismatched source repository accepted")
	}
}

// --- criterion-evidence boundary at CreatePullRequest (#281) ---

// adoptObserver completes the create flow: its CreatePullRequest adopts the
// created identity so the engine's full post-create verification (read-back,
// head recheck) succeeds and a passing body can be asserted end to end.
type adoptObserver struct{ *fakeObserver }

func (a *adoptObserver) CreatePullRequest(ctx context.Context, in CreatePullRequest) (int, error) {
	number, err := a.fakeObserver.CreatePullRequest(ctx, in)
	if err != nil {
		return number, err
	}
	a.pr = PullRequestState{Number: number, State: "open", BaseRepo: in.BaseRepo, BaseRef: in.BaseRef, HeadRepo: in.HeadRepo, HeadRef: in.HeadRef, HeadOID: a.work.OID, Title: in.Title, Body: in.Body, Draft: in.Draft}
	a.pulls = []PullRequestState{a.pr}
	return number, nil
}

func criteriaEngine(t *testing.T, p Policy, o *adoptObserver) *PolicyEngine {
	t.Helper()
	e, err := NewPolicyEngine(p, o, &fakePusher{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func galleryResolvePolicy(set CriterionSet) Policy {
	return Policy{RunUID: "uid-1", Mode: ModeResolveIssue, Provider: "gh", BaseRepo: "misospace/miso-gallery", BaseRef: "main", BaseOID: "base1", WorkRepo: "misospace/miso-gallery", WorkRef: "courier/work", WorkAnchorOID: "old", SourceIssue: misoGalleryIssue, Criteria: set}
}

func galleryResolveObserver() *adoptObserver {
	return &adoptObserver{fakeObserver: &fakeObserver{
		base: RepositoryState{Repo: "misospace/miso-gallery", Ref: "main", Exists: true, OID: "base2", Default: true, Protected: true},
		work: RepositoryState{Repo: "misospace/miso-gallery", Ref: "courier/work", Exists: true, OID: "old", ProtectionKnown: true, Writable: true, WriteKnown: true},
	}}
}

// TestCreatePREnforcesCriterionEvidenceBeforeForgeWrite is the native
// boundary of #281: every violation class is refused with the structured
// *CriteriaError naming the defect, and the observer's CreatePullRequest —
// the only forge write on this path — is never reached.
func TestCreatePREnforcesCriterionEvidenceBeforeForgeWrite(t *testing.T) {
	set := pinMisoGallerySet(t)
	honest := loadFixture(t, "miso-gallery-505-pr-honest.md")
	cases := []struct {
		name   string
		body   string
		assert func(*testing.T, *CriteriaError)
	}{
		{"block-missing", "Closes misospace/miso-gallery#502.\n\nShips the assets; the manifest is updated too.", func(t *testing.T, e *CriteriaError) {
			if !errors.Is(e, ErrCriteriaBlockMissing) {
				t.Errorf("want ErrCriteriaBlockMissing sentinel, reason %q", e.Reason)
			}
		}},
		{"missing-criterion-and-coverage-lie", loadFixture(t, "miso-gallery-505-pr-partial.md"), func(t *testing.T, e *CriteriaError) {
			if len(e.Missing) != 1 || e.Missing[0] != "AC-2" {
				t.Errorf("missing = %v, want [AC-2]", e.Missing)
			}
			if e.CoverageClaim != CoverageFull || e.CoverageDerived != CoveragePartial {
				t.Errorf("coverage claim %q derived %q: omitting AC-2 cannot claim full", e.CoverageClaim, e.CoverageDerived)
			}
		}},
		{"unknown-criterion", honest + "\n- AC-4 " + digestString("invented") + ": unknown\n", func(t *testing.T, e *CriteriaError) {
			if len(e.Unknown) != 1 || e.Unknown[0] != "AC-4" {
				t.Errorf("unknown = %v, want [AC-4]; the model cannot add criteria", e.Unknown)
			}
		}},
		{"malformed-trailing-row", honest + "\n- AC-\n", func(t *testing.T, e *CriteriaError) {
			if !strings.Contains(e.Reason, "malformed row") {
				t.Errorf("structural block defects must also travel as the structured type: %q", e.Reason)
			}
		}},
		{"stale-set-digest", strings.Replace(honest, set.Digest, "sha256:"+strings.Repeat("a", 64), 1), func(t *testing.T, e *CriteriaError) {
			if !strings.Contains(e.Reason, "the admitted criterion set is "+set.Digest) {
				t.Errorf("reason must name the admitted set digest: %q", e.Reason)
			}
		}},
		{"regression-check-without-location", strings.Replace(honest, "regression-check - `Dockerfile` `COPY assets/ assets/` ships the three PNGs; `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` fails if that COPY line is removed", "regression-check - inspected by hand, no automation", 1), func(t *testing.T, e *CriteriaError) {
			if len(e.BadStatus) != 1 || e.BadStatus[0] != "AC-1" {
				t.Errorf("bad status = %v, want [AC-1] (automated claim with no citable location)", e.BadStatus)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := galleryResolveObserver()
			e := criteriaEngine(t, galleryResolvePolicy(set), o)
			_, report, err := e.CreatePullRequest(context.Background(), "old", "title", tc.body, false)
			if err == nil {
				t.Fatalf("create accepted a %s body", tc.name)
			}
			var cerr *CriteriaError
			if !errors.As(err, &cerr) {
				t.Fatalf("want *CriteriaError, got %T: %v", err, err)
			}
			if report != nil {
				t.Error("a rejected body must not surface a report")
			}
			if o.created.Body != "" {
				t.Fatalf("observer CreatePullRequest reached the forge despite %s rejection", tc.name)
			}
			tc.assert(t, cerr)
		})
	}
}

// TestCreatePRLinkagePrecedesCriterionEvidence pins the boundary order with
// bodies that fail both contracts at once: whichever order enforcement
// happened in, the answer must come from the linkage contract, never the
// criteria contract.
func TestCreatePRLinkagePrecedesCriterionEvidence(t *testing.T) {
	set := pinMisoGallerySet(t)
	o := galleryResolveObserver()
	e := criteriaEngine(t, galleryResolvePolicy(set), o)
	for name, body := range map[string]string{
		"no-block":      "Addresses the icon assets.",
		"perfect-block": "Addresses the icon assets.\n\n" + honestBlockFor(set),
	} {
		_, _, err := e.CreatePullRequest(context.Background(), "old", "title", body, false)
		if err == nil {
			t.Fatalf("%s: create accepted a body without a closing reference", name)
		}
		var le *LinkageError
		if !errors.As(err, &le) {
			t.Fatalf("%s: want linkage rejection, got %v", name, err)
		}
		var cerr *CriteriaError
		if errors.As(err, &cerr) {
			t.Fatalf("%s: criteria contract answered before linkage: %v", name, err)
		}
	}
}

func honestBlockFor(set CriterionSet) string {
	var b strings.Builder
	b.WriteString("courier-criteria:v1 source=" + strings.ToLower(set.Issue.Canonical()) + "#" + strconv.Itoa(set.Issue.Number) + " set=" + set.Digest + " coverage=full\n\n")
	for _, c := range set.Criteria {
		b.WriteString("- " + c.ID + " " + c.Digest + ": regression-check - internal/broker/criteria.go covers " + c.ID + "\n")
	}
	return b.String()
}

// TestCreatePRAcceptsHonestCriterionEvidence covers the two honest records
// from the shared truth table: both pass the native boundary, reach the
// forge write, and the parsed report surfaces its derived coverage to the
// caller — partial stays visible as partial.
func TestCreatePRAcceptsHonestCriterionEvidence(t *testing.T) {
	set := pinMisoGallerySet(t)
	for _, tc := range []struct{ fixture, want string }{
		{"miso-gallery-505-pr-honest.md", CoverageFull},
		{"miso-gallery-505-pr-honest-partial.md", CoveragePartial},
	} {
		t.Run(tc.want, func(t *testing.T) {
			body := loadFixture(t, tc.fixture)
			o := galleryResolveObserver()
			e := criteriaEngine(t, galleryResolvePolicy(set), o)
			pr, report, err := e.CreatePullRequest(context.Background(), "old", "title", body, false)
			if err != nil {
				t.Fatalf("honest %s record rejected: %v", tc.want, err)
			}
			if report == nil || report.Coverage != tc.want || report.DerivedCoverage(set) != tc.want {
				t.Fatalf("report = %+v, want derived coverage %s surfaced to the caller", report, tc.want)
			}
			if o.created.Body != body || pr.Number != 42 {
				t.Fatalf("forge write mismatch: created=%+v pr=%+v", o.created, pr)
			}
		})
	}
}

// TestCreatePRCriterionEvidenceVacuousPaths: a pinned empty set and an
// unpinned pre-contract policy are both vacuous passes — linkage remains
// the only body contract, with no report and no refusal.
func TestCreatePRCriterionEvidenceVacuousPaths(t *testing.T) {
	empty, err := ExtractCriteria("No acceptance section here.\n\n- this is not an acceptance section", misoGalleryIssue)
	if err != nil || !empty.IsEmpty() || !empty.IsPinned() {
		t.Fatalf("want a pinned empty set, got %+v err %v", empty, err)
	}
	for name, set := range map[string]CriterionSet{"pinned-empty": empty, "unpinned": {}} {
		t.Run(name, func(t *testing.T) {
			o := galleryResolveObserver()
			e := criteriaEngine(t, galleryResolvePolicy(set), o)
			_, report, err := e.CreatePullRequest(context.Background(), "old", "title", "Closes #502. Ships the assets with no evidence block.", false)
			if err != nil {
				t.Fatalf("vacuous %s policy rejected: %v", name, err)
			}
			if report != nil {
				t.Errorf("no block was required; report = %+v", report)
			}
			if o.created.Body == "" {
				t.Fatal("observer was not reached")
			}
		})
	}
}

// --- contribute mode: fork-backed publication (#599) ---

func contributePolicy() Policy {
	return Policy{RunUID: "uid-1", Mode: ModeContribute, Provider: "gh", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base1", WorkRepo: "fork/repo", WorkRef: "courier/org/repo/goal-abc", WorkAnchorOID: "old", SourceIssue: SourceIssue{Owner: "org", Name: "repo", Number: 502}}
}

func forkObserver() *adoptObserver {
	return &adoptObserver{fakeObserver: &fakeObserver{
		base: RepositoryState{Repo: "org/repo", Ref: "main", Exists: true, OID: "base2", Default: true, Protected: true},
		work: RepositoryState{Repo: "fork/repo", Ref: "courier/org/repo/goal-abc", Exists: true, OID: "old", ProtectionKnown: true, Writable: true, WriteKnown: true},
	}}
}

func TestValidatePolicyContributeRequiresDistinctFork(t *testing.T) {
	p := contributePolicy()
	p.WorkRepo = p.BaseRepo
	if _, err := NewPolicyEngine(p, forkObserver(), &fakePusher{}); err == nil {
		t.Fatal("contribute policy with the base as its own head repository accepted")
	}
}

func TestValidatePolicyContributeOptionalSourceIssue(t *testing.T) {
	p := contributePolicy()
	p.SourceIssue = SourceIssue{}
	if _, err := NewPolicyEngine(p, forkObserver(), &fakePusher{}); err != nil {
		t.Fatalf("contribute policy without upstream issue context rejected: %v", err)
	}
	for _, mutate := range []func(*Policy){
		func(p *Policy) { p.SourceIssue = SourceIssue{Owner: "other", Name: "repo", Number: 502} },
		func(p *Policy) { p.SourceIssue = SourceIssue{Owner: "org", Name: "repo", Number: 0} },
		func(p *Policy) { p.SourceIssue = SourceIssue{Owner: "org", Name: "repo", Number: -3} },
	} {
		bad := contributePolicy()
		mutate(&bad)
		if _, err := NewPolicyEngine(bad, forkObserver(), &fakePusher{}); err == nil {
			t.Fatal("incomplete or mismatched contribute source-issue accepted")
		}
	}
}

func TestValidatePolicyContributeRejectsFixPRAnchorsAndUnbackedClosingChoice(t *testing.T) {
	for _, mutate := range []func(*Policy){
		func(p *Policy) { p.PRNumber = 7 },
		func(p *Policy) { p.HeadAnchorOID = "old" },
		func(p *Policy) { p.AllowClosingLinkage = true; p.SourceIssue = SourceIssue{} },
	} {
		bad := contributePolicy()
		mutate(&bad)
		if _, err := NewPolicyEngine(bad, forkObserver(), &fakePusher{}); err == nil {
			t.Fatal("invalid contribute policy accepted")
		}
	}
}

func TestCreatePRContributeRejectsClosingLinkageWithoutPolicyChoice(t *testing.T) {
	o := forkObserver()
	e := criteriaEngine(t, contributePolicy(), o)
	for _, body := range []string{"Fixes #502", "Closes org/repo#502", "Resolves #502"} {
		_, _, err := e.CreatePullRequest(context.Background(), "old", "title", body, false)
		if err == nil {
			t.Fatalf("contribute create accepted closing body %q without an admission-time choice", body)
		}
		if !errors.Is(err, ErrClosingLinkageNotAdmitted) {
			t.Fatalf("err = %v, want ErrClosingLinkageNotAdmitted", err)
		}
	}
	if o.created.Body != "" {
		t.Fatalf("observer CreatePullRequest reached the forge despite unadmitted closing linkage: %q", o.created.Body)
	}
	// The non-closing default reaches the forge write.
	if _, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Refs #502. Ships the contribution.", false); err != nil {
		t.Fatalf("non-closing default rejected: %v", err)
	}
	if o.created.Body == "" {
		t.Fatal("observer was not reached for the non-closing default")
	}
}

func TestCreatePRContributeAdmitsPolicyGatedClosingLinkage(t *testing.T) {
	p := contributePolicy()
	p.AllowClosingLinkage = true
	o := forkObserver()
	e := criteriaEngine(t, p, o)
	if _, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Fixes #502. Ships the contribution.", false); err != nil {
		t.Fatalf("policy-gated closing linkage rejected: %v", err)
	}
	if o.created.Body == "" {
		t.Fatal("observer was not reached for admitted closing linkage")
	}
	for _, body := range []string{"Fixes #999", "Closes other/repo#502", "Fixes #502. Fixes #503."} {
		o2 := forkObserver()
		e2 := criteriaEngine(t, p, o2)
		if _, _, err := e2.CreatePullRequest(context.Background(), "old", "title", body, false); err == nil {
			t.Fatalf("closing body %q accepted despite disagreeing with the trusted identity", body)
		}
		if o2.created.Body != "" {
			t.Fatalf("observer reached the forge despite invalid closing body %q", body)
		}
	}
}

func TestCreatePRContributeLinkageOptionalWithoutIssueContext(t *testing.T) {
	p := contributePolicy()
	p.SourceIssue = SourceIssue{}
	o := forkObserver()
	e := criteriaEngine(t, p, o)
	if _, _, err := e.CreatePullRequest(context.Background(), "old", "title", "A goal summary with no issue anywhere.", false); err != nil {
		t.Fatalf("contribution without issue context rejected: %v", err)
	}
	o2 := forkObserver()
	e2 := criteriaEngine(t, p, o2)
	if _, _, err := e2.CreatePullRequest(context.Background(), "old", "title", "Fixes #7", false); err == nil {
		t.Fatal("closing reference accepted with no trusted identity")
	}
	if o2.created.Body != "" {
		t.Fatal("observer reached the forge despite a closing reference with no trusted identity")
	}
}

func pinnedDiscoveryPR(number int, mutate func(*PullRequestState)) PullRequestState {
	pr := PullRequestState{Number: number, State: "open", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base2", HeadRepo: "fork/repo", HeadRef: "courier/org/repo/goal-abc", HeadOID: "old"}
	if mutate != nil {
		mutate(&pr)
	}
	return pr
}

func TestCreatePRRediscoveryAdoptsSamePinnedPullRequest(t *testing.T) {
	o := forkObserver()
	o.pulls = []PullRequestState{pinnedDiscoveryPR(5, nil)}
	e := criteriaEngine(t, contributePolicy(), o)
	got, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Refs #502", false)
	if err != nil {
		t.Fatalf("rediscovery failed: %v", err)
	}
	if got.Number != 5 || got.HeadRepo != "fork/repo" || got.HeadOID != "old" {
		t.Fatalf("rediscovered PR = %#v, want the existing fork-head PR 5", got)
	}
	if o.created.Body != "" {
		t.Fatal("a duplicate create reached the forge despite rediscovery")
	}
}

func TestCreatePRRediscoveryFailsClosedOnAmbiguousOrNonMatchingHead(t *testing.T) {
	cases := []struct {
		name   string
		pulls  []PullRequestState
		reason string
	}{
		{"ambiguous", []PullRequestState{pinnedDiscoveryPR(5, nil), pinnedDiscoveryPR(6, nil)}, "multiple pull requests claim the pinned head"},
		{"closed", []PullRequestState{pinnedDiscoveryPR(5, func(pr *PullRequestState) { pr.State = "closed" })}, "does not match the pinned identity"},
		{"head-moved", []PullRequestState{pinnedDiscoveryPR(5, func(pr *PullRequestState) { pr.HeadOID = "stale" })}, "does not match the pinned identity"},
		{"base-moved", []PullRequestState{pinnedDiscoveryPR(5, func(pr *PullRequestState) { pr.BaseRef = "release" })}, "does not match the pinned identity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := forkObserver()
			o.pulls = tc.pulls
			e := criteriaEngine(t, contributePolicy(), o)
			_, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Refs #502", false)
			if err == nil {
				t.Fatal("create proceeded despite a non-adoptable pinned head listing")
			}
			if !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err = %v, want reason %q", err, tc.reason)
			}
			if o.created.Body != "" {
				t.Fatal("a forge create reached the observer despite the failed-closed discovery")
			}
		})
	}
}

func TestCreatePRDiscoveryIgnoresForeignHeadClaims(t *testing.T) {
	// A record that does not claim the pinned full head identity (a
	// same-name fork or another branch) neither adopts nor blocks: create
	// proceeds under the normal guards.
	o := forkObserver()
	o.pulls = []PullRequestState{pinnedDiscoveryPR(9, func(pr *PullRequestState) { pr.HeadRepo = "other/repo" })}
	e := criteriaEngine(t, contributePolicy(), o)
	got, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Refs #502", false)
	if err != nil {
		t.Fatalf("create blocked by a foreign head claim: %v", err)
	}
	if got.Number != 42 || o.created.Body == "" {
		t.Fatalf("expected the normal create path, got %#v", got)
	}
}

func TestResolveIssueRediscoveryAdoptsSamePinnedPullRequest(t *testing.T) {
	// The discovery-before-create boundary is shared: a resolve-issue
	// re-publish after a create whose number was never recorded adopts the
	// existing same-identity PR instead of failing or duplicating.
	o := goodObserver(ModeResolveIssue)
	o.pulls = []PullRequestState{{Number: 11, State: "open", BaseRepo: "org/repo", BaseRef: "main", BaseOID: "base2", HeadRepo: "org/repo", HeadRef: "courier/work", HeadOID: "old"}}
	e := engine(t, goodPolicy(ModeResolveIssue), o, &fakePusher{})
	got, _, err := e.CreatePullRequest(context.Background(), "old", "title", "Closes #502", false)
	if err != nil {
		t.Fatalf("rediscovery failed: %v", err)
	}
	if got.Number != 11 {
		t.Fatalf("rediscovered PR = %#v, want 11", got)
	}
	if o.created.Body != "" {
		t.Fatal("a duplicate create reached the forge despite rediscovery")
	}
}
