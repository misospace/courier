package broker

import (
	"context"
	"errors"
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
	pushErr                          error
	pushedRepo, pushedRef, pushedOID string
	afterPush                        func()
}

func (f *fakePusher) IsAncestor(_ context.Context, _, ancestor, descendant string) (bool, error) {
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
func TestPublishRequiresPostPushExactOIDAndHandlesUncertainIdempotently(t *testing.T) {
	o := goodObserver(ModeFixPR)
	w := &fakePusher{ancestor: true, pushErr: errors.New("secret token")}
	e := engine(t, goodPolicy(ModeFixPR), o, w)
	w.afterPush = func() { o.work.OID = "new"; o.pr.HeadOID = "new" }
	got, err := e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "old", ProposedOID: "new"})
	if err != nil || !got.AlreadyPublished {
		t.Fatalf("exact uncertain push not recovered: %#v %v", got, err)
	}
	o.work.OID = "foreign"
	o.pr.HeadOID = "foreign"
	_, err = e.Publish(context.Background(), PublicationRequest{RunUID: "uid-1", ExpectedWorkOID: "new", ProposedOID: "next"})
	if err == nil {
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
	if _, err := e.CreatePullRequest(context.Background(), "candidate", "title", "body", false); err == nil {
		t.Fatal("create accepted without exact pinned work tip")
	}
	o.work.Exists = false
	o.work.OID = ""
	o.pulls = []PullRequestState{{Number: 9, HeadRepo: "fork/repo", HeadRef: "courier/work"}}
	if _, err := e.CreatePullRequest(context.Background(), "candidate", "title", "body", false); err == nil {
		t.Fatal("ignored same-name fork PR")
	}
}
