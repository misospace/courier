package broker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
)

// External-contribution security/failure matrix (#600). Each row drives the
// broker publication engine (forge adapter + policy engine) through the
// shared forgetest double and asserts both the outcome and the recorded
// forge ops, so a row proves what the engine did, not just what it
// reported. Rows whose transport-level or restart variants already have
// tests elsewhere are cited beside them:
//
//	 1. approved publication writes only the PR
//	    -> TestExternalMatrixApprovedPublicationWritesOnlyThePullRequest
//	    (flow: TestBrokerIntegrationResolvePublishAndCreatePR, integration_test.go)
//	 2. deficient body reaches no forge
//	    -> TestExternalMatrixDeficientBodyReachesNoForge
//	    (refusal: TestCreatePREnforcesSourceIssueLinkageBeforeForgeWrite, policy_test.go)
//	 3. issue context does not change publication
//	    -> TestExternalMatrixIssueContextDoesNotChangePublication
//	 4. pinned destination cannot be redirected
//	    -> TestExternalMatrixDestinationOutsidePinnedPolicyIsRefused (adapter guard)
//	    -> TestExternalMatrixProtectedDestinationDeniesBeforePush (engine guard)
//	    (transport pin: TestGitPusherUsesOnlyPinnedEndpointAndExactRef,
//	    TestGitPusherRejectsHostilePinsAndNonFastForward, git_transport_test.go;
//	    protected-ref denial: TestPublishRejectsProtectedAndUnknownDestination, policy_test.go)
//	 5. stale run UID denies before any forge contact
//	    -> TestExternalMatrixStaleRunUIDDeniesBeforeAnyForgeContact
//	    (gate: TestPublishRejectsRunUIDAndWrongRefOrRepository, policy_test.go)
//	 6. concurrent retries create exactly one PR
//	    -> TestExternalMatrixConcurrentRetriesCreateExactlyOnePR
//	    (raced duplicate: TestBrokerIntegrationDeniesConcurrentDuplicatePR, integration_test.go)
//	 7. lost create response creates no duplicate PR
//	    -> TestExternalMatrixLostCreateResponseCreatesNoDuplicatePR
//	    (restart-after-PR-creation ownership: TestRestartDoesNotRecoverFromCallerOIDOrAncestry,
//	    policy_test.go)
//	 8. uncertain push confirms idempotently without duplicate PR
//	    -> TestExternalMatrixUncertainPushConfirmsIdempotentlyWithoutDuplicatePR
//	    (TestPublishConfirmsUncertainPushWhenExactOIDIsLive,
//	    TestPublishConfirmsUncertainPushForNewResolveRef, policy_test.go)
//	 9. transient forge lookup fails closed then recovers
//	    -> TestExternalMatrixTransientForgeLookupFailsClosedThenRecovers
//	10. missing fork denies publication
//	    -> TestExternalMatrixMissingForkDeniesPublication
//	11. no write permission denies publication
//	    -> TestExternalMatrixNoWritePermissionDeniesPublication
//	    (unknown permission: TestPublishRejectsProtectedAndUnknownDestination, policy_test.go)
//	12. changed upstream base denies publication
//	    -> TestExternalMatrixChangedUpstreamBaseDeniesPublication
//	    (ancestry: TestPublishEnforcesExpectedTipAndFastForward, policy_test.go)
//	13. existing PR for pinned head is adopted, not duplicated
//	    -> TestExternalMatrixExistingPRForPinnedHeadIsAdoptedNotDuplicated
//	    (TestCreatePRGuardsPinnedHeadAndReturnedIdentity, policy_test.go)
//	14. stale branch ownership denies publication
//	    -> TestExternalMatrixStaleBranchOwnershipDeniesPublication
//	    (foreign tip: TestPublishRejectsForeignInitiallyPresentAndRacedAbsentTips, policy_test.go)
//	15. closed or merged upstream work denies fix-PR update
//	    -> TestExternalMatrixClosedOrMergedUpstreamWorkDeniesFixPR
//	    (TestUpdateFixPRGuardsPinnedIdentity, policy_test.go)

const (
	matrixBaseCanonical = "up/base"
	matrixWorkCanonical = "up/fork"
	matrixBaseRef       = "main"
	matrixWorkRef       = "courier/run"
	matrixBaseOID       = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
	matrixAnchorOID     = "a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2"
	matrixProposedOID   = "a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"
	matrixForeignOID    = "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"
)

func defaultMatrixConfig() forgetest.Config {
	return forgetest.Config{
		Provider:     forge.ProviderConfig{Name: "test"},
		Repositories: []forge.Repository{{ID: "repo-base", Canonical: matrixBaseCanonical, DefaultRef: matrixBaseRef}, {ID: "repo-fork", Canonical: matrixWorkCanonical, DefaultRef: matrixBaseRef}},
		Refs: map[string]map[string]string{
			matrixBaseCanonical: {matrixBaseRef: matrixBaseOID},
			matrixWorkCanonical: {matrixWorkRef: matrixAnchorOID},
		},
	}
}

func defaultMatrixPolicy() Policy {
	return Policy{
		RunUID:              "run-1",
		Mode:                ModeResolveIssue,
		Provider:            "test",
		BaseRepo:            matrixBaseCanonical,
		BaseRef:             matrixBaseRef,
		BaseOID:             matrixBaseOID,
		WorkRepo:            matrixWorkCanonical,
		WorkRef:             matrixWorkRef,
		WorkInitiallyAbsent: false,
		WorkAnchorOID:       matrixAnchorOID,
		SourceIssue:         SourceIssue{Owner: "up", Name: "base", Number: 123},
	}
}

// matrixPusher is the recording normal pusher for the matrix fixture. It
// applies the pushed ref move to the fake world (so post-push observation
// sees it) and reports the caller-supplied outcome separately, which is
// exactly the split the engine relies on: the transport's report never
// decides the outcome, the post-push world does.
type matrixPusher struct {
	fake         *forgetest.Fake
	baseRepo     string
	workRepo     string
	workRef      string
	baseAncestor bool
	workAncestor bool
	applyMove    bool
	pushErr      error
	pushCount    int
}

var _ Pusher = (*matrixPusher)(nil)

func newMatrixPusher(fake *forgetest.Fake) *matrixPusher {
	return &matrixPusher{
		fake:         fake,
		baseRepo:     matrixBaseCanonical,
		workRepo:     matrixWorkCanonical,
		workRef:      matrixWorkRef,
		baseAncestor: true,
		workAncestor: true,
		applyMove:    true,
	}
}

func (m *matrixPusher) IsAncestor(_ context.Context, repo, _, _ string) (bool, error) {
	if repo == m.baseRepo {
		return m.baseAncestor, nil
	}
	return m.workAncestor, nil
}

func (m *matrixPusher) Push(_ context.Context, repo, ref, oid string) error {
	m.pushCount++
	if m.applyMove && repo == m.workRepo && ref == m.workRef {
		m.fake.SetRef(m.workRepo, m.workRef, oid)
	}
	return m.pushErr
}

// newMatrixFixture wires the default matrix world: forgetest-seeded base and
// fork, forge adapter, recording pusher, and policy engine over the default
// resolve-issue policy. mutate may adjust the seeded fake and the policy
// before the engine is built.
func newMatrixFixture(t *testing.T, mutate func(f *forgetest.Fake, p *Policy)) (*forgetest.Fake, *matrixPusher, *PolicyEngine) {
	t.Helper()
	return newMatrixFixtureWithConfig(t, defaultMatrixConfig(), mutate)
}

// newMatrixFixtureWithConfig is newMatrixFixture over a caller-supplied
// seed, for rows that change what the world contains before anything is
// built (a seeded work item, a missing repository).
func newMatrixFixtureWithConfig(t *testing.T, cfg forgetest.Config, mutate func(f *forgetest.Fake, p *Policy)) (*forgetest.Fake, *matrixPusher, *PolicyEngine) {
	t.Helper()
	fake := forgetest.New(cfg)
	p := defaultMatrixPolicy()
	if mutate != nil {
		mutate(fake, &p)
	}
	baseRepo := forge.Repository{ID: "repo-base", Canonical: matrixBaseCanonical, DefaultRef: matrixBaseRef}
	workRepo := forge.Repository{ID: "repo-fork", Canonical: matrixWorkCanonical, DefaultRef: matrixBaseRef}
	adapter, err := NewForgeAdapter(p, fake, fake, baseRepo, workRepo)
	if err != nil {
		t.Fatalf("new forge adapter: %v", err)
	}
	pusher := newMatrixPusher(fake)
	engine, err := NewPolicyEngine(p, adapter, pusher)
	if err != nil {
		t.Fatalf("new policy engine: %v", err)
	}
	return fake, pusher, engine
}

func matrixPublishRequest(expected string) PublicationRequest {
	return PublicationRequest{RunUID: "run-1", ExpectedWorkOID: expected, ProposedOID: matrixProposedOID}
}

func TestExternalMatrixApprovedPublicationWritesOnlyThePullRequest(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, nil)
	ctx := context.Background()
	res, err := engine.Publish(ctx, matrixPublishRequest(matrixAnchorOID))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.OID != matrixProposedOID || res.AlreadyPublished {
		t.Fatalf("publish result = %+v, want the fresh proposed OID", res)
	}
	if pusher.pushCount != 1 {
		t.Fatalf("push count = %d, want exactly one ordinary push", pusher.pushCount)
	}
	body := "Ships the packaging fix.\n\nCloses up/base#123"
	pr, report, err := engine.CreatePullRequest(ctx, matrixProposedOID, "issue 123", body, false)
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if report != nil {
		t.Fatalf("unpinned policy returned a criteria report: %+v", report)
	}
	if pr.Number <= 0 || pr.HeadOID != matrixProposedOID {
		t.Fatalf("created PR = %#v, want the pinned head at the proposed OID", pr)
	}
	if got := fake.WriteCount(); got != 1 {
		t.Fatalf("write count = %d, want exactly one pull-request create", got)
	}
	creates := fake.OpsOf(forgetest.OpCreatePullRequest)
	if len(creates) != 1 {
		t.Fatalf("create ops = %d, want exactly one: %+v", len(creates), creates)
	}
	in := creates[0].Input
	if creates[0].Repo != matrixBaseCanonical || in.Base != matrixBaseRef || in.HeadRepo != matrixWorkCanonical || in.HeadRef != matrixWorkRef {
		t.Fatalf("create op identity = repo %q input %+v, want the pinned base and full head", creates[0].Repo, in)
	}
	if !strings.Contains(in.Body, "Closes up/base#123") {
		t.Fatalf("create op body = %q, want the trusted closing reference", in.Body)
	}
	if got := fake.OpsOf(forgetest.OpUpdatePullRequest); len(got) != 0 {
		t.Fatalf("unexpected update ops: %+v", got)
	}
	if got := fake.OpsOf(forgetest.OpCommentPullRequest); len(got) != 0 {
		t.Fatalf("unexpected comment ops: %+v", got)
	}
	for _, op := range fake.Ops() {
		switch op.Kind {
		case forgetest.OpResolveRepository, forgetest.OpReadRef, forgetest.OpReadEffectiveProtection,
			forgetest.OpCanWriteRepository, forgetest.OpReadPullRequest, forgetest.OpListPullRequestsByHead,
			forgetest.OpCreatePullRequest:
		default:
			t.Fatalf("forbidden recorded op kind %q", op.Kind)
		}
	}
}

// The pre-forge body refusal is covered by TestCreatePREnforcesSourceIssueLinkageBeforeForgeWrite
// in policy_test.go; this row adds the recorded-ops dimension through the real adapter and fake.
func TestExternalMatrixDeficientBodyReachesNoForge(t *testing.T) {
	fake, _, engine := newMatrixFixture(t, nil)
	_, _, err := engine.CreatePullRequest(context.Background(), matrixAnchorOID, "issue 123", "This change ships the icon assets and updates the manifest.", false)
	if err == nil {
		t.Fatal("deficient body accepted")
	}
	if !strings.Contains(err.Error(), "publication denied") {
		t.Fatalf("denial = %v, want the publication denial", err)
	}
	if got := fake.Ops(); len(got) != 0 {
		t.Fatalf("the body contract reached the forge: %+v", got)
	}
}

func TestExternalMatrixIssueContextDoesNotChangePublication(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seedIssue bool
	}{{"with seeded work item", true}, {"without work item", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultMatrixConfig()
			if tc.seedIssue {
				cfg.WorkItems = map[forge.WorkItemRef]forge.WorkItem{
					{Repo: matrixBaseCanonical, ID: "123"}: {ID: "123", Repo: matrixBaseCanonical, Title: "issue 123", Body: "seeded issue body", State: "open"},
				}
			}
			fake, _, engine := newMatrixFixtureWithConfig(t, cfg, nil)
			ctx := context.Background()
			if _, err := engine.Publish(ctx, matrixPublishRequest(matrixAnchorOID)); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if _, _, err := engine.CreatePullRequest(ctx, matrixProposedOID, "issue 123", "Ships the packaging fix.\n\nCloses up/base#123", false); err != nil {
				t.Fatalf("CreatePullRequest: %v", err)
			}
			if got := fake.WriteCount(); got != 1 {
				t.Fatalf("write count = %d, want exactly one pull-request create", got)
			}
			if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 1 {
				t.Fatalf("create ops = %d, want exactly one", len(got))
			}
			// The contract defines no work-item write verb, so one is
			// unrepresentable; the observable half is that the engine
			// never even reads the seeded issue.
			if got := fake.OpsOf(forgetest.OpReadWorkItem); len(got) != 0 {
				t.Fatalf("the engine read a work item: %+v", got)
			}
		})
	}
}

// Hostile remote URL redirection is pinned at the git transport layer by
// TestGitPusherUsesOnlyPinnedEndpointAndExactRef and
// TestGitPusherRejectsHostilePinsAndNonFastForward in git_transport_test.go;
// this row pins the policy-engine side of the same boundary: a protected
// destination is denied before the pusher is ever asked.
func TestExternalMatrixProtectedDestinationDeniesBeforePush(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, func(f *forgetest.Fake, p *Policy) {
		f.SetProtected(matrixWorkCanonical, true)
	})
	_, err := engine.Publish(context.Background(), matrixPublishRequest(matrixAnchorOID))
	if err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("denial = %v, want the protected-destination denial", err)
	}
	if pusher.pushCount != 0 {
		t.Fatalf("denied publication pushed: %d calls", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("denied publication wrote: %d", got)
	}
}

// The forge adapter is the component that binds every provider call to the
// pinned policy identity: a read, head listing, or create naming a repository
// or destination outside the pinned policy must be refused with no provider
// call recorded, so a substituted destination cannot even probe the forge.
func TestExternalMatrixDestinationOutsidePinnedPolicyIsRefused(t *testing.T) {
	fake := forgetest.New(defaultMatrixConfig())
	p := defaultMatrixPolicy()
	baseRepo := forge.Repository{ID: "repo-base", Canonical: matrixBaseCanonical, DefaultRef: matrixBaseRef}
	workRepo := forge.Repository{ID: "repo-fork", Canonical: matrixWorkCanonical, DefaultRef: matrixBaseRef}
	adapter, err := NewForgeAdapter(p, fake, fake, baseRepo, workRepo)
	if err != nil {
		t.Fatalf("new forge adapter: %v", err)
	}
	ctx := context.Background()
	if _, err := adapter.Repository(ctx, "evil/base", matrixBaseRef); err == nil || !strings.Contains(err.Error(), "outside pinned policy") {
		t.Fatalf("hostile repository read = %v, want the outside-policy refusal", err)
	}
	if _, err := adapter.FindPullRequest(ctx, "evil/head", matrixWorkRef); err == nil || !strings.Contains(err.Error(), "outside pinned policy") {
		t.Fatalf("hostile head listing = %v, want the outside-policy refusal", err)
	}
	if _, err := adapter.CreatePullRequest(ctx, CreatePullRequest{BaseRepo: "evil/base", BaseRef: matrixBaseRef, HeadRepo: matrixWorkCanonical, HeadRef: matrixWorkRef, Title: "t", Body: "Closes up/base#123"}); err == nil || !strings.Contains(err.Error(), "destination differs from pinned policy") {
		t.Fatalf("hostile create = %v, want the destination refusal", err)
	}
	if got := fake.Ops(); len(got) != 0 {
		t.Fatalf("the outside-policy refusals reached the forge: %+v", got)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("the outside-policy refusals wrote: %d", got)
	}
}

// The run-UID gate is covered by TestPublishRejectsRunUIDAndWrongRefOrRepository in
// policy_test.go; this row adds the recorded-ops dimension: the mismatch is
// decided before any forge call is made.
func TestExternalMatrixStaleRunUIDDeniesBeforeAnyForgeContact(t *testing.T) {
	fake, _, engine := newMatrixFixture(t, nil)
	_, err := engine.Publish(context.Background(), PublicationRequest{RunUID: "other", ExpectedWorkOID: matrixAnchorOID, ProposedOID: matrixProposedOID})
	if err == nil || !strings.Contains(err.Error(), "run UID") {
		t.Fatalf("denial = %v, want the run-UID mismatch", err)
	}
	if got := fake.Ops(); len(got) != 0 {
		t.Fatalf("the run-UID denial reached the forge: %+v", got)
	}
}

// The raced-duplicate denial is covered by TestBrokerIntegrationDeniesConcurrentDuplicatePR in
// integration_test.go; this row pins the single-PR guarantee for same-identity
// retries issued concurrently: PolicyEngine's mutex serializes the retries,
// the first creates, and every later call discovers the single existing
// request and adopts it — the recorded creates prove exactly one write and
// every caller converges on the same pull request.
func TestExternalMatrixConcurrentRetriesCreateExactlyOnePR(t *testing.T) {
	fake, _, engine := newMatrixFixture(t, nil)
	ctx := context.Background()
	body := "Ships the packaging fix.\n\nCloses up/base#123"
	const n = 4
	errs := make([]error, n)
	numbers := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pr, _, err := engine.CreatePullRequest(ctx, matrixAnchorOID, "issue 123", body, false)
			numbers[i] = pr.Number
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("retry %d = %v, want every caller to converge on the single pull request (create or adoption)", i, err)
		}
	}
	for _, number := range numbers {
		if number != numbers[0] {
			t.Fatalf("callers converged on different pull requests: %v", numbers)
		}
	}
	if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 1 {
		t.Fatalf("create ops = %d, want exactly one", len(got))
	}
	if got := fake.PullRequests(); len(got) != 1 {
		t.Fatalf("pull requests = %d, want exactly one: %+v", len(got), got)
	}
}

// Restart-after-PR-creation ownership is covered by
// TestRestartDoesNotRecoverFromCallerOIDOrAncestry in policy_test.go; this
// row is the lost-response window: the forge write landed, the reply was
// lost, and the retry must adopt the discovered PR by base/head identity
// rather than create a duplicate.
func TestExternalMatrixLostCreateResponseCreatesNoDuplicatePR(t *testing.T) {
	fake, _, engine := newMatrixFixture(t, nil)
	ctx := context.Background()
	body := "Ships the packaging fix.\n\nCloses up/base#123"
	fake.FailNextAfterEffect(forgetest.OpCreatePullRequest, errors.New("connection reset"))
	_, _, err := engine.CreatePullRequest(ctx, matrixAnchorOID, "issue 123", body, false)
	if err == nil {
		t.Fatal("lost create response reported success")
	}
	if got := fake.PullRequests(); len(got) != 1 {
		t.Fatalf("pull requests after the lost response = %d, want exactly one", len(got))
	}
	landed := fake.PullRequests()[0]
	adopted, _, err := engine.CreatePullRequest(ctx, matrixAnchorOID, "issue 123", body, false)
	if err != nil {
		t.Fatalf("retry = %v, want adoption of the discovered pull request", err)
	}
	if adopted.Number != landed.Number || adopted.HeadOID != matrixAnchorOID {
		t.Fatalf("adopted = %+v, want the landed request %d at the pinned head", adopted, landed.Number)
	}
	if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 1 {
		t.Fatalf("create ops = %d, want exactly one (the attempt with the lost reply)", len(got))
	}
	if got := fake.PullRequests(); len(got) != 1 {
		t.Fatalf("pull requests after the retry = %d, want no duplicate", len(got))
	}
}

// Uncertain-push confirmation is covered by TestPublishConfirmsUncertainPushWhenExactOIDIsLive
// and TestPublishConfirmsUncertainPushForNewResolveRef in policy_test.go;
// this row adds the recorded-ops dimension: the confirmed uncertain push
// leads to exactly one create, never a duplicate.
func TestExternalMatrixUncertainPushConfirmsIdempotentlyWithoutDuplicatePR(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, nil)
	pusher.pushErr = errors.New("transport read-back failed")
	ctx := context.Background()
	res, err := engine.Publish(ctx, matrixPublishRequest(matrixAnchorOID))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if res.OID != matrixProposedOID || !res.AlreadyPublished {
		t.Fatalf("publish result = %+v, want the proposed OID with AlreadyPublished", res)
	}
	_, _, err = engine.CreatePullRequest(ctx, matrixProposedOID, "issue 123", "Ships the packaging fix.\n\nCloses up/base#123", false)
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 1 {
		t.Fatalf("create ops = %d, want exactly one", len(got))
	}
	if got := fake.PullRequests(); len(got) != 1 {
		t.Fatalf("pull requests = %d, want exactly one", len(got))
	}
}

func TestExternalMatrixTransientForgeLookupFailsClosedThenRecovers(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, nil)
	ctx := context.Background()
	fake.FailNext(forgetest.OpReadRef, errors.New("503"))
	_, err := engine.Publish(ctx, matrixPublishRequest(matrixAnchorOID))
	if err == nil || !strings.Contains(err.Error(), "observe") {
		t.Fatalf("denial = %v, want the failed-close observation error", err)
	}
	if pusher.pushCount != 0 {
		t.Fatalf("failed publication pushed: %d", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("failed publication wrote: %d", got)
	}
	res, err := engine.Publish(ctx, matrixPublishRequest(matrixAnchorOID))
	if err != nil {
		t.Fatalf("recovered Publish: %v", err)
	}
	if res.OID != matrixProposedOID || res.AlreadyPublished {
		t.Fatalf("recovered result = %+v, want the fresh proposed OID", res)
	}
	if pusher.pushCount != 1 {
		t.Fatalf("push count = %d, want exactly one across both attempts", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("the publications wrote to the forge: %d", got)
	}
}

func TestExternalMatrixMissingForkDeniesPublication(t *testing.T) {
	cfg := defaultMatrixConfig()
	cfg.Repositories = []forge.Repository{{ID: "repo-base", Canonical: matrixBaseCanonical, DefaultRef: matrixBaseRef}}
	fake, pusher, engine := newMatrixFixtureWithConfig(t, cfg, nil)
	_, err := engine.Publish(context.Background(), matrixPublishRequest(matrixAnchorOID))
	if err == nil || !strings.Contains(err.Error(), "observe pinned work ref") {
		t.Fatalf("denial = %v, want the failed fork observation", err)
	}
	if pusher.pushCount != 0 {
		t.Fatalf("denied publication pushed: %d", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("denied publication wrote: %d", got)
	}
}

func TestExternalMatrixNoWritePermissionDeniesPublication(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, func(f *forgetest.Fake, p *Policy) {
		f.SetWritable(matrixWorkCanonical, false)
	})
	_, err := engine.Publish(context.Background(), matrixPublishRequest(matrixAnchorOID))
	if err == nil || !strings.Contains(err.Error(), "write") {
		t.Fatalf("denial = %v, want the write-permission denial", err)
	}
	if pusher.pushCount != 0 {
		t.Fatalf("denied publication pushed: %d", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("denied publication wrote: %d", got)
	}
}

// The ancestry half is covered by TestPublishEnforcesExpectedTipAndFastForward in
// policy_test.go; the deleted-base-ref half is new at the fake level.
func TestExternalMatrixChangedUpstreamBaseDeniesPublication(t *testing.T) {
	t.Run("base tip not an ancestor of the proposal", func(t *testing.T) {
		fake, pusher, engine := newMatrixFixture(t, nil)
		pusher.baseAncestor = false
		_, err := engine.Publish(context.Background(), matrixPublishRequest(matrixAnchorOID))
		if err == nil || !strings.Contains(err.Error(), "live base tip") {
			t.Fatalf("denial = %v, want the base-ancestry denial", err)
		}
		if pusher.pushCount != 0 {
			t.Fatalf("denied publication pushed: %d", pusher.pushCount)
		}
		if got := fake.WriteCount(); got != 0 {
			t.Fatalf("denied publication wrote: %d", got)
		}
	})
	t.Run("base ref deleted", func(t *testing.T) {
		fake, pusher, engine := newMatrixFixture(t, nil)
		fake.DeleteRef(matrixBaseCanonical, matrixBaseRef)
		_, err := engine.Publish(context.Background(), matrixPublishRequest(matrixAnchorOID))
		if err == nil || !strings.Contains(err.Error(), "pinned base") {
			t.Fatalf("denial = %v, want the pinned-base denial", err)
		}
		if pusher.pushCount != 0 {
			t.Fatalf("denied publication pushed: %d", pusher.pushCount)
		}
		if got := fake.WriteCount(); got != 0 {
			t.Fatalf("denied publication wrote: %d", got)
		}
	})
}

// The same-head discovery is covered by TestCreatePRGuardsPinnedHeadAndReturnedIdentity in
// policy_test.go; this row pins the adoption boundary of discovery: exactly
// one open PR at the exact pinned head whose body satisfies this policy's
// contract is adopted with zero create ops, while a discovered PR whose body
// does not satisfy the contract is a foreign publication on the run's head
// and fails closed — it is never blessed and never duplicated over.
func TestExternalMatrixExistingPRForPinnedHeadIsAdoptedNotDuplicated(t *testing.T) {
	seedPR := func(body string) func(*forgetest.Fake, *Policy) {
		return func(f *forgetest.Fake, _ *Policy) {
			f.AddPullRequest(forge.PullRequest{
				Number:   101,
				Repo:     matrixBaseCanonical,
				BaseRepo: matrixBaseCanonical,
				BaseRef:  matrixBaseRef,
				BaseSHA:  matrixBaseOID,
				HeadRepo: matrixWorkCanonical,
				HeadRef:  matrixWorkRef,
				HeadSHA:  matrixAnchorOID,
				State:    "open",
				Title:    "existing",
				Body:     body,
			})
		}
	}
	t.Run("contract-satisfying PR is adopted without a create", func(t *testing.T) {
		fake, _, engine := newMatrixFixture(t, seedPR("Ships the packaging fix.\n\nCloses up/base#123"))
		adopted, _, err := engine.CreatePullRequest(context.Background(), matrixAnchorOID, "issue 123", "Ships the packaging fix.\n\nCloses up/base#123", false)
		if err != nil {
			t.Fatalf("adoption = %v, want the discovered request returned", err)
		}
		if adopted.Number != 101 || adopted.HeadOID != matrixAnchorOID {
			t.Fatalf("adopted = %+v, want the seeded request 101 at the pinned head", adopted)
		}
		if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 0 {
			t.Fatalf("create ops = %d, want none: the seeded PR is adopted, not duplicated", len(got))
		}
	})
	t.Run("foreign-body PR on the pinned head fails closed", func(t *testing.T) {
		fake, _, engine := newMatrixFixture(t, seedPR("someone else's publication, no closing reference"))
		_, _, err := engine.CreatePullRequest(context.Background(), matrixAnchorOID, "issue 123", "Ships the packaging fix.\n\nCloses up/base#123", false)
		if err == nil || !strings.Contains(err.Error(), "discovered pull request body violates the pinned linkage contract") {
			t.Fatalf("denial = %v, want the discovered-body linkage refusal", err)
		}
		if got := fake.OpsOf(forgetest.OpCreatePullRequest); len(got) != 0 {
			t.Fatalf("create ops = %d, want none: a foreign publication is neither blessed nor overwritten", len(got))
		}
	})
}

// Foreign tips are covered by TestPublishRejectsForeignInitiallyPresentAndRacedAbsentTips in
// policy_test.go; this row is the stale-ownership variant: even a caller
// that asserts the foreign tip as its expected value cannot adopt it.
func TestExternalMatrixStaleBranchOwnershipDeniesPublication(t *testing.T) {
	fake, pusher, engine := newMatrixFixture(t, func(f *forgetest.Fake, p *Policy) {
		f.SetRef(matrixWorkCanonical, matrixWorkRef, matrixForeignOID)
	})
	_, err := engine.Publish(context.Background(), PublicationRequest{RunUID: "run-1", ExpectedWorkOID: matrixForeignOID, ProposedOID: matrixProposedOID})
	if err == nil || !strings.Contains(err.Error(), "not an admitted or previously confirmed") {
		t.Fatalf("denial = %v, want the branch-ownership denial", err)
	}
	if pusher.pushCount != 0 {
		t.Fatalf("denied publication pushed: %d", pusher.pushCount)
	}
	if got := fake.WriteCount(); got != 0 {
		t.Fatalf("denied publication wrote: %d", got)
	}
}

// Pinned-PR identity guards are covered by TestUpdateFixPRGuardsPinnedIdentity in
// policy_test.go; this row covers the upstream work state. A real forge
// reports a merged request as state "closed" with a separate merged marker
// (internal/github/forge.go), and the adapter's PullRequestState carries only
// State, so "closed" is how both closed and merged upstream work reaches the
// engine and denies the update; only an open request with the exact live head
// carries exactly one update write.
func TestExternalMatrixClosedOrMergedUpstreamWorkDeniesFixPR(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prState    string
		wantDenied bool
	}{
		{"closed upstream work", "closed", true},
		{"merged upstream work (reported as closed)", "closed", true},
		{"open upstream work", "open", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, _, engine := newMatrixFixture(t, func(f *forgetest.Fake, p *Policy) {
				p.Mode = ModeFixPR
				p.PRNumber = 7
				p.HeadAnchorOID = matrixAnchorOID
				p.WorkAnchorOID = matrixAnchorOID
				p.SourceIssue = SourceIssue{}
				f.AddPullRequest(forge.PullRequest{
					Number:   7,
					Repo:     matrixBaseCanonical,
					BaseRepo: matrixBaseCanonical,
					BaseRef:  matrixBaseRef,
					BaseSHA:  matrixBaseOID,
					HeadRepo: matrixWorkCanonical,
					HeadRef:  matrixWorkRef,
					HeadSHA:  matrixAnchorOID,
					State:    tc.prState,
					Title:    "before",
					Body:     "old",
				})
			})
			err := engine.UpdateFixPR(context.Background(), UpdatePullRequest{Title: "after"})
			if tc.wantDenied {
				if err == nil || !strings.Contains(err.Error(), "not open") {
					t.Fatalf("denial = %v, want the not-open denial", err)
				}
				if got := fake.WriteCount(); got != 0 {
					t.Fatalf("denied update wrote: %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateFixPR: %v", err)
			}
			updates := fake.OpsOf(forgetest.OpUpdatePullRequest)
			if len(updates) != 1 {
				t.Fatalf("update ops = %d, want exactly one: %+v", len(updates), updates)
			}
			if updates[0].Update.Title != "after" {
				t.Fatalf("update op = %+v, want the new title", updates[0])
			}
			if updates[0].PR.Number != 7 || updates[0].PR.Repo != matrixBaseCanonical {
				t.Fatalf("update op targeted %+v, want the pinned request 7 on %s", updates[0].PR, matrixBaseCanonical)
			}
		})
	}
}
