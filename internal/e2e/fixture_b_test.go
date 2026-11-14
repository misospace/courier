package e2e

// Fixture B replays an injected provider throttle: the scripted model answers
// the coordinator's turn with an HTTP 429 and a Retry-After hint, the shape a
// rate-limited provider returns.
//
// The composition is Fixture A's, unchanged: the production reconciler and
// the production secure control admit the run in envtest, the production
// harness gateway and coordinator run the turn, the production broker status
// path persists the trusted terminal result, and the production policy engine
// is wired in even though the failing turn never reaches a publication.
//
// The injected failure is one: the provider throttles. Everything the fixture
// asserts is a consequence of that one injection:
//
//   - no fake heartbeat: the failed turn earned nothing, so no heartbeat was
//     written for it,
//   - no model hammering: the turn's request count is the production bound,
//   - a truthful classified termination: the run ends blocked-external, with
//     the diagnostic that names the cause,
//   - the attempt is finished and the source is told so,
//   - the lane's capacity is released,
//   - nothing was published.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge/forgetest"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/topology"
)

// fixtureB is Fixture A's world with one difference: the model script
// throttles the coordinator's turn.
type fixtureB struct {
	*fixtureA
}

// throttledResponses is the injected failure: the capability probe succeeds,
// and every request from the coordinator's turn on is answered with an HTTP
// 429 and a one-second Retry-After hint.
//
// The fault is scripted four times because that is the production bound for
// one failing turn: the harness sends the request once and retries it at most
// maxStreamRetries times more. A fifth request finds the script exhausted, so
// the gateway answers with its own refusal and the fixture's count assertion
// fails with the number it saw.
func throttledResponses() []modelResponse {
	fault := &modelFault{
		status:  http.StatusTooManyRequests,
		headers: map[string]string{"Retry-After": "1"},
		body:    `{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"the provider is rate limiting this deployment"}}`,
	}
	responses := []modelResponse{{sse: sse(contentChunk("probe"), finishChunk("stop"))}}
	for attempt := 0; attempt < 4; attempt++ {
		responses = append(responses, modelResponse{fault: fault})
	}
	return responses
}

// TestFixtureBProviderThrottle is the fixture itself.
func TestFixtureBProviderThrottle(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureDeadline)
	defer cancel()

	f := newFixtureB(t)
	f.admit(t, ctx)
	f.runControl(t, ctx)
	f.settle(t, ctx)
	f.assertions(t, ctx)
}

// newFixtureB builds the throttled world: the same production composition as
// Fixture A, with a model script that fails the coordinator's turn.
func newFixtureB(t *testing.T) *fixtureB {
	t.Helper()
	world := newFixtureWorld(t, throttledResponses(), fixtureProposal{name: "widget.txt", content: "the widget\n"})
	world.src = newRecordingSource("receipt-" + world.git.BaseOID[:8])
	world.sourceAdapter = world.src
	world.runName = "e2e-fixture-b"
	world.laneName = "e2e-lane-b"
	world.namespace = "courier-runs-b"
	world.prepareCluster(t)
	return &fixtureB{fixtureA: world}
}

// runControl plays the control pod: the capability gate succeeds, and the
// coordinator's turn is throttled by the provider.
func (f *fixtureB) runControl(t *testing.T, ctx context.Context) {
	t.Helper()
	gateway := f.newGateway()
	f.buildPublication(t, ctx)

	reviews := newTokenReviews("system:serviceaccount:"+f.namespace+":"+topology.ControlSAName(f.run.Name),
		f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
	reviews.register(fixtureBrokerToken, "system:serviceaccount:"+f.namespace+":"+topology.ControlSAName(f.run.Name),
		f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
	authenticator := f.buildAuthenticator(t, ctx, reviews)
	f.statusTransport, f.reporter = f.buildStatusPath(t, ctx, authenticator, f.controlPod, fixtureBrokerToken)

	probe := f.newProbe(t, ctx, authenticator, fixtureBrokerToken)
	f.capabilityGate(t, ctx, gateway, probe)

	delegator := f.newDelegator(t, f.controlPod, f.workerKey)
	f.publisher = f.publisherFor(f.proposal, "",
		"Add the widget",
		fmt.Sprintf("Closes %s/%s#%d.", fixtureOwner, fixtureRepo, fixtureIssue))
	coordinator := f.newCoordinator(t, gateway, delegator, f.publisher, f.reporter)

	f.result = coordinator.Run(ctx, f.invocation(t))
	// The harness's own classification of the throttled provider decides the
	// ending: a rate-limited provider is a recoverable wait on an external
	// resource, never a verdict on the work.
	if f.result.Kind != courierv1alpha1.NativeResultBlockedExternal {
		t.Fatalf("harness result = %+v, want the blocked-external ending a throttled provider earns", f.result)
	}
	if f.result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("harness outcome = %q, want %q", f.result.Outcome, executor.OutcomeBlockedExternal)
	}
	// The trusted terminal result is persisted through the authenticated
	// status path before the control process exits, exactly as the control
	// binary does. The run published nothing, so there is no checkpoint and
	// no confirmed commit to record.
	if err := f.reporter.TerminalResult(ctx, courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultBlockedExternal,
		Reason:            courierv1alpha1.NativeResultReason(courierv1alpha1.NativeResultBlockedExternal),
		CoordinatorPodUID: string(f.controlPod.UID),
		At:                metav1.NewTime(time.Now()),
	}, nil); err != nil {
		t.Fatalf("terminal result: %v", err)
	}
}

// settle ends the control pod the way the kubelet would and drives the
// operator to the run's classified ending.
func (f *fixtureB) settle(t *testing.T, ctx context.Context) {
	t.Helper()
	f.terminateControlPod(t, ctx, f.controlPod, blockedExternalHandoff(f.result.Reason), 2)
	f.waitForPhase(t, ctx, courierv1alpha1.PhaseBlockedExternal)
}

// assertions are the fixture's claims. Every one of them is negative where
// it can be: the fixture proves what did not happen as much as what did.
func (f *fixtureB) assertions(t *testing.T, ctx context.Context) {
	t.Helper()
	run := f.fetch(t)

	// No fake heartbeat: the throttled turn earned no stream chunk and no
	// verified tool boundary, so no heartbeat was written — neither through
	// the status path nor onto the run. A heartbeat may only follow real
	// earned activity.
	if run.Status.Heartbeat != nil {
		t.Fatalf("the run carries the heartbeat %+v for a turn that earned no activity", run.Status.Heartbeat)
	}
	if beats := f.statusTransport.Heartbeats(); len(beats) != 0 {
		t.Fatalf("the harness wrote %d heartbeats for the failed turn, want none", len(beats))
	}
	for _, event := range f.log.Events() {
		if strings.HasPrefix(event, "status-heartbeat:") {
			t.Fatalf("the harness wrote a heartbeat for the failed turn: %s", event)
		}
	}

	// No repeated model hammering: the failed turn sent no more requests
	// than the production bound allows. The bound is the session's own: one
	// request plus maxStreamRetries in-process retries of a retryable
	// gateway failure (internal/harness/session.go). The capability probe's
	// request is the one before it, so the failing turn may send four.
	const failingTurnBound = 5
	if got := f.model.Requests(); got > failingTurnBound {
		t.Fatalf("model requests = %d, want at most %d: the capability probe plus the failed turn's 1 + maxStreamRetries attempts", got, failingTurnBound)
	}
	if got := f.model.Requests(); got < 2 {
		t.Fatalf("model requests = %d, want the throttled turn to have been retried at least once", got)
	}
	if f.model.Exceeded() != 0 {
		t.Fatalf("the scripted gateway refused %d unscripted requests: the harness sent more requests than its own bound allows", f.model.Exceeded())
	}
	requests := f.model.RequestLog()
	if len(requests) != f.model.Requests() {
		t.Fatalf("model request log = %d entries, want the %d the gateway recorded", len(requests), f.model.Requests())
	}
	for i, request := range requests {
		if request.Model != requests[0].Model {
			t.Fatalf("model request %d named %q, want the same bound model as the probe (%q)", i+1, request.Model, requests[0].Model)
		}
	}

	// Truthful classified termination: the run carries the trusted native
	// result the harness itself produced, and the operator's diagnostic
	// names the observed cause. Nothing claims success.
	if run.Status.NativeResult == nil {
		t.Fatal("the run carries no trusted native result: the failing turn's ending was never recorded")
	}
	if run.Status.NativeResult.Kind != courierv1alpha1.NativeResultBlockedExternal {
		t.Fatalf("native result kind = %q, want %q", run.Status.NativeResult.Kind, courierv1alpha1.NativeResultBlockedExternal)
	}
	if run.Status.NativeResult.CoordinatorPodUID != string(f.controlPod.UID) {
		t.Fatalf("native result UID = %q, want the control pod %q", run.Status.NativeResult.CoordinatorPodUID, f.controlPod.UID)
	}
	if run.Status.TerminalDiagnostic == nil {
		t.Fatal("the run carries no terminal diagnostic: the failing ending was never classified")
	}
	if run.Status.TerminalDiagnostic.Category != courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal {
		t.Fatalf("terminal diagnostic category = %q, want %q", run.Status.TerminalDiagnostic.Category, courierv1alpha1.TerminalDiagnosticCategoryBlockedExternal)
	}
	if !strings.Contains(run.Status.TerminalDiagnostic.Reason, "blocked on an external prerequisite") {
		t.Fatalf("terminal diagnostic reason = %q, want it to name the observed cause", run.Status.TerminalDiagnostic.Reason)
	}
	if run.Status.Phase != courierv1alpha1.PhaseBlockedExternal {
		t.Fatalf("phase = %q, want %q", run.Status.Phase, courierv1alpha1.PhaseBlockedExternal)
	}
	if run.Status.Phase == courierv1alpha1.PhaseDone || run.Status.Phase == courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q: the throttled run must not claim a successful ending", run.Status.Phase)
	}
	if run.Status.PR != "" {
		t.Fatalf("run PR = %q, want none: the throttled run published nothing", run.Status.PR)
	}
	if run.Status.LastCommit != "" {
		t.Fatalf("run lastCommit = %q, want none: the throttled run published nothing", run.Status.LastCommit)
	}
	if run.Status.FinishedAt == nil {
		t.Fatal("the run recorded no end: the operator's terminal transition never ran")
	}

	// Source and attempt fencing: the attempt is finished, the source was
	// told the work is blocked on an external prerequisite and may re-offer
	// it, and the claim it carries is the one the source issued.
	if run.Status.SourceReceipt != f.src.receipt {
		t.Fatalf("run source receipt = %q, want the receipt the claim issued", run.Status.SourceReceipt)
	}
	reports := f.src.Reports()
	if len(reports) != 1 {
		t.Fatalf("source reports = %d, want exactly one lifecycle report", len(reports))
	}
	if reports[0].Result != source.ResultBlockedExternal {
		t.Fatalf("lifecycle report result = %q, want %q: the throttled run is recoverable, not a failure verdict", reports[0].Result, source.ResultBlockedExternal)
	}
	if reports[0].State != source.StateInProgress {
		t.Fatalf("lifecycle report state = %q, want %q: a recoverable block keeps the work item active for re-offer", reports[0].State, source.StateInProgress)
	}
	if reports[0].PR != "" {
		t.Fatalf("lifecycle report PR = %q, want none: nothing was published", reports[0].PR)
	}
	if !strings.Contains(reports[0].Error, "rate-limit") {
		t.Fatalf("lifecycle report error = %q, want it to name the throttled provider", reports[0].Error)
	}
	// The work item was claimed exactly once, and no second run claimed it.
	assertSingleRunForWorkItem(t, f.namespace, fmt.Sprint(fixtureIssue))

	// No stuck lane reservation: the failed run's terminal phase reserves no
	// lane capacity, so the lane's concurrency is available again.
	if controller.PhaseConsumesCapacity(run.Status.Phase) {
		t.Fatalf("phase = %q still reserves the lane: the failed run left a stuck lane reservation", run.Status.Phase)
	}
	assertNoLaneReservation(t, f.namespace, f.laneName)
	// And the operator proves it: another run on the same lane is admitted,
	// because the failed run released the lane's only slot.
	f.admitSecondRunOnLane(t, ctx)

	// The failure left no half-done publication: no pull request was
	// created, and the work ref was never published.
	assertNoPullRequest(t, ctx, f.provider, f.policy.BaseRepo, f.policy.WorkRepo, f.policy.WorkRef)
	if tip := f.provider.Tip(f.policy.WorkRepo, f.policy.WorkRef); tip != "" {
		t.Fatalf("the work ref tip = %q, want none: the throttled run published nothing", tip)
	}
	if creates := f.provider.OpsOf(forgetest.LocalGitOpCreatePullRequest); len(creates) != 0 {
		t.Fatalf("the forge recorded %d pull-request creates, want none", len(creates))
	}

	// The shared negative claims every fixture makes: the deployment's
	// credential reached no recorded surface, and the source adapter saw
	// only the calls its contract defines — one claim per work item, the
	// contract's own states and outcomes, and nothing else.
	assertNoCredentialLeakage(t, fixtureCredential, f.operatorNamespace,
		surface(t, "recorded status patches", f.statusTransport.Patches()),
		surface(t, "the CoderRun's status", run.Status),
		surface(t, "the source's recorded calls", f.src.Calls()),
		surface(t, "the source's recorded lifecycle", f.src.Reports()),
	)
	assertNoForbiddenSourceMutations(t, f.src, fmt.Sprint(fixtureIssue), fmt.Sprint(fixtureSecondIssue))
}

// admitSecondRunOnLane admits another run on the fixture's lane: the failed
// run released the lane's only slot, so the operator admits a new run there.
// It is the observable proof that the failure left no stuck lane reservation.
func (f *fixtureB) admitSecondRunOnLane(t *testing.T, ctx context.Context) {
	t.Helper()
	second := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-fixture-b-second", Namespace: f.namespace},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "manual",
			WorkItemID: fmt.Sprint(fixtureSecondIssue),
			Repo:       fixtureOwner + "/" + fixtureRepo,
			Ref:        fixtureSecondIssue,
			Lane:       f.laneName,
		},
	}
	if err := envtestClient.Create(ctx, second); err != nil {
		t.Fatalf("create the second run: %v", err)
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: second.Namespace, Name: second.Name}}
	for {
		if _, err := f.reconciler.Reconcile(ctx, request); err != nil {
			t.Fatalf("reconcile the second run: %v", err)
		}
		read := &courierv1alpha1.CoderRun{}
		if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(second), read); err != nil {
			t.Fatal(err)
		}
		if read.Status.Phase == courierv1alpha1.PhaseRunning {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the second run never reached Running (it is %q): the failed run left the lane's capacity reserved", read.Status.Phase)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// blockedExternalHandoff renders the termination handoff the control binary
// writes for a recoverable blocked-external ending: exit 2 with the
// needs-human phase/result pair the operator's vocabulary requires, the
// declared outcome, and the trusted native result kind.
func blockedExternalHandoff(reason string) string {
	return "COURIER_TERMINATION " + fmt.Sprintf(
		`{"phase":"NeedsHuman","result":"needs-human","exit_code":2,"reason":%q,"outcome":"blocked_external","result_kind":"blocked_external"}`,
		reason)
}
