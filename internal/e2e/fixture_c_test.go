package e2e

// Fixture C replays a restart around the status/checkpoint/publication
// boundary.
//
// The first control incarnation publishes its commit and opens the pull
// request, and then the incarnation ends before it can record its checkpoint
// and its confirmed commit. A replacement control pod — a new incarnation
// with a new UID, the same owner, the same service account — is provisioned,
// and the control side runs again against the SAME run.
//
// The fence is production code, not the fixture's: the broker's status writer
// fences every heartbeat and every terminal result to the authenticated
// control incarnation and requires exactly one live control pod owned by the
// run UID, and the broker's policy engine owns the publication fence and the
// duplicate-pull-request guard. The fixture drives them and asserts what they
// decided.
//
// What the fixture proves:
//
//   - the source identity did not move: the same work item, the same
//     receipt, no successor run,
//   - the publication stayed singular: two publication attempts, one pull
//     request, one create,
//   - no stale publication survived: the run's confirmed commit is the OID
//     the surviving incarnation published, and the dead incarnation's status
//     write was refused,
//   - the operator's own state stayed inspectable and coherent after the
//     restart.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/topology"
)

// fixtureC is Fixture A's world with two commits to publish and a control
// pod that is replaced mid-run.
type fixtureC struct {
	*fixtureA
	// reviews is the fixture's TokenReview backend: the dead incarnation's
	// credential and the replacement's own.
	reviews *tokenReviews
	// firstPublisher is the first incarnation's publication seam.
	firstPublisher *fixturePublisher
	// secondPublisher is the replacement incarnation's publication seam.
	secondPublisher *fixturePublisher
	// staleReporter is the first incarnation's status reporter: the
	// credential it holds names a pod that no longer exists.
	staleReporter *harness.StatusReporter
	// staleTransport records what the dead incarnation asked the broker to
	// write.
	staleTransport *recordingStatusTransport
	// replacement is the second incarnation's control pod.
	replacement *corev1.Pod
	// deadPodUID is the ended incarnation's pod UID.
	deadPodUID string
	// staleWriteErr is the dead incarnation's refused status write.
	staleWriteErr error
	// runUIDBefore is the run's UID before the restart.
	runUIDBefore string
	// brokerUIDBefore is the broker pod's UID before the restart: the
	// broker is a separate pod in the run's topology, and a control
	// incarnation's death does not replace it.
	brokerUIDBefore string
}

// restartResponses is the model script of the two incarnations: the first
// incarnation's probe, its tool turn, its outcome declaration, then the
// replacement incarnation's probe and its own outcome declaration. Both
// outcome declarations carry the pull-request title and body (#844): the
// mode, not the world, decides the shape, and the second incarnation's
// create route adopts the discovered pull request instead of opening a
// second one.
func restartResponses() []modelResponse {
	changes := fixtureChangesDeclaration()
	return []modelResponse{
		{sse: sse(contentChunk("probe"), finishChunk("stop"))},
		{sse: sse(toolCallChunk(0, "call-shell", "shell", `{"command":"echo courier"}`), finishChunk("tool_calls"))},
		{sse: sse(contentChunk(changes), finishChunk("stop"))},
		{sse: sse(contentChunk("probe"), finishChunk("stop"))},
		{sse: sse(contentChunk(changes), finishChunk("stop"))},
	}
}

// TestFixtureCRestartAroundPublication is the fixture itself.
func TestFixtureCRestartAroundPublication(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureDeadline)
	defer cancel()

	f := newFixtureC(t)
	f.admit(t, ctx)
	f.runFirstIncarnation(t, ctx)
	f.restart(t, ctx)
	f.runSecondIncarnation(t, ctx)
	f.settle(t, ctx)
	f.assertions(t, ctx)
}

// newFixtureC builds the restart world: two commits the control side may
// publish, and a model script for the two incarnations.
func newFixtureC(t *testing.T) *fixtureC {
	t.Helper()
	world := newFixtureWorld(t, restartResponses(),
		fixtureProposal{name: "widget.txt", content: "the widget\n"},
		fixtureProposal{name: "gadget.txt", content: "the gadget\n"})
	world.src = newRecordingSource("receipt-" + world.git.BaseOID[:8])
	world.sourceAdapter = world.src
	world.runName = "e2e-fixture-c"
	world.laneName = "e2e-lane-c"
	world.namespace = "courier-runs-c"
	world.prepareCluster(t)
	return &fixtureC{fixtureA: world}
}

// runFirstIncarnation plays the first control pod: the capability gate, the
// model turn with its earned tool activity, and the publication that opens
// the pull request.
//
// The incarnation ends here, before its checkpoint and its confirmed commit
// are recorded: that is the boundary the restart happens around.
func (f *fixtureC) runFirstIncarnation(t *testing.T, ctx context.Context) {
	t.Helper()
	gateway := f.newGateway()
	f.buildPublication(t, ctx)

	reviews := f.tokenReviews(t)
	authenticator := f.buildAuthenticator(t, ctx, reviews)
	f.statusTransport, f.reporter = f.buildStatusPath(t, ctx, authenticator, f.controlPod, fixtureBrokerToken)
	f.staleTransport, f.staleReporter = f.statusTransport, f.reporter

	probe := f.newProbe(t, ctx, authenticator, fixtureBrokerToken)
	f.capabilityGate(t, ctx, gateway, probe)

	// The proposal reaches the broker over the broker's own import route:
	// the work ref is still absent, so the run's first commit creates it.
	f.importProposal(t, ctx, f.proposals[0], "", fixtureBrokerToken)

	delegator := f.newDelegator(t, f.controlPod, f.workerKey)
	f.firstPublisher = f.publisherFor(t, f.proposals[0], "", fixtureBrokerToken)
	coordinator := f.newCoordinator(t, gateway, delegator, f.firstPublisher, f.reporter)

	result := coordinator.Run(ctx, f.invocation(t))
	if result.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("harness result = %+v, want a published ending", result)
	}
	// The first incarnation recorded the pull-request identity through its
	// own trusted status path (#844): the settle observer is not wired yet,
	// so the run's recorded PR can only be the harness's write.
	if got := f.fetch(t).Status.PR; got != fmt.Sprint(f.firstPublisher.opened.Number) {
		t.Fatalf("run PR = %q after the first incarnation, want the opened pull request %d recorded through the trusted status path", got, f.firstPublisher.opened.Number)
	}
}

// restart ends the control pod and provisions its replacement: a new
// incarnation with a new UID, the same owner, and the same service account.
// The replacement's credential is bound to it, exactly as the projected token
// of a real pod is.
func (f *fixtureC) restart(t *testing.T, ctx context.Context) {
	t.Helper()
	f.runUIDBefore = string(f.fetch(t).UID)
	// The broker pod is its own pod in the run's topology: the operator's
	// launch ensures it exists and replaces it only when it is missing or
	// failed, so a control incarnation's death leaves the broker standing.
	// The successor's publication authority is therefore the broker's own
	// confirmation of the first incarnation's tip — the trust the broker
	// exists to hold. The fixture asserts the broker pod was not replaced.
	f.brokerUIDBefore = string(f.brokerPod.UID)
	old := f.controlPod
	f.deadPodUID = string(old.UID)
	if err := envtestClient.Delete(ctx, old, client.Preconditions{UID: &old.UID}); err != nil {
		t.Fatalf("end the control incarnation: %v", err)
	}
	// The pod is unscheduled in envtest, so the API server removes it at
	// once — the way the operator's own relaunch frees the deterministic
	// name for the replacement.
	deadline := time.Now().Add(5 * time.Second)
	for {
		read := &corev1.Pod{}
		err := envtestClient.Get(ctx, types.NamespacedName{Namespace: old.Namespace, Name: old.Name}, read)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ended control pod still exists: %v", read.UID)
		}
		time.Sleep(20 * time.Millisecond)
	}

	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            old.Name,
			Namespace:       old.Namespace,
			Labels:          old.Labels,
			Annotations:     old.Annotations,
			OwnerReferences: old.OwnerReferences,
		},
		Spec: old.Spec,
	}
	created := &corev1.Pod{}
	if err := envtestClient.Create(ctx, replacement); err != nil {
		t.Fatalf("provision the replacement control pod: %v", err)
	}
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: old.Namespace, Name: old.Name}, created); err != nil {
		t.Fatalf("read the replacement control pod: %v", err)
	}
	if created.UID == "" {
		t.Fatal("the replacement control pod carries no UID")
	}
	if string(created.UID) == string(old.UID) {
		t.Fatalf("the replacement control pod reused the old UID %q: it is not a new incarnation", created.UID)
	}
	f.replacement = created
	f.controlPod = created
	// The replacement incarnation signs with its own key and holds its own
	// credential: the worker's trust follows the incarnation.
	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	f.worker.setKey(pub)
	f.workerKey = priv
	// The replacement's credential authenticates as the replacement pod; the
	// dead incarnation's credential still names the pod that is gone.
	f.tokenReviews(t).register(fixtureBrokerTokenTwo,
		"system:serviceaccount:"+f.namespace+":"+topology.ControlSAName(f.run.Name),
		f.controlSAUID, created.Name, string(created.UID))
	// The dead incarnation's last write, attempted the moment it was
	// replaced: the production fence must refuse it — its credential names
	// a pod that no longer exists.
	f.staleWriteErr = f.staleWrite(t, ctx)
	// The refused write left nothing behind: the run carries no checkpoint
	// from the dead incarnation.
	if read := f.fetch(t); read.Status.Checkpoint != nil && read.Status.Checkpoint.Plan == staleCheckpointPlan {
		t.Fatalf("the run carries the dead incarnation's checkpoint %+v: the refused write landed anyway", read.Status.Checkpoint)
	}
}

// runSecondIncarnation plays the replacement control pod: the capability
// gate again, the model turn that declares the outcome, the publication the
// broker's fence accepts, and the durable status the run's ending rests on.
func (f *fixtureC) runSecondIncarnation(t *testing.T, ctx context.Context) {
	t.Helper()
	gateway := f.newGateway()
	// The replacement publishes the commit on top of the one the dead
	// incarnation published: the broker's fence accepts it as a
	// fast-forward of the confirmed tip.
	authenticator := f.buildAuthenticator(t, ctx, f.tokenReviews(t))
	f.statusTransport, f.reporter = f.buildStatusPath(t, ctx, authenticator, f.replacement, fixtureBrokerTokenTwo)

	probe := f.newProbe(t, ctx, authenticator, fixtureBrokerTokenTwo)
	f.capabilityGate(t, ctx, gateway, probe)

	// The replacement's proposal reaches the broker over the broker's own
	// import route, with the confirmed tip as the expected one.
	f.importProposal(t, ctx, f.proposals[1], f.proposals[0], fixtureBrokerTokenTwo)

	delegator := f.newDelegator(t, f.replacement, f.workerKey)
	f.secondPublisher = f.publisherFor(t, f.proposals[1], f.proposals[0], fixtureBrokerTokenTwo)
	coordinator := f.newCoordinator(t, gateway, delegator, f.secondPublisher, f.reporter)

	result := coordinator.Run(ctx, f.invocation(t))
	if result.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("harness result = %+v, want a published ending", result)
	}
	// The durable status the run's ending rests on: the checkpoint and the
	// broker-confirmed commit, written through the authenticated path and
	// fenced to the surviving incarnation.
	if err := f.reporter.CheckpointPublished(ctx, &courierv1alpha1.Checkpoint{
		Plan: survivingCheckpointPlan,
	}, f.proposals[1]); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.reporter.TerminalResult(ctx, courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultPublished,
		Reason:            courierv1alpha1.NativeResultReason(courierv1alpha1.NativeResultPublished),
		CoordinatorPodUID: string(f.replacement.UID),
		At:                metav1.NewTime(time.Now()),
	}, nil); err != nil {
		t.Fatalf("terminal result: %v", err)
	}
}

// tokenReviews returns the fixture's TokenReview backend, building it on
// first use with the first incarnation's identity: the dead incarnation's
// credential authenticates as the pod it was minted for, and the
// replacement's own credential as the pod that replaced it.
func (f *fixtureC) tokenReviews(t *testing.T) *tokenReviews {
	t.Helper()
	if f.reviews == nil {
		username := "system:serviceaccount:" + f.namespace + ":" + topology.ControlSAName(f.run.Name)
		f.reviews = newTokenReviews(username, f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
		f.reviews.register(fixtureBrokerToken, username, f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
	}
	return f.reviews
}

// staleCheckpointPlan is the plan the dead incarnation's refused write
// carries: the fixture asserts the run never holds it.
const staleCheckpointPlan = "the pre-restart checkpoint"

// survivingCheckpointPlan is the plan the surviving incarnation's own
// checkpoint carries.
const survivingCheckpointPlan = "fix the widget"

// staleWrite is the dead incarnation's last status write: the checkpoint
// carrying the commit it published. The production fence must refuse it —
// its credential names a pod that no longer exists.
func (f *fixtureC) staleWrite(t *testing.T, ctx context.Context) error {
	t.Helper()
	return f.staleReporter.CheckpointPublished(ctx, &courierv1alpha1.Checkpoint{
		Plan: staleCheckpointPlan,
	}, f.proposals[0])
}

// settle ends the replacement control pod the way the kubelet would and
// drives the operator to the run's settled ending.
func (f *fixtureC) settle(t *testing.T, ctx context.Context) {
	t.Helper()
	f.terminateControlPod(t, ctx, f.replacement, nativeHandoff("published"), 0)
	observer := &fixtureObserver{pullRequest: f.secondPublisher.opened.Number, head: f.proposals[1]}
	f.reconciler.Observer = observer
	f.waitForPhase(t, ctx, courierv1alpha1.PhaseAwaitingReview)
}

// assertions are the fixture's claims. Every one of them is negative where
// it can be: the fixture proves what did not happen as much as what did.
func (f *fixtureC) assertions(t *testing.T, ctx context.Context) {
	t.Helper()
	run := f.fetch(t)

	// The source identity did not move: the same work item, the same
	// receipt, and no successor run beside it.
	if run.Status.SourceReceipt != f.src.receipt {
		t.Fatalf("run source receipt = %q, want the receipt the claim issued", run.Status.SourceReceipt)
	}
	if string(run.UID) != f.runUIDBefore {
		t.Fatalf("run UID = %q, want the incarnation's own UID %q: the restart must not rematerialize the run", run.UID, f.runUIDBefore)
	}
	assertNoForbiddenSourceMutations(t, f.src, fmt.Sprint(fixtureIssue))
	assertSingleRunForWorkItem(t, f.namespace, fmt.Sprint(fixtureIssue))

	// The dead incarnation's status write was refused: its credential names
	// a pod that no longer exists, so the broker refuses the credential
	// itself, and the run's durable state never carries the write.
	if f.staleWriteErr == nil {
		t.Fatal("the dead incarnation's status write was accepted: the broker's incarnation fence did not hold")
	}
	var callErr *harness.BrokerCallError
	if !errors.As(f.staleWriteErr, &callErr) {
		t.Fatalf("the dead incarnation's status write failed with %v, want the broker's own refusal", f.staleWriteErr)
	}
	if callErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("the dead incarnation's status write was refused with %d, want %d: the broker must refuse the stale credential itself, not merely reject the write", callErr.StatusCode, http.StatusUnauthorized)
	}
	if run.Status.Checkpoint != nil && run.Status.Checkpoint.Plan != survivingCheckpointPlan {
		t.Fatalf("the run carries the checkpoint %+v, want the surviving incarnation's", run.Status.Checkpoint)
	}
	// The broker pod survived the control restart, so the successor's
	// publication was authorized by the broker's own confirmation of the
	// first incarnation's tip.
	if got := string(f.brokerPod.UID); got != f.brokerUIDBefore {
		t.Fatalf("broker pod UID = %q, want the surviving %q: the restart replaced the broker", got, f.brokerUIDBefore)
	}
	// Every heartbeat the dead incarnation wrote names the incarnation that
	// earned it: a heartbeat is fenced to the pod that produced it.
	for _, beat := range f.staleTransport.Heartbeats() {
		if beat.Heartbeat == nil || beat.Heartbeat.CoordinatorPodUID != f.deadPodUID {
			t.Fatalf("the dead incarnation recorded the heartbeat %+v, which is not fenced to its own pod %q", beat.Heartbeat, f.deadPodUID)
		}
	}

	// No stale publication survived: the run's confirmed commit is the OID
	// the surviving incarnation published — not the dead incarnation's, and
	// not empty.
	if run.Status.LastCommit != f.proposals[1] {
		t.Fatalf("run lastCommit = %q, want the published %q, not the pre-restart %q", run.Status.LastCommit, f.proposals[1], f.proposals[0])
	}
	if run.Status.LastCommit == f.proposals[0] {
		t.Fatalf("run lastCommit = %q: the dead incarnation's stale publication survived the restart", run.Status.LastCommit)
	}
	if run.Status.LastCommit == "" {
		t.Fatal("run lastCommit is empty: the surviving incarnation's confirmed commit was never recorded")
	}
	if tip := f.provider.Tip(f.policy.WorkRepo, f.policy.WorkRef); tip != f.proposals[1] {
		t.Fatalf("the work ref tip = %q, want the published %q", tip, f.proposals[1])
	}
	// The publication fence held: the published commit descends from the
	// admission anchor, and the run UID is the one the policy is bound to.
	runGit(t, f.git.Source, "merge-base", "--is-ancestor", f.git.BaseOID, f.proposals[1])

	// The publication stayed singular: two publication attempts, one pull
	// request, one create on the forge.
	if f.firstPublisher.publications != 1 || f.secondPublisher.publications != 1 {
		t.Fatalf("publication attempts = %d and %d, want one each", f.firstPublisher.publications, f.secondPublisher.publications)
	}
	assertExactlyOnePullRequest(t, ctx, f.provider, f.policy.BaseRepo, f.policy.WorkRepo, f.policy.WorkRef, f.firstPublisher.opened.Number)
	pr := f.secondPublisher.opened
	if pr.Number != f.firstPublisher.opened.Number {
		t.Fatalf("the replacement incarnation opened pull request #%d, want the existing #%d", pr.Number, f.firstPublisher.opened.Number)
	}
	if pr.HeadOID != f.proposals[1] {
		t.Fatalf("pull request head OID = %q, want the published %q", pr.HeadOID, f.proposals[1])
	}
	if run.Status.PR != fmt.Sprint(pr.Number) {
		t.Fatalf("run PR = %q, want the pull request %d", run.Status.PR, pr.Number)
	}

	// The operator's own state stayed inspectable and coherent: the run is
	// awaiting review on the verified pull request, it recorded its end, and
	// its conditions carry the settled lifecycle report.
	if run.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview", run.Status.Phase)
	}
	if run.Status.FinishedAt == nil {
		t.Fatal("the run recorded no end: the operator's terminal transition never ran")
	}
	reported := findCondition(run, courierv1alpha1.ConditionLifecycleReported)
	if reported == nil {
		t.Fatalf("the run carries no %s condition: the lifecycle report never settled", courierv1alpha1.ConditionLifecycleReported)
	}
	if reported.Status != metav1.ConditionTrue {
		t.Fatalf("the %s condition = %q, want True", courierv1alpha1.ConditionLifecycleReported, reported.Status)
	}
	if run.Status.NativeResult == nil {
		t.Fatal("the run carries no trusted native result")
	}
	if run.Status.NativeResult.CoordinatorPodUID != string(f.replacement.UID) {
		t.Fatalf("native result UID = %q, want the surviving incarnation %q", run.Status.NativeResult.CoordinatorPodUID, f.replacement.UID)
	}
	if run.Status.NativeResult.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("native result kind = %q, want published", run.Status.NativeResult.Kind)
	}
	// The source's recorded lifecycle is coherent: the claim, the
	// in-progress transition, the in-review transition, and the one report.
	calls := f.src.Calls()
	if len(calls) == 0 || (calls[0].Op != "claim" && calls[0].Op != "claim-with-receipt") {
		t.Fatalf("source calls = %v, want the claim first", f.src.CallNames())
	}
	firstTransition, lastTransition := -1, -1
	for i, call := range calls {
		switch call.Op {
		case "transition":
			if firstTransition < 0 {
				firstTransition = i
			}
			lastTransition = i
		case "report":
			if lastTransition < 0 || i < lastTransition {
				t.Fatalf("the source received a lifecycle report before the in-review transition: %v", f.src.CallNames())
			}
		}
	}
	if firstTransition < 0 {
		t.Fatalf("source calls = %v, want the lifecycle transitions", f.src.CallNames())
	}
	if calls[firstTransition].State != source.StateInProgress {
		t.Fatalf("first transition = %+v, want the in-progress transition", calls[firstTransition])
	}
	if calls[lastTransition].State != source.StateInReview {
		t.Fatalf("last transition = %+v, want the in-review transition", calls[lastTransition])
	}
	reports := f.src.Reports()
	if len(reports) != 1 {
		t.Fatalf("source reports = %d, want exactly one lifecycle report", len(reports))
	}
	if reports[0].State != source.StateInReview || reports[0].Result != source.ResultReady {
		t.Fatalf("lifecycle report = %+v, want the in-review ready result", reports[0])
	}
	if reports[0].PR != fmt.Sprint(pr.Number) {
		t.Fatalf("lifecycle report PR = %q, want the pull request %d", reports[0].PR, pr.Number)
	}

	// The shared negative claims every fixture makes: the deployment's
	// credential reached no recorded surface, the source adapter saw only
	// the calls its contract defines, and the publication is singular.
	assertNoCredentialLeakage(t, fixtureCredential, f.operatorNamespace,
		surface(t, "the dead incarnation's recorded status patches", f.staleTransport.Patches()),
		surface(t, "the surviving incarnation's recorded status patches", f.statusTransport.Patches()),
		surface(t, "the CoderRun's status", run.Status),
		surface(t, "the published pull request", pr),
		surface(t, "the source's recorded calls", f.src.Calls()),
		surface(t, "the source's recorded lifecycle", f.src.Reports()),
	)
	assertNoForbiddenSourceMutations(t, f.src, fmt.Sprint(fixtureIssue))
}

// findCondition returns the run's condition of the given type, or nil.
func findCondition(run *courierv1alpha1.CoderRun, conditionType string) *metav1.Condition {
	for i := range run.Status.Conditions {
		if run.Status.Conditions[i].Type == conditionType {
			return &run.Status.Conditions[i]
		}
	}
	return nil
}
