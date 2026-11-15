package e2e

// Fixture A replays the integrated native control path end to end.
//
// The composition, in the order the run goes through it:
//
//	admission        the production reconciler admits the run in envtest
//	                 through the production secure control: the source is
//	                 claimed with a receipt, the publication policy is
//	                 resolved and persisted, and the secure topology is
//	                 provisioned (a real control pod, worker pod, and broker
//	                 pod exist as API objects and never run).
//	source claim     the recording source adapter issues a receipt; the
//	                 operator persists it as the run's source receipt.
//	capability gate  the production harness probe and gate decide the run may
//	                 proceed, against the production broker's own capability
//	                 report.
//	model turn       the production harness coordinator runs a real role-bound
//	                 turn over the production gateway against the local
//	                 scripted model, dispatches a real worker task over the
//	                 signed protocol, and earns a tool boundary.
//	status progress  the harness's trusted status path writes through the
//	                 production broker status writer into the real API server.
//	publication     the production broker policy engine publishes the
//	                 proposal to the pinned work ref and opens exactly one
//	                 pull request, through the production forge adapter over
//	                 the local-git forge double.
//	settlement       the operator observes the control pod's termination with
//	                 the fenced native terminal result, verifies the world,
//	                 and settles the source.

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
	couriergit "github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/source"
	"github.com/misospace/courier/internal/source/manual"
	"github.com/misospace/courier/internal/status"
	"github.com/misospace/courier/internal/topology"
)

// fixtureDeadline bounds the whole fixture. It is a failure ceiling, never
// the mechanism under test: a regression that hangs the model, the worker,
// or the broker must fail the fixture rather than hang the suite.
const fixtureDeadline = 3 * time.Minute

// The fixture's canonical repository identity. A resolve-issue run publishes
// into the base repository itself, so the base and work repositories are one
// identity.
const (
	fixtureOwner   = "acme"
	fixtureRepo    = "widgets"
	fixtureRepoID  = "repo-1"
	fixtureIssue   = 122
	fixtureDefault = "main"
)

// fixtureSecondIssue is a second work item in the same repository. The lane
// capacity proof needs a second run the failed run's lane can admit.
const fixtureSecondIssue = 123

// fixtureProviderName is the provider registration's name. It is the
// identity the persisted publication policy pins and the broker's policy
// binds.
const fixtureProviderName = "local"

// fixtureA holds every piece of the fixture.
type fixtureA struct {
	// log is the shared timeline of what the doubles observed.
	log *eventLog
	// git is the local git world.
	git *gitFixture
	// provider is the local-git forge double.
	provider *forgetest.LocalGit
	// model is the scripted local model gateway.
	model *scriptedModel
	// worker is the signed-protocol worker double.
	worker *workerDouble
	// workerKey is the control incarnation's signing keypair.
	workerKey ed25519.PrivateKey
	// src is the recording source adapter.
	src *recordingSource
	// sourceAdapter is the adapter registered under the source name. The
	// recording adapter is the fixture's own; the non-dispatch fixture
	// registers the production manual adapter instead.
	sourceAdapter source.Adapter

	// namespace is the dedicated run namespace.
	namespace string
	// operatorNamespace is the namespace the deployment's credential
	// Secrets live in.
	operatorNamespace string
	// runName and laneName are the fixture's own object names.
	runName  string
	laneName string
	// lane is the run's lane profile.
	lane *courierv1alpha1.LaneProfile
	// run is the run under test.
	run *courierv1alpha1.CoderRun
	// reconciler is the production reconciler.
	reconciler *controller.CoderRunReconciler

	// proposal is the commit the control side proposes for publication.
	proposal string
	// proposals are every commit the fixture's control side may publish, in
	// commit order.
	proposals []string
	// bundlePath is the git bundle the broker imports the proposal from.
	bundlePath string
	// runUID is the live run's UID.
	runUID string
	// engine is the production broker policy engine.
	engine *broker.PolicyEngine
	// policy is the broker policy bound to the live run.
	policy broker.Policy
	// adapter is the production forge adapter over the local-git double.
	adapter *broker.ForgeAdapter
	// gitBroker is the broker's own private git object store.
	gitBroker *couriergit.Broker
	// publisher is the trusted publication seam the coordinator drives.
	publisher *fixturePublisher
	// workerURL is the worker double's listener.
	workerURL string

	// controlPod, workerPod, and brokerPod are the provisioned topology.
	controlPod *corev1.Pod
	workerPod  *corev1.Pod
	brokerPod  *corev1.Pod
	// controlSAUID is the control service account's UID.
	controlSAUID string

	// statusTransport records the harness's status patches.
	statusTransport *recordingStatusTransport
	// reporter is the production status reporter.
	reporter *harness.StatusReporter
	// coordinator is the production harness coordinator.
	// result is the harness's terminal result.
	result executor.HarnessResult
	// pullRequest is the pull request the publication opened.
	pullRequest broker.PullRequestState
}

// fixturePublisher is the trusted publication seam the coordinator drives.
// It holds no policy of its own: every publication decision — the pinned
// identity, the fast-forward check, the duplicate-pull-request guard, the
// linkage contract — is the production broker policy engine's.
type fixturePublisher struct {
	engine   *broker.PolicyEngine
	runUID   string
	proposal string
	// expected is the work tip this incarnation expects to find, exactly as
	// the production control side derives it from the live world: empty for
	// the run's first commit, the live tip for every later one.
	expected string
	// opened is the pull request the publication opened.
	opened broker.PullRequestState
	// publications counts the publication attempts this seam made, so a
	// fixture can prove that two publications produced one pull request.
	publications int
}

var _ harness.Publisher = (*fixturePublisher)(nil)
var _ harness.PullRequestOpener = (*fixturePublisher)(nil)

// Integrate is unreachable in this fixture: the coordinator integrates only
// a delegated brief's artifact, and the fixture's model delegates nothing.
func (p *fixturePublisher) Integrate(context.Context, harness.IntegrationRequest) (harness.Integration, error) {
	return harness.Integration{}, errors.New("e2e: no brief was delegated, so there is nothing to integrate")
}

// Publish fast-forwards the pinned work ref to the proposal through the
// production broker policy engine. The pull request is not opened here: the
// coordinator's finish opens it through OpenPullRequest with the title and
// body the model declared (#844).
func (p *fixturePublisher) Publish(ctx context.Context) (harness.Publication, error) {
	p.publications++
	result, err := p.engine.Publish(ctx, broker.PublicationRequest{RunUID: p.runUID, ProposedOID: p.proposal, ExpectedWorkOID: p.expected})
	if err != nil {
		return harness.Publication{}, err
	}
	return harness.Publication{OID: result.OID, AlreadyPublished: result.AlreadyPublished}, nil
}

// OpenPullRequest opens the run's pull request at the proposed head through
// the production broker policy engine's create route (#844), with the
// title and body the model declared: the linkage contract and the
// duplicate-pull-request guard are the engine's decision, so a re-open after
// a restart adopts the discovered pull request instead of creating a second
// one.
func (p *fixturePublisher) OpenPullRequest(ctx context.Context, title, body string, draft bool) (harness.PullRequest, error) {
	pr, report, err := p.engine.CreatePullRequest(ctx, p.proposal, title, body, draft)
	if err != nil {
		return harness.PullRequest{}, classifyCreateRefusal(err)
	}
	p.opened = pr
	opened := harness.PullRequest{
		Number:   pr.Number,
		State:    pr.State,
		HeadRepo: pr.HeadRepo,
		HeadRef:  pr.HeadRef,
		HeadOID:  pr.HeadOID,
	}
	if report != nil {
		opened.Coverage = report.Coverage
	}
	return opened, nil
}

// classifyCreateRefusal maps the policy engine's answer to the create route
// into the coordinator's taxonomy, the way the broker's own HTTP route
// classifies it (internal/broker/server.go): the route answers every error
// the engine returns with HTTP 422, and the harness reads a non-retryable 4xx
// as a definite denial — a blocked world, never an infrastructure failure.
//
// The fixture classifies the engine's answer instead of matching its error
// text, so a reworded refusal still reads as a refusal instead of silently
// becoming a transport failure. Only the transport class stays an
// infrastructure failure: a cancelled or expired call never reached an
// answer, exactly as it never reaches one on the wire.
func classifyCreateRefusal(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &harness.PublicationBlockedError{Reason: err.Error(), Err: err}
}

// TestCreateRefusalClassification pins the classification the coordinator
// sees when the create route refuses: a refusal the engine states is a
// blocked world, whatever the engine words it with, and only the transport
// class is an infrastructure failure.
func TestCreateRefusalClassification(t *testing.T) {
	for _, refusal := range []error{
		errors.New("publication denied: the proposed commit is not the exact live head"),
		errors.New("publication denied: multiple pull requests claim the pinned head"),
		errors.New("check existing pull request failed"),
	} {
		classified := classifyCreateRefusal(refusal)
		var blocked *harness.PublicationBlockedError
		if !errors.As(classified, &blocked) {
			t.Fatalf("classified refusal = %v, want a publication block", classified)
		}
		if blocked.Err != refusal {
			t.Fatalf("blocked error = %v, want the engine's own answer", blocked.Err)
		}
		if blocked.Reason != refusal.Error() {
			t.Fatalf("blocked reason = %q, want the engine's own wording %q", blocked.Reason, refusal.Error())
		}
	}
	for _, transport := range []error{context.Canceled, context.DeadlineExceeded} {
		classified := classifyCreateRefusal(transport)
		var blocked *harness.PublicationBlockedError
		if errors.As(classified, &blocked) {
			t.Fatalf("classified transport failure = %v, want an infrastructure failure", classified)
		}
	}
	if classified := classifyCreateRefusal(nil); classified != nil {
		t.Fatalf("classified absence = %v, want no error", classified)
	}
}

// TestFixtureANativeControlPath is the fixture itself.
func TestFixtureANativeControlPath(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureDeadline)
	defer cancel()

	f := newFixtureA(t)
	f.admit(t, ctx)
	f.runControl(t, ctx)
	f.settle(t, ctx)
	f.assertions(t, ctx)
}

// newFixtureA builds the fixture's world: the local git forge, the scripted
// model gateway, the worker double, the recording source, and the run the
// operator will admit.
func newFixtureA(t *testing.T) *fixtureA {
	t.Helper()
	f := newFixtureWorld(t, scriptedResponses(), fixtureProposal{name: "widget.txt", content: "the widget\n"})
	f.src = newRecordingSource("receipt-" + f.git.BaseOID[:8])
	f.sourceAdapter = f.src
	f.runName = "e2e-fixture-a"
	f.laneName = "e2e-lane"
	f.prepareCluster(t)
	return f
}

// newManualFixtureA builds the same world with the production manual
// adapter registered under the source name: a source with no queue, no
// dispatch, and no lifecycle report.
func newManualFixtureA(t *testing.T) *fixtureA {
	t.Helper()
	f := newFixtureWorld(t, scriptedResponses(), fixtureProposal{name: "widget.txt", content: "the widget\n"})
	f.sourceAdapter = manual.New(nil)
	f.runName = "e2e-non-dispatch"
	f.laneName = "e2e-non-dispatch-lane"
	f.prepareCluster(t)
	return f
}

// fixtureProposal is one commit the fixture's control side may publish.
type fixtureProposal struct {
	// name is the file the proposal changes.
	name string
	// content is the file's new content.
	content string
}

// scriptedResponses is the fixture world's model script: the capability
// gate's completion probe, the coordinator's first turn (one worker task),
// and the coordinator's second turn (the outcome declaration). The resolve-
// issue run opens its pull request through the broker's create route, so the
// changes declaration carries the pull-request title and body (#844) that
// route validates against the pinned linkage.
func scriptedResponses() []modelResponse {
	return []modelResponse{
		{sse: sse(contentChunk("probe"), finishChunk("stop"))},
		{sse: sse(toolCallChunk(0, "call-shell", "shell", `{"command":"echo courier"}`), finishChunk("tool_calls"))},
		{sse: sse(contentChunk(fixtureChangesDeclaration()), finishChunk("stop"))},
	}
}

// fixtureChangesDeclaration is the changes outcome declaration the scripted
// coordinator emits on a run that opens its pull request: the title and body
// the broker's create route validates against the pinned linkage.
func fixtureChangesDeclaration() string {
	return fmt.Sprintf(`{"outcome":"changes","prTitle":"Add the widget","prBody":"Closes %s/%s#%d."}`, fixtureOwner, fixtureRepo, fixtureIssue)
}

// newFixtureWorld builds the parts every fixture shares: the local git
// forge, the scripted model gateway, and the worker double. The model script
// and the proposals are the fixture's own: the script decides what the
// provider does, and the proposals are the commits the control side may
// publish.
func newFixtureWorld(t *testing.T, responses []modelResponse, proposals ...fixtureProposal) *fixtureA {
	t.Helper()
	log := &eventLog{}

	gitWorld := newGitFixture(t)
	oids := make([]string, 0, len(proposals))
	for _, proposal := range proposals {
		oids = append(oids, gitWorld.Propose(t, proposal.name, proposal.content))
	}
	// The bundle is written after every proposal, so it carries the whole
	// history the control side may publish.
	bundle := gitWorld.Bundle(t)

	canonical := fixtureOwner + "/" + fixtureRepo
	repository := forge.Repository{ID: fixtureRepoID, Canonical: canonical, DefaultRef: fixtureDefault}
	provider := forgetest.NewLocalGit(forgetest.LocalGitConfig{
		Provider: forge.ProviderConfig{Name: fixtureProviderName, Endpoint: "https://forge.invalid/"},
		Repositories: map[string]forge.Repository{
			canonical: repository,
		},
		Paths: map[string]string{
			canonical: gitWorld.Bare,
		},
		WorkItems: map[string]forge.WorkItem{
			forgetest.LocalGitWorkItemKey(canonical, fmt.Sprint(fixtureIssue)): {
				ID:    fmt.Sprint(fixtureIssue),
				Repo:  canonical,
				Title: "Add the widget",
				// The issue body carries no acceptance section: the pinned
				// criterion set is then empty, so the pull-request body
				// contract is the closing linkage alone.
				Body:  "## Problem\n\nThe widget is missing.\n",
				State: "open",
			},
			forgetest.LocalGitWorkItemKey(canonical, fmt.Sprint(fixtureSecondIssue)): {
				ID:    fmt.Sprint(fixtureSecondIssue),
				Repo:  canonical,
				Title: "Add the gadget",
				Body:  "## Problem\n\nThe gadget is missing.\n",
				State: "open",
			},
		},
		NextPullRequest: 10,
	})

	model := newScriptedModelWith(t, log, responses)

	pub, priv, err := protocol.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	worker := newWorkerDouble(pub, gitWorld.BaseOID, log)
	workerServer := httptest.NewServer(worker)
	t.Cleanup(workerServer.Close)

	return &fixtureA{
		log:               log,
		git:               gitWorld,
		provider:          provider,
		model:             model,
		worker:            worker,
		workerKey:         priv,
		workerURL:         workerServer.URL,
		namespace:         "courier-runs",
		operatorNamespace: "courier-system",
		proposal:          oids[0],
		proposals:         oids,
		bundlePath:        bundle,
	}
}

// prepareCluster creates the namespaces, the deployment's credential Secret,
// the lane profile, and the run the operator admits.
func (f *fixtureA) prepareCluster(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	// The dedicated run namespace with the restricted Pod Security label the
	// secure path refuses to run without.
	if err := envtestClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   f.namespace,
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
	}}); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create run namespace: %v", err)
	}
	if err := envtestClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: f.operatorNamespace,
	}}); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create operator namespace: %v", err)
	}
	// The deployment's provider credential, which the operator copies into
	// the per-run secret the broker reads.
	if err := envtestClient.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: f.operatorNamespace,
		Name:      "forge-creds",
	}, Data: map[string][]byte{"token": []byte(fixtureCredential)}}); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create credential secret: %v", err)
	}
	// The deployment-scoped grant the chart renders for the broker's
	// TokenReview creation. The secure preflight proves the broker's own
	// grants before it serves, so the grant must exist.
	if err := envtestClient.Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{
		Name: topology.TokenReviewClusterRole,
	}, Rules: []rbacv1.PolicyRule{{
		APIGroups: []string{"authentication.k8s.io"},
		Resources: []string{"tokenreviews"},
		Verbs:     []string{"create"},
	}}}); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create token-review cluster role: %v", err)
	}

	f.lane = &courierv1alpha1.LaneProfile{
		ObjectMeta: metav1.ObjectMeta{Name: f.laneName, Namespace: f.namespace},
		Spec: courierv1alpha1.LaneProfileSpec{
			Concurrency: 1,
			Roles:       map[string]string{"coordinator": "test/coordinator"},
		},
	}
	if err := envtestClient.Create(ctx, f.lane); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create lane: %v", err)
	}

	f.run = &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: f.runName, Namespace: f.namespace},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:       courierv1alpha1.ModeResolveIssue,
			Source:     "manual",
			WorkItemID: fmt.Sprint(fixtureIssue),
			Repo:       fixtureOwner + "/" + fixtureRepo,
			Ref:        fixtureIssue,
			Lane:       f.lane.Name,
		},
	}
	if err := envtestClient.Create(ctx, f.run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	created := &courierv1alpha1.CoderRun{}
	if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(f.run), created); err != nil {
		t.Fatalf("read run: %v", err)
	}
	f.runUID = string(created.UID)

	clientset, err := kubernetes.NewForConfig(envtestConfig)
	if err != nil {
		t.Fatalf("clientset: %v", err)
	}
	registry, err := forge.Load([]byte(`{"providers":[{"name":"local","type":"github","endpoint":"https://forge.invalid/","credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["acme/*"]}]}`), "github")
	if err != nil {
		t.Fatalf("forge registry: %v", err)
	}
	sources := controller.NewSourceRegistry(map[string]source.Adapter{"manual": f.sourceAdapter})
	f.reconciler = &controller.CoderRunReconciler{
		Client:    envtestClient,
		APIReader: envtestClient,
		Sources:   sources,
		StatusWriter: status.KubePatchWriter{
			Client: envtestClient,
		},
		Launch: func(context.Context, *courierv1alpha1.CoderRun) error {
			// The secure path owns the launch; the legacy launcher must
			// never run on it.
			return errors.New("the legacy launch must not run on the secure path")
		},
		Now: func() time.Time { return time.Now() },
	}
	secure := &controller.SecureControl{
		Client:    envtestClient,
		APIReader: envtestClient,
		Sources:   sources,
		Config: controller.SecureConfig{
			RunNamespace:      f.namespace,
			Registry:          registry,
			HarnessImage:      "harness:test",
			ProbeImage:        "probe:test",
			LiveProbes:        false,
			OperatorNamespace: f.operatorNamespace,
			Providers: func(context.Context, *forge.Registration) (controller.AdmissionProvider, error) {
				return controller.AdmissionProvider{Provider: f.provider, Policy: f.provider, HeadLister: f.provider}, nil
			},
			Kubernetes: clientset,
			Now:        func() time.Time { return time.Now() },
		},
	}
	f.reconciler.Secure = secure
	secure.Patcher = f.reconciler
}

// admit drives the operator's admission: the source is claimed with a
// receipt, the publication policy is resolved and persisted, and the secure
// topology is provisioned.
func (f *fixtureA) admit(t *testing.T, ctx context.Context) {
	t.Helper()
	// The admission may be refused for a transient reason — the API
	// server's RBAC authorizer needs a moment to observe the topology's own
	// grants, and the operator's own preflight backoff bounds its own
	// retries — so the reconcile is driven until the run reaches Running,
	// exactly as a controller would.
	var firstErr error
	for {
		_, err := f.reconciler.Reconcile(ctx, f.request())
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if err == nil {
			run := f.fetch(t)
			if run.Status.Phase == courierv1alpha1.PhaseRunning {
				break
			}
			// A terminal phase is an admission the operator refused, not a
			// slow one: report it now instead of waiting out the deadline.
			if terminalPhase(run.Status.Phase) {
				t.Fatalf("admission ended in the terminal phase %q: %s", run.Status.Phase, terminalReason(run))
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("admission did not finish: %v (first error %v)", ctx.Err(), firstErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	run := f.fetch(t)
	if run.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running after admission (first error %v)", run.Status.Phase, firstErr)
	}
	if run.Status.PublicationPolicy == nil {
		t.Fatal("admission must persist the publication policy")
	}
	policy := run.Status.PublicationPolicy
	if policy.BaseRepo != fixtureOwner+"/"+fixtureRepo || policy.BaseRef != fixtureDefault {
		t.Fatalf("pinned base = %q/%q", policy.BaseRepo, policy.BaseRef)
	}
	if policy.BaseOID != f.git.BaseOID {
		t.Fatalf("pinned base OID = %q, want the live base tip %q", policy.BaseOID, f.git.BaseOID)
	}
	if policy.WorkRepo != policy.BaseRepo || policy.WorkRef != branch.ResolveIssueBranch(policy.BaseRepo, fixtureIssue) {
		t.Fatalf("pinned work identity = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	if !policy.WorkInitiallyAbsent {
		t.Fatal("the pinned work ref is absent at admission and must be recorded as such")
	}
	f.controlPod = f.pod(t, topology.ComponentCoordinator)
	f.workerPod = f.pod(t, topology.ComponentWorker)
	f.brokerPod = f.pod(t, topology.ComponentBroker)
	sa := &corev1.ServiceAccount{}
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: f.namespace, Name: topology.ControlSAName(f.run.Name)}, sa); err != nil {
		t.Fatalf("read control service account: %v", err)
	}
	f.controlSAUID = string(sa.UID)
}

// runControl plays the control pod: the capability gate, the model turn with
// its earned tool activity, the authenticated status progress, and the
// fenced publication.
func (f *fixtureA) runControl(t *testing.T, ctx context.Context) {
	t.Helper()
	gateway := f.newGateway()

	// The publication seam: the production broker policy engine over the
	// production forge adapter and the local-git forge double.
	f.buildPublication(t, ctx)
	f.importProposal(t, ctx, f.proposal, f.git.BaseOID)

	// The trusted status path: the production broker authenticator, the
	// production status writer, and the production trusted status handler,
	// served over the fixture's own listener.
	reviews := newTokenReviews("system:serviceaccount:"+f.namespace+":"+topology.ControlSAName(f.run.Name),
		f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
	reviews.register(fixtureBrokerToken, "system:serviceaccount:"+f.namespace+":"+topology.ControlSAName(f.run.Name),
		f.controlSAUID, f.controlPod.Name, string(f.controlPod.UID))
	authenticator := f.buildAuthenticator(t, ctx, reviews)
	f.statusTransport, f.reporter = f.buildStatusPath(t, ctx, authenticator, f.controlPod, fixtureBrokerToken)

	// The native capability gate: the production probe and gate over the
	// production broker's own capability report.
	probe := f.newProbe(t, ctx, authenticator, fixtureBrokerToken)
	f.capabilityGate(t, ctx, gateway, probe)

	delegator := f.newDelegator(t, f.controlPod, f.workerKey)
	f.publisher = f.publisherFor(f.proposal, "")
	coordinator := f.newCoordinator(t, gateway, delegator, f.publisher, f.reporter)

	f.result = coordinator.Run(ctx, f.invocation(t))
	if f.result.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("harness result = %+v, want a published ending", f.result)
	}
	f.pullRequest = f.publisher.opened

	// The finish recorded the pull-request identity through the trusted
	// status path (#844) before the published claim stands: the settle
	// observer that also names the pull request is not wired yet, so this
	// record can only be the harness's own write.
	if got := f.fetch(t).Status.PR; got != fmt.Sprint(f.pullRequest.Number) {
		t.Fatalf("run PR = %q right after the finish, want the opened pull request %d already recorded through the trusted status path", got, f.pullRequest.Number)
	}

	// The durable status the run's ending rests on: the checkpoint and the
	// broker-confirmed commit, written through the authenticated path.
	if err := f.reporter.CheckpointPublished(ctx, &courierv1alpha1.Checkpoint{
		Plan: "fix the widget",
	}, f.proposal); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := f.reporter.TerminalResult(ctx, courierv1alpha1.NativeResult{
		Kind:              courierv1alpha1.NativeResultPublished,
		Reason:            courierv1alpha1.NativeResultReason(courierv1alpha1.NativeResultPublished),
		CoordinatorPodUID: string(f.controlPod.UID),
		At:                metav1.NewTime(time.Now()),
	}, nil); err != nil {
		t.Fatalf("terminal result: %v", err)
	}
}

// startBrokerServer runs the production broker over the fixture's own TLS
// listener: the policy engine the publication runs on, the production
// authenticator, the production bundle importer, and the provider's
// capability report.
func (f *fixtureA) startBrokerServer(t *testing.T, authenticator broker.RequestAuthenticator, importer broker.BundleImporter) *httptest.Server {
	t.Helper()
	scratch := filepath.Join(f.git.Root, "broker-scratch")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	tls, err := topology.GenerateBrokerTLS([]string{"localhost"})
	if err != nil {
		t.Fatal(err)
	}
	certFile := filepath.Join(f.git.Root, "broker.crt")
	keyFile := filepath.Join(f.git.Root, "broker.key")
	writeFile(t, certFile, string(tls.ServerCertPEM))
	writeFile(t, keyFile, string(tls.ServerKeyPEM))
	server, err := broker.NewServer(broker.ServerConfig{
		Policy:        f.engine,
		Authenticator: authenticator,
		Importer:      importer,
		ScratchDir:    scratch,
		TLSCertFile:   certFile,
		TLSKeyFile:    keyFile,
		Capabilities:  fixtureCapabilities{provider: f.provider, endpoint: f.git.Bare},
	})
	if err != nil {
		t.Fatalf("broker server: %v", err)
	}
	httpServer := httptest.NewTLSServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return httpServer
}

// fixtureCapabilities reports the provider's capability surface the way the
// broker reports it: the pinned provider's identity, the operations it
// registers, and the git endpoint's readiness.
type fixtureCapabilities struct {
	provider forge.Provider
	endpoint string
}

var _ broker.CapabilityReporter = fixtureCapabilities{}

// CapabilityReport renders the report.
func (c fixtureCapabilities) CapabilityReport() broker.CapabilityReport {
	caps := c.provider.Capabilities()
	report := broker.CapabilityReport{
		Provider: broker.ProviderReport{
			Name:     c.provider.Config().Name,
			Type:     "github",
			Endpoint: c.provider.Config().Endpoint,
		},
		Git: broker.GitReport{Endpoint: c.endpoint, Ready: true},
	}
	for _, capability := range caps.List() {
		availability := caps.Available(capability)
		report.Operations = append(report.Operations, broker.OperationReport{
			Name:      string(capability),
			Available: availability.Available,
			Detail:    availability.Detail,
		})
	}
	return report
}

// settle marks the control pod's container terminated the way the kubelet
// would and drives the operator to the run's settled ending.
func (f *fixtureA) settle(t *testing.T, ctx context.Context) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: f.namespace, Name: f.controlPod.Name}, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 0,
			Message:  nativeHandoff("published"),
		}},
	}}
	if err := envtestClient.Status().Update(ctx, pod); err != nil {
		t.Fatalf("terminate the control pod: %v", err)
	}
	// The operator verifies the world before it settles: the observation
	// names the published pull request and a green check, and the settle
	// needs two identical observations.
	observer := &fixtureObserver{pullRequest: f.pullRequest, head: f.proposal}
	f.reconciler.Observer = observer
	for attempt := 0; attempt < 6; attempt++ {
		if _, err := f.reconciler.Reconcile(ctx, f.request()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		run := f.fetch(t)
		if run.Status.Phase == courierv1alpha1.PhaseAwaitingReview {
			return
		}
	}
	t.Fatalf("phase = %q, want AwaitingReview after the operator verified the world", f.fetch(t).Status.Phase)
}

// fixtureObserver is the world observer the operator verifies the published
// pull request with. It reports the pull request the publication opened and
// a green check on it.
type fixtureObserver struct {
	pullRequest broker.PullRequestState
	head        string
}

func (o *fixtureObserver) Observe(context.Context, string, controller.HeadRef) (controller.PRObservation, error) {
	return controller.PRObservation{
		PR:   fmt.Sprint(o.pullRequest.Number),
		Head: o.head,
		Checks: []controller.CheckObservation{
			{Name: "check-a", State: controller.CheckStatePassed},
		},
	}, nil
}

// TestFixtureANonDispatchSource proves the native path has no Dispatch
// dependency: the run is owned by the production manual adapter — a source
// with no queue, no dispatch, and no lifecycle report — and the native
// secure path still admits it, pins its publication identity, and
// provisions its topology.
func TestFixtureANonDispatchSource(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), fixtureDeadline)
	defer cancel()

	f := newManualFixtureA(t)
	f.admit(t, ctx)

	run := f.fetch(t)
	if run.Status.SourceReceipt != "" {
		t.Fatalf("source receipt = %q, want none: the manual adapter mints no claim receipt", run.Status.SourceReceipt)
	}
	policy := run.Status.PublicationPolicy
	if policy == nil {
		t.Fatal("admission must persist the publication policy")
	}
	if policy.BaseRepo != fixtureOwner+"/"+fixtureRepo || policy.BaseRef != fixtureDefault {
		t.Fatalf("pinned base = %q/%q", policy.BaseRepo, policy.BaseRef)
	}
	if policy.WorkRepo != policy.BaseRepo || policy.WorkRef != branch.ResolveIssueBranch(policy.BaseRepo, fixtureIssue) {
		t.Fatalf("pinned work identity = %q/%q", policy.WorkRepo, policy.WorkRef)
	}
	// The topology is provisioned: a control pod, a worker pod, and a
	// broker pod exist as API objects.
	for _, component := range []string{topology.ComponentCoordinator, topology.ComponentWorker, topology.ComponentBroker} {
		pod := f.pod(t, component)
		if pod.UID == "" {
			t.Fatalf("the %s pod carries no UID", component)
		}
	}
}

// assertions are the fixture's claims. Every one of them is negative where
// it can be: the fixture proves what did not happen as much as what did.
func (f *fixtureA) assertions(t *testing.T, ctx context.Context) {
	t.Helper()
	run := f.fetch(t)

	// The published identity is exactly the run's pinned identity.
	if got := f.provider.Tip(f.policy.WorkRepo, f.policy.WorkRef); got != f.proposal {
		t.Fatalf("published work tip = %q, want the proposal %q", got, f.proposal)
	}
	pr := f.pullRequest
	if pr.BaseRepo != f.policy.BaseRepo || pr.BaseRef != f.policy.BaseRef {
		t.Fatalf("pull request base = %q/%q, want the pinned %q/%q", pr.BaseRepo, pr.BaseRef, f.policy.BaseRepo, f.policy.BaseRef)
	}
	if pr.HeadRepo != f.policy.WorkRepo || pr.HeadRef != f.policy.WorkRef {
		t.Fatalf("pull request head = %q/%q, want the pinned %q/%q", pr.HeadRepo, pr.HeadRef, f.policy.WorkRepo, f.policy.WorkRef)
	}
	if pr.HeadOID != f.proposal {
		t.Fatalf("pull request head OID = %q, want the published %q", pr.HeadOID, f.proposal)
	}
	if pr.BaseOID != f.policy.BaseOID {
		t.Fatalf("pull request base OID = %q, want the pinned %q", pr.BaseOID, f.policy.BaseOID)
	}
	if run.Status.PR != fmt.Sprint(pr.Number) {
		t.Fatalf("run PR = %q, want the published pull request %d", run.Status.PR, pr.Number)
	}
	if run.Status.Branch != f.policy.WorkRef {
		t.Fatalf("run branch = %q, want the pinned publication branch %q", run.Status.Branch, f.policy.WorkRef)
	}

	// Exactly one pull request claims the pinned head: a second one would be
	// a duplicate publication.
	pulls, err := f.provider.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: f.policy.BaseRepo}, f.policy.WorkRepo, f.policy.WorkRef)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 1 {
		t.Fatalf("pull requests on the pinned head = %d, want exactly one", len(pulls))
	}
	if pulls[0].Number != pr.Number {
		t.Fatalf("the pull request on the pinned head is #%d, want #%d", pulls[0].Number, pr.Number)
	}
	if creates := f.provider.OpsOf(forgetest.LocalGitOpCreatePullRequest); len(creates) != 1 {
		t.Fatalf("the forge recorded %d pull-request creates, want exactly one: the publication must not duplicate itself", len(creates))
	}
	// The shared negative claims every fixture makes: the deployment's
	// credential reached no recorded surface, the source adapter saw only
	// the calls its contract defines, and the publication is singular.
	//
	// The workload specs are scanned too: the credential belongs in the
	// run's secret, which the broker reads, and nowhere in a pod the
	// operator renders.
	assertNoCredentialLeakage(t, fixtureCredential, f.operatorNamespace,
		surface(t, "recorded status patches", f.statusTransport.Patches()),
		surface(t, "the CoderRun's status", run.Status),
		surface(t, "the CoderRun's metadata", run.ObjectMeta),
		surface(t, "the published pull request", pr),
		surface(t, "the source's recorded calls", f.src.Calls()),
		surface(t, "the source's recorded lifecycle", f.src.Reports()),
		surface(t, "the control pod's spec", f.controlPod.Spec),
		surface(t, "the worker pod's spec", f.workerPod.Spec),
		surface(t, "the broker pod's spec", f.brokerPod.Spec),
	)
	assertNoForbiddenSourceMutations(t, f.src, fmt.Sprint(fixtureIssue))
	assertExactlyOnePullRequest(t, ctx, f.provider, f.policy.BaseRepo, f.policy.WorkRepo, f.policy.WorkRef, pr.Number)

	// No merge path exists, and the evidence is the production surface, not
	// the double's own vocabulary:
	//
	//   - the forge contract's capability vocabulary is closed, so a provider
	//     can never advertise a merge verb (internal/forge/forge.go),
	//   - the coordinator offers the model no merge-shaped tool, so no turn
	//     can ask for one (internal/harness/coordinator.go),
	//   - the published pull request is still open, and the run is not Done.
	//
	// The worker's shell is the residual path a replay cannot close: the
	// worker holds no forge credential and the secure topology denies it
	// network access to the forge, but envtest enforces no network policy.
	// That stays a live-cluster check, named in the runbook.
	for _, capability := range []forge.Capability{"merge", "merge-pull-request", "close-pull-request"} {
		if forge.NewCapabilities(capability).Has(capability) {
			t.Fatalf("the forge contract accepted the merge-shaped capability %q", capability)
		}
	}
	for _, tool := range f.offeredTools() {
		if isMergeShaped(tool) {
			t.Fatalf("the coordinator offered the model the merge-shaped tool %q", tool)
		}
	}
	if pr.State != "open" {
		t.Fatalf("pull request state = %q, want it open: nothing merged it", pr.State)
	}
	if run.Status.Phase == courierv1alpha1.PhaseDone {
		t.Fatal("the run reached Done; a merge happened that the contract has no verb for")
	}

	// The status progress that landed came from the authenticated control
	// pod identity.
	if run.Status.Heartbeat == nil {
		t.Fatal("no heartbeat landed on the run")
	}
	if run.Status.Heartbeat.CoordinatorPodUID != string(f.controlPod.UID) {
		t.Fatalf("heartbeat UID = %q, want the control pod %q", run.Status.Heartbeat.CoordinatorPodUID, f.controlPod.UID)
	}
	if run.Status.Heartbeat.Kind != "stream" && run.Status.Heartbeat.Kind != "tool" {
		t.Fatalf("heartbeat kind = %q, want an earned kind", run.Status.Heartbeat.Kind)
	}
	if run.Status.Checkpoint == nil {
		t.Fatal("no checkpoint landed on the run")
	}
	if run.Status.LastCommit != f.proposal {
		t.Fatalf("run lastCommit = %q, want the published %q", run.Status.LastCommit, f.proposal)
	}
	if run.Status.NativeResult == nil {
		t.Fatal("no trusted native result landed on the run")
	}
	if run.Status.NativeResult.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("native result kind = %q, want published", run.Status.NativeResult.Kind)
	}
	if run.Status.NativeResult.CoordinatorPodUID != string(f.controlPod.UID) {
		t.Fatalf("native result UID = %q, want the control pod %q", run.Status.NativeResult.CoordinatorPodUID, f.controlPod.UID)
	}

	// No heartbeat was produced before real model and tool activity: the
	// first heartbeat came after the model answered and after the worker's
	// task completed. The worker double executes nothing, so the task it
	// accepts is complete the moment it is dispatched; the event therefore
	// marks the verified completion the harness's tool boundary reports.
	events := f.log.Events()
	firstModel := f.log.IndexOf("model-request")
	firstWorker := f.log.IndexOf("worker-task-completed")
	if firstModel < 0 {
		t.Fatal("the model gateway received no request")
	}
	if firstWorker < 0 {
		t.Fatal("the worker completed no task")
	}
	firstHeartbeat := -1
	for i, event := range events {
		if strings.HasPrefix(event, "status-heartbeat:") {
			firstHeartbeat = i
			break
		}
	}
	if firstHeartbeat < 0 {
		t.Fatal("the harness wrote no heartbeat")
	}
	if firstHeartbeat < firstModel {
		t.Fatalf("the first heartbeat at %d was written before the model's first request at %d: status was written before any activity was earned", firstHeartbeat, firstModel)
	}
	if firstHeartbeat < firstWorker {
		t.Fatalf("the first heartbeat at %d was written before the worker completed a task at %d: status was written before any tool completed", firstHeartbeat, firstWorker)
	}
	if len(f.statusTransport.Heartbeats()) == 0 {
		t.Fatal("the harness wrote no heartbeat")
	}

	// The source saw the claim, the transitions, and the report, in that
	// order, and the receipt it issued is the one the run carries. A
	// retried admission may claim and release again before the run is
	// launched, so the sequence is anchored on the report: the lifecycle
	// that led to it is the in-progress transition, the in-review
	// transition, and the report itself.
	calls := f.src.Calls()
	report := -1
	for i, call := range calls {
		if call.Op == "report" {
			report = i
			break
		}
	}
	if report < 0 {
		t.Fatalf("source calls = %v, want a lifecycle report", f.src.CallNames())
	}
	if report < 2 {
		t.Fatalf("source calls = %v, want the transitions before the report", f.src.CallNames())
	}
	if calls[report-2].Op != "transition" || calls[report-2].State != source.StateInProgress {
		t.Fatalf("the call before the in-review transition = %+v, want the in-progress transition", calls[report-2])
	}
	if calls[report-1].Op != "transition" || calls[report-1].State != source.StateInReview {
		t.Fatalf("the call before the report = %+v, want the in-review transition", calls[report-1])
	}
	first := -1
	for i, call := range calls {
		if call.Op == "transition" {
			first = i
			break
		}
	}
	if first < 0 {
		t.Fatalf("source calls = %v, want a transition", f.src.CallNames())
	}
	for _, call := range calls[:first] {
		if call.Op != "claim-with-receipt" && call.Op != "claim" && call.Op != "release" {
			t.Fatalf("source call %+v happened before the first transition", call)
		}
	}
	if calls[first].State != source.StateInProgress {
		t.Fatalf("first transition = %+v, want the in-progress transition", calls[first])
	}
	reports := f.src.Reports()
	if len(reports) != 1 {
		t.Fatalf("source reports = %d, want exactly one", len(reports))
	}
	if reports[0].State != source.StateInReview || reports[0].Result != source.ResultReady {
		t.Fatalf("lifecycle report = %+v, want the in-review ready result", reports[0])
	}
	if reports[0].PR != fmt.Sprint(pr.Number) {
		t.Fatalf("lifecycle report PR = %q, want the published pull request %d", reports[0].PR, pr.Number)
	}
	if run.Status.SourceReceipt != f.src.receipt {
		t.Fatalf("run source receipt = %q, want the receipt the claim issued", run.Status.SourceReceipt)
	}

	// The run ended truthfully: it is awaiting review on a published pull
	// request, it recorded the terminal transition, and it records no cause
	// it did not observe.
	if run.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview", run.Status.Phase)
	}
	if run.Status.FinishedAt == nil {
		t.Fatal("the run recorded no end: the operator's terminal transition never ran")
	}
	if run.Status.TerminalDiagnostic != nil {
		t.Fatalf("terminal diagnostic = %+v, want none: a published ending observed no failure", run.Status.TerminalDiagnostic)
	}

	// The model and the worker were bounded: the fixture asked for three
	// requests and one task, and got exactly that.
	requests := f.model.RequestLog()
	if f.model.Requests() != 3 {
		t.Fatalf("model requests = %d, want the capability probe and the two turns", f.model.Requests())
	}
	if len(requests) != 3 {
		t.Fatalf("model request log = %d entries, want the capability probe and the two turns", len(requests))
	}
	if requests[0].Model != requests[1].Model || requests[1].Model != requests[2].Model {
		t.Fatalf("the model requests named different models: %v", requests)
	}
	if !slices.Contains(requests[1].Tools, "shell") {
		t.Fatalf("the coordinator's turn offered the tools %v, want the production shell tool", requests[1].Tools)
	}
	if got := len(f.worker.Dispatches()); got != 2 {
		t.Fatalf("worker dispatches = %d, want the snapshot materialization and the shell task", got)
	}
	if got := f.worker.Snapshots(); got != 1 {
		t.Fatalf("worker snapshots = %d, want one", got)
	}
}

// offeredTools returns the tool names the coordinator offered the model in
// its turn: the production tool surface a turn could have called.
func (f *fixtureA) offeredTools() []string {
	requests := f.model.RequestLog()
	if len(requests) < 2 {
		return nil
	}
	return requests[1].Tools
}

// nativeHandoff renders the termination handoff the control binary writes
// when it ends: the phase and result pair the operator's validation
// accepts, plus the trusted native result kind.
func nativeHandoff(kind string) string {
	return "COURIER_TERMINATION " + fmt.Sprintf(
		`{"phase":"Verifying","result":"success","exit_code":0,"reason":"published through the broker","outcome":"changes","result_kind":%q}`,
		kind)
}

// request is the reconcile request for the fixture's run.
func (f *fixtureA) request() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: f.run.Namespace, Name: f.run.Name}}
}

// fetch reads the run from the API server.
func (f *fixtureA) fetch(t *testing.T) *courierv1alpha1.CoderRun {
	t.Helper()
	run := &courierv1alpha1.CoderRun{}
	if err := envtestClient.Get(context.Background(), client.ObjectKeyFromObject(f.run), run); err != nil {
		t.Fatalf("read run: %v", err)
	}
	return run
}

// pod reads one provisioned topology pod of this run.
//
// The lookup is bound to the run's own label: several fixtures share the run
// namespace, so a component label alone could hand this fixture another
// run's pod and make its topology claim vacuous.
func (f *fixtureA) pod(t *testing.T, component string) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := envtestClient.List(context.Background(), pods, client.InNamespace(f.namespace)); err != nil {
		t.Fatal(err)
	}
	run := topology.RunLabelValue(f.runName)
	for i := range pods.Items {
		if pods.Items[i].Labels[topology.LabelRun] != run {
			continue
		}
		if pods.Items[i].Labels["courier.misospace.dev/component"] == component {
			return &pods.Items[i]
		}
	}
	t.Fatalf("the secure topology of run %q has no %s pod", f.runName, component)
	return nil
}
