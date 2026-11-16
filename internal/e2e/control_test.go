package e2e

// This file holds the production control-side composition every fixture
// drives: the model gateway over the local scripted model, the lane's role
// bindings, the broker's capability report, the authenticated trusted status
// path, the signed worker delegation, and the coordinator that ties them
// together.
//
// Every seam here is a production package. The fixtures differ only in what
// they inject and what they assert, so the composition is written once and
// the three fixtures cannot drift apart.

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/broker"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/forge"
	couriergit "github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

// fixtureBrokerToken is the bearer token the fixture's control side presents
// to the broker's trusted status path.
const fixtureBrokerToken = "fixture-broker-token"

// fixtureBrokerTokenTwo is the bearer token a replacement control incarnation
// presents. A credential is bound to the pod that holds it, so the two
// incarnations never share one.
const fixtureBrokerTokenTwo = "fixture-broker-token-two"

// newGateway builds the production harness gateway over the local scripted
// model.
func (f *fixtureA) newGateway() *harness.Gateway {
	return &harness.Gateway{BaseURL: f.model.BaseURL(), HTTP: f.model.server.Client()}
}

// newBindings resolves the production role bindings of the fixture's lane.
func (f *fixtureA) newBindings(t *testing.T) harness.Bindings {
	t.Helper()
	bindings, err := harness.BindRoles(map[string]string{harness.RoleCoordinator: "test/coordinator"})
	if err != nil {
		t.Fatal(err)
	}
	return bindings
}

// buildPublication binds the production broker policy to the live run and
// builds the trusted publication seam over the local-git forge double: the
// production forge adapter, the production git pusher, and the production
// policy engine whose every publication decision — the pinned identity, the
// fast-forward check, the duplicate-pull-request guard — is the production
// one.
func (f *fixtureA) buildPublication(t *testing.T, ctx context.Context) {
	t.Helper()
	policy, err := broker.PolicyFromRun(f.fetch(t), fixtureProviderName, fixtureOwner+"/"+fixtureRepo)
	if err != nil {
		t.Fatalf("bind broker policy: %v", err)
	}
	f.policy = policy
	adapter, err := broker.NewForgeAdapter(policy, f.provider, f.provider,
		forge.Repository{ID: fixtureRepoID, Canonical: policy.BaseRepo, DefaultRef: fixtureDefault},
		forge.Repository{ID: fixtureRepoID, Canonical: policy.WorkRepo, DefaultRef: fixtureDefault})
	if err != nil {
		t.Fatalf("forge adapter: %v", err)
	}
	f.adapter = adapter
	gitBroker, err := couriergit.NewBroker(ctx, filepath.Join(f.git.Root, "broker"))
	if err != nil {
		t.Fatalf("git broker: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(gitBroker.Directory()) })
	f.gitBroker = gitBroker
	pusher, err := broker.NewGitPusher(ctx, broker.GitTransportConfig{Policy: policy, Git: gitBroker, Resolver: f.provider})
	if err != nil {
		t.Fatalf("git pusher: %v", err)
	}
	engine, err := broker.NewPolicyEngine(policy, adapter, pusher)
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	f.engine = engine
}

// importProposal imports the proposal into the broker's own private object
// store, the way the control side does before it publishes: the broker
// verifies the bundle carries the proposed commit and that it is a
// fast-forward of the expected tip.
func (f *fixtureA) importProposal(t *testing.T, ctx context.Context, proposal, expected string) {
	t.Helper()
	if err := f.gitBroker.ImportBundle(ctx, f.bundlePath, proposal, expected); err != nil {
		t.Fatalf("import proposal %s: %v", proposal, err)
	}
}

// buildAuthenticator builds the production broker authenticator over the
// fixture's TokenReview backend: the credential is pod-bound, and the
// authenticator's live reads decide whether the incarnation is the current
// one.
func (f *fixtureA) buildAuthenticator(t *testing.T, ctx context.Context, reviews *tokenReviews) *broker.Authenticator {
	t.Helper()
	authenticator, err := broker.NewAuthenticator(ctx, envtestClient, reviews.Clientset(),
		f.namespace, f.brokerPod.Name, topology.ControlSAName(f.run.Name), f.controlSAUID)
	if err != nil {
		t.Fatalf("broker authenticator: %v", err)
	}
	return authenticator
}

// buildStatusPath builds the production trusted status path: the broker's
// status writer, the live-world validator that re-checks a lastCommit against
// the live work ref, the production trusted status handler, and the recording
// transport the fixture observes. The token is the control incarnation's own
// credential, so a stale incarnation's write is refused by the production
// fence rather than by the fixture.
func (f *fixtureA) buildStatusPath(t *testing.T, ctx context.Context, authenticator broker.RequestAuthenticator, controlPod *corev1.Pod, token string) (*recordingStatusTransport, *harness.StatusReporter) {
	t.Helper()
	writer, err := broker.NewStatusWriter(envtestClient, envtestClient, f.namespace, f.run.Name)
	if err != nil {
		t.Fatalf("broker status writer: %v", err)
	}
	// The live-world validator is the production one: a lastCommit is
	// accepted only while the live work ref carries exactly that OID.
	validator := func(ctx context.Context, _ *courierv1alpha1.CoderRun, _ *corev1.Pod, patch broker.HarnessPatch) error {
		if patch.LastCommit == nil || *patch.LastCommit == "" {
			return nil
		}
		state, err := f.adapter.Repository(ctx, f.policy.WorkRepo, f.policy.WorkRef)
		if err != nil {
			return &broker.StatusUnavailableError{Err: fmt.Errorf("live work ref read failed: %w", err)}
		}
		if !state.Exists || state.OID != *patch.LastCommit {
			return errors.New("lastCommit does not match the live work ref tip")
		}
		return nil
	}
	handler, err := broker.NewTrustedStatusHandler(authenticator, writer, validator)
	if err != nil {
		t.Fatalf("trusted status handler: %v", err)
	}
	statusServer := httptest.NewServer(handler)
	t.Cleanup(statusServer.Close)

	client := &harness.BrokerClient{
		BaseURL:     statusServer.URL,
		TokenSource: harness.NewStaticBrokerTokenSource([]byte(token)),
		HTTP:        statusServer.Client(),
	}
	transport := newRecordingStatusTransport(client, f.log)
	reporter := harness.NewStatusReporter(transport, string(controlPod.UID), string(f.workerPod.UID), 0)
	return transport, reporter
}

// newProbe builds the production capability probe over the fixture's broker
// server: the capability report the broker itself serves. The token is the
// control incarnation's own credential, so the probe authenticates as the
// incarnation it belongs to.
func (f *fixtureA) newProbe(t *testing.T, ctx context.Context, authenticator broker.RequestAuthenticator, token string) *harness.BrokerProbeClient {
	t.Helper()
	brokerServer := f.startBrokerServer(t, authenticator, f.gitBroker)
	probe := &harness.BrokerProbeClient{
		BaseURL:     brokerServer.URL,
		TokenSource: harness.NewStaticBrokerTokenSource([]byte(token)),
		HTTP:        brokerServer.Client(),
	}
	return probe
}

// capabilityGate runs the production probe and the production gate: the run
// proceeds only when the broker's own capability report and the model
// transport both answer.
func (f *fixtureA) capabilityGate(t *testing.T, ctx context.Context, gateway *harness.Gateway, probe *harness.BrokerProbeClient) {
	t.Helper()
	bindings := f.newBindings(t)
	capabilities := harness.ProbeCapabilities(ctx, harness.ProbeDeps{
		Transport:          gateway,
		Bindings:           bindings,
		Broker:             probe,
		Mode:               string(courierv1alpha1.ModeResolveIssue),
		PublicationPresent: true,
		SnapshotPresent:    true,
		ModelProbe: func(ctx context.Context, model string) ([]harness.Event, error) {
			return gateway.StreamChat(ctx, harness.ChatRequest{
				Model:    model,
				Messages: []harness.Message{{Role: "user", Content: "capability probe"}},
			})
		},
	})
	if _, err := harness.Gate(capabilities); err != nil {
		t.Fatalf("capability gate refused the run: %v (%+v)", err, capabilities)
	}
}

// newDelegator builds the production delegation seam over the signed worker
// protocol, bound to the control incarnation that owns it.
func (f *fixtureA) newDelegator(t *testing.T, controlPod *corev1.Pod, key ed25519.PrivateKey) *harness.Delegator {
	t.Helper()
	delegator, err := harness.NewDelegator(&protocol.Client{BaseURL: f.workerURL}, harness.WorkerIdentity{
		RunUID:        f.runUID,
		ControlPodUID: string(controlPod.UID),
		WorkerPodUID:  string(f.workerPod.UID),
		Key:           key,
	})
	if err != nil {
		t.Fatalf("delegator: %v", err)
	}
	return delegator
}

// newCoordinator builds the production harness coordinator over the
// production gateway, the production delegation seam, and the trusted
// publication seam.
func (f *fixtureA) newCoordinator(t *testing.T, gateway *harness.Gateway, delegator *harness.Delegator, publisher harness.Publisher, reporter *harness.StatusReporter) *harness.Coordinator {
	t.Helper()
	coordinator, err := harness.NewCoordinator(harness.CoordinatorConfig{
		Transport: gateway,
		Bindings:  f.newBindings(t),
		Worker:    delegator,
		Snapshot: func(context.Context) (harness.Snapshot, error) {
			return harness.Snapshot{
				Data:    []byte("e2e-snapshot"),
				Tip:     f.git.BaseOID,
				BaseTip: f.git.BaseOID,
			}, nil
		},
		Publisher: publisher,
		Activity:  reporter,
		Status:    reporter,
	})
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	return coordinator
}

// invocation builds the production invocation for the live run.
func (f *fixtureA) invocation(t *testing.T) executor.Invocation {
	t.Helper()
	lane := &courierv1alpha1.LaneProfile{}
	if err := envtestClient.Get(context.Background(), types.NamespacedName{Namespace: f.namespace, Name: f.lane.Name}, lane); err != nil {
		t.Fatal(err)
	}
	invocation, err := executor.NewInvocationWithReviewHandoff(f.fetch(t), lane, topology.ControlWorkspacePath, executor.ContributionExecution{}, "")
	if err != nil {
		t.Fatalf("invocation: %v", err)
	}
	return invocation
}

// publisherFor builds the trusted publication seam the coordinator drives.
// The proposal is the commit this incarnation publishes; expected is the work
// tip it expects to find, exactly as the production control side derives it
// from the live world. The pull-request title and body are not part of the
// seam: the coordinator's finish passes the model's declared pair to
// OpenPullRequest (#844).
//
// The seam reaches the broker over its own HTTP routes, with the
// incarnation's own credential, so the route's answers are the evidence.
func (f *fixtureA) publisherFor(t *testing.T, proposal, expected, token string) *fixturePublisher {
	t.Helper()
	return &fixturePublisher{
		broker:   f.brokerClient(t, token),
		proposal: proposal,
		expected: expected,
	}
}

// brokerClient builds the production control-side client for the fixture's
// broker server: the typed API the real control side calls, with the
// incarnation's own bearer token.
func (f *fixtureA) brokerClient(t *testing.T, token string) *harness.BrokerClient {
	t.Helper()
	if f.brokerServer == nil {
		t.Fatal("the publication seam needs the fixture's broker server")
	}
	return &harness.BrokerClient{
		BaseURL:     f.brokerServer.URL,
		TokenSource: harness.NewStaticBrokerTokenSource([]byte(token)),
		HTTP:        f.brokerServer.Client(),
	}
}

// terminateControlPod marks the control pod's container terminated the way
// the kubelet would, with the termination handoff the control binary writes.
func (f *fixtureA) terminateControlPod(t *testing.T, ctx context.Context, pod *corev1.Pod, handoff string, exitCode int32) {
	t.Helper()
	current := &corev1.Pod{}
	if err := envtestClient.Get(ctx, types.NamespacedName{Namespace: f.namespace, Name: pod.Name}, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = corev1.PodFailed
	current.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: topology.ControlContainerName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: exitCode,
			Message:  handoff,
		}},
	}}
	if err := envtestClient.Status().Update(ctx, current); err != nil {
		t.Fatalf("terminate the control pod: %v", err)
	}
}

// waitForPhase drives the operator until the run reaches the wanted phase.
func (f *fixtureA) waitForPhase(t *testing.T, ctx context.Context, phase courierv1alpha1.Phase) {
	t.Helper()
	for attempt := 0; attempt < 8; attempt++ {
		if _, err := f.reconciler.Reconcile(ctx, f.request()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		run := f.fetch(t)
		// The merge path is closed, so no settle path may reach Done: the
		// claim is checked at every step of the path, not only at its end.
		f.assertNeverDone(t, run)
		if run.Status.Phase == phase {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the run never reached phase %q (it is %q)", phase, f.fetch(t).Status.Phase)
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("phase = %q, want %q", f.fetch(t).Status.Phase, phase)
}
