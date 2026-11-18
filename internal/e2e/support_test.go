package e2e

// This file holds the fixture's local doubles. Every one of them implements a
// production interface and substitutes only what a test cannot reach from
// outside a cluster:
//
//   - recordingSource implements source.Adapter, source.ClaimerWithReceipt,
//     and source.Reporter: the source's own lifecycle API, recorded so the
//     fixture can assert the claim -> transition -> report order.
//   - recordingStatusTransport implements harness.StatusTransport and
//     delegates to the authenticated broker path, so the fixture sees every
//     patch the harness asked for while the write itself is the production
//     one.
//   - workerDouble implements the signed worker protocol's HTTP surface: the
//     transport control talks to an untrusted worker over.
//   - tokenReviews implements the Kubernetes TokenReview backend: the token
//     minting a real cluster performs for the control pod's projected
//     credential.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/source"
)

// eventLog is one ordered record of what the fixture's doubles observed. It
// gives the fixture a single timeline across the model gateway and the
// status transport, so an ordering claim (no heartbeat before the model
// produced anything) is checkable rather than assumed.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

// Append records one event.
func (l *eventLog) Append(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

// Events returns the recorded events in order.
func (l *eventLog) Events() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	copy(out, l.events)
	return out
}

// IndexOf returns the position of the first event with the given name, or -1.
func (l *eventLog) IndexOf(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, event := range l.events {
		if event == name {
			return i
		}
	}
	return -1
}

// sourceCall is one recorded call into the source adapter.
type sourceCall struct {
	// Op names the adapter method the operator called.
	Op string
	// ID is the work item the call named.
	ID string
	// State is the lifecycle state a transition named.
	State source.State
}

// recordingSource is the fixture's source adapter. It records every call the
// operator makes and issues a receipt the operator must persist and carry
// into settlement.
type recordingSource struct {
	mu sync.Mutex

	// receipt is the fencing token the claim issues.
	receipt string
	// calls records the adapter calls in order.
	calls []sourceCall
	// reports records the lifecycle reports in order.
	reports []source.Lifecycle
}

var (
	_ source.Adapter            = (*recordingSource)(nil)
	_ source.ClaimerWithReceipt = (*recordingSource)(nil)
	_ source.Reporter           = (*recordingSource)(nil)
)

// newRecordingSource builds the recording adapter with the receipt it
// issues.
func newRecordingSource(receipt string) *recordingSource {
	return &recordingSource{receipt: receipt}
}

// Discover returns no work: the fixture materializes the run itself.
func (s *recordingSource) Discover(context.Context) ([]source.WorkItem, error) {
	return nil, nil
}

// Claim records the claim.
func (s *recordingSource) Claim(_ context.Context, item source.WorkItem) error {
	s.record(sourceCall{Op: "claim", ID: item.ID})
	return nil
}

// ClaimWithReceipt records the claim and issues the receipt the operator
// persists as the run's source receipt.
func (s *recordingSource) ClaimWithReceipt(_ context.Context, item source.WorkItem) (source.WorkItem, error) {
	s.record(sourceCall{Op: "claim-with-receipt", ID: item.ID})
	item.Context = s.receipt
	return item, nil
}

// Release records the release.
func (s *recordingSource) Release(_ context.Context, item source.WorkItem) error {
	s.record(sourceCall{Op: "release", ID: item.ID})
	return nil
}

// Transition records the lifecycle transition.
func (s *recordingSource) Transition(_ context.Context, item source.WorkItem, state source.State) error {
	s.record(sourceCall{Op: "transition", ID: item.ID, State: state})
	return nil
}

// Report records the lifecycle report.
func (s *recordingSource) Report(_ context.Context, item source.WorkItem, lifecycle source.Lifecycle) error {
	s.record(sourceCall{Op: "report", ID: item.ID})
	s.mu.Lock()
	s.reports = append(s.reports, lifecycle)
	s.mu.Unlock()
	return nil
}

// Resolve records the resolve.
func (s *recordingSource) Resolve(_ context.Context, item source.WorkItem) error {
	s.record(sourceCall{Op: "resolve", ID: item.ID})
	return nil
}

// Calls returns the recorded adapter calls in order.
func (s *recordingSource) Calls() []sourceCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sourceCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// CallNames returns the recorded adapter calls as their operation names.
func (s *recordingSource) CallNames() []string {
	calls := s.Calls()
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.Op)
	}
	return out
}

// Reports returns the recorded lifecycle reports in order.
func (s *recordingSource) Reports() []source.Lifecycle {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]source.Lifecycle, len(s.reports))
	copy(out, s.reports)
	return out
}

// record appends one recorded call.
func (s *recordingSource) record(call sourceCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

// recordingStatusTransport is the harness's trusted status path. It records
// every patch the harness asked for and forwards it to the authenticated
// transport — the production broker client — so the write that lands on the
// CoderRun is the broker's own fenced write.
type recordingStatusTransport struct {
	// inner is the authenticated transport. Nil records without writing.
	inner harness.StatusTransport
	// log is the fixture's shared timeline.
	log *eventLog

	mu      sync.Mutex
	patches []harness.StatusPatch
}

var _ harness.StatusTransport = (*recordingStatusTransport)(nil)

// newRecordingStatusTransport builds the recording transport over an
// authenticated one.
func newRecordingStatusTransport(inner harness.StatusTransport, log *eventLog) *recordingStatusTransport {
	return &recordingStatusTransport{inner: inner, log: log}
}

// PostStatus records the patch and forwards it.
func (t *recordingStatusTransport) PostStatus(ctx context.Context, patch harness.StatusPatch) error {
	t.mu.Lock()
	t.patches = append(t.patches, patch)
	t.mu.Unlock()
	if t.log != nil {
		switch {
		case patch.Heartbeat != nil:
			t.log.Append("status-heartbeat:" + patch.Heartbeat.Kind)
		default:
			t.log.Append("status-patch")
		}
	}
	if t.inner == nil {
		return nil
	}
	return t.inner.PostStatus(ctx, patch)
}

// Patches returns the recorded patches in order.
func (t *recordingStatusTransport) Patches() []harness.StatusPatch {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]harness.StatusPatch, len(t.patches))
	copy(out, t.patches)
	return out
}

// Heartbeats returns the recorded heartbeat patches in order.
func (t *recordingStatusTransport) Heartbeats() []harness.StatusPatch {
	out := []harness.StatusPatch{}
	for _, patch := range t.Patches() {
		if patch.Heartbeat != nil {
			out = append(out, patch)
		}
	}
	return out
}

// workerSignedRequest mirrors the signed worker protocol's request shape.
type workerSignedRequest struct {
	Envelope  protocol.Envelope `json:"envelope"`
	Signature []byte            `json:"signature"`
	Payload   []byte            `json:"payload,omitempty"`
}

// workerDouble is the untrusted worker's side of the signed protocol. It
// verifies every envelope's signature with the control incarnation's public
// key, answers every dispatch with a completed result, and records what it
// was asked to do. It executes nothing: the fixture's work is not the point,
// the protocol's shape is.
type workerDouble struct {
	mu sync.Mutex

	// key is the control incarnation's public key.
	key ed25519.PublicKey
	// tip is the snapshot tip the workspace materialization echoes.
	tip string
	// log is the fixture's shared timeline.
	log *eventLog
	// results holds the terminal state of every dispatched operation.
	results map[string]protocol.ResultState
	// dispatches records the operation IDs it was asked to run, in order.
	dispatches []string
	// snapshots counts the uploaded workspace snapshots.
	snapshots int
}

func newWorkerDouble(key ed25519.PublicKey, tip string, log *eventLog) *workerDouble {
	return &workerDouble{key: key, tip: tip, log: log, results: map[string]protocol.ResultState{}}
}

// ServeHTTP answers the worker protocol's routes.
func (w *workerDouble) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks":
		w.handleDispatch(rw, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/snapshot":
		w.mu.Lock()
		w.snapshots++
		w.mu.Unlock()
		rw.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/tasks/") && strings.HasSuffix(r.URL.Path, "/release"):
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprint(rw, `{"status":"released"}`)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/") && strings.HasSuffix(r.URL.Path, "/result"):
		w.handleResult(rw, r)
	default:
		http.NotFound(rw, r)
	}
}

// handleDispatch verifies the signed envelope and records the operation.
func (w *workerDouble) handleDispatch(rw http.ResponseWriter, r *http.Request) {
	var request workerSignedRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(rw, "unreadable request", http.StatusBadRequest)
		return
	}
	if err := request.Envelope.Verify(w.key, request.Signature); err != nil {
		http.Error(rw, "invalid signature", http.StatusUnauthorized)
		return
	}
	if err := request.Envelope.Validate(time.Now()); err != nil {
		http.Error(rw, "invalid envelope", http.StatusUnauthorized)
		return
	}
	stdout := ""
	if strings.HasPrefix(request.Envelope.OpID, "snapshot.") {
		// The workspace materialization echoes the tip it materialized at;
		// trusted control compares it with the dispatched tip.
		stdout = w.tip
	}
	w.mu.Lock()
	w.dispatches = append(w.dispatches, request.Envelope.OpID)
	w.results[request.Envelope.OpID] = protocol.ResultState{
		Status: protocol.ResultCompleted,
		Result: &protocol.Result{Status: protocol.ResultCompleted, ExitCode: 0, StdoutTail: stdout},
	}
	w.mu.Unlock()
	if w.log != nil {
		// The double executes nothing, so the task it accepted is complete
		// the moment it is dispatched. The event therefore marks a verified
		// completion, not the receipt of the dispatch.
		w.log.Append("worker-task-completed")
	}
	rw.Header().Set("Content-Type", "application/json")
	fmt.Fprint(rw, `{"status":"running"}`)
}

// handleResult answers a result poll with the operation's terminal state.
func (w *workerDouble) handleResult(rw http.ResponseWriter, r *http.Request) {
	opID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/result")
	w.mu.Lock()
	state, ok := w.results[opID]
	w.mu.Unlock()
	if !ok {
		http.NotFound(rw, r)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(rw).Encode(state); err != nil {
		http.Error(rw, "encode failed", http.StatusInternalServerError)
	}
}

// Dispatches returns the operation IDs the worker was asked to run.
func (w *workerDouble) Dispatches() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, len(w.dispatches))
	copy(out, w.dispatches)
	return out
}

// setKey replaces the control incarnation's public key: a replacement
// control incarnation signs with its own key, exactly as the operator's
// relaunch gives the new pod a fresh signing secret.
func (w *workerDouble) setKey(key ed25519.PublicKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.key = key
}

// Snapshots reports how many workspace snapshots the worker received.
func (w *workerDouble) Snapshots() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshots
}

// tokenReviews is the Kubernetes TokenReview backend: the token minting a
// real cluster performs for the control pod's projected broker credential.
// It answers every review with the authenticated control identity, so the
// production authenticator's live checks — audience, service account, pod
// binding — are the ones that decide.
//
// A credential is bound to the pod that holds it, so the backend answers per
// token: a registered token authenticates as the incarnation it was minted
// for, and an unregistered token falls back to the default identity. That is
// what makes a stale incarnation's write observable — its credential names a
// pod that no longer exists, exactly as a rotated-out projected token does.
type tokenReviews struct {
	mu sync.Mutex

	// username is the authenticated subject.
	username string
	// uid is the authenticated subject's UID.
	uid string
	// podName and podUID bind the credential to one control pod.
	podName string
	podUID  string
	// byToken maps a bearer token to the identity it authenticates.
	byToken map[string]tokenIdentity
}

// tokenIdentity is one authenticated subject: the service account, and the
// control pod the credential is bound to.
type tokenIdentity struct {
	username string
	uid      string
	podName  string
	podUID   string
}

// newTokenReviews builds the TokenReview backend.
func newTokenReviews(username, uid, podName, podUID string) *tokenReviews {
	return &tokenReviews{username: username, uid: uid, podName: podName, podUID: podUID, byToken: map[string]tokenIdentity{}}
}

// register binds a bearer token to the identity it authenticates: the control
// pod that holds the credential.
func (r *tokenReviews) register(token, username, uid, podName, podUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byToken == nil {
		r.byToken = map[string]tokenIdentity{}
	}
	r.byToken[token] = tokenIdentity{username: username, uid: uid, podName: podName, podUID: podUID}
}

// identityFor resolves the identity one reviewed token authenticates as.
func (r *tokenReviews) identityFor(token string) tokenIdentity {
	r.mu.Lock()
	defer r.mu.Unlock()
	if identity, ok := r.byToken[token]; ok {
		return identity
	}
	return tokenIdentity{username: r.username, uid: r.uid, podName: r.podName, podUID: r.podUID}
}

// Clientset returns a clientset that answers TokenReview creations with the
// configured identity.
func (r *tokenReviews) Clientset() *k8sfake.Clientset {
	clientset := k8sfake.NewSimpleClientset()
	clientset.Fake.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		identity := r.identityFor(reviewedToken(action))
		return true, &authenticationv1.TokenReview{
			Status: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				Audiences:     []string{"courier-broker"},
				User: authenticationv1.UserInfo{
					Username: identity.username,
					UID:      identity.uid,
					Extra: map[string]authenticationv1.ExtraValue{
						"authentication.kubernetes.io/pod-name": {identity.podName},
						"authentication.kubernetes.io/pod-uid":  {identity.podUID},
					},
				},
			},
		}, nil
	})
	return clientset
}

// reviewedToken extracts the credential one TokenReview creation carries.
func reviewedToken(action k8stesting.Action) string {
	create, ok := action.(k8stesting.CreateAction)
	if !ok {
		return ""
	}
	review, ok := create.GetObject().(*authenticationv1.TokenReview)
	if !ok {
		return ""
	}
	return review.Spec.Token
}

// isMergeShaped reports whether a tool name asks for a merge, or for the
// publication the coordinator must never hand to the model. The coordinator's
// tool surface is production code, so this is where the "no agent self-merge
// path" claim has evidence.
func isMergeShaped(tool string) bool {
	lowered := strings.ToLower(tool)
	for _, verb := range []string{"merge", "publish", "close-pull-request", "close_pull_request"} {
		if strings.Contains(lowered, verb) {
			return true
		}
	}
	return false
}

// terminalPhase reports whether a phase ends the run. An admission that
// reaches one of them was refused, not delayed, so the fixture reports it
// immediately instead of waiting out its deadline.
func terminalPhase(phase courierv1alpha1.Phase) bool {
	switch phase {
	case courierv1alpha1.PhaseNeedsHuman,
		courierv1alpha1.PhaseDone,
		courierv1alpha1.PhaseAlreadyDone,
		courierv1alpha1.PhaseFailed,
		courierv1alpha1.PhaseBlockedExternal,
		courierv1alpha1.PhaseHandedOff:
		return true
	}
	return false
}

// terminalReason renders what the run recorded about its own ending, so a
// refused admission reports the cause the operator observed.
func terminalReason(run *courierv1alpha1.CoderRun) string {
	if run.Status.TerminalDiagnostic != nil {
		return fmt.Sprintf("%s: %s", run.Status.TerminalDiagnostic.Category, run.Status.TerminalDiagnostic.Reason)
	}
	if run.Status.NativeResult != nil {
		return fmt.Sprintf("%s: %s", run.Status.NativeResult.Kind, run.Status.NativeResult.Reason)
	}
	for _, condition := range run.Status.Conditions {
		if condition.Status == metav1.ConditionFalse || condition.Status == metav1.ConditionUnknown {
			return fmt.Sprintf("%s=%s: %s", condition.Type, condition.Status, condition.Message)
		}
	}
	return "the run recorded no reason"
}
