package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

// pullRequestDeclaration is the declared changes title/body a resolve-issue
// run must carry for the finish step (#844).
var pullRequestDeclaration = &declaredPullRequest{
	title: "Resolve issue #7",
	body:  "Closes #7. Ships the fix.",
}

// relaunchPublisher stands in for the broker's idempotent create route: the
// same pinned head always resolves to the same pull request, so a relaunch
// adopts rather than duplicates (#844).
type relaunchPublisher struct {
	mockPublisher
	mu      sync.Mutex
	started bool
}

func (p *relaunchPublisher) OpenPullRequest(_ context.Context, title, body string, draft bool) (PullRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pullRequests = append(p.pullRequests, pullRequestCall{title: title, body: body, draft: draft})
	return PullRequest{Number: 900, State: "open"}, nil
}

// coveragePublisher stands in for the broker create route returning derived
// criterion coverage alongside the created pull request (#844).
type coveragePublisher struct {
	mockPublisher
}

func (p *coveragePublisher) OpenPullRequest(_ context.Context, title, body string, draft bool) (PullRequest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pullRequests = append(p.pullRequests, pullRequestCall{title: title, body: body, draft: draft})
	return PullRequest{Number: 901, State: "open", Coverage: "set=sha256:" + strings.Repeat("a", 64)}, nil
}

func coordinatorWithFinishSeams(t *testing.T, publisher Publisher, transport StatusTransport) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://gateway.invalid"},
		Bindings:  testBindings(),
		Publisher: publisher,
		Status:    NewStatusReporter(transport, testControlUID, testWorkerUID, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

// TestFinishOpensTheDeclaredPullRequestOnceAndRecordsItsIdentity is the
// done-condition core of #844: a declared changes ending publishes the branch,
// opens exactly one pull request through the broker path with the model's
// declared title and body as a non-draft, and records the created number on
// the status path before claiming published.
func TestFinishOpensTheDeclaredPullRequestOnceAndRecordsItsIdentity(t *testing.T) {
	publisher := &mockPublisher{}
	transport := &recordingStatusTransport{}
	coordinator := coordinatorWithFinishSeams(t, publisher, transport)

	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultPublished || result.Outcome != executor.OutcomeChanges {
		t.Fatalf("result = %+v, want a published changes ending", result)
	}
	if publisher.count() != 1 {
		t.Fatalf("publications = %d, want the branch published once before the pull request", publisher.count())
	}
	if publisher.opens() != 1 {
		t.Fatalf("pull-request opens = %d, want exactly one", publisher.opens())
	}
	call := publisher.pullRequests[0]
	if call.title != pullRequestDeclaration.title || call.body != pullRequestDeclaration.body {
		t.Fatalf("open call = %+v, want the declared title and body", call)
	}
	if call.draft {
		t.Fatal("the pull request was opened as a draft; trusted control owns draft-ness")
	}
	if !strings.Contains(result.Reason, "#901") {
		t.Fatalf("reason = %q, want the created pull request number named", result.Reason)
	}
	var recorded *PullRequestRecord
	for _, patch := range transport.sent() {
		if patch.PullRequest != nil {
			recorded = patch.PullRequest
		}
	}
	if recorded == nil || recorded.Number != 901 {
		t.Fatalf("status patches = %+v, want the created number recorded through the status path", transport.sent())
	}
}

// TestFinishCoverageRidesTheReason proves the derived criterion coverage the
// broker returns is surfaced in the terminal reason, not swallowed (#844).
func TestFinishCoverageRidesTheReason(t *testing.T) {
	transport := &recordingStatusTransport{}
	coordinator := coordinatorWithFinishSeams(t, &coveragePublisher{}, transport)

	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultPublished {
		t.Fatalf("result = %+v, want a published ending", result)
	}
	if !strings.Contains(result.Reason, "set=sha256:"+strings.Repeat("a", 64)) {
		t.Fatalf("reason = %q, want the derived coverage named", result.Reason)
	}
}

// TestFinishAdoptsTheSamePullRequestIdentityOnRelaunch proves the crash-and-
// relaunch path: the broker's idempotent create route resolves the same pull
// request for the same pinned head, and re-recording the identical identity
// through the status path is the accepted idempotent rewrite — one pull
// request across the relaunch, no second open at a different number.
func TestFinishAdoptsTheSamePullRequestIdentityOnRelaunch(t *testing.T) {
	publisher := &relaunchPublisher{}
	transport := &recordingStatusTransport{}
	coordinator := coordinatorWithFinishSeams(t, publisher, transport)

	inv := testInvocation()
	for attempt := 0; attempt < 2; attempt++ {
		result := coordinator.finish(context.Background(), inv, executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
		if result.Kind != courierv1alpha1.NativeResultPublished {
			t.Fatalf("attempt %d result = %+v, want a published ending", attempt, result)
		}
		if !strings.Contains(result.Reason, "#900") {
			t.Fatalf("attempt %d reason = %q, want the adopted identity named", attempt, result.Reason)
		}
	}
	if publisher.opens() != 2 {
		t.Fatalf("pull-request opens = %d, want the create route called once per incarnation", publisher.opens())
	}
	numbers := map[int]bool{}
	for _, patch := range transport.sent() {
		if patch.PullRequest != nil {
			numbers[patch.PullRequest.Number] = true
		}
	}
	if len(numbers) != 1 || !numbers[900] {
		t.Fatalf("recorded identities = %v, want only the adopted pull request #900", numbers)
	}
}

// TestFinishIsBlockedExternalWhenThePullRequestCreateIsRefused proves the
// blocked-classification contract: when the trusted world refuses the create
// — the body contract, a foreign tip, a moved base — nothing is claimed
// published, the pull request is never recorded, and the ending is
// blocked_external (#844).
func TestFinishIsBlockedExternalWhenThePullRequestCreateIsRefused(t *testing.T) {
	publisher := &mockPublisher{}
	publisher.pullRequestErr = &PublicationBlockedError{Reason: "the pull-request body contract refused the declared body"}
	transport := &recordingStatusTransport{}
	coordinator := coordinatorWithFinishSeams(t, publisher, transport)

	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultBlockedExternal || result.Outcome != executor.OutcomeBlockedExternal {
		t.Fatalf("result = %+v, want a blocked_external ending", result)
	}
	if !strings.Contains(result.Reason, "body contract") {
		t.Fatalf("reason = %q, want the broker's refusal stated", result.Reason)
	}
	for _, patch := range transport.sent() {
		if patch.PullRequest != nil {
			t.Fatalf("status patches = %+v, want nothing recorded for a refused create", transport.sent())
		}
	}
}

// TestFinishOpensNoPullRequestForRunsWithoutThatPath proves the mode gate:
// fix-pr and rebase runs take over pull requests that already exist, so their
// declared changes ending never calls the create seam even when the publisher
// could open one (#844).
func TestFinishOpensNoPullRequestForRunsWithoutThatPath(t *testing.T) {
	for _, mode := range []courierv1alpha1.Mode{courierv1alpha1.ModeFixPR, courierv1alpha1.ModeRebase} {
		publisher := &mockPublisher{}
		transport := &recordingStatusTransport{}
		coordinator := coordinatorWithFinishSeams(t, publisher, transport)
		inv := testInvocation()
		inv.Mode = mode

		result := coordinator.finish(context.Background(), inv, executor.OutcomeChanges, "done", nil, nil)
		if result.Kind != courierv1alpha1.NativeResultPublished {
			t.Fatalf("mode %s: result = %+v, want a published ending", mode, result)
		}
		if publisher.opens() != 0 {
			t.Fatalf("mode %s: pull-request opens = %d, want none", mode, publisher.opens())
		}
		for _, patch := range transport.sent() {
			if patch.PullRequest != nil {
				t.Fatalf("mode %s: status patches = %+v, want no pull-request identity", mode, transport.sent())
			}
		}
	}
}

// TestFinishIsInfrastructureFailureWithoutThePullRequestPath proves the seam
// is load-bearing: an opens-mode changes ending on a build with no
// pull-request path never reports published (#844).
func TestFinishIsInfrastructureFailureWithoutThePullRequestPath(t *testing.T) {
	transport := &recordingStatusTransport{}
	coordinator := coordinatorWithFinishSeams(t, &stubPublisher{}, transport)

	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("result = %+v, want an infrastructure failure without the pull-request path", result)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "pull request creation is not configured") {
		t.Fatalf("error = %v, want the missing seam named", result.Err)
	}
}

// pullRequestFailingTransport serves errors only to pull-request identity
// writes, standing in for a broker status-path failure after a create
// succeeded.
type pullRequestFailingTransport struct {
	recordingStatusTransport
}

func (f *pullRequestFailingTransport) PostStatus(_ context.Context, patch StatusPatch) error {
	if patch.PullRequest != nil {
		return errors.New("status transport refused")
	}
	return f.recordingStatusTransport.PostStatus(context.Background(), patch)
}

// TestFinishIsInfrastructureFailureWhenTheIdentityNeverBecomesDurable proves
// the recovery contract: a created pull request whose status write fails
// never yields a published claim; the infrastructure ending lets relaunch
// adopt the same pull request and re-record it idempotently.
func TestFinishIsInfrastructureFailureWhenTheIdentityNeverBecomesDurable(t *testing.T) {
	publisher := &mockPublisher{}
	transport := &pullRequestFailingTransport{}
	coordinator := coordinatorWithFinishSeams(t, publisher, transport)

	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("result = %+v, want an infrastructure failure on the lost status write", result)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "901") {
		t.Fatalf("error = %v, want the created pull request number named for recovery", result.Err)
	}
}

// TestParseOutcomePullRequestDeclaration pins the mode-gated grammar: a
// changes ending on a run that opens its pull request must declare a bounded
// title and body, and a run that never opens one must not declare them (#844).
func TestParseOutcomePullRequestDeclaration(t *testing.T) {
	const valid = `{"outcome":"changes","prTitle":"Resolve issue #7","prBody":"Closes #7. Ships the fix."}`
	var longTitle = `{"outcome":"changes","prTitle":"` + strings.Repeat("t", 257) + `","prBody":"Closes #7. Ships the fix."}`
	var longBody = `{"outcome":"changes","prTitle":"Resolve issue #7","prBody":"` + strings.Repeat("b", 64*1024+1) + `","x":1}`

	for _, mode := range []courierv1alpha1.Mode{courierv1alpha1.ModeResolveIssue, courierv1alpha1.ModeContribute} {
		if _, _, _, err := parseOutcome(`{"outcome":"changes"}`, mode); err == nil {
			t.Fatalf("mode %s: a bare changes declaration parsed without the required prTitle and prBody", mode)
		}
		if _, _, _, err := parseOutcome(`{"outcome":"changes","prTitle":"Resolve issue #7"}`, mode); err == nil {
			t.Fatalf("mode %s: a changes declaration missing prBody parsed", mode)
		}
		if _, _, pr, err := parseOutcome(valid, mode); err != nil || pr == nil ||
			pr.title != "Resolve issue #7" || pr.body != "Closes #7. Ships the fix." {
			t.Fatalf("mode %s: the valid declaration must parse into the declared title and body: pr = %#v, err = %v", mode, pr, err)
		}
		if _, _, _, err := parseOutcome(longTitle, mode); err == nil {
			t.Fatalf("mode %s: an over-bound prTitle parsed", mode)
		}
	}
	for _, mode := range []courierv1alpha1.Mode{courierv1alpha1.ModeFixPR, courierv1alpha1.ModeRebase} {
		if _, _, pr, err := parseOutcome(`{"outcome":"changes"}`, mode); err != nil || pr != nil {
			t.Fatalf("mode %s: a bare changes declaration must parse with no pull-request data: pr = %#v, err = %v", mode, pr, err)
		}
		if _, _, _, err := parseOutcome(valid, mode); err == nil {
			t.Fatalf("mode %s: a pr-carrying changes declaration parsed although this run never opens a pull request", mode)
		}
	}
	if _, _, _, err := parseOutcome(`{"outcome":"no_change_needed","evidence":"done","prTitle":"Resolve issue #7","prBody":"Closes #7. Ships the fix."}`, courierv1alpha1.ModeResolveIssue); err == nil {
		t.Fatal("pull-request fields on a non-changes outcome parsed")
	}
	// The bound is checked before the JSON tail: an over-bound body rides in
	// a document whose parse would otherwise succeed.
	if _, _, _, err := parseOutcome(longBody, courierv1alpha1.ModeResolveIssue); err == nil {
		t.Fatal("an over-bound prBody parsed")
	}
}

// TestStatusReporterRecordsPullRequestIdentity proves the harness side of
// the durable record: the created number rides a dedicated status patch, and
// the broker fence — not the reporter — owns one-identity-per-run (#844).
func TestStatusReporterRecordsPullRequestIdentity(t *testing.T) {
	transport := &recordingStatusTransport{}
	reporter := testReporter(transport)
	if err := reporter.PullRequestOpened(context.Background(), 844); err != nil {
		t.Fatal(err)
	}
	patches := transport.sent()
	if len(patches) != 1 || patches[0].PullRequest == nil || patches[0].PullRequest.Number != 844 {
		t.Fatalf("patches = %+v, want one patch carrying pull request 844", patches)
	}
	if err := reporter.PullRequestOpened(context.Background(), 0); err == nil {
		t.Fatal("a malformed pull-request number reached the wire")
	}
	if len(transport.sent()) != 1 {
		t.Fatal("the malformed write reached the broker")
	}
}

// TestCriteriaEnvelopeIsPinnedIntoTheCoordinatorSession proves the native
// coordinator receives the operator's pinned criterion envelope as system
// framing — it has no shell to read COURIER_CRITERIA itself — and that the
// model-surface digest covers the identical section text (#844).
func TestCriteriaEnvelopeIsPinnedIntoTheCoordinatorSession(t *testing.T) {
	criteriaJSON := modelsurfaceCriteriaJSON(t, "sha256:"+strings.Repeat("b", 64))
	gateway, mock, cleanup := gatewayFor(gatewayResponse{body: sse(
		contentChunk(`{"outcome":"blocked_external","missing":"a decision from the human"}`), finishChunk("stop"))})
	defer cleanup()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: gateway,
		Bindings:  testBindings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	inv := testInvocation()
	inv.Criteria = criteriaJSON
	result := coordinator.Run(context.Background(), inv)
	if result.Kind != courierv1alpha1.NativeResultBlockedExternal {
		t.Fatalf("result = %+v, want the blocked_external ending", result)
	}
	if len(mock.calls) != 1 {
		t.Fatalf("model requests = %d, want one", len(mock.calls))
	}
	var request struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal([]byte(mock.calls[0].rawBody), &request); err != nil {
		t.Fatal(err)
	}
	pinned := criteriaEnvelopeSection(criteriaJSON)
	var pinnedAt string
	for _, message := range request.Messages {
		if message.Role == "system" && message.Content == pinned {
			pinnedAt = message.Role
		}
	}
	if pinnedAt == "" {
		t.Fatalf("no system message carries the pinned criterion envelope:\n%s", mock.calls[0].rawBody)
	}
	// The model-surface section digest must be the digest of the identical
	// pinned text: the transcript and the provenance cannot drift.
	surface, err := coordinator.ModelSurface(ModelSurfaceInputs{
		GatewayTransport: "openai",
		CriteriaJSON:     criteriaJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded modelSurface
	if err := json.Unmarshal(surface.Encoding, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, candidate := range decoded.Sections {
		if candidate.Name != "criteria.envelope" {
			continue
		}
		found = true
		if candidate.Role != "system" || candidate.Digest != textDigest(pinned) {
			t.Fatalf("section = %+v, want the system digest of the pinned section text", candidate)
		}
	}
	if !found {
		t.Fatalf("sections = %+v, want a criteria.envelope section", decoded.Sections)
	}
}

// TestFinishOpensNoPullRequestWithoutAStatusPath proves ordering: the
// identity write is load-bearing, so a build that cannot make the identity
// durable must refuse the side effect, not open a pull request it can never
// record (#844).
func TestFinishOpensNoPullRequestWithoutAStatusPath(t *testing.T) {
	publisher := &mockPublisher{}
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Transport: &Gateway{BaseURL: "http://gateway.invalid"},
		Bindings:  testBindings(),
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.finish(context.Background(), testInvocation(), executor.OutcomeChanges, "done", nil, pullRequestDeclaration)
	if result.Kind != courierv1alpha1.NativeResultInfrastructureFailed {
		t.Fatalf("result = %+v, want an infrastructure failure without a status path", result)
	}
	if publisher.opens() != 0 {
		t.Fatalf("pull-request opens = %d, want none without a status path", publisher.opens())
	}
}
