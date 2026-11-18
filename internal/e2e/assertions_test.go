package e2e

// This file holds the negative assertions every fixture shares. They are the
// claims the issue makes about the whole integrated path, not about one
// fixture's happy path:
//
//   - no credential ever reaches a recorded surface (the deployment's provider
//     token, the run's status, the published pull request, or the source's
//     recorded lifecycle),
//   - the source adapter only ever receives the calls and states its contract
//     defines — never an unexpected mutation of the work item, never a second
//     claim of the same item,
//   - the publication is singular: exactly one pull request on the pinned head
//     and exactly one create on the forge.
//
// Every helper scans recorded values. None of them asks a human to look at an
// output and judge it.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
	"github.com/misospace/courier/internal/source"
)

// fixtureCredential is the deployment's provider credential: the value the
// fixture seeds into the operator namespace's Secret and that the operator
// copies into the per-run secret the broker reads. It is the string every
// fixture scans for.
const fixtureCredential = "fixture-token"

// scannedSurface is one recorded value the credential scan looks at.
type scannedSurface struct {
	// name identifies the surface in a failure message.
	name string
	// value is the recorded content.
	value string
}

// assertNoCredentialLeakage proves the fixture's credential reached none of
// the recorded surfaces: the status patches the harness asked for, the run's
// own status, the published pull request, or the source's recorded
// lifecycle.
//
// The scan is not vacuous: the credential must still be live in the operator
// namespace's Secret, so the fixture proves a real credential stayed out of
// every recorded surface rather than proving that no credential existed.
func assertNoCredentialLeakage(t *testing.T, secret, operatorNamespace string, surfaces ...scannedSurface) {
	t.Helper()
	if secret == "" {
		t.Fatal("the credential scan needs the credential itself")
	}
	if len(surfaces) == 0 {
		t.Fatal("the credential scan needs at least one surface to scan")
	}
	live := &corev1.Secret{}
	if err := envtestClient.Get(context.Background(), client.ObjectKey{Namespace: operatorNamespace, Name: "forge-creds"}, live); err != nil {
		t.Fatalf("read the deployment credential secret: %v", err)
	}
	if got := string(live.Data["token"]); got != secret {
		t.Fatalf("the deployment credential secret holds %q, want the fixture's credential: the scan would be vacuous", got)
	}
	for _, surface := range surfaces {
		if strings.Contains(surface.value, secret) {
			t.Fatalf("the credential leaked into the %s: %s", surface.name, surface.value)
		}
	}
}

// surface renders one recorded value for the credential scan.
func surface(t *testing.T, name string, value any) scannedSurface {
	t.Helper()
	return scannedSurface{name: name, value: renderRecorded(t, value)}
}

// renderRecorded renders a recorded value as the text a scan can look at: the
// JSON encoding for structured values, the raw text for strings.
func renderRecorded(t *testing.T, value any) string {
	t.Helper()
	switch typed := value.(type) {
	case string:
		return typed
	case nil:
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("render %T for the scan: %v", value, err)
	}
	return string(encoded)
}

// allowedSourceOps is the source contract's own call vocabulary: an adapter
// receives exactly these calls, in the order the operator's lifecycle defines.
var allowedSourceOps = map[string]bool{
	"claim":              true,
	"claim-with-receipt": true,
	"release":            true,
	"transition":         true,
	"report":             true,
	"resolve":            true,
}

// allowedSourceStates is the source contract's lifecycle vocabulary.
var allowedSourceStates = map[source.State]bool{
	source.StateInProgress: true,
	source.StateInReview:   true,
	source.StateNeedsHuman: true,
}

// allowedSourceResults is the source contract's outcome vocabulary.
var allowedSourceResults = map[source.Result]bool{
	source.ResultStarted:           true,
	source.ResultReady:             true,
	source.ResultBlocked:           true,
	source.ResultCapacityExhausted: true,
	source.ResultBlockedExternal:   true,
	source.ResultFailed:            true,
	source.ResultHandedOff:         true,
	source.ResultNoChange:          true,
	source.ResultAlreadyDone:       true,
}

// assertNoForbiddenSourceMutations proves the source adapter only received
// the calls its contract defines, for the expected work items, with the
// contract's own states and outcomes — and that no work item was claimed
// twice at once.
//
// The negative claims are the point: an unexpected call, an unknown state, a
// work item the run does not own, or a second live claim of the same item is
// a source mutation the operator must never make.
//
// A claim the operator released and then made again is not a mutation: a
// failed launch releases the claim and returns the run to Pending, and the
// next admission claims it again. What must never happen is two live claims
// of one item, so the check counts live claims, not claim events.
func assertNoForbiddenSourceMutations(t *testing.T, src *recordingSource, itemIDs ...string) {
	t.Helper()
	if src == nil {
		t.Fatal("the source mutation check needs the recording adapter")
	}
	expected := map[string]bool{}
	for _, itemID := range itemIDs {
		expected[itemID] = true
	}
	liveClaims := map[string]int{}
	for _, call := range src.Calls() {
		if !allowedSourceOps[call.Op] {
			t.Fatalf("the source received the call %q, which is not in the source contract's vocabulary", call.Op)
		}
		if !expected[call.ID] {
			t.Fatalf("the source received a call for the work item %q, want one of %v", call.ID, itemIDs)
		}
		switch call.Op {
		case "claim", "claim-with-receipt":
			liveClaims[call.ID]++
			if liveClaims[call.ID] > 1 {
				t.Fatalf("the work item %q holds %d live claims: the operator must never hold two claims on the same item", call.ID, liveClaims[call.ID])
			}
		case "release":
			if liveClaims[call.ID] == 0 {
				t.Fatalf("the work item %q was released without a live claim", call.ID)
			}
			liveClaims[call.ID]--
		case "transition":
			if !allowedSourceStates[call.State] {
				t.Fatalf("the source received the transition state %q, which is not in the source contract's vocabulary", call.State)
			}
		}
	}
	for _, report := range src.Reports() {
		if !allowedSourceStates[report.State] {
			t.Fatalf("the source received the lifecycle state %q, which is not in the source contract's vocabulary", report.State)
		}
		if !allowedSourceResults[report.Result] {
			t.Fatalf("the source received the lifecycle result %q, which is not in the source contract's vocabulary", report.Result)
		}
	}
}

// assertExactlyOnePullRequest proves the pinned head carries exactly one pull
// request and that the forge recorded exactly one create for it: the
// publication is singular, and a second create would be a duplicate
// publication.
func assertExactlyOnePullRequest(t *testing.T, ctx context.Context, provider *forgetest.LocalGit, baseRepo, workRepo, workRef string, wantNumber int) {
	t.Helper()
	pulls, err := provider.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: baseRepo}, workRepo, workRef)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 1 {
		t.Fatalf("pull requests on the pinned head = %d, want exactly one", len(pulls))
	}
	if pulls[0].Number != wantNumber {
		t.Fatalf("the pull request on the pinned head is #%d, want #%d", pulls[0].Number, wantNumber)
	}
	creates := provider.OpsOf(forgetest.LocalGitOpCreatePullRequest)
	if len(creates) != 1 {
		t.Fatalf("the forge recorded %d pull-request creates, want exactly one: the publication must not duplicate itself", len(creates))
	}
}

// assertNoPullRequest proves nothing was published: no pull request claims the
// pinned head and the forge recorded no create.
func assertNoPullRequest(t *testing.T, ctx context.Context, provider *forgetest.LocalGit, baseRepo, workRepo, workRef string) {
	t.Helper()
	pulls, err := provider.ListPullRequestsByHead(ctx, forge.PullRequestRef{Repo: baseRepo}, workRepo, workRef)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 0 {
		t.Fatalf("pull requests on the pinned head = %d, want none: the failed run published nothing", len(pulls))
	}
	if creates := provider.OpsOf(forgetest.LocalGitOpCreatePullRequest); len(creates) != 0 {
		t.Fatalf("the forge recorded %d pull-request creates, want none: the failed run published nothing", len(creates))
	}
}

// assertNoLaneReservation proves the lane's capacity is available again: no
// run on the lane is in a phase that reserves it.
//
// The operator's lane reservation IS the run's phase — a run in Claimed or
// Running reserves the lane's concurrency, and every other phase reserves
// nothing (internal/controller/admission.go). There is no separate reservation
// object for a non-Dispatch source, so the run's phase is the whole signal,
// and the fixture reads it with the operator's own rule rather than a copy of
// it.
func assertNoLaneReservation(t *testing.T, namespace, lane string) {
	t.Helper()
	runs := &courierv1alpha1.CoderRunList{}
	if err := envtestClient.List(context.Background(), runs, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Spec.Lane != lane {
			continue
		}
		if controller.PhaseConsumesCapacity(run.Status.Phase) {
			t.Fatalf("the run %q still reserves the lane %q in phase %q: the lane's capacity was not released", run.Name, lane, run.Status.Phase)
		}
	}
}

// assertSingleRunForWorkItem proves exactly one CoderRun owns the work item:
// the restart never materialized a successor run beside the live one.
func assertSingleRunForWorkItem(t *testing.T, namespace, workItemID string) {
	t.Helper()
	runs := &courierv1alpha1.CoderRunList{}
	if err := envtestClient.List(context.Background(), runs, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	found := 0
	for i := range runs.Items {
		if runs.Items[i].Spec.WorkItemID == workItemID {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("CoderRuns for the work item %q = %d, want exactly one", workItemID, found)
	}
}
