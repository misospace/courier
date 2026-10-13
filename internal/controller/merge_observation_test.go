package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
)

// fakePullRequestReader is the forge read-pull-request capability seam for
// tests: it answers the exact read with a canned pull request or error and
// records every requested identity, so a test can prove the observation keys
// on the recorded PR number rather than on a branch name.
type fakePullRequestReader struct {
	pull         forge.PullRequest
	err          error
	noCapability bool
	calls        int
	requests     []forge.PullRequestRef
}

func (f *fakePullRequestReader) Capabilities() forge.Capabilities {
	if f.noCapability {
		return forge.Capabilities{}
	}
	return forge.NewCapabilities(forge.CapabilityReadPullRequest)
}

func (f *fakePullRequestReader) ReadPullRequest(_ context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	f.calls++
	f.requests = append(f.requests, ref)
	if f.err != nil {
		return forge.PullRequest{}, f.err
	}
	return f.pull, nil
}

// reviewRun returns a settled AwaitingReview run: its own execution ended at
// reapClock, its in-review report published, and its status carries the exact
// pull-request identity the merge observation must read by.
func reviewRun(name string) *courierv1alpha1.CoderRun {
	run := admissionRun(name, "local", courierv1alpha1.PhaseAwaitingReview)
	run.Status.PR = "42"
	run.Status.Branch = "courier/acme/widgets/issue-1"
	run.Status.HeadRepo = "acme/widgets"
	run.Status.FinishedAt = &metav1.Time{Time: reapClock}
	run.Status.Conditions = []metav1.Condition{{
		Type:               lifecycleReportedCondition,
		Status:             metav1.ConditionTrue,
		Reason:             "Published",
		LastTransitionTime: metav1.NewTime(reapClock),
	}}
	return run
}

func observedPR(mutate func(forge.PullRequest) forge.PullRequest) forge.PullRequest {
	pull := forge.PullRequest{
		Number:   42,
		Repo:     "acme/widgets",
		BaseRepo: "acme/widgets",
		HeadRepo: "acme/widgets",
		HeadRef:  "courier/acme/widgets/issue-1",
		HeadSHA:  "headsha1",
		State:    "open",
	}
	return mutate(pull)
}

func mergedPR() forge.PullRequest {
	return observedPR(func(pull forge.PullRequest) forge.PullRequest {
		pull.State = "closed"
		pull.Merged = true
		return pull
	})
}

func TestAwaitingReviewMergedPRFinishesRunThroughReap(t *testing.T) {
	item := &admissionSource{}
	reader := &fakePullRequestReader{pull: mergedPR()}
	run := reviewRun("merged")
	finishedBefore := run.Status.FinishedAt.Time
	c := phaseClient(t, run)

	first := reapReconciler(c, item, reapClock)
	first.PullRequests = reader
	if _, err := first.Reconcile(context.Background(), admissionRequest("merged")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(reader.requests) != 1 || reader.requests[0] != (forge.PullRequestRef{Repo: "acme/widgets", Number: 42}) {
		t.Fatalf("reads = %#v, want one exact read of PR 42 in the run's base repository", reader.requests)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("merged"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseDone {
		t.Fatalf("phase = %q, want Done after a confirmed merge on the recorded identity", stored.Status.Phase)
	}
	if stored.Status.FinishedAt == nil || !stored.Status.FinishedAt.Time.Equal(finishedBefore) {
		t.Fatalf("finishedAt = %v, want the originally recorded %v preserved", stored.Status.FinishedAt, finishedBefore)
	}
	if len(item.resolved) != 0 {
		t.Fatalf("resolved IDs = %#v, want none; the source resolve belongs to the Done path", item.resolved)
	}
	if len(item.reports) != 1 {
		t.Fatalf("reports = %#v, want exactly the one Done lifecycle report", item.reports)
	}
	if item.reports[0].PR != "42" {
		t.Fatalf("report PR = %q, want the carried PR 42", item.reports[0].PR)
	}
	if item.reports[0].IdempotencyKey != "coderun/default/merged/Done" {
		t.Fatalf("report idempotency key = %q, want the deterministic Done key", item.reports[0].IdempotencyKey)
	}

	// A brand-new reconciler (controller restart) resolves the source once,
	// and only then starts the retention clock.
	second := reapReconciler(c, item, reapClock.Add(time.Minute))
	second.PullRequests = reader
	result, err := second.Reconcile(context.Background(), admissionRequest("merged"))
	if err != nil {
		t.Fatalf("Done Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("RequeueAfter = %v, want the full retention window after the resolve", result.RequeueAfter)
	}
	if len(item.resolved) != 1 || item.resolved[0] != "merged" {
		t.Fatalf("resolved IDs = %#v, want the source resolved exactly once", item.resolved)
	}
	if reader.calls != 1 {
		t.Fatalf("pull-request reads = %d, want none once the run is Done", reader.calls)
	}
	if err := c.Get(context.Background(), admissionKey("merged"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got := stored.Annotations[doneAtAnnotation]; got != reapClock.Add(time.Minute).UTC().Format(time.RFC3339) {
		t.Fatalf("done-at = %q, want the marker stamped after the resolve", got)
	}

	// Past retention the run is deleted, and deletion never re-resolves.
	third := reapReconciler(c, item, reapClock.Add(12*time.Minute))
	if _, err := third.Reconcile(context.Background(), admissionRequest("merged")); err != nil {
		t.Fatalf("retention Reconcile() error = %v", err)
	}
	if err := c.Get(context.Background(), admissionKey("merged"), &courierv1alpha1.CoderRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get merged run = %v, want NotFound after retention", err)
	}
	if len(item.resolved) != 1 {
		t.Fatalf("resolved IDs = %#v, want exactly one resolve across the run's whole life", item.resolved)
	}
}

func TestAwaitingReviewMergeObservationNeverFinishesWithoutConfirmedIdentity(t *testing.T) {
	tests := []struct {
		name   string
		reader *fakePullRequestReader
	}{
		{"open pull request", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest { return pull })}},
		{"closed without merge", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest {
			pull.State = "closed"
			return pull
		})}},
		{"forge read failure", &fakePullRequestReader{err: errors.New("github unavailable")}},
		{"merged from a changed head repository", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest {
			pull.State = "closed"
			pull.Merged = true
			pull.HeadRepo = "forkowner/widgets"
			return pull
		})}},
		{"merged under a changed head ref", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest {
			pull.State = "closed"
			pull.Merged = true
			pull.HeadRef = "some-other-branch"
			return pull
		})}},
		{"read answers a different number", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest {
			pull.State = "closed"
			pull.Merged = true
			pull.Number = 7
			return pull
		})}},
		{"read answers a different base repository", &fakePullRequestReader{pull: observedPR(func(pull forge.PullRequest) forge.PullRequest {
			pull.State = "closed"
			pull.Merged = true
			pull.BaseRepo = "other/widgets"
			pull.Repo = "other/widgets"
			return pull
		})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := &admissionSource{}
			run := reviewRun("stay")
			finishedBefore := run.Status.FinishedAt.Time
			c := phaseClient(t, run)
			reconciler := reapReconciler(c, item, reapClock)
			reconciler.PullRequests = test.reader

			result, err := reconciler.Reconcile(context.Background(), admissionRequest("stay"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want the observation never to fail the run", err)
			}
			if result.RequeueAfter != mergeObservationRequeueDelay {
				t.Fatalf("RequeueAfter = %v, want the slow observation cadence", result.RequeueAfter)
			}
			var stored courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), admissionKey("stay"), &stored); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if stored.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
				t.Fatalf("phase = %q, want AwaitingReview; this observation is not proof of merged work", stored.Status.Phase)
			}
			if stored.Status.FinishedAt == nil || !stored.Status.FinishedAt.Time.Equal(finishedBefore) {
				t.Fatal("finishedAt changed without a terminal transition")
			}
			if len(item.resolved) != 0 || len(item.reports) != 0 {
				t.Fatalf("source calls = resolve %#v reports %#v, want none", item.resolved, item.reports)
			}
			if _, ok := stored.Annotations[doneAtAnnotation]; ok {
				t.Fatal("done-at marker written without a Done phase; the retention clock must not start")
			}
		})
	}
}

func TestAwaitingReviewMergeObservationSkipsRunsWithoutExactIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(run *courierv1alpha1.CoderRun)
	}{
		{"no recorded pull request number", func(run *courierv1alpha1.CoderRun) { run.Status.PR = "" }},
		{"pull request identity is not a number", func(run *courierv1alpha1.CoderRun) { run.Status.PR = "https://github.com/acme/widgets/pull/42" }},
		{"no recorded head branch", func(run *courierv1alpha1.CoderRun) { run.Status.Branch = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := &admissionSource{}
			reader := &fakePullRequestReader{pull: mergedPR()}
			run := reviewRun("vague")
			test.mutate(run)
			c := phaseClient(t, run)
			reconciler := reapReconciler(c, item, reapClock)
			reconciler.PullRequests = reader

			result, err := reconciler.Reconcile(context.Background(), admissionRequest("vague"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if result.RequeueAfter != 0 {
				t.Fatalf("RequeueAfter = %v, want no timer; a run without an exact recorded identity must not poll", result.RequeueAfter)
			}
			if reader.calls != 0 {
				t.Fatalf("pull-request reads = %d, want none; observation may not guess an identity", reader.calls)
			}
			var stored courierv1alpha1.CoderRun
			if err := c.Get(context.Background(), admissionKey("vague"), &stored); err != nil {
				t.Fatalf("get run: %v", err)
			}
			if stored.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
				t.Fatalf("phase = %q, want AwaitingReview", stored.Status.Phase)
			}
		})
	}
}

func TestAwaitingReviewMergeObservationSkippedWithoutForgeCapability(t *testing.T) {
	item := &admissionSource{}
	reader := &fakePullRequestReader{pull: mergedPR(), noCapability: true}
	c := phaseClient(t, reviewRun("nocap"))
	reconciler := reapReconciler(c, item, reapClock)
	reconciler.PullRequests = reader

	result, err := reconciler.Reconcile(context.Background(), admissionRequest("nocap"))
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want no timer; a provider without the read capability is never asked", result.RequeueAfter)
	}
	if reader.calls != 0 {
		t.Fatalf("pull-request reads = %d, want none", reader.calls)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("nocap"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview", stored.Status.Phase)
	}
}

func TestAwaitingReviewPendingReportRetriesBeforeMergeObservation(t *testing.T) {
	item := &admissionSource{}
	reader := &fakePullRequestReader{pull: mergedPR()}
	run := reviewRun("queued")
	run.Status.Conditions = []metav1.Condition{{
		Type:               lifecycleReportedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             lifecycleReportPendingReason,
		LastTransitionTime: metav1.NewTime(reapClock),
	}}
	c := phaseClient(t, run)
	reconciler := reapReconciler(c, item, reapClock)
	reconciler.PullRequests = reader

	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("queued")); err != nil {
		t.Fatalf("retry Reconcile() error = %v", err)
	}
	if reader.calls != 0 {
		t.Fatalf("pull-request reads = %d, want none while the in-review report is still pending", reader.calls)
	}
	if len(item.reports) != 1 || item.reports[0].Result != source.ResultReady {
		t.Fatalf("reports = %#v, want the retried ready report first", item.reports)
	}
	var stored courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), admissionKey("queued"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseAwaitingReview {
		t.Fatalf("phase = %q, want AwaitingReview on the report-retry reconcile", stored.Status.Phase)
	}

	// With the report settled, the next reconcile observes the merge and the
	// run finishes through the Done path.
	if _, err := reconciler.Reconcile(context.Background(), admissionRequest("queued")); err != nil {
		t.Fatalf("observe Reconcile() error = %v", err)
	}
	if reader.calls != 1 {
		t.Fatalf("pull-request reads = %d, want one once the report settled", reader.calls)
	}
	if err := c.Get(context.Background(), admissionKey("queued"), &stored); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.Status.Phase != courierv1alpha1.PhaseDone {
		t.Fatalf("phase = %q, want Done after the report settled and the merge was confirmed", stored.Status.Phase)
	}
}

func TestLateDoneTransitionRecordsRunMetricsOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder := NewRunRecorder(registry)

	// The AwaitingReview run's metrics were recorded when FinishedAt was first
	// set, before this process existed; the late Done transition must not
	// count the run a second time.
	run := reviewRun("late-done")
	c := phaseClient(t, run)
	first := reapReconciler(c, &admissionSource{}, reapClock)
	first.Metrics = recorder
	first.PullRequests = &fakePullRequestReader{pull: mergedPR()}
	if _, err := first.Reconcile(context.Background(), admissionRequest("late-done")); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == "courier_coderuns_total" {
			t.Fatalf("a late AwaitingReview to Done transition recorded terminal metrics again: %v", family)
		}
	}

	// The pre-#250 entry stays counted: a run merged while Verifying has no
	// recorded FinishedAt yet, so its Done transition is its first terminal
	// transition.
	verifying := admissionRun("verified-done", "local", courierv1alpha1.PhaseVerifying)
	verifying.Status.PR = "42"
	verifyingClient := phaseClient(t, verifying)
	second := reapReconciler(verifyingClient, &admissionSource{}, reapClock)
	second.Metrics = recorder
	second.Observer = fakeWorldObserver{observation: PRObservation{PR: "42", Merged: true}}
	if _, err := second.Reconcile(context.Background(), admissionRequest("verified-done")); err != nil {
		t.Fatalf("Verifying Reconcile() error = %v", err)
	}
	labels := map[string]string{"lane": "local", "mode": "resolve-issue", "phase": "Done"}
	if got := gaugeCounter(t, registry, labels); got != 1 {
		t.Fatalf("courier_coderuns_total = %v, want 1 for the first terminal transition", got)
	}
}
