package controller

import (
	"context"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// The secure preflight's permanent-class, non-terminal verdicts are the
// operator's own retryable failures: the deployment is misconfigured, but the
// run may not be terminalized on them, because a signal that clears on a later
// pass would strand a recoverable run. Left alone, that path cycles the run
// Pending → Claimed → Pending as fast as the API server allows: the failed
// pass rolls the claim back to Pending, and that status write wakes the
// controller immediately. The gate below bounds that churn — a failed attempt
// records how many consecutive attempts have failed, and the next attempt
// waits a bounded, growing delay before it is made. Nothing here changes a
// verdict class: the verdict itself is still the operator's to read.
const (
	// preflightBackoffBaseDelay is the wait after the first consecutive
	// failed secure preflight.
	preflightBackoffBaseDelay = 15 * time.Second
	// preflightBackoffMaxDelay bounds the wait however many consecutive
	// attempts have failed.
	preflightBackoffMaxDelay = 5 * time.Minute
)

// preflightFailedAtAnnotation records, as an RFC3339 timestamp, when the last
// failed secure preflight attempt happened. The marker lives on the object
// because process memory dies with the controller, and a restart must not turn
// the bounded wait back into the tight churn it replaced.
const preflightFailedAtAnnotation = "courier.misospace.dev/preflight-failed-at"

// preflightFailuresAnnotation records how many consecutive secure preflight
// attempts have failed. The count drives the delay schedule: each further
// consecutive failure waits longer, up to the ceiling.
const preflightFailuresAnnotation = "courier.misospace.dev/preflight-failures"

// preflightBackoffDelay returns the wait before the n-th consecutive failed
// secure preflight is retried: the base delay doubled once per further
// consecutive failure, capped at the ceiling.
//
// The doubling stops as soon as the ceiling is reached, so the shift is
// bounded by the schedule itself: a run that failed thousands of times still
// computes a bounded delay, and the count can never overflow it.
func preflightBackoffDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := preflightBackoffBaseDelay
	for n := 1; n < failures; n++ {
		if delay >= preflightBackoffMaxDelay {
			return preflightBackoffMaxDelay
		}
		delay *= 2
	}
	if delay > preflightBackoffMaxDelay {
		return preflightBackoffMaxDelay
	}
	return delay
}

// preflightBackoffRemaining reports how long the run must still wait before
// its next secure preflight attempt, and whether it must wait at all.
//
// The gate fails open: a run that carries no marker, an unreadable marker, an
// elapsed one, or one the clock has not reached yet is due immediately,
// because a gate that cannot be read must never strand a run that could still
// succeed. Both markers are part of the gate: the count decides how long the
// wait is, so a timestamp without a readable count is a gate the operator
// cannot read, and it holds the run back not at all. A future-dated marker is
// due for the same reason the merge observation's marker is: clock skew must
// not decide that a run waits.
func (r *CoderRunReconciler) preflightBackoffRemaining(run *courier.CoderRun) (time.Duration, bool) {
	raw, ok := run.Annotations[preflightFailedAtAnnotation]
	if !ok {
		return 0, false
	}
	stampedAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, false
	}
	failures, ok := recordedPreflightFailures(run)
	if !ok {
		return 0, false
	}
	delay := preflightBackoffDelay(failures)
	elapsed := r.clock().UTC().Sub(stampedAt)
	if elapsed < 0 || elapsed >= delay {
		return 0, false
	}
	return delay - elapsed, true
}

// recordedPreflightFailures reads the consecutive-failure count exactly as the
// run records it, and reports whether the run carries a readable one. A count
// the operator cannot read is no count: the gate that depends on it cannot be
// read either, so the gate reader treats it as no gate at all rather than
// guess a delay, and the arming path treats it as no recorded failure to
// build on.
func recordedPreflightFailures(run *courier.CoderRun) (int, bool) {
	raw, ok := run.Annotations[preflightFailuresAnnotation]
	if !ok {
		return 0, false
	}
	count, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || count < 1 {
		return 0, false
	}
	return count, true
}

// livePreflightFailures reads the recorded failure count from the API server
// rather than from the caller's copy of the run.
//
// The caller's copy usually comes from the informer cache, which can lag the
// arming write this function is about to make: a cache that still shows the
// previous count would restart the schedule at the base delay on every pass,
// and a cache that shows none would let the count never grow at all. The
// uncached read is what the admission count already does for the same reason
// (#548). A read that fails falls back to the caller's copy: an unreadable
// live count must not stop the gate from being armed at all.
func (r *CoderRunReconciler) livePreflightFailures(ctx context.Context, run *courier.CoderRun) *courier.CoderRun {
	live := &courier.CoderRun{}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(run), live); err != nil {
		return run
	}
	return live
}

// armPreflightBackoff records the failed secure preflight attempt on the run:
// the consecutive-failure count grows by one and the timestamp is restamped,
// so the next attempt waits the next longer delay.
//
// The write is a narrow merge patch of the run's own metadata, exactly as the
// merge observation's marker is written: the gate is operator-owned state and
// never rides the status patch, whose transport is the run's own record of
// its execution.
func (r *CoderRunReconciler) armPreflightBackoff(ctx context.Context, run *courier.CoderRun) error {
	// The first failure counts as one; a run whose count is missing or
	// unreadable has no recorded failure to build on, so it starts over
	// rather than inheriting a count nobody wrote.
	failures := 1
	if previous, ok := recordedPreflightFailures(r.livePreflightFailures(ctx, run)); ok {
		failures = previous + 1
		if failures <= previous {
			// The count already holds the largest number it can, so
			// it stays there: a wrapped count would be written as a
			// negative number, which no reader could use.
			failures = previous
		}
	}
	before := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[preflightFailuresAnnotation] = strconv.Itoa(failures)
	run.Annotations[preflightFailedAtAnnotation] = r.clock().UTC().Format(time.RFC3339)
	return r.Patch(ctx, run, client.MergeFrom(before))
}

// clearPreflightBackoff removes the failed-preflight gate from the run, so a
// later failure starts from the base delay again. A run that carries no gate
// is left untouched: the clear never writes.
func (r *CoderRunReconciler) clearPreflightBackoff(ctx context.Context, run *courier.CoderRun) error {
	if _, ok := run.Annotations[preflightFailedAtAnnotation]; !ok {
		if _, ok := run.Annotations[preflightFailuresAnnotation]; !ok {
			return nil
		}
	}
	before := run.DeepCopy()
	delete(run.Annotations, preflightFailedAtAnnotation)
	delete(run.Annotations, preflightFailuresAnnotation)
	return r.Patch(ctx, run, client.MergeFrom(before))
}

// clearPreflightBackoffAfterLaunch spends the gate a failed preflight armed,
// once the launch has passed that preflight.
//
// The clear is best-effort: a stale marker on a run that is past the preflight
// is inert — the gate is read on the admission path and on the resume of a
// claimed run, and neither is this run's path any more — while failing the
// launch over it would strand a run that has already passed the gate in
// `Claimed`, holding its lane slot and its source claim for as long as the
// write keeps failing.
func (r *CoderRunReconciler) clearPreflightBackoffAfterLaunch(ctx context.Context, run *courier.CoderRun) {
	if err := r.clearPreflightBackoff(ctx, run); err != nil {
		log.FromContext(ctx).Error(err, "could not clear the secure preflight backoff; the run keeps a stale gate marker",
			"run", run.Name)
	}
}
