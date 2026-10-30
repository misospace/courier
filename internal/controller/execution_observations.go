package controller

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// executionObservationTimeout bounds a single execution-observation report
// attempt. Each attempt carries its own deadline, so a wedged endpoint costs
// at most this much per attempt; the failure cooldown below bounds how often
// the same run pays it.
const executionObservationTimeout = 10 * time.Second

// executionObservationRetryDelay is the cooldown after a transient report
// failure: a failed report costs at most one bounded attempt per run per
// window, so a wedged-but-reachable endpoint cannot occupy a reconcile worker
// more than once per window. A real state change always bypasses it.
const executionObservationRetryDelay = 60 * time.Second

// observationKey is the process-local identity of a run in the
// execution-observation maps: "<namespace>/<name>".
func observationKey(namespace, name string) string {
	return namespace + "/" + name
}

// observationMarker is a run's last successfully reported observation: the
// attempt's UID plus the state the source accepted. The UID makes a recreated
// run (same namespace/name, new UID) never dedupe against its predecessor, so
// a delete-and-recreate that coalesces before the worker sees the deletion
// still re-reports instead of being suppressed by a stale marker.
type observationMarker struct {
	uid   string
	state source.ExecutionState
}

// observationRetry is a transient-failure cooldown: it names the exact
// attempt (UID plus state) whose report failed and the time the next attempt
// for that pair may start. Suppression applies only to that pair, so a UID or
// state change is new information and is never held back, while a wedged
// endpoint is retried at most once per window for the attempt it is failing.
type observationRetry struct {
	attempt   observationMarker
	notBefore time.Time
}

// executionObservationState maps a run phase to the execution-observation
// state it represents. Pending, Claimed, and unset phases carry no
// observation: the work has not started executing, and absence is rendered by
// the source as unknown coverage, never as proof of anything.
func executionObservationState(phase courierv1alpha1.Phase) (source.ExecutionState, bool) {
	switch phase {
	case courierv1alpha1.PhaseRunning:
		return source.ExecutionRunning, true
	case courierv1alpha1.PhaseVerifying:
		return source.ExecutionVerifying, true
	case courierv1alpha1.PhaseAwaitingReview:
		return source.ExecutionAwaitingReview, true
	case courierv1alpha1.PhaseDone:
		return source.ExecutionSucceeded, true
	case courierv1alpha1.PhaseFailed:
		return source.ExecutionFailed, true
	case courierv1alpha1.PhaseNeedsHuman:
		return source.ExecutionBlocked, true
	case courierv1alpha1.PhaseBlockedExternal:
		return source.ExecutionBlockedExternal, true
	case courierv1alpha1.PhaseHandedOff:
		return source.ExecutionHandedOff, true
	default:
		return "", false
	}
}

// publishExecutionObservation reports the run's current execution state to
// its source adapter when the adapter supports execution observations.
//
// Invariants:
//
//   - Execution observations are non-authoritative, best-effort
//     observability. A report failure, rejection, or absence must never gate
//     discovery, claim/admission, launch, status updates, publication,
//     settlement, recovery, capacity release, or retention: this function
//     never returns an error and never requeues, and no outcome changes what
//     Reconcile does next.
//   - Telemetry is bounded, not free: a failed report costs at most one
//     bounded attempt per run per executionObservationRetryDelay window, each
//     attempt carrying its own executionObservationTimeout deadline, and the
//     window never holds back a state change. The source ages the last
//     accepted state, so a delayed or skipped report is unknown coverage,
//     never absence of execution.
//   - It infers no model progress beyond the run's own phases.
//   - Observations carry no secrets, prompts, or work output.
//   - Telemetry never claims source ownership: it carries no claim receipts,
//     fence tokens, or lease proof.
//
// Reporting is state-change only, per process: a run reports a state once per
// process lifetime (a recreated run is a new UID and always re-reports), and a
// process restart re-reconciles every run and re-publishes the current
// observation, which converges server-side because reports are idempotent
// current-state upserts.
func (r *CoderRunReconciler) publishExecutionObservation(ctx context.Context, run *courierv1alpha1.CoderRun) {
	state, ok := executionObservationState(run.Status.Phase)
	if !ok {
		return
	}
	adapter, item, err := r.adapterAndWorkItem(run)
	if err != nil {
		// Runs without a registered source adapter have no execution feed;
		// absence means unknown coverage, not an error.
		return
	}
	observer, ok := adapter.(source.ExecutionObserver)
	if !ok {
		return
	}

	key := observationKey(run.Namespace, run.Name)
	runUID := string(run.UID)

	r.obsMu.Lock()
	degraded := r.obsDegraded[run.Spec.Source]
	marker, hasMarker := r.obsLast[key]
	// A UID change is a new attempt and never dedupes, even into the same
	// state its predecessor already reported.
	deduped := hasMarker && marker.uid == runUID && marker.state == state
	// The cooldown holds back only a repeat of the exact attempt whose
	// report failed. A UID or state change bypasses the window; a repeat of
	// the same failed pair waits for it.
	retry, hasRetry := r.obsRetry[key]
	cooldown := !deduped && hasRetry &&
		retry.attempt.uid == runUID && retry.attempt.state == state &&
		time.Now().Before(retry.notBefore)
	r.obsMu.Unlock()
	if degraded || deduped || cooldown {
		return
	}

	workIdentity := item.ID
	if ident, ok := adapter.(source.WorkIdentifier); ok {
		workIdentity = ident.WorkIdentity(item.ID)
	}
	observation := source.ExecutionObservation{
		RunUID:       runUID,
		RunNamespace: run.Namespace,
		RunName:      run.Name,
		WorkItemID:   item.ID,
		WorkIdentity: workIdentity,
		Mode:         string(run.Spec.Mode),
		Source:       run.Spec.Source,
		State:        state,
		ObservedAt:   time.Now().UTC(),
		PR:           run.Status.PR,
	}

	callCtx, cancel := context.WithTimeout(ctx, executionObservationTimeout)
	defer cancel()
	err = observer.ReportExecution(callCtx, observation)
	if err == nil {
		r.obsMu.Lock()
		if r.obsLast == nil {
			r.obsLast = make(map[string]observationMarker)
		}
		r.obsLast[key] = observationMarker{uid: runUID, state: state}
		delete(r.obsRetry, key)
		r.obsMu.Unlock()
		return
	}
	if errors.Is(err, source.ErrUnsupportedExecutionObservations) {
		r.obsMu.Lock()
		alreadyDegraded := r.obsDegraded[run.Spec.Source]
		if !alreadyDegraded {
			if r.obsDegraded == nil {
				r.obsDegraded = make(map[string]bool)
			}
			r.obsDegraded[run.Spec.Source] = true
		}
		r.obsMu.Unlock()
		if !alreadyDegraded {
			// Latched for the process; re-probed by the next operator
			// process after a peer upgrade.
			log.FromContext(ctx).V(1).Info("execution observations degraded for source",
				"source", run.Spec.Source,
				"run", key)
		}
		return
	}
	r.obsMu.Lock()
	if r.obsRetry == nil {
		r.obsRetry = make(map[string]observationRetry)
	}
	r.obsRetry[key] = observationRetry{
		attempt:   observationMarker{uid: runUID, state: state},
		notBefore: time.Now().Add(executionObservationRetryDelay),
	}
	r.obsMu.Unlock()
	log.FromContext(ctx).V(1).Info("execution observation report failed; retrying after cooldown",
		"run", key, "state", string(state),
		"cooldown", executionObservationRetryDelay.String(), "error", err.Error())
}

// forgetExecutionObservation drops a run's execution-observation memory —
// last reported state and failure cooldown — so a recreated run with the same
// namespace/name re-reports its current state without inheriting a stale
// marker or a half-elapsed cooldown. A deleted run must leave no residue in
// process memory.
func (r *CoderRunReconciler) forgetExecutionObservation(name types.NamespacedName) {
	key := observationKey(name.Namespace, name.Name)
	r.obsMu.Lock()
	delete(r.obsLast, key)
	delete(r.obsRetry, key)
	r.obsMu.Unlock()
}
