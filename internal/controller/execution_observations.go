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
// attempt. Each attempt carries its own deadline so a slow or wedged source
// endpoint can never block the reconcile loop: telemetry must not stall
// reconciliation.
const executionObservationTimeout = 10 * time.Second

// observationKey is the process-local identity of a run in the
// execution-observation maps: "<namespace>/<name>".
func observationKey(namespace, name string) string {
	return namespace + "/" + name
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
//     never returns an error and never requeues.
//   - It infers no model progress beyond the run's own phases.
//   - Observations carry no secrets, prompts, or work output.
//   - Telemetry never claims source ownership: it carries no claim receipts,
//     fence tokens, or lease proof.
//
// Reporting is state-change only, per process: each run reports a state once,
// and a process restart re-reconciles every run and re-publishes the current
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
	r.obsMu.Lock()
	if r.obsDegraded[run.Spec.Source] || r.obsLast[key] == state {
		r.obsMu.Unlock()
		return
	}
	r.obsMu.Unlock()

	workIdentity := item.ID
	if ident, ok := adapter.(source.WorkIdentifier); ok {
		workIdentity = ident.WorkIdentity(item.ID)
	}
	observation := source.ExecutionObservation{
		RunUID:       string(run.UID),
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
			r.obsLast = make(map[string]source.ExecutionState)
		}
		r.obsLast[key] = state
		r.obsMu.Unlock()
		return
	}
	if errors.Is(err, source.ErrUnsupportedExecutionObservations) {
		r.obsMu.Lock()
		first := r.obsDegraded[run.Spec.Source]
		if !first {
			if r.obsDegraded == nil {
				r.obsDegraded = make(map[string]bool)
			}
			r.obsDegraded[run.Spec.Source] = true
		}
		r.obsMu.Unlock()
		if !first {
			// Latched for the process; re-probed by the next operator
			// process after a peer upgrade.
			log.FromContext(ctx).V(1).Info("execution observations degraded for source",
				"source", run.Spec.Source,
				"run", key)
		}
		return
	}
	log.FromContext(ctx).V(1).Info("execution observation report failed",
		"run", key, "state", string(state), "error", err.Error())
}

// forgetExecutionObservation drops a run's last-reported execution state so a
// recreated run with the same namespace/name re-reports its current state.
// A deleted run must leave no residue in process memory.
func (r *CoderRunReconciler) forgetExecutionObservation(name types.NamespacedName) {
	r.obsMu.Lock()
	delete(r.obsLast, observationKey(name.Namespace, name.Name))
	r.obsMu.Unlock()
}
