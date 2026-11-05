package harness

import (
	"errors"
	"fmt"
	"sync"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/protocol"
)

// prerequisiteClass is the bounded, code-owned vocabulary of prerequisite
// failures for model-directed work. It names what trusted control observed
// about the workspace or about its own trusted status path (#741), never
// what the model said about it, and it never carries worker
// output: every class is a fact the harness itself witnessed.
type prerequisiteClass string

const (
	// prerequisiteRefused: the trusted world or the worker definitively
	// refused the preparation. A refusal is final — an immediate retry
	// cannot change it.
	prerequisiteRefused prerequisiteClass = "refused"
	// prerequisiteMismatch: the materialized workspace does not hold the
	// snapshot control dispatched. Nothing may be dispatched against it.
	prerequisiteMismatch prerequisiteClass = "mismatch"
	// prerequisiteTaskFailed: the fixed preparation task ran and failed.
	// One failure may be a legitimate transient, so it is observed but
	// never decisive on its own.
	prerequisiteTaskFailed prerequisiteClass = "task_failed"
	// prerequisiteLocalFailure: the broker call that prepares the workspace
	// failed without the trusted world stating anything — a local
	// construction or decoding failure, or a response that is no API
	// answer. The failure is usually permanent, but recurrence — not a
	// single observation — establishes it, and the ending is an
	// infrastructure failure: the world never refused, so the condition is
	// never blocked-external and never a refusal (#744).
	prerequisiteLocalFailure prerequisiteClass = "local_failure"
	// prerequisiteStatusRefused: the run's trusted status path definitively
	// refused to persist the operation's status record, so the operation
	// never dispatched. A recording failure of trusted control's own
	// plumbing is not a workspace condition and not an external prerequisite
	// the world could still satisfy, so the ending is an infrastructure
	// failure, never blocked-external (#741). A retryable status-path
	// failure — the broker momentarily unreachable — is not this class: it
	// is transient and still retries through the ordinary tool error.
	prerequisiteStatusRefused prerequisiteClass = "status_refused"
	// prerequisiteFenced: the worker is fenced — no further workspace
	// operation may run against it, whatever the model asks for.
	prerequisiteFenced prerequisiteClass = "fenced"
)

// The fixed reason texts every prerequisite failure carries. They are
// single-line, code-owned, and never an excerpt of untrusted worker output:
// the only variable part is the fixed task's own exit status, a number the
// worker reported about its own execution.
const (
	prerequisiteRefusedReason       = "the workspace snapshot prerequisite was refused by the trusted world"
	prerequisiteMismatchReason      = "the worker workspace does not hold the dispatched snapshot"
	prerequisiteFencedReason        = "the worker is fenced: no further workspace operation may run against it"
	prerequisiteTaskFailedFormat    = "the fixed workspace preparation task failed with exit code %d"
	prerequisiteLocalFailReason     = "the workspace snapshot could not be prepared: the broker call failed locally and the failure recurred"
	prerequisiteStatusRefusedReason = "the trusted status path refused the operation's status record; no operation can be dispatched"
)

// prerequisiteFailureTripCount is the number of stable preparation failures
// that establish a condition for the task-failed class: one failure may be a
// transient the next attempt clears, so recurrence — not a single failure —
// is what makes it terminal.
const prerequisiteFailureTripCount = 2

// prerequisiteFailure is a typed workspace-prerequisite failure: the class
// trusted control observed, the fixed reason the run ends with, and the
// terminal kind that reason implies.
type prerequisiteFailure struct {
	class  prerequisiteClass
	reason string
	kind   courierv1alpha1.NativeResultKind
}

func (f *prerequisiteFailure) Error() string { return f.reason }

// refusedPrerequisite is the refusal class: the trusted world or the worker
// definitively refused the preparation.
func refusedPrerequisite() *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteRefused,
		reason: prerequisiteRefusedReason,
		kind:   courierv1alpha1.NativeResultBlockedExternal,
	}
}

// mismatchedPrerequisite is the mismatch class: the worker workspace does
// not hold the snapshot control dispatched.
func mismatchedPrerequisite() *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteMismatch,
		reason: prerequisiteMismatchReason,
		kind:   courierv1alpha1.NativeResultBlockedExternal,
	}
}

// failedPrerequisiteTask is the task-failed class: the fixed preparation task
// ran and failed with its own exit status.
func failedPrerequisiteTask(exitCode int) *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteTaskFailed,
		reason: fmt.Sprintf(prerequisiteTaskFailedFormat, exitCode),
		kind:   courierv1alpha1.NativeResultBlockedExternal,
	}
}

// localBrokerFailure is the local-failure class: the broker call that
// prepares the workspace failed without the trusted world stating a denial.
// The honest recovery is operator relaunch once the run's own broker
// plumbing is fixed, so the ending is an infrastructure failure — never a
// refusal, and never the blocked-external wait on an external prerequisite
// the world could still satisfy (#744).
func localBrokerFailure() *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteLocalFailure,
		reason: prerequisiteLocalFailReason,
		kind:   courierv1alpha1.NativeResultInfrastructureFailed,
	}
}

// fencedPrerequisite is the fenced class: the worker may still be running
// repository-authored work, so no further workspace operation may run against
// it. The honest recovery is worker replacement, not another turn.
func fencedPrerequisite() *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteFenced,
		reason: prerequisiteFencedReason,
		kind:   courierv1alpha1.NativeResultInfrastructureFailed,
	}
}

// statusPathRefused is the status-path class: the run's trusted status path
// definitively refused to persist the operation's status record, so the
// operation never dispatched. The ending is an infrastructure failure — a
// recording failure of trusted control's own plumbing is never the
// blocked-external wait on an external prerequisite the world could still
// satisfy (#741); the honest recovery is operator relaunch once the run's
// own status plumbing is fixed.
func statusPathRefused() *prerequisiteFailure {
	return &prerequisiteFailure{
		class:  prerequisiteStatusRefused,
		reason: prerequisiteStatusRefusedReason,
		kind:   courierv1alpha1.NativeResultInfrastructureFailed,
	}
}

// workerFence is the sticky worker fence (#414 review): once an operation's
// termination could not be observed, no further dispatch may run against that
// worker. It is a typed error so the prerequisite classifier can name it as
// the fence it is, while its text stays exactly what the run's handoff has
// always carried.
type workerFence struct {
	reason string
}

func (f *workerFence) Error() string { return f.reason }

// classifyPrerequisite maps one preparation failure to the prerequisite
// class it establishes, or nil when it establishes none. The order is the
// contract: a typed failure is itself, the #414 bootstrap blocker owns its
// own case and is never double-handled, the fence is final, an unverified
// termination is not a prerequisite failure in its own right (the fence it
// set is), a definite status-path refusal is a recording failure of trusted
// control's own plumbing — its own class, an infrastructure failure, never a
// workspace refusal (#741) — while a retryable status-path failure and a
// retryable broker failure are transient, a broker
// error that positively states a denial is a refusal while a non-retryable
// broker error that states no denial is a local failure — a construction or
// decoding failure, or a response that is no API answer, that the trusted
// world never refused — established by recurrence like a task failure, a
// definite worker rejection is a refusal, and anything unclassified never
// trips the breaker.
//
// The unverified check precedes the refusal checks on purpose: an unverified
// termination can carry the worker's own rejection inside its chain, and a
// termination the harness could not verify is never a refusal — the run must
// fail closed, not call the world done.
func classifyPrerequisite(err error) *prerequisiteFailure {
	if err == nil {
		return nil
	}
	var failure *prerequisiteFailure
	if errors.As(err, &failure) {
		return failure
	}
	var blocker *bootstrapBlocker
	if errors.As(err, &blocker) {
		return nil
	}
	var fence *workerFence
	if errors.As(err, &fence) {
		return fencedPrerequisite()
	}
	if errors.Is(err, ErrTerminationUnverified) {
		return nil
	}
	var persist *statusPersistFailure
	if errors.As(err, &persist) {
		// A definite status-path refusal ends the run (#741): the operation
		// never dispatched, nothing a model turn can change will make the
		// next write land, and a recording failure of trusted control's own
		// plumbing is an infrastructure failure, never the blocked-external
		// wait on an external prerequisite. A retryable status-path failure
		// — the broker momentarily unreachable — is transient: it keeps the
		// ordinary tool-error path, so the model's legitimate retry stands.
		if isBrokerRetryable(persist.err) {
			return nil
		}
		return statusPathRefused()
	}
	var callErr *BrokerCallError
	if errors.As(err, &callErr) {
		if callErr.Retryable {
			return nil
		}
		// A refusal is a fact the broker stated, never one control inferred:
		// a non-retryable error that states a definite denial — the same 4xx
		// rule the worker side applies — is the broker's own refusal, while
		// a non-retryable error that states no denial is a local
		// construction or decoding failure — an unreadable CA, a malformed
		// request or response, an oversized snapshot — or a response that is
		// no API answer: the trusted world never refused (#737 review). The
		// failure is usually permanent, so it is observed as the
		// local-failure class and the latch decides whether it has become a
		// condition (#744): the ending after recurrence is an infrastructure
		// failure, never a refusal of the broker's world-state.
		if definiteWorkerRejectionStatus(callErr.StatusCode) {
			return refusedPrerequisite()
		}
		return localBrokerFailure()
	}
	var transportErr *protocol.TransportError
	if errors.As(err, &transportErr) {
		if definiteWorkerRejectionStatus(transportErr.StatusCode) {
			return refusedPrerequisite()
		}
		return nil
	}
	return nil
}

// prerequisiteLatch is the incarnation's circuit breaker for workspace
// prerequisites. It counts the stable preparation failures the incarnation
// observed and trips once trusted control knows the condition cannot clear:
// a refusal, a mismatch, a fence, or a definite status-path refusal is one
// observation enough, because each is a final fact; recurrence — across any
// class, so alternating classes
// cannot evade it — establishes the task-failed and local-failure classes.
// The latch never un-trips and never forgets a trip.
type prerequisiteLatch struct {
	mu       sync.Mutex
	failures int
	trip     *prerequisiteFailure
	consumed bool
}

// observe records one stable preparation failure. A nil failure is not a
// failure at all and changes nothing.
func (l *prerequisiteLatch) observe(failure *prerequisiteFailure) {
	if failure == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failures++
	if l.trip != nil {
		return
	}
	switch failure.class {
	case prerequisiteRefused, prerequisiteMismatch, prerequisiteFenced, prerequisiteStatusRefused:
		l.trip = failure
	default:
		if l.failures >= prerequisiteFailureTripCount {
			l.trip = failure
		}
	}
}

// peek reports the trip without consuming it: a sub-agent may look, but only
// the coordinator's own loop ends the run.
func (l *prerequisiteLatch) peek() *prerequisiteFailure {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.trip
}

// take hands the trip to the one consumer that ends the run. The trip itself
// is never forgotten — peek keeps reporting it — but only the first
// consumer takes it. A latch that has not tripped yet is not consumed: the
// trip may still arrive.
func (l *prerequisiteLatch) take() *prerequisiteFailure {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.trip == nil || l.consumed {
		return nil
	}
	l.consumed = true
	return l.trip
}
