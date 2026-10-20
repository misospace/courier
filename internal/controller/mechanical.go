package controller

// CIParkTrigger is the strict trigger-separation verdict for a run parked on
// CI observation (#475).
type CIParkTrigger string

const (
	// CIParkHold keeps the run parked: checks are pending or not yet
	// registered, so no decision is possible yet. The caller owns the
	// bounded external observation window that eventually ends the hold.
	CIParkHold CIParkTrigger = "hold"
	// CIParkResumeRepair wakes the coordinator for one bounded repair.
	CIParkResumeRepair CIParkTrigger = "resume-repair"
	// CIParkFinish ends the parked attempt without waking the coordinator:
	// mechanical green, a non-mechanical failure, an exhausted budget, an
	// unobservable pull request, or a superseded head all finish rather
	// than repair.
	CIParkFinish CIParkTrigger = "finish"
)

// MechanicalCheckSelector reports whether one observed CI check is
// explicitly configured as mechanical. Only checks it accepts can ever wake
// a parked coordinator; AI-review workflows, human review state, and merge
// state are never selectable because they never reach this predicate — the
// classification below refuses any failed check the selector does not
// accept.
type MechanicalCheckSelector func(name string) bool

// ClassifyCIParkTrigger applies strict trigger separation to one observation
// of the parked head: only configured mechanical checks failing on the exact
// parked head, with repair budget remaining, may wake the coordinator.
// parkedHead is the recorded park head; repairsRemaining is the operator's
// remaining budget. Never re-reads world state and never writes: the caller
// re-reads fresh state and fails closed on any contradiction between calls.
func ClassifyCIParkTrigger(observation PRObservation, parkedHead string, selector MechanicalCheckSelector, repairsRemaining int) CIParkTrigger {
	if observation.Merged {
		return CIParkFinish
	}
	if observation.PR == "" || observation.Draft {
		return CIParkFinish
	}
	if parkedHead == "" {
		return CIParkFinish
	}
	if observation.Head != parkedHead {
		return CIParkFinish
	}
	if len(observation.Checks) == 0 {
		return CIParkHold
	}
	hasFailed := false
	for _, check := range observation.Checks {
		if check.State == CheckStatePending {
			return CIParkHold
		}
		if check.State == CheckStateFailed {
			hasFailed = true
		}
	}
	if !hasFailed {
		return CIParkFinish
	}
	for _, check := range observation.Checks {
		if check.State == CheckStateFailed && (selector == nil || !selector(check.Name)) {
			return CIParkFinish
		}
	}
	if repairsRemaining <= 0 {
		return CIParkFinish
	}
	return CIParkResumeRepair
}
