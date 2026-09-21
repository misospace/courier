package controller

import "context"

type CheckState string

const (
	CheckStatePending CheckState = "pending"
	CheckStatePassed  CheckState = "passed"
	CheckStateFailed  CheckState = "failed"
)

type CheckObservation struct {
	State CheckState
}

type PRObservation struct {
	PR     string
	Draft  bool
	Checks []CheckObservation
}

// WorldObserver reads the pull request and CI state for a completed run.
type WorldObserver interface {
	Observe(context.Context, string, string) (PRObservation, error)
}

type observationState string

const (
	observationNeedsHuman observationState = "needs-human"
	observationPending    observationState = "pending"
	observationPassed     observationState = "passed"
	observationFailed     observationState = "failed"
)

func observeState(observation PRObservation) observationState {
	if observation.PR == "" || observation.Draft || len(observation.Checks) == 0 {
		return observationNeedsHuman
	}
	for _, check := range observation.Checks {
		if check.State == CheckStatePending {
			return observationPending
		}
	}
	for _, check := range observation.Checks {
		if check.State != CheckStatePassed {
			return observationFailed
		}
	}
	return observationPassed
}
