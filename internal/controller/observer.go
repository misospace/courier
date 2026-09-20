package controller

import "context"

// CheckObservation is the controller-neutral result of one CI check.
type CheckObservation struct {
	Conclusion string
}

// PRObservation is the controller-neutral state of a pull request and its CI.
type PRObservation struct {
	PR     string
	Draft  bool
	Checks []CheckObservation
}

// WorldObserver reads the pull request and CI state for a completed run.
type WorldObserver interface {
	Observe(context.Context, string, string) (PRObservation, error)
}

func observationReady(observation PRObservation) bool {
	if observation.PR == "" || observation.Draft || len(observation.Checks) == 0 {
		return false
	}
	for _, check := range observation.Checks {
		switch check.Conclusion {
		case "success", "skipped", "neutral":
		default:
			return false
		}
	}
	return true
}
