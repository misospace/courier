package controller

import (
	"context"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// LaunchFunc starts the coordinator for an admitted CoderRun.
//
// Keeping launch behind a small function makes admission independently
// testable. The production launcher can be added without changing the
// capacity accounting: the run is Claimed before launch and returned to
// Pending when launch fails.
type LaunchFunc func(context.Context, *courierv1alpha1.CoderRun) error

// phaseConsumesCapacity reports whether a run has been admitted to a lane.
// Claimed reserves capacity while the operator is launching the coordinator;
// Running continues to reserve it until the run reaches a terminal phase.
func phaseConsumesCapacity(phase courierv1alpha1.Phase) bool {
	return phase == courierv1alpha1.PhaseClaimed || phase == courierv1alpha1.PhaseRunning
}

// admittedCount returns the number of capacity-consuming runs on lane.
// Terminal and otherwise non-admitted phases intentionally do not count.
func admittedCount(runs []courierv1alpha1.CoderRun, lane string) int {
	count := 0
	for i := range runs {
		if runs[i].Spec.Lane == lane && phaseConsumesCapacity(runs[i].Status.Phase) {
			count++
		}
	}
	return count
}

// laneHasCapacity reports whether one more run can be admitted. A zero
// concurrency is treated as the API's default of one; malformed profiles
// should not strand all work indefinitely.
func laneHasCapacity(runs []courierv1alpha1.CoderRun, lane string, concurrency int) bool {
	if concurrency <= 0 {
		concurrency = 1
	}
	return admittedCount(runs, lane) < concurrency
}
