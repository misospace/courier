package controller

import "testing"

// TestObserveStateCheckStates pins the settle-path aggregation of check
// states, including that an unrecognized CheckStateUnknown fails the
// observation closed like any non-passed state rather than counting as
// pending or passing.
func TestObserveStateCheckStates(t *testing.T) {
	cases := []struct {
		name   string
		checks []CheckObservation
		want   observationState
	}{
		{
			name:   "empty check set is pending",
			checks: nil,
			want:   observationPending,
		},
		{
			name:   "all passed passes",
			checks: []CheckObservation{{Name: "lint", State: CheckStatePassed}},
			want:   observationPassed,
		},
		{
			name:   "failed check fails",
			checks: []CheckObservation{{Name: "lint", State: CheckStateFailed}},
			want:   observationFailed,
		},
		{
			name: "unknown check state fails closed",
			checks: []CheckObservation{
				{Name: "lint", State: CheckStatePassed},
				{Name: "mystery", State: CheckStateUnknown},
			},
			want: observationFailed,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			observation := PRObservation{PR: "42", Head: "head1", Checks: tc.checks}
			if got := observeState(observation); got != tc.want {
				t.Fatalf("observeState() = %q, want %q", got, tc.want)
			}
		})
	}
}
