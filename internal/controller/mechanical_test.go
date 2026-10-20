package controller

import "testing"

func mechanicalObservation(head string, checks ...CheckObservation) PRObservation {
	return PRObservation{
		PR:     "42",
		Head:   head,
		Checks: checks,
	}
}

// TestClassifyCIParkTrigger covers the strict trigger-separation decision
// table for a parked run's observation (#475).
func TestClassifyCIParkTrigger(t *testing.T) {
	mechanical := func(name string) bool {
		switch name {
		case "lint", "unit":
			return true
		default:
			return false
		}
	}
	cases := []struct {
		name        string
		observation PRObservation
		parkedHead  string
		selector    MechanicalCheckSelector
		repairsLeft int
		want        CIParkTrigger
	}{
		{
			name: "merged observation finishes without repair",
			observation: func() PRObservation {
				obs := mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStateFailed})
				obs.Merged = true
				return obs
			}(),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name:        "missing pull request finishes",
			observation: PRObservation{Head: "head1", Checks: []CheckObservation{{Name: "lint", State: CheckStateFailed}}},
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "draft pull request finishes",
			observation: func() PRObservation {
				obs := mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStateFailed})
				obs.Draft = true
				return obs
			}(),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name:        "empty parked head fails closed",
			observation: mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStateFailed}),
			parkedHead:  "",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name:        "superseded head finishes instead of repairing",
			observation: mechanicalObservation("head2", CheckObservation{Name: "lint", State: CheckStateFailed}),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name:        "empty check set holds",
			observation: mechanicalObservation("head1"),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkHold,
		},
		{
			name: "pending mechanical check holds",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStatePending},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkHold,
		},
		{
			name: "mechanical failure beside pending non-mechanical check resumes repair",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStateFailed},
				CheckObservation{Name: "ai-review", State: CheckStatePending},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkResumeRepair,
		},
		{
			name: "pending non-mechanical check alone finishes",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "ai-review", State: CheckStatePending},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "unknown check state finishes despite mechanical failure",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStateFailed},
				CheckObservation{Name: "mystery", State: CheckState("unknown")},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "unknown check state finishes ahead of pending mechanical check",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStatePending},
				CheckObservation{Name: "mystery", State: CheckState("unknown")},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "non-mechanical failure finishes without waking the coordinator",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "ai-review", State: CheckStateFailed},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "mixed failures finish when any failed check is not mechanical",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStateFailed},
				CheckObservation{Name: "ai-review", State: CheckStateFailed},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "all-mechanical failures resume repair with budget left",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStateFailed},
				CheckObservation{Name: "unit", State: CheckStateFailed},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkResumeRepair,
		},
		{
			name:        "all-mechanical failure with zero budget finishes",
			observation: mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStateFailed}),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 0,
			want:        CIParkFinish,
		},
		{
			name:        "all-mechanical failure with negative budget finishes",
			observation: mechanicalObservation("head1", CheckObservation{Name: "unit", State: CheckStateFailed}),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: -2,
			want:        CIParkFinish,
		},
		{
			name:        "nil selector refuses a mechanical failure too",
			observation: mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStateFailed}),
			parkedHead:  "head1",
			selector:    nil,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "nil selector finishes on pending checks because nothing is configured mechanical",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStatePending},
			),
			parkedHead:  "head1",
			selector:    nil,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name:        "nil selector holds on empty checks",
			observation: mechanicalObservation("head1"),
			parkedHead:  "head1",
			selector:    nil,
			repairsLeft: 3,
			want:        CIParkHold,
		},
		{
			name:        "all green finishes",
			observation: mechanicalObservation("head1", CheckObservation{Name: "lint", State: CheckStatePassed}),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
		{
			name: "passed beside non-mechanical failure finishes",
			observation: mechanicalObservation("head1",
				CheckObservation{Name: "lint", State: CheckStatePassed},
				CheckObservation{Name: "ai-review", State: CheckStateFailed},
			),
			parkedHead:  "head1",
			selector:    mechanical,
			repairsLeft: 3,
			want:        CIParkFinish,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyCIParkTrigger(tc.observation, tc.parkedHead, tc.selector, tc.repairsLeft)
			if got != tc.want {
				t.Fatalf("ClassifyCIParkTrigger() = %q, want %q", got, tc.want)
			}
		})
	}
}
