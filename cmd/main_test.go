package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/source/dispatch"
)

type observerWithHeadResolver struct{}

func (observerWithHeadResolver) Observe(context.Context, string, controller.HeadRef) (controller.PRObservation, error) {
	return controller.PRObservation{}, nil
}

func (observerWithHeadResolver) ResolveHead(context.Context, *courierv1alpha1.CoderRun) (controller.HeadRef, error) {
	return controller.HeadRef{Repo: "acme/demo", Branch: "feature/existing"}, nil
}

func TestDispatchPRStateCheckerRequiresGitHubObserver(t *testing.T) {
	if _, err := dispatchPRStateChecker(nil); !errors.Is(err, dispatch.ErrPRStateCheckerNeeded) {
		t.Fatalf("dispatchPRStateChecker() error = %v, want checker-required", err)
	}
}

func TestExistingPRHeadResolver(t *testing.T) {
	observer := observerWithHeadResolver{}
	if got := existingPRHeadResolver(observer); got == nil {
		t.Fatal("existingPRHeadResolver() returned nil for a compatible observer")
	}
	if got := existingPRHeadResolver(nil); got != nil {
		t.Fatal("existingPRHeadResolver(nil) returned a resolver")
	}
}

func TestResolveDispatchBindings(t *testing.T) {
	tests := []struct {
		name      string
		queueLane string
		lane      string
		repeated  []string
		want      []dispatchBinding
		wantErr   bool
	}{
		{
			name:      "shorthand only",
			queueLane: "queue-a",
			lane:      "lane-a",
			want:      []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}},
		},
		{
			name:     "repeated only",
			repeated: []string{"queue-a:lane-a", "queue-b:lane-b"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}, {queueLane: "queue-b", laneProfile: "lane-b"}},
		},
		{
			name:      "shorthand and repeated mix rejected",
			queueLane: "queue-a",
			lane:      "lane-a",
			repeated:  []string{"queue-b:lane-b"},
			wantErr:   true,
		},
		{
			name:      "queue lane only rejected",
			queueLane: "queue-a",
			wantErr:   true,
		},
		{
			name:    "lane profile only rejected",
			lane:    "lane-a",
			wantErr: true,
		},
		{
			name:     "repeated without colon rejected",
			repeated: []string{"queue-a"},
			wantErr:  true,
		},
		{
			name:     "repeated empty half rejected",
			repeated: []string{"queue-a:"},
			wantErr:  true,
		},
		{
			name:     "duplicate queue lane rejected",
			repeated: []string{"queue-a:lane-a", "queue-a:lane-b"},
			wantErr:  true,
		},
		{
			name:     "duplicate lane profile accepted",
			repeated: []string{"queue-a:lane-x", "queue-b:lane-x"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-x"}, {queueLane: "queue-b", laneProfile: "lane-x"}},
		},
		{
			name:     "whitespace trimmed",
			repeated: []string{" queue-a : lane-a "},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane-a"}},
		},
		{
			name:     "repeated whitespace-only rejected",
			repeated: []string{" "},
			wantErr:  true,
		},
		{
			name:     "repeated colon in lane profile kept verbatim",
			repeated: []string{"queue-a:lane:a"},
			want:     []dispatchBinding{{queueLane: "queue-a", laneProfile: "lane:a"}},
		},
		{
			name:      "whitespace-only shorthand rejected",
			queueLane: " ",
			wantErr:   true,
		},
		{
			name: "all empty",
			want: []dispatchBinding{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveDispatchBindings(test.queueLane, test.lane, test.repeated)
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolveDispatchBindings() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDispatchBindings() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("resolveDispatchBindings() = %#v, want %#v", got, test.want)
			}
		})
	}
}
