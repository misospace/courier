package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	courierlog "github.com/misospace/courier/internal/log"
	"github.com/misospace/courier/internal/source"
)

func TestReconcilerEmitsRunScopedPhaseTransitionEvents(t *testing.T) {
	passedObserver := fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStatePassed}}}}
	failedObserver := fakeWorldObserver{observation: PRObservation{PR: "42", Checks: []CheckObservation{{State: CheckStateFailed}}}}
	tests := []struct {
		name     string
		phase    courierv1alpha1.Phase
		objects  func(run *courierv1alpha1.CoderRun) []runtime.Object
		observer *fakeWorldObserver
		launch   bool
		// settledGreen presets status.checkFingerprint so the single
		// all-green observation matches the prior candidate and settles
		// immediately, per the consecutive-green AwaitingReview rule.
		settledGreen bool
		wantPhase    courierv1alpha1.Phase
	}{
		{
			name:      "admission transitions to Running",
			phase:     courierv1alpha1.PhasePending,
			launch:    true,
			wantPhase: courierv1alpha1.PhaseRunning,
		},
		{
			name:      "successful coordinator exit transitions to Verifying",
			phase:     courierv1alpha1.PhaseRunning,
			objects:   func(run *courierv1alpha1.CoderRun) []runtime.Object { return []runtime.Object{coordinatorPod(run, 0)} },
			wantPhase: courierv1alpha1.PhaseVerifying,
		},
		{
			name:      "failed coordinator exit transitions to Failed",
			phase:     courierv1alpha1.PhaseRunning,
			objects:   func(run *courierv1alpha1.CoderRun) []runtime.Object { return []runtime.Object{coordinatorPod(run, 17)} },
			wantPhase: courierv1alpha1.PhaseFailed,
		},
		{
			name:         "checks passed transitions to AwaitingReview",
			phase:        courierv1alpha1.PhaseVerifying,
			observer:     &passedObserver,
			settledGreen: true,
			wantPhase:    courierv1alpha1.PhaseAwaitingReview,
		},
		{
			name:      "failed checks transition to NeedsHuman",
			phase:     courierv1alpha1.PhaseVerifying,
			observer:  &failedObserver,
			wantPhase: courierv1alpha1.PhaseNeedsHuman,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := admissionRun("run", "local", tt.phase)
			if tt.settledGreen {
				run.Status.CheckFingerprint = checkSetFingerprint(tt.observer.observation)
			}
			var objects []runtime.Object
			if tt.objects != nil {
				objects = tt.objects(run)
			}
			client := phaseClient(t, append([]runtime.Object{admissionLane("local", 1), run}, objects...)...)
			var eventsOut bytes.Buffer
			reconciler := &CoderRunReconciler{
				Client:       client,
				Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
				StatusWriter: fakeStatusWriter{client: client},
				Events:       courierlog.NewEmitter(&eventsOut, courierlog.LevelInfo),
			}
			if tt.observer != nil {
				reconciler.Observer = *tt.observer
			}
			if tt.launch {
				reconciler.Launch = func(context.Context, *courierv1alpha1.CoderRun) error { return nil }
			}
			if _, err := reconciler.Reconcile(context.Background(), admissionRequest("run")); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			var events []map[string]any
			for i, line := range strings.Split(strings.TrimSpace(eventsOut.String()), "\n") {
				if line == "" {
					continue
				}
				var event map[string]any
				if err := json.Unmarshal([]byte(line), &event); err != nil {
					t.Fatalf("line %d is not valid JSON", i+1)
				}
				if event["run_id"] != "run" {
					t.Fatalf("line %d is missing run scope", i+1)
				}
				events = append(events, event)
			}
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want exactly one phase transition", len(events))
			}
			event := events[0]
			if event["event"] != courierlog.EventPhaseTransition {
				t.Fatalf("event type = %v, want %v", event["event"], courierlog.EventPhaseTransition)
			}
			if event["status"] != string(tt.wantPhase) {
				t.Fatalf("transition status = %v, want %v", event["status"], tt.wantPhase)
			}
			if event["repo"] != "acme/widgets" || event["ref"] != float64(1) || event["mode"] != "resolve-issue" {
				t.Fatalf("event lost repo/ref/mode metadata: %v", event)
			}
		})
	}
}

// TestPhaseTransitionDetailFollowsPerRunDebug proves verbosity is a per-run
// property, not a process-global one: one shared reconciler and emitter
// serves a quiet run and a debug run, and only the debug run's event carries
// detail.
func TestPhaseTransitionDetailFollowsPerRunDebug(t *testing.T) {
	quiet := admissionRun("run-quiet", "local", courierv1alpha1.PhasePending)
	loud := admissionRun("run-loud", "local", courierv1alpha1.PhasePending)
	loud.Spec.Debug = true
	client := phaseClient(t, admissionLane("local", 2), quiet, loud)
	var eventsOut bytes.Buffer
	reconciler := &CoderRunReconciler{
		Client:       client,
		Sources:      NewSourceRegistry(map[string]source.Adapter{"test": &admissionSource{}}),
		StatusWriter: fakeStatusWriter{client: client},
		Events:       courierlog.NewEmitter(&eventsOut, courierlog.LevelInfo),
		Launch:       func(context.Context, *courierv1alpha1.CoderRun) error { return nil },
	}
	for _, name := range []string{"run-quiet", "run-loud"} {
		if _, err := reconciler.Reconcile(context.Background(), admissionRequest(name)); err != nil {
			t.Fatalf("Reconcile(%s) error = %v", name, err)
		}
	}

	events := map[string]map[string]any{}
	for i, line := range strings.Split(strings.TrimSpace(eventsOut.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d is not valid JSON", i+1)
		}
		runID, _ := event["run_id"].(string)
		if _, dup := events[runID]; dup {
			t.Fatalf("run %s emitted more than one transition", runID)
		}
		events[runID] = event
	}
	if len(events) != 2 {
		t.Fatalf("emitted %d events, want one per run", len(events))
	}

	quietEvent := events["run-quiet"]
	if quietEvent == nil || quietEvent["event"] != courierlog.EventPhaseTransition || quietEvent["status"] != string(courierv1alpha1.PhaseRunning) {
		t.Fatalf("quiet run event = %v, want a Running phase.transition", quietEvent)
	}
	if _, ok := quietEvent["detail"]; ok {
		t.Fatal("debug=false run must not carry event detail")
	}
	loudEvent := events["run-loud"]
	if loudEvent == nil || loudEvent["status"] != string(courierv1alpha1.PhaseRunning) {
		t.Fatalf("debug run event = %v, want a Running phase.transition", loudEvent)
	}
	detail, ok := loudEvent["detail"].(map[string]any)
	if !ok {
		t.Fatal("debug=true run must carry event detail")
	}
	if detail["branch"] == "" || detail["lane"] != "local" {
		t.Fatalf("debug run detail = %v, want branch and lane", detail)
	}
}
