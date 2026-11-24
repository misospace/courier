package controller

import (
	"testing"
	"time"

	"github.com/misospace/courier/internal/readapi"
)

// TestReadAPIReapMirrorPinsControllerValues pins the reap facts the operator
// read model mirrors from this package. The read model states a retained run's
// phase TTL, its expiry off the durable marker, and which marker is present, so
// a value that moves here and not there would make the page promise an expiry or
// a marker the reaper will not honor. The wiring passes the configured flag
// values (cmd/main.go), so the retention pin covers the fallback every
// projection uses when nothing overrides it.
func TestReadAPIReapMirrorPinsControllerValues(t *testing.T) {
	for _, tc := range []struct {
		name       string
		controller time.Duration
		readapi    time.Duration
	}{
		{"done retention", defaultDoneRetention, readapi.DefaultDoneRetention},
		{"failed retention", defaultFailedRetention, readapi.DefaultFailedRetention},
		{"needs-human retention", defaultNeedsHumanRetention, readapi.DefaultNeedsHumanRetention},
	} {
		if tc.controller != tc.readapi {
			t.Errorf("%s: controller %s, readapi %s", tc.name, tc.controller, tc.readapi)
		}
	}
	for _, tc := range []struct {
		name       string
		controller string
		readapi    string
	}{
		{"done-at marker", doneAtAnnotation, readapi.AnnotationDoneAt},
		{"failed-at marker", failedAtAnnotation, readapi.AnnotationFailedAt},
		{"needs-human-at marker", needsHumanAtAnnotation, readapi.AnnotationNeedsHumanAt},
		{"already-done-at marker", alreadyDoneAtAnnotation, readapi.AnnotationAlreadyDoneAt},
	} {
		if tc.controller != tc.readapi {
			t.Errorf("%s: controller %q, readapi %q", tc.name, tc.controller, tc.readapi)
		}
	}
}
