package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
)

type CheckState string

const (
	CheckStatePending CheckState = "pending"
	CheckStatePassed  CheckState = "passed"
	CheckStateFailed  CheckState = "failed"
)

type CheckObservation struct {
	// Name is the stable external identifier of the check (a check-run name
	// or a commit-status context). Registration is asynchronous, so identity
	// — not state — is what a settle decision can be built on.
	Name  string
	State CheckState
}

type PRObservation struct {
	PR    string
	Draft bool
	// Head is the source's identifier for the commit under observation. A new
	// push re-registers checks under the same names, so the fingerprint treats
	// a different head as a different check set.
	Head   string
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
	if observation.PR == "" || observation.Draft {
		return observationNeedsHuman
	}
	if len(observation.Checks) == 0 {
		return observationPending
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

// checkSetFingerprint is the compact, stable identity of an observed check
// set: the head commit plus the sorted external check identifiers. It is
// deliberately independent of check state, which changes as checks complete,
// and of completion timestamps, which change on every read. An observation
// with no checks carries no identity and yields an empty fingerprint.
func checkSetFingerprint(observation PRObservation) string {
	if len(observation.Checks) == 0 {
		return ""
	}
	names := make([]string, 0, len(observation.Checks))
	for _, check := range observation.Checks {
		names = append(names, check.Name)
	}
	slices.Sort(names)
	digest := sha256.Sum256([]byte(observation.Head + "\x00" + strings.Join(names, "\x00")))
	return hex.EncodeToString(digest[:])
}
