package main

import (
	"context"

	"github.com/misospace/courier/internal/devenv"
	couriergit "github.com/misospace/courier/internal/git"
)

// bootstrapReasonLimit bounds the redacted bootstrap receipt reason in the
// workspace.bootstrap event detail, reusing the executor's rune-truncation
// idiom so one receipt line can never grow without limit.
const bootstrapReasonLimit = 500

// bootstrapContractState classifies the checked-out contract against the
// operator-approved base revision through the shared devenv orchestration
// (#414): absent at head → StateAbsent; config plus a committed lock, both
// regular and byte-identical to base → StateApproved; anything unprovable,
// including a contract whose lock is absent → StateModified. Resolve and read
// errors return the zero State with the error: the caller skips bootstrap with
// a receipt reason and never executes content it cannot prove matches the
// approved base. The zero State is meaningless on its own; branch on the
// error first.
func bootstrapContractState(ctx context.Context, dir, baseRev, headRev string) (devenv.State, error) {
	baseOID, err := couriergit.ResolveCommit(ctx, dir, baseRev)
	if err != nil {
		return "", err
	}
	read := func(ctx context.Context, revision, path string) ([]byte, bool, bool, error) {
		return couriergit.BlobAtRegular(ctx, dir, revision, path)
	}
	return devenv.Classify(ctx, read, baseOID, headRev)
}

// approvedContractSkipReason is the workspace.bootstrap receipt reason for an
// approved contract: the legacy executor is credential-bearing and never
// executes repository contract content, so the verdict is always a skip —
// but the receipt names whether the runtime environment could provision the
// contract's tools at all, so an operator selecting an unprovisioned image
// sees the missing mise executable before model work rather than a mid-run
// tool hunt.
func approvedContractSkipReason(misePresent bool) string {
	reason := "bootstrap contract approved but not executed: the legacy executor is credential-bearing; the approved bootstrap runs only in the secure worker topology"
	if !misePresent {
		reason += "; the runtime environment has no mise executable on PATH"
	}
	return reason
}
