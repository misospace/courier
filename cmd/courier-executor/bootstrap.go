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
