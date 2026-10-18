package main

import (
	"context"
	"os"
	"strings"

	"github.com/misospace/courier/internal/devenv"
	couriergit "github.com/misospace/courier/internal/git"
)

// bootstrapReasonLimit bounds the redacted bootstrap receipt reason in the
// workspace.bootstrap event detail, reusing the executor's rune-truncation
// idiom so one receipt line can never grow without limit.
const bootstrapReasonLimit = 500

// contractFilesAt reads the bootstrap contract files (.mise/config.toml and
// .mise/mise.lock) at one revision through the hardened git read. A read
// failure is NOT folded into absence: it returns an error the caller treats
// as "state unreadable" and skips bootstrap conservatively (#414).
func contractFilesAt(ctx context.Context, dir, revision string) (devenv.Files, error) {
	config, hasConfig, err := couriergit.BlobAt(ctx, dir, revision, devenv.ConfigPath)
	if err != nil {
		return devenv.Files{}, err
	}
	lock, hasLock, err := couriergit.BlobAt(ctx, dir, revision, devenv.LockPath)
	if err != nil {
		return devenv.Files{}, err
	}
	return devenv.Files{Config: config, Lock: lock, HasConfig: hasConfig, HasLock: hasLock}, nil
}

// bootstrapContractState classifies the checked-out contract against the
// operator-approved base revision (#414): absent at head → StateAbsent; equal
// to base → StateApproved; differing → StateModified. Resolve and read errors
// return the zero State with the error: the caller skips bootstrap with a
// receipt reason and never executes content it cannot prove matches the
// approved base. The zero State is meaningless on its own; branch on the
// error first.
func bootstrapContractState(ctx context.Context, dir, baseRev, headRev string) (devenv.State, error) {
	baseOID, err := couriergit.ResolveCommit(ctx, dir, baseRev)
	if err != nil {
		return "", err
	}
	head, err := contractFilesAt(ctx, dir, headRev)
	if err != nil {
		return "", err
	}
	base, err := contractFilesAt(ctx, dir, baseOID)
	if err != nil {
		return "", err
	}
	return devenv.Compare(base, head), nil
}

// bootstrapBlockerTermination maps a failed bootstrap execution to the run's
// blocked-external NeedsHuman handoff (#414). No model work has happened yet,
// so there is no captured evidence to take; the operator must see why the
// repository's declared contract could not run.
func bootstrapBlockerTermination(result devenv.Result) termination {
	return termination{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: exitNeedsHuman,
		Outcome:  outcomeBlockedExternal,
		Category: terminalCategoryBlockedExternal,
		Reason:   "workspace bootstrap failed: " + result.Reason,
	}
}

// applyBootstrapEnv installs the deterministic bootstrap delta into this
// process so the later direct-argv children (capability preflight, the model
// process, any shell it spawns) inherit it. Protected keys (#414) are never
// overridden.
func applyBootstrapEnv(delta []string) {
	for _, kv := range delta {
		name, value, ok := strings.Cut(kv, "=")
		if ok && !devenv.ProtectedKeys(name) {
			_ = os.Setenv(name, value)
		}
	}
}
