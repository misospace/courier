// Package branch contains the small amount of branch policy shared by the
// operator and the coordinator harness.
package branch

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// ModeResolveIssue and ModeFixPR are kept as strings here so this package
	// remains independent of the Kubernetes API package.
	ModeResolveIssue = "resolve-issue"
	ModeFixPR        = "fix-pr"
)

var (
	ErrInvalidRepository = errors.New("repository must be owner/name")
	ErrInvalidReference  = errors.New("reference must be positive")
	ErrMissingPRBranch   = errors.New("fix-pr requires the existing pull request branch")
	ErrUnknownMode       = errors.New("unknown run mode")
)

// ResolveIssueBranch derives the stable branch for an issue run. The
// repository is part of the name deliberately: the function is pure and the
// result cannot accidentally collide if branch names are copied between
// repositories. No fallback branch is returned for invalid input.
func ResolveIssueBranch(repo string, issue int) string {
	if !validRepo(repo) || issue < 1 {
		return ""
	}
	return fmt.Sprintf("courier/%s/issue-%d", repo, issue)
}

// ResolveIssue is a concise alias for ResolveIssueBranch.
func ResolveIssue(repo string, issue int) string {
	return ResolveIssueBranch(repo, issue)
}

// AdoptFixPRBranch returns the branch attached to an existing pull request.
// A fix-pr run must adopt that branch; deriving one from the PR number would
// create a new branch and leave the PR untouched. Empty input is rejected by
// returning the empty string.
func AdoptFixPRBranch(existing string) string {
	if strings.TrimSpace(existing) == "" {
		return ""
	}
	return existing
}

// FixPR is a concise alias for AdoptFixPRBranch.
func FixPR(existing string) string {
	return AdoptFixPRBranch(existing)
}

// Select chooses a run's branch according to its mode. Resolve-issue derives
// a deterministic branch. Fix-pr always adopts existingBranch, and never
// derives a branch from ref.
func Select(mode, repo string, ref int, existingBranch string) (string, error) {
	switch mode {
	case ModeResolveIssue:
		if !validRepo(repo) {
			return "", ErrInvalidRepository
		}
		if ref < 1 {
			return "", ErrInvalidReference
		}
		return ResolveIssueBranch(repo, ref), nil
	case ModeFixPR:
		if AdoptFixPRBranch(existingBranch) == "" {
			return "", ErrMissingPRBranch
		}
		return existingBranch, nil
	default:
		return "", ErrUnknownMode
	}
}

// Resolve is an alias for Select for callers that model branch resolution as
// a single operation.
func Resolve(mode, repo string, ref int, existingBranch string) (string, error) {
	return Select(mode, repo, ref, existingBranch)
}

func validRepo(repo string) bool {
	if strings.TrimSpace(repo) != repo {
		return false
	}
	parts := strings.Split(repo, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" &&
		!strings.ContainsAny(repo, " \t\r\n")
}
