// Package branch contains the small amount of branch policy shared by the
// operator and the coordinator harness.
package branch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
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

func validRepo(repo string) bool {
	if strings.TrimSpace(repo) != repo {
		return false
	}
	parts := strings.Split(repo, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" &&
		!strings.ContainsAny(repo, " \t\r\n")
}

// mintedKeyPattern matches the idempotency key the ad-hoc source mints:
// "adhoc-" plus twelve lowercase hex digits. A minted key contributes its
// own hex to the branch suffix; any other caller-supplied key shape is
// digested, because a raw key may carry characters a git ref component
// forbids.
var mintedKeyPattern = regexp.MustCompile(`^adhoc-([0-9a-f]{12})$`)

// goalKeyHex returns the twelve-hex branch suffix for an idempotency key.
// The digest path's collision space is shared with minted keys by chance
// alone (one branch in 2^48 per repository); a colliding tip is a foreign
// branch and is refused by the work-ref admission semantics, never
// adopted or overwritten.
func goalKeyHex(key string) string {
	if minted := mintedKeyPattern.FindStringSubmatch(key); minted != nil {
		return minted[1]
	}
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])[:12]
}

// ResolveGoalBranch derives the stable work branch for a contribution:
// courier/<base-owner>/<base-repo>/goal-<12 hex of the idempotency key>.
// The base repository is part of the name deliberately, mirroring
// ResolveIssueBranch: the function is pure, the result cannot collide
// across repositories, and no issue number is involved. No fallback
// branch is returned for invalid input.
func ResolveGoalBranch(baseRepo, key string) string {
	if !validRepo(baseRepo) || strings.TrimSpace(key) == "" {
		return ""
	}
	return fmt.Sprintf("courier/%s/goal-%s", baseRepo, goalKeyHex(key))
}
