// Package branch contains the small amount of branch policy shared by the
// operator and the coordinator harness.
package branch

import (
	"fmt"
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
