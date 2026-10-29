package branch

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestResolveIssueBranchIsDeterministicAndNamesIdentity(t *testing.T) {
	const repo = "acme/widgets"
	const issue = 42

	want := "courier/acme/widgets/issue-42"
	if got := ResolveIssueBranch(repo, issue); got != want {
		t.Fatalf("ResolveIssueBranch() = %q, want %q", got, want)
	}
	if got := ResolveIssueBranch(repo, issue); got != ResolveIssueBranch(repo, issue) {
		t.Fatal("ResolveIssueBranch() is not deterministic")
	}
	if got := ResolveIssueBranch(repo, 43); got == want {
		t.Fatal("different issue references must not share a branch")
	}
	if got := ResolveIssueBranch("other/widgets", issue); got == want {
		t.Fatal("different repositories must not share a branch")
	}
}

func TestResolveIssueBranchRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo string
		ref  int
	}{
		{name: "missing owner", repo: "/widgets", ref: 1},
		{name: "missing repository", repo: "acme/", ref: 1},
		{name: "too many path components", repo: "acme/team/widgets", ref: 1},
		{name: "whitespace", repo: "acme/my widgets", ref: 1},
		{name: "zero ref", repo: "acme/widgets", ref: 0},
		{name: "negative ref", repo: "acme/widgets", ref: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveIssueBranch(tc.repo, tc.ref); got != "" {
				t.Fatalf("ResolveIssueBranch(%q, %d) = %q, want empty", tc.repo, tc.ref, got)
			}
		})
	}
}

func TestResolveGoalBranchCarriesKeyIdentity(t *testing.T) {
	const repo = "someone/project"
	const key = "adhoc-abcdef123456"

	want := "courier/someone/project/goal-abcdef123456"
	if got := ResolveGoalBranch(repo, key); got != want {
		t.Fatalf("ResolveGoalBranch() = %q, want %q", got, want)
	}
	if got := ResolveGoalBranch(repo, key); got != ResolveGoalBranch(repo, key) {
		t.Fatal("ResolveGoalBranch() is not deterministic")
	}
	if got := ResolveGoalBranch(repo, "adhoc-abcdef123457"); got == want {
		t.Fatal("different idempotency keys must not share a branch")
	}
	if got := ResolveGoalBranch("other/project", key); got == want {
		t.Fatal("different repositories must not share a branch")
	}
}

func TestResolveGoalBranchDigestsForeignKeys(t *testing.T) {
	// A caller-supplied key is not hex and may carry characters a git ref
	// component forbids, so its suffix is derived: the branch is still
	// deterministic and still a valid ref for every key the source layer
	// accepts.
	const repo = "someone/project"
	const key = "release-contrib:v1.2"
	digest := sha256.Sum256([]byte(key))
	want := "courier/someone/project/goal-" + hex.EncodeToString(digest[:])[:12]
	if got := ResolveGoalBranch(repo, key); got != want {
		t.Fatalf("ResolveGoalBranch() = %q, want %q", got, want)
	}
	branch := ResolveGoalBranch(repo, key)
	for _, forbidden := range []string{":", "..", " ", "*", "?", "["} {
		if strings.Contains(branch, forbidden) {
			t.Fatalf("branch %q carries ref-forbidden %q", branch, forbidden)
		}
	}
}

func TestResolveGoalBranchRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo string
		key  string
	}{
		{name: "missing owner", repo: "/widgets", key: "adhoc-abcdef123456"},
		{name: "missing repository", repo: "acme/", key: "adhoc-abcdef123456"},
		{name: "too many path components", repo: "acme/team/widgets", key: "adhoc-abcdef123456"},
		{name: "whitespace", repo: "acme/my widgets", key: "adhoc-abcdef123456"},
		{name: "empty key", repo: "acme/widgets", key: ""},
		{name: "blank key", repo: "acme/widgets", key: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveGoalBranch(tc.repo, tc.key); got != "" {
				t.Fatalf("ResolveGoalBranch(%q, %q) = %q, want empty", tc.repo, tc.key, got)
			}
		})
	}
}
