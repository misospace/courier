package branch

import (
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
