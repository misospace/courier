package branch

import (
	"errors"
	"testing"
)

func TestResolveIssueBranchIsDeterministicAndNamesIdentity(t *testing.T) {
	const repo = "acme/widgets"
	const issue = 42

	want := "courier/acme/widgets/issue-42"
	if got := ResolveIssueBranch(repo, issue); got != want {
		t.Fatalf("ResolveIssueBranch() = %q, want %q", got, want)
	}
	if got := ResolveIssue(repo, issue); got != want {
		t.Fatalf("ResolveIssue() = %q, want %q", got, want)
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

func TestAdoptFixPRBranchPreservesExistingBranch(t *testing.T) {
	for _, existing := range []string{"feature/fix-17", "dependabot/go_modules/example", "main"} {
		if got := AdoptFixPRBranch(existing); got != existing {
			t.Fatalf("AdoptFixPRBranch(%q) = %q, want %q", existing, got, existing)
		}
		if got := FixPR(existing); got != existing {
			t.Fatalf("FixPR(%q) = %q, want %q", existing, got, existing)
		}
	}
}

func TestAdoptFixPRBranchRejectsMissingBranch(t *testing.T) {
	for _, existing := range []string{"", " ", "\t"} {
		if got := AdoptFixPRBranch(existing); got != "" {
			t.Fatalf("AdoptFixPRBranch(%q) = %q, want empty", existing, got)
		}
	}
}

func TestSelectResolveIssueDerivesBranch(t *testing.T) {
	got, err := Select(ModeResolveIssue, "acme/widgets", 42, "ignored/pr-branch")
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got != "courier/acme/widgets/issue-42" {
		t.Fatalf("Select() = %q, want deterministic issue branch", got)
	}
}

func TestSelectFixPRAdoptsExistingBranch(t *testing.T) {
	got, err := Select(ModeFixPR, "acme/widgets", 42, "feature/fix-17")
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	if got != "feature/fix-17" {
		t.Fatalf("Select() = %q, want existing PR branch", got)
	}
}

func TestSelectRejectsInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		repo string
		ref  int
		pr   string
		want error
	}{
		{name: "invalid repo", mode: ModeResolveIssue, repo: "widgets", ref: 1, want: ErrInvalidRepository},
		{name: "invalid ref", mode: ModeResolveIssue, repo: "acme/widgets", ref: 0, want: ErrInvalidReference},
		{name: "missing PR branch", mode: ModeFixPR, repo: "acme/widgets", ref: 17, want: ErrMissingPRBranch},
		{name: "unknown mode", mode: "other", repo: "acme/widgets", ref: 1, want: ErrUnknownMode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Select(tc.mode, tc.repo, tc.ref, tc.pr)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Select() error = %v, want %v", err, tc.want)
			}
		})
	}
}
