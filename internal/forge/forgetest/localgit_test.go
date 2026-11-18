package forgetest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/forge/forgetest"
)

// localGitFixture builds a double with one repository, one work item, and
// one pull request, the state every operation of the double needs.
func localGitFixture(t *testing.T) *forgetest.LocalGit {
	t.Helper()
	const repo = "github.com/acme/web"
	return forgetest.NewLocalGit(forgetest.LocalGitConfig{
		Provider: forge.ProviderConfig{Name: "localgit"},
		Repositories: map[string]forge.Repository{
			"acme/web": {ID: "acme/web", Canonical: repo, DefaultRef: "main"},
		},
		WorkItems: map[string]forge.WorkItem{
			forgetest.LocalGitWorkItemKey(repo, "1"): {ID: "1", Repo: repo, Title: "do it"},
		},
		PullRequests: []forge.PullRequest{
			{Number: 7, Repo: repo, BaseRepo: repo, BaseRef: "main", HeadRepo: repo, HeadRef: "feat", Title: "t", State: "open"},
		},
	})
}

// TestLocalGitServesEveryRegisteredCapability proves the double registers
// only what it serves: a capability gate that passes on the double is a gate
// the double can satisfy. Every registered capability is exercised, and no
// call may answer the contract's capability-miss error.
func TestLocalGitServesEveryRegisteredCapability(t *testing.T) {
	ctx := context.Background()
	g := localGitFixture(t)
	const repo = "github.com/acme/web"
	pr := forge.PullRequestRef{Repo: repo, Number: 7}
	item := forge.WorkItemRef{Repo: repo, ID: "1"}

	served := map[forge.Capability]func() error{
		forge.CapabilityReadWorkItem: func() error {
			_, err := g.ReadWorkItem(ctx, item)
			return err
		},
		forge.CapabilityReadPullRequest: func() error {
			_, err := g.ReadPullRequest(ctx, pr)
			return err
		},
		forge.CapabilityListReviews: func() error {
			_, err := g.ListReviews(ctx, pr)
			return err
		},
		forge.CapabilityListComments: func() error {
			_, err := g.ListComments(ctx, pr)
			return err
		},
		forge.CapabilityReadChecks: func() error {
			_, err := g.ReadChecks(ctx, pr, "abc123")
			return err
		},
		forge.CapabilityCreatePullRequest: func() error {
			_, err := g.CreatePullRequest(ctx, forge.CreatePullRequestInput{
				Repo: repo, HeadRepo: repo, HeadRef: "feat", Base: "main", Title: "t",
			})
			return err
		},
		forge.CapabilityUpdatePullRequest: func() error {
			_, err := g.UpdatePullRequest(ctx, pr, forge.UpdatePullRequestInput{Title: "t2"})
			return err
		},
		forge.CapabilityComment: func() error {
			_, err := g.CommentPullRequest(ctx, pr, "a comment")
			return err
		},
		forge.CapabilityCommentWorkItem: func() error {
			_, err := g.CommentWorkItem(ctx, item, "a note")
			return err
		},
	}

	caps := g.Capabilities()
	for _, capability := range caps.List() {
		call, ok := served[capability]
		if !ok {
			t.Fatalf("the double registers capability %q, which no test exercises", capability)
		}
		err := call()
		if err != nil {
			t.Fatalf("the double registers capability %q but refuses the operation: %v", capability, err)
		}
		if errors.Is(err, forge.ErrUnsupported) {
			t.Fatalf("the double registers capability %q but answers the capability-miss error", capability)
		}
	}
	for capability := range served {
		if !caps.Has(capability) {
			t.Fatalf("the double serves capability %q without registering it", capability)
		}
	}
}

// TestLocalGitUnknownObjectsAreNotFound proves a missing object is a
// not-found answer, never the contract's capability-miss error: the double
// registers the capability, so a caller must not read a missing object as a
// provider that cannot serve the operation.
func TestLocalGitUnknownObjectsAreNotFound(t *testing.T) {
	ctx := context.Background()
	g := localGitFixture(t)
	const repo = "github.com/acme/web"
	missing := forge.PullRequestRef{Repo: repo, Number: 404}

	if _, err := g.ReadPullRequest(ctx, missing); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadPullRequest on an unknown pull request = %v, want ErrNotFound", err)
	}
	if _, err := g.ListReviews(ctx, missing); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ListReviews on an unknown pull request = %v, want ErrNotFound", err)
	}
	if _, err := g.ListComments(ctx, missing); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ListComments on an unknown pull request = %v, want ErrNotFound", err)
	}
	if _, err := g.ReadChecks(ctx, missing, "abc123"); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadChecks on an unknown pull request = %v, want ErrNotFound", err)
	}
	if _, err := g.CommentPullRequest(ctx, missing, "a comment"); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("CommentPullRequest on an unknown pull request = %v, want ErrNotFound", err)
	}
	if _, err := g.ReadWorkItem(ctx, forge.WorkItemRef{Repo: repo, ID: "99"}); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("ReadWorkItem on an unknown work item = %v, want ErrNotFound", err)
	}
	if _, err := g.CommentWorkItem(ctx, forge.WorkItemRef{Repo: repo, ID: "99"}, "a note"); !errors.Is(err, forgetest.ErrNotFound) {
		t.Fatalf("CommentWorkItem on an unknown work item = %v, want ErrNotFound", err)
	}
}

// TestLocalGitCommentWritesAreObservable proves a comment the control side
// wrote is readable back, and that the recorded call carries what was
// written.
func TestLocalGitCommentWritesAreObservable(t *testing.T) {
	ctx := context.Background()
	g := localGitFixture(t)
	const repo = "github.com/acme/web"
	pr := forge.PullRequestRef{Repo: repo, Number: 7}
	item := forge.WorkItemRef{Repo: repo, ID: "1"}

	written, err := g.CommentPullRequest(ctx, pr, "a pull request comment")
	if err != nil {
		t.Fatalf("CommentPullRequest: %v", err)
	}
	comments, err := g.ListComments(ctx, pr)
	if err != nil {
		t.Fatalf("ListComments: %v", err)
	}
	if len(comments) != 1 || comments[0].ID != written.ID || comments[0].Body != "a pull request comment" {
		t.Fatalf("comments = %+v, want the one comment that was written", comments)
	}

	noted, err := g.CommentWorkItem(ctx, item, "a work item note")
	if err != nil {
		t.Fatalf("CommentWorkItem: %v", err)
	}
	notes := g.WorkItemComments(item)
	if len(notes) != 1 || notes[0].ID != noted.ID || notes[0].Body != "a work item note" {
		t.Fatalf("work item comments = %+v, want the one comment that was written", notes)
	}

	recorded := g.OpsOf(forgetest.LocalGitOpCommentPullRequest)
	if len(recorded) != 1 || recorded[0].Body != "a pull request comment" || recorded[0].Number != pr.Number {
		t.Fatalf("recorded comment calls = %+v, want the one call that carried the body", recorded)
	}
	if _, err := g.ReadChecks(ctx, pr, "abc123"); err != nil {
		t.Fatalf("ReadChecks: %v", err)
	}
	checks := g.OpsOf(forgetest.LocalGitOpReadChecks)
	if len(checks) != 1 || checks[0].Head != "abc123" {
		t.Fatalf("recorded check reads = %+v, want the one read that carried the head", checks)
	}
}
