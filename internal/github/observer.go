package github

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
)

// Observer reads pull requests and their CI state from GitHub.
type Observer struct {
	Client *Client
}

var _ controller.WorldObserver = Observer{}
var _ controller.ExistingPRHeadResolver = Observer{}

// Observe reads the open pull request for a run's head and its CI state from
// the base repository. The pull request lives on, and its checks are reported
// by, the base repository; the head owner comes from the resolved head
// identity, because a fork pull request's head lives on the fork.
func (o Observer) Observe(ctx context.Context, base string, head controller.HeadRef) (controller.PRObservation, error) {
	if o.Client == nil {
		return controller.PRObservation{}, fmt.Errorf("GitHub observer: nil client")
	}
	if strings.TrimSpace(head.Branch) == "" {
		return controller.PRObservation{}, fmt.Errorf("GitHub observer: empty head branch")
	}
	baseOwner, baseRepo, err := splitRepository(base)
	if err != nil {
		return controller.PRObservation{}, err
	}
	// A fork pull request is listed on the base repository but its head
	// qualifier names the fork's owner. An empty head repository means a
	// same-repository head, so the base owner is the head owner.
	headOwner := baseOwner
	if headRepo := strings.TrimSpace(head.Repo); headRepo != "" {
		forkOwner, _, forkErr := splitRepository(headRepo)
		if forkErr != nil {
			return controller.PRObservation{}, fmt.Errorf("GitHub observer: invalid head repository %q: %w", headRepo, forkErr)
		}
		headOwner = forkOwner
	}
	pulls, err := o.Client.PullRequestsForHead(ctx, baseOwner, baseRepo, headOwner, head.Branch)
	if err != nil {
		return controller.PRObservation{}, err
	}
	for _, pull := range pulls {
		if !strings.EqualFold(pull.State, "open") || pull.Head.Ref != head.Branch {
			continue
		}
		// Check runs and commit statuses attach to the head commit on the
		// base repository. The branch name is only a fallback for heads
		// resolved without a SHA, which are always same-repository heads.
		ref := pull.Head.SHA
		if ref == "" {
			ref = head.SHA
		}
		if ref == "" {
			ref = head.Branch
		}
		checks, err := o.Client.GetCheckRuns(ctx, baseOwner, baseRepo, ref)
		if err != nil {
			return controller.PRObservation{}, err
		}
		statuses, err := o.Client.GetCommitStatuses(ctx, baseOwner, baseRepo, ref)
		if err != nil {
			return controller.PRObservation{}, err
		}
		observed := controller.PRObservation{PR: strconv.Itoa(pull.Number), Draft: pull.Draft, Head: pull.Head.SHA}
		for _, check := range checks.CheckRuns {
			observed.Checks = append(observed.Checks, controller.CheckObservation{Name: check.Name, State: checkState(check.Status, check.Conclusion)})
		}
		for _, status := range statuses.Statuses {
			observed.Checks = append(observed.Checks, controller.CheckObservation{Name: status.Context, State: commitStatusState(status.State)})
		}
		return observed, nil
	}
	return controller.PRObservation{}, nil
}

func (o Observer) ResolveHead(ctx context.Context, run *courierv1alpha1.CoderRun) (controller.HeadRef, error) {
	if o.Client == nil {
		return controller.HeadRef{}, fmt.Errorf("GitHub observer: nil client")
	}
	if run == nil {
		return controller.HeadRef{}, fmt.Errorf("GitHub observer: nil run")
	}
	owner, repo, err := splitRepository(run.Spec.Repo)
	if err != nil {
		return controller.HeadRef{}, err
	}
	pull, err := o.Client.GetPullRequest(ctx, owner, repo, run.Spec.Ref)
	if err != nil {
		return controller.HeadRef{}, err
	}
	if strings.TrimSpace(pull.Head.Ref) == "" {
		return controller.HeadRef{}, fmt.Errorf("GitHub observer: pull request %d has no head ref", run.Spec.Ref)
	}
	return controller.HeadRef{Repo: pull.Head.Repo.FullName, Branch: pull.Head.Ref, SHA: pull.Head.SHA}, nil
}

func splitRepository(repository string) (string, string, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("GitHub observer: invalid repository %q", repository)
	}
	return parts[0], parts[1], nil
}

func checkState(status, conclusion string) controller.CheckState {
	switch strings.ToLower(status) {
	case "queued", "in_progress", "pending", "requested", "waiting":
		return controller.CheckStatePending
	}
	switch strings.ToLower(conclusion) {
	case "success", "skipped", "neutral":
		return controller.CheckStatePassed
	default:
		return controller.CheckStateFailed
	}
}

func commitStatusState(state string) controller.CheckState {
	switch strings.ToLower(state) {
	case "pending":
		return controller.CheckStatePending
	case "success":
		return controller.CheckStatePassed
	default:
		return controller.CheckStateFailed
	}
}
