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

func (o Observer) Observe(ctx context.Context, repository, branch string) (controller.PRObservation, error) {
	if o.Client == nil {
		return controller.PRObservation{}, fmt.Errorf("GitHub observer: nil client")
	}
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return controller.PRObservation{}, err
	}
	pulls, err := o.Client.PullRequestsForHead(ctx, owner, repo, branch)
	if err != nil {
		return controller.PRObservation{}, err
	}
	for _, pull := range pulls {
		if !strings.EqualFold(pull.State, "open") || pull.Head.Ref != branch {
			continue
		}
		ref := pull.Head.SHA
		if ref == "" {
			ref = branch
		}
		checks, err := o.Client.GetCheckRuns(ctx, owner, repo, ref)
		if err != nil {
			return controller.PRObservation{}, err
		}
		statuses, err := o.Client.GetCommitStatuses(ctx, owner, repo, ref)
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
