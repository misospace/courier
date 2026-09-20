package github

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/misospace/courier/internal/controller"
)

// Observer reads pull requests and their CI state from GitHub.
type Observer struct {
	Client *Client
}

var _ controller.WorldObserver = Observer{}

func (o Observer) Observe(ctx context.Context, repository, branch string) (controller.PRObservation, error) {
	if o.Client == nil {
		return controller.PRObservation{}, fmt.Errorf("GitHub observer: nil client")
	}
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return controller.PRObservation{}, fmt.Errorf("GitHub observer: invalid repository %q", repository)
	}
	pulls, err := o.Client.PullRequestsForHead(ctx, parts[0], parts[1], branch)
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
		checks, err := o.Client.GetCheckRuns(ctx, parts[0], parts[1], ref)
		if err != nil {
			return controller.PRObservation{}, err
		}
		observed := controller.PRObservation{PR: strconv.Itoa(pull.Number), Draft: pull.Draft}
		for _, check := range checks.CheckRuns {
			observed.Checks = append(observed.Checks, controller.CheckObservation{Conclusion: check.Conclusion})
		}
		return observed, nil
	}
	return controller.PRObservation{}, nil
}
