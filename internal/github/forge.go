package github

import (
	"context"
	"strconv"

	"github.com/misospace/courier/internal/forge"
)

// Provider adapts the GitHub REST Client to the forge-agnostic forge.Provider
// contract. It implements only the operations this client already supports and
// reports the rest as unsupported, so the GitHub adapter stays scoped to
// covered operations. Like the client, it never merges and never exposes a raw
// request: those verbs do not exist here.
type Provider struct {
	cfg    forge.ProviderConfig
	client *Client
	caps   forge.Capabilities
}

var _ forge.Provider = (*Provider)(nil)

// NewProvider builds a forge.Provider over a GitHub client. cfg carries the
// endpoint, name, and a credential *reference* (never a resolved credential);
// the caller is responsible for constructing client with a broker-resolved
// token. NewProvider registers capabilities for the operations this client
// covers and marks the rest as unavailable.
func NewProvider(cfg forge.ProviderConfig, client *Client) *Provider {
	return &Provider{
		cfg:    cfg,
		client: client,
		caps: forge.NewCapabilities(
			forge.CapabilityReadPullRequest,
			forge.CapabilityReadChecks,
			forge.CapabilityCreatePullRequest,
			forge.CapabilityUpdatePullRequest,
			forge.CapabilityComment,
		),
	}
}

// Config returns the provider's forge-agnostic configuration.
func (p *Provider) Config() forge.ProviderConfig { return p.cfg }

// Capabilities reports the operations this provider registers.
func (p *Provider) Capabilities() forge.Capabilities { return p.caps.Clone() }

// ReadWorkItem is not covered by this client.
func (p *Provider) ReadWorkItem(ctx context.Context, ref forge.WorkItemRef) (forge.WorkItem, error) {
	return forge.WorkItem{}, forge.ErrUnsupported
}

// ReadPullRequest reads the current pull request from GitHub.
func (p *Provider) ReadPullRequest(ctx context.Context, ref forge.PullRequestRef) (forge.PullRequest, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return forge.PullRequest{}, err
	}
	pr, err := p.client.GetPullRequest(ctx, owner, repo, ref.Number)
	if err != nil {
		return forge.PullRequest{}, err
	}
	return toForgePR(ref.Repo, pr), nil
}

// ListReviews is not covered by this client.
func (p *Provider) ListReviews(ctx context.Context, ref forge.PullRequestRef) ([]forge.Review, error) {
	return nil, forge.ErrUnsupported
}

// ListComments is not covered by this client.
func (p *Provider) ListComments(ctx context.Context, ref forge.PullRequestRef) ([]forge.Comment, error) {
	return nil, forge.ErrUnsupported
}

// ReadChecks lists the CI checks attached to the given head commit.
func (p *Provider) ReadChecks(ctx context.Context, ref forge.PullRequestRef, headSHA string) ([]forge.Check, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return nil, err
	}
	runs, err := p.client.GetCheckRuns(ctx, owner, repo, headSHA)
	if err != nil {
		return nil, err
	}
	checks := make([]forge.Check, 0, len(runs.CheckRuns))
	for _, run := range runs.CheckRuns {
		url := run.HTMLURL
		if url == "" {
			url = run.DetailsURL
		}
		checks = append(checks, forge.Check{Name: run.Name, State: run.Status, Conclusion: run.Conclusion, URL: url})
	}
	return checks, nil
}

// CreatePullRequest opens a new pull request.
func (p *Provider) CreatePullRequest(ctx context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	owner, repo, err := splitRepository(in.Repo)
	if err != nil {
		return forge.PullRequest{}, err
	}
	pr, err := p.client.CreatePullRequest(ctx, owner, repo, CreatePullRequestRequest{
		Title: in.Title,
		Head:  in.Head,
		Base:  in.Base,
		Body:  in.Body,
		Draft: in.Draft,
	})
	if err != nil {
		return forge.PullRequest{}, err
	}
	return toForgePR(in.Repo, pr), nil
}

// UpdatePullRequest applies the non-zero fields of in to a pull request.
func (p *Provider) UpdatePullRequest(ctx context.Context, ref forge.PullRequestRef, in forge.UpdatePullRequestInput) (forge.PullRequest, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return forge.PullRequest{}, err
	}
	pr, err := p.client.UpdatePullRequest(ctx, owner, repo, ref.Number, UpdatePullRequestRequest{
		Title: in.Title,
		Body:  in.Body,
		State: in.State,
		Draft: in.Draft,
	})
	if err != nil {
		return forge.PullRequest{}, err
	}
	return toForgePR(ref.Repo, pr), nil
}

// CommentPullRequest adds a comment to a pull request.
func (p *Provider) CommentPullRequest(ctx context.Context, ref forge.PullRequestRef, body string) (forge.Comment, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return forge.Comment{}, err
	}
	c, err := p.client.AddComment(ctx, owner, repo, ref.Number, body)
	if err != nil {
		return forge.Comment{}, err
	}
	return forge.Comment{ID: strconv.FormatInt(c.ID, 10), Body: c.Body, URL: c.HTMLURL}, nil
}

// toForgePR maps a GitHub pull request to the forge-agnostic shape, stamping
// the repository the request was made against.
func toForgePR(repo string, pr PullRequest) forge.PullRequest {
	return forge.PullRequest{
		Number:  pr.Number,
		Repo:    repo,
		Title:   pr.Title,
		Body:    pr.Body,
		State:   pr.State,
		Draft:   pr.Draft,
		HeadSHA: pr.Head.SHA,
		HeadRef: pr.Head.Ref,
		BaseRef: pr.Base.Ref,
		URL:     pr.HTMLURL,
		Merged:  pr.MergedAt != "",
	}
}
