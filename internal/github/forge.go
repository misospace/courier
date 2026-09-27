package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
var _ forge.RepositoryPolicyProvider = (*Provider)(nil)

// redactedError is an error whose string form was sanitized with
// forge.RedactDetail before it crossed the forge boundary. It unwraps to the
// original, so typed errors (for example *APIError) stay reachable through
// errors.As.
type redactedError struct {
	original error
	message  string
}

func (e *redactedError) Error() string { return e.message }
func (e *redactedError) Unwrap() error { return e.original }

// boundaryError returns err with its string form passed through
// forge.RedactDetail, so the common credential shapes (a URL with embedded
// userinfo, a bearer token in an error message) do not reach the core in a
// provider error. It is a minimal guard, not a general secrets scanner, and
// it preserves the original error for typed inspection.
func boundaryError(err error) error {
	if err == nil {
		return nil
	}
	if redacted := forge.RedactDetail(err.Error()); redacted != err.Error() {
		return &redactedError{original: err, message: redacted}
	}
	return err
}

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

// ResolveRepository returns GitHub's canonical identity and default branch.
func (p *Provider) ResolveRepository(ctx context.Context, name string) (forge.Repository, error) {
	owner, repo, err := splitRepository(name)
	if err != nil {
		return forge.Repository{}, err
	}
	got, err := p.client.GetRepository(ctx, owner, repo)
	if err != nil {
		return forge.Repository{}, boundaryError(err)
	}
	if got.ID == 0 || got.FullName == "" || got.DefaultBranch == "" {
		return forge.Repository{}, fmt.Errorf("GitHub repository response is missing canonical identity or default branch")
	}
	return forge.Repository{ID: strconv.FormatInt(got.ID, 10), Canonical: got.FullName, DefaultRef: got.DefaultBranch}, nil
}

// ReadRef returns the exact current branch OID, or Exists=false for an
// authoritative 404. Other API failures remain errors.
func (p *Provider) ReadRef(ctx context.Context, repository forge.Repository, branch string) (forge.RefState, error) {
	owner, repo, err := splitRepository(repository.Canonical)
	if err != nil {
		return forge.RefState{}, err
	}
	got, err := p.client.GetBranchRef(ctx, owner, repo, branch)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return forge.RefState{Ref: branch, Exists: false}, nil
		}
		return forge.RefState{}, boundaryError(err)
	}
	if got.Ref == "" || got.Object.SHA == "" {
		return forge.RefState{}, fmt.Errorf("GitHub branch ref response is incomplete")
	}
	return forge.RefState{Ref: branch, OID: got.Object.SHA, Exists: true}, nil
}

// ReadEffectiveProtection fails closed when GitHub cannot authoritatively
// evaluate all rules applying to the ref. Bypass eligibility is unreported by
// GitHub's endpoint, so any applicable ruleset is conservatively protected.
func (p *Provider) ReadEffectiveProtection(ctx context.Context, repository forge.Repository, branch string) (forge.Protection, error) {
	owner, repo, err := splitRepository(repository.Canonical)
	if err != nil {
		return forge.Protection{}, err
	}
	rules, err := p.client.GetEffectiveBranchRules(ctx, owner, repo, branch)
	if err != nil {
		return forge.Protection{}, boundaryError(err)
	}
	if len(rules) != 0 {
		return forge.Protection{Protected: true}, nil
	}
	if _, err := p.client.GetBranchProtection(ctx, owner, repo, branch); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			// GitHub's classic protection endpoint uses 404 for an unprotected
			// branch. The rules endpoint has already succeeded and reported no
			// applicable rulesets, so together these establish no effective rule.
			return forge.Protection{Protected: false}, nil
		}
		return forge.Protection{}, boundaryError(err)
	}
	return forge.Protection{}, fmt.Errorf("GitHub API cannot authoritatively establish effective protection and bypass status for this ref")
}

// CanWriteRepository reports GitHub's token-derived push permission. An
// absent permission object is not evidence of write access.
func (p *Provider) CanWriteRepository(ctx context.Context, repository forge.Repository) (bool, error) {
	owner, repo, err := splitRepository(repository.Canonical)
	if err != nil {
		return false, err
	}
	got, err := p.client.GetRepository(ctx, owner, repo)
	if err != nil {
		return false, boundaryError(err)
	}
	if got.Permissions == nil {
		return false, fmt.Errorf("GitHub repository response is missing credential permissions")
	}
	return got.Permissions.Push, nil
}

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
		return forge.PullRequest{}, boundaryError(err)
	}
	return toForgePR(pr)
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
		return nil, boundaryError(err)
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
		return forge.PullRequest{}, boundaryError(err)
	}
	return toForgePR(pr)
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
	})
	if err != nil {
		return forge.PullRequest{}, boundaryError(err)
	}
	return toForgePR(pr)
}

// CommentPullRequest adds a comment to a pull request.
func (p *Provider) CommentPullRequest(ctx context.Context, ref forge.PullRequestRef, body string) (forge.Comment, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return forge.Comment{}, err
	}
	c, err := p.client.AddComment(ctx, owner, repo, ref.Number, body)
	if err != nil {
		return forge.Comment{}, boundaryError(err)
	}
	return forge.Comment{ID: strconv.FormatInt(c.ID, 10), Body: c.Body, URL: c.HTMLURL}, nil
}

// toForgePR maps a GitHub pull request to the provider-neutral shape. The
// base and head repository identities come from the forge response itself —
// the head may be a fork distinct from the base — and are never substituted
// from the repository the request was made against. A missing head (for
// example a deleted fork) or base repository identity is an error, so
// incomplete API data stays detectable rather than being silently backfilled.
func toForgePR(pr PullRequest) (forge.PullRequest, error) {
	baseRepo := pr.Base.Repo.FullName
	if baseRepo == "" {
		return forge.PullRequest{}, fmt.Errorf("pull request %d: response is missing the base repository identity", pr.Number)
	}
	headRepo := pr.Head.Repo.FullName
	if headRepo == "" {
		return forge.PullRequest{}, fmt.Errorf("pull request %d: response is missing the head repository identity", pr.Number)
	}
	return forge.PullRequest{
		Number:   pr.Number,
		Repo:     baseRepo,
		BaseRepo: baseRepo,
		BaseRef:  pr.Base.Ref,
		BaseSHA:  pr.Base.SHA,
		HeadRepo: headRepo,
		HeadRef:  pr.Head.Ref,
		HeadSHA:  pr.Head.SHA,
		Title:    pr.Title,
		Body:     pr.Body,
		State:    pr.State,
		Draft:    pr.Draft,
		URL:      pr.HTMLURL,
		Merged:   pr.MergedAt != "",
	}, nil
}
