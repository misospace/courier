package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
var _ forge.PullRequestHeadLister = (*Provider)(nil)

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
// the caller is responsible for constructing client against cfg.Endpoint and
// with a broker-resolved token, so every read and write goes to the
// registered API host rather than the GitHub.com default. NewProvider
// registers capabilities for the operations this client covers and marks the
// rest as unavailable.
func NewProvider(cfg forge.ProviderConfig, client *Client) *Provider {
	return &Provider{
		cfg:    cfg,
		client: client,
		caps: forge.NewCapabilities(
			forge.CapabilityReadWorkItem,
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

// ReadWorkItem reads one issue by number. The reference ID is the issue
// number as a decimal string — the identity admission already pins in the
// immutable spec. The response's own identity is verified against the
// request: a response naming a different or missing number is refused, so a
// misrouted or inconsistent answer can never stand in for the requested
// issue. The issues API also serves pull requests; a pull-request response
// is refused rather than laundered into an issue read.
func (p *Provider) ReadWorkItem(ctx context.Context, ref forge.WorkItemRef) (forge.WorkItem, error) {
	owner, repo, err := splitRepository(ref.Repo)
	if err != nil {
		return forge.WorkItem{}, err
	}
	number, err := strconv.Atoi(strings.TrimSpace(ref.ID))
	if err != nil || number < 1 {
		return forge.WorkItem{}, fmt.Errorf("GitHub work item ID must be a positive issue number, got %q", ref.ID)
	}
	issue, err := p.client.GetIssue(ctx, owner, repo, number)
	if err != nil {
		return forge.WorkItem{}, boundaryError(err)
	}
	if issue.Number != number {
		return forge.WorkItem{}, fmt.Errorf("GitHub issue read for %d returned number %d; the response does not name the requested issue", number, issue.Number)
	}
	if issue.PullRequest != nil {
		return forge.WorkItem{}, fmt.Errorf("GitHub work item %d is a pull request, not an issue", number)
	}
	return forge.WorkItem{
		ID:    strconv.Itoa(issue.Number),
		Repo:  ref.Repo,
		Title: issue.Title,
		Body:  issue.Body,
		State: issue.State,
		URL:   issue.HTMLURL,
	}, nil
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

// ListPullRequestsByHead returns all PRs queried by head owner and branch, then
// filters exact repository and ref identities locally. GitHub's head filter is
// only owner:branch, so repository identity must be checked in the response.
// Records whose forge-carried identity does not match the requested head are
// dropped here; callers that must distinguish "no pull request exists" from
// "the only records claiming this head selector belong to another repository"
// use ListPullRequestsWithForeign instead, because an empty filtered list must
// never be read as confirmed absence on a world the listing did not settle.
func (p *Provider) ListPullRequestsByHead(ctx context.Context, base forge.PullRequestRef, headRepo, headRef string) ([]forge.PullRequest, error) {
	matches, _, err := p.listPullRequestsByHead(ctx, base, headRepo, headRef)
	return matches, err
}

// ListPullRequestsWithForeign is the identity-checked head listing that keeps
// foreign records visible: it returns the PRs whose full base- and head-
// repository and ref identities match the request separately from the PRs
// GitHub's owner:branch head selector surfaced whose identity differs from the
// requested pair (a same-owner fork claiming the same branch, or a response
// that does not carry the queried base repository). A foreign record can
// neither be adopted as the run's pull request nor silently deleted toward a
// "missing" claim, so callers that fail closed on a foreign head get them
// separated rather than dropped. A record missing either repository identity
// outright is a read failure, as in toForgePR.
func (p *Provider) ListPullRequestsWithForeign(ctx context.Context, base forge.PullRequestRef, headRepo, headRef string) (matches, foreign []forge.PullRequest, err error) {
	return p.listPullRequestsByHead(ctx, base, headRepo, headRef)
}

func (p *Provider) listPullRequestsByHead(ctx context.Context, base forge.PullRequestRef, headRepo, headRef string) (matches, foreign []forge.PullRequest, err error) {
	baseOwner, baseName, err := splitRepository(base.Repo)
	if err != nil {
		return nil, nil, err
	}
	headOwner, _, err := splitRepository(headRepo)
	if err != nil {
		return nil, nil, err
	}
	pulls, err := p.client.PullRequestsForHeadAllPages(ctx, baseOwner, baseName, headOwner, headRef)
	if err != nil {
		return nil, nil, boundaryError(err)
	}
	for _, raw := range pulls {
		pr, err := toForgePR(raw)
		if err != nil {
			return nil, nil, err
		}
		if pr.BaseRepo == base.Repo && pr.HeadRepo == headRepo && pr.HeadRef == headRef {
			matches = append(matches, pr)
		} else {
			foreign = append(foreign, pr)
		}
	}
	return matches, foreign, nil
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
		checks = append(checks, forge.Check{Name: run.Name, State: run.Status, Conclusion: run.Conclusion, URL: url, Output: checkOutput(run.Output)})
	}
	return checks, nil
}

// CreatePullRequest opens a new pull request.
func (p *Provider) CreatePullRequest(ctx context.Context, in forge.CreatePullRequestInput) (forge.PullRequest, error) {
	owner, repo, err := splitRepository(in.Repo)
	if err != nil {
		return forge.PullRequest{}, err
	}
	head := in.Head
	if in.HeadRepo != "" || in.HeadRef != "" {
		if in.HeadRepo == "" || in.HeadRef == "" {
			return forge.PullRequest{}, fmt.Errorf("GitHub pull request head repository and ref must both be set")
		}
		headOwner, _, err := splitRepository(in.HeadRepo)
		if err != nil {
			return forge.PullRequest{}, err
		}
		// Ignore Head when full trusted identity is supplied; it is a legacy
		// selector and may be caller-controlled.
		head = headOwner + ":" + in.HeadRef
	}
	pr, err := p.client.CreatePullRequest(ctx, owner, repo, CreatePullRequestRequest{
		Title: in.Title,
		Head:  head,
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
// Mergeability maps conservatively: GitHub's mergeable_state "dirty" is the
// authoritative conflict signal and wins over the boolean, the "unknown"
// state means the answer is still being computed and maps to nil even when a
// stale boolean is present, and a pending or absent answer maps to nil —
// callers must treat nil as unverifiable rather than satisfied (#513).
func toForgePR(pr PullRequest) (forge.PullRequest, error) {
	baseRepo := pr.Base.Repo.FullName
	if baseRepo == "" {
		return forge.PullRequest{}, fmt.Errorf("pull request %d: response is missing the base repository identity", pr.Number)
	}
	headRepo := pr.Head.Repo.FullName
	if headRepo == "" {
		return forge.PullRequest{}, fmt.Errorf("pull request %d: response is missing the head repository identity", pr.Number)
	}
	mergeable := pr.Mergeable
	switch pr.MergeableState {
	case "dirty":
		conflicting := false
		mergeable = &conflicting
	case "unknown":
		mergeable = nil
	}
	return forge.PullRequest{
		Number:    pr.Number,
		Repo:      baseRepo,
		BaseRepo:  baseRepo,
		BaseRef:   pr.Base.Ref,
		BaseSHA:   pr.Base.SHA,
		HeadRepo:  headRepo,
		HeadRef:   pr.Head.Ref,
		HeadSHA:   pr.Head.SHA,
		Title:     pr.Title,
		Body:      pr.Body,
		State:     pr.State,
		Draft:     pr.Draft,
		URL:       pr.HTMLURL,
		Merged:    pr.MergedAt != "",
		Mergeable: mergeable,
	}, nil
}
