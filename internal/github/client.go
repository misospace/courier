// Package github provides the small GitHub surface needed by a coordinator.
//
// It intentionally does not expose a merge operation. Merging is a human gate
// in Courier's workflow.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

const (
	defaultBaseURL = "https://api.github.com/"
	apiVersion     = "2022-11-28"
)

// HTTPDoer is the part of http.Client used by Client. Keeping it small makes
// the GitHub adapter straightforward to test without a live repository.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client is a minimal GitHub REST API client for pull requests and CI checks.
type Client struct {
	baseURL    *url.URL
	token      string
	httpClient HTTPDoer
}

// NewClient creates a GitHub client. baseURL is normally left as
// https://api.github.com/; a custom URL is useful for GitHub Enterprise and
// tests. The optional HTTP client defaults to http.DefaultClient.
func NewClient(baseURL, token string, clients ...HTTPDoer) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse GitHub base URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("GitHub base URL must include scheme and host")
	}
	if len(clients) > 1 {
		return nil, errors.New("at most one HTTP client may be supplied")
	}
	httpClient := HTTPDoer(http.DefaultClient)
	if len(clients) == 1 && clients[0] != nil {
		httpClient = clients[0]
	}
	return &Client{baseURL: u, token: token, httpClient: httpClient}, nil
}

// NewGitHubClient is an explicit constructor alias for callers that prefer a
// descriptive name. It is equivalent to NewClient.
func NewGitHubClient(baseURL, token string, httpClient ...HTTPDoer) (*Client, error) {
	return NewClient(baseURL, token, httpClient...)
}

// New creates a client against GitHub.com. Use NewClient when a GitHub
// Enterprise base URL is required.
func New(token string, httpClient ...HTTPDoer) (*Client, error) {
	return NewClient(defaultBaseURL, token, httpClient...)
}

// PullRequest is the subset of a GitHub pull request that Courier needs.
type PullRequest struct {
	Number         int    `json:"number"`
	URL            string `json:"url"`
	HTMLURL        string `json:"html_url"`
	State          string `json:"state"`
	Title          string `json:"title"`
	Body           string `json:"body"`
	Draft          bool   `json:"draft"`
	Mergeable      *bool  `json:"mergeable"`
	MergeableState string `json:"mergeable_state"`
	Head           Ref    `json:"head"`
	Base           Ref    `json:"base"`
}

// Ref identifies a branch and its current commit in a pull request response.
type Ref struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// CreatePullRequestRequest is the payload for creating a pull request.
type CreatePullRequestRequest struct {
	Title string `json:"title"`
	Head  string `json:"head"`
	Base  string `json:"base"`
	Body  string `json:"body,omitempty"`
	Draft bool   `json:"draft,omitempty"`
}

// UpdatePullRequestRequest is the payload for updating a pull request. A
// field omitted from this value is left unchanged by GitHub.
type UpdatePullRequestRequest struct {
	Title               string `json:"title,omitempty"`
	Body                string `json:"body,omitempty"`
	State               string `json:"state,omitempty"`
	Base                string `json:"base,omitempty"`
	MaintainerCanModify *bool  `json:"maintainer_can_modify,omitempty"`
}

// Comment is an issue/PR comment returned by GitHub.
type Comment struct {
	ID      int64  `json:"id"`
	Body    string `json:"body"`
	URL     string `json:"url"`
	HTMLURL string `json:"html_url"`
}

// CommentRequest is the payload for adding a PR comment.
type CommentRequest struct {
	Body string `json:"body"`
}

// CheckRun is a single CI check reported by GitHub.
type CheckRun struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	DetailsURL string `json:"details_url"`
	HTMLURL    string `json:"html_url"`
	HeadSHA    string `json:"head_sha"`
}

// CheckRuns is the response from GitHub's check-runs endpoint.
type CheckRuns struct {
	TotalCount int        `json:"total_count"`
	CheckRuns  []CheckRun `json:"check_runs"`
}

type CommitStatus struct {
	State   string `json:"state"`
	Context string `json:"context"`
}

type CommitStatuses struct {
	TotalCount int            `json:"total_count"`
	Statuses   []CommitStatus `json:"statuses"`
}

// APIError reports a non-2xx GitHub response. Body is retained for callers
// that need the provider's diagnostic, but it is capped to avoid retaining a
// huge response in a run's error path.
type APIError struct {
	StatusCode int
	Message    string
	Body       string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("GitHub API: %s", e.Message)
	}
	return fmt.Sprintf("GitHub API returned HTTP %d", e.StatusCode)
}

// CreatePullRequest opens a pull request from head into base.
func (c *Client) CreatePullRequest(ctx context.Context, owner, repo string, in CreatePullRequestRequest) (PullRequest, error) {
	var out PullRequest
	err := c.doJSON(ctx, http.MethodPost, repoEndpoint(owner, repo, "pulls"), in, &out)
	return out, err
}

// CreatePR is a short alias for CreatePullRequest.
func (c *Client) CreatePR(ctx context.Context, owner, repo string, in CreatePullRequestRequest) (PullRequest, error) {
	return c.CreatePullRequest(ctx, owner, repo, in)
}

// UpdatePullRequest updates metadata on an existing pull request. It does not
// merge the pull request; no merge endpoint is part of this client.
func (c *Client) UpdatePullRequest(ctx context.Context, owner, repo string, number int, in UpdatePullRequestRequest) (PullRequest, error) {
	var out PullRequest
	err := c.doJSON(ctx, http.MethodPatch, repoEndpoint(owner, repo, "pulls", strconv.Itoa(number)), in, &out)
	return out, err
}

// UpdatePR is a short alias for UpdatePullRequest.
func (c *Client) UpdatePR(ctx context.Context, owner, repo string, number int, in UpdatePullRequestRequest) (PullRequest, error) {
	return c.UpdatePullRequest(ctx, owner, repo, number, in)
}

// AddComment adds a comment to a pull request conversation.
func (c *Client) AddComment(ctx context.Context, owner, repo string, number int, body string) (Comment, error) {
	var out Comment
	err := c.doJSON(ctx, http.MethodPost, repoEndpoint(owner, repo, "issues", strconv.Itoa(number), "comments"), CommentRequest{Body: body}, &out)
	return out, err
}

// CommentPR is a short alias for AddComment.
func (c *Client) CommentPR(ctx context.Context, owner, repo string, number int, body string) (Comment, error) {
	return c.AddComment(ctx, owner, repo, number, body)
}

// GetPullRequest reads the current pull request from GitHub.
func (c *Client) GetPullRequest(ctx context.Context, owner, repo string, number int) (PullRequest, error) {
	var out PullRequest
	err := c.doJSON(ctx, http.MethodGet, repoEndpoint(owner, repo, "pulls", strconv.Itoa(number)), nil, &out)
	return out, err
}

// ReadPR is a short alias for GetPullRequest.
func (c *Client) ReadPR(ctx context.Context, owner, repo string, number int) (PullRequest, error) {
	return c.GetPullRequest(ctx, owner, repo, number)
}

// PullRequestsForHead lists every pull request whose head is owner:branch,
// including merged and closed ones. state=all is deliberate: an adoption
// guard must see pull requests in any state, not only open ones.
func (c *Client) PullRequestsForHead(ctx context.Context, owner, repo, branch string) ([]PullRequest, error) {
	var out []PullRequest
	err := c.doJSONQuery(ctx, http.MethodGet, repoEndpoint(owner, repo, "pulls"), headFilterQuery(owner, branch), nil, &out)
	return out, err
}

// headFilterQuery builds the pulls-list query for a head ref owned by the
// repository. GitHub requires the owner:branch form of the head filter.
func headFilterQuery(owner, branch string) string {
	return "head=" + url.QueryEscape(owner) + ":" + url.QueryEscape(branch) + "&state=all"
}

// GetCheckRuns reads all CI checks for a commit, branch, or tag ref.
func (c *Client) GetCheckRuns(ctx context.Context, owner, repo, ref string) (CheckRuns, error) {
	endpoint := repoEndpoint(owner, repo, "commits", ref, "check-runs")
	var out CheckRuns
	for page := 1; ; page++ {
		var current CheckRuns
		err := c.doJSONQuery(ctx, http.MethodGet, endpoint, pageQuery(page), nil, &current)
		if err != nil {
			return CheckRuns{}, err
		}
		if page == 1 {
			out.TotalCount = current.TotalCount
		} else if current.TotalCount != out.TotalCount {
			return CheckRuns{}, fmt.Errorf("GitHub check-runs total_count changed from %d to %d during pagination", out.TotalCount, current.TotalCount)
		}
		if len(current.CheckRuns) == 0 && len(out.CheckRuns) < out.TotalCount {
			return CheckRuns{}, fmt.Errorf("GitHub check-runs response ended after %d of %d checks", len(out.CheckRuns), out.TotalCount)
		}
		out.CheckRuns = append(out.CheckRuns, current.CheckRuns...)
		if len(out.CheckRuns) > out.TotalCount {
			return CheckRuns{}, fmt.Errorf("GitHub check-runs response returned %d of %d checks", len(out.CheckRuns), out.TotalCount)
		}
		if len(out.CheckRuns) == out.TotalCount {
			return out, nil
		}
	}
}

// GetCommitStatuses reads all legacy statuses for a commit, branch, or tag ref.
func (c *Client) GetCommitStatuses(ctx context.Context, owner, repo, ref string) (CommitStatuses, error) {
	endpoint := repoEndpoint(owner, repo, "commits", ref, "status")
	var out CommitStatuses
	for page := 1; ; page++ {
		var current CommitStatuses
		err := c.doJSONQuery(ctx, http.MethodGet, endpoint, pageQuery(page), nil, &current)
		if err != nil {
			return CommitStatuses{}, err
		}
		if page == 1 {
			out.TotalCount = current.TotalCount
		} else if current.TotalCount != out.TotalCount {
			return CommitStatuses{}, fmt.Errorf("GitHub commit-status total_count changed from %d to %d during pagination", out.TotalCount, current.TotalCount)
		}
		if len(current.Statuses) == 0 && len(out.Statuses) < out.TotalCount {
			return CommitStatuses{}, fmt.Errorf("GitHub commit-status response ended after %d of %d statuses", len(out.Statuses), out.TotalCount)
		}
		out.Statuses = append(out.Statuses, current.Statuses...)
		if len(out.Statuses) > out.TotalCount {
			return CommitStatuses{}, fmt.Errorf("GitHub commit-status response returned %d of %d statuses", len(out.Statuses), out.TotalCount)
		}
		if len(out.Statuses) == out.TotalCount {
			return out, nil
		}
	}
}

func pageQuery(page int) string {
	return "page=" + strconv.Itoa(page) + "&per_page=100"
}

// GetChecks is a concise alias for GetCheckRuns.
func (c *Client) GetChecks(ctx context.Context, owner, repo, ref string) (CheckRuns, error) {
	return c.GetCheckRuns(ctx, owner, repo, ref)
}

// ReadChecks is a short alias for GetCheckRuns.
func (c *Client) ReadChecks(ctx context.Context, owner, repo, ref string) (CheckRuns, error) {
	return c.GetCheckRuns(ctx, owner, repo, ref)
}

func repoEndpoint(owner, repo string, parts ...string) string {
	all := []string{"repos", owner, repo}
	all = append(all, parts...)
	for i := range all {
		all[i] = url.PathEscape(all[i])
	}
	return strings.Join(all, "/")
}

func (c *Client) endpoint(relative string) *url.URL {
	return c.endpointWithQuery(relative, "")
}

func (c *Client) endpointWithQuery(relative, query string) *url.URL {
	u := *c.baseURL
	basePath := u.EscapedPath()
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}
	rawPath := path.Join(basePath, relative)
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		// relative is assembled by this package and only contains valid escapes.
		// Keep a safe fallback for a future caller that adds an invalid segment.
		decodedPath = path.Join(u.Path, relative)
		rawPath = ""
	}
	u.Path = decodedPath
	u.RawPath = rawPath
	if u.EscapedPath() == u.Path {
		u.RawPath = ""
	}
	u.RawQuery = query
	return &u
}

func (c *Client) doJSON(ctx context.Context, method, relative string, input, output any) error {
	return c.doJSONQuery(ctx, method, relative, "", input, output)
}

// doJSONQuery is doJSON with a raw query string, for list endpoints that
// filter server-side.
func (c *Client) doJSONQuery(ctx context.Context, method, relative, query string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode GitHub request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpointWithQuery(relative, query).String(), body)
	if err != nil {
		return fmt.Errorf("create GitHub request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	req.Header.Set("User-Agent", "courier")
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub request: %w", err)
	}
	if resp == nil {
		return errors.New("GitHub request: HTTP client returned a nil response")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read GitHub response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		apiErr := &APIError{StatusCode: resp.StatusCode, Body: string(data)}
		var payload struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(data, &payload) == nil {
			apiErr.Message = payload.Message
		}
		return apiErr
	}
	if output == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("decode GitHub response: %w", err)
	}
	return nil
}
