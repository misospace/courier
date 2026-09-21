package dispatch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/misospace/courier/internal/source"
)

const (
	defaultTimeout = 30 * time.Second
)

var (
	ErrInvalidBaseURL       = errors.New("dispatch client: invalid base URL")
	ErrMissingToken         = errors.New("dispatch client: bearer token is required")
	ErrUnknownWorkID        = errors.New("dispatch client: invalid work item ID")
	ErrPRStateCheckerNeeded = errors.New("dispatch client: upstream pull request state checker is required")
)

// HTTPDoer is the HTTP transport used by HTTPClient.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Task is a Dispatch queue response.
type Task struct {
	Type        string       `json:"type"`
	ShouldRun   bool         `json:"shouldRun"`
	Issue       *Issue       `json:"issue,omitempty"`
	PullRequest *PullRequest `json:"pullRequest,omitempty"`
	Reasons     []string     `json:"reasons,omitempty"`
}

// Issue identifies the issue associated with a Dispatch task.
type Issue struct {
	ID     string `json:"id"`
	Repo   string `json:"repoFullName"`
	Number int    `json:"number"`
}

// PullRequest identifies the pull request associated with a Dispatch task.
type PullRequest struct {
	Repo   string `json:"repoFullName"`
	Number int    `json:"number"`
	URL    string `json:"url,omitempty"`
	State  string `json:"state,omitempty"`
	Merged bool   `json:"merged,omitempty"`
	Closed bool   `json:"closed,omitempty"`
}

// PullRequestState is the upstream state used to reject stale follow-up work.
type PullRequestState struct {
	State  string
	Merged bool
}

// PullRequestStateChecker reads the current upstream pull request state.
type PullRequestStateChecker interface {
	CheckPullRequestState(context.Context, string, int) (PullRequestState, error)
}

// PullRequestStateCheckerFunc adapts a function to PullRequestStateChecker.
type PullRequestStateCheckerFunc func(context.Context, string, int) (PullRequestState, error)

func (f PullRequestStateCheckerFunc) CheckPullRequestState(ctx context.Context, repo string, number int) (PullRequestState, error) {
	return f(ctx, repo, number)
}

type workDescriptor struct {
	Type        string `json:"type"`
	IssueID     string `json:"issueId,omitempty"`
	IssueRepo   string `json:"issueRepo,omitempty"`
	IssueNumber int    `json:"issueNumber,omitempty"`
	Repo        string `json:"repo,omitempty"`
	Number      int    `json:"number,omitempty"`
	PRURL       string `json:"prURL,omitempty"`
}

// APIError reports a non-2xx Dispatch response.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("Dispatch API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("Dispatch API returned HTTP %d: %s", e.StatusCode, e.Message)
}

// HTTPClient implements the Dispatch HTTP API behind the source adapter.
type HTTPClient struct {
	baseURL        *url.URL
	token          string
	agentName      string
	queueLane      string
	httpClient     HTTPDoer
	prStateChecker PullRequestStateChecker
}

// NewClient creates a Dispatch HTTP client without a queue lane override.
func NewClient(baseURL, agentName, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	return newClient(baseURL, agentName, "", token, timeout, clients...)
}

// NewClientWithLane creates a Dispatch HTTP client with a queue lane override.
func NewClientWithLane(baseURL, agentName, queueLane, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	return newClient(baseURL, agentName, queueLane, token, timeout, clients...)
}

// WithPullRequestStateChecker configures stale follow-up detection.
func (c *HTTPClient) WithPullRequestStateChecker(checker PullRequestStateChecker) *HTTPClient {
	c.prStateChecker = checker
	return c
}

func newClient(baseURL, agentName, queueLane, token string, timeout time.Duration, clients ...HTTPDoer) (*HTTPClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, ErrInvalidBaseURL
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, ErrInvalidBaseURL
	}
	if strings.TrimSpace(agentName) == "" {
		return nil, errors.New("dispatch client: agent name is required")
	}
	if strings.TrimSpace(token) == "" {
		return nil, ErrMissingToken
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if len(clients) > 1 {
		return nil, errors.New("dispatch client: at most one HTTP client may be supplied")
	}
	httpClient := HTTPDoer(&http.Client{Timeout: timeout})
	if len(clients) == 1 && clients[0] != nil {
		httpClient = clients[0]
	}
	return &HTTPClient{
		baseURL:    u,
		token:      strings.TrimSpace(token),
		agentName:  strings.TrimSpace(agentName),
		queueLane:  strings.TrimSpace(queueLane),
		httpClient: httpClient,
	}, nil
}

func (c *HTTPClient) Discover(ctx context.Context) ([]source.WorkItem, error) {
	var task Task
	path := "/api/agents/" + url.PathEscape(c.agentName) + "/next-task"
	if err := c.do(ctx, http.MethodGet, path, nil, &task); err != nil {
		return nil, err
	}
	if !task.ShouldRun || task.Type == "idle" {
		return nil, nil
	}
	if task.Type != "implement" && task.Type != "followup-pr" {
		return nil, fmt.Errorf("dispatch client: unsupported task type %q", task.Type)
	}
	itemID, err := taskWorkItem(task)
	if err != nil {
		return nil, err
	}
	repo, ref, err := taskRef(task)
	if err != nil {
		return nil, err
	}
	mode := "resolve-issue"
	if task.Type == "followup-pr" {
		state, err := c.checkPullRequestState(ctx, task.PullRequest)
		if err != nil {
			return nil, err
		}
		if state.Merged || strings.EqualFold(state.State, "closed") {
			if err := c.markPRFixStale(ctx, task.PullRequest, state); err != nil {
				return nil, err
			}
			return nil, nil
		}
		mode = "fix-pr"
	}
	return []source.WorkItem{{ID: itemID, Mode: mode, Repo: repo, Ref: ref}}, nil
}

func (c *HTTPClient) Claim(ctx context.Context, id string) error {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	if d.IssueID == "" {
		return nil
	}
	body := map[string]any{
		"issueId":      d.IssueID,
		"repoFullName": d.IssueRepo,
		"issueNumber":  d.IssueNumber,
		"agentName":    c.agentName,
	}
	return c.do(ctx, http.MethodPost, "/api/issues/claim", body, nil)
}

func (c *HTTPClient) Release(ctx context.Context, id string) error {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	if d.IssueID == "" {
		return nil
	}
	body := map[string]any{
		"issueId":      d.IssueID,
		"repoFullName": d.IssueRepo,
		"issueNumber":  d.IssueNumber,
		"agentName":    c.agentName,
	}
	return c.do(ctx, http.MethodPost, "/api/issues/unclaim", body, nil)
}

func (c *HTTPClient) SetStatus(ctx context.Context, id, status string) error {
	d, err := c.resolveIssue(ctx, id)
	if err != nil {
		return err
	}
	if strings.TrimPrefix(status, "status/") == "needs-human" {
		status = "blocked"
	}
	if d.IssueID == "" {
		return nil
	}
	body := map[string]any{
		"issueId":      d.IssueID,
		"repoFullName": d.IssueRepo,
		"issueNumber":  d.IssueNumber,
		"status":       strings.TrimPrefix(status, "status/"),
		"agentName":    c.agentName,
	}
	if strings.TrimPrefix(status, "status/") == "blocked" {
		body["blockedReason"] = "Courier run requires human intervention"
	}
	return c.do(ctx, http.MethodPost, "/api/issues/status", body, nil)
}

func (c *HTTPClient) checkPullRequestState(ctx context.Context, pullRequest *PullRequest) (PullRequestState, error) {
	if pullRequest == nil || pullRequest.Repo == "" || pullRequest.Number <= 0 {
		return PullRequestState{}, errors.New("dispatch client: followup task has no valid pull request")
	}
	if c.prStateChecker == nil {
		return PullRequestState{}, ErrPRStateCheckerNeeded
	}
	return c.prStateChecker.CheckPullRequestState(ctx, pullRequest.Repo, pullRequest.Number)
}

func (c *HTTPClient) markPRFixBlocked(ctx context.Context, d workDescriptor, note string) error {
	if d.Type != "followup-pr" {
		return nil
	}
	if strings.TrimSpace(note) == "" {
		note = "Courier run requires human intervention"
	}
	return c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
		"repo":   d.Repo,
		"pr":     d.Number,
		"status": "BLOCKED",
		"note":   note,
	}, nil)
}

func (c *HTTPClient) markPRFixStale(ctx context.Context, pullRequest *PullRequest, state PullRequestState) error {
	note := "upstream pull request is closed without merge"
	if state.Merged {
		note = "upstream pull request is merged"
	}
	return c.do(ctx, http.MethodPost, "/api/pr-fix-queue/mark", map[string]any{
		"repo":   pullRequest.Repo,
		"pr":     pullRequest.Number,
		"status": "STALE",
		"note":   note,
	}, nil)
}

func (c *HTTPClient) reportTaskWithPR(ctx context.Context, d workDescriptor, outcome, failure, observedPR string) error {
	body := map[string]any{
		"taskType":     d.Type,
		"outcome":      outcome,
		"repoFullName": d.Repo,
	}
	if d.Type == "followup-pr" {
		body["pullRequestNumber"] = d.Number
		if d.PRURL != "" {
			body["pullRequestUrl"] = d.PRURL
		}
	} else {
		body["issueNumber"] = d.Number
	}
	if d.Type == "followup-pr" && d.IssueNumber > 0 {
		body["issueNumber"] = d.IssueNumber
	}
	if number, rawURL := parsePullRequest(observedPR); number > 0 {
		body["pullRequestNumber"] = number
		if rawURL != "" {
			body["pullRequestUrl"] = rawURL
		}
	}
	if failure != "" {
		body["error"] = failure
	}
	return c.do(ctx, http.MethodPost, "/api/agents/"+url.PathEscape(c.agentName)+"/tasks/report", body, nil)
}

func parsePullRequest(value string) (int, string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, ""
	}
	if number, err := strconv.Atoi(value); err == nil && number > 0 {
		return number, ""
	}
	u, err := url.Parse(value)
	if err != nil || u.Path == "" {
		return 0, ""
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if strings.EqualFold(parts[i], "pull") {
			number, err := strconv.Atoi(parts[i+1])
			if err == nil && number > 0 {
				return number, value
			}
		}
	}
	return 0, ""
}

func (c *HTTPClient) PreLaunch(ctx context.Context, id string) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	if d.Type != "followup-pr" {
		return nil
	}
	pullRequest := &PullRequest{Repo: d.Repo, Number: d.Number}
	state, err := c.checkPullRequestState(ctx, pullRequest)
	if err != nil {
		return err
	}
	if state.Merged || strings.EqualFold(state.State, "closed") {
		if err := c.markPRFixStale(ctx, pullRequest, state); err != nil {
			return err
		}
		return fmt.Errorf("%w: upstream pull request %s#%d is no longer open", source.ErrStaleWork, d.Repo, d.Number)
	}
	return nil
}

func (c *HTTPClient) Report(ctx context.Context, id string, lifecycle source.Lifecycle) error {
	d, err := decodeWorkID(id)
	if err != nil {
		return err
	}
	switch lifecycle.Result {
	case source.ResultReady:
		outcome := "pr_updated"
		if d.Type == "implement" {
			outcome = "pr_opened"
		}
		return c.reportTaskWithPR(ctx, d, outcome, "", lifecycle.PR)
	case source.ResultBlocked:
		reportErr := c.reportTaskWithPR(ctx, d, "blocked", lifecycle.Error, lifecycle.PR)
		return errors.Join(reportErr, c.markPRFixBlocked(ctx, d, lifecycle.Error))
	case source.ResultFailed:
		reportErr := c.reportTaskWithPR(ctx, d, "failed", lifecycle.Error, lifecycle.PR)
		return errors.Join(reportErr, c.markPRFixBlocked(ctx, d, lifecycle.Error))
	default:
		return nil
	}
}

func (c *HTTPClient) Resolve(ctx context.Context, id string) error {
	if _, err := decodeWorkID(id); err != nil {
		return err
	}
	return c.SetStatus(ctx, id, "done")
}

// EncodeWorkID creates the opaque source ID carried by a CoderRun.
func EncodeWorkID(task Task) string {
	d := workDescriptor{Type: task.Type}
	if task.Issue != nil {
		d.IssueID = strings.TrimSpace(task.Issue.ID)
		d.IssueRepo = task.Issue.Repo
		d.IssueNumber = task.Issue.Number
		d.Repo = task.Issue.Repo
		d.Number = task.Issue.Number
	}
	if task.PullRequest != nil {
		d.Repo = task.PullRequest.Repo
		d.Number = task.PullRequest.Number
		d.PRURL = task.PullRequest.URL
	}
	payload, _ := json.Marshal(d)
	return base64.RawURLEncoding.EncodeToString(payload)
}

type issueState struct {
	IssueID string `json:"issueId"`
}

func (c *HTTPClient) resolveIssue(ctx context.Context, id string) (workDescriptor, error) {
	d, err := decodeWorkID(id)
	if err != nil {
		return workDescriptor{}, err
	}
	if d.IssueRepo == "" || d.IssueNumber <= 0 || (d.Type == "followup-pr" && d.IssueID != "") {
		return d, nil
	}
	path := "/api/issues/state?repo=" + url.QueryEscape(d.IssueRepo) + "&number=" + strconv.Itoa(d.IssueNumber)
	var state issueState
	if err := c.do(ctx, http.MethodGet, path, nil, &state); err != nil {
		return workDescriptor{}, err
	}
	if strings.TrimSpace(state.IssueID) == "" {
		return workDescriptor{}, errors.New("dispatch client: issue state response has no issue ID")
	}
	d.IssueID = state.IssueID
	return d, nil
}

func decodeWorkID(id string) (workDescriptor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return workDescriptor{}, fmt.Errorf("%w: %v", ErrUnknownWorkID, err)
	}
	var d workDescriptor
	if err := json.Unmarshal(payload, &d); err != nil || d.Type == "" || d.Repo == "" || d.Number <= 0 {
		return workDescriptor{}, ErrUnknownWorkID
	}
	if d.IssueID != "" && (d.IssueRepo == "" || d.IssueNumber <= 0) {
		return workDescriptor{}, ErrUnknownWorkID
	}
	return d, nil
}

func (c *HTTPClient) do(ctx context.Context, method, requestPath string, body any, response any) error {
	relative, err := url.Parse(requestPath)
	if err != nil || relative.IsAbs() {
		return fmt.Errorf("dispatch client: invalid request path")
	}
	requestURL := *c.baseURL
	basePath := requestURL.EscapedPath()
	if basePath == "" {
		basePath = "/"
	}
	rawPath := path.Join(basePath, relative.EscapedPath())
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return fmt.Errorf("dispatch client: invalid request path")
	}
	requestURL.Path = decodedPath
	requestURL.RawPath = rawPath
	if requestURL.EscapedPath() == requestURL.Path {
		requestURL.RawPath = ""
	}
	requestURL.RawQuery = relative.RawQuery
	if relative.Path == "/api/agents/"+url.PathEscape(c.agentName)+"/next-task" {
		query := requestURL.Query()
		query.Set("lane", c.queueLane)
		requestURL.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		message := strings.TrimSpace(string(data))
		message = strings.ReplaceAll(message, c.token, "[redacted]")
		return &APIError{StatusCode: resp.StatusCode, Message: message}
	}
	if response == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(response)
}

func taskWorkItem(task Task) (string, error) {
	if task.Type == "implement" && (task.Issue == nil || task.Issue.Repo == "" || task.Issue.Number <= 0) {
		return "", errors.New("dispatch client: implement task has no valid issue")
	}
	if task.Type == "followup-pr" && (task.PullRequest == nil || task.PullRequest.Repo == "" || task.PullRequest.Number <= 0) {
		return "", errors.New("dispatch client: followup task has no valid pull request")
	}
	if task.Type == "followup-pr" && task.Issue != nil && (task.Issue.Repo == "" || task.Issue.Number <= 0) {
		return "", errors.New("dispatch client: followup task has invalid issue")
	}
	return EncodeWorkID(task), nil
}

func taskRef(task Task) (string, int, error) {
	if task.Type == "implement" && task.Issue != nil {
		return task.Issue.Repo, task.Issue.Number, nil
	}
	if task.Type == "followup-pr" && task.PullRequest != nil {
		return task.PullRequest.Repo, task.PullRequest.Number, nil
	}
	return "", 0, errors.New("dispatch client: task has no valid reference")
}
