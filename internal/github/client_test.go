package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
)

func TestPullRequestOperations(t *testing.T) {
	t.Parallel()
	var requests []struct {
		method string
		path   string
		body   map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(data) > 0 {
			if err := json.Unmarshal(data, &body); err != nil {
				t.Errorf("decode request: %v", err)
			}
		}
		requests = append(requests, struct {
			method string
			path   string
			body   map[string]any
		}{r.Method, r.URL.Path, body})
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			if r.Method == http.MethodPost {
				_, _ = io.WriteString(w, `{"number":7,"html_url":"https://github.com/acme/demo/pull/7","head":{"ref":"work","sha":"abc"}}`)
				return
			}
		case "/repos/acme/demo/pulls/7":
			_, _ = io.WriteString(w, `{"number":7,"title":"updated"}`)
			return
		case "/repos/acme/demo/issues/7/comments":
			_, _ = io.WriteString(w, `{"id":9,"body":"looks good"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "secret")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	created, err := client.CreatePullRequest(ctx, "acme", "demo", CreatePullRequestRequest{
		Title: "new PR", Head: "work", Base: "main", Body: "details", Draft: true,
	})
	if err != nil || created.Number != 7 {
		t.Fatalf("create = %#v, %v", created, err)
	}
	updated, err := client.UpdatePullRequest(ctx, "acme", "demo", 7, UpdatePullRequestRequest{Title: "updated"})
	if err != nil || updated.Title != "updated" {
		t.Fatalf("update = %#v, %v", updated, err)
	}
	comment, err := client.AddComment(ctx, "acme", "demo", 7, "looks good")
	if err != nil || comment.ID != 9 {
		t.Fatalf("comment = %#v, %v", comment, err)
	}
	if len(requests) != 3 {
		t.Fatalf("got %d requests, want 3", len(requests))
	}
	if requests[0].method != http.MethodPost || requests[0].body["head"] != "work" {
		t.Fatalf("unexpected create request: %#v", requests[0])
	}
	if requests[1].method != http.MethodPatch || requests[1].body["title"] != "updated" {
		t.Fatalf("unexpected update request: %#v", requests[1])
	}
	if requests[2].body["body"] != "looks good" {
		t.Fatalf("unexpected comment request: %#v", requests[2])
	}
}

func TestReadPullRequestAndChecks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.EscapedPath() {
		case "/api/v3/repos/acme/demo/pulls/7":
			_, _ = io.WriteString(w, `{"number":7,"head":{"sha":"abc"}}`)
		case "/api/v3/repos/acme/demo/commits/feature%2Fone/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"id":2,"name":"build","status":"completed","conclusion":"success","head_sha":"abc"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL+"/api/v3/", "")
	if err != nil {
		t.Fatal(err)
	}
	pr, err := client.GetPullRequest(context.Background(), "acme", "demo", 7)
	if err != nil || pr.Head.SHA != "abc" {
		t.Fatalf("pull request = %#v, %v", pr, err)
	}
	checks, err := client.GetChecks(context.Background(), "acme", "demo", "feature/one")
	if err != nil || checks.TotalCount != 1 || checks.CheckRuns[0].Conclusion != "success" {
		t.Fatalf("checks = %#v, %v", checks, err)
	}
}

func TestPullRequestsForHead(t *testing.T) {
	t.Parallel()
	var rawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/demo/pulls" {
			http.NotFound(w, r)
			return
		}
		rawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work"}},{"number":9,"state":"merged"}]`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	pulls, err := client.PullRequestsForHead(context.Background(), "acme", "demo", "work")
	if err != nil {
		t.Fatalf("PullRequestsForHead: %v", err)
	}
	if len(pulls) != 2 || pulls[0].Number != 12 || pulls[0].State != "open" || pulls[1].State != "merged" {
		t.Fatalf("pulls = %#v, want an open and a merged PR", pulls)
	}
	if !strings.Contains(rawQuery, "head=acme:work") {
		t.Fatalf("query %q does not filter on head=owner:branch", rawQuery)
	}
	if !strings.Contains(rawQuery, "state=all") {
		t.Fatalf("query %q must use state=all so closed and merged PRs are visible", rawQuery)
	}
}

func TestCheckStateMapsGitHubStatusAndConclusion(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		conclusion string
		want       controller.CheckState
	}{
		{name: "queued", status: "queued", want: controller.CheckStatePending},
		{name: "in progress", status: "in_progress", want: controller.CheckStatePending},
		{name: "success", status: "completed", conclusion: "success", want: controller.CheckStatePassed},
		{name: "skipped", status: "completed", conclusion: "skipped", want: controller.CheckStatePassed},
		{name: "neutral", status: "completed", conclusion: "neutral", want: controller.CheckStatePassed},
		{name: "cancelled", status: "completed", conclusion: "cancelled", want: controller.CheckStateFailed},
		{name: "unknown", status: "completed", conclusion: "future-result", want: controller.CheckStateFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkState(tt.status, tt.conclusion); got != tt.want {
				t.Fatalf("checkState(%q, %q) = %q, want %q", tt.status, tt.conclusion, got, tt.want)
			}
		})
	}
}

func TestObserverReportsDraftPullRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","draft":true,"head":{"ref":"work","sha":"abc"}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"conclusion":"success"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", "work")
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR != "12" || !observation.Draft {
		t.Fatalf("observation = %#v, want draft PR 12", observation)
	}
	if len(observation.Checks) != 1 || observation.Checks[0].State != controller.CheckStatePassed {
		t.Fatalf("checks = %#v, want one passed generic check state", observation.Checks)
	}
}

func TestObserverResolvesExistingPRHead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/demo/pulls/12" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"number":12,"head":{"ref":"feature/fix-pr"}}`)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	run := &courierv1alpha1.CoderRun{}
	run.Spec.Repo = "acme/demo"
	run.Spec.Ref = 12
	got, err := (Observer{Client: client}).ResolveHead(context.Background(), run)
	if err != nil {
		t.Fatalf("ResolveHead() error = %v", err)
	}
	if got != "feature/fix-pr" {
		t.Fatalf("ResolveHead() = %q, want feature/fix-pr", got)
	}
}

func TestObserverResolveHeadRejectsInvalidRepository(t *testing.T) {
	client, err := NewClient("http://invalid.example", "")
	if err != nil {
		t.Fatal(err)
	}
	run := &courierv1alpha1.CoderRun{}
	run.Spec.Repo = "invalid"
	run.Spec.Ref = 12
	_, err = (Observer{Client: client}).ResolveHead(context.Background(), run)
	if err == nil || !strings.Contains(err.Error(), "invalid repository") {
		t.Fatalf("ResolveHead() error = %v, want invalid repository error", err)
	}
}

func TestObserverResolveHeadRejectsEmptyHeadRef(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"number":12,"head":{}}`)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	run := &courierv1alpha1.CoderRun{}
	run.Spec.Repo = "acme/demo"
	run.Spec.Ref = 12
	_, err = (Observer{Client: client}).ResolveHead(context.Background(), run)
	if err == nil || !strings.Contains(err.Error(), "no head ref") {
		t.Fatalf("ResolveHead() error = %v, want no head ref error", err)
	}
}

func TestPullRequestsForHeadAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"message":"internal error"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PullRequestsForHead(context.Background(), "acme", "demo", "work")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("error = %v (%T), want a 500 APIError", err, err)
	}
}

func TestAPIErrorAndAuthentication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("X-GitHub-Api-Version"); got != apiVersion {
			t.Errorf("api version = %q", got)
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"resource not accessible"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetPullRequest(context.Background(), "acme", "demo", 1)
	var apiErr *APIError
	if !strings.Contains(err.Error(), "resource not accessible") || !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("error = %v (%T)", err, err)
	}
}
