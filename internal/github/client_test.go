package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/forge"
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

func TestCreateComment(t *testing.T) {
	t.Parallel()
	var method, reqPath, auth string
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if len(data) > 0 {
			if err := json.Unmarshal(data, &body); err != nil {
				t.Errorf("decode request: %v", err)
			}
		}
		method, reqPath, auth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":9,"body":"looks good"}`)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CreateComment(context.Background(), "acme", "demo", 7, "looks good"); err != nil {
		t.Fatalf("CreateComment: %v", err)
	}
	if method != http.MethodPost || reqPath != "/repos/acme/demo/issues/7/comments" {
		t.Fatalf("request = %s %s, want POST /repos/acme/demo/issues/7/comments", method, reqPath)
	}
	if body["body"] != "looks good" {
		t.Fatalf("body = %#v, want the comment body", body)
	}
	if auth != "Bearer secret" {
		t.Fatalf("authorization = %q, want the client token", auth)
	}
}

func TestCreateCommentAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"documentation_url":"https://docs.github.com/rest"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret")
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateComment(context.Background(), "acme", "demo", 7, "nope")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusForbidden {
		t.Fatalf("error = %v (%T), want a 403 APIError", err, err)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %q, want the status in the message", err)
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
		case "/api/v3/repos/acme/demo/commits/feature%2Fone/status":
			_, _ = io.WriteString(w, `{"total_count":0,"statuses":[]}`)
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

func TestWithBaseURLRetargetsCarryingCredential(t *testing.T) {
	var enterpriseAuth string
	enterprise := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/repos/acme/demo" {
			http.NotFound(w, r)
			return
		}
		enterpriseAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":3,"full_name":"acme/demo","default_branch":"main"}`)
	}))
	defer enterprise.Close()
	original := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("WithBaseURL must not leave requests going to the original base: %s", r.URL.Path)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer original.Close()

	client, err := NewClient(original.URL, "secret")
	if err != nil {
		t.Fatal(err)
	}
	rebased, err := client.WithBaseURL(enterprise.URL + "/api/v3")
	if err != nil {
		t.Fatal(err)
	}
	repo, err := rebased.GetRepository(context.Background(), "acme", "demo")
	if err != nil || repo.FullName != "acme/demo" {
		t.Fatalf("repository = %#v, %v", repo, err)
	}
	if enterpriseAuth != "Bearer secret" {
		t.Fatalf("authorization = %q, want the client credential carried over", enterpriseAuth)
	}

	if _, err := client.WithBaseURL("   "); err == nil {
		t.Fatal("an empty base URL must fail closed, not fall back to the GitHub.com default")
	}
	if _, err := client.WithBaseURL("ghes.example.test/api"); err == nil {
		t.Fatal("a base URL without scheme and host must be refused")
	}
}

func TestPullRequestsForHeadAllPagesQueriesHeadAndAllStates(t *testing.T) {
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
	pulls, err := client.PullRequestsForHeadAllPages(context.Background(), "acme", "demo", "octocat", "work")
	if err != nil {
		t.Fatalf("PullRequestsForHeadAllPages: %v", err)
	}
	if len(pulls) != 2 || pulls[0].Number != 12 || pulls[0].State != "open" || pulls[1].State != "merged" {
		t.Fatalf("pulls = %#v, want an open and a merged PR", pulls)
	}
	if !strings.Contains(rawQuery, "head=octocat:work") {
		t.Fatalf("query %q does not filter on head=headOwner:branch", rawQuery)
	}
	if !strings.Contains(rawQuery, "state=all") {
		t.Fatalf("query %q must use state=all so closed and merged PRs are visible", rawQuery)
	}
}

func TestPullRequestsForHeadAllPagesPaginate(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages = append(pages, r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "1" {
			pulls := make([]PullRequest, 100)
			for i := range pulls {
				pulls[i].Number = i + 1
			}
			_ = json.NewEncoder(w).Encode(pulls)
			return
		}
		_, _ = io.WriteString(w, `[{"number":101}]`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}

	pulls, err := client.PullRequestsForHeadAllPages(context.Background(), "acme", "demo", "acme", "work")
	if err != nil || len(pulls) != 101 {
		t.Fatalf("pulls = %d, %v; want 101 pull requests", len(pulls), err)
	}
	if got := strings.Join(pages, ","); got != "1,2" {
		t.Fatalf("pages = %q, want 1,2", got)
	}
}

func TestPullRequestsForHeadAllPagesStopsAtCeiling(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		pulls := make([]PullRequest, 100)
		for i := range pulls {
			pulls[i].Number = i + 1
		}
		_ = json.NewEncoder(w).Encode(pulls)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PullRequestsForHeadAllPages(context.Background(), "acme", "demo", "acme", "work")
	if err == nil || !strings.Contains(err.Error(), "exceeded 1000 pages") {
		t.Fatalf("error = %v, want page-ceiling error", err)
	}
	if requests != maxPullRequestPages {
		t.Fatalf("requests = %d, want %d", requests, maxPullRequestPages)
	}
}

func TestGetCheckRunsAndCommitStatusesPaginate(t *testing.T) {
	var requests []string
	var pages []string
	var perPages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RequestURI())
		pages = append(pages, r.URL.Query().Get("page"))
		perPages = append(perPages, r.URL.Query().Get("per_page"))
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/commits/abc/check-runs":
			switch r.URL.Query().Get("page") {
			case "1":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"id":1}]}`)
			case "2":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"id":2}]}`)
			}
		case "/repos/acme/demo/commits/abc/status":
			switch r.URL.Query().Get("page") {
			case "1":
				_, _ = io.WriteString(w, `{"total_count":2,"statuses":[{"state":"success"}]}`)
			case "2":
				_, _ = io.WriteString(w, `{"total_count":2,"statuses":[{"state":"pending"}]}`)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	checks, err := client.GetCheckRuns(context.Background(), "acme", "demo", "abc")
	if err != nil || len(checks.CheckRuns) != 2 {
		t.Fatalf("checks = %#v, %v", checks, err)
	}
	statuses, err := client.GetCommitStatuses(context.Background(), "acme", "demo", "abc")
	if err != nil || len(statuses.Statuses) != 2 {
		t.Fatalf("statuses = %#v, %v", statuses, err)
	}
	if got, want := strings.Join(pages, ","), "1,2,1,2"; got != want {
		t.Fatalf("pages = %q, want %q", got, want)
	}
	for _, perPage := range perPages {
		if perPage != "100" {
			t.Fatalf("per_page = %q, want 100", perPage)
		}
	}
	want := []string{"/repos/acme/demo/commits/abc/check-runs", "/repos/acme/demo/commits/abc/check-runs", "/repos/acme/demo/commits/abc/status", "/repos/acme/demo/commits/abc/status"}
	for i, request := range requests {
		if !strings.HasPrefix(request, want[i]+"?") {
			t.Fatalf("request %d = %q, want endpoint %q", i, request, want[i])
		}
	}
}

func TestCheckRunsRejectPrematureEmptyPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "1" {
			_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"id":1}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[]}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetCheckRuns(context.Background(), "acme", "demo", "abc")
	if err == nil || !strings.Contains(err.Error(), "ended after 1 of 2") {
		t.Fatalf("error = %v, want incomplete pagination error", err)
	}
}

func TestCheckRunsRejectChangingTotalCount(t *testing.T) {
	tests := []struct {
		name        string
		secondTotal int
	}{
		{name: "increases", secondTotal: 3},
		{name: "decreases", secondTotal: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Query().Get("page") {
				case "1":
					_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"id":1}]}`)
				case "2":
					_, _ = fmt.Fprintf(w, `{"total_count":%d,"check_runs":[{"id":2}]}`, tt.secondTotal)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "")
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.GetCheckRuns(context.Background(), "acme", "demo", "abc")
			if err == nil || !strings.Contains(err.Error(), "total_count changed from 2 to") {
				t.Fatalf("error = %v, want changing total_count error", err)
			}
		})
	}
}

func TestCommitStatusesRejectChangingTotalCount(t *testing.T) {
	tests := []struct {
		name        string
		secondTotal int
	}{
		{name: "increases", secondTotal: 3},
		{name: "decreases", secondTotal: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Query().Get("page") {
				case "1":
					_, _ = io.WriteString(w, `{"total_count":2,"statuses":[{"state":"success"}]}`)
				case "2":
					_, _ = fmt.Fprintf(w, `{"total_count":%d,"statuses":[{"state":"success"}]}`, tt.secondTotal)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "")
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.GetCommitStatuses(context.Background(), "acme", "demo", "abc")
			if err == nil || !strings.Contains(err.Error(), "total_count changed from 2 to") {
				t.Fatalf("error = %v, want changing total_count error", err)
			}
		})
	}
}

func TestCommitStatusesRejectPrematureEmptyPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "1" {
			_, _ = io.WriteString(w, `{"total_count":2,"statuses":[{"state":"success"}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"total_count":2,"statuses":[]}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetCommitStatuses(context.Background(), "acme", "demo", "abc")
	if err == nil || !strings.Contains(err.Error(), "ended after 1 of 2") {
		t.Fatalf("error = %v, want incomplete pagination error", err)
	}
}

func TestCommitStatusStateMapsGitHubState(t *testing.T) {
	for state, want := range map[string]controller.CheckState{
		"pending":      controller.CheckStatePending,
		"success":      controller.CheckStatePassed,
		"failure":      controller.CheckStateFailed,
		"error":        controller.CheckStateFailed,
		"future-state": controller.CheckStateUnknown,
		"":             controller.CheckStateUnknown,
	} {
		if got := commitStatusState(state); got != want {
			t.Fatalf("commitStatusState(%q) = %q, want %q", state, got, want)
		}
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
		{name: "timed out", status: "completed", conclusion: "timed_out", want: controller.CheckStateFailed},
		{name: "startup failure", status: "completed", conclusion: "startup_failure", want: controller.CheckStateFailed},
		{name: "unknown conclusion stays unknown", status: "completed", conclusion: "future-result", want: controller.CheckStateUnknown},
		{name: "missing conclusion stays unknown", status: "completed", want: controller.CheckStateUnknown},
		{name: "unrecognized status defers to conclusion", status: "weird", conclusion: "success", want: controller.CheckStatePassed},
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
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","draft":true,"head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"conclusion":"success"}]}`)
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":0,"statuses":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
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

func TestObserverCombinesChecksAndStatuses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"name":"build","status":"completed","conclusion":"success"}]}`)
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":1,"statuses":[{"context":"deploy","state":"failure"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(observation.Checks) != 2 || observation.Checks[0].State != controller.CheckStatePassed || observation.Checks[1].State != controller.CheckStateFailed {
		t.Fatalf("checks = %#v, want passed check and failed status", observation.Checks)
	}
	if observation.Head != "abc" {
		t.Fatalf("Head = %q, want the observed commit sha", observation.Head)
	}
	if observation.Checks[0].Name != "build" || observation.Checks[1].Name != "deploy" {
		t.Fatalf("check identities = %q/%q, want the stable external names", observation.Checks[0].Name, observation.Checks[1].Name)
	}
}

func TestObserverLaterPageFailureDoesNotPass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			switch r.URL.Query().Get("page") {
			case "1":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"status":"completed","conclusion":"success"}]}`)
			case "2":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"status":"completed","conclusion":"failure"}]}`)
			}
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":0,"statuses":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(observation.Checks) != 2 || observation.Checks[1].State != controller.CheckStateFailed {
		t.Fatalf("checks = %#v, want later failed check", observation.Checks)
	}
}

func TestObserverLaterPagePendingDoesNotPass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			switch r.URL.Query().Get("page") {
			case "1":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"status":"completed","conclusion":"success"}]}`)
			case "2":
				_, _ = io.WriteString(w, `{"total_count":2,"check_runs":[{"status":"pending"}]}`)
			}
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":0,"statuses":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(observation.Checks) != 2 || observation.Checks[1].State != controller.CheckStatePending {
		t.Fatalf("checks = %#v, want later pending check", observation.Checks)
	}
}

func TestObserverPendingStatusDoesNotPass(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":0,"check_runs":[]}`)
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":1,"statuses":[{"state":"pending"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(observation.Checks) != 1 || observation.Checks[0].State != controller.CheckStatePending {
		t.Fatalf("checks = %#v, want one pending status", observation.Checks)
	}
}

func TestObserverStatusOnlySuccessIsPassed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/pulls":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","sha":"abc","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case "/repos/acme/demo/commits/abc/check-runs":
			_, _ = io.WriteString(w, `{"total_count":0,"check_runs":[]}`)
		case "/repos/acme/demo/commits/abc/status":
			_, _ = io.WriteString(w, `{"total_count":1,"statuses":[{"state":"success"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/demo", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR == "" || len(observation.Checks) != 1 || observation.Checks[0].State != controller.CheckStatePassed {
		t.Fatalf("observation = %#v, want status-only passed observation", observation)
	}
}

// A fork pull request is listed on the base repository but its head lives on
// the fork, so the pulls query must carry the fork's owner while the pull
// request, its check runs, and its commit statuses are all read from the base
// repository for the head SHA.
func TestObserverForkHeadQueriesBaseRepoWithForkOwner(t *testing.T) {
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requests[r.URL.Path+"?"+r.URL.RawQuery]++
		switch {
		case r.URL.Path == "/repos/acme/demo/pulls" && r.URL.Query().Get("head") == "octocat:work":
			_, _ = io.WriteString(w, `[{"number":12,"state":"open","head":{"ref":"work","repo":{"full_name":"octocat/widgets"},"sha":"abc123"},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`)
		case r.URL.Path == "/repos/acme/demo/commits/abc123/check-runs":
			_, _ = io.WriteString(w, `{"total_count":1,"check_runs":[{"name":"build","status":"completed","conclusion":"success"}]}`)
		case r.URL.Path == "/repos/acme/demo/commits/abc123/status":
			_, _ = io.WriteString(w, `{"total_count":0,"statuses":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "octocat/widgets", Branch: "work", SHA: "abc123"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR != "12" {
		t.Fatalf("observation = %#v, want fork PR 12", observation)
	}
	queriedForkHead := false
	for path := range requests {
		if strings.HasPrefix(path, "/repos/acme/demo/pulls?") && strings.Contains(path, "head=octocat:work") && strings.Contains(path, "state=all") {
			queriedForkHead = true
		}
	}
	if !queriedForkHead {
		t.Fatalf("pulls requests = %#v, want a paginated base-repo query filtered to the fork head owner", requests)
	}
	if _, ok := requests["/repos/acme/demo/commits/abc123/check-runs?page=1&per_page=100"]; !ok {
		t.Fatalf("check-run requests = %#v, want check runs read from the base repo for the head SHA", requests)
	}
}

func TestObserverResolvesExistingPRHead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/demo/pulls/12" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"number":12,"head":{"ref":"feature/fix-pr","sha":"abc123","label":"octo:feature/fix-pr","repo":{"owner":{"login":"octo"},"full_name":"octo/demo"}},"base":{"ref":"main","sha":"def456","label":"acme:main","repo":{"owner":{"login":"acme"},"full_name":"acme/demo"}}}`)
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
	if got.Repo != "octo/demo" || got.Branch != "feature/fix-pr" || got.SHA != "abc123" {
		t.Fatalf("ResolveHead() = %#v, want the fork head identity distinct from the base repo", got)
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

func TestPullRequestsForHeadAllPagesAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"message":"internal error"}`)
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PullRequestsForHeadAllPages(context.Background(), "acme", "demo", "acme", "work")
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

func TestGetIssue(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/acme/demo/issues/502":
			_, _ = io.WriteString(w, `{"number":502,"title":"Package the assets","body":"## Acceptance\n- one\n- two","state":"open","html_url":"https://github.com/acme/demo/issues/502"}`)
		case "/repos/acme/demo/issues/503":
			// GitHub returns null for an absent issue body.
			_, _ = io.WriteString(w, `{"number":503,"title":"Empty body","body":null,"state":"open"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	issue, err := client.GetIssue(context.Background(), "acme", "demo", 502)
	if err != nil {
		t.Fatalf("GetIssue() error = %v", err)
	}
	if issue.Number != 502 || issue.Title != "Package the assets" || issue.Body != "## Acceptance\n- one\n- two" || issue.State != "open" {
		t.Fatalf("GetIssue() = %#v, want the full issue snapshot", issue)
	}
	if issue.PullRequest != nil {
		t.Fatalf("GetIssue() = %#v, want no pull-request marker on an issue", issue)
	}
	empty, err := client.GetIssue(context.Background(), "acme", "demo", 503)
	if err != nil {
		t.Fatalf("GetIssue() with null body error = %v", err)
	}
	if empty.Body != "" {
		t.Fatalf("null body must decode to the empty string, got %q", empty.Body)
	}
	for _, method := range methods {
		if method != http.MethodGet {
			t.Fatalf("issue read must be GET-only, saw %s", method)
		}
	}
	_, err = client.GetIssue(context.Background(), "acme", "demo", 404)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		t.Fatalf("missing issue error = %v (%T), want a 404 APIError", err, err)
	}
}

// stalledHandler answers a request only after the client has given up, which is
// how a forge that accepts a connection and then stops behaves: the client's
// own bound, not the caller's context, is what ends the call.
func stalledHandler(partialBody bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if partialBody {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"number":7,`)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
	}
}

func TestDefaultRequestTimeoutIsFinite(t *testing.T) {
	t.Parallel()
	client, err := New("token")
	if err != nil {
		t.Fatal(err)
	}
	doer, ok := client.httpClient.(*http.Client)
	if !ok {
		t.Fatalf("default transport = %T, want *http.Client", client.httpClient)
	}
	if doer.Timeout != defaultRequestTimeout {
		t.Fatalf("default transport timeout = %v, want %v", doer.Timeout, defaultRequestTimeout)
	}
	if defaultRequestTimeout <= 0 {
		t.Fatalf("defaultRequestTimeout = %v, want a finite bound", defaultRequestTimeout)
	}
}

func TestRequestBoundTerminatesStalledHeaders(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(stalledHandler(false))
	defer server.Close()
	client, err := newClient(server.URL, "token", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = client.GetPullRequest(context.Background(), "acme", "demo", 7)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled-header error = %v (%T), want a deadline error", err, err)
	}
	if !strings.Contains(err.Error(), "GitHub request:") {
		t.Fatalf("stalled-header error = %v, want the header wait to be the phase that failed", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("stalled headers held the read for %v, want the request bound to end it", elapsed)
	}
}

func TestRequestBoundTerminatesStalledBody(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(stalledHandler(true))
	defer server.Close()
	client, err := newClient(server.URL, "token", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = client.GetPullRequest(context.Background(), "acme", "demo", 7)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled-body error = %v (%T), want a deadline error", err, err)
	}
	if !strings.Contains(err.Error(), "read GitHub response:") {
		t.Fatalf("stalled-body error = %v, want the body read to be the phase that failed", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("stalled body held the read for %v, want the request bound to end it", elapsed)
	}
}

func TestInjectedHTTPDoerKeepsTheRequestBound(t *testing.T) {
	t.Parallel()
	blocking := &blockingHTTPDoer{}
	client, err := newClient("https://api.github.invalid/", "token", 100*time.Millisecond, blocking)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = client.GetPullRequest(context.Background(), "acme", "demo", 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("injected-doer error = %v (%T), want a deadline error", err, err)
	}
	if blocking.calls != 1 {
		t.Fatalf("injected doer calls = %d, want 1", blocking.calls)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("injected doer held the read for %v, want the request bound to end it", elapsed)
	}
}

// blockingHTTPDoer stands in for a test HTTPDoer that never answers. It blocks
// on the request context rather than on a timer so the client's own bound is
// the only thing that can release it.
type blockingHTTPDoer struct {
	calls int
}

func (d *blockingHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	d.calls++
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// stallingBodyHTTPDoer answers with headers and a body that never finishes,
// which only the request context can interrupt: an injected transport carries no
// timeout of its own, so the client's per-request bound is the sole bound on the
// body read.
type stallingBodyHTTPDoer struct {
	body *contextBody
}

func (d stallingBodyHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	d.body.ctx = req.Context()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       d.body,
	}, nil
}

type contextBody struct {
	ctx context.Context
}

func (b *contextBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *contextBody) Close() error {
	return nil
}

func TestInjectedHTTPDoerBoundsTheBodyRead(t *testing.T) {
	t.Parallel()
	doer := stallingBodyHTTPDoer{body: &contextBody{}}
	client, err := newClient("https://api.github.invalid/", "token", 100*time.Millisecond, doer)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = client.GetPullRequest(context.Background(), "acme", "demo", 7)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("injected-body error = %v (%T), want a deadline error", err, err)
	}
	if !strings.Contains(err.Error(), "read GitHub response:") {
		t.Fatalf("injected-body error = %v, want the body read to be the phase that failed", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("injected body held the read for %v, want the request bound to end it", elapsed)
	}
}

func TestEarlierCallerDeadlineStaysAuthoritative(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"number":7,"state":"open"}`)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	// The default 30-second bound is far longer than the response, so a read that
	// returns on the caller's deadline proves that deadline stayed in force.
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = client.GetPullRequest(ctx, "acme", "demo", 7)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller-deadline error = %v (%T), want the caller's deadline to end the read", err, err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("caller deadline was not authoritative: read returned after %v", elapsed)
	}
}

func TestBoundedRequestStillCompletes(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(50 * time.Millisecond):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"number":7,"title":"Bound the read","state":"open"}`)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	pull, err := client.GetPullRequest(context.Background(), "acme", "demo", 7)
	if err != nil {
		t.Fatalf("GetPullRequest() error = %v", err)
	}
	if pull.Number != 7 || pull.Title != "Bound the read" {
		t.Fatalf("GetPullRequest() = %#v, want the slow response decoded", pull)
	}
}

// TestAPIErrorTransientAvailability pins the availability classification
// the executor's blocked-external ending (#455) relies on: rate limits and
// server errors kept the forge read from completing, a 403 is transient
// only when its message or body names the rate limit or abuse protection,
// and other statuses are verdicts the forge did reach.
func TestAPIErrorTransientAvailability(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  *APIError
		want bool
	}{
		{"429", &APIError{StatusCode: 429}, true},
		{"500", &APIError{StatusCode: 500, Message: "Internal Server Error"}, true},
		{"502", &APIError{StatusCode: 502, Body: "Bad Gateway"}, true},
		{"403 rate limit message", &APIError{StatusCode: 403, Message: "API rate limit exceeded for this IP"}, true},
		{"403 secondary rate body", &APIError{StatusCode: 403, Body: "You have exceeded a secondary rate limit"}, true},
		{"403 abuse", &APIError{StatusCode: 403, Message: "ABUSE detection triggered"}, true},
		{"403 plain", &APIError{StatusCode: 403, Message: "Resource not accessible by integration"}, false},
		{"401", &APIError{StatusCode: 401, Message: "Bad credentials"}, false},
		{"404", &APIError{StatusCode: 404, Message: "Not Found"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.TransientAvailability(); got != tc.want {
				t.Fatalf("TransientAvailability() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTransportErrorTransientAvailability proves the transport-send failure
// the client wraps keeps its human-readable text, unwraps to the original
// error, and classifies as transient availability (#455).
func TestTransportErrorTransientAvailability(t *testing.T) {
	t.Parallel()
	inner := errors.New(`Get "https://api.example.com/repos/a/b": dial tcp: connection refused`)
	err := &transportError{err: fmt.Errorf("GitHub request: %w", inner)}
	if err.Error() != "GitHub request: "+inner.Error() {
		t.Fatalf("transportError text = %q, want the GitHub request prefix preserved", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Fatal("errors.Is(transportError, inner) = false, want the original unwrapped")
	}
	if !forge.IsTransientAvailability(err) {
		t.Fatal("forge.IsTransientAvailability(transportError) = false, want true")
	}
}
