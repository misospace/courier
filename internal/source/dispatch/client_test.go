package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

func TestHTTPClientDiscoverMapsTasksAndQueueLane(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/api/agents/worker/next-task" || r.URL.Query().Get("lane") != "normal" {
			t.Fatalf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Task{
			Type:      "implement",
			ShouldRun: true,
			Issue:     &Issue{Repo: "acme/widgets", Number: 42},
		})
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL+"/prefix/", "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Mode != "resolve-issue" || items[0].Repo != "acme/widgets" || items[0].Ref != 42 {
		t.Fatalf("items = %#v", items)
	}
	if items[0].ID == "" {
		t.Fatal("item has an empty opaque ID")
	}
}

func TestHTTPClientDiscoverMapsFollowupPR(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Task{
			Type:        "followup-pr",
			ShouldRun:   true,
			Issue:       &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 41},
			PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42"},
			PRFixItem:   &PRFixItem{ID: "queue-item", Generation: 1},
		})
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "open"}, nil
	}))
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Mode != "fix-pr" || items[0].Ref != 42 {
		t.Fatalf("items = %#v", items)
	}
	descriptor, err := decodeWorkID(items[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.IssueID != "" || descriptor.IssueNumber != 41 || descriptor.Number != 42 || descriptor.PRFixID != "queue-item" || descriptor.Generation != 1 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
}

func TestHTTPClientPRFixGenerationChangesWorkID(t *testing.T) {
	base := Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}}
	if got, want := EncodeWorkID(base), EncodeWorkID(base); got != want {
		t.Fatalf("same PR-fix item IDs differ: %q != %q", got, want)
	}
	changed := base
	changed.PRFixItem = &PRFixItem{ID: "queue-item", Generation: 2}
	if got, want := EncodeWorkID(base), EncodeWorkID(changed); got == want {
		t.Fatalf("generation change did not change work ID: %q", got)
	}
}

func TestHTTPClientLinkedFollowupFallbackIDIgnoresIssueIdentity(t *testing.T) {
	base := Task{
		Type:        "followup-pr",
		Issue:       &Issue{ID: "issue-id-1", Repo: "acme/widgets", Number: 41},
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42},
	}
	changedIssue := base
	changedIssue.Issue = &Issue{ID: "issue-id-2", Repo: "acme/widgets", Number: 41}
	if got, want := EncodeWorkID(base), EncodeWorkID(changedIssue); got != want {
		t.Fatalf("linked followup fallback IDs differ: %q != %q", got, want)
	}
	descriptor, err := decodeWorkID(EncodeWorkID(base))
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.IssueID != "" || descriptor.PRFixID != "" || descriptor.Generation != 0 {
		t.Fatalf("fallback descriptor carries mutation identity: %#v", descriptor)
	}
}

func TestHTTPClientPreLaunchAllowsOpenFollowup(t *testing.T) {
	var staleMarked bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pr-fix-queue/mark" {
			staleMarked = true
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "open"}, nil
	}))
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.PreLaunch(context.Background(), id); err != nil {
		t.Fatalf("PreLaunch() error = %v", err)
	}
	if staleMarked {
		t.Fatal("PreLaunch() marked open followup stale")
	}
}

func TestHTTPClientPreLaunchRejectsClosedFollowup(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pr-fix-queue/mark" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "closed"}, nil
	}))
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.PreLaunch(context.Background(), id); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("PreLaunch() error = %v, want ErrStaleWork", err)
	}
	if request["repo"] != "acme/widgets" || request["pr"] != float64(42) || request["status"] != "STALE" || request["generation"] != float64(1) {
		t.Fatalf("stale request = %#v", request)
	}
}

func TestHTTPClientPreLaunchRejectsMergedFollowup(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pr-fix-queue/mark" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "closed", Merged: true}, nil
	}))
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.PreLaunch(context.Background(), id); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("PreLaunch() error = %v, want ErrStaleWork", err)
	}
	if request["repo"] != "acme/widgets" || request["pr"] != float64(42) || request["status"] != "STALE" || request["note"] != "upstream pull request is merged" || request["generation"] != float64(1) {
		t.Fatalf("stale request = %#v", request)
	}
}

func TestHTTPClientDiscoverMarksClosedUnmergedFollowupStale(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agents/worker/next-task":
			_ = json.NewEncoder(w).Encode(Task{Type: "followup-pr", ShouldRun: true, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
		case "/api/pr-fix-queue/mark":
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "closed"}, nil
	}))
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 || request["note"] != "upstream pull request is closed without merge" {
		t.Fatalf("items/request = %#v/%#v", items, request)
	}
}

func TestHTTPClientDiscoverMarksMergedFollowupStale(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agents/worker/next-task":
			_ = json.NewEncoder(w).Encode(Task{Type: "followup-pr", ShouldRun: true, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
		case "/api/pr-fix-queue/mark":
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
		return PullRequestState{State: "closed", Merged: true}, nil
	}))
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("items = %#v, want no work", items)
	}
	if request["repo"] != "acme/widgets" || request["pr"] != float64(42) || request["status"] != "STALE" || request["generation"] != float64(1) {
		t.Fatalf("stale request = %#v", request)
	}
}

func TestHTTPClientDiscoverRejectsFollowupWithoutStateChecker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Task{Type: "followup-pr", ShouldRun: true, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Discover(context.Background()); !errors.Is(err, ErrPRStateCheckerNeeded) {
		t.Fatalf("Discover() error = %v, want ErrPRStateCheckerNeeded", err)
	}
}

func TestHTTPClientIdleReturnsNoWork(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Task{Type: "idle", ShouldRun: false})
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	items, err := client.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if items != nil {
		t.Fatalf("items = %#v, want nil", items)
	}
}

func TestHTTPClientClaimResolvesImplementIssueIDFromState(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/issues/state" {
			if r.URL.Query().Get("repo") != "acme/widgets" || r.URL.Query().Get("number") != "42" {
				t.Fatalf("state query = %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(issueState{IssueID: "opaque-issue"})
			return
		}
		if r.URL.Path != "/api/issues/claim" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "stale-client-id", Repo: "acme/widgets", Number: 42}})
	if err := client.Claim(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if request["issueId"] != "opaque-issue" || request["repoFullName"] != "acme/widgets" || request["issueNumber"] != float64(42) || request["agentName"] != "worker" {
		t.Fatalf("request = %#v", request)
	}
}

func TestHTTPClientImplementNeedsHumanUsesBlockedStatus(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/issues/state" {
			_ = json.NewEncoder(w).Encode(issueState{IssueID: "opaque-issue"})
			return
		}
		if r.URL.Path != "/api/issues/status" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	if err := client.SetStatus(context.Background(), id, "needs-human"); err != nil {
		t.Fatal(err)
	}
	if request["status"] != "blocked" || request["blockedReason"] == nil {
		t.Fatalf("request = %#v", request)
	}
}

func TestHTTPClientLinkedFollowupLifecycleDoesNotCallIssueEndpoints(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/api/issues/") {
			t.Fatalf("linked followup called issue endpoint %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{
		Type:        "followup-pr",
		Issue:       &Issue{ID: "opaque-issue", Repo: "acme/widgets", Number: 41},
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42"},
	})
	ctx := context.Background()
	if err := client.Claim(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := client.Release(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := client.SetStatus(ctx, id, "needs-human"); err != nil {
		t.Fatal(err)
	}
	if err := client.Resolve(ctx, id); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("linked followup made unexpected requests: %#v", paths)
	}
}

func TestHTTPClientReportMapsObservedPR(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/api/agents/worker/tasks/report" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL+"/prefix/", "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateInReview, Result: source.ResultReady, PR: "https://github.com/acme/widgets/pull/77"}); err != nil {
		t.Fatal(err)
	}
	if request["taskType"] != "implement" || request["outcome"] != "pr_opened" || request["repoFullName"] != "acme/widgets" || request["issueNumber"] != float64(42) || request["pullRequestNumber"] != float64(77) || request["pullRequestUrl"] != "https://github.com/acme/widgets/pull/77" {
		t.Fatalf("request = %#v", request)
	}
}

func TestHTTPClientReportCarriesIdempotencyKey(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/api/agents/worker/tasks/report" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL+"/prefix/", "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	key := "coderun/default/run-1/needs-human"
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateInReview, Result: source.ResultReady, PR: "https://github.com/acme/widgets/pull/77", IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}
	if request["idempotencyKey"] != key {
		t.Fatalf("request = %#v", request)
	}
}

func TestHTTPClientReportOmitsEmptyIdempotencyKey(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateInReview, Result: source.ResultReady}); err != nil {
		t.Fatal(err)
	}
	if _, ok := request["idempotencyKey"]; ok {
		t.Fatalf("request unexpectedly included idempotencyKey: %#v", request)
	}
}

func TestHTTPClientReportIgnoresLifecycleWithoutResult(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "implement", Issue: &Issue{Repo: "acme/widgets", Number: 42}})
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateInReview}); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("Report made unexpected requests: %#v", paths)
	}
}

func TestHTTPClientReportUsesTaskReportAndPRFixQueueOnlyForFollowups(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		if r.URL.Path == "/api/issues/state" || strings.HasPrefix(r.URL.Path, "/api/issues/") {
			t.Fatalf("followup report called issue endpoint %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", Issue: &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 41}, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultBlocked, Error: "blocked"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"/api/agents/worker/tasks/report", "/api/pr-fix-queue/mark"}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("request paths = %#v, want %#v", requests, want)
	}
}

func TestHTTPClientReportBlockedSkipRedundantPRFixQueueMark(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pr-fix-queue/mark" {
			t.Fatalf("skip-queue-mark blocked report made a queue request: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/agents/worker/tasks/report" {
			t.Fatalf("request = %s %s, want POST /api/agents/worker/tasks/report", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", Issue: &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 41}, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultBlocked, Error: "blocked", SkipRedundantPRFixQueueMark: true}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0]["taskType"] != "followup-pr" || requests[0]["outcome"] != "blocked" || requests[0]["error"] != "blocked" {
		t.Fatalf("requests = %#v, want exactly one blocked task report", requests)
	}
}

func TestHTTPClientReportMapsBlockedAndFailed(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/api/agents/worker/tasks/report"
		if len(requests)%2 == 1 {
			wantPath = "/api/pr-fix-queue/mark"
		}
		if r.Method != http.MethodPost || r.URL.Path != wantPath {
			t.Fatalf("request %d = %s %s, want POST %s", len(requests), r.Method, r.URL.Path, wantPath)
		}
		body, _ := io.ReadAll(r.Body)
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77, URL: "https://github.com/acme/widgets/pull/77"}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultBlocked, Error: "blocked"}); err != nil {
		t.Fatal(err)
	}
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultFailed, Error: "failed"}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 4 || requests[0]["taskType"] != "followup-pr" || requests[0]["outcome"] != "blocked" || requests[0]["error"] != "blocked" || requests[1]["status"] != "BLOCKED" || requests[1]["repo"] != "acme/widgets" || requests[1]["pr"] != float64(77) || requests[2]["taskType"] != "followup-pr" || requests[2]["outcome"] != "failed" || requests[2]["error"] != "failed" || requests[3]["status"] != "BLOCKED" || requests[3]["repo"] != "acme/widgets" || requests[3]["pr"] != float64(77) {
		t.Fatalf("requests = %#v", requests)
	}
}

func TestWorkIdentityIgnoresTaskURLForQueueBackedFollowups(t *testing.T) {
	attempt := func(url string, generation int) string {
		return EncodeWorkID(Task{Type: "followup-pr", Issue: &Issue{Repo: "acme/widgets", Number: 111}, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 113, URL: url}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: generation}})
	}
	viaPR := attempt("https://api.github.com/repos/acme/widgets/pulls/113", 1)
	viaJob := attempt("https://github.com/acme/widgets/actions/runs/36320266950/job/108622630814", 1)
	if viaPR == viaJob {
		t.Fatal("test setup: expected the two offers to encode differently")
	}
	if WorkIdentity(viaPR) != WorkIdentity(viaJob) {
		t.Fatalf("same attempt has different identities: %q vs %q", WorkIdentity(viaPR), WorkIdentity(viaJob))
	}
	if WorkIdentity(attempt("https://api.github.com/repos/acme/widgets/pulls/113", 2)) == WorkIdentity(viaPR) {
		t.Fatal("a new generation must be a new identity")
	}

	linked := EncodeWorkID(Task{Type: "followup-pr", Issue: &Issue{Repo: "acme/widgets", Number: 41}, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}})
	if WorkIdentity(linked) != linked {
		t.Fatalf("linked-PR follow-up identity = %q, want its opaque ID", WorkIdentity(linked))
	}
	implement := EncodeWorkID(Task{Type: "implement", Issue: &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 42}})
	if WorkIdentity(implement) != implement {
		t.Fatalf("implement identity = %q, want its opaque ID", WorkIdentity(implement))
	}
	if WorkIdentity("not-a-dispatch-id") != "not-a-dispatch-id" {
		t.Fatal("an undecodable ID must keep itself as its identity")
	}
}

func TestEncodeWorkIDDropsNonPullRequestURLs(t *testing.T) {
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 113, URL: "https://github.com/acme/widgets/actions/runs/1/job/2"}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 1}})
	d, err := decodeWorkID(id)
	if err != nil {
		t.Fatal(err)
	}
	if d.PRURL != "" {
		t.Fatalf("PRURL = %q, want a CI job URL dropped so it is never reported as pullRequestUrl", d.PRURL)
	}
}

func TestHTTPClientQueueBackedFollowupEchoesAttemptToken(t *testing.T) {
	var reports, marks []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		switch r.URL.Path {
		case "/api/agents/worker/tasks/report":
			reports = append(reports, request)
		case "/api/pr-fix-queue/mark":
			marks = append(marks, request)
		default:
			t.Fatalf("unexpected request path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 3}})
	for _, result := range []source.Result{source.ResultReady, source.ResultBlocked, source.ResultFailed} {
		if err := client.Report(context.Background(), id, source.Lifecycle{Result: result, Error: "why", IdempotencyKey: "run-1/" + string(result)}); err != nil {
			t.Fatalf("Report(%s) error = %v", result, err)
		}
	}

	want := map[string]any{"id": "queue-item", "generation": float64(3)}
	if len(reports) != 3 {
		t.Fatalf("reports = %#v, want one per result", reports)
	}
	for _, report := range reports {
		if !reflect.DeepEqual(report["prFixItem"], want) {
			t.Fatalf("report prFixItem = %#v, want %#v", report["prFixItem"], want)
		}
	}
	if len(marks) != 2 {
		t.Fatalf("marks = %#v, want one per blocked/failed result", marks)
	}
	for _, mark := range marks {
		if mark["generation"] != float64(3) {
			t.Fatalf("mark generation = %#v, want 3", mark["generation"])
		}
	}
}

func TestHTTPClientLinkedFollowupReportOmitsAttemptToken(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", Issue: &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 41}, PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}})
	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultReady}); err != nil {
		t.Fatal(err)
	}
	if _, ok := request["prFixItem"]; ok {
		t.Fatalf("linked-PR follow-up has no issued attempt, but report included prFixItem: %#v", request)
	}
}

func TestHTTPClientFollowupReportWithoutLinkedIssueOmitsIssueNumber(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}})
	if err := client.Report(context.Background(), id, source.Lifecycle{Result: source.ResultReady}); err != nil {
		t.Fatal(err)
	}
	if request["outcome"] != "pr_updated" {
		t.Fatalf("request = %#v", request)
	}
	if _, ok := request["issueNumber"]; ok {
		t.Fatalf("request unexpectedly included issueNumber: %#v", request)
	}
}

func TestHTTPClientRedactsTokenFromAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"secret-token"}`))
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Discover(context.Background())
	if err == nil || strings.Contains(err.Error(), "secret-token") || !strings.Contains(err.Error(), "redacted") {
		t.Fatalf("error = %v", err)
	}
}

func TestHTTPClientReportMapsPRFixGenerationMismatchToSuperseded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/worker/tasks/report" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path != "/api/pr-fix-queue/mark" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"PR fix queue item generation mismatch: expected generation 3 did not match the item's current generation"}`))
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 3}})
	if err := client.Report(context.Background(), id, source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultBlocked, Error: "blocked"}); !errors.Is(err, source.ErrSuperseded) {
		t.Fatalf("Report() error = %v, want ErrSuperseded", err)
	}
}

func TestHTTPClientReportOtherConflictIsNotSuperseded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/worker/tasks/report" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path != "/api/pr-fix-queue/mark" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"some other conflict"}`))
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 3}})
	err = client.Report(context.Background(), id, source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultBlocked, Error: "blocked"})
	if err == nil {
		t.Fatal("Report() error = nil, want a non-superseded error")
	}
	if errors.Is(err, source.ErrSuperseded) {
		t.Fatalf("Report() error = %v, a non-mismatch 409 must not map to ErrSuperseded", err)
	}
}

func TestHTTPClientReportServerErrorIsNotSuperseded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/worker/tasks/report" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path != "/api/pr-fix-queue/mark" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"PR fix queue item generation mismatch"}`))
	}))
	defer server.Close()

	client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}, PRFixItem: &PRFixItem{ID: "queue-item", Generation: 3}})
	err = client.Report(context.Background(), id, source.Lifecycle{State: source.StateNeedsHuman, Result: source.ResultBlocked, Error: "blocked"})
	if err == nil {
		t.Fatal("Report() error = nil, want a non-superseded error")
	}
	if errors.Is(err, source.ErrSuperseded) {
		t.Fatalf("Report() error = %v, a 5xx must not map to ErrSuperseded", err)
	}
}

func TestNewClientValidation(t *testing.T) {
	for name, args := range map[string][]string{
		"base URL": {"", "worker", "normal", "token"},
		"agent":    {"http://example.test", "", "normal", "token"},
		"token":    {"http://example.test", "worker", "normal", ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewClientWithLane(args[0], args[1], args[2], args[3], time.Second)
			if err == nil {
				t.Fatal("NewClientWithLane() accepted invalid configuration")
			}
		})
	}
	if _, err := NewClientWithLane("http://example.test", "worker", "normal", "token", 0); err != nil {
		t.Fatal(err)
	}
}
