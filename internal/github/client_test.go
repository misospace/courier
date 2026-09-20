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
