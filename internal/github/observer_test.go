package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/forge"
)

func TestObserveReportsMergedPullRequestWhenNoneIsOpen(t *testing.T) {
	tests := []struct {
		name       string
		pulls      string
		wantPR     string
		wantMerged bool
	}{
		{
			name:       "merged",
			pulls:      `[{"number":9,"state":"closed","merged_at":"2026-09-26T19:30:00Z","head":{"ref":"work","sha":"abc"}}]`,
			wantPR:     "9",
			wantMerged: true,
		},
		{
			name:  "closed without merge",
			pulls: `[{"number":9,"state":"closed","merged_at":null,"head":{"ref":"work","sha":"abc"}}]`,
		},
		{
			name:  "merged pull request for another branch",
			pulls: `[{"number":9,"state":"closed","merged_at":"2026-09-26T19:30:00Z","head":{"ref":"other","sha":"abc"}}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/repos/acme/demo/pulls" {
					t.Errorf("unexpected request %s; a closed or merged PR needs no check lookup", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tt.pulls)
			}))
			defer server.Close()
			client, err := NewClient(server.URL, "token")
			if err != nil {
				t.Fatal(err)
			}

			observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work"})
			if err != nil {
				t.Fatalf("Observe() error = %v", err)
			}
			if observation.PR != tt.wantPR || observation.Merged != tt.wantMerged {
				t.Fatalf("observation = %#v, want PR %q merged %v", observation, tt.wantPR, tt.wantMerged)
			}
		})
	}
}

// recordingObserverTransport answers observation reads with a fixed body and
// records request destinations, so endpoint-routing tests assert the host
// without any network.
type recordingObserverTransport struct {
	requests []*http.Request
}

func (t *recordingObserverTransport) Do(req *http.Request) (*http.Response, error) {
	t.requests = append(t.requests, req.Clone(context.Background()))
	var body string
	switch path := req.URL.Path; {
	case strings.HasSuffix(path, "/pulls"):
		body = `[{"number":7,"state":"open","head":{"ref":"agent/work","sha":"headsha","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`
	case strings.HasSuffix(path, "/pulls/7"):
		body = `{"number":7,"state":"open","head":{"ref":"agent/work","sha":"headsha","repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}`
	case strings.HasSuffix(path, "/check-runs"):
		body = `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"success"}]}`
	case strings.HasSuffix(path, "/status"):
		body = `{"total_count":0,"statuses":[]}`
	default:
		body = `{}`
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func testForgeRegistry(t *testing.T, endpoint string) *forge.Registry {
	t.Helper()
	providers := fmt.Sprintf(`{"providers":[{"name":"ghe","type":"github","endpoint":%q,"credentials":{"forge-api":{"secretName":"forge-creds","key":"token"}},"serves":["acme/*"]}]}`, endpoint)
	registry, err := forge.Load([]byte(providers), "github")
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	return registry
}

func TestObserverRoutesReadsToRegistrationEndpoint(t *testing.T) {
	transport := &recordingObserverTransport{}
	client, err := New("token", transport)
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{Client: client, Registrations: testForgeRegistry(t, "https://ghes.example.test/api/v3")}
	ctx := context.Background()

	if _, err := observer.Observe(ctx, "acme/demo", controller.HeadRef{Branch: "agent/work"}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if _, err := observer.ResolveHead(ctx, &courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Repo: "acme/demo", Ref: 7}}); err != nil {
		t.Fatalf("ResolveHead() error = %v", err)
	}
	if _, err := observer.ReadPullRequest(ctx, forge.PullRequestRef{Repo: "acme/demo", Number: 7}); err != nil {
		t.Fatalf("ReadPullRequest() error = %v", err)
	}
	if len(transport.requests) == 0 {
		t.Fatal("no observation requests recorded")
	}
	for _, req := range transport.requests {
		if req.URL.Host != "ghes.example.test" {
			t.Fatalf("observation read for %s went to %q, want the registration host", req.URL.Path, req.URL.Host)
		}
		if !strings.HasPrefix(req.URL.Path, "/api/v3/repos/acme/demo") {
			t.Fatalf("observation read path = %q, want the registration API prefix", req.URL.Path)
		}
	}
}

func TestObserverUnsettableRepositoryNeverReachesDefaultHost(t *testing.T) {
	transport := &recordingObserverTransport{}
	client, err := New("token", transport)
	if err != nil {
		t.Fatal(err)
	}
	observer := Observer{Client: client, Registrations: testForgeRegistry(t, "https://ghes.example.test/api/v3")}
	ctx := context.Background()

	if _, err := observer.Observe(ctx, "other/demo", controller.HeadRef{Branch: "agent/work"}); err == nil {
		t.Fatal("Observe() on an unserved repository returned no error")
	}
	if _, err := observer.ResolveHead(ctx, &courierv1alpha1.CoderRun{Spec: courierv1alpha1.CoderRunSpec{Repo: "other/demo", Ref: 7}}); err == nil {
		t.Fatal("ResolveHead() on an unserved repository returned no error")
	}
	if len(transport.requests) != 0 {
		t.Fatalf("an unsettled repository made %d requests; it must make none", len(transport.requests))
	}
}
