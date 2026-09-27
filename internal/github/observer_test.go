package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/misospace/courier/internal/controller"
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
