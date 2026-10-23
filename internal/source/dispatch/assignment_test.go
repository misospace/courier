package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/misospace/courier/internal/source"
)

func followupTask(reasons []string) Task {
	return Task{
		Type:        "followup-pr",
		ShouldRun:   true,
		Issue:       &Issue{ID: "issue-id", Repo: "acme/widgets", Number: 41},
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42"},
		PRFixItem:   &PRFixItem{ID: "queue-item", Generation: 1},
		Reasons:     reasons,
	}
}

func linkedFollowupTask(reasons []string) Task {
	return Task{
		Type:        "followup-pr",
		ShouldRun:   true,
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 42, URL: "https://github.com/acme/widgets/pull/42"},
		Reasons:     reasons,
	}
}

func discoverAssignment(t *testing.T, task Task) source.WorkItem {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(task)
	}))
	defer server.Close()
	client, err := NewClientWithLane(server.URL, "worker", "normal", "token", time.Second)
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
	if len(items) != 1 {
		t.Fatalf("items = %#v, want exactly one", items)
	}
	if items[0].Mode != "fix-pr" {
		t.Fatalf("mode = %q, want fix-pr", items[0].Mode)
	}
	return items[0]
}

func TestHTTPClientDiscoverAssignment(t *testing.T) {
	overflowReasons := make([]string, 17)
	for i := range overflowReasons {
		overflowReasons[i] = fmt.Sprintf("reason-%d", i+1)
	}
	cases := []struct {
		name        string
		task        Task
		wantKind    string
		wantReasons []string
	}{
		{
			name:        "merge-conflict",
			task:        followupTask([]string{"Merge conflict detected: PR is CONFLICTING", "PR comment: resolve it"}),
			wantKind:    source.AssignmentMergeConflict,
			wantReasons: []string{"Merge conflict detected: PR is CONFLICTING", "PR comment: resolve it"},
		},
		{
			name:        "merge state change dirty is merge-conflict",
			task:        followupTask([]string{"PR review: APPROVED", "Merge state change: DIRTY"}),
			wantKind:    source.AssignmentMergeConflict,
			wantReasons: []string{"PR review: APPROVED", "Merge state change: DIRTY"},
		},
		{
			name:        "ci-failure",
			task:        followupTask([]string{"Failing check: unit-test (failure)"}),
			wantKind:    source.AssignmentCIFailure,
			wantReasons: []string{"Failing check: unit-test (failure)"},
		},
		{
			name:        "review-feedback",
			task:        followupTask([]string{"PR review: CHANGES_REQUESTED", "Review comment on src/a.go:3"}),
			wantKind:    source.AssignmentReviewFeedback,
			wantReasons: []string{"PR review: CHANGES_REQUESTED", "Review comment on src/a.go:3"},
		},
		{
			name:        "classification priority overrides entry order",
			task:        followupTask([]string{"Failing check: unit-test (failure)", "Merge conflict detected: PR is CONFLICTING"}),
			wantKind:    source.AssignmentMergeConflict,
			wantReasons: []string{"Failing check: unit-test (failure)", "Merge conflict detected: PR is CONFLICTING"},
		},
		{
			name:        "linked followup without prFixItem",
			task:        linkedFollowupTask([]string{"Failing check: unit-test (failure)"}),
			wantKind:    source.AssignmentCIFailure,
			wantReasons: []string{"Failing check: unit-test (failure)"},
		},
		{
			name:        "offer without reasons",
			task:        followupTask(nil),
			wantKind:    "",
			wantReasons: nil,
		},
		{
			name:        "whitespace-only reasons are dropped",
			task:        followupTask([]string{"   \t\n  "}),
			wantKind:    "",
			wantReasons: nil,
		},
		{
			name:        "control characters are replaced and trimmed",
			task:        followupTask([]string{"\x1b[2Jhello\x00world\r", "\x00"}),
			wantKind:    source.AssignmentOther,
			wantReasons: []string{"[2Jhello world"},
		},
		{
			name:        "exact duplicates collapse",
			task:        followupTask([]string{"a check fails", "a check fails"}),
			wantKind:    source.AssignmentCIFailure,
			wantReasons: []string{"a check fails"},
		},
		{
			name:        "more than sixteen reasons keep the first sixteen",
			task:        followupTask(overflowReasons),
			wantKind:    source.AssignmentOther,
			wantReasons: overflowReasons[:16],
		},
		{
			name:        "five thousand rune reason truncates to 1024 code points",
			task:        followupTask([]string{strings.Repeat("é", 5000)}),
			wantKind:    source.AssignmentOther,
			wantReasons: []string{strings.Repeat("é", 1024)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := discoverAssignment(t, tc.task)
			if item.Assignment.Kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q", item.Assignment.Kind, tc.wantKind)
			}
			if !reflect.DeepEqual(item.Assignment.Reasons, tc.wantReasons) {
				t.Fatalf("reasons = %#v, want %#v", item.Assignment.Reasons, tc.wantReasons)
			}
		})
	}
}

func TestHTTPClientAssignmentNeverForksWorkIdentity(t *testing.T) {
	base := followupTask(nil)
	withReasons := followupTask([]string{"Merge conflict detected: PR is CONFLICTING", "PR comment: resolve it"})
	if got, want := EncodeWorkID(withReasons), EncodeWorkID(base); got != want {
		t.Fatalf("work ID changed with reasons: %q != %q", got, want)
	}
	if got, want := WorkIdentity(EncodeWorkID(withReasons)), WorkIdentity(EncodeWorkID(base)); got != want {
		t.Fatalf("work identity changed with reasons: %q != %q", got, want)
	}
}
