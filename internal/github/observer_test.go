package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/controller"
)

// verifiedPull renders a pull-request record whose base and head repository
// identity both carry the run's own repository, the shape the identity-checked
// listing requires before a record may be read as this run's pull request.
func verifiedPull(number int, state, headRef, headSHA string) string {
	return fmt.Sprintf(`{"number":%d,"state":%q,"merged_at":null,"head":{"ref":%q,"sha":%q,"repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}`,
		number, state, headRef, headSHA)
}

// foreignPull renders a same-owner fork record: it claims the head selector
// owner:branch the run queries, but its head lives in another repository, so
// the forge response does not carry the run's own identity.
func foreignPull(number int, state, headRef, headSHA, headRepo string) string {
	return fmt.Sprintf(`{"number":%d,"state":%q,"merged_at":null,"head":{"ref":%q,"sha":%q,"repo":{"full_name":%q}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}`,
		number, state, headRef, headSHA, headRepo)
}

// observerServer serves the GitHub endpoints one observation reads: the head
// listing from pulls, the check runs and commit statuses of the observed head
// commit. It returns the client and the recorded head-selector query.
func observerServer(t *testing.T, pulls func(page string) string, checks string, statuses string) (*Client, *string) {
	t.Helper()
	headQuery := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/pulls"):
			headQuery = r.URL.RawQuery
			_, _ = io.WriteString(w, pulls(r.URL.Query().Get("page")))
		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			_, _ = io.WriteString(w, checks)
		case strings.HasSuffix(r.URL.Path, "/status"):
			_, _ = io.WriteString(w, statuses)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	return client, &headQuery
}

func TestObserveReportsPullRequestWithVerifiedIdentity(t *testing.T) {
	client, headQuery := observerServer(t,
		func(string) string { return "[" + verifiedPull(7, "open", "work", "sha-7") + "]" },
		`{"total_count":1,"check_runs":[{"name":"build","status":"completed","conclusion":"success"}]}`,
		`{"total_count":1,"statuses":[{"context":"legacy","state":"pending"}]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR != "7" || observation.Head != "sha-7" || observation.Draft || observation.Merged {
		t.Fatalf("observation = %#v, want PR 7 at sha-7, open and not draft", observation)
	}
	if len(observation.Checks) != 2 || observation.Checks[0].Name != "build" || observation.Checks[0].State != controller.CheckStatePassed ||
		observation.Checks[1].Name != "legacy" || observation.Checks[1].State != controller.CheckStatePending {
		t.Fatalf("checks = %#v, want the passed check run and the pending commit status", observation.Checks)
	}
	// A fork head is listed on the base repository but queried under the fork's
	// owner, so the observed head repository must reach the selector.
	if !strings.Contains(*headQuery, "head=acme:work") || !strings.Contains(*headQuery, "state=all") {
		t.Fatalf("query %q must filter on head=acme:work with state=all", *headQuery)
	}
}

func TestObserveQueriesAForkHeadUnderTheForkOwner(t *testing.T) {
	client, headQuery := observerServer(t,
		func(string) string {
			return "[" + foreignPull(11, "open", "work", "sha-11", "acme/fork") + "]"
		},
		`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	// The run's resolved head repository is the fork, so the fork's own record
	// carries the run's identity and is this run's pull request.
	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "acme/fork", Branch: "work"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR != "11" {
		t.Fatalf("observation = %#v, want fork PR 11", observation)
	}
	if !strings.Contains(*headQuery, "head=acme:work") {
		t.Fatalf("query %q must query the fork owner for a resolved fork head", *headQuery)
	}
}

func TestObserveFailsClosedOnForeignHeadClaimBesideTheRunsOwnPullRequest(t *testing.T) {
	client, _ := observerServer(t,
		func(string) string {
			return "[" + verifiedPull(7, "open", "work", "sha-7") + "," + foreignPull(4, "open", "work", "sha-4", "acme/fork") + "]"
		},
		`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err == nil {
		t.Fatalf("Observe() = %#v, want a foreign head claim to fail closed even beside the run's own pull request", observation)
	}
	if !strings.Contains(err.Error(), "unverified") || !strings.Contains(err.Error(), "work") {
		t.Fatalf("error = %v, want it to name the head branch and the unverified observation", err)
	}
}

func TestObserveFailsClosedWhenNoRecordCarriesTheRunsIdentity(t *testing.T) {
	client, _ := observerServer(t,
		func(string) string {
			return "[" + foreignPull(4, "open", "work", "sha-4", "acme/fork") + "]"
		},
		`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err == nil {
		t.Fatalf("Observe() = %#v, want a record from another repository to fail closed instead of reporting no pull request", observation)
	}
	if observation.PR != "" || observation.Merged {
		t.Fatalf("observation = %#v, want no pull request state published from another repository's record", observation)
	}
}

func TestObserveFailsClosedWhenTheResponseOmitsRepositoryIdentity(t *testing.T) {
	client, _ := observerServer(t,
		func(string) string {
			return `[{"number":7,"state":"open","head":{"ref":"work","sha":"sha-7"},"base":{"ref":"main"}}]`
		},
		`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err == nil {
		t.Fatalf("Observe() = %#v, want an identity-less record to fail closed", observation)
	}
	if observation.PR != "" {
		t.Fatalf("observation = %#v, want no pull request published from an identity-less record", observation)
	}
}

func TestObserveReadsEveryPageOfTheHeadListing(t *testing.T) {
	// The identity-checked listing paginates, so a pull request recorded beyond
	// the first page is still this run's: page one is filled to the page size
	// with closed records carrying the run's identity.
	pages := map[string][]PullRequest{"1": make([]PullRequest, 100), "2": {}}
	for i := range pages["1"] {
		pages["1"][i] = PullRequest{
			Number: i + 1,
			State:  "closed",
			Head:   Ref{Ref: "work", SHA: fmt.Sprintf("sha-%d", i+1), Repo: RepoRef{FullName: "acme/demo"}},
			Base:   Ref{Ref: "main", Repo: RepoRef{FullName: "acme/demo"}},
		}
	}
	pages["2"] = append(pages["2"], PullRequest{
		Number: 101,
		State:  "open",
		Head:   Ref{Ref: "work", SHA: "sha-101", Repo: RepoRef{FullName: "acme/demo"}},
		Base:   Ref{Ref: "main", Repo: RepoRef{FullName: "acme/demo"}},
	})
	client, _ := observerServer(t,
		func(page string) string {
			if page == "" {
				page = "1"
			}
			encoded, err := json.Marshal(pages[page])
			if err != nil {
				t.Fatalf("encode page %s: %v", page, err)
			}
			return string(encoded)
		},
		`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if observation.PR != "101" || observation.Head != "sha-101" {
		t.Fatalf("observation = %#v, want the second page's pull request 101", observation)
	}
}

func TestObserveRejectsAnInvalidHeadRepository(t *testing.T) {
	client, _ := observerServer(t,
		func(string) string { return "[]" }, `{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

	if _, err := (Observer{Client: client}).Observe(context.Background(), "acme/demo", controller.HeadRef{Repo: "not-a-repository", Branch: "work"}); err == nil {
		t.Fatal("Observe() error = nil, want an invalid head repository refused before any read")
	}
}

func TestObserveReportsMergedPullRequestWhenNoneIsOpen(t *testing.T) {
	tests := []struct {
		name       string
		pulls      string
		wantPR     string
		wantMerged bool
		wantErr    bool
	}{
		{
			name: "merged",
			pulls: `[{"number":9,"state":"closed","merged_at":"2026-09-26T19:30:00Z","head":{"ref":"work","sha":"abc",` +
				`"repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`,
			wantPR:     "9",
			wantMerged: true,
		},
		{
			name: "closed without merge",
			pulls: `[{"number":9,"state":"closed","merged_at":null,"head":{"ref":"work","sha":"abc",` +
				`"repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`,
		},
		{
			// A record the head selector surfaced that does not carry the run's
			// own head identity is a foreign claim under the identity-checked
			// listing: it fails the observation closed rather than being skipped
			// toward a "no pull request" claim.
			name: "merged pull request for another branch",
			pulls: `[{"number":9,"state":"closed","merged_at":"2026-09-26T19:30:00Z","head":{"ref":"other","sha":"abc",` +
				`"repo":{"full_name":"acme/demo"}},"base":{"ref":"main","repo":{"full_name":"acme/demo"}}}]`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := observerServer(t,
				func(string) string { return tt.pulls },
				`{"total_count":0,"check_runs":[]}`, `{"total_count":0,"statuses":[]}`)

			observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work"})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Observe() = %#v, want the foreign claim to fail closed", observation)
				}
				return
			}
			if err != nil {
				t.Fatalf("Observe() error = %v", err)
			}
			if observation.PR != tt.wantPR || observation.Merged != tt.wantMerged {
				t.Fatalf("observation = %#v, want PR %q merged %v", observation, tt.wantPR, tt.wantMerged)
			}
		})
	}
}
