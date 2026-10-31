package dispatch

// The three CI-park routes (/api/pr-fix-queue/cipark, cireserve, cirepair),
// their wire shapes, and their coded refusal reasons exercised below are
// Courier's proposed cross-repo contract for the Dispatch server's PR-fix
// queue deferral (connector side tracked as misospace/courier#495; the
// fence-less close on cirepair that releases a deferral whose opportunity
// ended without a reservation, as misospace/courier#534). Until the server
// lands, these fixtures are the pin: update this file in lockstep with the
// dispatch implementation, as internal/source/dispatch/contract_test.go does
// for its mirrored upstream allowlist.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

const (
	ciparkGen = 3
)

const (
	ciparkHead1 = "aaaaaaaa11111111bbbbbbbb22222222cccccccc"
	ciparkHead2 = "dddddddd33333333eeeeeeee44444444ffffffff"
)

// ciparkWorkItem is the queue-backed follow-up work the lifecycle acts on.
func ciparkWorkItem() source.WorkItem {
	return source.WorkItem{ID: EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77},
		PRFixItem:   &PRFixItem{ID: "queue-item-1", Generation: ciparkGen},
	})}
}

func ciparkImplementItem() source.WorkItem {
	return source.WorkItem{ID: EncodeWorkID(Task{
		Type:  "implement",
		Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 42},
	})}
}

// ciParkClient builds a Dispatch HTTP client over serverURL with the mixnet
// queue lane and an always-open pull request state checker.
func ciParkClient(t *testing.T, serverURL string) (*HTTPClient, *Adapter) {
	t.Helper()
	client, err := NewClientWithLane(serverURL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(_ context.Context, repo string, number int) (PullRequestState, error) {
		if repo != "acme/widgets" || number != 77 {
			t.Errorf("pull request check = %s#%d, want acme/widgets#77", repo, number)
		}
		return PullRequestState{State: "open"}, nil
	}))
	return client, New(client)
}

func ciParkDiscover(t *testing.T, adapter *Adapter) []source.WorkItem {
	t.Helper()
	items, err := adapter.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	return items
}

// TestDispatchCIParkWireBodies pins the exact request bodies of the three
// lifecycle endpoints: the queue item pivots by repo+pr+generation exactly
// like the mark/ack family, so no prFixId or handoutToken travels, the park
// carries no fence, only a repushed outcome carries a head, and the
// deferral's fence-less close (#534) travels on cirepair with no fence and
// no head, keyed by the park notice's own idempotency key.
func TestDispatchCIParkWireBodies(t *testing.T) {
	var mu sync.Mutex
	bodies := map[string][]map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var captured map[string]any
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&captured)
		mu.Lock()
		bodies[r.URL.Path] = append(bodies[r.URL.Path], captured)
		mu.Unlock()
		switch {
		case r.URL.Path == "/api/pr-fix-queue/cireserve":
			_ = json.NewEncoder(w).Encode(map[string]string{"fenceToken": "fence-one"})
		case strings.HasPrefix(r.URL.Path, "/api/pr-fix-queue/ci"):
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, _ := ciParkClient(t, server.URL)
	item := ciparkWorkItem()
	ctx := context.Background()

	if err := client.ParkCI(ctx, item.ID, source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-key"}); err != nil {
		t.Fatalf("ParkCI: %v", err)
	}
	if _, err := client.ReserveCIRepair(ctx, item.ID, source.CIRepairReservation{FenceToken: "fence-one", IdempotencyKey: "reserve-key", PR: "77", Head: ciparkHead1}); err != nil {
		t.Fatalf("ReserveCIRepair: %v", err)
	}
	if err := client.SettleCIRepair(ctx, item.ID, source.CIRepairOutcome{FenceToken: "fence-one", IdempotencyKey: "outcome-key", PR: "77", Head: ciparkHead2, Result: source.CIRepairRepushed}); err != nil {
		t.Fatalf("SettleCIRepair(repushed): %v", err)
	}
	if err := client.SettleCIRepair(ctx, item.ID, source.CIRepairOutcome{FenceToken: "fence-one", IdempotencyKey: "outcome-key", PR: "77", Result: source.CIRepairExhausted}); err != nil {
		t.Fatalf("SettleCIRepair(exhausted): %v", err)
	}
	if err := client.SettleCIRepair(ctx, item.ID, source.CIRepairOutcome{FenceToken: "fence-one", IdempotencyKey: "unsupported-key", PR: "77", Result: source.CIRepairUnsupported}); err != nil {
		t.Fatalf("SettleCIRepair(unsupported): %v", err)
	}
	// The fence-less close rides the same route with no fence and no head:
	// releasing a deferral settles no attempt, so nothing fenced travels.
	if err := client.ReleaseCIPark(ctx, item.ID, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-key"}); err != nil {
		t.Fatalf("ReleaseCIPark: %v", err)
	}

	park := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "head": ciparkHead1, "idempotencyKey": "park-key"}
	reserve := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "head": ciparkHead1, "idempotencyKey": "reserve-key", "fenceToken": "fence-one"}
	repushed := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "head": ciparkHead2, "idempotencyKey": "outcome-key", "fenceToken": "fence-one", "result": "repushed"}
	exhausted := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "idempotencyKey": "outcome-key", "fenceToken": "fence-one", "result": "exhausted"}
	unsupported := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "idempotencyKey": "unsupported-key", "fenceToken": "fence-one", "result": "unsupported"}
	release := map[string]any{"repo": "acme/widgets", "generation": float64(ciparkGen), "agentName": "worker", "idempotencyKey": "park-key", "result": "unsupported"}
	for endpoint, want := range map[string]map[string]any{
		"/api/pr-fix-queue/cipark":    park,
		"/api/pr-fix-queue/cireserve": reserve,
	} {
		if got := bodies[endpoint]; len(got) != 1 || !reflect.DeepEqual(got[0], want) {
			t.Errorf("%s body = %#v, want %#v", endpoint, got, want)
		}
	}
	if got := bodies["/api/pr-fix-queue/cirepair"]; len(got) != 4 {
		t.Fatalf("cirepair bodies = %d, want 4", len(got))
	} else if !reflect.DeepEqual(got[0], repushed) || !reflect.DeepEqual(got[1], exhausted) || !reflect.DeepEqual(got[2], unsupported) || !reflect.DeepEqual(got[3], release) {
		t.Errorf("cirepair bodies = %#v/%#v/%#v/%#v, want the repushed, exhausted, unsupported, and fence-less close shapes", got[0], got[1], got[2], got[3])
	}
	for endpoint, captures := range bodies {
		for _, captured := range captures {
			for key := range captured {
				if key == "prFixId" || key == "pr_fix_id" || key == "handoutToken" {
					t.Errorf("%s body carries forbidden key %q: %#v", endpoint, key, captured)
				}
			}
		}
	}
	if _, ok := bodies["/api/pr-fix-queue/cipark"][0]["fenceToken"]; ok {
		t.Error("park body carries a fenceToken")
	}
	if _, ok := bodies["/api/pr-fix-queue/cirepair"][1]["head"]; ok {
		t.Error("exhausted outcome carries a head")
	}
	if _, ok := bodies["/api/pr-fix-queue/cirepair"][3]["fenceToken"]; ok {
		t.Error("the fence-less close carries a fenceToken")
	}
	if _, ok := bodies["/api/pr-fix-queue/cirepair"][3]["head"]; ok {
		t.Error("the fence-less close carries a head")
	}
}

// TestDispatchCIParkAdapterLifecycle drives the full lifecycle through the
// adapter as source.CIParkLifecycle: the grant echoes the fence token the
// reservation pinned, and only the reservation re-reads the pull request
// state; a closed or merged pull request refuses the reservation without a
// POST.
func TestDispatchCIParkAdapterLifecycle(t *testing.T) {
	fixtures := map[string]struct {
		state    PullRequestState
		wantOpen bool
	}{
		"open":   {state: PullRequestState{State: "open"}, wantOpen: true},
		"closed": {state: PullRequestState{State: "closed"}},
		"merged": {state: PullRequestState{Merged: true}},
	}
	for name, tc := range fixtures {
		tc := tc
		t.Run(name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				switch r.URL.Path {
				case "/api/pr-fix-queue/cipark", "/api/pr-fix-queue/cirepair":
					w.WriteHeader(http.StatusNoContent)
				case "/api/pr-fix-queue/cireserve":
					_ = json.NewEncoder(w).Encode(map[string]string{"fenceToken": "fence-one"})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "not found", http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewClientWithLane(server.URL, "worker", "normal", "secret-token", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			f := &ciParkStateful{state: tc.state}
			client.WithPullRequestStateChecker(f)
			adapter := New(client)
			var lifecycle source.CIParkLifecycle = adapter
			item := ciparkWorkItem()
			ctx := context.Background()

			if err := lifecycle.ReportCIParked(ctx, item, source.CIParkNotice{PR: "https://github.com/acme/widgets/pull/77", Head: ciparkHead1, IdempotencyKey: "park-key"}); err != nil {
				t.Fatalf("ReportCIParked: %v", err)
			}
			grant, err := lifecycle.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-one", IdempotencyKey: "reserve-key", PR: "77", Head: ciparkHead1})
			if tc.wantOpen {
				if err != nil {
					t.Fatalf("ReserveCIRepair: %v", err)
				}
				if grant.FenceToken != "fence-one" {
					t.Fatalf("grant fence = %q, want the echoed reservation token", grant.FenceToken)
				}
			} else if !errors.Is(err, source.ErrStaleWork) {
				t.Fatalf("ReserveCIRepair error = %v, want ErrStaleWork", err)
			}
			if err := lifecycle.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence-one", IdempotencyKey: "outcome-key", PR: "77", Head: ciparkHead2, Result: source.CIRepairRepushed}); err != nil {
				t.Fatalf("ReportCIRepair: %v", err)
			}
			// The release rides the same lifecycle without a fence, and the
			// close POST is the only request beyond the park, reservation,
			// and outcome.
			if err := lifecycle.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-key"}); err != nil {
				t.Fatalf("ReleaseCIPark: %v", err)
			}
			if f.checks != 1 {
				t.Fatalf("pull request state re-reads = %d, want 1 (only the reservation re-validates)", f.checks)
			}
			if !tc.wantOpen {
				// The refusal fired before the reservation POST, so only the
				// park, the outcome, and the close reached the server.
				if requests != 3 {
					t.Fatalf("requests = %d, want 3 (park, outcome, and close only)", requests)
				}
			} else if requests != 4 {
				t.Fatalf("requests = %d, want 4 (park, reservation, outcome, and close)", requests)
			}
		})
	}
}

// ciParkStateful is a counting pull request state checker with a stub state.
type ciParkStateful struct {
	state  PullRequestState
	checks int
}

func (s *ciParkStateful) CheckPullRequestState(context.Context, string, int) (PullRequestState, error) {
	s.checks++
	return s.state, nil
}

// TestDispatchCIParkUnsupported pins the fails-closed gates: a client
// without the park lifecycle capability, implement runs, queue-less
// follow-ups, and out-of-range descriptors all report
// source.ErrUnsupportedCIPark without any API call, and the adapter's
// capability catalogue tracks what the client actually implements.
func TestDispatchCIParkUnsupported(t *testing.T) {
	notices := []struct {
		name string
		item source.WorkItem
	}{
		{"implement", ciparkImplementItem()},
		{"followup-without-queue-item", source.WorkItem{ID: EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77}})}},
	}
	for _, notice := range notices {
		notice := notice
		t.Run(notice.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if strings.HasPrefix(r.URL.Path, "/api/pr-fix-queue/ci") {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				http.Error(w, "not found", http.StatusNotFound)
			}))
			defer server.Close()
			_, adapter := ciParkClient(t, server.URL)
			item := notice.item
			ctx := context.Background()

			if err := adapter.ReportCIParked(ctx, item, source.CIParkNotice{PR: "42", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrUnsupportedCIPark) {
				t.Errorf("ReportCIParked() = %v, want ErrUnsupportedCIPark", err)
			}
			if grant, err := adapter.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "42", Head: ciparkHead1}); !errors.Is(err, source.ErrUnsupportedCIPark) || grant != (source.CIRepairGrant{}) {
				t.Errorf("ReserveCIRepair() = (%#v, %v), want no grant and ErrUnsupportedCIPark", grant, err)
			}
			if err := adapter.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "42", Head: ciparkHead1, Result: source.CIRepairUnsupported}); !errors.Is(err, source.ErrUnsupportedCIPark) {
				t.Errorf("ReportCIRepair() = %v, want ErrUnsupportedCIPark", err)
			}
			if err := adapter.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "42", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrUnsupportedCIPark) {
				t.Errorf("ReleaseCIPark() = %v, want ErrUnsupportedCIPark", err)
			}
			if requests != 0 {
				t.Errorf("requests = %d, want 0 (the gate refuses before any API call)", requests)
			}
		})
	}

	// An over-range descriptor is decodeable but invalid: a plain
	// invalid-descriptor error matching AckHandout, never the per-work-kind
	// ErrUnsupportedCIPark, which would permanently mislabel the connector as
	// unable to park queue-backed work.
	t.Run("invalid-descriptor-plain-error", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			prFixItem  *PRFixItem
			pullNumber int
		}{
			{name: "generation-over-limit", prFixItem: &PRFixItem{ID: "queue-item-1", Generation: maxSafeInteger + 1}, pullNumber: 77},
			{name: "number-over-limit", prFixItem: &PRFixItem{ID: "queue-item-1", Generation: ciparkGen}, pullNumber: maxSafeInteger + 1},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				id := EncodeWorkID(Task{Type: "followup-pr", PullRequest: &PullRequest{Repo: "acme/widgets", Number: tc.pullNumber}, PRFixItem: tc.prFixItem})
				client, _ := ciParkClient(t, "http://localhost")
				if err := client.ParkCI(context.Background(), id, source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); errors.Is(err, source.ErrUnsupportedCIPark) || !strings.Contains(err.Error(), "invalid PR-fix descriptor") {
					t.Errorf("ParkCI(over-range) = %v, want a plain invalid-descriptor error", err)
				}
				if err := client.ReleaseCIPark(context.Background(), id, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); errors.Is(err, source.ErrUnsupportedCIPark) || !strings.Contains(err.Error(), "invalid PR-fix descriptor") {
					t.Errorf("ReleaseCIPark(over-range) = %v, want a plain invalid-descriptor error", err)
				}
			})
		}
	})

	t.Run("client-without-capability", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		em := connector.NewEmitter(reg, nil)
		adapter := New(&basicClient{}, WithConnectorDiagnostics(em))
		var lifecycle source.CIParkLifecycle = adapter
		item := source.WorkItem{ID: "work-123"}
		ctx := context.Background()
		if err := lifecycle.ReportCIParked(ctx, item, source.CIParkNotice{PR: "1", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Errorf("ReportCIParked() = %v, want ErrUnsupportedCIPark", err)
		}
		if _, err := lifecycle.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "1", Head: ciparkHead1}); !errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Errorf("ReserveCIRepair() = %v, want ErrUnsupportedCIPark", err)
		}
		if err := lifecycle.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "1", Result: source.CIRepairUnsupported}); !errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Errorf("ReportCIRepair() = %v, want ErrUnsupportedCIPark", err)
		}
		if err := lifecycle.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "1", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Errorf("ReleaseCIPark() = %v, want ErrUnsupportedCIPark", err)
		}
		if _, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": defaultConnectorName, "capability": connector.CapabilityCIParkLifecycle, "state": connector.CapabilityUnsupported,
		}); !ok {
			t.Error("capability catalogue did not report ci-park-lifecycle unsupported")
		}
	})

	t.Run("corrupt-id-is-decode-error", func(t *testing.T) {
		client, _ := ciParkClient(t, "http://localhost")
		if err := client.ParkCI(context.Background(), "not-a-work-id", source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); errors.Is(err, source.ErrUnsupportedCIPark) || !errors.Is(err, ErrUnknownWorkID) {
			t.Errorf("ParkCI(corrupt id) = %v, want a decode error, not ErrUnsupportedCIPark", err)
		}
	})
}

// TestDispatchCIParkValidationFailures pins the client-side validations: a
// mismatched pull request, an invalid head, a missing idempotency key or
// fence, or a head on a no-repush outcome fails with a plain error and makes
// no API call, so a retried transition never bypasses the fence.
func TestDispatchCIParkValidationFailures(t *testing.T) {
	item := ciparkWorkItem()
	head := ciparkHead1
	cases := []struct {
		name string
		call func(*HTTPClient) error
	}{
		{"pull-request-mismatch-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: "99", Head: head, IdempotencyKey: "k"})
		}},
		{"pull-request-mismatch-reserve", func(c *HTTPClient) error {
			_, err := c.ReserveCIRepair(context.Background(), item.ID, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "99", Head: head})
			return err
		}},
		{"pull-request-mismatch-outcome", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "99", Result: source.CIRepairExhausted})
		}},
		{"pull-request-missing-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{Head: head, IdempotencyKey: "k"})
		}},
		{"pull-request-repo-mismatch-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: "https://github.com/evil/other/pull/77", Head: head, IdempotencyKey: "k"})
		}},
		{"pull-request-repo-mismatch-reserve", func(c *HTTPClient) error {
			_, err := c.ReserveCIRepair(context.Background(), item.ID, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "https://github.com/evil/other/pull/77", Head: head})
			return err
		}},
		{"pull-request-repo-mismatch-outcome", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "https://github.com/evil/other/pull/77", Result: source.CIRepairExhausted})
		}},
		{"head-short-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: "77", Head: "aa11", IdempotencyKey: "k"})
		}},
		{"head-not-hex-reserve", func(c *HTTPClient) error {
			_, err := c.ReserveCIRepair(context.Background(), item.ID, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: "zzzzzzz"})
			return err
		}},
		{"head-whitespace-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: "77", Head: "       ", IdempotencyKey: "k"})
		}},
		{"idempotency-key-empty-outcome", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", PR: "77", Result: source.CIRepairExhausted})
		}},
		{"idempotency-key-blank-reserve", func(c *HTTPClient) error {
			_, err := c.ReserveCIRepair(context.Background(), item.ID, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "   ", PR: "77", Head: head})
			return err
		}},
		{"idempotency-key-over-limit-park", func(c *HTTPClient) error {
			return c.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: "77", Head: head, IdempotencyKey: strings.Repeat("k", maxCIIdempotencyKeyLength+1)})
		}},
		{"fence-empty-reserve", func(c *HTTPClient) error {
			_, err := c.ReserveCIRepair(context.Background(), item.ID, source.CIRepairReservation{IdempotencyKey: "k", PR: "77", Head: head})
			return err
		}},
		{"fence-blank-outcome", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: " ", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted})
		}},
		{"repush-without-head", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairRepushed})
		}},
		{"exhausted-with-head", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: head, Result: source.CIRepairExhausted})
		}},
		{"unsupported-with-head", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: head, Result: source.CIRepairUnsupported})
		}},
		{"unknown-result", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairResult("fixed")})
		}},
		{"empty-result", func(c *HTTPClient) error {
			return c.SettleCIRepair(context.Background(), item.ID, source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77"})
		}},
		// The fence-less close is validated on the same descriptor, pull
		// request, and key gates, and none of the fence gates: there is no
		// fence to validate, and no validation may require one.
		{"release-pull-request-mismatch", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{PR: "99", Head: head, IdempotencyKey: "k"})
		}},
		{"release-pull-request-missing", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{Head: head, IdempotencyKey: "k"})
		}},
		{"release-pull-request-repo-mismatch", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{PR: "https://github.com/evil/other/pull/77", Head: head, IdempotencyKey: "k"})
		}},
		{"release-idempotency-key-empty", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{PR: "77", Head: head})
		}},
		{"release-idempotency-key-blank", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{PR: "77", Head: head, IdempotencyKey: "   "})
		}},
		{"release-idempotency-key-over-limit", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), item.ID, source.CIParkRelease{PR: "77", Head: head, IdempotencyKey: strings.Repeat("k", maxCIIdempotencyKeyLength+1)})
		}},
		{"release-corrupt-work-id", func(c *HTTPClient) error {
			return c.ReleaseCIPark(context.Background(), "not-a-work-id", source.CIParkRelease{PR: "77", Head: head, IdempotencyKey: "k"})
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				http.Error(w, "unexpected call", http.StatusInternalServerError)
			}))
			defer server.Close()
			client, _ := ciParkClient(t, server.URL)
			if err := tc.call(client); err != nil {
				if errors.Is(err, source.ErrUnsupportedCIPark) || errors.Is(err, source.ErrSuperseded) || errors.Is(err, source.ErrStaleWork) {
					t.Fatalf("error = %v, want a plain validation error", err)
				}
			} else {
				t.Fatal("invalid payload was accepted")
			}
			if requests != 0 {
				t.Fatalf("requests = %d, want 0 (failed closed before send)", requests)
			}
		})
	}
}

// TestDispatchCIParkPullRequestIdentity pins the local identity guard: a
// pull request spelled as a URL must name the work item's repository,
// case-insensitively — a same-number pull request from a different repo is
// refused before any request — while a bare number carries no repository
// identity and leans on the item's repo alone.
func TestDispatchCIParkPullRequestIdentity(t *testing.T) {
	item := ciparkWorkItem()
	cases := []struct {
		name    string
		pr      string
		wantErr bool
	}{
		{"bare number", "77", false},
		{"same repository", "https://github.com/acme/widgets/pull/77", false},
		{"same repository, different case", "https://github.com/ACME/Widgets/pull/77", false},
		{"same number, different repository", "https://github.com/evil/other/pull/77", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client, _ := ciParkClient(t, server.URL)
			err := client.ParkCI(context.Background(), item.ID, source.CIParkNotice{PR: tc.pr, Head: ciparkHead1, IdempotencyKey: "k"})
			if tc.wantErr != (err != nil) {
				t.Fatalf("ParkCI(pr=%q) error = %v, wantErr = %v", tc.pr, err, tc.wantErr)
			}
			if got := requests; got != 0 && tc.wantErr {
				t.Fatalf("requests = %d, want 0 (refused before send)", got)
			} else if got != 1 && !tc.wantErr {
				t.Fatalf("requests = %d, want 1", got)
			}
		})
	}
}

// TestDispatchCIParkGenerationMismatch pins the exact-once identity signal:
// a generation-fenced refusal on any of the three lifecycle endpoints maps
// to source.ErrSuperseded and counts identity/generation-advanced exactly
// once, never as a protocol error.
func TestDispatchCIParkGenerationMismatch(t *testing.T) {
	cases := []struct {
		name string
		call func(*Adapter) error
	}{
		{"park", func(a *Adapter) error {
			return a.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
		{"reserve", func(a *Adapter) error {
			_, err := a.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
			return err
		}},
		{"outcome", func(a *Adapter) error {
			return a.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted})
		}},
		{"close", func(a *Adapter) error {
			return a.ReleaseCIPark(context.Background(), ciparkWorkItem(), source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusConflict, "generation mismatch: expected 4"))
			if err := tc.call(f.adapter); !errors.Is(err, source.ErrSuperseded) {
				t.Fatalf("err = %v, want ErrSuperseded", err)
			}
			if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityGenerationAdvanced); got != 1 {
				t.Fatalf("identity/generation-advanced counter = %v, want exactly 1", got)
			}
			if total := totalProtocolErrors(t, f); total != 0 {
				t.Fatalf("superseded lifecycle exchange counted %v protocol-error diagnostics, want 0", total)
			}
		})
	}
}

func openChecker() func(*HTTPClient) {
	return func(c *HTTPClient) {
		c.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
			return PullRequestState{State: "open"}, nil
		}))
	}
}

func ciParkRefused(t *testing.T, status int, body string) func(*diagnosticsFixture, http.ResponseWriter, *http.Request) {
	return func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/pr-fix-queue/ci") {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// TestDispatchCIParkCodedRefusals pins the coded {"reason": "<code>"} 409
// refusal vocabulary and its census: a recognizable code counts one
// identity diagnostic under the code itself, while other statuses — and
// unrecognized codes — keep only the protocol-error class the exchange
// would carry.
func TestDispatchCIParkCodedRefusals(t *testing.T) {
	calls := []struct {
		endpoint string
		call     func(*Adapter) error
	}{
		{"cipark", func(a *Adapter) error {
			return a.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
		{"cireserve", func(a *Adapter) error {
			_, err := a.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
			return err
		}},
		{"cirepair", func(a *Adapter) error {
			return a.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted})
		}},
		{"cirepair-close", func(a *Adapter) error {
			return a.ReleaseCIPark(context.Background(), ciparkWorkItem(), source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
	}
	for _, call := range calls {
		call := call
		t.Run(call.endpoint, func(t *testing.T) {
			t.Run("mappable", func(t *testing.T) {
				for _, tc := range []struct {
					reason string
					want   error
				}{
					{"fence-held", source.ErrSuperseded},
					{"fence-gone", source.ErrSuperseded},
					{"opportunity-missing", source.ErrStaleWork},
					{"head-superseded", source.ErrStaleWork},
					{"fence-settled", source.ErrStaleWork},
				} {
					f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusConflict, fmt.Sprintf(`{"reason":%q}`, tc.reason)))
					err := call.call(f.adapter)
					if !errors.Is(err, tc.want) {
						t.Fatalf("reason %q err = %v, want %v", tc.reason, err, tc.want)
					}
					// The census: the coded refusal reclassifies into a source
					// outcome and the census reason is the refusal code
					// itself, counted once under identity; the absorbed
					// deferred exchange is never also counted as a
					// protocol-error class.
					if got := diagnosticsCounter(t, f, connector.SignalIdentity, tc.reason); got != 1 {
						t.Fatalf("reason %q identity/%s counter = %v, want exactly 1", tc.reason, tc.reason, got)
					}
					if total := totalProtocolErrors(t, f); total != 0 {
						t.Fatalf("reason %q reclassified refusal counted %v protocol-error diagnostics, want 0 (absorbed)", tc.reason, total)
					}
				}
			})
			t.Run("unmappable", func(t *testing.T) {
				for _, tc := range []struct {
					name   string
					status int
					body   string
				}{
					{"plain-body-409", http.StatusConflict, "fence conflict"},
					{"unknown-code-409", http.StatusConflict, `{"reason":"something-else"}`},
					// A bare 404 on any park-family route reclassifies into
					// the permanent unsupported sentinel: the refusal
					// vocabulary is 409-coded, so a 404 is version-skew
					// evidence the deployment adopted none of the routes.
					// It is pinned by
					// TestDispatchCIParkRoute404LatchesUnsupported instead
					// of here.
					{"not-found", http.StatusNotFound, "no such queue item"},
					{"server-error", http.StatusInternalServerError, "boom"},
				} {
					if tc.name == "not-found" {
						continue
					}
					f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, tc.status, tc.body))
					err := call.call(f.adapter)
					if err == nil || errors.Is(err, source.ErrSuperseded) || errors.Is(err, source.ErrStaleWork) || errors.Is(err, source.ErrUnsupportedCIPark) {
						t.Fatalf("%s %s err = %v, want an unmapped plain error", tc.name, call.endpoint, err)
					}
					var apiErr *APIError
					if !errors.As(err, &apiErr) || apiErr.StatusCode != tc.status {
						t.Fatalf("%s err = %v, want an APIError with status %d", tc.name, err, tc.status)
					}
					// The census: an exchange the park family cannot
					// reclassify counts exactly one protocol error under its
					// HTTP class and never fabricates an identity signal —
					// none of the coded refusal's identity census reasons
					// gains a count either.
					wantReason := reasonHTTP4xx
					if tc.status >= http.StatusInternalServerError {
						wantReason = reasonHTTP5xx
					}
					if got := diagnosticsCounter(t, f, connector.SignalProtocolError, wantReason); got != 1 {
						t.Errorf("%s: protocol-error/%s counter = %v, want exactly 1", tc.name, wantReason, got)
					}
					if total := totalProtocolErrors(t, f); total != 1 {
						t.Errorf("%s: total protocol-error diagnostics = %v, want exactly 1", tc.name, total)
					}
					for _, reason := range []string{
						reasonIdentityFenceHeld, reasonIdentityFenceGone, reasonIdentityOpportunityMissing,
						reasonIdentityHeadSuperseded, reasonIdentityFenceSettled,
					} {
						if got := diagnosticsCounter(t, f, connector.SignalIdentity, reason); got != -1 {
							t.Errorf("%s: fabricated identity/%s count = %v, want none", tc.name, reason, got)
						}
					}
				}
			})
		})
	}
}

// TestDispatchCIParkReleaseRouteAbsentIsUnsupported pins the advisory
// release's version-skew posture (#534): a bare 404 on the fence-less close
// reclassifies into the permanent source.ErrUnsupportedCIPark. Under the
// pinned contract a deferral that does not exist is a 409-coded refusal, so
// a bare 404 is evidence the deployment has not adopted the release at all
// (an older Dispatch), and retrying it forever would hold Courier's terminal
// lifecycle behind a close the server can never take. The exchange's
// deferred 4xx class is counted once by the reclassification site, and the
// observation latches: every park-family exchange afterwards short-circuits
// with the sentinel instead of churning the same 404.
func TestDispatchCIParkReleaseRouteAbsentIsUnsupported(t *testing.T) {
	f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusNotFound, "no such queue item"))
	if err := f.adapter.ReleaseCIPark(context.Background(), ciparkWorkItem(), source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrUnsupportedCIPark) {
		t.Fatalf("ReleaseCIPark() = %v, want ErrUnsupportedCIPark", err)
	}
	if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP4xx); got != 1 {
		t.Fatalf("protocol-error/http-4xx counter = %v, want exactly 1 (the reclassified exchange counts once)", got)
	}
	if total := totalProtocolErrors(t, f); total != 1 {
		t.Fatalf("total protocol-error diagnostics = %v, want exactly 1", total)
	}
	if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityOpportunityMissing); got != -1 {
		t.Fatalf("fabricated identity/%s count = %v, want none", reasonIdentityOpportunityMissing, got)
	}
	if err := f.adapter.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted}); !errors.Is(err, source.ErrUnsupportedCIPark) {
		t.Fatalf("ReportCIRepair() = %v, want the latched ErrUnsupportedCIPark without another exchange", err)
	}
	if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP4xx); got != 1 {
		t.Fatalf("protocol-error/http-4xx counter after the latch = %v, want still 1 (the short-circuit performs no exchange)", got)
	}

	// The sibling never-deliverable refusal: a 400 from a #495-era route
	// that validates a required fence token arrives wrapped in the choke
	// point's permanent-refusal sentinel, the posture the controller's
	// release delivery also drops on. The close never claims the
	// unsupported sentinel here — the two classifications stay disjoint.
	f400 := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusBadRequest, "fence token required"))
	if err := f400.adapter.ReleaseCIPark(context.Background(), ciparkWorkItem(), source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); !errors.Is(err, source.ErrPermanentRefusal) || errors.Is(err, source.ErrUnsupportedCIPark) {
		t.Fatalf("ReleaseCIPark(400) = %v, want ErrPermanentRefusal and not ErrUnsupportedCIPark", err)
	}
	if got := diagnosticsCounter(t, f400, connector.SignalProtocolError, reasonHTTP4xx); got != 1 {
		t.Fatalf("400 protocol-error/http-4xx counter = %v, want exactly 1 (counted at the choke point)", got)
	}
	if total := totalProtocolErrors(t, f400); total != 1 {
		t.Fatalf("400 total protocol-error diagnostics = %v, want exactly 1", total)
	}
	if err := f400.adapter.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted}); errors.Is(err, source.ErrUnsupportedCIPark) {
		t.Fatalf("ReportCIRepair(400) = %v, want an ordinary permanent refusal, not the unsupported sentinel", err)
	}
}

// assertCIParkCapability pins the capability catalogue series the CI park
// lifecycle currently advertises: exactly one state series for the
// capability under the fixture's connector identity.
func assertCIParkCapability(t *testing.T, f *diagnosticsFixture, wantState string) {
	t.Helper()
	if _, ok := findSeries(t, f.reg, connector.MetricCapabilityInfo, map[string]string{
		"connector": defaultConnectorName, "capability": connector.CapabilityCIParkLifecycle, "state": wantState,
	}); !ok {
		t.Fatalf("capability catalogue missing ci-park-lifecycle state %q", wantState)
	}
	for _, state := range connector.CapabilityStates {
		if state == wantState {
			continue
		}
		if _, ok := findSeries(t, f.reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": defaultConnectorName, "capability": connector.CapabilityCIParkLifecycle, "state": state,
		}); ok {
			t.Fatalf("capability catalogue still reports ci-park-lifecycle state %q alongside %q", state, wantState)
		}
	}
}

// TestDispatchCIParkRoute404LatchesUnsupported pins the safe-degradation
// posture for a Dispatch deployment that has not adopted the CI-park routes
// at all (#559): the family's refusal vocabulary is 409-coded, so a bare 404
// on any park-family POST is permanent version-skew evidence, never a
// per-item condition. The exchange reclassifies into the permanent
// source.ErrUnsupportedCIPark and the client latches the capability as
// absent for the process — the capability catalogue gauge flips from
// supported to unsupported, and the latch adds no diagnostic of its own
// because the 404's protocol-error class was already counted once at the
// do() choke point. Every park-family exchange afterwards short-circuits at
// entry with no HTTP round trip, ending the retry churn and never spending a
// repair attempt against a queue that can never fence anything. Temporary
// failures keep today's semantics: a 5xx, a transport failure, a timeout,
// and a 409-coded refusal are not route evidence and latch nothing, and the
// reservation's pull-request state pre-read keeps its own 404 semantics —
// a pull request that no longer exists is stale work, not route absence.
func TestDispatchCIParkRoute404LatchesUnsupported(t *testing.T) {
	calls := []struct {
		name string
		call func(*Adapter) error
	}{
		{"park", func(a *Adapter) error {
			return a.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
		{"reserve", func(a *Adapter) error {
			_, err := a.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
			return err
		}},
		{"settle", func(a *Adapter) error {
			return a.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted})
		}},
		{"release", func(a *Adapter) error {
			return a.ReleaseCIPark(context.Background(), ciparkWorkItem(), source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}},
	}
	for _, first := range calls {
		first := first
		t.Run(first.name, func(t *testing.T) {
			requests := 0
			f := newDiagnosticsFixture(t, openChecker(), func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				if !strings.HasPrefix(r.URL.Path, "/api/pr-fix-queue/ci") {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
				requests++
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("no such queue item"))
			})
			if f.adapter.CIParkLifecycleUnsupported() {
				t.Fatal("CIParkLifecycleUnsupported() = true before any exchange, want not-known-unsupported")
			}
			firstErr := first.call(f.adapter)
			if !errors.Is(firstErr, source.ErrUnsupportedCIPark) {
				t.Fatalf("%s on an unadopted route = %v, want ErrUnsupportedCIPark", first.name, firstErr)
			}
			var apiErr *APIError
			if !errors.As(firstErr, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
				t.Fatalf("reclassified %s error chain = %v, want the wrapped 404 APIError to remain inspectable", first.name, firstErr)
			}
			if requests != 1 {
				t.Fatalf("park-family requests = %d, want exactly 1", requests)
			}
			if !f.adapter.CIParkLifecycleUnsupported() {
				t.Fatal("CIParkLifecycleUnsupported() = false after the route's 404, want latched")
			}
			// The census: the 404's exchange class was counted once at the
			// do() choke point, and the latch adds no diagnostic of its own.
			if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP4xx); got != 1 {
				t.Fatalf("protocol-error/http-4xx counter = %v, want exactly 1", got)
			}
			if total := totalProtocolErrors(t, f); total != 1 {
				t.Fatalf("total protocol-error diagnostics = %v, want exactly 1", total)
			}
			assertCIParkCapability(t, f, connector.CapabilityUnsupported)

			// The latch ends the churn: every park-family exchange
			// short-circuits at entry, with no HTTP round trip and no
			// further diagnostic.
			for _, then := range calls {
				if err := then.call(f.adapter); !errors.Is(err, source.ErrUnsupportedCIPark) {
					t.Fatalf("latched %s = %v, want ErrUnsupportedCIPark", then.name, err)
				}
			}
			grant, err := f.adapter.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
			if !errors.Is(err, source.ErrUnsupportedCIPark) || grant != (source.CIRepairGrant{}) {
				t.Fatalf("latched reserve = (%#v, %v), want a zero grant with ErrUnsupportedCIPark", grant, err)
			}
			if requests != 1 {
				t.Fatalf("park-family requests after the latch = %d, want still 1 (the short-circuit performs no exchange)", requests)
			}
			if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP4xx); got != 1 {
				t.Fatalf("protocol-error/http-4xx counter after the latch = %v, want still 1", got)
			}
			if total := totalProtocolErrors(t, f); total != 1 {
				t.Fatalf("total protocol-error diagnostics after the latch = %v, want still 1", total)
			}
		})
	}

	t.Run("server error keeps the ordinary retry semantics", func(t *testing.T) {
		requests := 0
		f := newDiagnosticsFixture(t, openChecker(), func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
			requests++
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
		})
		park := func() error {
			return f.adapter.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
		}
		if err := park(); err == nil || errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("park against a 500 = %v, want the ordinary server error", err)
		}
		if err := park(); err == nil || errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("park again against a 500 = %v, want the ordinary server error without a short-circuit", err)
		}
		if requests != 2 {
			t.Fatalf("park-family requests = %d, want 2 (a 5xx latches nothing and short-circuits nothing)", requests)
		}
		if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP5xx); got != 2 {
			t.Fatalf("protocol-error/http-5xx counter = %v, want 2", got)
		}
		if f.adapter.CIParkLifecycleUnsupported() {
			t.Fatal("CIParkLifecycleUnsupported() = true after a 5xx, want not-known-unsupported")
		}
		assertCIParkCapability(t, f, connector.CapabilitySupported)
	})

	t.Run("transport failure latches nothing", func(t *testing.T) {
		f := newDiagnosticsFixture(t, openChecker(), func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "gone", http.StatusNotFound)
		})
		f.client.httpClient = failingDoer{}
		if err := f.adapter.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); err == nil || errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("park against a dead server = %v, want the transport failure", err)
		}
		if f.adapter.CIParkLifecycleUnsupported() {
			t.Fatal("CIParkLifecycleUnsupported() = true after a transport failure, want not-known-unsupported")
		}
		assertCIParkCapability(t, f, connector.CapabilitySupported)
	})

	t.Run("timeout latches nothing", func(t *testing.T) {
		f := &diagnosticsFixture{}
		f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(time.Second)
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(f.server.Close)
		f.reg = prometheus.NewRegistry()
		f.em = connector.NewEmitter(f.reg, nil)
		client, err := NewClient(f.server.URL, "worker", "secret-token", 50*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		client.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(context.Context, string, int) (PullRequestState, error) {
			return PullRequestState{State: "open"}, nil
		}))
		f.client = client
		f.adapter = New(client, WithConnectorDiagnostics(f.em))
		if err := f.adapter.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"}); err == nil || errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("park across a timeout = %v, want the transient timeout failure", err)
		}
		if f.adapter.CIParkLifecycleUnsupported() {
			t.Fatal("CIParkLifecycleUnsupported() = true after a timeout, want not-known-unsupported")
		}
		assertCIParkCapability(t, f, connector.CapabilitySupported)
	})

	t.Run("coded refusal latches nothing", func(t *testing.T) {
		refusalCodes := []struct {
			code      string
			wantError error
		}{
			{"fence-held", source.ErrSuperseded},
			{"opportunity-missing", source.ErrStaleWork},
			{"head-superseded", source.ErrStaleWork},
			{"fence-settled", source.ErrStaleWork},
		}
		for _, code := range refusalCodes {
			requests := 0
			f := newDiagnosticsFixture(t, openChecker(), func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				requests++
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"reason": code.code})
			})
			park := func() error {
				return f.adapter.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
			}
			if err := park(); !errors.Is(err, code.wantError) {
				t.Fatalf("park against a %q refusal = %v, want %v", code.code, err, code.wantError)
			}
			if err := park(); !errors.Is(err, code.wantError) {
				t.Fatalf("park again against a %q refusal = %v, want %v without a short-circuit", code.code, err, code.wantError)
			}
			if requests != 2 {
				t.Fatalf("park-family requests for %q = %d, want 2 (a coded refusal latches nothing)", code.code, requests)
			}
			if f.adapter.CIParkLifecycleUnsupported() {
				t.Fatalf("CIParkLifecycleUnsupported() = true after a %q coded refusal, want not-known-unsupported", code.code)
			}
			assertCIParkCapability(t, f, connector.CapabilitySupported)
		}
	})

	t.Run("reservation pre-read 404 is stale work, not route absence", func(t *testing.T) {
		requests := 0
		checks := 0
		f := newDiagnosticsFixture(t, func(c *HTTPClient) {
			c.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(_ context.Context, _ string, _ int) (PullRequestState, error) {
				checks++
				if checks == 1 {
					return PullRequestState{}, &APIError{StatusCode: http.StatusNotFound, Message: "no such pull request"}
				}
				return PullRequestState{State: "open"}, nil
			}))
		}, func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
			requests++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("no such queue item"))
		})
		item := ciparkWorkItem()
		_, err := f.adapter.ReserveCIRepair(context.Background(), item, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
		if err == nil || errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("reserve with a 404 pre-read = %v, want the pre-read's own ordinary 404", err)
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			t.Fatalf("reserve with a 404 pre-read = %v, want the pre-read APIError unchanged", err)
		}
		if requests != 0 {
			t.Fatalf("park-family requests after a 404 pre-read = %d, want 0 (the reservation never POSTed)", requests)
		}
		if f.adapter.CIParkLifecycleUnsupported() {
			t.Fatal("CIParkLifecycleUnsupported() = true after a pre-read 404, want not-known-unsupported")
		}
		assertCIParkCapability(t, f, connector.CapabilitySupported)
		// The pre-read's 404 latched nothing: the next reservation reaches
		// the route, and that route's own 404 is what latches.
		if _, err := f.adapter.ReserveCIRepair(context.Background(), item, source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k2", PR: "77", Head: ciparkHead1}); !errors.Is(err, source.ErrUnsupportedCIPark) {
			t.Fatalf("reserve after the pre-read 404 = %v, want the route's own ErrUnsupportedCIPark", err)
		}
		if requests != 1 {
			t.Fatalf("park-family requests = %d, want 1 (the second reservation POSTed)", requests)
		}
		assertCIParkCapability(t, f, connector.CapabilityUnsupported)
	})
}

type ciParkRequest struct {
	Repo           string `json:"repo"`
	Generation     int    `json:"generation"`
	AgentName      string `json:"agentName"`
	Head           string `json:"head"`
	IdempotencyKey string `json:"idempotencyKey"`
	FenceToken     string `json:"fenceToken"`
	Result         string `json:"result"`
}

// ciParkDispatch models Dispatch's CI-park deferral contract (issues
// #495/#534) with in-memory state: the three generation-fenced, idempotent
// lifecycle endpoints plus the next-task handout, which stays quiet while
// the deferral is active and hands out again only after settlement or the
// fence-less close. A committed idempotency key replays its original
// acceptance without touching state, so retries after a lost response never
// double-apply a transition.
type ciParkDispatch struct {
	mu sync.Mutex

	generation int
	budget     int

	parked   bool
	head     string
	reserved bool
	fence    string
	settled  bool

	parkKeys    map[string]bool
	reserveKeys map[string]string
	outcomeKeys map[string]bool

	bookings int
	handouts int
	closes   int
}

func newCIParkDispatch() *ciParkDispatch {
	return &ciParkDispatch{
		generation:  ciparkGen,
		budget:      3,
		parkKeys:    map[string]bool{},
		reserveKeys: map[string]string{},
		outcomeKeys: map[string]bool{},
	}
}

func (s *ciParkDispatch) serve(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body ciParkRequest
		if r.Body != nil {
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.URL.Path {
		case "/api/pr-fix-queue/cipark":
			s.cipark(w, body)
		case "/api/pr-fix-queue/cireserve":
			s.cireserve(w, body)
		case "/api/pr-fix-queue/cirepair":
			s.cirepair(w, body)
		case "/api/agents/worker/next-task":
			s.nextTask(w)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
		}
	}
}

func (s *ciParkDispatch) fenceRefused(generation int, w http.ResponseWriter) bool {
	if generation != s.generation {
		http.Error(w, fmt.Sprintf("generation mismatch: expected %d", s.generation), http.StatusConflict)
		return true
	}
	return false
}

func (s *ciParkDispatch) refuse(w http.ResponseWriter, reason string) {
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]string{"reason": reason})
}

func (s *ciParkDispatch) cipark(w http.ResponseWriter, body ciParkRequest) {
	if s.parkKeys[body.IdempotencyKey] {
		// A committed park replays without state churn beyond the refresh.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.fenceRefused(body.Generation, w) {
		return
	}
	s.parkKeys[body.IdempotencyKey] = true
	s.parked = true
	s.head = body.Head
	// A fresh notice re-establishes the deferral for the key it presents:
	// whatever closure state the previous deferral of this queue item
	// reached — settled or released — is its own, and the new deferral
	// starts open until it is settled or closed again.
	s.reserved = false
	s.fence = ""
	s.settled = false
	w.WriteHeader(http.StatusNoContent)
}

func (s *ciParkDispatch) cireserve(w http.ResponseWriter, body ciParkRequest) {
	if fence, ok := s.reserveKeys[body.IdempotencyKey]; ok {
		// A committed booking replays its grant without re-booking.
		_ = json.NewEncoder(w).Encode(map[string]string{"fenceToken": fence})
		return
	}
	if s.fenceRefused(body.Generation, w) {
		return
	}
	switch {
	case !s.parked:
		s.refuse(w, "opportunity-missing")
	case s.reserved && s.fence == body.FenceToken:
		// Re-presenting the pinned fence stays one booking.
		_ = json.NewEncoder(w).Encode(map[string]string{"fenceToken": s.fence})
	case s.reserved:
		s.refuse(w, "fence-held")
	case s.head != body.Head:
		s.refuse(w, "head-superseded")
	default:
		s.reserved = true
		s.fence = body.FenceToken
		s.reserveKeys[body.IdempotencyKey] = body.FenceToken
		s.bookings++
		s.budget--
		_ = json.NewEncoder(w).Encode(map[string]string{"fenceToken": body.FenceToken})
	}
}

func (s *ciParkDispatch) cirepair(w http.ResponseWriter, body ciParkRequest) {
	if s.outcomeKeys[body.IdempotencyKey] {
		// A committed outcome replays its acceptance, never the mutation.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.fenceRefused(body.Generation, w) {
		return
	}
	if body.FenceToken == "" {
		// The fence-less close (#534): a deferral whose opportunity ended
		// without a reservation is released by the park notice's own
		// idempotency key. The close never settles a granted reservation,
		// so a live fence holds the deferral out of reach, and a deferral
		// that is absent — never established, unknown key, released, or
		// already settled — is refused advisory with the "no deferral
		// exists" vocabulary.
		if s.reserved {
			s.refuse(w, "fence-held")
			return
		}
		if !s.parked || !s.parkKeys[body.IdempotencyKey] {
			s.refuse(w, "opportunity-missing")
			return
		}
		s.outcomeKeys[body.IdempotencyKey] = true
		s.settled = true
		s.parked = false // stewardship back to Dispatch without a booking
		s.closes++
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if s.settled {
		s.refuse(w, "fence-settled")
		return
	}
	if !s.reserved || s.fence != body.FenceToken {
		s.refuse(w, "fence-gone")
		return
	}
	s.outcomeKeys[body.IdempotencyKey] = true
	switch body.Result {
	case "repushed":
		s.head = body.Head
		s.reserved = false // stays parked: CI observation restarts on the new head
	case "exhausted", "unsupported":
		s.settled = true
		s.parked = false // stewardship back to Dispatch
		s.reserved = false
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *ciParkDispatch) nextTask(w http.ResponseWriter) {
	if s.parked {
		// The active deferral suppresses the modeled handout.
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "idle", "shouldRun": false})
		return
	}
	s.handouts++
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":        "followup-pr",
		"shouldRun":   true,
		"pullRequest": map[string]any{"repoFullName": "acme/widgets", "number": 77},
		"prFixItem":   map[string]any{"id": "queue-item-1", "generation": s.generation},
	})
}

// ciParkState is an unlocked snapshot of the modeled deferral state.
type ciParkState struct {
	parked, reserved, settled  bool
	head                       string
	bookings, budget, handouts int
	closes                     int
}

func (s *ciParkDispatch) snapshot() ciParkState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ciParkState{
		parked: s.parked, reserved: s.reserved, settled: s.settled, head: s.head,
		bookings: s.bookings, budget: s.budget, handouts: s.handouts, closes: s.closes,
	}
}

// TestDispatchCIParkCoordination is the acceptance fixture: one repair
// opportunity per deferral, no duplicate PR-fix handout while it is reserved,
// exact head identity, one budget booking per attempt, and idempotent replay
// of the whole cycle from a fresh client (controller restart with lost
// responses).
func TestDispatchCIParkCoordination(t *testing.T) {
	fake := newCIParkDispatch()
	server := httptest.NewServer(fake.serve(t))
	defer server.Close()
	item := ciparkWorkItem()
	ctx := context.Background()
	newAdapter := func() *Adapter {
		_, a := ciParkClient(t, server.URL)
		return a
	}
	adapter := newAdapter()

	// The park announces the deferral; the first reservation books the one
	// opportunity and echoes the pinned fence token back as the grant.
	if err := adapter.ReportCIParked(ctx, item, source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-1"}); err != nil {
		t.Fatalf("ReportCIParked: %v", err)
	}
	grant, err := adapter.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-one", IdempotencyKey: "reserve-1", PR: "77", Head: ciparkHead1})
	if err != nil {
		t.Fatalf("ReserveCIRepair: %v", err)
	}
	if grant.FenceToken != "fence-one" {
		t.Fatalf("grant fence = %q, want the echoed reservation token", grant.FenceToken)
	}
	// A live reservation prevents a fresh competing PR-fix handout.
	if items := ciParkDiscover(t, adapter); len(items) != 0 {
		t.Fatalf("discover while deferral active = %d items, want none", len(items))
	}
	// A second worker's reservation under a different token is fenced off
	// while the reservation is still open.
	if _, err := adapter.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-two", IdempotencyKey: "reserve-2", PR: "77", Head: ciparkHead1}); !errors.Is(err, source.ErrSuperseded) {
		t.Fatalf("second reservation error = %v, want ErrSuperseded", err)
	}
	// The repushed outcome updates the fenced opportunity on this head and
	// closes only this reservation; the deferral stays active, so the queue
	// stays quiet.
	if err := adapter.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence-one", IdempotencyKey: "outcome-1", PR: "77", Head: ciparkHead2, Result: source.CIRepairRepushed}); err != nil {
		t.Fatalf("ReportCIRepair(repushed): %v", err)
	}
	if items := ciParkDiscover(t, adapter); len(items) != 0 {
		t.Fatalf("discover after repush = %d items, want none (deferral still active)", len(items))
	}
	// Re-park on the new head and reserve the second opportunity.
	if err := adapter.ReportCIParked(ctx, item, source.CIParkNotice{PR: "77", Head: ciparkHead2, IdempotencyKey: "park-2"}); err != nil {
		t.Fatalf("ReportCIParked(head2): %v", err)
	}
	grant2, err := adapter.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-two", IdempotencyKey: "reserve-3", PR: "77", Head: ciparkHead2})
	if err != nil {
		t.Fatalf("ReserveCIRepair(second opportunity): %v", err)
	}
	if grant2.FenceToken != "fence-two" {
		t.Fatalf("second grant fence = %q, want the echoed reservation token", grant2.FenceToken)
	}
	// Exhaustion hands stewardship back to Dispatch: the next-task handout
	// resumes.
	if err := adapter.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence-two", IdempotencyKey: "outcome-2", PR: "77", Result: source.CIRepairExhausted}); err != nil {
		t.Fatalf("ReportCIRepair(exhausted): %v", err)
	}
	items := ciParkDiscover(t, adapter)
	if len(items) != 1 || items[0].ID != item.ID {
		t.Fatalf("discover after settlement = %#v, want the resumed handout", items)
	}
	if got := fake.snapshot(); got.bookings != 2 || got.budget != 1 || got.handouts != 1 || !got.settled || got.parked || got.head != ciparkHead2 {
		t.Fatalf("state after the cycle = %#v, want two bookings, one budget left, one handout, settled deferral on %s", got, ciparkHead2)
	}

	// Idempotent replay from a fresh client: same payloads, same keys, same
	// acceptances, no double-booking and no state churn.
	replay := newAdapter()
	for _, notice := range []source.CIParkNotice{
		{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-1"},
		{PR: "77", Head: ciparkHead2, IdempotencyKey: "park-2"},
	} {
		if err := replay.ReportCIParked(ctx, item, notice); err != nil {
			t.Fatalf("replay ReportCIParked(%s): %v", notice.Head, err)
		}
	}
	for _, reservation := range []source.CIRepairReservation{
		{FenceToken: "fence-one", IdempotencyKey: "reserve-1", PR: "77", Head: ciparkHead1},
		{FenceToken: "fence-two", IdempotencyKey: "reserve-3", PR: "77", Head: ciparkHead2},
	} {
		grant, err := replay.ReserveCIRepair(ctx, item, reservation)
		if err != nil {
			t.Fatalf("replay ReserveCIRepair(%s): %v", reservation.IdempotencyKey, err)
		}
		if grant.FenceToken != reservation.FenceToken {
			t.Fatalf("replay grant = %q, want the committed %q", grant.FenceToken, reservation.FenceToken)
		}
	}
	for _, outcome := range []source.CIRepairOutcome{
		{FenceToken: "fence-one", IdempotencyKey: "outcome-1", PR: "77", Head: ciparkHead2, Result: source.CIRepairRepushed},
		{FenceToken: "fence-two", IdempotencyKey: "outcome-2", PR: "77", Result: source.CIRepairExhausted},
	} {
		if err := replay.ReportCIRepair(ctx, item, outcome); err != nil {
			t.Fatalf("replay ReportCIRepair(%s): %v", outcome.IdempotencyKey, err)
		}
	}
	// A fresh reservation against the settled deferral is stale work, and a
	// lost-response outcome whose retry arrives after settlement is refused,
	// not applied: exact-once is preserved across the restart.
	if _, err := replay.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-fresh-worker", IdempotencyKey: "reserve-restart", PR: "77", Head: ciparkHead2}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("post-settlement reservation error = %v, want ErrStaleWork", err)
	}
	if err := replay.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence-two", IdempotencyKey: "outcome-restart", PR: "77", Result: source.CIRepairUnsupported}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("post-settlement outcome error = %v, want ErrStaleWork (fence-settled)", err)
	}
	if got := fake.snapshot(); got.bookings != 2 || got.budget != 1 || got.handouts != 1 || !got.settled || got.parked || got.head != ciparkHead2 {
		t.Fatalf("state after replay = %#v, want the cycle's bookings, budget, handout, and settlement untouched", got)
	}

	// The release cycle (#534): a deferral whose opportunity ends without a
	// reservation is closed fence-lessly, keyed by the park notice's own
	// idempotency key, and the handout resumes with no budget spent.
	if err := adapter.ReportCIParked(ctx, item, source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-3"}); err != nil {
		t.Fatalf("ReportCIParked(pre-release): %v", err)
	}
	if items := ciParkDiscover(t, adapter); len(items) != 0 {
		t.Fatalf("discover while re-parked = %d items, want none", len(items))
	}
	if err := adapter.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-3"}); err != nil {
		t.Fatalf("ReleaseCIPark: %v", err)
	}
	if items := ciParkDiscover(t, adapter); len(items) != 1 || items[0].ID != item.ID {
		t.Fatalf("discover after the close = %#v, want the resumed handout", items)
	}
	// A committed close replays its acceptance for a retry that lost the
	// response; a never-established key is refused advisory as stale work.
	if err := replay.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-3"}); err != nil {
		t.Fatalf("replay ReleaseCIPark: %v", err)
	}
	if err := replay.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-never"}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("close on a never-established key = %v, want ErrStaleWork (the refusal is advisory on this route)", err)
	}
	if got := fake.snapshot(); got.closes != 1 || !got.settled || got.parked || got.bookings != 2 || got.budget != 1 {
		t.Fatalf("state after the close cycle = %#v, want one close, no bookings or budget spent, and a released deferral", got)
	}

	// Fenced settlement keeps its own semantics: a granted reservation is
	// settled by the fenced outcome's fence alone, a close cannot release a
	// deferral out from under it, and after settlement the same close is
	// refused advisory (the deferral no longer exists).
	heldModel := newCIParkDispatch()
	heldServer := httptest.NewServer(heldModel.serve(t))
	defer heldServer.Close()
	_, heldAdapter := ciParkClient(t, heldServer.URL)
	if err := heldAdapter.ReportCIParked(ctx, item, source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-4"}); err != nil {
		t.Fatalf("ReportCIParked(held): %v", err)
	}
	if _, err := heldAdapter.ReserveCIRepair(ctx, item, source.CIRepairReservation{FenceToken: "fence-three", IdempotencyKey: "reserve-4", PR: "77", Head: ciparkHead1}); err != nil {
		t.Fatalf("ReserveCIRepair(held): %v", err)
	}
	if err := heldAdapter.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-4"}); !errors.Is(err, source.ErrSuperseded) {
		t.Fatalf("close under a live reservation = %v, want ErrSuperseded (fence-held)", err)
	}
	if err := heldAdapter.ReportCIRepair(ctx, item, source.CIRepairOutcome{FenceToken: "fence-three", IdempotencyKey: "outcome-4", PR: "77", Result: source.CIRepairExhausted}); err != nil {
		t.Fatalf("ReportCIRepair(held, exhausted): %v", err)
	}
	if items := ciParkDiscover(t, heldAdapter); len(items) != 1 || items[0].ID != item.ID {
		t.Fatalf("discover after the settled held-deferral = %#v, want the resumed handout", items)
	}
	if err := heldAdapter.ReleaseCIPark(ctx, item, source.CIParkRelease{PR: "77", Head: ciparkHead1, IdempotencyKey: "park-4"}); !errors.Is(err, source.ErrStaleWork) {
		t.Fatalf("close after settlement = %v, want ErrStaleWork (the deferral no longer exists)", err)
	}
	if got := heldModel.snapshot(); got.bookings != 1 || got.budget != 2 || got.handouts != 1 || got.closes != 0 || !got.settled || got.parked {
		t.Fatalf("held model state = %#v, want one booking, one handout, no committed close, settled deferral", got)
	}
}

// TestDispatchCIParkDiagnostics pins the exact-once diagnostic accounting of
// the lifecycle endpoints (§3): a generation fence counts identity exactly
// once and never as a protocol error, a coded refusal counts its identity
// census class — the refusal code itself — exactly once and never a
// protocol error, a server failure counts http-5xx once, and the
// capability catalogue declares the new seam.
func TestDispatchCIParkDiagnostics(t *testing.T) {
	t.Run("capability-catalogue", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, ciParkRefused(t, http.StatusInternalServerError, "boom"))
		if _, ok := findSeries(t, f.reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": defaultConnectorName, "capability": connector.CapabilityCIParkLifecycle, "state": connector.CapabilitySupported,
		}); !ok {
			t.Fatal("capability catalogue did not declare ci-park-lifecycle supported")
		}
	})
	t.Run("generation-fence-counts-identity-once", func(t *testing.T) {
		for _, call := range []struct {
			name string
			run  func(*Adapter) error
		}{
			{"park", func(a *Adapter) error {
				return a.ReportCIParked(context.Background(), ciparkWorkItem(), source.CIParkNotice{PR: "77", Head: ciparkHead1, IdempotencyKey: "k"})
			}},
			{"reserve", func(a *Adapter) error {
				_, err := a.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
				return err
			}},
			{"outcome", func(a *Adapter) error {
				return a.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairExhausted})
			}},
		} {
			f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusConflict, "generation mismatch: expected 4"))
			if err := call.run(f.adapter); !errors.Is(err, source.ErrSuperseded) {
				t.Fatalf("%s: err = %v, want ErrSuperseded", call.name, err)
			}
			if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityGenerationAdvanced); got != 1 {
				t.Fatalf("%s: identity/generation-advanced counter = %v, want exactly 1", call.name, got)
			}
			if total := totalProtocolErrors(t, f); total != 0 {
				t.Fatalf("%s: superseded exchange counted %v protocol-error diagnostics, want 0 (absorbed)", call.name, total)
			}
		}
	})
	t.Run("coded-refusal-counts-identity-once", func(t *testing.T) {
		f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusConflict, `{"reason":"fence-held"}`))
		if _, err := f.adapter.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1}); !errors.Is(err, source.ErrSuperseded) {
			t.Fatalf("err = %v, want ErrSuperseded", err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityFenceHeld); got != 1 {
			t.Fatalf("identity/%s counter = %v, want exactly 1", reasonIdentityFenceHeld, got)
		}
		if total := totalProtocolErrors(t, f); total != 0 {
			t.Fatalf("total protocol-error diagnostics = %v, want 0 (the census counts the identity class, not the exchange)", total)
		}
	})
	t.Run("server-error-counts-http-5xx-once", func(t *testing.T) {
		f := newDiagnosticsFixture(t, nil, ciParkRefused(t, http.StatusInternalServerError, "boom"))
		if err := f.adapter.ReportCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairOutcome{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Result: source.CIRepairUnsupported}); err == nil || errors.Is(err, source.ErrSuperseded) || errors.Is(err, source.ErrStaleWork) {
			t.Fatalf("err = %v, want a plain server failure", err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalProtocolError, reasonHTTP5xx); got != 1 {
			t.Fatalf("protocol-error/http-5xx counter = %v, want exactly 1", got)
		}
		if total := totalProtocolErrors(t, f); total != 1 {
			t.Fatalf("total protocol-error diagnostics = %v, want exactly 1", total)
		}
	})

	t.Run("coded-refusal-embedding-mismatch-literal-maps-once", func(t *testing.T) {
		// A coded body that embeds the generation mismatch literal in other
		// fields is classified by its reason code alone: exactly one
		// sentinel, exactly one counted identity census diagnostic.
		f := newDiagnosticsFixture(t, openChecker(), ciParkRefused(t, http.StatusConflict, `{"reason":"head-superseded","detail":"generation mismatch: expected 4"}`))
		_, err := f.adapter.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
		if !errors.Is(err, source.ErrStaleWork) {
			t.Fatalf("err = %v, want ErrStaleWork", err)
		}
		if errors.Is(err, source.ErrSuperseded) {
			t.Fatal("err also satisfies ErrSuperseded; the reason code must own the classification")
		}
		if got := diagnosticsCounter(t, f, connector.SignalIdentity, reasonIdentityHeadSuperseded); got != 1 {
			t.Fatalf("identity/%s counter = %v, want exactly 1", reasonIdentityHeadSuperseded, got)
		}
		if total := totalProtocolErrors(t, f); total != 0 {
			t.Fatalf("total protocol-error diagnostics = %v, want 0 (the code's census absorbs the exchange)", total)
		}
	})
}

// ciParkPRStateChecker pins one fixed pull request state for a reserve test's
// revalidation read; the checker read is a local function, not a counted
// exchange.
func ciParkPRStateChecker(t *testing.T, state PullRequestState) func(*HTTPClient) {
	t.Helper()
	return func(c *HTTPClient) {
		c.WithPullRequestStateChecker(PullRequestStateCheckerFunc(func(_ context.Context, repo string, number int) (PullRequestState, error) {
			if repo != "acme/widgets" || number != 77 {
				t.Errorf("pull request check = %s#%d, want acme/widgets#77", repo, number)
			}
			return state, nil
		}))
	}
}

// TestDispatchCIReserveClosedPullRequestCensus pins the reserve-side
// revalidation census (§3.4): a reservation against a closed or merged pull
// request is refused before any POST and counts revalidation under the
// actual staleness cause exactly like PreLaunch does; no exchange is
// involved, so no protocol-error or identity census is fabricated.
func TestDispatchCIReserveClosedPullRequestCensus(t *testing.T) {
	cases := []struct {
		name       string
		state      PullRequestState
		wantReason string
	}{
		{"closed-pr", PullRequestState{State: "closed"}, reasonRevalidationClosedUnmerged},
		{"merged-pr", PullRequestState{State: "", Merged: true}, reasonRevalidationMerged},
		// Merged wins over a closed spelling, the same priority PreLaunch
		// uses for the identical condition.
		{"closed-and-merged-pr", PullRequestState{State: "closed", Merged: true}, reasonRevalidationMerged},
	}
	for _, tc := range cases {
		f := newDiagnosticsFixture(t, ciParkPRStateChecker(t, tc.state), func(_ *diagnosticsFixture, _ http.ResponseWriter, r *http.Request) {
			t.Errorf("unexpected request %s %s: the reserve refusal precedes the POST", r.Method, r.URL.Path)
		})
		_, err := f.adapter.ReserveCIRepair(context.Background(), ciparkWorkItem(), source.CIRepairReservation{FenceToken: "fence", IdempotencyKey: "k", PR: "77", Head: ciparkHead1})
		if !errors.Is(err, source.ErrStaleWork) {
			t.Fatalf("%s: err = %v, want ErrStaleWork", tc.name, err)
		}
		if got := diagnosticsCounter(t, f, connector.SignalRevalidation, tc.wantReason); got != 1 {
			t.Fatalf("%s: revalidation/%s counter = %v, want exactly 1", tc.name, tc.wantReason, got)
		}
		for _, reason := range []string{
			reasonIdentityFenceHeld, reasonIdentityFenceGone, reasonIdentityOpportunityMissing,
			reasonIdentityHeadSuperseded, reasonIdentityFenceSettled,
		} {
			if got := diagnosticsCounter(t, f, connector.SignalIdentity, reason); got != -1 {
				t.Errorf("%s: fabricated identity/%s count = %v, want none", tc.name, reason, got)
			}
		}
		if total := totalProtocolErrors(t, f); total != 0 {
			t.Errorf("%s: counted %v protocol-error diagnostics, want 0 (a refused checker read is no exchange)", tc.name, total)
		}
	}
}
