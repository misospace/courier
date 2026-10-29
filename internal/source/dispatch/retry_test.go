package dispatch

// The two attempt-requeue routes exercised here (POST
// /api/pr-fix-queue/attempt-requeue and POST /api/issues/requeue), their wire
// shapes, and their coded 409 refusal reasons are Courier's proposed
// cross-repo contract for the Dispatch server's explicit-retry requeue (#97,
// misospace/courier#608). Dispatch main's only requeue route, POST
// /api/pr-fix-queue/requeue, is an unfenced non-idempotent maintainer reset;
// until the server adopts this contract every deployment answers the pinned
// routes with a bare 404, which the client latches as permanent version skew
// (source.ErrUnsupportedRetry) rather than approximating the requeue with the
// ordinary lifecycle calls. These fixtures are the pin: update this file in
// lockstep with the server implementation.

import (
	"context"
	"encoding/base64"
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
	"unicode/utf8"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

const (
	// retryTestQueueReceipt is the hand-out receipt issued for
	// ciparkWorkItem's queue-backed attempt; the requeue fence is its
	// handoutToken.
	retryTestQueueReceipt = `{"prFixId":"queue-item-1","generation":3,"handoutToken":"handout-1"}`
	// retryTestClaimReceipt is the claim receipt issued for
	// ciparkImplementItem's issue; the requeue fence is its claimToken.
	retryTestClaimReceipt = `{"claimToken":"claim-1"}`
)

// retryTestClient builds the lane-registered Dispatch client the wire tests
// drive, mirroring the CI-park fixture construction.
func retryTestClient(t *testing.T, serverURL string) *HTTPClient {
	t.Helper()
	client, err := NewClientWithLane(serverURL, "worker", "normal", "secret-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// retryTestGate records every request the guarded server sees and fails the
// test on any request outside the pinned set, so a requeue path can never be
// answered from another Dispatch route.
type retryTestGate struct {
	mu      sync.Mutex
	allowed map[string]bool
	hits    map[string]int
}

func newRetryTestGate(t *testing.T, allowed ...string) *retryTestGate {
	t.Helper()
	g := &retryTestGate{allowed: map[string]bool{}, hits: map[string]int{}}
	for _, key := range allowed {
		g.allowed[key] = true
	}
	return g
}

// observe records one request and reports whether the caller may answer it.
// A request outside the pinned set fails the test and is refused, so no
// unpinned route can ever produce a response the client would accept.
func (g *retryTestGate) observe(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	key := r.Method + " " + r.URL.Path
	g.mu.Lock()
	ok := g.allowed[key]
	if ok {
		g.hits[key]++
	}
	g.mu.Unlock()
	if !ok {
		t.Errorf("attempt requeue issued unpinned request %s", key)
		http.Error(w, "unpinned route", http.StatusForbidden)
		return false
	}
	return true
}

// hitCount reports how many times one pinned request was issued.
func (g *retryTestGate) hitCount(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.hits[key]
}

// seen reports every pinned request issued, with its hit count.
func (g *retryTestGate) seen() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := map[string]int{}
	for key, n := range g.hits {
		out[key] = n
	}
	return out
}

// reset clears the hit census so the next phase of the test starts fresh.
func (g *retryTestGate) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hits = map[string]int{}
}

// TestDispatchAttemptRequeueWireBodies pins the request bodies the two
// requeue routes carry: the queue route pivots on repo+pr+generation like the
// mark/ack family and fences with the issued hand-out token, and the issue
// route rides the claim receipt like the capacity handoff. The fenced token
// never travels as its own field, and the response's attempt identity is
// derived in the exact spelling the runner normalizes offers to (or forwarded
// verbatim on the issue route, which has no generation to hand back).
func TestDispatchAttemptRequeueWireBodies(t *testing.T) {
	t.Run("queue-backed", func(t *testing.T) {
		var mu sync.Mutex
		bodies := map[string][]map[string]any{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var captured map[string]any
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&captured)
			mu.Lock()
			bodies[r.URL.Path] = append(bodies[r.URL.Path], captured)
			mu.Unlock()
			switch r.URL.Path {
			case "/api/pr-fix-queue/attempt-requeue":
				_ = json.NewEncoder(w).Encode(map[string]any{
					"success": true, "replayed": false,
					"prFixItem": map[string]any{"id": "queue-item-1", "generation": 4},
				})
			default:
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		result, err := client.RequeueAttempt(context.Background(), ciparkWorkItem().ID, source.AttemptRequeueRequest{
			FenceToken:     retryTestQueueReceipt,
			IdempotencyKey: "coderun/ns/rr/key",
			Reason:         "req-1",
		})
		if err != nil {
			t.Fatalf("RequeueAttempt() error = %v", err)
		}
		// The reissued attempt identity is derived from the response's item
		// and generation in the exact spelling the runner normalizes offers
		// to, so the successor correlates the predecessor.
		wantID := "followup-pr/acme/widgets#77/queue-item-1@4"
		if result.AttemptID != wantID {
			t.Fatalf("AttemptID = %q, want %q", result.AttemptID, wantID)
		}
		successor := EncodeWorkID(Task{
			Type:        "followup-pr",
			PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77},
			PRFixItem:   &PRFixItem{ID: "queue-item-1", Generation: 4},
		})
		if got := WorkIdentity(successor); got != result.AttemptID {
			t.Fatalf("WorkIdentity(successor) = %q, want %q", got, result.AttemptID)
		}
		if got := bodies["/api/pr-fix-queue/attempt-requeue"]; len(got) != 1 || !reflect.DeepEqual(got[0], map[string]any{
			"repo":           "acme/widgets",
			"pr":             float64(77),
			"generation":     float64(3),
			"agentName":      "worker",
			"fenceToken":     "handout-1",
			"idempotencyKey": "coderun/ns/rr/key",
			"note":           "req-1",
		}) {
			t.Fatalf("attempt-requeue body = %#v, want the repo+pr+generation pivot fenced with the hand-out token", got)
		}
		if len(bodies) != 1 {
			t.Fatalf("requests hit %#v, want only the attempt-requeue route", bodies)
		}
	})
	t.Run("implement", func(t *testing.T) {
		var mu sync.Mutex
		bodies := map[string][]map[string]any{}
		var stateLookups []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var captured map[string]any
			_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&captured)
			mu.Lock()
			if r.URL.Path == "/api/issues/state" {
				stateLookups = append(stateLookups, r.URL.Query().Get("repo")+"#"+r.URL.Query().Get("number"))
			} else {
				bodies[r.URL.Path] = append(bodies[r.URL.Path], captured)
			}
			mu.Unlock()
			switch r.URL.Path {
			case "/api/issues/state":
				_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-1"})
			case "/api/issues/requeue":
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "replayed": true, "attemptId": "attempt-9"})
			default:
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		result, err := client.RequeueAttempt(context.Background(), ciparkImplementItem().ID, source.AttemptRequeueRequest{
			FenceToken:     retryTestClaimReceipt,
			IdempotencyKey: "coderun/ns/rr/key-implement",
			Reason:         "req-2",
		})
		if err != nil {
			t.Fatalf("RequeueAttempt() error = %v", err)
		}
		// An issue has no generation to hand back: any attempt identity the
		// server offers is forwarded verbatim.
		if result.AttemptID != "attempt-9" {
			t.Fatalf("AttemptID = %q, want attempt-9 forwarded verbatim", result.AttemptID)
		}
		mu.Lock()
		lookups := append([]string(nil), stateLookups...)
		mu.Unlock()
		if len(lookups) != 1 || !reflect.DeepEqual(lookups, []string{"acme/widgets#42"}) {
			t.Fatalf("state lookups = %#v, want exactly the implement issue's state read", lookups)
		}
		if got := bodies["/api/issues/requeue"]; len(got) != 1 || !reflect.DeepEqual(got[0], map[string]any{
			"issueId":        "issue-1",
			"repoFullName":   "acme/widgets",
			"issueNumber":    float64(42),
			"agentName":      "worker",
			"claimToken":     "claim-1",
			"idempotencyKey": "coderun/ns/rr/key-implement",
			"note":           "req-2",
		}) {
			t.Fatalf("issues/requeue body = %#v, want the issue identity fenced with the claim token", got)
		}
	})
	t.Run("success-without-identity", func(t *testing.T) {
		// A response that names no reissued item is an honest success: the
		// source requeued without exposing a new identity, and discovery
		// observes the successor later.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/pr-fix-queue/attempt-requeue" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "replayed": false})
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		result, err := client.RequeueAttempt(context.Background(), ciparkWorkItem().ID, source.AttemptRequeueRequest{
			FenceToken:     retryTestQueueReceipt,
			IdempotencyKey: "coderun/ns/rr/key",
		})
		if err != nil {
			t.Fatalf("RequeueAttempt() error = %v", err)
		}
		if result.AttemptID != "" {
			t.Fatalf("AttemptID = %q, want empty when the response names no item", result.AttemptID)
		}
	})
	t.Run("unconfirmed-success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/pr-fix-queue/attempt-requeue" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false})
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		result, err := client.RequeueAttempt(context.Background(), ciparkWorkItem().ID, source.AttemptRequeueRequest{
			FenceToken:     retryTestQueueReceipt,
			IdempotencyKey: "coderun/ns/rr/key",
		})
		if err == nil {
			t.Fatal("RequeueAttempt() error = nil, want an error for an unconfirmed success")
		}
		if result.AttemptID != "" {
			t.Fatalf("AttemptID = %q, want none for an unconfirmed success", result.AttemptID)
		}
	})
}

// TestDispatchAttemptRequeueRequiresKeyAndFence pins the pre-exchange
// validation: the route has no unfenced operator, so a missing or over-long
// idempotency key is a configuration fault reported as a plain error, and a
// fence that is not the run's issued Dispatch receipt — or a receipt that
// fences another attempt, or that carries no usable token — is
// unsupported-retry evidence, never a guess. No request is issued in any
// case, and a corrupt or unreadable work ID stays a plain descriptor error.
func TestDispatchAttemptRequeueRequiresKeyAndFence(t *testing.T) {
	queue := ciparkWorkItem()
	implement := ciparkImplementItem()
	queueless := source.WorkItem{ID: EncodeWorkID(Task{
		Type:        "followup-pr",
		PullRequest: &PullRequest{Repo: "acme/widgets", Number: 77},
	})}
	mystery := source.WorkItem{ID: EncodeWorkID(Task{
		Type:  "mystery",
		Issue: &Issue{ID: "issue-1", Repo: "acme/widgets", Number: 42},
	})}
	tests := []struct {
		name            string
		item            source.WorkItem
		key             string
		fence           string
		wantErr         error
		wantUnsupported bool
	}{
		{name: "missing-idempotency-key", item: queue, key: "", fence: retryTestQueueReceipt},
		{name: "blank-idempotency-key", item: queue, key: "   ", fence: retryTestQueueReceipt},
		{name: "over-long-idempotency-key", item: queue, key: strings.Repeat("k", 513), fence: retryTestQueueReceipt},
		{name: "missing-fence", item: queue, key: "coderun/ns/rr/key", fence: "", wantUnsupported: true},
		{name: "non-receipt-fence", item: queue, key: "coderun/ns/rr/key", fence: "handout-1", wantUnsupported: true},
		{name: "receipt-fencing-another-attempt", item: queue, key: "coderun/ns/rr/key", fence: `{"prFixId":"other-item","generation":9,"handoutToken":"handout-1"}`, wantUnsupported: true},
		{name: "queue-receipt-without-handout", item: queue, key: "coderun/ns/rr/key", fence: `{"prFixId":"queue-item-1","generation":3}`, wantUnsupported: true},
		{name: "implement-receipt-without-claim", item: implement, key: "coderun/ns/rr/key", fence: retryTestQueueReceipt, wantUnsupported: true},
		{name: "implement-work-without-issue-identity", item: source.WorkItem{ID: base64.RawURLEncoding.EncodeToString([]byte(`{"type":"implement","repo":"acme/widgets","number":42}`))}, key: "coderun/ns/rr/key", fence: `{"claimToken":"claim-1"}`, wantUnsupported: true},
		{name: "queueless-followup", item: queueless, key: "coderun/ns/rr/key", fence: retryTestQueueReceipt, wantUnsupported: true},
		{name: "unsupported-work-type", item: mystery, key: "coderun/ns/rr/key", fence: retryTestQueueReceipt, wantUnsupported: true},
		{name: "corrupt-work-id", item: source.WorkItem{ID: "not-a-work-id"}, key: "coderun/ns/rr/key", fence: retryTestQueueReceipt, wantErr: ErrUnknownWorkID},
		{name: "non-json-work-id", item: source.WorkItem{ID: base64.RawURLEncoding.EncodeToString([]byte("garbage"))}, key: "coderun/ns/rr/key", fence: retryTestQueueReceipt, wantErr: ErrUnknownWorkID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				t.Errorf("requeue issued %s %s before validation passed", r.Method, r.URL.Path)
				http.Error(w, "not found", http.StatusNotFound)
			}))
			defer server.Close()
			client := retryTestClient(t, server.URL)
			_, err := client.RequeueAttempt(context.Background(), tt.item.ID, source.AttemptRequeueRequest{
				FenceToken: tt.fence, IdempotencyKey: tt.key,
			})
			if requests != 0 {
				t.Fatalf("issued %d request(s) before the exchange, want none", requests)
			}
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}
				if errors.Is(err, source.ErrUnsupportedRetry) {
					t.Fatalf("error = %v; an unreadable ID says nothing about what the deployment can requeue", err)
				}
			} else if tt.wantUnsupported {
				if !errors.Is(err, source.ErrUnsupportedRetry) {
					t.Fatalf("error = %v, want source.ErrUnsupportedRetry", err)
				}
			} else if err == nil {
				t.Fatal("error = nil, want a plain pre-exchange validation error")
			}
		})
	}
}

// TestDispatchAttemptRequeueRefusalMapping pins the requeue family's coded
// 409 refusal vocabulary: each code maps to the source-neutral refusal the
// core settles on, counts the refusal exactly once in the connector census
// under the identity signal, and absorbs the deferred exchange so the
// refusal is never also counted as a protocol error. A 409 with an
// unrecognized code, or no reason at all, proves nothing and stays transient,
// a 5xx stays transient, and a 400 keeps the choke point's
// permanent-refusal classification.
func TestDispatchAttemptRequeueRefusalMapping(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		status        int
		wantReason    string
		wantMoot      bool
		wantSentinel  error
		denySentinels []error
		wantIdentity  string
		wantProtocol  float64
	}{
		{
			name:         "stale-generation",
			body:         `{"reason":"stale-generation"}`,
			status:       http.StatusConflict,
			wantReason:   "stale-generation",
			wantSentinel: source.ErrStaleCapacitySettlement,
			denySentinels: []error{
				source.ErrSuperseded, source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantIdentity: "requeue-stale-generation",
		},
		{
			name:         "superseded",
			body:         `{"reason":"superseded"}`,
			status:       http.StatusConflict,
			wantReason:   "already-requeued",
			wantMoot:     true,
			wantSentinel: source.ErrSuperseded,
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantIdentity: "requeue-superseded",
		},
		{
			name:       "item-closed",
			body:       `{"reason":"item-closed"}`,
			status:     http.StatusConflict,
			wantReason: "source-refused",
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded,
				source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantIdentity: "requeue-item-closed",
		},
		{
			name:       "human-hold",
			body:       `{"reason":"human-hold"}`,
			status:     http.StatusConflict,
			wantReason: "held-for-human",
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded,
				source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantIdentity: "requeue-human-hold",
		},
		{
			name:   "unrecognized-code",
			body:   `{"reason":"something-new"}`,
			status: http.StatusConflict,
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded,
				source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantProtocol: 1,
		},
		{
			name:   "plain-text-409",
			body:   "fence conflict",
			status: http.StatusConflict,
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded,
				source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantProtocol: 1,
		},
		{
			name:   "5xx-stays-transient",
			body:   "boom",
			status: http.StatusServiceUnavailable,
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded,
				source.ErrPermanentRefusal, source.ErrUnsupportedRetry,
			},
			wantProtocol: 1,
		},
		{
			name:         "400-keeps-choke-point-classification",
			body:         "fence token required",
			status:       http.StatusBadRequest,
			wantSentinel: source.ErrPermanentRefusal,
			denySentinels: []error{
				source.ErrStaleCapacitySettlement, source.ErrSuperseded, source.ErrUnsupportedRetry,
			},
			wantProtocol: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDiagnosticsFixture(t, nil, func(_ *diagnosticsFixture, w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/pr-fix-queue/attempt-requeue" {
					t.Errorf("requeue refusal issued %s %s", r.Method, r.URL.Path)
					http.Error(w, "unpinned route", http.StatusForbidden)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			})
			_, err := f.adapter.RequeueAttempt(context.Background(), ciparkWorkItem(), source.AttemptRequeueRequest{
				FenceToken:     retryTestQueueReceipt,
				IdempotencyKey: "coderun/ns/rr/key",
			})
			var refusal *source.RetryRefusalError
			if tt.wantReason != "" {
				if !errors.As(err, &refusal) {
					t.Fatalf("error = %v, want *source.RetryRefusalError", err)
				}
				if refusal.Reason != tt.wantReason {
					t.Fatalf("refusal.Reason = %q, want %q", refusal.Reason, tt.wantReason)
				}
				if refusal.Moot != tt.wantMoot {
					t.Fatalf("refusal.Moot = %v, want %v", refusal.Moot, tt.wantMoot)
				}
			} else if errors.As(err, &refusal) {
				t.Fatalf("error = %v, want a non-refusal exchange error", err)
			}
			if tt.wantSentinel != nil && !errors.Is(err, tt.wantSentinel) {
				t.Fatalf("error = %v, want %v", err, tt.wantSentinel)
			}
			for _, denied := range tt.denySentinels {
				if errors.Is(err, denied) {
					t.Fatalf("error = %v, must not satisfy %v", err, denied)
				}
			}
			wantIdentity := -1.0
			if tt.wantIdentity != "" {
				wantIdentity = 1
			}
			if got := diagnosticsCounter(t, f, connector.SignalIdentity, tt.wantIdentity); got != wantIdentity {
				t.Fatalf("identity/%s census = %v, want %v", tt.wantIdentity, got, wantIdentity)
			}
			if got := totalProtocolErrors(t, f); got != tt.wantProtocol {
				t.Fatalf("protocol-error total = %v, want %v", got, tt.wantProtocol)
			}
		})
	}
}

// TestDispatchAttemptRequeueLatchesOnBare404 pins the version-skew latch: a
// bare 404 on a requeue route is permanent evidence that the deployment never
// adopted the contract, so the client latches it for the process and every
// later requeue fails closed without a single request — the implement route's
// issue-state read is never re-issued either. A 5xx and a coded 409 are not
// route evidence and leave the latch clear, and the latch never touches the
// unrelated CI-park observation.
func TestDispatchAttemptRequeueLatchesOnBare404(t *testing.T) {
	ctx := context.Background()
	t.Run("queue-route-404-latches", func(t *testing.T) {
		gate := newRetryTestGate(t, "POST /api/pr-fix-queue/attempt-requeue")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if gate.observe(t, w, r) {
				http.Error(w, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		item := ciparkWorkItem()
		request := source.AttemptRequeueRequest{FenceToken: retryTestQueueReceipt, IdempotencyKey: "coderun/ns/rr/key"}
		if _, err := client.RequeueAttempt(ctx, item.ID, request); !errors.Is(err, source.ErrUnsupportedRetry) {
			t.Fatalf("first RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
		}
		if !client.retryRequeueUnsupported() {
			t.Fatal("the bare 404 did not latch retryUnsupported")
		}
		if client.ciParkUnsupported.Load() {
			t.Fatal("the requeue latch must not touch the CI-park observation")
		}
		if got := gate.hitCount("POST /api/pr-fix-queue/attempt-requeue"); got != 1 {
			t.Fatalf("the first attempt hit the route %d times, want exactly once", got)
		}
		gate.reset()
		for i := 0; i < 2; i++ {
			if _, err := client.RequeueAttempt(ctx, item.ID, request); !errors.Is(err, source.ErrUnsupportedRetry) {
				t.Fatalf("latched RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
			}
		}
		if got := gate.seen(); len(got) != 0 {
			t.Fatalf("the latched client still issued %#v, want none", got)
		}
	})
	t.Run("5xx-does-not-latch", func(t *testing.T) {
		gate := newRetryTestGate(t, "POST /api/pr-fix-queue/attempt-requeue")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if gate.observe(t, w, r) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, "boom")
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		item := ciparkWorkItem()
		request := source.AttemptRequeueRequest{FenceToken: retryTestQueueReceipt, IdempotencyKey: "coderun/ns/rr/key"}
		for i := 0; i < 2; i++ {
			_, err := client.RequeueAttempt(ctx, item.ID, request)
			if errors.Is(err, source.ErrUnsupportedRetry) {
				t.Fatalf("5xx RequeueAttempt() error = %v, must not latch as unsupported", err)
			}
			if err == nil {
				t.Fatal("RequeueAttempt() error = nil, want the transient failure")
			}
		}
		if client.retryRequeueUnsupported() {
			t.Fatal("a 5xx is not route evidence and must not latch")
		}
		if got := gate.hitCount("POST /api/pr-fix-queue/attempt-requeue"); got != 2 {
			t.Fatalf("the route was hit %d times, want both attempts to reach the server", got)
		}
	})
	t.Run("coded-409-does-not-latch", func(t *testing.T) {
		gate := newRetryTestGate(t, "POST /api/pr-fix-queue/attempt-requeue")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if gate.observe(t, w, r) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				fmt.Fprint(w, `{"reason":"stale-generation"}`)
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		item := ciparkWorkItem()
		request := source.AttemptRequeueRequest{FenceToken: retryTestQueueReceipt, IdempotencyKey: "coderun/ns/rr/key"}
		for i := 0; i < 2; i++ {
			_, err := client.RequeueAttempt(ctx, item.ID, request)
			if !errors.Is(err, source.ErrStaleCapacitySettlement) {
				t.Fatalf("coded RequeueAttempt() error = %v, want the stale-generation refusal", err)
			}
		}
		if client.retryRequeueUnsupported() {
			t.Fatal("a coded 409 refusal is not route evidence and must not latch")
		}
		if got := gate.hitCount("POST /api/pr-fix-queue/attempt-requeue"); got != 2 {
			t.Fatalf("the route was hit %d times, want both attempts to reach the server", got)
		}
	})
	t.Run("implement-route-404-latches", func(t *testing.T) {
		gate := newRetryTestGate(t, "GET /api/issues/state", "POST /api/issues/requeue")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !gate.observe(t, w, r) {
				return
			}
			switch r.URL.Path {
			case "/api/issues/state":
				_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-1"})
			case "/api/issues/requeue":
				http.Error(w, "not found", http.StatusNotFound)
			}
		}))
		defer server.Close()
		client := retryTestClient(t, server.URL)
		item := ciparkImplementItem()
		request := source.AttemptRequeueRequest{FenceToken: retryTestClaimReceipt, IdempotencyKey: "coderun/ns/rr/key"}
		if _, err := client.RequeueAttempt(ctx, item.ID, request); !errors.Is(err, source.ErrUnsupportedRetry) {
			t.Fatalf("first RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
		}
		if !client.retryRequeueUnsupported() {
			t.Fatal("the bare 404 did not latch retryUnsupported")
		}
		if got := gate.hitCount("GET /api/issues/state"); got != 1 {
			t.Fatalf("the first attempt issued %d state reads, want one before the 404", got)
		}
		gate.reset()
		if _, err := client.RequeueAttempt(ctx, item.ID, request); !errors.Is(err, source.ErrUnsupportedRetry) {
			t.Fatalf("latched RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
		}
		if got := gate.seen(); len(got) != 0 {
			t.Fatalf("the latched implement requeue still issued %#v, want none — not even the state read", got)
		}
	})
}

// TestDispatchAttemptRequeueNeverIssuesOrdinaryLifecycleCalls pins the
// no-emulation rule: through success, every coded refusal, and the 404 path,
// a fenced requeue reaches only its pinned route (plus the issue-state read
// implement work resolves through), never approximating the requeue with an
// ordinary claim, unclaim, status, mark, ack, report, maintainer requeue, or
// CI-park call.
func TestDispatchAttemptRequeueNeverIssuesOrdinaryLifecycleCalls(t *testing.T) {
	// The ordinary Dispatch calls a fenced requeue must never be
	// approximated with: each one would mutate ownership or state that the
	// fenced transition alone is allowed to change.
	forbidden := []string{
		"POST /api/issues/claim",
		"POST /api/issues/unclaim",
		"POST /api/issues/status",
		"POST /api/pr-fix-queue/mark",
		"POST /api/pr-fix-queue/ack",
		"POST /api/pr-fix-queue/requeue",
		"POST /api/agents/worker/tasks/report",
		"POST /api/pr-fix-queue/cipark",
		"POST /api/pr-fix-queue/cireserve",
		"POST /api/pr-fix-queue/cirepair",
	}
	scenarios := []struct {
		name   string
		kind   string
		status int
		body   string
	}{
		{name: "queue-success", kind: "queue", status: http.StatusOK, body: `{"success":true,"replayed":false,"prFixItem":{"id":"queue-item-1","generation":4}}`},
		{name: "queue-stale-generation", kind: "queue", status: http.StatusConflict, body: `{"reason":"stale-generation"}`},
		{name: "queue-superseded", kind: "queue", status: http.StatusConflict, body: `{"reason":"superseded"}`},
		{name: "queue-item-closed", kind: "queue", status: http.StatusConflict, body: `{"reason":"item-closed"}`},
		{name: "queue-human-hold", kind: "queue", status: http.StatusConflict, body: `{"reason":"human-hold"}`},
		{name: "queue-404", kind: "queue", status: http.StatusNotFound, body: "not found"},
		{name: "implement-success", kind: "implement", status: http.StatusOK, body: `{"success":true,"replayed":false}`},
		{name: "implement-stale-generation", kind: "implement", status: http.StatusConflict, body: `{"reason":"stale-generation"}`},
		{name: "implement-superseded", kind: "implement", status: http.StatusConflict, body: `{"reason":"superseded"}`},
		{name: "implement-item-closed", kind: "implement", status: http.StatusConflict, body: `{"reason":"item-closed"}`},
		{name: "implement-human-hold", kind: "implement", status: http.StatusConflict, body: `{"reason":"human-hold"}`},
		{name: "implement-404", kind: "implement", status: http.StatusNotFound, body: "not found"},
	}
	for _, tt := range scenarios {
		t.Run(tt.name, func(t *testing.T) {
			var item source.WorkItem
			var fence string
			var allowed []string
			var wantSeen map[string]int
			switch tt.kind {
			case "queue":
				item = ciparkWorkItem()
				fence = retryTestQueueReceipt
				allowed = []string{"POST /api/pr-fix-queue/attempt-requeue"}
				wantSeen = map[string]int{"POST /api/pr-fix-queue/attempt-requeue": 1}
			case "implement":
				item = ciparkImplementItem()
				fence = retryTestClaimReceipt
				allowed = []string{"GET /api/issues/state", "POST /api/issues/requeue"}
				wantSeen = map[string]int{"GET /api/issues/state": 1, "POST /api/issues/requeue": 1}
			}
			gate := newRetryTestGate(t, allowed...)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !gate.observe(t, w, r) {
					return
				}
				switch r.URL.Path {
				case "/api/issues/state":
					_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-1"})
				default:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tt.status)
					fmt.Fprint(w, tt.body)
				}
			}))
			defer server.Close()
			client := retryTestClient(t, server.URL)
			_, err := client.RequeueAttempt(context.Background(), item.ID, source.AttemptRequeueRequest{
				FenceToken:     fence,
				IdempotencyKey: "coderun/ns/rr/key",
			})
			switch tt.status {
			case http.StatusNotFound:
				if !errors.Is(err, source.ErrUnsupportedRetry) {
					t.Fatalf("RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
				}
			case http.StatusOK:
				if err != nil {
					t.Fatalf("RequeueAttempt() error = %v, want a clean success", err)
				}
			default:
				if err == nil {
					t.Fatal("RequeueAttempt() error = nil, want a classified refusal")
				}
			}
			if got := gate.seen(); !reflect.DeepEqual(got, wantSeen) {
				t.Fatalf("requests seen = %#v, want %#v", got, wantSeen)
			}
			for _, path := range forbidden {
				if got := gate.hitCount(path); got != 0 {
					t.Fatalf("ordinary route %s was hit %d times, want none", path, got)
				}
			}
		})
	}
}

// TestDispatchRetryReasonsMatchCRDVocabulary pins the adapter's reported
// refusal reasons against api/v1alpha1's closed retry vocabulary. The
// source-neutral dispatch package carries no Kubernetes dependency, so the
// equality lives here and any drift trips a test.
func TestDispatchRetryReasonsMatchCRDVocabulary(t *testing.T) {
	tests := []struct {
		name          string
		adapterReason string
		crdReason     string
	}{
		{name: "stale-generation", adapterReason: retryReasonStaleGeneration, crdReason: courierv1alpha1.RetryRefusalStaleGeneration},
		{name: "held-for-human", adapterReason: retryReasonHeldForHuman, crdReason: courierv1alpha1.RetryRefusalHeldForHuman},
		{name: "source-refused", adapterReason: retryReasonSourceRefused, crdReason: courierv1alpha1.RetryRefusalSourceRefused},
		{name: "already-requeued", adapterReason: retryReasonAlreadyRequeued, crdReason: courierv1alpha1.RetryRefusalAlreadyRequeued},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.adapterReason != tt.crdReason {
				t.Fatalf("adapter reason = %q, want the CRD vocabulary value %q", tt.adapterReason, tt.crdReason)
			}
		})
	}
}

// TestDispatchAdapterRequeueWithoutCapability pins the capability boundary:
// an adapter whose client cannot fence a requeue fails closed
// source.ErrUnsupportedRetry without issuing anything, and the validation
// order still short-circuits first — a nil client reports ErrNilClient and an
// empty work item ID reports ErrEmptyWorkItemID before the capability check
// ever runs.
func TestDispatchAdapterRequeueWithoutCapability(t *testing.T) {
	item := ciparkImplementItem()
	request := source.AttemptRequeueRequest{FenceToken: retryTestClaimReceipt, IdempotencyKey: "coderun/ns/rr/key"}
	t.Run("no-capability-client", func(t *testing.T) {
		// basicClient carries no requeue capability and no HTTP transport at
		// all, so this path cannot issue a request by construction.
		adapter := New(&basicClient{})
		if _, err := adapter.RequeueAttempt(context.Background(), item, request); !errors.Is(err, source.ErrUnsupportedRetry) {
			t.Fatalf("RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
		}
	})
	t.Run("nil-client", func(t *testing.T) {
		adapter := New(nil)
		if _, err := adapter.RequeueAttempt(context.Background(), item, request); !errors.Is(err, ErrNilClient) {
			t.Fatalf("RequeueAttempt() error = %v, want ErrNilClient", err)
		}
	})
	t.Run("empty-item-id", func(t *testing.T) {
		adapter := New(&basicClient{})
		if _, err := adapter.RequeueAttempt(context.Background(), source.WorkItem{}, request); !errors.Is(err, ErrEmptyWorkItemID) {
			t.Fatalf("RequeueAttempt() error = %v, want ErrEmptyWorkItemID", err)
		}
	})
}

// TestDispatchAttemptRequeueNoteIsBounded pins the audit note's bound: the
// operator's request ID is untrusted data forwarded for the source's audit, so
// an over-long value travels truncated at the contract maximum and never
// mangled mid-rune, and an absent note travels empty rather than fabricated.
func TestDispatchAttemptRequeueNoteIsBounded(t *testing.T) {
	long := strings.Repeat("a", 200) + strings.Repeat("é", 200)
	cases := []struct {
		name    string
		reason  string
		wantLen int
	}{
		{name: "short-note", reason: "req-1", wantLen: 5},
		{name: "empty-note", reason: "   ", wantLen: 0},
		{name: "over-long-note", reason: long, wantLen: maxRetryNoteLength},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var note string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/issues/state" {
					_ = json.NewEncoder(w).Encode(map[string]any{"issueId": "issue-1"})
					return
				}
				var body map[string]any
				_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
				note, _ = body["note"].(string)
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			}))
			defer server.Close()
			client := retryTestClient(t, server.URL)
			if _, err := client.RequeueAttempt(context.Background(), ciparkImplementItem().ID, source.AttemptRequeueRequest{
				FenceToken: retryTestClaimReceipt, IdempotencyKey: "coderun/ns/rr/key", Reason: tc.reason,
			}); err != nil {
				t.Fatalf("RequeueAttempt() error = %v", err)
			}
			if len(note) != tc.wantLen {
				t.Fatalf("note length = %d, want %d", len(note), tc.wantLen)
			}
			if !utf8.ValidString(note) {
				t.Fatalf("note = %q, want valid UTF-8 at the truncation boundary", note)
			}
		})
	}
}

// TestDispatchAttemptRequeueReissuedIdentityMustMatch pins that a success
// naming a queue item other than the one this attempt requeued, or a
// non-positive generation, is an error with no identity: a silently wrong
// attempt identity would link a successor to the wrong predecessor, which is
// worse than no identity at all (discovery then materializes the successor
// under its ordinary name and correlation lives in the retry events).
func TestDispatchAttemptRequeueReissuedIdentityMustMatch(t *testing.T) {
	for name, response := range map[string]map[string]any{
		"other-item":     {"success": true, "prFixItem": map[string]any{"id": "queue-item-2", "generation": 4}},
		"zero-generated": {"success": true, "prFixItem": map[string]any{"id": "queue-item-1", "generation": 0}},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			client := retryTestClient(t, server.URL)
			result, err := client.RequeueAttempt(context.Background(), ciparkWorkItem().ID, source.AttemptRequeueRequest{
				FenceToken: retryTestQueueReceipt, IdempotencyKey: "coderun/ns/rr/key",
			})
			if err == nil {
				t.Fatalf("RequeueAttempt() error = nil, want a mismatched reissue refused; result = %+v", result)
			}
			if result.AttemptID != "" {
				t.Fatalf("AttemptID = %q, want empty alongside the refusal", result.AttemptID)
			}
		})
	}
}

// TestDispatchAttemptRequeueBare404IsRouteEvidence pins the deliberate posture
// the latch takes on a 404: the pinned contract reserves 404 for route absence
// and codes every per-item condition 409, so a 404 that carries an item-shaped
// body is still read as version skew and latches for the process. The
// alternative — reading a per-item meaning into a status the contract does not
// grant it — would let one ambiguous response decide an operator's retry, and
// the retry request would keep retrying a route the deployment may not have at
// all. A server that answers per-item conditions with 404 trips this pin.
func TestDispatchAttemptRequeueBare404IsRouteEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"pr-fix item not found"}`, http.StatusNotFound)
	}))
	defer server.Close()
	client := retryTestClient(t, server.URL)
	_, err := client.RequeueAttempt(context.Background(), ciparkWorkItem().ID, source.AttemptRequeueRequest{
		FenceToken: retryTestQueueReceipt, IdempotencyKey: "coderun/ns/rr/key",
	})
	if !errors.Is(err, source.ErrUnsupportedRetry) {
		t.Fatalf("RequeueAttempt() error = %v, want source.ErrUnsupportedRetry", err)
	}
	if !client.retryUnsupported.Load() {
		t.Fatal("a 404 on a requeue route must latch the deployment as not having adopted it")
	}
}
