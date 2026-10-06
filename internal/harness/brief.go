package harness

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// ErrDuplicateBrief reports a dispatch of a briefID that already exists in
// the run. Brief IDs identify work units: two executions of one ID would
// double-apply work, so the second is always rejected.
var ErrDuplicateBrief = errors.New("harness: brief ID already exists in this run")

// ErrBriefCancelled reports dispatch or retry of a brief whose work unit was
// cancelled. A tombstone is final for the work unit: new work needs a new
// brief ID.
var ErrBriefCancelled = errors.New("harness: brief is cancelled and cannot run again")

// ErrBriefUnknown reports an operation against a brief ID control never
// registered.
var ErrBriefUnknown = errors.New("harness: unknown brief ID")

// Brief is one typed delegation unit (HARNESS.md §5). It carries a stable
// briefID unique within the run, the objective, settled decisions with exact
// values, the files the delegate owns, non-goals, and an observable success
// check. Every field is fixed by trusted control from the coordinator's
// request — the worker never amends a brief.
type Brief struct {
	ID           string     `json:"id"`
	Role         string     `json:"role"`
	Objective    string     `json:"objective"`
	Decisions    []Decision `json:"decisions,omitempty"`
	OwnedFiles   []string   `json:"ownedFiles,omitempty"`
	NonGoals     []string   `json:"nonGoals,omitempty"`
	SuccessCheck string     `json:"successCheck"`
}

// Decision is one settled decision with its exact value. Delegates apply the
// value; they do not re-derive it.
type Decision struct {
	Summary string `json:"summary"`
	Value   string `json:"value"`
}

// BriefResult is the outcome of one brief. The summary is untrusted
// worker/agent output — a claim, never validation input. Commit is the
// trusted local integration commit recorded after the artifact passed §5
// validation and the broker confirmed publication; it is empty when the
// brief integrated no changes. A result can never publish anything.
type BriefResult struct {
	BriefID string `json:"briefID"`
	Summary string `json:"summary"`
	Commit  string `json:"commit,omitempty"`
}

// Validate checks a brief's structural invariants before registration: a
// brief without an identity, a role, an objective, or a success check is not
// dispatchable work. The ID must be a single URL path segment because the
// worker protocol carries operation IDs derived from it.
func (b Brief) Validate() error {
	if strings.TrimSpace(b.ID) == "" {
		return errors.New("harness: brief requires an ID")
	}
	if !isBriefID(b.ID) {
		return errors.New("harness: brief ID must be letters, digits, dots, dashes, or underscores")
	}
	if strings.TrimSpace(b.Role) == "" {
		return errors.New("harness: brief requires a role")
	}
	if strings.TrimSpace(b.Objective) == "" {
		return errors.New("harness: brief requires an objective")
	}
	if strings.TrimSpace(b.SuccessCheck) == "" {
		return errors.New("harness: brief requires an observable success check")
	}
	for _, decision := range b.Decisions {
		if strings.TrimSpace(decision.Summary) == "" || strings.TrimSpace(decision.Value) == "" {
			return errors.New("harness: brief decisions must carry a summary and an exact value")
		}
	}
	return nil
}

// maxBriefIDLength caps brief IDs so composed operation IDs
// ("<kind>.<briefID>.<nonce>") stay within the worker protocol's 128-byte
// single path segment bound: "shell." (6) + 96 + "." (1) + 16 hex = 119.
const maxBriefIDLength = 96

// isBriefID reports whether the ID is a non-empty single URL path segment
// from a conservative alphabet, short enough to compose into operation IDs.
func isBriefID(id string) bool {
	if id == "" || len(id) > maxBriefIDLength {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// BriefRegistry is the run's brief ledger, owned by trusted control. It
// enforces the dedupe and cancellation-tombstone semantics of §5: one
// execution per brief ID, cancellation final — any later dispatch or retry
// with a cancelled ID is rejected regardless of what any worker saw.
type BriefRegistry struct {
	mu      sync.Mutex
	briefs  map[string]Brief
	order   []string
	tombs   map[string]struct{}
	results map[string]BriefResult
}

// NewBriefRegistry returns an empty ledger.
func NewBriefRegistry() *BriefRegistry {
	return &BriefRegistry{
		briefs:  make(map[string]Brief),
		tombs:   make(map[string]struct{}),
		results: make(map[string]BriefResult),
	}
}

// RecordResult binds the delegate's actual result to its stable brief ID.
// The result is untrusted data retained for the coordinator's own
// publication plan; recording it never advances any status.
func (r *BriefRegistry) RecordResult(briefID string, result BriefResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.briefs[briefID]; !ok {
		return
	}
	result.BriefID = briefID
	r.results[briefID] = result
}

// Result returns the recorded result for a brief, if any.
func (r *BriefRegistry) Result(briefID string) (BriefResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, ok := r.results[briefID]
	return result, ok
}

// Register records a new brief. A duplicate ID is rejected without any
// worker contact.
func (r *BriefRegistry) Register(brief Brief) (Brief, error) {
	if err := brief.Validate(); err != nil {
		return Brief{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.briefs[brief.ID]; ok {
		return Brief{}, fmt.Errorf("%w: %s", ErrDuplicateBrief, brief.ID)
	}
	r.briefs[brief.ID] = brief
	r.order = append(r.order, brief.ID)
	return brief, nil
}

// Cancel tombstones a registered brief and reports whether a worker
// operation for it may still be live (the caller reconciles that operation).
func (r *BriefRegistry) Cancel(briefID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.briefs[briefID]; !ok {
		return false, fmt.Errorf("%w: %s", ErrBriefUnknown, briefID)
	}
	r.tombs[briefID] = struct{}{}
	return true, nil
}

// Cancelled reports whether a brief ID is tombstoned.
func (r *BriefRegistry) Cancelled(briefID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.tombs[briefID]
	return ok
}

// Lookup returns a registered brief.
func (r *BriefRegistry) Lookup(briefID string) (Brief, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	brief, ok := r.briefs[briefID]
	return brief, ok
}

// BriefIDs returns every registered brief ID in registration order.
func (r *BriefRegistry) BriefIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

// CompletedBriefs renders the durable checkpoint record (§7): every brief
// with a recorded result, in registration order. The summary is the untrusted
// outcome text retained for recovery; the commit is the integration commit
// whose publication the broker confirmed. It is exactly the existing
// Checkpoint shape — plan plus ordered completedBriefs — with no new fields.
func (r *BriefRegistry) CompletedBriefs() []courierv1alpha1.Brief {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []courierv1alpha1.Brief
	for _, id := range r.order {
		if result, ok := r.results[id]; ok {
			out = append(out, courierv1alpha1.Brief{ID: id, Summary: result.Summary, Commit: result.Commit})
		}
	}
	return out
}

// newOpID mints a fresh operation ID: unique within the run across retries
// and pod restarts, single URL path segment, carrying the kind and the brief
// it belongs to (empty for coordinator-level work).
func newOpID(kind, briefID string) string {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		panic("harness: entropy unavailable")
	}
	if briefID == "" {
		return fmt.Sprintf("%s.%s", kind, hex.EncodeToString(nonce))
	}
	return fmt.Sprintf("%s.%s.%s", kind, briefID, hex.EncodeToString(nonce))
}
