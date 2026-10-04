package harness

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
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

// BriefResult is the outcome of one brief. Everything in it is untrusted
// worker/agent output: a summary is a claim, and an artifact is data that
// trusted validation (#125) must verify before integration. A result can
// never publish anything.
type BriefResult struct {
	BriefID  string `json:"briefID"`
	Summary  string `json:"summary"`
	Artifact []byte `json:"artifact,omitempty"`
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
	mu     sync.Mutex
	briefs map[string]Brief
	order  []string
	tombs  map[string]struct{}
}

// NewBriefRegistry returns an empty ledger.
func NewBriefRegistry() *BriefRegistry {
	return &BriefRegistry{
		briefs: make(map[string]Brief),
		tombs:  make(map[string]struct{}),
	}
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
