package harness

import (
	"errors"
	"fmt"
)

// Context budget and step-boundary compaction policy (#243).
//
// The harness owns the context policy; the executor implements it. Today the
// native harness sets no budget and the OpenCode executor's auto-compaction
// fires reactively at its own default threshold, so a worker can ride its
// prompt past the bound model alias's input limit and only then compact. This
// file is the pure, executor-neutral seam that makes the limit a declared,
// profile-owned value and the trigger a mechanical, pre-ceiling step-boundary
// decision:
//
//   - ResolveContextBudget takes the minimum of the LaneProfile's declared
//     ceiling and the gateway-verified model input limit; a declared ceiling
//     used without a verified read is named `configured`, never `healthy`,
//     and neither declared nor verified is an explicit failure rather than an
//     implicit default.
//   - ShouldCompact fires only at a step boundary, before the next turn, when
//     the transcript has reached the compact point or the next turn plus the
//     output reservation would cross the effective limit.
//   - PlanCompaction replaces the oldest non-protected turns with one digest
//     and never digests the pinned brief, settled Decisions, OwnedFiles, or
//     the most recent turn; when protected state cannot fit it fails closed
//     rather than truncating mid-task.
//
// The shared truth table in budget_test.go is the source of truth for those
// choices. Wiring — the CRD field, the gateway probe, the session loop, and
// the session.compacted emitter — belongs to the bounded implementation
// children named in HARNESS.md §5 and §11.

const (
	// DefaultCompactAtPercent is the fraction (as an integer percent) of the
	// effective input limit at which a step-boundary compaction fires when the
	// policy declares none. 75 is 0.75 of the window.
	DefaultCompactAtPercent = 75
	// MinCompactAtPercent and MaxCompactAtPercent bound the compact fraction:
	// a point at or below 0 would compact every turn, and one at or above 100
	// would not precede the ceiling.
	MinCompactAtPercent = 1
	MaxCompactAtPercent = 99
)

var (
	// ErrNoContextCapacity reports a role with neither a declared budget nor a
	// gateway-verified model input limit. The harness refuses to substitute an
	// implicit context window: absence of a verified capability is not
	// successful capacity management.
	ErrNoContextCapacity = errors.New("harness: no declared or gateway-verified context capacity")
	// ErrContextReservation reports an output reservation that leaves no room
	// for input.
	ErrContextReservation = errors.New("harness: context output reservation leaves no input room")
	// ErrCompactionCannotFit reports that digesting every non-protected turn
	// still cannot satisfy prompt + output reservation <= effective limit.
	// Protected state is never truncated to make room.
	ErrCompactionCannotFit = errors.New("harness: compaction cannot fit protected state under the effective limit")
)

// ContextBudgetInput is the resolved policy input for one role: the ceiling
// the LaneProfile declared and the input limit the gateway verified. Either
// may be zero (not declared / not verified); CompactAtPercent zero selects
// DefaultCompactAtPercent.
type ContextBudgetInput struct {
	Role                   string
	DeclaredMaxInputTokens int
	ProbedMaxInputTokens   int
	CompactAtPercent       int
	OutputReservation      int
}

// ContextBudget is one role's resolved context policy. State reuses the §5
// capability vocabulary: healthy (gateway-verified), configured (declared but
// not verified), unavailable (neither — the caller must not proceed as if a
// budget existed). EffectiveLimit is the minimum of the declared and verified
// limits; CompactPoint is the step-boundary trigger in tokens.
type ContextBudget struct {
	Role              string
	State             string
	EffectiveLimit    int
	CompactPoint      int
	OutputReservation int
	Detail            string
}

// ResolveContextBudget derives a role's effective limit and compact point.
// The effective limit is the minimum of the declared budget and the verified
// gateway capability: a profile can only lower the limit, never raise it past
// what the gateway advertises. A verified read is `healthy`; a declared
// ceiling used because the gateway could not verify is `configured`; neither
// is ErrNoContextCapacity.
func ResolveContextBudget(in ContextBudgetInput) (ContextBudget, error) {
	if in.DeclaredMaxInputTokens < 0 || in.ProbedMaxInputTokens < 0 {
		return ContextBudget{}, fmt.Errorf("harness: context budget for role %q has a negative token limit", in.Role)
	}
	if in.OutputReservation < 0 {
		return ContextBudget{}, fmt.Errorf("harness: context budget for role %q has a negative output reservation", in.Role)
	}
	percent := in.CompactAtPercent
	if percent == 0 {
		percent = DefaultCompactAtPercent
	}
	if percent < MinCompactAtPercent || percent > MaxCompactAtPercent {
		return ContextBudget{}, fmt.Errorf("harness: context budget for role %q compactAtPercent %d is outside [%d,%d]", in.Role, percent, MinCompactAtPercent, MaxCompactAtPercent)
	}

	effective := 0
	state := StateUnavailable
	detail := ""
	switch {
	case in.DeclaredMaxInputTokens > 0 && in.ProbedMaxInputTokens > 0:
		effective = min(in.DeclaredMaxInputTokens, in.ProbedMaxInputTokens)
		state = StateHealthy
		detail = "effective limit is the lower of the declared budget and the gateway-verified model input limit"
	case in.ProbedMaxInputTokens > 0:
		effective = in.ProbedMaxInputTokens
		state = StateHealthy
		detail = "effective limit is the gateway-verified model input limit"
	case in.DeclaredMaxInputTokens > 0:
		effective = in.DeclaredMaxInputTokens
		state = StateConfigured
		detail = "effective limit is the declared budget; the gateway did not verify the model input limit"
	default:
		return ContextBudget{}, fmt.Errorf("%w: role %q", ErrNoContextCapacity, in.Role)
	}

	if in.OutputReservation >= effective {
		return ContextBudget{}, fmt.Errorf("%w: role %q reserves %d of %d", ErrContextReservation, in.Role, in.OutputReservation, effective)
	}
	point := effective * percent / 100
	if point < 1 {
		point = 1
	}
	return ContextBudget{
		Role:              in.Role,
		State:             state,
		EffectiveLimit:    effective,
		CompactPoint:      point,
		OutputReservation: in.OutputReservation,
		Detail:            detail,
	}, nil
}

// ShouldCompact reports whether a step boundary must compact before the next
// turn: the transcript has reached the compact point, or the next turn's
// prompt plus the output reservation would cross the effective limit. It is
// evaluated between turns, never mid-turn. A zero budget owns no policy and
// never triggers.
func (b ContextBudget) ShouldCompact(promptTokens int) bool {
	if b.EffectiveLimit <= 0 {
		return false
	}
	if promptTokens >= b.CompactPoint {
		return true
	}
	return promptTokens+b.OutputReservation > b.EffectiveLimit
}

// FitsAfterCompaction reports whether a post-compaction prompt plus the output
// reservation stays within the effective limit. Compaction that cannot reach
// this state fails closed rather than truncating protected state.
func (b ContextBudget) FitsAfterCompaction(promptTokens int) bool {
	return promptTokens+b.OutputReservation <= b.EffectiveLimit
}

// CompactionInput is one step-boundary compaction decision over a transcript.
// MessageTokens is the per-message prompt cost, oldest first; Protected marks
// messages that must never be digested (the pinned brief/objective, settled
// Decisions, OwnedFiles, and the most recent turn); DigestTokens is the
// estimated cost of the single digest message that replaces the selected
// span.
type CompactionInput struct {
	MessageTokens []int
	Protected     []bool
	DigestTokens  int
	Budget        ContextBudget
}

// CompactionPlan is the selected span and its token accounting. Digested lists
// the message indexes replaced by the digest, oldest first; Retained lists the
// protected indexes that survive verbatim. After includes the digest cost, so
// whenever Compacted is true, After < Before (the digest must reclaim tokens)
// and After + budget.OutputReservation <= EffectiveLimit.
type CompactionPlan struct {
	Compacted bool
	Digested  []int
	Retained  []int
	Before    int
	After     int
	Reclaimed int
}

// PlanCompaction selects the oldest non-protected turns to replace with one
// digest until the post-condition holds, and never digests a protected
// message. A plan is accepted only when it both fits and actually reclaims
// tokens (After < Before): a digest that costs as much as it replaces would
// re-trigger on the next boundary forever. When even digesting every
// non-protected turn cannot fit or reclaim, it fails closed with
// ErrCompactionCannotFit. A transcript already within budget is returned
// unchanged (Compacted false).
func PlanCompaction(in CompactionInput) (CompactionPlan, error) {
	before := 0
	for _, tokens := range in.MessageTokens {
		if tokens < 0 {
			return CompactionPlan{}, errors.New("harness: compaction input has a negative message token count")
		}
		before += tokens
	}
	if in.DigestTokens < 0 {
		return CompactionPlan{}, errors.New("harness: compaction input has a negative digest token count")
	}
	plan := CompactionPlan{Before: before, After: before, Retained: protectedIndexes(in.Protected)}
	if !in.Budget.ShouldCompact(before) {
		return plan, nil
	}
	reclaimed := 0
	digested := make([]int, 0, len(in.MessageTokens))
	for i, tokens := range in.MessageTokens {
		if protectedAt(in.Protected, i) {
			continue
		}
		digested = append(digested, i)
		reclaimed += tokens
		after := before - reclaimed + in.DigestTokens
		if after < before && in.Budget.FitsAfterCompaction(after) {
			plan.Compacted = true
			plan.Digested = digested
			plan.After = after
			plan.Reclaimed = reclaimed
			return plan, nil
		}
	}
	return CompactionPlan{}, fmt.Errorf("%w: before %d, digest %d, protected %d, effective %d, reservation %d",
		ErrCompactionCannotFit, before, in.DigestTokens, before-reclaimed, in.Budget.EffectiveLimit, in.Budget.OutputReservation)
}

// CompactEvent is the payload of the session.compacted run event (#243),
// whose canonical name is log.EventSessionCompacted. It reports token counts
// only: before and after the compaction and the reclaimed total, plus the role
// and its bound model. It is observability, never status, and never lets a
// model assert liveness or completion.
type CompactEvent struct {
	Role      string
	Model     string
	Before    int
	After     int
	Reclaimed int
}

func protectedAt(protected []bool, i int) bool {
	return i < len(protected) && protected[i]
}

func protectedIndexes(protected []bool) []int {
	var out []int
	for i, p := range protected {
		if p {
			out = append(out, i)
		}
	}
	return out
}
