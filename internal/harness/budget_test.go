package harness

import (
	"errors"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/log"
)

// The shared behavioral truth table for the context-budget and step-boundary
// compaction policy (#243). It is the source of truth for the four settled
// choices: per-role budget resolution, effective limit = min(declared,
// gateway-verified), the pre-ceiling step-boundary trigger with output
// reservation, and digest selection that preserves settled state. The CRD
// field, the gateway probe, the session loop, and the session.compacted
// emitter are bounded implementation children (#295, #296, #297); this table
// proves the pure policy they will call, and deliberately proves nothing about
// their wiring.

func TestResolveContextBudgetTruthTable(t *testing.T) {
	tests := []struct {
		name      string
		in        ContextBudgetInput
		wantState string
		wantLimit int
		wantPoint int
		wantErr   error
	}{
		{
			name:      "declared and verified take the lower gateway limit",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 100000, ProbedMaxInputTokens: 80000, CompactAtPercent: 75},
			wantState: StateHealthy, wantLimit: 80000, wantPoint: 60000,
		},
		{
			name:      "declared ceiling lowers a higher gateway limit",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 60000, ProbedMaxInputTokens: 80000, CompactAtPercent: 75},
			wantState: StateHealthy, wantLimit: 60000, wantPoint: 45000,
		},
		{
			name:      "verified only uses the gateway limit and default fraction",
			in:        ContextBudgetInput{Role: "reviewer", ProbedMaxInputTokens: 128000},
			wantState: StateHealthy, wantLimit: 128000, wantPoint: 96000,
		},
		{
			name:      "declared only is configured, never healthy",
			in:        ContextBudgetInput{Role: "coordinator", DeclaredMaxInputTokens: 100000},
			wantState: StateConfigured, wantLimit: 100000, wantPoint: 75000,
		},
		{
			name:    "neither declared nor verified fails closed",
			in:      ContextBudgetInput{Role: "coder"},
			wantErr: ErrNoContextCapacity,
		},
		{
			name:      "explicit compact fraction",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 90},
			wantState: StateConfigured, wantLimit: 1000, wantPoint: 900,
		},
		{
			name:      "reservation below the fraction does not move the point",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 100},
			wantState: StateConfigured, wantLimit: 1000, wantPoint: 750,
		},
		{
			name:      "minimum compact fraction",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 1},
			wantState: StateConfigured, wantLimit: 1000, wantPoint: 10,
		},
		{
			name:      "maximum compact fraction",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 99},
			wantState: StateConfigured, wantLimit: 1000, wantPoint: 990,
		},
		{
			name:      "declared equal to verified stays healthy at the shared limit",
			in:        ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 80000, ProbedMaxInputTokens: 80000, CompactAtPercent: 75},
			wantState: StateHealthy, wantLimit: 80000, wantPoint: 60000,
		},
		{
			name:    "compact fraction at the ceiling is rejected",
			in:      ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 100},
			wantErr: errors.New("compactAtPercent"),
		},
		{
			name:    "negative verified limit is rejected",
			in:      ContextBudgetInput{Role: "coder", ProbedMaxInputTokens: -1},
			wantErr: errors.New("negative token limit"),
		},
		{
			name:    "reservation at the limit leaves no input room",
			in:      ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 1000},
			wantErr: ErrContextReservation,
		},
		{
			name:    "negative declared limit is rejected",
			in:      ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: -1, ProbedMaxInputTokens: 1000},
			wantErr: errors.New("negative token limit"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveContextBudget(tt.in)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("expected error matching %v, got nil (%+v)", tt.wantErr, got)
				}
				if !errors.Is(err, tt.wantErr) && !strings.Contains(err.Error(), tt.wantErr.Error()) {
					t.Fatalf("error %v does not match %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.State != tt.wantState || got.EffectiveLimit != tt.wantLimit || got.CompactPoint != tt.wantPoint {
				t.Fatalf("resolved %+v, want state=%s limit=%d point=%d", got, tt.wantState, tt.wantLimit, tt.wantPoint)
			}
			if got.OutputReservation != tt.in.OutputReservation {
				t.Fatalf("reservation = %d, want %d", got.OutputReservation, tt.in.OutputReservation)
			}
		})
	}
}

func TestContextBudgetShouldCompactTruthTable(t *testing.T) {
	resolved := func(t *testing.T, in ContextBudgetInput) ContextBudget {
		t.Helper()
		budget, err := ResolveContextBudget(in)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		return budget
	}
	fraction := resolved(t, ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 100})
	reservation := resolved(t, ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 99, OutputReservation: 200})

	tests := []struct {
		name   string
		budget ContextBudget
		tokens int
		want   bool
	}{
		{name: "no budget never triggers", budget: ContextBudget{}, tokens: 1000000, want: false},
		{name: "below the fraction", budget: fraction, tokens: 749, want: false},
		{name: "at the fraction", budget: fraction, tokens: 750, want: true},
		{name: "above the fraction", budget: fraction, tokens: 900, want: true},
		{name: "below the fraction and the reservation fits", budget: reservation, tokens: 700, want: false},
		{name: "below the fraction but the reservation would cross the ceiling", budget: reservation, tokens: 850, want: true},
		{name: "reservation exactly at the ceiling does not trigger", budget: reservation, tokens: 800, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.budget.ShouldCompact(tt.tokens); got != tt.want {
				t.Fatalf("ShouldCompact(%d) with %+v = %v, want %v", tt.tokens, tt.budget, got, tt.want)
			}
		})
	}
}

func TestPlanCompactionTruthTable(t *testing.T) {
	budget := func(t *testing.T, declared, percent, reservation int) ContextBudget {
		t.Helper()
		b, err := ResolveContextBudget(ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: declared, CompactAtPercent: percent, OutputReservation: reservation})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		return b
	}

	tests := []struct {
		name             string
		in               CompactionInput
		wantCompacted    bool
		wantDigested     []int
		wantRetained     []int
		wantAfter        int
		wantDigestedToks int
		wantNetSaved     int
		wantErr          error
	}{
		{
			name: "within budget is unchanged",
			in: CompactionInput{
				MessageTokens: []int{100, 100}, Protected: []bool{true, false}, DigestTokens: 10,
				Budget: budget(t, 1000, 75, 100),
			},
			wantCompacted: false, wantRetained: []int{0}, wantAfter: 200,
		},
		{
			name: "at the fraction digests the oldest non-protected turn until it fits",
			in: CompactionInput{
				MessageTokens: []int{500, 300, 200}, DigestTokens: 50,
				Budget: budget(t, 1000, 75, 100),
			},
			wantCompacted: true, wantDigested: []int{0}, wantAfter: 550, wantDigestedToks: 500, wantNetSaved: 450,
		},
		{
			name: "protected turns are never digested",
			in: CompactionInput{
				MessageTokens: []int{600, 100, 300}, Protected: []bool{true, false, false}, DigestTokens: 50,
				Budget: budget(t, 1000, 75, 100),
			},
			wantCompacted: true, wantDigested: []int{1, 2}, wantRetained: []int{0}, wantAfter: 650, wantDigestedToks: 400, wantNetSaved: 350,
		},
		{
			name: "protected state that cannot fit fails closed",
			in: CompactionInput{
				MessageTokens: []int{900, 50}, Protected: []bool{true, false}, DigestTokens: 50,
				Budget: budget(t, 1000, 75, 100),
			},
			wantErr: ErrCompactionCannotFit,
		},
		{
			name: "an all-protected transcript that cannot fit fails closed",
			in: CompactionInput{
				MessageTokens: []int{500, 500}, Protected: []bool{true, true}, DigestTokens: 10,
				Budget: budget(t, 1000, 75, 100),
			},
			wantErr: ErrCompactionCannotFit,
		},
		{
			name: "digesting every non-protected turn still cannot fit fails closed",
			in: CompactionInput{
				MessageTokens: []int{900, 200}, Protected: []bool{true, false}, DigestTokens: 10,
				Budget: budget(t, 1000, 75, 100),
			},
			wantErr: ErrCompactionCannotFit,
		},
		{
			name: "a digest costlier than what it reclaims fails closed",
			in: CompactionInput{
				MessageTokens: []int{10, 10}, DigestTokens: 100,
				Budget: budget(t, 20, 75, 0),
			},
			wantErr: ErrCompactionCannotFit,
		},
		{
			name: "a digest that reclaims nothing fails closed",
			in: CompactionInput{
				MessageTokens: []int{80}, DigestTokens: 80,
				Budget: budget(t, 100, 75, 0),
			},
			wantErr: ErrCompactionCannotFit,
		},
		{
			name: "an empty transcript is unchanged",
			in: CompactionInput{
				Budget: budget(t, 100, 75, 0),
			},
			wantCompacted: false, wantAfter: 0,
		},
		{
			name: "fixed request cost reaches the compact point the transcript alone misses",
			in: CompactionInput{
				MessageTokens: []int{400, 200}, Protected: []bool{true, false}, DigestTokens: 50, FixedTokens: 200,
				Budget: budget(t, 1000, 75, 0),
			},
			wantCompacted: true, wantDigested: []int{1}, wantRetained: []int{0}, wantAfter: 650, wantDigestedToks: 200, wantNetSaved: 150,
		},
		{
			name: "fixed cost that alone exceeds the limit fails closed over protected state",
			in: CompactionInput{
				MessageTokens: []int{900}, Protected: []bool{true}, DigestTokens: 10, FixedTokens: 200,
				Budget: budget(t, 1000, 75, 0),
			},
			wantErr: ErrCompactionCannotFit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PlanCompaction(tt.in)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Compacted != tt.wantCompacted {
				t.Fatalf("Compacted = %v, want %v (%+v)", got.Compacted, tt.wantCompacted, got)
			}
			if !equalInts(got.Digested, tt.wantDigested) {
				t.Fatalf("Digested = %v, want %v", got.Digested, tt.wantDigested)
			}
			if !equalInts(got.Retained, tt.wantRetained) {
				t.Fatalf("Retained = %v, want %v", got.Retained, tt.wantRetained)
			}
			if got.After != tt.wantAfter {
				t.Fatalf("After = %d, want %d", got.After, tt.wantAfter)
			}
			if got.DigestedTokens != tt.wantDigestedToks {
				t.Fatalf("DigestedTokens = %d, want %d", got.DigestedTokens, tt.wantDigestedToks)
			}
			if got.NetSaved != tt.wantNetSaved {
				t.Fatalf("NetSaved = %d, want %d", got.NetSaved, tt.wantNetSaved)
			}
			if got.Compacted {
				if got.DigestedTokens-got.NetSaved != tt.in.DigestTokens {
					t.Fatalf("DigestedTokens(%d) - NetSaved(%d) = %d, want digest cost %d", got.DigestedTokens, got.NetSaved, got.DigestedTokens-got.NetSaved, tt.in.DigestTokens)
				}
				if got.NetSaved <= 0 {
					t.Fatalf("NetSaved = %d, want > 0 when Compacted", got.NetSaved)
				}
				if !tt.in.Budget.FitsAfterCompaction(got.After) {
					t.Fatalf("post-condition violated: After %d + reservation %d > effective %d", got.After, tt.in.Budget.OutputReservation, tt.in.Budget.EffectiveLimit)
				}
				if got.BudgetAfter.CompactPoint <= got.After {
					t.Fatalf("hysteresis violated: post-compaction compact point %d does not exceed After %d", got.BudgetAfter.CompactPoint, got.After)
				}
				if got.BudgetAfter.ShouldCompact(got.After) {
					t.Fatalf("post-compaction ShouldCompact(After=%d) is true with compact point %d", got.After, got.BudgetAfter.CompactPoint)
				}
			}
			for _, index := range got.Digested {
				if protectedAt(tt.in.Protected, index) {
					t.Fatalf("digested protected message index %d", index)
				}
			}
		})
	}
}

func TestFitsAfterCompactionBoundary(t *testing.T) {
	budget, err := ResolveContextBudget(ContextBudgetInput{Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 200})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !budget.FitsAfterCompaction(800) {
		t.Fatalf("800 + 200 should fit 1000")
	}
	if budget.FitsAfterCompaction(801) {
		t.Fatalf("801 + 200 must not fit 1000")
	}
}

// TestPlanCompactionTwoBoundaryHysteresis pins the post-compaction
// hysteresis: a successful compaction must return a BudgetAfter whose
// compact point sits above the post-compaction After, so the next step
// boundary's ShouldCompact does not re-fire on the just-compacted state.
// The reviewer's example (effective 1000, point 750, reserve 100, transcript
// [100,750] with [false,true] protected, digest 90) used to accept 840
// while leaving ShouldCompact(840) true on the same budget, which then
// re-triggered into ErrCompactionCannotFit on the very next boundary. The
// second compact call must therefore run against BudgetAfter, not the
// original budget.
func TestPlanCompactionTwoBoundaryHysteresis(t *testing.T) {
	budget, err := ResolveContextBudget(ContextBudgetInput{
		Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 100,
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	first, err := PlanCompaction(CompactionInput{
		MessageTokens: []int{100, 750},
		Protected:     []bool{false, true},
		DigestTokens:  90,
		Budget:        budget,
	})
	if err != nil {
		t.Fatalf("first boundary: %v", err)
	}
	if !first.Compacted {
		t.Fatalf("first boundary did not compact")
	}
	if first.After != 840 {
		t.Fatalf("first boundary After = %d, want 840", first.After)
	}
	if first.DigestedTokens != 100 || first.NetSaved != 10 {
		t.Fatalf("first boundary tokens: DigestedTokens=%d NetSaved=%d, want 100/10", first.DigestedTokens, first.NetSaved)
	}
	if first.BudgetAfter.CompactPoint <= first.After {
		t.Fatalf("first boundary did not raise compact point: %d <= %d", first.BudgetAfter.CompactPoint, first.After)
	}

	second, err := PlanCompaction(CompactionInput{
		MessageTokens: []int{90, 750},
		Protected:     []bool{false, true},
		DigestTokens:  90,
		Budget:        first.BudgetAfter,
	})
	if err != nil {
		t.Fatalf("second boundary: %v", err)
	}
	if second.Compacted {
		t.Fatalf("second boundary should not re-fire on the post-compaction state (got Compacted=%+v)", second)
	}
	if second.BudgetAfter.CompactPoint != first.BudgetAfter.CompactPoint {
		t.Fatalf("second boundary raised compact point: was %d, now %d", first.BudgetAfter.CompactPoint, second.BudgetAfter.CompactPoint)
	}

	withoutHysteresis, err := PlanCompaction(CompactionInput{
		MessageTokens: []int{90, 750},
		Protected:     []bool{false, true},
		DigestTokens:  90,
		Budget:        budget,
	})
	if err == nil {
		t.Fatalf("original budget should have failed closed: got %+v", withoutHysteresis)
	}
	if !errors.Is(err, ErrCompactionCannotFit) {
		t.Fatalf("original budget error = %v, want ErrCompactionCannotFit", err)
	}
}

// TestPlanCompactionFailsClosedAfterDigestingEveryTurn pins the fail-closed
// branch the truth table row of the same name exercises: every non-protected
// turn is digested, the digest does reclaim tokens, and the post-condition is
// still violated because protected state alone outweighs the room left under
// the effective limit. Without this, a transcript can neither progress nor
// reclaim further, and failing closed is the only outcome that keeps protected
// state intact.
func TestPlanCompactionFailsClosedAfterDigestingEveryTurn(t *testing.T) {
	budget, err := ResolveContextBudget(ContextBudgetInput{
		Role: "coder", DeclaredMaxInputTokens: 1000, CompactAtPercent: 75, OutputReservation: 100,
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	in := CompactionInput{
		MessageTokens: []int{900, 200},
		Protected:     []bool{true, false},
		DigestTokens:  10,
		Budget:        budget,
	}
	if _, err := PlanCompaction(in); !errors.Is(err, ErrCompactionCannotFit) {
		t.Fatalf("error = %v, want ErrCompactionCannotFit", err)
	}

	var before int
	for _, tokens := range in.MessageTokens {
		before += tokens
	}
	after := before - in.MessageTokens[1] + in.DigestTokens
	if after >= before {
		t.Fatalf("fixture no longer reclaims: after %d >= before %d, so this row would exercise the zero-reclaim branch", after, before)
	}
	if budget.FitsAfterCompaction(after) {
		t.Fatalf("fixture now fits: after %d + reservation %d <= effective %d, so this row would compact", after, budget.OutputReservation, budget.EffectiveLimit)
	}
}

func TestSessionCompactedEventVocabulary(t *testing.T) {
	if log.EventSessionCompacted != "session.compacted" {
		t.Fatalf("log event = %q", log.EventSessionCompacted)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
