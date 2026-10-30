package adhoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// contributeRun builds a contribute-mode run exactly as Admit
// materializes it, with a caller-supplied source context.
func contributeRun(t *testing.T, sourceContext string) *courierv1alpha1.CoderRun {
	t.Helper()
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "contrib", Name: "courier-test-run"},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:          courierv1alpha1.ModeContribute,
			Source:        SourceName,
			WorkItemID:    "adhoc-0123456789ab",
			SourceContext: sourceContext,
		},
	}
}

// testContextJSON renders payload under the source-owned key in the same
// shape encodeContext writes, for hand-built contexts.
func testContextJSON(t *testing.T, payload any) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{contextKey: payload})
	if err != nil {
		t.Fatalf("marshal test context: %v", err)
	}
	return string(raw)
}

// TestContributionExecutionHappyPath is the decode of a run exactly as
// Admit materializes it: the goal survives byte-for-byte (punctuation, a
// double quote, a newline, and non-ASCII included), the base ref is
// recovered, and the decode is deterministic across calls and receivers —
// a resume or relaunch renders the identical goal.
func TestContributionExecutionHappyPath(t *testing.T) {
	ctx := context.Background()
	goal := "Fix login: handle \"edge case\"\nwhere ümlauts (— and émojis 🙂) appear."
	in := validAdmission()
	in.Goal = goal
	in.BaseRef = "trunk"

	c := newTestCluster(t)
	admitted, err := newAdapter(c).Admit(ctx, in)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	run := getRun(t, c, in.Namespace, admitted.RunName)

	first, err := newAdapter(c).ContributionExecution(run)
	if err != nil {
		t.Fatalf("ContributionExecution: %v", err)
	}
	if first.Goal != goal {
		t.Fatalf("Goal = %q; want the stored goal byte-for-byte %q", first.Goal, goal)
	}
	if first.BaseRef != "trunk" {
		t.Fatalf("BaseRef = %q; want %q", first.BaseRef, "trunk")
	}

	// The decode must not depend on the receiver: the same run through a
	// zero-value adapter yields the identical result.
	second, err := Adapter{}.ContributionExecution(run)
	if err != nil {
		t.Fatalf("ContributionExecution (zero adapter): %v", err)
	}
	if second != first {
		t.Fatalf("second decode %+v differs from first %+v; want identical", second, first)
	}
}

// TestContributionExecutionRoundTrip is the writer/reader pairing test:
// the context encodeContext produced for a valid admission decodes back
// to exactly the admitted goal and base ref, so the two sides cannot
// drift.
func TestContributionExecutionRoundTrip(t *testing.T) {
	in := validAdmission()
	raw, err := encodeContext(in)
	if err != nil {
		t.Fatalf("encodeContext: %v", err)
	}
	run := contributeRun(t, raw)

	got, err := newAdapter(nil).ContributionExecution(run)
	if err != nil {
		t.Fatalf("ContributionExecution: %v", err)
	}
	if got.Goal != in.Goal {
		t.Fatalf("Goal = %q; want the admitted %q", got.Goal, in.Goal)
	}
	if got.BaseRef != in.BaseRef {
		t.Fatalf("BaseRef = %q; want the admitted %q", got.BaseRef, in.BaseRef)
	}
}

// TestContributionExecutionIgnoresUntrustedAndUnknownFields pins the
// forward-compatible decode: the untrusted upstreamIssue is display
// context, not execution input, and unknown extra payload fields are
// skipped, so newer payloads still decode against an older reader.
func TestContributionExecutionIgnoresUntrustedAndUnknownFields(t *testing.T) {
	in := validAdmission()
	in.UpstreamIssue = "https://github.com/upstream/project/issues/42"
	raw, err := encodeContext(in)
	if err != nil {
		t.Fatalf("encodeContext: %v", err)
	}
	run := contributeRun(t, raw)

	got, err := newAdapter(nil).ContributionExecution(run)
	if err != nil {
		t.Fatalf("ContributionExecution: %v", err)
	}
	if got.Goal != in.Goal || got.BaseRef != in.BaseRef {
		t.Fatalf("got %+v; want goal %q and base ref %q", got, in.Goal, in.BaseRef)
	}

	unknown := testContextJSON(t, map[string]any{
		"goal":          "g",
		"baseRef":       "main",
		"someNew":       "field",
		"nested":        map[string]any{"a": 1},
		"upstreamIssue": "issue-1",
	})
	got, err = newAdapter(nil).ContributionExecution(contributeRun(t, unknown))
	if err != nil {
		t.Fatalf("ContributionExecution(unknown fields): %v", err)
	}
	if got.Goal != "g" || got.BaseRef != "main" {
		t.Fatalf("got %+v; want goal %q and base ref %q", got, "g", "main")
	}
}

// TestContributionExecutionNilRun fails closed on a nil run with the
// sentinel.
func TestContributionExecutionNilRun(t *testing.T) {
	_, err := newAdapter(nil).ContributionExecution(nil)
	if !errors.Is(err, ErrNilRun) {
		t.Fatalf("err = %v; want ErrNilRun", err)
	}
}

// TestContributionExecutionFailClosed pins every decode failure arm: each
// is rejected with ErrInvalidExecutionContext and a specific reason, and
// no partial execution input is ever returned.
func TestContributionExecutionFailClosed(t *testing.T) {
	validContext := testContextJSON(t, contextPayload{Goal: "g", BaseRef: "main"})
	mutated := func(mut func(*courierv1alpha1.CoderRun)) *courierv1alpha1.CoderRun {
		run := contributeRun(t, validContext)
		mut(run)
		return run
	}

	cases := []struct {
		name       string
		run        *courierv1alpha1.CoderRun
		wantReason string
	}{
		{
			name:       "foreign-source",
			run:        mutated(func(r *courierv1alpha1.CoderRun) { r.Spec.Source = "dispatch" }),
			wantReason: fmt.Sprintf("owned by source %q, not %q", "dispatch", SourceName),
		},
		{
			name:       "wrong-mode",
			run:        mutated(func(r *courierv1alpha1.CoderRun) { r.Spec.Mode = courierv1alpha1.ModeResolveIssue }),
			wantReason: fmt.Sprintf("is in mode %q, not %q", courierv1alpha1.ModeResolveIssue, courierv1alpha1.ModeContribute),
		},
		{
			name:       "empty-context",
			run:        contributeRun(t, ""),
			wantReason: "empty source context",
		},
		{
			name:       "blank-context",
			run:        contributeRun(t, "   "),
			wantReason: "empty source context",
		},
		{
			name:       "oversized-context",
			run:        contributeRun(t, testContextJSON(t, map[string]any{"goal": strings.Repeat("x", source.MaxSourceContextBytes*2), "baseRef": "main"})),
			wantReason: fmt.Sprintf("source context exceeds %d bytes", source.MaxSourceContextBytes),
		},
		{
			name:       "malformed-json",
			run:        contributeRun(t, `{"goal": "unterminated`),
			wantReason: "not valid json",
		},
		{
			name:       "different-top-level-key",
			run:        contributeRun(t, `{"other.source.dev/v1":{"goal":"g","baseRef":"main"}}`),
			wantReason: `carries key "other.source.dev/v1", not the`,
		},
		{
			name:       "multiple-top-level-keys",
			run:        contributeRun(t, `{"a":1,"b":2}`),
			wantReason: "has 2 top-level keys, want 1",
		},
		{
			name:       "payload-is-array",
			run:        contributeRun(t, testContextJSON(t, []any{1, 2})),
			wantReason: "value is not the",
		},
		{
			name:       "payload-is-scalar",
			run:        contributeRun(t, testContextJSON(t, "just a string")),
			wantReason: "value is not the",
		},
		{
			name:       "missing-goal",
			run:        contributeRun(t, testContextJSON(t, contextPayload{BaseRef: "main"})),
			wantReason: "goal is missing or blank",
		},
		{
			name:       "blank-goal",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "   ", BaseRef: "main"})),
			wantReason: "goal is missing or blank",
		},
		{
			name:       "missing-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g"})),
			wantReason: "base ref is missing or blank",
		},
		{
			name:       "blank-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g", BaseRef: " "})),
			wantReason: "base ref is missing or blank",
		},
		{
			name:       "overlong-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g", BaseRef: strings.Repeat("r", maxBaseRefBytes+1)})),
			wantReason: fmt.Sprintf("base ref exceeds %d bytes", maxBaseRefBytes),
		},
		{
			name:       "whitespace-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g", BaseRef: "main branch"})),
			wantReason: "must not contain whitespace",
		},
		{
			name:       "leading-slash-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g", BaseRef: "/main"})),
			wantReason: "must not start or end with /",
		},
		{
			name:       "trailing-slash-base-ref",
			run:        contributeRun(t, testContextJSON(t, contextPayload{Goal: "g", BaseRef: "main/"})),
			wantReason: "must not start or end with /",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newAdapter(nil).ContributionExecution(tc.run)
			if err == nil {
				t.Fatalf("ContributionExecution succeeded with %+v; want failure", got)
			}
			if !errors.Is(err, ErrInvalidExecutionContext) {
				t.Fatalf("err = %v; want ErrInvalidExecutionContext", err)
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("err = %q; want a reason containing %q", err, tc.wantReason)
			}
		})
	}
}
