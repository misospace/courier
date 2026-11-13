package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
)

// The fix-pr launch reads the reviewer's remediation handoff (#755) through
// the configured resolver, pins the read to the head the run was admitted
// against, and degrades — never fails the launch — when no handoff exists.

type recordingHandoffResolver struct {
	calls  int
	repo   string
	number int
	who    string
	head   string

	handoff string
	found   bool
	err     error
}

func (r *recordingHandoffResolver) ReviewHandoff(_ context.Context, repo string, number int, reviewerLogin, headSHA string) (string, bool, error) {
	r.calls++
	r.repo, r.number, r.who, r.head = repo, number, reviewerLogin, headSHA
	return r.handoff, r.found, r.err
}

func fixPRSecureRun(name string) *courier.CoderRun {
	run := secureRun(name)
	run.Spec.Mode = courier.ModeFixPR
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:        string(run.UID),
		PRNumber:      7,
		HeadAnchorOID: "cafe1234cafe1234cafe1234cafe1234cafe1234",
	}
	return run
}

const handoffTestPayload = `{"schema":"review-handoff/v1","review_id":"r1","head_sha":"cafe1234","verdict":"request_changes","review_result":"issues","required_checks":"complete","items":[],"items_total":0,"items_truncated":false}`

func TestControlInvocationReadsReviewerHandoffForFixPR(t *testing.T) {
	control, c := secureControl(t)
	resolver := &recordingHandoffResolver{handoff: handoffTestPayload, found: true}
	control.ReviewHandoffs = resolver
	control.Config.ReviewerIdentity = "pr-reviewer[bot]"
	run := fixPRSecureRun("handoff-hit")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	invocation, err := control.controlInvocation(context.Background(), run)
	if err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
	if resolver.repo != run.Spec.Repo || resolver.number != run.Spec.Ref || resolver.who != "pr-reviewer[bot]" {
		t.Fatalf("resolver read %+v, want repo %q pr %d identity pr-reviewer[bot]", resolver, run.Spec.Repo, run.Spec.Ref)
	}
	if resolver.head != "cafe1234cafe1234cafe1234cafe1234cafe1234" {
		t.Fatalf("resolver head = %q, want the admission-pinned head anchor", resolver.head)
	}
	if !strings.Contains(invocation.Goal, handoffTestPayload) {
		t.Fatalf("goal omits the handoff:\n%s", invocation.Goal)
	}
	if !strings.Contains(invocation.Goal, "resolve every blocker and major finding") {
		t.Fatalf("goal omits the fix instruction:\n%s", invocation.Goal)
	}
}

func TestControlInvocationPrefersObservedHeadOverPinnedAnchor(t *testing.T) {
	control, c := secureControl(t)
	resolver := &recordingHandoffResolver{}
	control.ReviewHandoffs = resolver
	control.Config.ReviewerIdentity = "pr-reviewer"
	run := fixPRSecureRun("handoff-live-head")
	run.Status.HeadSHA = "beef0000beef0000beef0000beef0000beef0000"
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := control.controlInvocation(context.Background(), run); err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	if resolver.head != run.Status.HeadSHA {
		t.Fatalf("resolver head = %q, want the observed head %q", resolver.head, run.Status.HeadSHA)
	}
}

func TestControlInvocationDegradesWithoutHandoff(t *testing.T) {
	plain, err := executor.Goal(fixPRSecureRun("handoff-degraded"))
	if err != nil {
		t.Fatalf("render ordinary fix-pr goal: %v", err)
	}
	for name, resolver := range map[string]*recordingHandoffResolver{
		"not found":  {},
		"read error": {err: errors.New("forge read failed")},
	} {
		t.Run(name, func(t *testing.T) {
			control, c := secureControl(t)
			control.ReviewHandoffs = resolver
			control.Config.ReviewerIdentity = "pr-reviewer"
			run := fixPRSecureRun("handoff-" + name)
			if err := c.Create(context.Background(), run); err != nil {
				t.Fatal(err)
			}
			invocation, err := control.controlInvocation(context.Background(), run)
			if err != nil {
				t.Fatalf("controlInvocation must survive a missing handoff: %v", err)
			}
			if invocation.Goal != plain {
				t.Fatalf("degraded goal drifted from the ordinary goal:\n got %q\nwant %q", invocation.Goal, plain)
			}
		})
	}
}

func TestControlInvocationStaysDarkWithoutIdentityOrResolver(t *testing.T) {
	plain, err := executor.Goal(fixPRSecureRun("handoff-dark"))
	if err != nil {
		t.Fatalf("render ordinary fix-pr goal: %v", err)
	}
	control, c := secureControl(t)
	resolver := &recordingHandoffResolver{handoff: handoffTestPayload, found: true}
	control.ReviewHandoffs = resolver
	run := fixPRSecureRun("handoff-no-identity")
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	invocation, err := control.controlInvocation(context.Background(), run)
	if err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver consulted %d times without a configured reviewer identity", resolver.calls)
	}
	if invocation.Goal != plain {
		t.Fatalf("unset identity changed the goal:\n got %q\nwant %q", invocation.Goal, plain)
	}

	// A configured identity with no wired resolver must not panic and must
	// not change the goal.
	control2, _ := secureControl(t)
	control2.Config.ReviewerIdentity = "pr-reviewer"
	invocation2, err := control2.controlInvocation(context.Background(), run)
	if err != nil {
		t.Fatalf("controlInvocation without a resolver: %v", err)
	}
	if invocation2.Goal != plain {
		t.Fatalf("unwired resolver changed the goal:\n got %q\nwant %q", invocation2.Goal, plain)
	}
}

func TestControlInvocationNeverReadsHandoffOutsideFixPR(t *testing.T) {
	control, c := secureControl(t)
	resolver := &recordingHandoffResolver{handoff: handoffTestPayload, found: true}
	control.ReviewHandoffs = resolver
	control.Config.ReviewerIdentity = "pr-reviewer"
	run := secureRun("handoff-resolve-issue")
	run.Status.PublicationPolicy = &courier.PublicationPolicy{RunUID: string(run.UID)}
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := control.controlInvocation(context.Background(), run); err != nil {
		t.Fatalf("controlInvocation: %v", err)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver consulted for %s mode", run.Spec.Mode)
	}
}
