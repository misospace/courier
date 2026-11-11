package main

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/git"
)

func TestApplyOutcomeGateGoalScratchPath(t *testing.T) {
	goal := "declare the ending in /var/tmp/courier-scratch/outcome.json when done"
	got := applyOutcomeGateGoal(goal, nil, "/run/scratch/outcome.json")
	want := "declare the ending in /run/scratch/outcome.json when done"
	if got != want {
		t.Fatalf("applyOutcomeGateGoal() = %q, want %q", got, want)
	}
}

func TestApplyOutcomeGateGoalLeavesOtherTextUntouched(t *testing.T) {
	goal := "declare the ending in /other/place/outcome.json when done"
	got := applyOutcomeGateGoal(goal, nil, "/run/scratch/outcome.json")
	if got != goal {
		t.Fatalf("applyOutcomeGateGoal() = %q, want the goal unchanged", got)
	}
}

func TestApplyOutcomeGateGoalConflictNote(t *testing.T) {
	conflict := &git.MergeConflictError{Base: "main", Paths: []string{"a.go", "b.go"}}
	goal := "ship the fix"
	got := applyOutcomeGateGoal(goal, conflict, "/run/scratch/outcome.json")
	if !strings.HasPrefix(got, "ship the fix\n\n") {
		t.Fatalf("applyOutcomeGateGoal() must append the conflict note after the goal: %q", got)
	}
	note := strings.TrimPrefix(got, "ship the fix\n\n")
	if note != conflictNote(conflict) {
		t.Fatalf("appended note = %q, want conflictNote() output %q", note, conflictNote(conflict))
	}
	for _, frag := range []string{"mid-merge", "main", "a.go, b.go", "never reset, rebase, force-push, or discard"} {
		if !strings.Contains(note, frag) {
			t.Errorf("note %q missing fragment %q", note, frag)
		}
	}
}

func TestApplyOutcomeGateGoalScratchPathRewrittenBeforeConflictNote(t *testing.T) {
	conflict := &git.MergeConflictError{Base: "main", Paths: []string{"a.go"}}
	goal := "declare in /var/tmp/courier-scratch/outcome.json"
	got := applyOutcomeGateGoal(goal, conflict, "/run/scratch/outcome.json")
	if !strings.HasPrefix(got, "declare in /run/scratch/outcome.json\n\n") {
		t.Fatalf("goal half must be rewritten and precede the conflict note: %q", got)
	}
	if strings.Contains(got, "/var/tmp/courier-scratch/outcome.json") {
		t.Fatalf("default scratch path survived the rewrite: %q", got)
	}
}

func TestRunStartupGuardsApplyInOrder(t *testing.T) {
	var calls []string
	mk := func(name string, code int, stop bool) launchGuard {
		return func(context.Context) (int, bool) {
			calls = append(calls, name)
			return code, stop
		}
	}
	pass := []launchGuard{mk("adoption", 0, false), mk("head", 0, false), mk("base", 0, false)}
	if code, stop := runStartupGuards(context.Background(), pass); stop || code != 0 {
		t.Fatalf("runStartupGuards() = (%d, %v), want (0, false) when every guard passes", code, stop)
	}
	if want := []string{"adoption", "head", "base"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("guard order = %v, want %v", calls, want)
	}
}

func TestRunStartupGuardsShortCircuitOnFirstTerminal(t *testing.T) {
	var calls []string
	mk := func(name string, code int, stop bool) launchGuard {
		return func(context.Context) (int, bool) {
			calls = append(calls, name)
			return code, stop
		}
	}
	guards := []launchGuard{mk("adoption", 0, false), mk("head", exitNeedsHuman, true), mk("base", 1, true)}
	if code, stop := runStartupGuards(context.Background(), guards); !stop || code != exitNeedsHuman {
		t.Fatalf("runStartupGuards() = (%d, %v), want (%d, true) on the first terminal verdict", code, stop, exitNeedsHuman)
	}
	if want := []string{"adoption", "head"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("guards after the first terminal verdict ran: %v, want %v", calls, want)
	}
}

func TestOrderedLaunchGuardsPassForANeutralRun(t *testing.T) {
	report := newReporter(io.Discard, io.Discard, config{RunID: "run-1", Repo: "o/r"})
	guards := orderedLaunchGuards(report)
	if len(guards) != 3 {
		t.Fatalf("orderedLaunchGuards() returned %d guards, want 3 (adoption, fix-pr head, fix-pr base)", len(guards))
	}
	// A run in neither resolve-issue nor a PR-repair mode must fall through
	// every guard without emitting a handoff: adoption only decides for
	// resolve-issue, the head and base checks only for fix-pr and rebase.
	if code, stop := runStartupGuards(context.Background(), guards); stop || code != 0 {
		t.Fatalf("runStartupGuards() = (%d, %v), want (0, false) for a neutral mode", code, stop)
	}
}
