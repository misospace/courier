package executor

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// contributeEnv is a minimal contribute-mode run-context environment: a run
// with no source-owned work item, so its ref is exactly zero.
func contributeEnv() map[string]string {
	return map[string]string{
		"COURIER_RUN_NAME":      "run-contribute",
		"COURIER_RUN_NAMESPACE": "ns",
		"COURIER_MODE":          string(courierv1alpha1.ModeContribute),
		"COURIER_REF":           "0",
		"COURIER_REPO":          "acme/app",
		"COURIER_GOAL":          "g",
		"COURIER_MODEL":         "model-a",
		"COURIER_WORKSPACE":     "/work",
	}
}

// TestContributionEnvRoundTrip pins that a contribute-mode invocation carries
// its source-decoded input through the run-context environment intact: the
// ref is zero, the mode round-trips, and the verbatim goal built from the
// contribution input survives the encode/decode cycle.
func TestContributionEnvRoundTrip(t *testing.T) {
	run := &courierv1alpha1.CoderRun{
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:  courierv1alpha1.ModeContribute,
			Repo:  "acme/app",
			Ref:   0,
			Debug: true,
		},
		Status: courierv1alpha1.CoderRunStatus{
			Branch:   "feature/x",
			HeadRepo: "acme/app-fork",
			HeadSHA:  "abc123",
		},
	}
	run.Name = "run-contribute"
	run.Namespace = "ns"
	lane := &courierv1alpha1.LaneProfile{
		Spec: courierv1alpha1.LaneProfileSpec{
			Framing: "be concise",
			Roles:   map[string]string{"coordinator": "model-a", "worker": "model-b"},
		},
	}
	contribution := ContributionExecution{Goal: "Add a retry helper", BaseRef: "main"}
	inv, err := NewInvocationWithContribution(run, lane, "/work", contribution)
	if err != nil {
		t.Fatalf("NewInvocationWithContribution error = %v", err)
	}
	if inv.Ref != 0 {
		t.Fatalf("contribute invocation Ref = %d, want 0", inv.Ref)
	}
	if !strings.Contains(inv.Goal, `"Add a retry helper"`) {
		t.Fatalf("contribute goal does not carry the objective:\n%q", inv.Goal)
	}
	values := map[string]string{}
	for _, env := range RunContextEnvironment(inv) {
		values[env.Name] = env.Value
	}
	dec, err := InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatalf("InvocationFromEnv error = %v", err)
	}
	if !reflect.DeepEqual(inv, dec) {
		t.Fatalf("decoded = %+v, want %+v", dec, inv)
	}
}

// TestContributionEnvRejectsPositiveRef pins that a contribute-mode
// environment carrying a positive ref fails closed at decode time: a
// contribution never references an issue or PR number.
func TestContributionEnvRejectsPositiveRef(t *testing.T) {
	env := contributeEnv()
	env["COURIER_REF"] = "1"
	if _, err := InvocationFromEnv(func(k string) string { return env[k] }); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("InvocationFromEnv(contribute, ref=1) error = %v, want ErrInvalidReference", err)
	}
}

// TestEnvRejectsResolveIssueRefZero pins the inverse: a resolve-issue
// environment whose ref is zero fails closed — non-contribute modes require a
// positive work-item reference.
func TestEnvRejectsResolveIssueRefZero(t *testing.T) {
	env := contributeEnv()
	env["COURIER_MODE"] = string(courierv1alpha1.ModeResolveIssue)
	env["COURIER_REF"] = "0"
	if _, err := InvocationFromEnv(func(k string) string { return env[k] }); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("InvocationFromEnv(resolve-issue, ref=0) error = %v, want ErrInvalidReference", err)
	}
}
