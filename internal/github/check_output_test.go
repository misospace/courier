package github

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/misospace/courier/internal/controller"
	"github.com/misospace/courier/internal/forge"
)

// TestObserveCarriesBoundedCheckOutput pins that a check run's published
// output reaches the observation as bounded log evidence: the summary and the
// text are carried, the evidence is cut at the per-check bound, and a cut is
// marked so a reader can tell a short log from a cut one.
func TestObserveCarriesBoundedCheckOutput(t *testing.T) {
	long := strings.Repeat("failure detail ", 20000)
	client, _ := observerServer(t,
		func(string) string { return "[" + verifiedPull(7, "open", "work", "sha-7") + "]" },
		fmt.Sprintf(`{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"failure","output":{"title":"unit failed","summary":"one assertion failed","text":%q}}]}`, long),
		`{"total_count":0,"statuses":[]}`)

	observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(observation.Checks) != 1 {
		t.Fatalf("checks = %#v, want the one check run", observation.Checks)
	}
	output := observation.Checks[0].Output
	if output == "" {
		t.Fatalf("observation check carries no log evidence")
	}
	if !strings.Contains(output, "one assertion failed") {
		t.Fatalf("observation output %q must carry the check's summary", output)
	}
	if len(output) > checkOutputMaxBytes {
		t.Fatalf("observation output is %d bytes, want at most %d", len(output), checkOutputMaxBytes)
	}
	if !strings.Contains(output, checkOutputTruncated) {
		t.Fatalf("observation output %q must mark the cut evidence", output)
	}
}

// TestObserveWithoutCheckOutputYieldsNoEvidence pins that a check run without
// published output — or with an output object that carries nothing — leaves
// the observation evidence-free: an absent log is not a placeholder.
func TestObserveWithoutCheckOutputYieldsNoEvidence(t *testing.T) {
	for name, checks := range map[string]string{
		"no output object":    `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"failure"}]}`,
		"empty output object": `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"failure","output":{}}]}`,
		"blank output":        `{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"failure","output":{"title":"  ","summary":"","text":"\n"}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := observerServer(t,
				func(string) string { return "[" + verifiedPull(7, "open", "work", "sha-7") + "]" },
				checks,
				`{"total_count":0,"statuses":[]}`)

			observation, err := Observer{Client: client}.Observe(context.Background(), "acme/demo", controller.HeadRef{Branch: "work", SHA: "sha-head"})
			if err != nil {
				t.Fatalf("Observe() error = %v", err)
			}
			if len(observation.Checks) != 1 {
				t.Fatalf("checks = %#v, want the one check run", observation.Checks)
			}
			if observation.Checks[0].Output != "" {
				t.Fatalf("observation output = %q, want no evidence", observation.Checks[0].Output)
			}
		})
	}
}

// TestReadChecksCarriesTheSameBoundedEvidence pins that the forge capability
// surface carries the same bounded evidence the observer reports: the
// capability read and the observation are two renderings of one check run and
// must not drift.
func TestReadChecksCarriesTheSameBoundedEvidence(t *testing.T) {
	client, _ := observerServer(t,
		func(string) string { return "[]" },
		`{"total_count":1,"check_runs":[{"name":"unit","status":"completed","conclusion":"failure","output":{"summary":"one assertion failed","text":"details"}}]}`,
		`{"total_count":0,"statuses":[]}`)

	provider := NewProvider(forge.ProviderConfig{Name: "gh", Endpoint: "https://api.github.com/"}, client)
	checks, err := provider.ReadChecks(context.Background(), forge.PullRequestRef{Repo: "acme/demo", Number: 7}, "sha-7")
	if err != nil {
		t.Fatalf("ReadChecks() error = %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("checks = %#v, want the one check run", checks)
	}
	if !strings.Contains(checks[0].Output, "one assertion failed") {
		t.Fatalf("forge check output %q must carry the check's summary", checks[0].Output)
	}
	if len(checks[0].Output) > checkOutputMaxBytes {
		t.Fatalf("forge check output is %d bytes, want at most %d", len(checks[0].Output), checkOutputMaxBytes)
	}
}
