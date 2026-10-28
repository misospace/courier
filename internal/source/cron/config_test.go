package cron

import (
	"strings"
	"testing"
)

func mustParse(t *testing.T, data string) []Schedule {
	t.Helper()
	schedules, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return schedules
}

func TestParseValidConfig(t *testing.T) {
	schedules := mustParse(t, `
schedules:
  - name: nightly-triage
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "30 2 * * *"
    timezone: America/New_York
  - name: weekly-sweep
    repo: misospace/courier
    issue: 99
    lane: local
    schedule: "0 4 * * 1"
    mode: fix-pr
    debug: true
`)
	if len(schedules) != 2 {
		t.Fatalf("len(schedules) = %d, want 2", len(schedules))
	}
	first := schedules[0]
	if first.Name != "nightly-triage" || first.Repo != "misospace/courier" || first.Issue != 15 ||
		first.Lane != "frontier" || first.Schedule != "30 2 * * *" || first.Timezone != "America/New_York" {
		t.Fatalf("first schedule = %+v", first)
	}
	if first.Mode != ModeResolveIssue {
		t.Fatalf("mode default = %q, want %q", first.Mode, ModeResolveIssue)
	}
	second := schedules[1]
	if second.Mode != ModeFixPR || !second.Debug || second.Timezone != "" {
		t.Fatalf("second schedule = %+v", second)
	}
}

func TestParseRejectsUnknownField(t *testing.T) {
	if _, err := Parse([]byte(`
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 1
    lane: frontier
    schedule: "* * * * *"
    cron: "* * * * *"
`)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v, want unknown-field rejection", err)
	}
}

func TestParseRejectsInvalidEntries(t *testing.T) {
	valid := func(name string) string {
		return `
schedules:
  - name: ` + name + `
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "* * * * *"
`
	}
	cases := []struct {
		name    string
		data    string
		wantErr string
	}{
		{"missing name", `
schedules:
  - repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "* * * * *"
`, "name is required"},
		{"invalid name", valid(`"not a label!"`), "invalid name"},
		{"missing repo", `
schedules:
  - name: nightly
    issue: 15
    lane: frontier
    schedule: "* * * * *"
`, "repo"},
		{"bad repo form", `
schedules:
  - name: nightly
    repo: misospace/courier/extra
    issue: 15
    lane: frontier
    schedule: "* * * * *"
`, "repo must be owner/name"},
		{"zero issue", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 0
    lane: frontier
    schedule: "* * * * *"
`, "issue"},
		{"negative issue", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: -3
    lane: frontier
    schedule: "* * * * *"
`, "issue"},
		{"missing lane", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    schedule: "* * * * *"
`, "lane is required"},
		{"missing schedule", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    lane: frontier
`, "schedule is required"},
		{"invalid cron expression", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "not a schedule"
`, "invalid schedule"},
		{"invalid timezone", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "* * * * *"
    timezone: Not/AZone
`, "invalid timezone"},
		{"invalid mode", `
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "* * * * *"
    mode: scheduled
`, `mode must be "resolve-issue" or "fix-pr"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.data))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestParseRejectsDuplicateNames(t *testing.T) {
	_, err := Parse([]byte(`
schedules:
  - name: nightly
    repo: misospace/courier
    issue: 15
    lane: frontier
    schedule: "* * * * *"
  - name: nightly
    repo: misospace/courier
    issue: 16
    lane: frontier
    schedule: "* * * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("duplicate name error = %v, want duplicate-name rejection", err)
	}
}

func TestParseEmptyDocument(t *testing.T) {
	schedules, err := Parse([]byte("schedules: []\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(schedules) != 0 {
		t.Fatalf("len(schedules) = %d, want 0", len(schedules))
	}
}
