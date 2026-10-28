package cron

import (
	"fmt"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

// Mode values a schedule may declare. They mirror the CoderRun modes the
// source runner accepts; resolve-issue is the default for scheduled work.
const (
	ModeResolveIssue = "resolve-issue"
	ModeFixPR        = "fix-pr"
)

// Schedule is one declared scheduled job in the cron-schedules config file.
// All fields are validated by Parse; the compiled form is carried on Job.
type Schedule struct {
	// Name identifies the job. It is part of the occurrence identity, so it
	// must be stable across restarts: renaming a job orphans its history.
	Name string `json:"name"`
	// Repo is the target repository in owner/name form.
	Repo string `json:"repo"`
	// Issue is the issue number the scheduled run addresses; it becomes the
	// WorkItem Ref.
	Issue int `json:"issue"`
	// Lane names the Courier LaneProfile assigned to materialized runs.
	Lane string `json:"lane"`
	// Schedule is a standard 5-field cron expression evaluated in Location.
	Schedule string `json:"schedule"`
	// Timezone is an optional IANA location name; empty means UTC.
	Timezone string `json:"timezone,omitempty"`
	// Mode is the CoderRun mode; empty means resolve-issue.
	Mode string `json:"mode,omitempty"`
	// Debug marks materialized runs debug.
	Debug bool `json:"debug,omitempty"`
}

// Parse reads a cron-schedules config document: a top-level schedules list
// of Schedule entries. Unknown fields are rejected. Every field is
// validated here so a malformed deployment fails startup clearly, naming
// the entry and field, instead of failing per poll at discovery time.
func Parse(data []byte) ([]Schedule, error) {
	var document struct {
		Schedules []Schedule `json:"schedules"`
	}
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		return nil, fmt.Errorf("parse cron schedules config: %w", err)
	}
	schedules := make([]Schedule, 0, len(document.Schedules))
	seen := make(map[string]int, len(document.Schedules))
	for i, schedule := range document.Schedules {
		parsed, err := parseSchedule(schedule, i)
		if err != nil {
			return nil, err
		}
		if prior, ok := seen[parsed.Name]; ok {
			return nil, fmt.Errorf("cron schedule %d: duplicate name %q (already declared at index %d)", i, parsed.Name, prior)
		}
		seen[parsed.Name] = i
		schedules = append(schedules, parsed)
	}
	return schedules, nil
}

func parseSchedule(schedule Schedule, index int) (Schedule, error) {
	label := fmt.Sprintf("cron schedule %d", index)
	name := strings.TrimSpace(schedule.Name)
	if name == "" {
		return Schedule{}, fmt.Errorf("%s: name is required", label)
	}
	if labelErrs := utilvalidation.IsDNS1123Label(name); len(labelErrs) > 0 {
		return Schedule{}, fmt.Errorf("%s %q: invalid name: %s", label, name, strings.Join(labelErrs, "; "))
	}
	label = fmt.Sprintf("cron schedule %q", name)
	repo := strings.TrimSpace(schedule.Repo)
	owner, project, found := strings.Cut(repo, "/")
	if !found || owner == "" || project == "" || strings.Contains(project, "/") {
		return Schedule{}, fmt.Errorf("%s: repo must be owner/name, got %q", label, schedule.Repo)
	}
	if schedule.Issue < 1 {
		return Schedule{}, fmt.Errorf("%s: issue must be a positive issue number", label)
	}
	lane := strings.TrimSpace(schedule.Lane)
	if lane == "" {
		return Schedule{}, fmt.Errorf("%s: lane is required", label)
	}
	if strings.TrimSpace(schedule.Schedule) == "" {
		return Schedule{}, fmt.Errorf("%s: schedule is required", label)
	}
	if _, err := compile(schedule.Schedule, schedule.Timezone); err != nil {
		return Schedule{}, fmt.Errorf("%s: %w", label, err)
	}
	mode := strings.TrimSpace(schedule.Mode)
	if mode == "" {
		mode = ModeResolveIssue
	}
	if mode != ModeResolveIssue && mode != ModeFixPR {
		return Schedule{}, fmt.Errorf("%s: mode must be %q or %q, got %q", label, ModeResolveIssue, ModeFixPR, mode)
	}
	return Schedule{
		Name:     name,
		Repo:     repo,
		Issue:    schedule.Issue,
		Lane:     lane,
		Schedule: strings.TrimSpace(schedule.Schedule),
		Timezone: strings.TrimSpace(schedule.Timezone),
		Mode:     mode,
		Debug:    schedule.Debug,
	}, nil
}

// job is the compiled form of one Schedule carried by the adapter.
type job struct {
	name     string
	repo     string
	issue    int
	lane     string
	mode     string
	debug    bool
	schedule cronSchedule
}

func compileJob(schedule Schedule) (job, error) {
	compiled, err := compile(schedule.Schedule, schedule.Timezone)
	if err != nil {
		return job{}, err
	}
	return job{
		name:     schedule.Name,
		repo:     schedule.Repo,
		issue:    schedule.Issue,
		lane:     schedule.Lane,
		mode:     schedule.Mode,
		debug:    schedule.Debug,
		schedule: compiled,
	}, nil
}

// occurrenceID is the deterministic identity of one scheduled occurrence:
// the job name and the unix timestamp of the scheduled boundary. Repeated
// polls and manager restarts derive the same identity for the same
// occurrence, while future occurrences remain distinct.
func (j job) occurrenceID(boundary time.Time) string {
	return j.name + "@" + fmt.Sprintf("%d", boundary.Unix())
}
