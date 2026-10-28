package cron

import (
	"fmt"
	"strings"
	"time"

	// The manager image may not carry the OS tzdata package; embedding the
	// database keeps every declared IANA timezone resolvable in every image.
	_ "time/tzdata"

	"github.com/hashicorp/cronexpr"
)

// cronSchedule wraps one compiled cron expression and the location it is
// evaluated in. Boundaries are computed in the configured location, so
// wall-clock expressions follow local time across DST transitions.
type cronSchedule struct {
	expr *cronexpr.Expression
	loc  *time.Location
}

// compile parses a standard five-field cron expression and resolves its
// evaluation location. An empty timezone means UTC. Wider cronexpr syntax
// (six or seven fields with an explicit year, or @descriptor forms) is
// rejected: a year-pinned expression exhausts its match list and Next
// starts returning the zero time, which a recurring cursor must never see.
// The returned schedule is also proven satisfiable: a five-field expression
// that can never match (for example "0 0 30 2 *") would otherwise compile,
// start, and silently never fire.
func compile(spec, timezone string) (cronSchedule, error) {
	loc := time.UTC
	if timezone != "" {
		loaded, err := time.LoadLocation(timezone)
		if err != nil {
			return cronSchedule{}, fmt.Errorf("invalid timezone %q: %w", timezone, err)
		}
		loc = loaded
	}
	if fields := strings.Fields(spec); len(fields) != 5 {
		return cronSchedule{}, fmt.Errorf("invalid schedule %q: want exactly five fields (minute hour day-of-month month day-of-week)", spec)
	}
	expr, err := cronexpr.Parse(spec)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("invalid schedule %q: %w", spec, err)
	}
	if expr.Next(time.Now()).IsZero() {
		return cronSchedule{}, fmt.Errorf("invalid schedule %q: can never fire", spec)
	}
	return cronSchedule{expr: expr, loc: loc}, nil
}

// next returns the first boundary strictly after after, expressed in the
// schedule's location. A zero return means the expression can match no
// further instant; callers must treat that as schedule exhaustion, never as
// a boundary.
func (s cronSchedule) next(after time.Time) time.Time {
	return s.expr.Next(after.In(s.loc))
}
