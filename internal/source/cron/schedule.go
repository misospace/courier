package cron

import (
	"fmt"
	"time"

	"github.com/hashicorp/cronexpr"
)

// cronSchedule wraps one compiled cron expression and the location it is
// evaluated in. Boundaries are computed in the configured location, so
// wall-clock expressions follow local time across DST transitions.
type cronSchedule struct {
	expr *cronexpr.Expression
	loc  *time.Location
}

// compile parses a standard 5-field cron expression and resolves its
// evaluation location. An empty timezone means UTC.
func compile(spec, timezone string) (cronSchedule, error) {
	loc := time.UTC
	if timezone != "" {
		loaded, err := time.LoadLocation(timezone)
		if err != nil {
			return cronSchedule{}, fmt.Errorf("invalid timezone %q: %w", timezone, err)
		}
		loc = loaded
	}
	expr, err := cronexpr.Parse(spec)
	if err != nil {
		return cronSchedule{}, fmt.Errorf("invalid schedule %q: %w", spec, err)
	}
	return cronSchedule{expr: expr, loc: loc}, nil
}

// next returns the first boundary strictly after after, expressed in the
// schedule's location.
func (s cronSchedule) next(after time.Time) time.Time {
	return s.expr.Next(after.In(s.loc))
}
