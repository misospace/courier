package cron

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/source"
)

func utc(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

func hourly(name string) Schedule {
	return Schedule{
		Name:     name,
		Repo:     "misospace/courier",
		Issue:    15,
		Lane:     "frontier",
		Schedule: "0 * * * *",
		Mode:     ModeResolveIssue,
	}
}

func ids(items []source.WorkItem) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.ID)
	}
	return out
}

func discover(t *testing.T, a *Adapter, at string) []source.WorkItem {
	t.Helper()
	items, err := a.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover at %s: %v", at, err)
	}
	return items
}

// TestOccurrenceIdentityIsTheScheduledBoundary proves the occurrence
// identity derives from the scheduled boundary, never the poll instant:
// repeated polls within one boundary window deduplicate to nothing, and a
// later boundary carries a distinct identity.
func TestOccurrenceIdentityIsTheScheduledBoundary(t *testing.T) {
	now := utc(t, "2026-01-05T10:00:00Z")
	current := now
	a := New([]Schedule{hourly("triage")}, nil, WithNow(func() time.Time { return current }))

	items := discover(t, a, "10:00")
	if len(items) != 0 {
		t.Fatalf("first poll offered %v, want nothing before the first boundary", ids(items))
	}
	current = utc(t, "2026-01-05T12:00:30Z")
	items = discover(t, a, "12:00:30")
	if len(items) != 1 {
		t.Fatalf("boundary poll offered %d items, want 1", len(items))
	}
	want := utc(t, "2026-01-05T12:00:00Z")
	if items[0].ID != fmt.Sprintf("triage@%d", want.Unix()) {
		t.Fatalf("occurrence ID = %q, want job@boundary-epoch %d", items[0].ID, want.Unix())
	}
	current = utc(t, "2026-01-05T12:59:00Z")
	if got := discover(t, a, "12:59"); len(got) != 0 {
		t.Fatalf("repeat poll within the same boundary offered %v, want nothing", ids(got))
	}
	current = utc(t, "2026-01-05T13:05:00Z")
	items = discover(t, a, "13:05")
	if len(items) != 1 {
		t.Fatalf("poll past one boundary offered %d items, want 1", len(items))
	}
	wantHour := utc(t, "2026-01-05T13:00:00Z")
	if items[0].ID != fmt.Sprintf("triage@%d", wantHour.Unix()) {
		t.Fatalf("occurrence ID = %q, want the next hourly boundary epoch %d", items[0].ID, wantHour.Unix())
	}
	current = utc(t, "2026-01-06T12:00:01Z")
	items = discover(t, a, "next day")
	if len(items) != 1 {
		t.Fatalf("next-day poll offered %d items, want 1", len(items))
	}
	wantNext := utc(t, "2026-01-06T12:00:00Z")
	if items[0].ID != fmt.Sprintf("triage@%d", wantNext.Unix()) {
		t.Fatalf("next occurrence ID = %q, want distinct boundary epoch %d", items[0].ID, wantNext.Unix())
	}
}

// TestWorkItemCarriesRunIdentity proves the offered WorkItem carries the
// run identity the source-neutral path needs and survives Spec
// materialization unchanged.
func TestWorkItemCarriesRunIdentity(t *testing.T) {
	now := utc(t, "2026-01-05T10:00:00Z")
	current := now
	a := New([]Schedule{{
		Name:     "triage",
		Repo:     "misospace/courier",
		Issue:    15,
		Lane:     "frontier",
		Schedule: "0 * * * *",
		Mode:     ModeFixPR,
		Debug:    true,
	}}, nil, WithNow(func() time.Time { return current }))

	current = utc(t, "2026-01-05T11:00:01Z")
	items := discover(t, a, "11:00:01")
	if len(items) != 1 {
		t.Fatalf("offered %d items, want 1", len(items))
	}
	item := items[0]
	if item.Mode != ModeFixPR || item.Repo != "misospace/courier" || item.Ref != 15 ||
		item.Lane != "frontier" || !item.Debug {
		t.Fatalf("work item = %+v", item)
	}
	if item.Context == "" {
		t.Fatal("work item context is empty; want a provenance note")
	}
	spec := item.Spec("cron")
	if spec.Source != "cron" || spec.WorkItemID != item.ID || spec.Mode != item.Mode ||
		spec.Repo != item.Repo || spec.Ref != item.Ref || spec.Lane != item.Lane ||
		spec.Debug != item.Debug {
		t.Fatalf("run spec = %+v, want a faithful copy of %+v", spec, item)
	}
}

// TestRestartSkipsMissedOccurrences proves the settled missed-run policy: a
// fresh adapter (a restarted manager) never offers boundaries that passed
// before its construction, and its first due occurrence is the first
// boundary strictly after startup.
func TestRestartSkipsMissedOccurrences(t *testing.T) {
	before := utc(t, "2026-01-05T10:00:00Z")
	current := before
	first := New([]Schedule{hourly("triage")}, nil, WithNow(func() time.Time { return current }))
	current = utc(t, "2026-01-05T11:00:01Z")
	if got := discover(t, first, "11:00:01"); len(got) != 1 {
		t.Fatalf("first process offered %v, want the 11:00 occurrence", ids(got))
	}

	// A manager restart at 14:30 restarts the cursor: occurrences 12:00,
	// 13:00, and 14:00 that passed while the process was down are skipped,
	// and the next offered boundary is 15:00.
	current = utc(t, "2026-01-05T14:30:00Z")
	restarted := New([]Schedule{hourly("triage")}, nil, WithNow(func() time.Time { return current }))
	if got := discover(t, restarted, "14:30"); len(got) != 0 {
		t.Fatalf("restarted process offered %v, want nothing until the next boundary", ids(got))
	}
	current = utc(t, "2026-01-05T15:00:01Z")
	got := discover(t, restarted, "15:00:01")
	if len(got) != 1 {
		t.Fatalf("restarted process offered %d items at 15:00:01, want 1", len(got))
	}
	want := utc(t, "2026-01-05T15:00:00Z")
	if got[0].ID != fmt.Sprintf("triage@%d", want.Unix()) {
		t.Fatalf("restarted occurrence ID = %q, want %q", got[0].ID, fmt.Sprintf("triage@%d", want.Unix()))
	}
}

// TestSingleGapOffersOnlyLatestOccurrence proves the bounded-miss shape of
// the missed-run policy: when one poll gap spans several boundaries, only
// the latest due occurrence is offered and the earlier ones stay skipped —
// no catch-up burst.
func TestSingleGapOffersOnlyLatestOccurrence(t *testing.T) {
	now := utc(t, "2026-01-05T10:00:00Z")
	current := now
	a := New([]Schedule{hourly("triage")}, nil, WithNow(func() time.Time { return current }))

	current = utc(t, "2026-01-05T14:30:00Z")
	items := discover(t, a, "14:30")
	if len(items) != 1 {
		t.Fatalf("offered %d items, want 1", len(items))
	}
	want := utc(t, "2026-01-05T14:00:00Z")
	if items[0].ID != fmt.Sprintf("triage@%d", want.Unix()) {
		t.Fatalf("offered ID = %q, want the latest boundary %q", items[0].ID, fmt.Sprintf("triage@%d", want.Unix()))
	}
	current = utc(t, "2026-01-05T14:40:00Z")
	if again := discover(t, a, "14:40"); len(again) != 0 {
		t.Fatalf("skipped boundaries resurfaced: %v", ids(again))
	}
}

// TestConsecutiveOccurrencesRemainDistinct proves the settled overlap
// policy: the adapter offers every due occurrence regardless of any
// still-active prior run — dedup is per occurrence identity only, and the
// lane's concurrency limit serializes execution.
func TestConsecutiveOccurrencesRemainDistinct(t *testing.T) {
	now := utc(t, "2026-01-05T10:00:00Z")
	current := now
	a := New([]Schedule{hourly("triage")}, nil, WithNow(func() time.Time { return current }))

	current = utc(t, "2026-01-05T11:00:01Z")
	first := discover(t, a, "11:00:01")
	current = utc(t, "2026-01-05T12:00:01Z")
	second := discover(t, a, "12:00:01")
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("offered %d then %d items, want 1 then 1", len(first), len(second))
	}
	if first[0].ID == second[0].ID {
		t.Fatalf("consecutive occurrences shared identity %q", first[0].ID)
	}
}

// TestTimezoneSemantics proves boundaries are computed in the configured
// location: a 02:00 America/New_York schedule lands at 07:00Z on ordinary
// winter days. On the 2026 spring-forward day (March 8) the 02:00 local
// time does not exist; cronexpr skips it and the next boundary is 02:00 EDT
// on March 9 — the nonexistent occurrence produces no run rather than a
// shifted one, which is pinned here so a dependency upgrade cannot change
// it silently.
func TestTimezoneSemantics(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	schedule := Schedule{
		Name:     "nightly",
		Repo:     "misospace/courier",
		Issue:    15,
		Lane:     "frontier",
		Schedule: "0 2 * * *",
		Timezone: "America/New_York",
	}
	current := utc(t, "2026-03-07T00:00:00Z")
	a := New([]Schedule{schedule}, nil, WithNow(func() time.Time { return current }))
	items := discover(t, a, "construction-time")
	if len(items) != 0 {
		t.Fatalf("construction-time poll offered %v, want nothing", ids(items))
	}
	current = utc(t, "2026-03-07T07:00:01Z")
	items = discover(t, a, "EST morning")
	if len(items) != 1 {
		t.Fatalf("offered %d items, want 1", len(items))
	}
	want := utc(t, "2026-03-07T07:00:00Z")
	if items[0].ID != fmt.Sprintf("nightly@%d", want.Unix()) {
		t.Fatalf("EST boundary ID = %q, want epoch %d (07:00Z)", items[0].ID, want.Unix())
	}

	// Spring-forward day: 02:00 EST does not exist; cronexpr skips the
	// nonexistent local time entirely, so nothing is due on March 8 and the
	// next boundary is 02:00 EDT on March 9 (06:00Z).
	current = utc(t, "2026-03-08T07:00:01Z")
	items = discover(t, a, "spring-forward morning")
	if len(items) != 0 {
		t.Fatalf("spring-forward day offered %v, want the nonexistent 02:00 occurrence skipped", ids(items))
	}
	current = utc(t, "2026-03-09T06:00:01Z")
	items = discover(t, a, "after spring-forward")
	if len(items) != 1 {
		t.Fatalf("offered %d items after spring-forward, want 1", len(items))
	}
	wantSpring := utc(t, "2026-03-09T06:00:00Z")
	if items[0].ID != fmt.Sprintf("nightly@%d", wantSpring.Unix()) {
		t.Fatalf("post-spring-forward boundary ID = %q, want the 02:00 EDT epoch %d", items[0].ID, wantSpring.Unix())
	}
}

// findSeries returns the series value matching the label set, mirroring the
// manual adapter's test helper.
func findSeries(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
					break
				}
			}
			if match {
				switch {
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue(), true
				case m.GetGauge() != nil:
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestConnectorNameOverride pins the multi-lane connector labeling.
func TestConnectorNameOverride(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	_ = New([]Schedule{hourly("triage")}, em, WithConnectorName("cron:frontier"))
	if got, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
		"connector": "cron:frontier", "capability": connector.CapabilityDiscovery, "state": connector.CapabilitySupported,
	}); !ok || got != 1 {
		t.Fatalf("overridden connector capability series = %v, ok=%v; want 1", got, ok)
	}
}

// TestCronCapabilityCatalogue pins the cron adapter's declared capabilities:
// only discovery and availability are supported and every other capability
// is explicitly unsupported, so a dashboard renders the omission instead of
// a fabricated panel.
func TestCronCapabilityCatalogue(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	_ = New([]Schedule{hourly("triage")}, em)

	states := map[string]string{
		connector.CapabilityDiscovery:             connector.CapabilitySupported,
		connector.CapabilityDiscoveryExclusions:   connector.CapabilityUnsupported,
		connector.CapabilityLifecycleReport:       connector.CapabilityUnsupported,
		connector.CapabilityPreLaunchRevalidation: connector.CapabilityUnsupported,
		connector.CapabilityCIParkLifecycle:       connector.CapabilityUnsupported,
		connector.CapabilityWorkIdentity:          connector.CapabilityUnsupported,
		connector.CapabilityAvailability:          connector.CapabilitySupported,
	}
	for capability, state := range states {
		got, ok := findSeries(t, reg, connector.MetricCapabilityInfo, map[string]string{
			"connector": connectorName, "capability": capability, "state": state,
		})
		if !ok || got != 1 {
			t.Errorf("capability %q state %q = %v, ok=%v; want 1", capability, state, got, ok)
		}
	}
}

// TestAvailabilityObservations checks the availability emissions: a
// successful Discover counts available and a failing one counts
// unreachable. Discover cannot fail in production — the adapter is
// in-memory — so the failure path is exercised through the same emissions a
// future error source would take.
func TestAvailabilityObservations(t *testing.T) {
	reg := prometheus.NewRegistry()
	em := connector.NewEmitter(reg, nil)
	a := New([]Schedule{hourly("triage")}, em, WithNow(func() time.Time {
		return utc(t, "2026-01-05T11:00:01Z")
	}))
	if _, err := a.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, ok := findSeries(t, reg, connector.MetricDiagnosticsTotal, map[string]string{
		"connector": connectorName, "signal": connector.SignalAvailability, "reason": reasonAvailable,
	}); !ok || got != 1 {
		t.Fatalf("availability/available counter = %v, ok=%v; want 1", got, ok)
	}
	// The unreachable emission takes the same path a failed Discover would
	// (Discover itself cannot fail today: the adapter is in-memory).
	em.Availability(connectorName, reasonUnreachable)
	if got, ok := findSeries(t, reg, connector.MetricDiagnosticsTotal, map[string]string{
		"connector": connectorName, "signal": connector.SignalAvailability, "reason": reasonUnreachable,
	}); !ok || got != 1 {
		t.Fatalf("availability/unreachable counter = %v, ok=%v; want 1", got, ok)
	}
}

// TestBoundedDiscoveryDefersWithoutConsuming proves the lane-capacity
// admission contract: DiscoverBoundedTo offers at most the bound, in config
// order, and jobs beyond the bound are not consumed — their cursors stay
// exactly where they were, so the latest due occurrence stays pending and a
// later discovery with capacity offers it under the same latest-only rule,
// never as a catch-up replay.
func TestBoundedDiscoveryDefersWithoutConsuming(t *testing.T) {
	// The adapter is constructed before the first boundary so the 11:00
	// occurrence is due rather than pre-startup (which the cursor would
	// skip by design).
	current := utc(t, "2026-01-05T10:00:00Z")
	a := New([]Schedule{hourly("triage"), hourly("audit")}, nil, WithNow(func() time.Time {
		return current
	}))
	current = utc(t, "2026-01-05T11:00:01Z")

	// A non-positive bound offers nothing and consumes nothing.
	if items, err := a.DiscoverBoundedTo(context.Background(), 0); err != nil || len(items) != 0 {
		t.Fatalf("DiscoverBoundedTo(0) = %v, %v; want no offers", items, err)
	}

	// Two jobs are due at the same 11:00 boundary; a bound of one admits
	// only the first job in config order.
	items, err := a.DiscoverBoundedTo(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	boundary := utc(t, "2026-01-05T11:00:00Z")
	if got, want := ids(items), []string{fmt.Sprintf("triage@%d", boundary.Unix())}; !slices.Equal(got, want) {
		t.Fatalf("bounded offers = %v, want %v", got, want)
	}

	// The bound still binds while the deferred job stays due: the second
	// job's cursor was not consumed, so it keeps offering — and once the
	// first job's boundary is past its cursor, the deferred job's latest
	// due occurrence is admitted.
	items, err = a.DiscoverBoundedTo(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids(items), []string{fmt.Sprintf("audit@%d", boundary.Unix())}; !slices.Equal(got, want) {
		t.Fatalf("deferred job offers = %v, want %v", got, want)
	}

	// Latest-only survives deferral, and deferral never consumes: hold the
	// deferred job back past a boundary, then an unbounded Discover at the
	// same instant offers the audit job's latest due boundary — the cursor
	// stayed where the deferral left it, and the boundary passed while
	// deferred is skipped, not caught up.
	current = utc(t, "2026-01-05T13:00:01Z")
	items, err = a.DiscoverBoundedTo(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	lateBoundary := utc(t, "2026-01-05T13:00:00Z")
	if got, want := ids(items), []string{fmt.Sprintf("triage@%d", lateBoundary.Unix())}; !slices.Equal(got, want) {
		t.Fatalf("later bounded offers = %v, want %v", got, want)
	}
	if got, want := ids(discover(t, a, "2026-01-05T13:00:01Z")), []string{fmt.Sprintf("audit@%d", lateBoundary.Unix())}; !slices.Equal(got, want) {
		t.Fatalf("deferred job's unbounded offers = %v, want %v", got, want)
	}
	// And the advanced cursors never replay before their next boundary.
	if items := discover(t, a, "2026-01-05T13:59:01Z"); len(items) != 0 {
		t.Fatalf("unbounded Discover after deferral advanced = %v, want none", items)
	}
}

// TestBoundedDiscoveryAdmitsEverythingWhenSlotsAllow proves a bound at or
// above the number of due jobs behaves exactly like unbounded Discover.
func TestBoundedDiscoveryAdmitsEverythingWhenSlotsAllow(t *testing.T) {
	current := utc(t, "2026-01-05T10:00:00Z")
	clock := func() time.Time { return current }
	boundedAdapter := New([]Schedule{hourly("triage"), hourly("audit")}, nil, WithNow(clock))
	unboundedAdapter := New([]Schedule{hourly("triage"), hourly("audit")}, nil, WithNow(clock))
	current = utc(t, "2026-01-05T11:00:01Z")
	bounded, err := boundedAdapter.DiscoverBoundedTo(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	unbounded := discover(t, unboundedAdapter, "2026-01-05T11:00:01Z")
	boundary := utc(t, "2026-01-05T11:00:00Z")
	want := []string{
		fmt.Sprintf("triage@%d", boundary.Unix()),
		fmt.Sprintf("audit@%d", boundary.Unix()),
	}
	if !slices.Equal(ids(bounded), want) {
		t.Fatalf("bounded=%v, want %v", ids(bounded), want)
	}
	if !slices.Equal(ids(unbounded), ids(bounded)) {
		t.Fatalf("bounded=%v unbounded=%v; want identical two-job offers", ids(bounded), ids(unbounded))
	}
}
