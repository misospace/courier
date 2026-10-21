package observability_test

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
)

type dashRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type dashTarget struct {
	Expr         string  `json:"expr"`
	LegendFormat string  `json:"legendFormat"`
	Datasource   dashRef `json:"datasource"`
}

type dashPanel struct {
	ID          int          `json:"id"`
	Type        string       `json:"type"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Datasource  dashRef      `json:"datasource"`
	Targets     []dashTarget `json:"targets"`
}

type dashVar struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Regex   string `json:"regex"`
	Current struct {
		Text  string `json:"text"`
		Value string `json:"value"`
	} `json:"current"`
}

type dashDoc struct {
	UID           string `json:"uid"`
	Title         string `json:"title"`
	SchemaVersion int    `json:"schemaVersion"`
	Timezone      string `json:"timezone"`
	Refresh       string `json:"refresh"`
	Templating    struct {
		List []dashVar `json:"list"`
	} `json:"templating"`
	Panels []dashPanel `json:"panels"`
}

func loadDashboard(t *testing.T) dashDoc {
	t.Helper()
	data, err := os.ReadFile("dashboards/courier-overview.json")
	if err != nil {
		t.Fatalf("read dashboard: %v", err)
	}
	var d dashDoc
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("parse dashboard: %v", err)
	}
	return d
}

func TestDashboardHeader(t *testing.T) {
	d := loadDashboard(t)
	if d.UID != "courier-overview" {
		t.Errorf("uid = %q, want courier-overview", d.UID)
	}
	if d.Title != "Courier Overview" {
		t.Errorf("title = %q, want Courier Overview", d.Title)
	}
	if d.SchemaVersion != 39 {
		t.Errorf("schemaVersion = %d, want 39", d.SchemaVersion)
	}
	if d.Timezone != "utc" {
		t.Errorf("timezone = %q, want utc", d.Timezone)
	}
	if d.Refresh != "1m" {
		t.Errorf("refresh = %q, want 1m", d.Refresh)
	}
}

func TestDashboardDatasourceVariable(t *testing.T) {
	d := loadDashboard(t)
	vars := d.Templating.List
	if len(vars) != 1 {
		t.Fatalf("want exactly one template variable, got %d", len(vars))
	}
	v := vars[0]
	if v.Name != "DS_PROMETHEUS" {
		t.Errorf("variable name = %q, want DS_PROMETHEUS", v.Name)
	}
	if v.Type != "datasource" {
		t.Errorf("variable type = %q, want datasource", v.Type)
	}
	if v.Regex != ".*Prometheus.*|.*VictoriaMetrics.*" {
		t.Errorf("variable regex = %q", v.Regex)
	}
	if v.Current.Value != "Prometheus" {
		t.Errorf("variable current value = %q, want Prometheus", v.Current.Value)
	}
}

func TestDashboardPanelsWellFormed(t *testing.T) {
	d := loadDashboard(t)
	if len(d.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
	allowedTypes := map[string]struct{}{
		"timeseries": {}, "stat": {}, "table": {}, "gauge": {},
	}
	seen := map[int]struct{}{}
	for _, p := range d.Panels {
		if _, dup := seen[p.ID]; dup {
			t.Errorf("panel %d: duplicate id", p.ID)
		}
		seen[p.ID] = struct{}{}
		if _, ok := allowedTypes[p.Type]; !ok {
			t.Errorf("panel %q: type %q is not a native panel type", p.Title, p.Type)
		}
		if strings.TrimSpace(p.Title) == "" {
			t.Errorf("panel %d: missing title", p.ID)
		}
		if strings.TrimSpace(p.Description) == "" {
			t.Errorf("panel %d (%s): missing description", p.ID, p.Title)
		}
		if p.Datasource.UID != "${DS_PROMETHEUS}" {
			t.Errorf("panel %q: datasource uid %q, want ${DS_PROMETHEUS}", p.Title, p.Datasource.UID)
		}
		if len(p.Targets) == 0 {
			t.Errorf("panel %q: no targets", p.Title)
			continue
		}
		for _, tgt := range p.Targets {
			if strings.TrimSpace(tgt.Expr) == "" {
				t.Errorf("panel %q target %q: empty expr", p.Title, tgt.LegendFormat)
			}
			if tgt.Datasource.UID != "${DS_PROMETHEUS}" {
				t.Errorf("panel %q target %q: datasource uid %q, want ${DS_PROMETHEUS}",
					p.Title, tgt.LegendFormat, tgt.Datasource.UID)
			}
		}
	}
}

func TestDashboardExprMetricsAreRegistered(t *testing.T) {
	d := loadDashboard(t)
	known := knownMetricNames(t)
	known["process_start_time_seconds"] = struct{}{}
	labels := map[string]struct{}{}
	metrics := map[string]struct{}{}
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			node, err := exprParser.ParseExpr(tgt.Expr)
			if err != nil {
				t.Errorf("panel %q: expr does not parse: %v\n%s", p.Title, err, tgt.Expr)
				continue
			}
			collect(node, labels, metrics)
		}
	}
	for m := range metrics {
		if _, ok := known[m]; !ok {
			t.Errorf("panel expr references unknown metric %q", m)
		}
		if strings.Contains(m, "connector") || strings.Contains(m, "dispatch") {
			t.Errorf("panel expr references excluded metric %q", m)
		}
	}
}

func TestDashboardSummaryGolden(t *testing.T) {
	d := loadDashboard(t)
	lines := make([]string, 0, len(d.Panels))
	for _, p := range d.Panels {
		for _, tgt := range p.Targets {
			lines = append(lines, p.Title+"|"+tgt.Expr)
		}
	}
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"

	const goldenPath = "testdata/dashboard-summary.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to regenerate): %v", err)
	}
	if got != string(want) {
		t.Fatalf("dashboard summary drifted from golden\nwant:\n%s\ngot:\n%s", want, got)
	}
}
