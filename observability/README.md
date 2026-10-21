# Courier observability assets

Portable dashboard and alert-rule assets for Courier, implementing
[docs/observability-dashboards-and-alerts.md](../docs/observability-dashboards-and-alerts.md).
That document is the **normative specification**: it defines the views, the
health conditions, and the empty/unknown semantics in backend-neutral terms.
The files here are one rendering of it, not the definition.

## Purpose and framing

Everything in this directory is **opt-in**. Courier is observable as an
independently useful application through its own `/metrics` endpoint and
structured stdout logs; it never depends on Grafana, Alertmanager, Dispatch, or
any monitoring stack. Operators who want pre-built views and alerts can adopt
these assets as-is or as a starting point; operators with other backends should
implement the specification directly and are not promised identical assets or
feature parity.

## Layout

```
observability/
  rules/courier-alerts.yml     A1–A8 alert rules (Prometheus/vmalert compatible)
  dashboards/courier-overview.json  standalone Grafana dashboard (uid courier-overview)
  README.md                    this file
  doc.go, *_test.go            package documentation and asset tests
  testdata/                    golden summaries pinning asset content
```

## Installing the assets

**Alert rules (Prometheus).** Add the rule file to your Prometheus
configuration and point it at Courier's metrics endpoint (the operator's
`--metrics-bind-address`, default `:8080`; the Helm chart's
`serviceMonitor.enabled` renders the scrape objects, off by default):

```yaml
rule_files:
  - /etc/prometheus/rules/courier-alerts.yml
```

**Alert rules (VictoriaMetrics).** `vmalert` consumes the same file with
`-rule` / `-datasource.url` flags; the PromQL shapes used here are
VictoriaMetrics-compatible.

**Dashboard (Grafana).** Import `dashboards/courier-overview.json`. It is
standalone: one data-source template variable (`DS_PROMETHEUS`, matching
Prometheus or VictoriaMetrics data sources), no plugins, no folder
assumptions, and no dependence on any other dashboard.

## The one deployment-specific knob

`CourierControllerDown` (A8) matches scrape targets with
`up{job=~".*courier.*"}`. Adjust that job matcher to your scrape
configuration. Nothing else in these assets is deployment-specific.

## OpenTelemetry pipelines: transport versus evaluator

OTLP is a **transport and data model, not an alert engine**. A viable OTel
integration names both halves:

- **Transport:** an OpenTelemetry Collector receives Courier's metrics (over
  OTLP or a scrape receiver) and optionally its structured stdout logs via a
  filelog receiver, and exports them to a TSDB.
- **Evaluator:** alert rules must run in something with a rules engine — a
  Prometheus server, VictoriaMetrics' `vmalert`, or a Grafana Mimir ruler.

Supported combinations:

| Transport | Evaluator | Dashboard | Alert catalogue |
| --- | --- | --- | --- |
| scrape `/metrics` | Prometheus / vmalert / Mimir ruler | any PromQL UI | supported |
| OTLP via Collector | Prometheus / vmalert / Mimir ruler | any TSDB UI | supported |
| OTLP via Collector | none | renders, but panels only | **unsupported** |
| OTel Collector processors only | — | — | **unsupported** |

The Collector's `routing`/`filter` processors may pre-process streams but are
not a general alert evaluator. An OTLP export with no evaluator renders the
dashboard but cannot evaluate the alert catalogue.

## What is deliberately excluded

**Connector panels and A9.** The dashboard implements specification sections
3.1–3.5 only. Section 3.6 (connector availability, exclusion pressure,
capability negotiation, protocol errors) and alert A9 are omitted because the
connector diagnostic signals do not exist yet (#291). Their absence affects no
other panel: Courier health is never gated on connector telemetry.

## Cardinality and privacy constraints

- Metric labels are bounded enums only: `source`, `lane`, `mode`, terminal
  `phase`, transition `from`/`to`, diagnostic `category`, verification and
  lifecycle `result`, token `kind`, and the closed discovery `reason`
  vocabulary. Run IDs, repository names, issue numbers, and error strings are
  never metric labels.
- Per-run correlation (`run_id`) lives in the structured log stream, not in
  metric dimensions; join it in logs, not in these panels.
- Log-derived views inherit Courier's redaction-before-export; integrations
  must not re-introduce unredacted content.
- Metrics and log retention are the deployed stack's responsibility; each
  panel's description states the effective window it shows.

## Unsupported combinations

Mirroring specification section 7:

- Identical dashboards, rules, or queries across backends is not promised;
  a backend may implement a subset but must mark unavailable views rather
  than render them as healthy.
- OTLP with no rules-engine evaluator cannot evaluate the alert catalogue.
- Any dependency on Dispatch telemetry for Courier health.
- Any dependency on specific namespaces, cluster or node names, credential
  sources, notification routes, Grafana, or Alertmanager.
- These assets are not deployment manifests; scraping and notification routing
  are downstream concerns.

## Verification

`go test ./observability/` validates both assets: every dashboard and alert
expression must parse as PromQL, reference only metrics Courier's collectors
actually register, and stay within the bounded label budget. A golden summary
pins the dashboard's panel/query content so edits are deliberate.

## Gaps the dashboard cannot paper over

Several specification views are marked "gap" or "partial" because the
underlying signals do not exist yet: per-source availability and discovery
latency, report latency, runs past their retention window, and restart causes.
Panels here render only what is computable today; see the specification's
section 2 for the full instrumentation-gap inventory.
