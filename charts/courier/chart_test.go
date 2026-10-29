package courier

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	bootstrap "github.com/misospace/courier/internal/bootstrap"
	"github.com/misospace/courier/internal/source/cron"
	"sigs.k8s.io/yaml"
)

func TestChartRendersDispatchConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.prFixExclusions=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test/custom-api",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=lane-y",
		"--set", "dispatch.pollInterval=47s",
		"--set", "dispatch.httpTimeout=19s",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.tokenSecret.key=agent-token",
	)
	for _, want := range []string{
		"--dispatch-enabled=true",
		"--dispatch-pr-fix-exclusions=true",
		"--dispatch-base-url=https://dispatch.example.test/custom-api",
		"--dispatch-agent-name=example-agent",
		"--dispatch-queue-lane=queue-x",
		"--dispatch-lane=lane-y",
		"--dispatch-poll-interval=47s",
		"--dispatch-http-timeout=19s",
		"name: DISPATCH_AGENT_TOKEN\n            valueFrom:\n              secretKeyRef:\n                key: agent-token\n                name: dispatch-token-secret",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("Dispatch-enabled chart output is missing %q", want)
		}
	}

	defaultExcluded := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=lane-y",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
	)
	if strings.Contains(defaultExcluded, "--dispatch-pr-fix-exclusions") {
		t.Fatal("Dispatch-enabled chart output unexpectedly contains PR-fix exclusion flag")
	}

	disabled := runHelm(t, chartDir, "template", "courier", ".", "--set", "dispatch.enabled=false")
	for _, unwanted := range []string{
		"--dispatch-enabled",
		"--dispatch-base-url",
		"--dispatch-agent-name",
		"--dispatch-queue-lane",
		"--dispatch-lane",
		"--dispatch-poll-interval",
		"--dispatch-http-timeout",
		"--dispatch-pr-fix-exclusions",
		"DISPATCH_AGENT_TOKEN",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("Dispatch-disabled chart output unexpectedly contains %q", unwanted)
		}
	}
}

func TestChartRendersLabelSourceConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")
	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "labelSource.enabled=true",
		"--set", "labelSource.repos={owner/one,owner/two}",
		"--set", "labelSource.triggerLabel=forge-work",
		"--set", "labelSource.fixLabel=review-fix",
		"--set", "labelSource.lane=lane-y",
		"--set", "labelSource.pollInterval=47s",
	)
	for _, want := range []string{
		"--label-source-enabled=true",
		"--label-source-repos=owner/one,owner/two",
		"--label-source-trigger-label=forge-work",
		"--label-source-fix-label=review-fix",
		"--label-source-lane=lane-y",
		"--label-source-poll-interval=47s",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("Label-source-enabled chart output is missing %q", want)
		}
	}

	disabled := runHelm(t, chartDir, "template", "courier", ".", "--set", "dispatch.enabled=false")
	for _, unwanted := range []string{
		"--label-source-enabled",
		"--label-source-repos",
		"--label-source-trigger-label",
		"--label-source-fix-label",
		"--label-source-lane",
		"--label-source-poll-interval",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("Label-source-disabled chart output unexpectedly contains %q", unwanted)
		}
	}
}

func TestRBACResourceNamesMatchCoderunCRD(t *testing.T) {
	crd := mustReadChartFile(t, "crd-manifests/courier.misospace.dev_coderruns.yaml")
	generated := mustReadRepositoryFile(t, "config/rbac/role.yaml")
	values := mustReadRepositoryFile(t, "charts/courier/values.yaml")

	if !strings.Contains(crd, "plural: coderruns") {
		t.Fatal("CoderRun CRD does not declare the coderruns plural")
	}
	for _, resource := range []string{"coderruns", "coderruns/status", "coderruns/finalizers"} {
		if !strings.Contains(generated, "- "+resource) {
			t.Fatalf("generated RBAC is missing resource %q", resource)
		}
		if !strings.Contains(values, "resources: ["+resource+"]") {
			t.Fatalf("chart RBAC is missing resource %q", resource)
		}
	}
	for _, content := range []struct {
		name string
		data string
	}{
		{name: "generated RBAC", data: generated},
		{name: "chart values", data: values},
	} {
		if strings.Contains(content.data, "coderuns") {
			t.Fatalf("%s still contains the stale coderuns resource name", content.name)
		}
	}

	// The failure-evidence intake (#115) grants the operator
	// create/get/list/patch/delete on secrets through a namespaced Role in the
	// chart, deliberately not through the generated operator ClusterRole.
	const evidenceRole = `    evidence:
      type: Role
      rules:
        - apiGroups: [""]
          resources: [secrets]
          verbs: [create, get, list, patch, delete]`
	if !strings.Contains(values, evidenceRole) {
		t.Fatal("chart values is missing the namespaced evidence Role with verbs [create, get, list, patch, delete] on secrets")
	}
	const evidenceBinding = `    evidence:
      type: RoleBinding
      roleRef:
        identifier: evidence
      subjects:
        - identifier: main`
	if !strings.Contains(values, evidenceBinding) {
		t.Fatal("chart values is missing the evidence RoleBinding to the main service account")
	}

	// The generated operator ClusterRole's secrets grant must stay
	// create/delete/get/list — no patch — so the namespaced grant never
	// widens the cluster-wide operator ClusterRole.
	secretsIdx := strings.Index(generated, "- secrets")
	if secretsIdx < 0 {
		t.Fatal("generated RBAC has no secrets resource")
	}
	secretsBlock := generated[secretsIdx:]
	if next := strings.Index(secretsBlock, "\n- apiGroups:"); next >= 0 {
		secretsBlock = secretsBlock[:next]
	}
	for _, want := range []string{"- create", "- delete", "- get", "- list"} {
		if !strings.Contains(secretsBlock, want) {
			t.Fatalf("generated RBAC secrets block is missing verb %q", want)
		}
	}
	if strings.Contains(secretsBlock, "- patch") {
		t.Fatal("generated RBAC secrets block must not carry the patch verb")
	}
}

func copyChart(t *testing.T) string {
	t.Helper()
	temporary := t.TempDir()
	chartDir := filepath.Join(temporary, "courier")
	if err := os.CopyFS(chartDir, os.DirFS(".")); err != nil {
		t.Fatalf("copy chart: %v", err)
	}
	return chartDir
}

func mustReadChartFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", path))
	if err != nil {
		t.Fatalf("read chart file %s: %v", path, err)
	}
	return string(data)
}

func mustReadRepositoryFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", path))
	if err != nil {
		t.Fatalf("read repository file %s: %v", path, err)
	}
	return string(data)
}

// chartProbeImage returns the chart's secure.probeImage value.
func chartProbeImage(t *testing.T) string {
	t.Helper()
	var values struct {
		Secure struct {
			ProbeImage string `json:"probeImage"`
		} `json:"secure"`
	}
	if err := yaml.Unmarshal([]byte(mustReadRepositoryFile(t, "charts/courier/values.yaml")), &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	image := strings.TrimSpace(values.Secure.ProbeImage)
	if image == "" {
		t.Fatal("chart values carry no secure.probeImage")
	}
	if !regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`).MatchString(image) {
		t.Fatalf("chart secure.probeImage = %q, want a digest-pinned image reference", image)
	}
	return image
}

func runHelm(t *testing.T, chartDir string, args ...string) string {
	t.Helper()
	command := exec.Command("helm", args...)
	command.Dir = chartDir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func runHelmOutput(t *testing.T, chartDir string, args ...string) (string, error) {
	t.Helper()
	command := exec.Command("helm", args...)
	command.Dir = chartDir
	output, err := command.CombinedOutput()
	return string(output), err
}

func TestServiceMonitorOptional(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	// Off by default: no ServiceMonitor and no metrics Service.
	disabled := runHelm(t, chartDir, "template", "courier", ".")
	for _, unwanted := range []string{
		"kind: ServiceMonitor",
		"courier-metrics",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("ServiceMonitor-disabled chart output unexpectedly contains %q", unwanted)
		}
	}

	// Enabled: a ServiceMonitor and a Service exposing port 8080.
	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "serviceMonitor.enabled=true",
	)
	for _, want := range []string{
		"kind: ServiceMonitor",
		"apiVersion: monitoring.coreos.com/v1",
		"kind: Service",
		"name: courier-metrics",
		"port: 8080",
		"path: /metrics",
		"interval: 30s",
		"scrapeTimeout: 10s",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("ServiceMonitor-enabled chart output is missing %q", want)
		}
	}

	// The metrics Service's selector must exactly equal the controller
	// Deployment's pod selector (spec.selector.matchLabels) so the Service
	// tracks the controller pods and nothing else.
	service := findDocument(t, enabled, "kind: Service", "name: courier-metrics")
	deployment := findDocument(t, enabled, "kind: Deployment", "name: courier")
	selector := blockLines(t, service, "selector:", 0)
	podSelector := deploymentPodSelector(t, deployment)
	if len(selector) == 0 {
		t.Fatal("metrics Service has no selector labels")
	}
	if !sameLabelSet(selector, podSelector) {
		t.Fatalf("metrics Service selector %v does not exactly equal the controller Deployment pod selector %v", selector, podSelector)
	}

	// The ServiceMonitor's selector must match labels actually present on
	// the metrics Service, or Prometheus would discover nothing to scrape.
	monitor := findDocument(t, enabled, "kind: ServiceMonitor", "name: courier-metrics")
	serviceLabels := blockLines(t, service, "labels:", 0)
	monitorSelector := blockLines(t, monitor, "matchLabels:", 0)
	if len(monitorSelector) == 0 {
		t.Fatal("ServiceMonitor has no selector matchLabels")
	}
	have := make(map[string]bool, len(serviceLabels))
	for _, label := range serviceLabels {
		have[label] = true
	}
	for _, label := range monitorSelector {
		if !have[label] {
			t.Fatalf("ServiceMonitor selector label %q is not set on the metrics Service (%v)", label, serviceLabels)
		}
	}
}

func TestChartRendersEvidenceIntake(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	// Off by default: no intake flags and no evidence Service.
	disabled := runHelm(t, chartDir, "template", "courier", ".")
	for _, unwanted := range []string{
		"--evidence-intake",
		"courier-evidence",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("Evidence-disabled chart output unexpectedly contains %q", unwanted)
		}
	}

	// Disabled evidence must not render the namespaced Secret authority, even
	// though values.yaml defines it statically.
	for _, kind := range []string{"kind: Role", "kind: RoleBinding"} {
		for _, doc := range missingDocuments(disabled, kind, "name: courier-evidence") {
			t.Fatalf("Evidence-disabled chart output rendered a document with %q and %q:\n%s", kind, "name: courier-evidence", doc)
		}
	}

	// Enabled: the intake flags and a ClusterIP Service exposing port 80 to
	// the listener's port. helm template without --namespace uses "default"
	// for Release.Namespace, so the service URL is the default-namespace one.
	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "evidence.enabled=true",
		"--set", "evidence.keySecret=evidence-key",
	)
	for _, want := range []string{
		"--evidence-intake-key-secret=evidence-key",
		"--evidence-intake-bind=:8082",
		"--evidence-intake-service=http://courier-evidence.default.svc",
		"kind: Service",
		"name: courier-evidence",
		"port: 80",
		"targetPort: 8082",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("Evidence-enabled chart output is missing %q", want)
		}
	}

	// Enabled evidence carries the namespaced Secret authority: a Role and a
	// RoleBinding that bjw-s names "<release>-<identifier>", so "courier-evidence".
	findDocument(t, enabled, "kind: Role", "name: courier-evidence")
	findDocument(t, enabled, "kind: RoleBinding", "name: courier-evidence")

	// The evidence Service's selector must exactly equal the controller
	// Deployment's pod selector so the Service tracks the controller pods
	// and nothing else.
	service := findDocument(t, enabled, "kind: Service", "name: courier-evidence")
	deployment := findDocument(t, enabled, "kind: Deployment", "name: courier")
	selector := blockLines(t, service, "selector:", 0)
	podSelector := deploymentPodSelector(t, deployment)
	if len(selector) == 0 {
		t.Fatal("evidence Service has no selector labels")
	}
	if !sameLabelSet(selector, podSelector) {
		t.Fatalf("evidence Service selector %v does not exactly equal the controller Deployment pod selector %v", selector, podSelector)
	}

	// Enabled without the key secret fails the render.
	out, err := runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "evidence.enabled=true",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail without evidence.keySecret, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires evidence.keySecret") {
		t.Fatalf("helm template failed without the expected message:\n%s", out)
	}
}

// TestChartRendersRetentionConfiguration checks the #287 retention block is
// rendered as the matching manager flags, and that an explicit args override
// wins over the value.
func TestChartRendersRetentionConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	out := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "retention.done=15m",
		"--set", "retention.failed=72h",
		"--set", "retention.needsHuman=240h",
		"--set", "retention.auditKeep=9",
	)
	for _, want := range []string{
		"--done-retention=15m",
		"--failed-retention=72h",
		"--needs-human-retention=240h",
		"--terminal-audit-keep=9",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("retention chart output is missing %q", want)
		}
	}

	// An explicit manager arg wins over the value, mirroring the evidence block.
	overridden := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "controllers.main.containers.main.args={--leader-elect,--done-retention=1m}",
		"--set", "retention.done=15m",
	)
	if strings.Contains(overridden, "--done-retention=15m") {
		t.Fatal("explicit --done-retention arg did not win over the retention.done value")
	}
	if !strings.Contains(overridden, "--done-retention=1m") {
		t.Fatalf("explicit --done-retention arg missing from output")
	}
}

// findDocument returns the YAML document (a block of the multi-doc helm
// output separated by ---) that contains a line equal to kindLine and a
// line equal to nameLine.
func findDocument(t *testing.T, output, kindLine, nameLine string) string {
	t.Helper()
	for _, doc := range strings.Split(output, "\n---") {
		hasKind, hasName := false, false
		for _, line := range strings.Split(doc, "\n") {
			switch strings.TrimSpace(line) {
			case kindLine:
				hasKind = true
			case nameLine:
				hasName = true
			}
		}
		if hasKind && hasName {
			return doc
		}
	}
	t.Fatalf("no document with a line %q and a line %q", kindLine, nameLine)
	return ""
}

// missingDocuments returns every document (a block of the multi-doc helm
// output separated by ---) that contains a line equal to kindLine and a line
// equal to nameLine. It is the inverse of findDocument: an empty result is the
// expected outcome for an assertion that a resource must not be rendered.
func missingDocuments(output, kindLine, nameLine string) []string {
	var matches []string
	for _, doc := range strings.Split(output, "\n---") {
		hasKind, hasName := false, false
		for _, line := range strings.Split(doc, "\n") {
			switch strings.TrimSpace(line) {
			case kindLine:
				hasKind = true
			case nameLine:
				hasName = true
			}
		}
		if hasKind && hasName {
			matches = append(matches, doc)
		}
	}
	return matches
}

// deploymentPodSelector returns the "key: value" lines of the controller
// Deployment's spec.selector.matchLabels (its pod selector).
func deploymentPodSelector(t *testing.T, doc string) []string {
	t.Helper()
	return blockLines(t, doc, "matchLabels:", 0)
}

// sameLabelSet reports whether a and b hold the same label lines, regardless
// of order or duplication.
func sameLabelSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, label := range a {
		counts[label]++
	}
	for _, label := range b {
		counts[label]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}

// blockLines returns the "key: value" lines in the indented block that
// follows the first line equal to header at or after line fromLine.
func blockLines(t *testing.T, doc, header string, fromLine int) []string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	start := -1
	for i := fromLine; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == header {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("header %q not found in document", header)
	}
	headerIndent := leadingSpaces(lines[start])
	var out []string
	for i := start + 1; i < len(lines); i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			continue
		}
		if leadingSpaces(line) <= headerIndent {
			break
		}
		out = append(out, strings.TrimSpace(line))
	}
	return out
}

func leadingSpaces(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

func TestChartRendersMultipleLaneBindings(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	out := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.lanes[0].queueLane=normal",
		"--set", "dispatch.lanes[0].laneProfile=default",
		"--set", "dispatch.lanes[0].agentName=courier-local",
		"--set", "dispatch.lanes[1].queueLane=normal",
		"--set", "dispatch.lanes[1].laneProfile=cloud",
		"--set", "dispatch.lanes[1].agentName=courier-cloud",
	)
	for _, want := range []string{
		"--dispatch-lane-binding=normal:default:courier-local",
		"--dispatch-lane-binding=normal:cloud:courier-cloud",
		"DISPATCH_AGENT_TOKEN",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("multi-lane chart output is missing %q", want)
		}
	}
	for _, unwanted := range []string{
		"--dispatch-queue-lane=",
		"--dispatch-lane=",
	} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("multi-lane chart output unexpectedly contains %q", unwanted)
		}
	}
}

func TestChartRejectsAmbiguousDispatchLaneValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	runHelm(t, chartDir, "dependency", "build")

	out, err := runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.lanes[0].queueLane=normal",
		"--set", "dispatch.lanes[0].laneProfile=default",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "cannot be combined") {
		t.Fatalf("helm template failed without the expected message:\n%s", out)
	}
}

func TestChartRejectsDuplicateDispatchAgentIdentity(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	runHelm(t, chartDir, "dependency", "build")

	out, err := runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.lanes[0].queueLane=normal",
		"--set", "dispatch.lanes[0].laneProfile=default",
		"--set", "dispatch.lanes[0].agentName=worker",
		"--set", "dispatch.lanes[1].queueLane=normal",
		"--set", "dispatch.lanes[1].laneProfile=escalation",
		"--set", "dispatch.lanes[1].agentName=worker",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail for duplicate agentName, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "duplicate agentName") {
		t.Fatalf("helm template failed without the expected duplicate agentName message:\n%s", out)
	}

	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "dispatch.lanes[0].queueLane=normal",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail for an incomplete lane binding, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires queueLane and laneProfile") {
		t.Fatalf("helm template failed without the expected message:\n%s", out)
	}
}

func TestChartRendersSecureTopologyConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	enabled := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.dependencyCacheService=cache-system/go-cache",
		"--set", "secure.modelGateway.url=http://litellm:4000/v1",
		"--set", "secure.modelGateway.keySecret=gateway-key",
		"--set", "secure.modelGateway.cidr=10.10.0.0/32",
		"--set", "secure.modelGateway.port=4000",
	)
	for _, want := range []string{
		"--secure-mode=true",
		"--forge-providers-file=/etc/courier/forge-providers.json",
		"--run-namespace=courier-runs",
		"--dependency-cache-service=cache-system/go-cache",
		"--model-gateway-url=http://litellm:4000/v1",
		"--model-gateway-key-secret=gateway-key",
		"--model-gateway-cidr=10.10.0.0/32",
		"--model-gateway-port=4000",
		"--probe-image=" + chartProbeImage(t),
		"name: courier-forge-providers",
		"courier-broker-tokenreview",
		"mountPath: /etc/courier",
	} {
		if !strings.Contains(enabled, want) {
			t.Fatalf("secure-enabled chart output is missing %q", want)
		}
	}
	if !strings.Contains(enabled, "\"serves\": [") && !strings.Contains(enabled, "\"serves\":") {
		t.Fatalf("providers ConfigMap does not carry the serves patterns")
	}

	disabled := runHelm(t, chartDir, "template", "courier", ".")
	for _, unwanted := range []string{
		"--secure-mode",
		"--forge-providers-file",
		"--run-namespace",
		"courier-forge-providers",
		"courier-broker-tokenreview",
	} {
		if strings.Contains(disabled, unwanted) {
			t.Fatalf("secure-disabled chart output unexpectedly contains %q", unwanted)
		}
	}
}

// TestChartRendersBootstrapLaneProfiles checks the #74 laneProfiles values
// transport: the values serialize into a release ConfigMap that the manager
// reads through the --bootstrap-lane-profiles-file seam, the rendered output
// never contains a LaneProfile manifest (the original CRD-ordering race stays
// closed), and the full portable spec — runtimeImage, multiple roles, and
// multi-line framing — round-trips exactly through the manager's own parser.
func TestChartRendersBootstrapLaneProfiles(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	valuesFile := filepath.Join(chartDir, "values-lanes.yaml")
	values := `laneProfiles:
  bootstrap:
    runtimeImage: ghcr.io/misospace/courier-go:0.1.0
    concurrency: 2
    roles:
      coordinator: litellm/qwen3.8-27b
      reviewer: litellm-anthropic/MiniMax-M3
    framing: |
      keep parallelism modest
      and leave the pull request ready for review
  cloud:
    concurrency: 4
    roles:
      coordinator: openai/gpt-5
    framing: elastic capacity, fan out freely
`
	if err := os.WriteFile(valuesFile, []byte(values), 0o600); err != nil {
		t.Fatalf("write lane values: %v", err)
	}

	out := runHelm(t, chartDir, "template", "courier", ".", "--values", "values-lanes.yaml")
	assertNoLaneProfileManifest(t, out)
	for _, want := range []string{
		"--bootstrap-lane-profiles-file=/etc/courier/lane-profiles/profiles.yaml",
		"name: courier-bootstrap-lane-profiles",
		"mountPath: /etc/courier/lane-profiles",
		"checksum/configMaps:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("bootstrap-lanes chart output is missing %q", want)
		}
	}

	configMap := findDocument(t, out, "kind: ConfigMap", "name: courier-bootstrap-lane-profiles")
	var rendered struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal([]byte(configMap), &rendered); err != nil {
		t.Fatalf("parse rendered bootstrap ConfigMap: %v", err)
	}
	config, ok := rendered.Data["profiles.yaml"]
	if !ok {
		t.Fatalf("bootstrap ConfigMap carries no profiles.yaml key:\n%s", configMap)
	}
	profiles, err := bootstrap.Parse([]byte(config))
	if err != nil {
		t.Fatalf("manager parser rejected the rendered bootstrap config: %v", err)
	}
	wantProfiles := []bootstrap.Profile{
		{
			Name:         "bootstrap",
			RuntimeImage: "ghcr.io/misospace/courier-go:0.1.0",
			Concurrency:  2,
			Roles: map[string]string{
				"coordinator": "litellm/qwen3.8-27b",
				"reviewer":    "litellm-anthropic/MiniMax-M3",
			},
			Framing: "keep parallelism modest\nand leave the pull request ready for review\n",
		},
		{
			Name:        "cloud",
			Concurrency: 4,
			Roles:       map[string]string{"coordinator": "openai/gpt-5"},
			Framing:     "elastic capacity, fan out freely",
		},
	}
	if !reflect.DeepEqual(profiles, wantProfiles) {
		t.Fatalf("bootstrap config round-trip mismatch:\n got: %#v\nwant: %#v", profiles, wantProfiles)
	}

	// Dispatch stays decoupled from the bootstrap lanes: referencing a
	// bootstrap lane name by laneProfile renders both features together.
	combined := runHelm(t, chartDir, "template", "courier", ".",
		"--values", "values-lanes.yaml",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=bootstrap",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
	)
	assertNoLaneProfileManifest(t, combined)
	for _, want := range []string{
		"--dispatch-queue-lane=queue-x",
		"--dispatch-lane=bootstrap",
		"--bootstrap-lane-profiles-file=/etc/courier/lane-profiles/profiles.yaml",
	} {
		if !strings.Contains(combined, want) {
			t.Fatalf("dispatch-plus-lanes chart output is missing %q", want)
		}
	}
}

// TestChartRollsManagerOnBootstrapLaneChanges pins the #74 upgrade contract:
// changing the laneProfiles values changes the manager pod template (via the
// library's configMaps checksum), identical values render a stable checksum,
// and the default render carries no bootstrap transport at all.
func TestChartRollsManagerOnBootstrapLaneChanges(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	writeLaneValues := func(framing string) string {
		t.Helper()
		path := filepath.Join(chartDir, "values-lanes.yaml")
		values := "laneProfiles:\n  local:\n    concurrency: 1\n    roles:\n      coordinator: litellm/qwen3.8-27b\n    framing: " + framing + "\n"
		if err := os.WriteFile(path, []byte(values), 0o600); err != nil {
			t.Fatalf("write lane values: %v", err)
		}
		return path
	}

	first := configMapsChecksum(t, runHelm(t, chartDir, "template", "courier", ".", "--values", writeLaneValues("keep it modest")))
	repeat := configMapsChecksum(t, runHelm(t, chartDir, "template", "courier", ".", "--values", writeLaneValues("keep it modest")))
	if first != repeat {
		t.Fatalf("identical lane values produced different pod-template checksums: %s vs %s", first, repeat)
	}
	changed := configMapsChecksum(t, runHelm(t, chartDir, "template", "courier", ".", "--values", writeLaneValues("capacity is elastic, fan out freely")))
	if first == changed {
		t.Fatalf("changed lane values did not change the pod-template checksum %s", first)
	}

	defaultRender := runHelm(t, chartDir, "template", "courier", ".")
	if strings.Contains(defaultRender, "checksum/configMaps") {
		t.Fatal("default render unexpectedly carries a configMaps checksum annotation")
	}
	for _, unwanted := range []string{
		"--bootstrap-lane-profiles-file",
		"courier-bootstrap-lane-profiles",
		"/etc/courier/lane-profiles",
	} {
		if strings.Contains(defaultRender, unwanted) {
			t.Fatalf("default render unexpectedly contains %q", unwanted)
		}
	}
}

// TestChartRejectsInvalidBootstrapLaneValues covers the render-time lane
// validation and the explicit-arg precedence: a user-supplied
// --bootstrap-lane-profiles-file arg wins over the laneProfiles values, which
// then render no ConfigMap, mount, or flag of their own.
func TestChartRejectsInvalidBootstrapLaneValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")

	out, err := runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "laneProfiles.Local.roles.coordinator=litellm/qwen3.8-27b",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail for an invalid lane name, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "must be a valid DNS-1123 label") {
		t.Fatalf("helm template failed without the expected DNS-1123 message:\n%s", out)
	}

	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "laneProfiles.local.concurrency=1",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail for a lane without roles, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires at least one entry under roles") {
		t.Fatalf("helm template failed without the expected roles message:\n%s", out)
	}

	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "laneProfiles.local=null",
	)
	if err == nil {
		t.Fatalf("expected helm template to fail for a non-mapping lane entry, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "must be a mapping of lane spec fields") {
		t.Fatalf("helm template failed without the expected mapping message:\n%s", out)
	}

	overridden := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "laneProfiles.local.roles.coordinator=litellm/qwen3.8-27b",
		"--set", "controllers.main.containers.main.args={--leader-elect,--bootstrap-lane-profiles-file=/custom/profiles.yaml}",
	)
	assertNoBootstrapTransport(t, overridden)
	if !strings.Contains(overridden, "--bootstrap-lane-profiles-file=/custom/profiles.yaml") {
		t.Fatalf("explicit --bootstrap-lane-profiles-file arg missing from output")
	}

	// The two-argument flag form must be recognized too: Go's flag package
	// takes the last occurrence, so an undetected split-form override would
	// let the chart's appended flag silently replace the user's path.
	split := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "laneProfiles.local.roles.coordinator=litellm/qwen3.8-27b",
		"--set", "controllers.main.containers.main.args={--leader-elect,--bootstrap-lane-profiles-file,/custom/profiles.yaml}",
	)
	assertNoBootstrapTransport(t, split)
	if !strings.Contains(split, "/custom/profiles.yaml") {
		t.Fatalf("split-form --bootstrap-lane-profiles-file path missing from output")
	}
}

// assertNoBootstrapTransport fails when a rendered chart still carries any
// part of the laneProfiles transport: the ConfigMap, the volume mount, or the
// pod template's configMaps checksum.
func assertNoBootstrapTransport(t *testing.T, output string) {
	t.Helper()
	for _, unwanted := range []string{
		"courier-bootstrap-lane-profiles",
		"/etc/courier/lane-profiles",
		"checksum/configMaps",
	} {
		if strings.Contains(output, unwanted) {
			t.Fatalf("explicit file arg did not disable the laneProfiles transport: output contains %q", unwanted)
		}
	}
}

// assertNoLaneProfileManifest fails when any rendered document is a LaneProfile
// object. The check is line-anchored: the LaneProfile CRD itself renders an
// indented names.kind field, which must not trip the guard.
func assertNoLaneProfileManifest(t *testing.T, output string) {
	t.Helper()
	manifest := regexp.MustCompile(`(?m)^kind: LaneProfile$`)
	for _, doc := range strings.Split(output, "\n---") {
		if manifest.MatchString(doc) {
			t.Fatalf("chart rendered a LaneProfile manifest:\n%s", doc)
		}
	}
}

// configMapsChecksum returns the pod template's configMaps checksum annotation
// value from a rendered chart.
func configMapsChecksum(t *testing.T, output string) string {
	t.Helper()
	match := regexp.MustCompile(`checksum/configMaps: ([0-9a-f]{64})`).FindStringSubmatch(output)
	if match == nil {
		t.Fatalf("rendered output carries no checksum/configMaps annotation")
	}
	return match[1]
}

// TestChartRendersCronSchedules checks the cron.schedules values transport:
// values serialize into a release ConfigMap the manager reads through the
// --cron-schedules-file seam, the rendered config round-trips through the
// manager's own parser, and the poll interval rides along.
func TestChartRendersCronSchedules(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")
	valuesFile := filepath.Join(chartDir, "values-cron.yaml")
	values := `cron:
  schedules:
    nightly-triage:
      repo: misospace/courier
      issue: 15
      lane: frontier
      schedule: "30 2 * * *"
      timezone: America/New_York
    weekly-sweep:
      repo: misospace/courier
      issue: 99
      lane: local
      schedule: "0 4 * * 1"
      mode: fix-pr
      debug: true
  pollInterval: 45s
`
	if err := os.WriteFile(valuesFile, []byte(values), 0o600); err != nil {
		t.Fatalf("write cron values: %v", err)
	}
	out := runHelm(t, chartDir, "template", "courier", ".", "--values", "values-cron.yaml")
	for _, want := range []string{
		"--cron-schedules-file=/etc/courier/cron-schedules/schedules.yaml",
		"--cron-poll-interval=45s",
		"name: courier-cron-schedules",
		"mountPath: /etc/courier/cron-schedules",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("cron chart output missing %q", want)
		}
	}
	configMap := findDocument(t, out, "kind: ConfigMap", "name: courier-cron-schedules")
	var rendered struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal([]byte(configMap), &rendered); err != nil {
		t.Fatalf("parse rendered cron ConfigMap: %v", err)
	}
	config, ok := rendered.Data["schedules.yaml"]
	if !ok {
		t.Fatalf("cron ConfigMap carries no schedules.yaml key:\n%s", configMap)
	}
	schedules, err := cron.Parse([]byte(config))
	if err != nil {
		t.Fatalf("manager parser rejected rendered cron config: %v", err)
	}
	if len(schedules) != 2 {
		t.Fatalf("parsed %d schedules, want 2", len(schedules))
	}
	if schedules[0].Name != "nightly-triage" || schedules[0].Repo != "misospace/courier" ||
		schedules[0].Issue != 15 || schedules[0].Lane != "frontier" ||
		schedules[0].Schedule != "30 2 * * *" || schedules[0].Timezone != "America/New_York" ||
		schedules[0].Mode != cron.ModeResolveIssue {
		t.Fatalf("nightly-triage round-trip mismatch: %#v", schedules[0])
	}
	if schedules[1].Name != "weekly-sweep" || schedules[1].Mode != cron.ModeFixPR ||
		!schedules[1].Debug || schedules[1].Lane != "local" {
		t.Fatalf("weekly-sweep round-trip mismatch: %#v", schedules[1])
	}
}

// TestChartRejectsInvalidCronValues covers render-time job validation and
// explicit-arg precedence: a user-supplied --cron-schedules-file arg wins
// over cron.schedules values, and the poll interval is still rendered when
// that precedence path is taken.
func TestChartRejectsInvalidCronValues(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	chartDir := copyChart(t)
	if err := exec.Command("helm", "repo", "add", "bjw-s-labs", "https://bjw-s-labs.github.io/helm-charts").Run(); err != nil {
		t.Fatalf("configure chart repository: %v", err)
	}
	runHelm(t, chartDir, "dependency", "build")
	out, err := runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.Bad_Name.repo=misospace/courier",
		"--set", "cron.schedules.Bad_Name.issue=15",
		"--set", "cron.schedules.Bad_Name.lane=frontier",
		"--set", "cron.schedules.Bad_Name.schedule=* * * * *",
	)
	if err == nil {
		t.Fatalf("expected helm template fail for an invalid job name, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "must be a valid DNS-1123 label") {
		t.Fatalf("helm template failed without expected DNS-1123 message:\n%s", out)
	}
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.nightly.repo=misospace/courier",
		"--set", "cron.schedules.nightly.lane=frontier",
		"--set", "cron.schedules.nightly.schedule=* * * * *",
	)
	if err == nil {
		t.Fatalf("expected helm template fail for a job without issue, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires a positive integer issue number") {
		t.Fatalf("helm template failed without expected issue message:\n%s", out)
	}
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.nightly.repo=misospace/courier",
		"--set", "cron.schedules.nightly.issue=-5",
		"--set", "cron.schedules.nightly.lane=frontier",
		"--set", "cron.schedules.nightly.schedule=* * * * *",
	)
	if err == nil {
		t.Fatalf("expected helm template fail for a negative issue, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires a positive integer issue number") {
		t.Fatalf("helm template failed without expected issue message:\n%s", out)
	}
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.nightly.repo=misospace/courier",
		"--set-json", "cron.schedules.nightly.issue=12.5",
		"--set", "cron.schedules.nightly.lane=frontier",
		"--set", "cron.schedules.nightly.schedule=* * * * *",
	)
	if err == nil {
		t.Fatalf("expected helm template fail for a fractional issue, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "requires a positive integer issue number") {
		t.Fatalf("helm template failed without expected issue message:\n%s", out)
	}
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.nightly.repo=misospace/courier",
		"--set", "cron.schedules.nightly.issue=15",
		"--set", "cron.schedules.nightly.lane=frontier",
		"--set", "cron.schedules.nightly.schedule=* * * * *",
		"--set", "cron.schedules.nightly.mode=scheduled",
	)
	if err == nil {
		t.Fatalf("expected helm template fail for an invalid mode, but it succeeded:\n%s", out)
	}
	if !strings.Contains(out, "mode must be resolve-issue or fix-pr") {
		t.Fatalf("helm template failed without expected mode message:\n%s", out)
	}
	overridden := runHelm(t, chartDir, "template", "courier", ".",
		"--set", "cron.schedules.nightly.repo=misospace/courier",
		"--set", "cron.schedules.nightly.issue=15",
		"--set", "cron.schedules.nightly.lane=frontier",
		"--set", "cron.schedules.nightly.schedule=* * * * *",
		"--set", "cron.pollInterval=45s",
		"--set", "controllers.main.containers.main.args={--leader-elect,--cron-schedules-file=/custom/schedules.yaml}",
	)
	if strings.Contains(overridden, "courier-cron-schedules") || strings.Contains(overridden, "/etc/courier/cron-schedules") {
		t.Fatalf("explicit file arg did not disable cron transport:\n%s", overridden)
	}
	if !strings.Contains(overridden, "--cron-schedules-file=/custom/schedules.yaml") {
		t.Fatalf("explicit --cron-schedules-file arg missing from output")
	}
	if !strings.Contains(overridden, "--cron-poll-interval=45s") {
		t.Fatalf("cron poll interval lost under explicit-arg precedence")
	}
}

func TestChartRendersSideBySideOwnershipConfiguration(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	runHelm(t, chartDir, "dependency", "build")

	// The default install carries no ownership surface at all: one
	// controller keeps the backward-compatible boundary.
	unscoped := runHelm(t, chartDir, "template", "courier", ".")
	for _, unwanted := range []string{"--owned-namespace", "--leader-election-id", "--broker-tokenreview-cluster-role"} {
		if strings.Contains(unscoped, unwanted) {
			t.Fatalf("default chart output unexpectedly contains %q", unwanted)
		}
	}
	if !strings.Contains(unscoped, "kind: CustomResourceDefinition") {
		t.Fatal("default chart output must still render the CRDs")
	}

	// A secure canary beside v1: owned run namespace, distinct lease,
	// distinct broker TokenReview ClusterRole, CRDs managed out-of-band.
	scoped := runHelm(t, chartDir, "template", "courier-native", ".",
		"--namespace", "courier-native-system",
		"--set", "crds.enabled=false",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.brokerTokenReviewClusterRole=courier-native-broker-tokenreview",
		"--set", "ownership.namespaces[0]=courier-runs",
		"--set", "ownership.leaderElectionID=courier-native.misospace.dev",
	)
	for _, want := range []string{
		"--owned-namespace=courier-runs",
		"--leader-election-id=courier-native.misospace.dev",
		"--broker-tokenreview-cluster-role=courier-native-broker-tokenreview",
		"name: courier-native-broker-tokenreview",
	} {
		if !strings.Contains(scoped, want) {
			t.Fatalf("scoped chart output missing %q", want)
		}
	}
	if strings.Contains(scoped, "kind: CustomResourceDefinition") {
		t.Fatal("crds.enabled=false must render no CRDs so a second release never claims them")
	}
	if strings.Contains(scoped, "name: courier-broker-tokenreview\n") {
		t.Fatal("an overridden broker TokenReview name must replace the historical name entirely")
	}

	// An explicit arg equal to the value stays valid; a contradicting one
	// fails instead of binding brokers to a ClusterRole that is never
	// rendered.
	aligned := runHelm(t, chartDir, "template", "courier-native", ".",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.brokerTokenReviewClusterRole=courier-native-broker-tokenreview",
		"--set-string", "controllers.main.containers.main.args[0]=--leader-elect",
		"--set-string", "controllers.main.containers.main.args[1]=--broker-tokenreview-cluster-role=courier-native-broker-tokenreview",
	)
	if count := strings.Count(aligned, "--broker-tokenreview-cluster-role="); count != 1 {
		t.Fatalf("aligned explicit arg produced %d broker TokenReview flags, want exactly 1", count)
	}
	out, err := runHelmOutput(t, chartDir, "template", "courier-native", ".",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.brokerTokenReviewClusterRole=courier-native-broker-tokenreview",
		"--set-string", "controllers.main.containers.main.args[0]=--broker-tokenreview-cluster-role=some-other-role",
	)
	if err == nil || !strings.Contains(out, "contradicts the rendered broker TokenReview ClusterRole") {
		t.Fatalf("expected a contradicting explicit broker role arg to fail, err = %v:\n%s", err, out)
	}

	// With the value left empty the chart renders the historical ClusterRole
	// name: an aligned explicit arg stays valid, a contradicting one fails
	// instead of binding brokers to a ClusterRole that is never rendered.
	historical := runHelm(t, chartDir, "template", "courier-historical", ".",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set-string", "controllers.main.containers.main.args[0]=--broker-tokenreview-cluster-role=courier-broker-tokenreview",
	)
	if count := strings.Count(historical, "--broker-tokenreview-cluster-role="); count != 1 {
		t.Fatalf("historical-aligned explicit arg produced %d broker TokenReview flags, want exactly 1", count)
	}
	if !strings.Contains(historical, "name: courier-broker-tokenreview") {
		t.Fatal("historical-aligned explicit arg must render the ClusterRole under the historical name")
	}
	out, err = runHelmOutput(t, chartDir, "template", "courier-historical", ".",
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set-string", "controllers.main.containers.main.args[0]=--broker-tokenreview-cluster-role=some-other-role",
	)
	if err == nil || !strings.Contains(out, "contradicts the rendered broker TokenReview ClusterRole") {
		t.Fatalf("expected a contradicting explicit broker role arg to fail with an empty value, err = %v:\n%s", err, out)
	}
}

func TestChartRejectsOwnershipBoundaryThatStrandsRuns(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	chartDir := copyChart(t)
	runHelm(t, chartDir, "dependency", "build")

	secureBase := []string{
		"--set", "secure.enabled=true",
		"--set", "secure.runNamespace=courier-runs",
		"--set", "secure.providers[0].name=github",
		"--set", "secure.providers[0].type=github",
		"--set", "secure.providers[0].endpoint=https://api.github.com/",
		"--set", "secure.providers[0].serves[0]=misospace/*",
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
	}

	// The boundary must carry secure.runNamespace, or the secure controller
	// owns nothing.
	out, err := runHelmOutput(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...), "--set", "ownership.namespaces[0]=somewhere-else")...)
	if err == nil || !strings.Contains(out, "must appear in ownership.namespaces") {
		t.Fatalf("expected a boundary excluding secure.runNamespace to fail, err = %v:\n%s", err, out)
	}

	// Discovery materializes runs in the release namespace; a boundary that
	// excludes it would strand every discovered run.
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--namespace", "courier-system",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=lane-y",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set", "ownership.namespaces[0]=somewhere-else",
	)
	if err == nil || !strings.Contains(out, "do not own") {
		t.Fatalf("expected a boundary excluding the discovery namespace to fail, err = %v:\n%s", err, out)
	}

	// An explicit --owned-namespace arg is the effective boundary: the same
	// couplings must apply to it, and it must not be silently overridden by
	// ownership.namespaces.
	out, err = runHelmOutput(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=somewhere-else")...)
	if err == nil || !strings.Contains(out, "must appear in explicit --owned-namespace args") {
		t.Fatalf("expected an explicit boundary excluding secure.runNamespace to fail, err = %v:\n%s", err, out)
	}

	aligned := runHelm(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=courier-runs")...)
	if count := strings.Count(aligned, "--owned-namespace="); count != 1 {
		t.Fatalf("aligned explicit owned-namespace arg produced %d owned-namespace flags, want exactly 1", count)
	}

	// The bare flag form takes its value from the next argument, so the
	// couplings see the same effective list.
	bare := runHelm(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace",
			"--set-string", "controllers.main.containers.main.args[1]=courier-runs")...)
	if count := strings.Count(bare, "--owned-namespace"); count != 1 {
		t.Fatalf("bare explicit owned-namespace arg produced %d owned-namespace flags, want exactly 1", count)
	}

	// The operator's flag parser consumes the next argument after a bare
	// --owned-namespace unconditionally, so a bare flag without a capturable
	// value fails at render.
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace")
	if err == nil || !strings.Contains(out, "has no value at render time") {
		t.Fatalf("expected a bare owned-namespace flag without a value to fail, err = %v:\n%s", err, out)
	}

	// Explicit args and agreeing ownership.namespaces render the explicit
	// boundary exactly once.
	agreeing := runHelm(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=courier-runs",
			"--set", "ownership.namespaces[0]=courier-runs")...)
	if count := strings.Count(agreeing, "--owned-namespace="); count != 1 {
		t.Fatalf("agreeing explicit arg and ownership.namespaces produced %d owned-namespace flags, want exactly 1", count)
	}

	out, err = runHelmOutput(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=somewhere-else",
			"--set", "ownership.namespaces[0]=courier-runs")...)
	if err == nil || !strings.Contains(out, "disagree") {
		t.Fatalf("expected contradicting explicit args and ownership.namespaces to fail, err = %v:\n%s", err, out)
	}

	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--namespace", "courier-system",
		"--set", "dispatch.enabled=true",
		"--set", "dispatch.baseURL=https://dispatch.example.test",
		"--set", "dispatch.agentName=example-agent",
		"--set", "dispatch.queueLane=queue-x",
		"--set", "dispatch.laneProfile=lane-y",
		"--set", "dispatch.tokenSecret.name=dispatch-token-secret",
		"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=somewhere-else",
	)
	if err == nil || !strings.Contains(out, "do not own") {
		t.Fatalf("expected an explicit boundary excluding the discovery namespace to fail, err = %v:\n%s", err, out)
	}

	// An explicit owned namespace name that is not a DNS-1123 label fails at
	// render too.
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set-string", "controllers.main.containers.main.args[0]=--owned-namespace=Bad_Name")
	if err == nil || !strings.Contains(out, "DNS-1123 label") {
		t.Fatalf("expected an invalid explicit owned namespace to fail, err = %v:\n%s", err, out)
	}

	// A secure release with discovery and no explicit list derives a
	// boundary of just secure.runNamespace, so it must live there.
	out, err = runHelmOutput(t, chartDir,
		append(append([]string{"template", "courier", "."}, secureBase...),
			"--namespace", "courier-system",
			"--set", "dispatch.enabled=true",
			"--set", "dispatch.baseURL=https://dispatch.example.test",
			"--set", "dispatch.agentName=example-agent",
			"--set", "dispatch.queueLane=queue-x",
			"--set", "dispatch.laneProfile=lane-y",
			"--set", "dispatch.tokenSecret.name=dispatch-token-secret")...)
	if err == nil || !strings.Contains(out, "must be deployed there") {
		t.Fatalf("expected secure discovery outside secure.runNamespace to fail, err = %v:\n%s", err, out)
	}

	// An owned namespace name that is not a DNS-1123 label fails at render.
	out, err = runHelmOutput(t, chartDir, "template", "courier", ".",
		"--set", "ownership.namespaces[0]=Bad_Name")
	if err == nil || !strings.Contains(out, "DNS-1123 label") {
		t.Fatalf("expected an invalid owned namespace to fail, err = %v:\n%s", err, out)
	}
}
