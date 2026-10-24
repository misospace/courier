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
		"--set", "secure.providers[0].credentials.forge-api.secretName=forge-creds",
		"--set", "secure.providers[0].credentials.forge-api.key=token",
		"--set", "secure.providers[0].serves[0]=misospace/*",
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
	for _, unwanted := range []string{
		"courier-bootstrap-lane-profiles",
		"/etc/courier/lane-profiles",
	} {
		if strings.Contains(overridden, unwanted) {
			t.Fatalf("explicit file arg did not disable the laneProfiles transport: output contains %q", unwanted)
		}
	}
	if !strings.Contains(overridden, "--bootstrap-lane-profiles-file=/custom/profiles.yaml") {
		t.Fatalf("explicit --bootstrap-lane-profiles-file arg missing from output")
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
