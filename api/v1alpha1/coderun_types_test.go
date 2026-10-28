package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// envtestClient is a client against a real API server carrying the generated
// CRDs, when KUBEBUILDER_ASSETS is set; validation-rule tests are then
// enforced by the API server exactly as an install would enforce them.
var envtestClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	server := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../charts/courier/crd-manifests"},
		ErrorIfCRDPathMissing: true,
	}
	config, err := server.Start()
	if err != nil {
		panic(err)
	}
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		panic(err)
	}
	envtestClient, err = client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	if err := server.Stop(); err != nil && code == 0 {
		code = 1
	}
	os.Exit(code)
}

// TestPublicationPolicyWire pins the persisted policy schema: it is bound to
// the run UID and must not carry control-pod identity, which belongs to the
// auth/status path only.
func TestPublicationPolicyWire(t *testing.T) {
	p := &PublicationPolicy{
		RunUID:              "run-uid",
		ProviderConfigRef:   "forge-a",
		BaseRepo:            "org/base",
		BaseRef:             "main",
		BaseOID:             "base-oid",
		WorkRepo:            "org/work",
		WorkRef:             "feature/x",
		WorkInitiallyAbsent: true,
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if _, ok := wire["controlPodUID"]; ok {
		t.Fatalf("policy persists control-pod identity: %s", raw)
	}
	if _, ok := wire["runUID"]; !ok {
		t.Fatalf("policy missing runUID binding: %s", raw)
	}
}

// TestCRDManifestLaneProfileResourcesSchema pins the generated CRD manifest to
// the narrowed LaneProfile pod-resources vocabulary.
func TestCRDManifestLaneProfileResourcesSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_laneprofiles.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var resources []string
	inProperties := false
	inResources := false
	indent := ""
	for _, line := range lines {
		if !inProperties {
			if strings.Contains(line, "properties:") {
				inProperties = true
			}
			continue
		}
		if !inResources {
			if i := strings.Index(line, "resources:"); i >= 0 {
				inResources = true
				indent = line[:i]
			}
			continue
		}
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		resources = append(resources, line)
	}
	block := strings.Join(resources, "\n")
	if strings.Contains(block, "claims:") {
		t.Fatal("LaneProfile CRD resources schema still accepts claims")
	}
	for _, key := range []string{"requests:", "limits:"} {
		if !strings.Contains(block, key) {
			t.Fatalf("LaneProfile CRD resources schema missing %s", key)
		}
	}
}

// TestCRDManifestPolicySchema pins the generated CRD manifest to the same
// invariant: no controlPodUID anywhere under the publicationPolicy schema.
func TestCRDManifestPolicySchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_coderruns.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var policy []string
	inPolicy := false
	indent := ""
	for _, line := range lines {
		if !inPolicy {
			if i := strings.Index(line, "publicationPolicy:"); i >= 0 {
				inPolicy = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the policy block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		policy = append(policy, line)
	}
	block := strings.Join(policy, "\n")
	if strings.Contains(block, "controlPodUID") {
		t.Fatal("CRD manifest persists controlPodUID under publicationPolicy")
	}
	if !strings.Contains(block, "runUID") {
		t.Fatal("CRD manifest missing runUID under publicationPolicy")
	}
}

// TestCIRepairContextWire pins the persisted repair-context schema: the head
// and the attempt are always present, because a repair context without them
// cannot be attributed to a head or counted against the run's budget.
func TestCIRepairContextWire(t *testing.T) {
	raw, err := json.Marshal(&CIRepairContext{
		Head:         "abc123",
		Attempt:      2,
		FailedChecks: []string{"unit"},
		Logs:         "unit: 1 failed",
	})
	if err != nil {
		t.Fatalf("marshal repair context: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal repair context: %v", err)
	}
	for _, key := range []string{"head", "attempt", "failedChecks", "logs"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("repair context missing %s: %s", key, raw)
		}
	}
	bare, err := json.Marshal(&CIRepairContext{Head: "abc123", Attempt: 1})
	if err != nil {
		t.Fatalf("marshal bare repair context: %v", err)
	}
	if strings.Contains(string(bare), "failedChecks") || strings.Contains(string(bare), "logs") {
		t.Fatalf("bare repair context carries optional evidence: %s", bare)
	}
}

// TestCIRepairContextDeepCopyIndependence pins that a copied run owns its
// repair evidence: the failed-check list of a copy must not be shared with
// the run it was copied from, or a repair attempt would rewrite the evidence
// of the attempt it is repairing.
func TestCIRepairContextDeepCopyIndependence(t *testing.T) {
	run := &CoderRun{
		Status: CoderRunStatus{
			CIRepair: &CIRepairContext{
				Head:         "abc123",
				Attempt:      1,
				FailedChecks: []string{"unit", "lint"},
				Logs:         "unit: 1 failed",
			},
		},
	}
	copied := run.DeepCopy()
	copied.Status.CIRepair.FailedChecks[0] = "mutated"
	copied.Status.CIRepair.Head = "def456"
	if run.Status.CIRepair.FailedChecks[0] != "unit" {
		t.Fatalf("original failed checks mutated through the copy: %v", run.Status.CIRepair.FailedChecks)
	}
	if run.Status.CIRepair.Head != "abc123" {
		t.Fatalf("original head mutated through the copy: %q", run.Status.CIRepair.Head)
	}
}

// TestCRDManifestCIRepairStatusSchema pins the generated CoderRun CRD to the
// repair-context bounds: the head is never empty (a repair belongs to a
// head) and the attempt is 1-based (a repair context never describes the
// zeroth attempt).
func TestCRDManifestCIRepairStatusSchema(t *testing.T) {
	manifest, err := os.ReadFile("../../charts/courier/crd-manifests/courier.misospace.dev_coderruns.yaml")
	if err != nil {
		t.Fatalf("read CRD manifest: %v", err)
	}
	lines := strings.Split(string(manifest), "\n")
	var block []string
	inRepair := false
	indent := ""
	for _, line := range lines {
		if !inRepair {
			if i := strings.Index(line, "ciRepair:"); i >= 0 {
				inRepair = true
				indent = line[:i]
			}
			continue
		}
		// A sibling key at the same indent ends the repair-context block.
		if strings.HasPrefix(line, indent) && len(line) > len(indent) && line[len(indent)] != ' ' {
			break
		}
		block = append(block, line)
	}
	if !inRepair {
		t.Fatal("CRD manifest has no status.ciRepair schema")
	}
	text := strings.Join(block, "\n")
	for _, want := range []string{"head:", "minLength: 1", "attempt:", "minimum: 1", "failedChecks:", "logs:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status.ciRepair schema missing %q:\n%s", want, text)
		}
	}
}

// TestNativeResultReasonContract pins the durable-reason contract (#127):
// every trusted kind carries a fixed, non-empty, code-owned reason within the
// shared MaxNativeResultReasonBytes bound, an unknown kind carries none, and
// the bound equals the kubebuilder MaxLength on the CRD field — one contract
// the CRD, the broker writer, and the control binary's persistence all share.
func TestNativeResultReasonContract(t *testing.T) {
	if MaxNativeResultReasonBytes != 1024 {
		t.Fatalf("MaxNativeResultReasonBytes = %d, want 1024: the kubebuilder MaxLength on NativeResult.Reason is its validated copy and they must move together", MaxNativeResultReasonBytes)
	}
	kinds := []NativeResultKind{
		NativeResultPublished,
		NativeResultNoChange,
		NativeResultNeedsHuman,
		NativeResultBlockedExternal,
		NativeResultCapacityExhausted,
		NativeResultWorkloadFailed,
		NativeResultInfrastructureFailed,
	}
	for _, kind := range kinds {
		reason := NativeResultReason(kind)
		if reason == "" {
			t.Fatalf("%q has no durable reason; every trusted kind carries its fixed code-owned label", kind)
		}
		if len(reason) > MaxNativeResultReasonBytes {
			t.Fatalf("%q reason is %d bytes, over the shared %d-byte durable bound", kind, len(reason), MaxNativeResultReasonBytes)
		}
		if again := NativeResultReason(kind); again != reason {
			t.Fatalf("%q reason is not stable: %q then %q", kind, reason, again)
		}
	}
	if NativeResultReason(NativeResultKind("genuinely_done")) != "" {
		t.Fatal("an unknown kind produced a durable reason")
	}
	if NativeResultReason("") != "" {
		t.Fatal("the empty kind produced a durable reason")
	}
}

// specFixture returns a valid spec for the given mode: resolve-issue/fix-pr
// carry a ref and no workRepo; contribute carries a workRepo, no ref, and a
// goal-bearing SourceContext.
func specFixture(mode Mode) CoderRunSpec {
	spec := CoderRunSpec{
		Mode:          mode,
		Source:        "cli",
		SourceContext: `{"goal":"add a security section to the README"}`,
		WorkItemID:    "adhoc-0123456789ab",
		Repo:          "acme/upstream",
		Lane:          "local",
	}
	switch mode {
	case ModeContribute:
		spec.WorkRepo = "contributor/fork"
	default:
		spec.Ref = 7
	}
	return spec
}

// TestCoderRunSpecModeValidation enforces the mode-dependent spec rules
// through the API server: a contribute run needs base repo, workRepo, and a
// goal-bearing SourceContext and must not carry a ref; resolve-issue and
// fix-pr validation is unchanged (ref required, no workRepo).
func TestCoderRunSpecModeValidation(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	cases := []struct {
		name    string
		spec    CoderRunSpec
		wantErr bool
	}{
		{"contribute accepted", specFixture(ModeContribute), false},
		{"contribute without workRepo rejected", func() CoderRunSpec {
			s := specFixture(ModeContribute)
			s.WorkRepo = ""
			return s
		}(), true},
		{"contribute with ref rejected", func() CoderRunSpec {
			s := specFixture(ModeContribute)
			s.Ref = 12
			return s
		}(), true},
		{"contribute with malformed workRepo rejected", func() CoderRunSpec {
			s := specFixture(ModeContribute)
			s.WorkRepo = "not-a-repo"
			return s
		}(), true},
		{"resolve-issue accepted", specFixture(ModeResolveIssue), false},
		{"resolve-issue without ref rejected", func() CoderRunSpec {
			s := specFixture(ModeResolveIssue)
			s.Ref = 0
			return s
		}(), true},
		{"resolve-issue with workRepo rejected", func() CoderRunSpec {
			s := specFixture(ModeResolveIssue)
			s.WorkRepo = "contributor/fork"
			return s
		}(), true},
		{"fix-pr accepted", specFixture(ModeFixPR), false},
		{"fix-pr without ref rejected", func() CoderRunSpec {
			s := specFixture(ModeFixPR)
			s.Ref = 0
			return s
		}(), true},
		{"fix-pr with workRepo rejected", func() CoderRunSpec {
			s := specFixture(ModeFixPR)
			s.WorkRepo = "contributor/fork"
			return s
		}(), true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := &CoderRun{}
			run.Name = fmt.Sprintf("mode-validation-%d", i)
			run.Namespace = "default"
			run.Spec = tc.spec
			err := envtestClient.Create(ctx, run)
			if tc.wantErr && err == nil {
				t.Fatalf("accepted %s, want rejection", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("rejected %s: %v", tc.name, err)
			}
		})
	}
}

// TestCoderRunSpecContributeRoundTrip pins that an accepted contribute run
// persists exactly the admitted identity: base repo, work fork, goal-bearing
// SourceContext, and no ref.
func TestCoderRunSpecContributeRoundTrip(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	run := &CoderRun{}
	run.Name = "contribute-round-trip"
	run.Namespace = "default"
	run.Spec = specFixture(ModeContribute)
	if err := envtestClient.Create(ctx, run); err != nil {
		t.Fatalf("create contribute run: %v", err)
	}
	var got CoderRun
	key := client.ObjectKeyFromObject(run)
	if err := envtestClient.Get(ctx, key, &got); err != nil {
		t.Fatalf("get contribute run: %v", err)
	}
	if got.Spec.Repo != "acme/upstream" || got.Spec.WorkRepo != "contributor/fork" {
		t.Fatalf("contribute run lost repo/workRepo: %+v", got.Spec)
	}
	if got.Spec.Ref != 0 {
		t.Fatalf("contribute run carries ref %d", got.Spec.Ref)
	}
	if !strings.Contains(got.Spec.SourceContext, "goal") {
		t.Fatalf("contribute run lost goal-bearing SourceContext: %q", got.Spec.SourceContext)
	}
}

// TestCoderRunSpecOptionalFieldsWire pins the wire contract the mode rules
// depend on: unset ref and workRepo are absent from the serialized spec, so
// Go writers cannot smuggle a zero-valued ref into a contribute run or an
// empty workRepo into a resolve-issue run.
func TestCoderRunSpecOptionalFieldsWire(t *testing.T) {
	contribute, err := json.Marshal(specFixture(ModeContribute))
	if err != nil {
		t.Fatalf("marshal contribute spec: %v", err)
	}
	if strings.Contains(string(contribute), `"ref"`) {
		t.Fatalf("contribute spec serializes ref: %s", contribute)
	}
	if !strings.Contains(string(contribute), `"workRepo"`) {
		t.Fatalf("contribute spec omits workRepo: %s", contribute)
	}
	resolve, err := json.Marshal(specFixture(ModeResolveIssue))
	if err != nil {
		t.Fatalf("marshal resolve-issue spec: %v", err)
	}
	if strings.Contains(string(resolve), `"workRepo"`) {
		t.Fatalf("resolve-issue spec serializes workRepo: %s", resolve)
	}
	if !strings.Contains(string(resolve), `"ref"`) {
		t.Fatalf("resolve-issue spec omits ref: %s", resolve)
	}
}

// TestCoderRunSpecExplicitZeroRefRejected pins the wire-level bound an
// omitempty typed writer cannot express: a submitted spec.ref of 0 is
// rejected by the minimum, so a contribute run never distinguishes "zero"
// from "absent" in stored state.
func TestCoderRunSpecExplicitZeroRefRejected(t *testing.T) {
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	for _, mode := range []Mode{ModeContribute, ModeResolveIssue} {
		t.Run(string(mode), func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": GroupVersion.String(),
				"kind":       "CoderRun",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("explicit-zero-ref-%s", mode),
					"namespace": "default",
				},
				"spec": map[string]any{
					"mode":       string(mode),
					"source":     "cli",
					"workItemID": "adhoc-0123456789ab",
					"repo":       "acme/upstream",
					"lane":       "local",
					"ref":        int64(0),
				},
			}}
			if mode == ModeContribute {
				unstructured.SetNestedField(obj.Object, "contributor/fork", "spec", "workRepo")
			}
			if err := envtestClient.Create(ctx, obj); err == nil {
				t.Fatalf("accepted explicit spec.ref: 0 for %s mode", mode)
			}
		})
	}
}
