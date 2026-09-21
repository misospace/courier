package status

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

type recordedPatch struct {
	name  types.NamespacedName
	patch []byte
}

type fakePatcher struct {
	patches []recordedPatch
	err     error
}

func (f *fakePatcher) PatchStatus(_ context.Context, name types.NamespacedName, patch []byte) error {
	f.patches = append(f.patches, recordedPatch{
		name:  name,
		patch: append([]byte(nil), patch...),
	})
	return f.err
}

func TestStatusWritersOwnDisjointFields(t *testing.T) {
	patcher := &fakePatcher{}
	name := types.NamespacedName{Namespace: "default", Name: "run"}
	operator := NewOperatorWriter(patcher)
	harness := NewHarnessWriter(patcher)

	phase := courierv1alpha1.PhaseRunning
	branch := "courier/issue-7"
	fingerprint := "observed-check-set"
	if err := operator.Patch(context.Background(), name, OperatorPatch{
		Phase:            phase,
		Branch:           &branch,
		PR:               "#42",
		CheckFingerprint: &fingerprint,
		Restarts:         2,
		Conditions:       []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
	}); err != nil {
		t.Fatalf("operator patch: %v", err)
	}
	checkpoint := &courierv1alpha1.Checkpoint{Plan: "finish the work"}
	if err := harness.Patch(context.Background(), name, HarnessPatch{
		Checkpoint: checkpoint,
		LastCommit: "abc123",
	}); err != nil {
		t.Fatalf("harness patch: %v", err)
	}

	if len(patcher.patches) != 2 {
		t.Fatalf("patch count = %d, want 2", len(patcher.patches))
	}

	var operatorPayload map[string]interface{}
	if err := json.Unmarshal(patcher.patches[0].patch, &operatorPayload); err != nil {
		t.Fatalf("decode operator patch: %v", err)
	}
	var harnessPayload map[string]interface{}
	if err := json.Unmarshal(patcher.patches[1].patch, &harnessPayload); err != nil {
		t.Fatalf("decode harness patch: %v", err)
	}
	operatorStatus := operatorPayload["status"].(map[string]interface{})
	harnessStatus := harnessPayload["status"].(map[string]interface{})
	for _, field := range []string{"phase", "branch", "pr", "checkFingerprint", "restarts", "conditions"} {
		if _, ok := operatorStatus[field]; !ok {
			t.Errorf("operator status missing owned field %q", field)
		}
	}
	for _, field := range []string{"checkpoint", "heartbeat", "lastCommit"} {
		if _, ok := operatorStatus[field]; ok {
			t.Errorf("operator patch unexpectedly contains harness field %q", field)
		}
	}
	if _, ok := harnessStatus["checkpoint"]; !ok {
		t.Error("harness patch missing checkpoint")
	}
	if _, ok := harnessStatus["lastCommit"]; !ok {
		t.Error("harness patch missing lastCommit")
	}
	for _, field := range []string{"phase", "branch", "pr", "checkFingerprint", "restarts", "conditions", "heartbeat"} {
		if _, ok := harnessStatus[field]; ok {
			t.Errorf("harness patch unexpectedly contains field %q", field)
		}
	}
}

func TestHeartbeatCoalescesSuccessfulActivity(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	patcher := &fakePatcher{}
	name := types.NamespacedName{Namespace: "default", Name: "run"}
	reporter := NewHeartbeat(NewHarnessWriter(patcher), name, clock, 10*time.Second)

	if err := reporter.Record(context.Background(), "stream"); err != nil {
		t.Fatalf("first activity: %v", err)
	}
	clock.now = clock.now.Add(2 * time.Second)
	if err := reporter.Record(context.Background(), "tool"); err != nil {
		t.Fatalf("coalesced activity: %v", err)
	}
	if got := len(patcher.patches); got != 1 {
		t.Fatalf("patches after coalesced activity = %d, want 1", got)
	}

	clock.now = clock.now.Add(8 * time.Second)
	if err := reporter.Record(context.Background(), "tool"); err != nil {
		t.Fatalf("cadence activity: %v", err)
	}
	if got := len(patcher.patches); got != 2 {
		t.Fatalf("patches after cadence = %d, want 2", got)
	}
	if got := decodeHeartbeat(t, patcher.patches[1].patch); got.Kind != "tool" || !got.At.Time.Equal(clock.now) {
		t.Fatalf("heartbeat = %#v, want latest tool activity at %v", got, clock.now)
	}
}

func TestHeartbeatRetriesFailedWriteAndDoesNotAdvanceClock(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	writeErr := errors.New("status conflict")
	patcher := &fakePatcher{err: writeErr}
	reporter := NewHeartbeat(NewHarnessWriter(patcher), types.NamespacedName{Name: "run"}, clock, time.Second)

	if err := reporter.Record(context.Background(), "stream"); !errors.Is(err, writeErr) {
		t.Fatalf("first error = %v, want %v", err, writeErr)
	}
	patcher.err = nil
	clock.now = clock.now.Add(100 * time.Millisecond)
	if err := reporter.Record(context.Background(), "tool"); err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if got := len(patcher.patches); got != 2 {
		t.Fatalf("patches = %d, want failed attempt plus retry", got)
	}
}

func TestHeartbeatPatchPreservesOperatorFieldsByConstruction(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	patcher := &fakePatcher{}
	reporter := NewHeartbeat(NewHarnessWriter(patcher), types.NamespacedName{Name: "run"}, clock, time.Second)

	if err := reporter.Record(context.Background(), "stream"); err != nil {
		t.Fatalf("activity: %v", err)
	}
	var payload struct {
		Status map[string]json.RawMessage `json:"status"`
	}
	if err := json.Unmarshal(patcher.patches[0].patch, &payload); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if _, ok := payload.Status["checkpoint"]; ok {
		t.Error("heartbeat patch unexpectedly contains checkpoint")
	}
	for _, field := range []string{"phase", "branch", "pr", "checkFingerprint", "restarts", "conditions"} {
		if _, ok := payload.Status[field]; ok {
			t.Errorf("heartbeat patch unexpectedly contains operator field %q", field)
		}
	}
}

func TestMarshalPatchCarriesOnlyStatus(t *testing.T) {
	patch, err := marshalPatch(types.NamespacedName{Namespace: "default", Name: "run"}, HarnessPatch{})
	if err != nil {
		t.Fatalf("marshal patch: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(patch, &got); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if !reflect.DeepEqual(got, map[string]interface{}{"status": map[string]interface{}{}}) {
		t.Fatalf("patch = %#v, want a status-only document", got)
	}
}

// statusClient builds a fake client whose merge-patch handling mirrors the
// status subresource, seeded with one CoderRun.
func statusClient(t *testing.T, run *courierv1alpha1.CoderRun) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Courier scheme: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&courierv1alpha1.CoderRun{}).
		WithRuntimeObjects(run).
		Build()
}

func statusTestRun() *courierv1alpha1.CoderRun {
	return &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"},
		Status: courierv1alpha1.CoderRunStatus{
			Phase:      courierv1alpha1.PhaseClaimed,
			Checkpoint: &courierv1alpha1.Checkpoint{Plan: "seeded checkpoint"},
		},
	}
}

func TestSequentialOperatorWritesPreserveOtherFields(t *testing.T) {
	name := types.NamespacedName{Namespace: "default", Name: "run"}
	kube := statusClient(t, statusTestRun())
	writer := NewOperatorWriter(KubePatchWriter{Client: kube})

	if err := writer.Patch(context.Background(), name, OperatorPatch{Phase: courierv1alpha1.PhaseRunning}); err != nil {
		t.Fatalf("phase write: %v", err)
	}
	branch := "courier/issue-7"
	if err := writer.Patch(context.Background(), name, OperatorPatch{Branch: &branch}); err != nil {
		t.Fatalf("branch write: %v", err)
	}
	fingerprint := "observed-check-set"
	if err := writer.Patch(context.Background(), name, OperatorPatch{CheckFingerprint: &fingerprint}); err != nil {
		t.Fatalf("fingerprint write: %v", err)
	}
	// The pointer form must let a reshaped check set clear stale evidence.
	cleared := ""
	if err := writer.Patch(context.Background(), name, OperatorPatch{CheckFingerprint: &cleared}); err != nil {
		t.Fatalf("fingerprint clear: %v", err)
	}

	var run courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), name, &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Phase != courierv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running preserved after the branch write", run.Status.Phase)
	}
	if run.Status.Branch != branch {
		t.Fatalf("branch = %q, want %q", run.Status.Branch, branch)
	}
	if run.Status.CheckFingerprint != "" {
		t.Fatalf("checkFingerprint = %q, want cleared by the empty pointer write", run.Status.CheckFingerprint)
	}
	if run.Status.Checkpoint == nil || run.Status.Checkpoint.Plan != "seeded checkpoint" {
		t.Fatalf("harness checkpoint was disturbed by operator writes: %#v", run.Status.Checkpoint)
	}
}

func TestSequentialHarnessWritesPreserveOtherFields(t *testing.T) {
	name := types.NamespacedName{Namespace: "default", Name: "run"}
	kube := statusClient(t, statusTestRun())
	writer := NewHarnessWriter(KubePatchWriter{Client: kube})

	if err := writer.Checkpoint(context.Background(), name, &courierv1alpha1.Checkpoint{Plan: "revised plan"}); err != nil {
		t.Fatalf("checkpoint write: %v", err)
	}
	if err := writer.Heartbeat(context.Background(), name, &courierv1alpha1.Heartbeat{Kind: "tool"}); err != nil {
		t.Fatalf("heartbeat write: %v", err)
	}

	var run courierv1alpha1.CoderRun
	if err := kube.Get(context.Background(), name, &run); err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status.Checkpoint == nil || run.Status.Checkpoint.Plan != "revised plan" {
		t.Fatalf("checkpoint = %#v, want the revised plan preserved after the heartbeat write", run.Status.Checkpoint)
	}
	if run.Status.Heartbeat == nil || run.Status.Heartbeat.Kind != "tool" {
		t.Fatalf("heartbeat = %#v, want the tool heartbeat", run.Status.Heartbeat)
	}
	if run.Status.Phase != courierv1alpha1.PhaseClaimed {
		t.Fatalf("phase = %q, want Claimed preserved; operator fields must be untouched", run.Status.Phase)
	}
}

func decodeHeartbeat(t *testing.T, patch []byte) courierv1alpha1.Heartbeat {
	t.Helper()
	var payload struct {
		Status struct {
			Heartbeat courierv1alpha1.Heartbeat `json:"heartbeat"`
		} `json:"status"`
	}
	if err := json.Unmarshal(patch, &payload); err != nil {
		t.Fatalf("decode heartbeat patch: %v", err)
	}
	return payload.Status.Heartbeat
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }
