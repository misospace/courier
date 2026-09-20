package status

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

type recordedPatch struct {
	name    types.NamespacedName
	patch   []byte
	manager string
}

type fakePatcher struct {
	patches []recordedPatch
	err     error
}

func (f *fakePatcher) PatchStatus(_ context.Context, name types.NamespacedName, patch []byte, manager string) error {
	f.patches = append(f.patches, recordedPatch{
		name:    name,
		patch:   append([]byte(nil), patch...),
		manager: manager,
	})
	return f.err
}

func TestStatusWritersUseSeparateManagersAndOwnedFields(t *testing.T) {
	patcher := &fakePatcher{}
	name := types.NamespacedName{Namespace: "default", Name: "run"}
	operator := NewOperatorWriter(patcher)
	harness := NewHarnessWriter(patcher)

	phase := courierv1alpha1.PhaseRunning
	branch := "courier/issue-7"
	if err := operator.Patch(context.Background(), name, OperatorPatch{
		Phase:      phase,
		Branch:     &branch,
		PR:         "#42",
		Restarts:   2,
		Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
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
	if got := patcher.patches[0].manager; got != OperatorFieldManager {
		t.Fatalf("operator manager = %q, want %q", got, OperatorFieldManager)
	}
	if got := patcher.patches[1].manager; got != HarnessFieldManager {
		t.Fatalf("harness manager = %q, want %q", got, HarnessFieldManager)
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
	for _, field := range []string{"phase", "branch", "pr", "restarts", "conditions"} {
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
	for _, field := range []string{"phase", "branch", "pr", "restarts", "conditions", "heartbeat"} {
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
	for _, field := range []string{"phase", "branch", "pr", "restarts", "conditions"} {
		if _, ok := payload.Status[field]; ok {
			t.Errorf("heartbeat patch unexpectedly contains operator field %q", field)
		}
	}
}

func TestMarshalPatchIncludesObjectIdentity(t *testing.T) {
	patch, err := marshalPatch(types.NamespacedName{Namespace: "default", Name: "run"}, HarnessPatch{})
	if err != nil {
		t.Fatalf("marshal patch: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(patch, &got); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if got["apiVersion"] != courierv1alpha1.GroupVersion.String() || got["kind"] != "CoderRun" {
		t.Fatalf("identity = %#v", got)
	}
	metadata := got["metadata"].(map[string]interface{})
	if !reflect.DeepEqual(metadata, map[string]interface{}{"name": "run", "namespace": "default"}) {
		t.Fatalf("metadata = %#v", metadata)
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
