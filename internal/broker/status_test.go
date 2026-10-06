package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courier "github.com/misospace/courier/api/v1alpha1"
)

const (
	testNamespace = "runs"
	testRunName   = "run-a"
)

func statusFixture(t *testing.T) (*StatusWriter, client.Client, StatusIdentity) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courier.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	uid := types.UID("run-uid-a")
	podUID := types.UID("pod-uid-a")
	controller := true
	run := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testRunName, UID: uid}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "control-a", UID: podUID, Labels: map[string]string{"courier.misospace.dev/coderrun": testRunName, "courier.misospace.dev/component": "coordinator"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: courier.GroupVersion.String(), Kind: "CoderRun", Name: testRunName, UID: uid, Controller: &controller}}}, Spec: corev1.PodSpec{ServiceAccountName: "control-sa"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&courier.CoderRun{}).WithObjects(run, pod).Build()
	w, err := NewStatusWriter(c, c, testNamespace, testRunName)
	if err != nil {
		t.Fatal(err)
	}
	return w, c, StatusIdentity{RunUID: uid, ControlPod: pod.Name, ControlPodUID: podUID, ControlServiceAccount: "control-sa"}
}

func TestStatusWriterRejectsCrossRunIdentity(t *testing.T) {
	w, c, identity := statusFixture(t)
	other := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: "run-b", UID: "run-uid-b"}}
	if err := c.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	identity.RunUID = other.UID
	value := "sha"
	if err := w.Write(context.Background(), identity, HarnessPatch{LastCommit: &value}, nil); err == nil {
		t.Fatal("expected cross-run identity rejection")
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(other), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastCommit != "" {
		t.Fatalf("other run was modified: %#v", got.Status)
	}
}

func TestStatusWriterRequiresLiveValidatorForLastCommit(t *testing.T) {
	w, _, identity := statusFixture(t)
	value := "published-sha"
	if err := w.Write(context.Background(), identity, HarnessPatch{LastCommit: &value}, nil); err == nil {
		t.Fatal("expected lastCommit without live-world validator to be rejected")
	}
}

func TestStatusWriterRejectsUnlabeledDuplicateControlPod(t *testing.T) {
	w, c, identity := statusFixture(t)
	controller := true
	duplicate := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      "control-duplicate",
		UID:       "pod-uid-duplicate",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: courier.GroupVersion.String(),
			Kind:       "CoderRun",
			Name:       testRunName,
			UID:        identity.RunUID,
			Controller: &controller,
		}},
	}, Spec: corev1.PodSpec{ServiceAccountName: identity.ControlServiceAccount}}
	if err := c.Create(context.Background(), duplicate); err != nil {
		t.Fatal(err)
	}

	checkpoint := &courier.Checkpoint{Plan: "must-not-write"}
	if err := w.Write(context.Background(), identity, HarnessPatch{Checkpoint: checkpoint}, nil); err == nil {
		t.Fatal("expected unlabeled duplicate control pod to make identity ambiguous")
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Checkpoint != nil {
		t.Fatalf("ambiguous control pod identity modified run status: %#v", got.Status)
	}
}

func TestStatusWriterRejectsStaleControlPod(t *testing.T) {
	w, c, identity := statusFixture(t)
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: identity.ControlPod}, pod); err != nil {
		t.Fatal(err)
	}
	pod.UID = "replacement-pod-uid"
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	value := "sha"
	if err := w.Write(context.Background(), identity, HarnessPatch{LastCommit: &value}, nil); err == nil {
		t.Fatal("expected stale pod rejection")
	}
}

type conflictOnceClient struct {
	client.Client
	onConflict func(context.Context) error
	conflicted bool
}

func (c *conflictOnceClient) Status() client.SubResourceWriter {
	return conflictOnceStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type conflictOnceStatusWriter struct {
	client.SubResourceWriter
	parent *conflictOnceClient
}

func (w conflictOnceStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if !w.parent.conflicted {
		w.parent.conflicted = true
		if err := w.parent.onConflict(ctx); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Group: courier.GroupVersion.Group, Resource: "coderruns"}, testRunName, errors.New("simulated concurrent status write"))
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestStatusWriterRetriesCASAndRevalidatesLiveWorld(t *testing.T) {
	w, c, identity := statusFixture(t)
	conditions := []metav1.Condition{{Type: "operator", Status: metav1.ConditionTrue, Reason: "concurrent"}}
	injected := false
	wrapped := &conflictOnceClient{Client: c, onConflict: func(ctx context.Context) error {
		live := &courier.CoderRun{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: testRunName}, live); err != nil {
			return err
		}
		live.Status.Conditions = conditions
		live.Status.Phase = courier.PhaseRunning
		if err := c.Status().Update(ctx, live); err != nil {
			return err
		}
		injected = true
		return nil
	}}
	w.writer = wrapped
	lastCommit := "published-sha"
	validated := 0
	validate := func(_ context.Context, run *courier.CoderRun, _ *corev1.Pod, patch HarnessPatch) error {
		validated++
		if patch.LastCommit == nil || *patch.LastCommit != lastCommit {
			t.Fatal("callback received unexpected patch")
		}
		if injected && run.Status.Phase != courier.PhaseRunning {
			t.Fatalf("retry callback did not see concurrent phase: %q", run.Status.Phase)
		}
		return nil
	}
	if err := w.Write(context.Background(), identity, HarnessPatch{LastCommit: &lastCommit}, validate); err != nil {
		t.Fatal(err)
	}
	if validated < 2 {
		t.Fatalf("validation calls = %d, want revalidation after conflict", validated)
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastCommit != lastCommit || got.Status.Phase != courier.PhaseRunning || len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != "operator" {
		t.Fatalf("concurrent fields lost or patch missing: %#v", got.Status)
	}
}

func TestStatusWriterOnlyChangesRequestedHarnessFields(t *testing.T) {
	w, c, identity := statusFixture(t)
	live := &courier.CoderRun{}
	key := client.ObjectKey{Namespace: testNamespace, Name: testRunName}
	if err := c.Get(context.Background(), key, live); err != nil {
		t.Fatal(err)
	}
	live.Status.Phase = courier.PhaseRunning
	live.Status.Branch = "operator-branch"
	live.Status.LastCommit = "old"
	live.Status.Checkpoint = &courier.Checkpoint{Plan: "existing"}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	checkpoint := &courier.Checkpoint{Plan: "new"}
	if err := w.Write(context.Background(), identity, HarnessPatch{Checkpoint: checkpoint}, nil); err != nil {
		t.Fatal(err)
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Checkpoint.Plan != "new" || got.Status.Phase != courier.PhaseRunning || got.Status.Branch != "operator-branch" || got.Status.LastCommit != "old" {
		t.Fatalf("status fields unexpectedly changed: %#v", got.Status)
	}
}

func activeOpFixture(opID string, coordinatorUID string) OperationAddition {
	return OperationAddition{
		OpID: opID,
		Operation: courier.ActiveOperation{
			BriefID:           "brief-1",
			CoordinatorPodUID: coordinatorUID,
			WorkerPodUID:      "worker-uid-a",
			DispatchedAt:      metav1.NewTime(metav1.Now().Rfc3339Copy().Time),
		},
	}
}

func TestStatusWriterAppliesAndClearsOperationEntries(t *testing.T) {
	w, c, identity := statusFixture(t)
	key := client.ObjectKey{Namespace: testNamespace, Name: testRunName}

	first := activeOpFixture("shell.brief-1.aaaa", string(identity.ControlPodUID))
	second := activeOpFixture("shell.brief-2.bbbb", string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &first}, nil); err != nil {
		t.Fatal(err)
	}
	// An identical re-add is idempotent: an uncertain acknowledgment is
	// resolved by re-sending the same entry.
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &first}, nil); err != nil {
		t.Fatalf("identical re-add refused: %v", err)
	}
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &second}, nil); err != nil {
		t.Fatal(err)
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.ActiveOperations) != 2 {
		t.Fatalf("activeOperations = %#v, want two entries", got.Status.ActiveOperations)
	}
	// Clearing one entry must never drop the other.
	if err := w.Write(context.Background(), identity, HarnessPatch{ClearOperation: first.OpID}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Status.ActiveOperations[second.OpID]; !ok {
		t.Fatalf("clearing %s dropped %s: %#v", first.OpID, second.OpID, got.Status.ActiveOperations)
	}
	if _, ok := got.Status.ActiveOperations[first.OpID]; ok {
		t.Fatalf("cleared entry %s survived: %#v", first.OpID, got.Status.ActiveOperations)
	}
	// Clearing an absent ID succeeds: clears are retried idempotently.
	if err := w.Write(context.Background(), identity, HarnessPatch{ClearOperation: first.OpID}, nil); err != nil {
		t.Fatalf("idempotent clear refused: %v", err)
	}
}

func TestStatusWriterRefusesConflictingReAdd(t *testing.T) {
	w, c, identity := statusFixture(t)
	addition := activeOpFixture("shell.brief-1.aaaa", string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &addition}, nil); err != nil {
		t.Fatal(err)
	}
	conflicting := addition
	conflicting.Operation.WorkerPodUID = "worker-uid-other"
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &conflicting}, nil); err == nil {
		t.Fatal("expected a conflicting re-add of the same operation ID to be refused")
	}
	got := &courier.CoderRun{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ActiveOperations[addition.OpID].WorkerPodUID != "worker-uid-a" {
		t.Fatalf("conflicting re-add overwrote dispatch evidence: %#v", got.Status.ActiveOperations)
	}
}

func TestStatusWriterFencesOperationAndHeartbeatIdentity(t *testing.T) {
	w, _, identity := statusFixture(t)
	foreign := activeOpFixture("shell.brief-1.aaaa", "pod-uid-previous-incarnation")
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &foreign}, nil); err == nil {
		t.Fatal("expected an operation attributing itself to another control incarnation to be refused")
	}
	emptyWorker := activeOpFixture("shell.brief-1.aaaa", string(identity.ControlPodUID))
	emptyWorker.Operation.WorkerPodUID = ""
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &emptyWorker}, nil); err == nil {
		t.Fatal("expected an operation without a worker UID to be refused")
	}
	malformed := activeOpFixture("shell/brief-1/aaaa", string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &malformed}, nil); err == nil {
		t.Fatal("expected a malformed operation ID to be refused")
	}
	foreignHeartbeat := &courier.Heartbeat{At: metav1.Now(), Kind: "tool", CoordinatorPodUID: "pod-uid-previous"}
	if err := w.Write(context.Background(), identity, HarnessPatch{Heartbeat: foreignHeartbeat}, nil); err == nil {
		t.Fatal("expected a heartbeat for another control incarnation to be refused")
	}
	unattributed := &courier.Heartbeat{At: metav1.Now(), Kind: "tool"}
	if err := w.Write(context.Background(), identity, HarnessPatch{Heartbeat: unattributed}, nil); err == nil {
		t.Fatal("expected an unattributed heartbeat to be refused")
	}
}

func TestStatusWriterReconcileOperationsDropsEveryEntry(t *testing.T) {
	w, c, identity := statusFixture(t)
	key := client.ObjectKey{Namespace: testNamespace, Name: testRunName}
	own := activeOpFixture("shell.brief-1.aaaa", string(identity.ControlPodUID))
	stale := activeOpFixture("shell.brief-0.0000", "pod-uid-previous-incarnation")
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &own}, nil); err != nil {
		t.Fatal(err)
	}
	// The stale-incarnation entry cannot be added through the fence; write it
	// directly to model a set persisted by a previous incarnation.
	live := &courier.CoderRun{}
	if err := c.Get(context.Background(), key, live); err != nil {
		t.Fatal(err)
	}
	live.Status.ActiveOperations = map[string]courier.ActiveOperation{
		own.OpID:   own.Operation,
		stale.OpID: stale.Operation,
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), identity, HarnessPatch{ReconcileOperations: true}, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, live); err != nil {
		t.Fatal(err)
	}
	if len(live.Status.ActiveOperations) != 0 {
		t.Fatalf("reconcile left entries behind: %#v", live.Status.ActiveOperations)
	}
}

func TestStatusWriterBoundsOperationSet(t *testing.T) {
	w, _, identity := statusFixture(t)
	for i := 0; i < maxActiveOperations; i++ {
		addition := activeOpIDFixture(i, string(identity.ControlPodUID))
		if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &addition}, nil); err != nil {
			t.Fatalf("entry %d refused: %v", i, err)
		}
	}
	overflow := activeOpIDFixture(maxActiveOperations, string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{AddOperation: &overflow}, nil); err == nil {
		t.Fatal("expected the bounded set to refuse another entry")
	}
}

func activeOpIDFixture(i int, coordinatorUID string) OperationAddition {
	return OperationAddition{
		OpID: "shell.brief-" + fmt.Sprint(i) + ".aaaa",
		Operation: courier.ActiveOperation{
			BriefID:           "brief-1",
			CoordinatorPodUID: coordinatorUID,
			WorkerPodUID:      "worker-uid-a",
			DispatchedAt:      metav1.NewTime(metav1.Now().Rfc3339Copy().Time),
		},
	}
}

// failingListReader fails every pod list, standing in for an API server
// that cannot answer the exactly-one-live-control scan.
type failingListReader struct {
	client.Client
}

func (failingListReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("api server unavailable")
}
