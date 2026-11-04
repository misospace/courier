package broker

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
)

func validCloseFixture(coordinatorUID string) *courier.NoChangeClose {
	return &courier.NoChangeClose{
		Criteria: []courier.NoChangeCloseCriterion{{
			Criterion: "the documented flag exists",
			Path:      "docs/flags.md",
			Excerpt:   "an excerpt of at least twenty-four bytes long",
		}},
		Evidence:          "the ask is already satisfied by existing work",
		CoordinatorPodUID: coordinatorUID,
		At:                metav1.NewTime(metav1.Now().Rfc3339Copy().Time),
	}
}

// TestStatusWriterAppliesNoChangeClose proves the grounded close proposal
// (#720) lands on the run's status through the same fenced write as every
// other harness field, standalone or beside a terminal result, and that the
// idempotent retry of the identical write succeeds — a lost acknowledgment
// must resolve, not deny.
func TestStatusWriterAppliesNoChangeClose(t *testing.T) {
	w, c, identity := statusFixture(t)
	proposal := validCloseFixture(string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: proposal}, nil); err != nil {
		t.Fatalf("write close proposal: %v", err)
	}
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: proposal.DeepCopy()}, nil); err != nil {
		t.Fatalf("idempotent rewrite of the identical proposal: %v", err)
	}
	var run courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, &run); err != nil {
		t.Fatal(err)
	}
	if run.Status.NoChange == nil || len(run.Status.NoChange.Criteria) != 1 ||
		run.Status.NoChange.Criteria[0].Path != "docs/flags.md" ||
		run.Status.NoChange.CoordinatorPodUID != string(identity.ControlPodUID) {
		t.Fatalf("persisted proposal = %#v, want the fenced grounded close", run.Status.NoChange)
	}
}

// TestStatusWriterNoChangeCloseIsSetOncePerIncarnation proves one control
// incarnation cannot record two different conclusions: a different proposal
// for the same incarnation is refused, and the recorded one stands.
func TestStatusWriterNoChangeCloseIsSetOncePerIncarnation(t *testing.T) {
	w, c, identity := statusFixture(t)
	first := validCloseFixture(string(identity.ControlPodUID))
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: first}, nil); err != nil {
		t.Fatalf("write first proposal: %v", err)
	}
	conflicting := validCloseFixture(string(identity.ControlPodUID))
	conflicting.Evidence = "a different conclusion for the same incarnation"
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: conflicting}, nil); err == nil {
		t.Fatal("conflicting proposal for the same incarnation was accepted")
	}
	var run courier.CoderRun
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: testRunName}, &run); err != nil {
		t.Fatal(err)
	}
	if run.Status.NoChange == nil || run.Status.NoChange.Evidence != first.Evidence {
		t.Fatalf("recorded proposal = %#v, want the first proposal left standing", run.Status.NoChange)
	}
}

// TestStatusWriterNoChangeCloseFencesIdentity proves the fence keeps a
// foreign or forged writer out and malformed proposals never reach the API:
// a UID that is not the authenticated incarnation, a missing time, an empty
// or over-full criterion set, and out-of-bound excerpt rows are each refused
// before any status write.
func TestStatusWriterNoChangeCloseFencesIdentity(t *testing.T) {
	w, _, identity := statusFixture(t)

	foreign := validCloseFixture("some-other-pod-uid")
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: foreign}, nil); err == nil {
		t.Fatal("proposal attributing itself to another incarnation was accepted")
	}

	untimed := validCloseFixture(string(identity.ControlPodUID))
	untimed.At = metav1.Time{}
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: untimed}, nil); err == nil {
		t.Fatal("proposal without a time was accepted")
	}

	empty := validCloseFixture(string(identity.ControlPodUID))
	empty.Criteria = nil
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: empty}, nil); err == nil {
		t.Fatal("proposal with no criteria was accepted")
	}

	overfull := validCloseFixture(string(identity.ControlPodUID))
	for i := 0; i < courier.MaxNoChangeCriteria; i++ {
		overfull.Criteria = append(overfull.Criteria, overfull.Criteria[0])
	}
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: overfull}, nil); err == nil {
		t.Fatal("proposal over the criterion bound was accepted")
	}

	shortExcerpt := validCloseFixture(string(identity.ControlPodUID))
	shortExcerpt.Criteria[0].Excerpt = "too short"
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: shortExcerpt}, nil); err == nil {
		t.Fatal("proposal with an under-bound excerpt was accepted")
	}

	longExcerpt := validCloseFixture(string(identity.ControlPodUID))
	longExcerpt.Criteria[0].Excerpt = strings.Repeat("x", courier.MaxNoChangeExcerptBytes+1)
	if err := w.Write(context.Background(), identity, HarnessPatch{NoChange: longExcerpt}, nil); err == nil {
		t.Fatal("proposal with an over-bound excerpt was accepted")
	}
}
