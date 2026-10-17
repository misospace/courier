package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// longRunName is a valid DNS-subdomain CoderRun name that overflows the
// 63-character Kubernetes label-value cap.
var longRunName = strings.Repeat("a", 63) + "-overflow"

func TestPodBelongsToRunMatchesBoundedRunLabel(t *testing.T) {
	run := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: longRunName, Namespace: "runs", UID: "run-uid"}}
	if topology.RunLabelValue(longRunName) == longRunName {
		t.Fatalf("test precondition: %q must overflow the label cap", longRunName)
	}
	tests := []struct {
		name  string
		pod   *corev1.Pod
		match bool
	}{
		{
			name: "topology control pod carries the bounded label",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "runs", Labels: map[string]string{executor.LabelRun: topology.RunLabelValue(longRunName)},
			}},
			match: true,
		},
		{
			name: "executor pod carries the bounded executor label",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "runs", Labels: map[string]string{executor.LabelRun: executor.RunLabelValue(longRunName)},
			}},
			match: true,
		},
		{
			name: "raw run name never matches",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "runs", Labels: map[string]string{executor.LabelRun: longRunName},
			}},
		},
		{
			name: "another run's bounded label never matches",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "runs", Labels: map[string]string{executor.LabelRun: topology.RunLabelValue("other-run")},
			}},
		},
		{
			name: "owner reference still decides without a label",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "runs",
				OwnerReferences: []metav1.OwnerReference{{Kind: "CoderRun", Name: longRunName, Controller: boolPtr(true)}},
			}},
			match: true,
		},
		{
			name: "other namespace never matches",
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "elsewhere", Labels: map[string]string{executor.LabelRun: topology.RunLabelValue(longRunName)},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podBelongsToRun(tt.pod, run); got != tt.match {
				t.Fatalf("podBelongsToRun = %v; want %v", got, tt.match)
			}
		})
	}
}

func TestPodOfComponentMatchesBoundedRunLabel(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	run := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: longRunName, Namespace: "runs", UID: "run-uid"}}
	pods := []ctrlclient.Object{
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "foreign", Namespace: "runs", Labels: map[string]string{topology.LabelRun: "other-run", "courier.misospace.dev/component": topology.ComponentCoordinator},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: longRunName + "-control", Namespace: "runs", Labels: map[string]string{
				topology.LabelRun: topology.RunLabelValue(longRunName), "courier.misospace.dev/component": topology.ComponentCoordinator,
			},
		}},
	}
	client := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(pods...).Build()
	pod, err := podOfComponent(context.Background(), client, run, topology.ComponentCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	if pod == nil || pod.Name != longRunName+"-control" {
		t.Fatalf("podOfComponent = %+v; want the run's control pod", pod)
	}
	brokerPod, err := podOfComponent(context.Background(), client, run, topology.ComponentBroker)
	if err != nil {
		t.Fatal(err)
	}
	if brokerPod != nil {
		t.Fatalf("podOfComponent found a broker pod for a run that renders none: %+v", brokerPod)
	}
}

func TestAppendOtherRunLabelSchemePods(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := func(name, label string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "runs", Labels: map[string]string{executor.LabelRun: label}}}
	}
	tests := []struct {
		name     string
		runName  string
		pods     []*corev1.Pod
		wantList []string
	}{
		{
			name:     "short name: both schemes agree, single list suffices",
			runName:  "short",
			pods:     []*corev1.Pod{pod("a", executor.RunLabelValue("short"))},
			wantList: []string{"a"},
		},
		{
			name:    "long name: both label schemes are listed",
			runName: longRunName,
			pods: []*corev1.Pod{
				pod("legacy", executor.RunLabelValue(longRunName)),
				pod("topology", topology.RunLabelValue(longRunName)),
				pod("foreign", topology.RunLabelValue("other-run")),
			},
			wantList: []string{"legacy", "topology"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := make([]ctrlclient.Object, 0, len(tt.pods))
			for _, p := range tt.pods {
				objects = append(objects, p)
			}
			lists := 0
			client := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
					lists++
					return c.List(ctx, list, opts...)
				},
			}).Build()
			run := &courier.CoderRun{ObjectMeta: metav1.ObjectMeta{Name: tt.runName, Namespace: "runs", UID: "run-uid"}}
			var list corev1.PodList
			if err := client.List(context.Background(), &list,
				ctrlclient.InNamespace("runs"),
				ctrlclient.MatchingLabels{executor.LabelRun: executor.RunLabelValue(tt.runName)},
			); err != nil {
				t.Fatal(err)
			}
			if err := appendOtherRunLabelSchemePods(context.Background(), client, run, &list); err != nil {
				t.Fatal(err)
			}
			// One list when both schemes agree; a second only for names the
			// schemes render differently.
			wantLists := 1
			if topology.RunLabelValue(tt.runName) != executor.RunLabelValue(tt.runName) {
				wantLists = 2
			}
			if lists != wantLists {
				t.Fatalf("pod list reads = %d; want %d", lists, wantLists)
			}
			got := map[string]bool{}
			for i := range list.Items {
				got[list.Items[i].Name] = true
			}
			for _, want := range tt.wantList {
				if !got[want] {
					t.Fatalf("list misses pod %q: got %v", want, got)
				}
			}
			if len(got) != len(tt.wantList) {
				t.Fatalf("list = %v; want exactly %v", got, tt.wantList)
			}
		})
	}
}
