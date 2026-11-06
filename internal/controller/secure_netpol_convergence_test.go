package controller

import (
	"context"
	"sync"
	"testing"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// seedLaunchedSecureRun persists the run, its restricted run namespace, and
// the credential source Secret the launch pass copies, mirroring
// launchedSecureRun for a run the test built itself so policies can be seeded
// under the run's rendered names first.
func seedLaunchedSecureRun(t *testing.T, c client.Client, run *courier.CoderRun) {
	t.Helper()
	if err := c.Create(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   secureTestNamespace,
			Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "courier-system", Name: "forge-creds"},
		Data:       map[string][]byte{"token": []byte("tok-123")},
	}); err != nil {
		t.Fatal(err)
	}
}

func netpolTestRun(name string) *courier.CoderRun {
	run := secureRun(name)
	run.Spec.Lane = "local"
	run.Status.Phase = courier.PhaseClaimed
	run.Status.PublicationPolicy = &courier.PublicationPolicy{
		RunUID:              string(run.UID),
		ProviderConfigRef:   "github",
		ProviderEndpoint:    "https://api.github.com/",
		CredentialRefDigest: secureRegistry().Registrations()[0].Projection().Digest,
		BaseRepo:            "Acme/Widgets",
		BaseRef:             "main",
		BaseOID:             "baseoid",
		WorkRepo:            "Acme/Widgets",
		WorkRef:             "courier/Acme/Widgets/issue-7",
		WorkInitiallyAbsent: true,
	}
	return run
}

// TestLaunchSecureConvergesDriftedNetworkPolicy drives the #712 gap: a policy
// an older operator version rendered stays frozen at that rendering when
// provisioning only creates policies and never updates them, so a rendering
// change — the #711 control-ingress repair, a moved dependency-cache pin —
// reaches only newly provisioned runs. The launch pass must converge every
// existing policy to the spec the current rendering produces, in place: the
// object keeps its identity, so no moment passes in which the pods have no
// policy at all.
func TestLaunchSecureConvergesDriftedNetworkPolicy(t *testing.T) {
	ctx := context.Background()
	run := netpolTestRun("drifted-netpol")
	control, c := secureControl(t)
	// The drifted renderings stand for what an older operator version wrote:
	// the worker policy pins a cache egress the current pass renders away
	// (no cache Service is configured here), and the control policy carries
	// the pre-#711 deny-all ingress that left the control pod unable to
	// receive its broker's replies.
	drifted := map[string]func(*networkingv1.NetworkPolicy){
		topology.WorkerNetworkPolicyName(run.Name): func(p *networkingv1.NetworkPolicy) {
			p.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{
				To: []networkingv1.NetworkPolicyPeer{{
					IPBlock: &networkingv1.IPBlock{CIDR: "10.0.0.9/32"},
				}},
			}}
		},
		topology.ControlNetworkPolicyName(run.Name): func(p *networkingv1.NetworkPolicy) {
			p.Spec.Ingress = nil
		},
	}
	for _, rendered := range topology.NetworkPolicies(run, nil, nil) {
		drift, ok := drifted[rendered.Name]
		if !ok {
			continue
		}
		stale := rendered.DeepCopy()
		stale.Spec = *rendered.Spec.DeepCopy()
		drift(stale)
		if err := c.Create(ctx, stale); err != nil {
			t.Fatalf("seed drifted policy %s: %v", stale.Name, err)
		}
	}
	seeded := map[string]types.UID{}
	for _, rendered := range topology.NetworkPolicies(run, nil, nil) {
		if _, ok := drifted[rendered.Name]; !ok {
			continue
		}
		seededPolicy := &networkingv1.NetworkPolicy{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(rendered), seededPolicy); err != nil {
			t.Fatalf("read seeded policy %s: %v", rendered.Name, err)
		}
		seeded[rendered.Name] = seededPolicy.UID
	}
	seedLaunchedSecureRun(t, c, run)
	counter := &policyWriteCounter{Client: control.Client}
	control.Client = counter

	if _, err := control.LaunchSecure(ctx, run); err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if len(counter.deletes) != 0 {
		t.Fatalf("convergence must update in place, never delete: %v", counter.deletes)
	}
	for _, rendered := range topology.NetworkPolicies(run, nil, nil) {
		if _, ok := drifted[rendered.Name]; !ok {
			continue
		}
		live := &networkingv1.NetworkPolicy{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(rendered), live); err != nil {
			t.Fatalf("read policy %s: %v", rendered.Name, err)
		}
		if !apiequality.Semantic.DeepEqual(rendered.Spec, live.Spec) {
			t.Fatalf("policy %s did not converge to the rendered spec", rendered.Name)
		}
		if live.UID != seeded[rendered.Name] {
			t.Fatalf("policy %s was not updated in place: seeded UID %q != live UID %q", rendered.Name, seeded[rendered.Name], live.UID)
		}
	}
}

// policyWriteCounter records the writes the launch path issues against
// NetworkPolicies, so the tests can prove convergence updates in place and
// an unchanged policy is written not at all.
type policyWriteCounter struct {
	client.Client
	mu      sync.Mutex
	creates []string
	updates []string
	deletes []string
}

func (p *policyWriteCounter) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*networkingv1.NetworkPolicy); ok {
		p.mu.Lock()
		p.creates = append(p.creates, obj.GetName())
		p.mu.Unlock()
	}
	return p.Client.Create(ctx, obj, opts...)
}

func (p *policyWriteCounter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*networkingv1.NetworkPolicy); ok {
		p.mu.Lock()
		p.updates = append(p.updates, obj.GetName())
		p.mu.Unlock()
	}
	return p.Client.Update(ctx, obj, opts...)
}

func (p *policyWriteCounter) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*networkingv1.NetworkPolicy); ok {
		p.mu.Lock()
		p.deletes = append(p.deletes, obj.GetName())
		p.mu.Unlock()
	}
	return p.Client.Delete(ctx, obj, opts...)
}

// TestLaunchSecureLeavesMatchingNetworkPolicy proves the convergence reads
// before it writes: a live policy that already carries the rendered spec must
// survive an idempotent pass with no create, update, or delete against any
// policy, so the reconciliation cannot churn the API server on every requeue
// and cannot paper over a wrongly-reported NotFound with a create.
func TestLaunchSecureLeavesMatchingNetworkPolicy(t *testing.T) {
	ctx := context.Background()
	run := netpolTestRun("matching-netpol")
	control, c := secureControl(t)
	for _, rendered := range topology.NetworkPolicies(run, nil, nil) {
		if err := c.Create(ctx, rendered); err != nil {
			t.Fatalf("seed policy %s: %v", rendered.Name, err)
		}
	}
	seedLaunchedSecureRun(t, c, run)
	counter := &policyWriteCounter{Client: control.Client}
	control.Client = counter

	if _, err := control.LaunchSecure(ctx, run); err != nil {
		t.Fatalf("LaunchSecure: %v", err)
	}
	if len(counter.updates) != 0 || len(counter.creates) != 0 || len(counter.deletes) != 0 {
		t.Fatalf("matching policies were written: creates %v updates %v deletes %v", counter.creates, counter.updates, counter.deletes)
	}
}
