package topology

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	courier "github.com/misospace/courier/api/v1alpha1"
)

// NetworkPolicies renders the run-scoped default-deny isolation:
//
//   - worker: no ingress except from the run's control pod on the task port;
//     no egress at all unless the dependency cache is pinned, in which case
//     egress is allowed only to that ClusterIP as an IP literal on the cache
//     port. No DNS, no metadata, no API, no cross-run paths.
//   - control: no ingress; egress to its broker, its worker, cluster DNS, and
//     the optional model-gateway slot.
//   - broker: ingress only from the run's control pod on its two TLS ports;
//     egress to cluster DNS and TCP 443 (the API server and the configured
//     forge/git endpoints). The broker is trusted code that pins destinations
//     itself; the policy bounds the blast radius of an unexpected compromise
//     and excludes link-local metadata addresses.
//
// Policies are applied before any workload pod is created.
func NetworkPolicies(run *courier.CoderRun, cache *CachePin, gateway *GatewayPin) []*networkingv1.NetworkPolicy {
	protocolTCP := corev1.ProtocolTCP
	protocolUDP := corev1.ProtocolUDP
	dnsPort := intstr.FromInt32(53)

	worker := &networkingv1.NetworkPolicy{
		ObjectMeta: objectMeta(run, WorkerNetworkPolicyName(run.Name), ComponentWorker),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: workerSelector(run.Name)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: coordinatorSelector(run.Name)},
				}},
				Ports: []networkingv1.NetworkPolicyPort{{
					Protocol: &protocolTCP,
					Port:     intOr(WorkerPort),
				}},
			}},
			// Egress stays nil when no cache is pinned: the worker can
			// reach nothing, which is the fail-closed default.
		},
	}
	if cache != nil && cache.IP != "" {
		cidr := cache.IP + "/32"
		if isIPv6(cache.IP) {
			cidr = cache.IP + "/128"
		}
		worker.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{CIDR: cidr},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: &protocolTCP,
				Port:     intOr(cache.Port),
			}},
		}}
	}

	egress := []networkingv1.NetworkPolicyEgressRule{
		{
			To: []networkingv1.NetworkPolicyPeer{{
				PodSelector: &metav1.LabelSelector{MatchLabels: brokerSelector(run.Name)},
			}},
			Ports: []networkingv1.NetworkPolicyPort{
				{Protocol: &protocolTCP, Port: intOr(8443)},
				{Protocol: &protocolTCP, Port: intOr(8444)},
			},
		},
		{
			To: []networkingv1.NetworkPolicyPeer{{
				PodSelector: &metav1.LabelSelector{MatchLabels: workerSelector(run.Name)},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: &protocolTCP,
				Port:     intOr(WorkerPort),
			}},
		},
		dnsEgressRule(protocolTCP, protocolUDP, dnsPort),
	}
	if gateway != nil && gateway.CIDR != "" {
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{CIDR: gateway.CIDR},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{
				Protocol: &protocolTCP,
				Port:     intOr(gateway.Port),
			}},
		})
	}
	control := &networkingv1.NetworkPolicy{
		ObjectMeta: objectMeta(run, ControlNetworkPolicyName(run.Name), ComponentCoordinator),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: coordinatorSelector(run.Name)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			// No ingress rules: nothing in the cluster may connect to
			// control.
			Ingress: []networkingv1.NetworkPolicyIngressRule{},
			Egress:  egress,
		},
	}

	brokerEgress := []networkingv1.NetworkPolicyEgressRule{
		dnsEgressRule(protocolTCP, protocolUDP, dnsPort),
		{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{
					CIDR:   "0.0.0.0/0",
					Except: []string{"169.254.0.0/16", "127.0.0.0/8"},
				},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}},
		},
		{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{
					CIDR:   "::/0",
					Except: []string{"fe80::/10"},
				},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}},
		},
	}
	broker := &networkingv1.NetworkPolicy{
		ObjectMeta: objectMeta(run, BrokerNetworkPolicyName(run.Name), ComponentBroker),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: brokerSelector(run.Name)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{{
					PodSelector: &metav1.LabelSelector{MatchLabels: coordinatorSelector(run.Name)},
				}},
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &protocolTCP, Port: intOr(8443)},
					{Protocol: &protocolTCP, Port: intOr(8444)},
				},
			}},
			Egress: brokerEgress,
		},
	}
	return []*networkingv1.NetworkPolicy{worker, control, broker}
}

func dnsEgressRule(protocolTCP, protocolUDP corev1.Protocol, dnsPort intstr.IntOrString) networkingv1.NetworkPolicyEgressRule {
	return networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{{
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"},
			},
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"k8s-app": "kube-dns"},
			},
		}},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &protocolUDP, Port: &dnsPort},
			{Protocol: &protocolTCP, Port: &dnsPort},
		},
	}
}

func workerSelector(runName string) map[string]string {
	return map[string]string{LabelRun: runName, "courier.misospace.dev/component": ComponentWorker}
}

func coordinatorSelector(runName string) map[string]string {
	return map[string]string{LabelRun: runName, "courier.misospace.dev/component": ComponentCoordinator}
}

func brokerSelector(runName string) map[string]string {
	return map[string]string{LabelRun: runName, "courier.misospace.dev/component": ComponentBroker}
}

func isIPv6(ip string) bool {
	for _, r := range ip {
		if r == ':' {
			return true
		}
	}
	return false
}

func intOr(v int32) *intstr.IntOrString {
	value := intstr.FromInt32(v)
	return &value
}
