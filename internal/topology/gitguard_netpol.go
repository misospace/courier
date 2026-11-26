package topology

import (
	"net"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// GitGuardEgressPolicy renders the coordinator pod's half of the opt-in
// git-guard egress boundary (#908 as reworked by #919): the guard now runs in
// its own per-run pod, so this policy can refuse the forge outright — the
// operator's forge CIDRs appear ONLY in the except lists of broad TCP-443
// rules, so a direct forge connection from any container of the coordinator
// pod is refused by the fabric, closing the #908 residual where the sidecar
// guard's own upstream TLS forced the forge CIDR to stay admitted. Every
// other TCP-443 destination stays admitted (the model-provider endpoint, MCP
// servers, any other HTTPS endpoint the deployment configures), mirroring the
// secure broker's own 443 posture; every non-443 path stays refused, notably
// the evidence-intake POST to its in-cluster HTTP service (a lost bundle, not
// a failed run). The named peers pin what the broad rule cannot express
// port-precisely: the run's guard Service (pod selector on the guard port,
// the post-DNAT shape most fabrics judge) and the Kubernetes API endpoint the
// observer containers read the run's credentials document through — named
// explicitly so a forge CIDR that accidentally contains that address cannot
// strangle the credentials read, because egress rules union. The policy is
// exact only where it is the only policy selecting the pod: egress rules of
// policies selecting the same pod are unioned, so a second policy can
// re-admit what this one refuses. Callers must provide at least one parsed
// CIDR and the cluster's API endpoint; a nil input renders nothing and the
// launcher refuses the launch instead of enforcing a half-boundary.
func GitGuardEgressPolicy(run *courier.CoderRun, cidrs []*net.IPNet, apiIP *net.IPNet) *networkingv1.NetworkPolicy {
	if run == nil || len(cidrs) == 0 || apiIP == nil {
		return nil
	}
	protocolTCP, protocolUDP := corev1.ProtocolTCP, corev1.ProtocolUDP
	egress := []networkingv1.NetworkPolicyEgressRule{
		dnsEgressRule(protocolTCP, protocolUDP, intstr.FromInt32(53)),
		{To: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: guardSelector(run.Name)}}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(executor.GitEgressPort)}}},
		{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: apiIP.String()}}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}}},
	}
	for _, family := range []string{"0.0.0.0/0", "::/0"} {
		_, broad, _ := net.ParseCIDR(family)
		except := gitGuardExceptions(broad)
		for _, cidr := range cidrs {
			if (cidr.IP.To4() != nil) == (broad.IP.To4() != nil) {
				except = append(except, cidr.String())
			}
		}
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{
			To:    []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: family, Except: except}}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}},
		})
	}
	return &networkingv1.NetworkPolicy{ObjectMeta: objectMeta(run, GitGuardNetworkPolicyName(run.Name), ComponentCoordinator), Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: coordinatorSelector(run.Name)},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress, networkingv1.PolicyTypeIngress},
		Ingress:     []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{LabelRun: RunLabelValue(run.Name)}}}}}},
		Egress:      egress,
	}}
}

// GitGuardPodNetworkPolicyName names the per-run guard pod's own policy.
func GitGuardPodNetworkPolicyName(runName string) string { return resourceName(runName, "-git-guard") }

// GitGuardPodNetworkPolicy always admits only the run's own pods to the
// guard port. When CIDRs and the API IP are configured, it also restricts
// guard egress to DNS, forge CIDRs, and the Kubernetes API; without them, the
// policy remains ingress-only so no other workload can borrow the run's git
// authority.
func GitGuardPodNetworkPolicy(run *courier.CoderRun, cidrs []*net.IPNet, apiIP *net.IPNet) *networkingv1.NetworkPolicy {
	if run == nil {
		return nil
	}
	var egress []networkingv1.NetworkPolicyEgressRule
	policyTypes := []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
	if len(cidrs) > 0 && apiIP != nil {
		protocolTCP, protocolUDP := corev1.ProtocolTCP, corev1.ProtocolUDP
		egress = []networkingv1.NetworkPolicyEgressRule{dnsEgressRule(protocolTCP, protocolUDP, intstr.FromInt32(53))}
		for _, cidr := range cidrs {
			egress = append(egress, networkingv1.NetworkPolicyEgressRule{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: cidr.String(), Except: gitGuardExceptions(cidr)}}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}}})
		}
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: apiIP.String()}}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}}})
		policyTypes = append(policyTypes, networkingv1.PolicyTypeEgress)
	}
	protocolTCP := corev1.ProtocolTCP
	return &networkingv1.NetworkPolicy{ObjectMeta: objectMeta(run, GitGuardPodNetworkPolicyName(run.Name), ComponentGitGuard), Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: guardSelector(run.Name)},
		PolicyTypes: policyTypes,
		Ingress: []networkingv1.NetworkPolicyIngressRule{{
			From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{LabelRun: RunLabelValue(run.Name)}}}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(executor.GitEgressPort)}},
		}},
		Egress: egress,
	}}
}

// guardSelector selects the run's guard pod: the peer the coordinator policy
// dials and the pod the guard policy protects.
func guardSelector(runName string) map[string]string {
	return map[string]string{LabelRun: RunLabelValue(runName), executor.LabelComponent: executor.GitGuardComponentName}
}

// GitGuardService renders the run's guard Service: the ClusterIP address the
// App-mode coordinator's git remotes are rewritten to, forwarding the guard
// port to the guard pod. The rendered spec names every field the API server
// defaults (type, session affinity, internal traffic policy) so a stored
// Service compares equal to the render and the launcher's converge path
// stays idle in steady state, exactly as the policy converge contract is
// pinned; the server-assigned ClusterIP fields are the launcher's to
// preserve.
func GitGuardService(run *courier.CoderRun) *corev1.Service {
	if run == nil {
		return nil
	}
	internalTrafficPolicyCluster := corev1.ServiceInternalTrafficPolicyCluster
	return &corev1.Service{ObjectMeta: objectMeta(run, executor.GitGuardServiceName(run.Name), ComponentGitGuard), Spec: corev1.ServiceSpec{
		Type:                  corev1.ServiceTypeClusterIP,
		SessionAffinity:       corev1.ServiceAffinityNone,
		InternalTrafficPolicy: &internalTrafficPolicyCluster,
		Selector:              guardSelector(run.Name), Ports: []corev1.ServicePort{{Name: "http", Protocol: corev1.ProtocolTCP, Port: executor.GitEgressPort, TargetPort: intstr.FromInt32(executor.GitEgressPort)}},
	}}
}

// gitGuardExceptions returns the non-routable ranges the admitted CIDR
// contains: the link-local metadata and loopback exclusions for IPv4, the
// link-local and AWS IPv6 metadata exclusions for IPv6, each intersected
// with the CIDR itself so the rendered policy never carries an except
// outside its own range — the API server rejects that outright.
func gitGuardExceptions(cidr *net.IPNet) []string {
	candidates := []string{"169.254.0.0/16", "127.0.0.0/8", "100.100.100.200/32"}
	if cidr.IP.To4() == nil {
		candidates = []string{"fe80::/10", "fd00:ec2::254/128"}
	}
	var except []string
	base := &net.IPNet{IP: cidr.IP.Mask(cidr.Mask), Mask: cidr.Mask}
	for _, candidate := range candidates {
		_, network, err := net.ParseCIDR(candidate)
		if err == nil && contained(network, base) {
			except = append(except, candidate)
		}
	}
	return except
}

// contained reports whether the whole candidate network lies inside base.
func contained(candidate, base *net.IPNet) bool {
	if (candidate.IP.To4() == nil) != (base.IP.To4() == nil) {
		return false
	}
	baseOnes, baseBits := base.Mask.Size()
	candidateOnes, candidateBits := candidate.Mask.Size()
	return baseBits == candidateBits && candidateOnes >= baseOnes && base.Contains(candidate.IP)
}

// ParseGitGuardCIDRs parses the deployment's operator-provided forge CIDRs.
// Every entry must be a valid CIDR; the first malformed entry names itself
// in the error so the deployment fails at configuration time.
func ParseGitGuardCIDRs(raw []string) ([]*net.IPNet, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	parsed := make([]*net.IPNet, 0, len(raw))
	for _, entry := range raw {
		_, cidr, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, cidr)
	}
	return parsed, nil
}
