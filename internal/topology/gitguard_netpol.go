package topology

import (
	"net"

	courier "github.com/misospace/courier/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// GitGuardEgressPolicy renders the opt-in legacy coordinator egress boundary
// (#908): the policy admits DNS egress to cluster DNS, ingress from the
// run's own pods (the reply path of a pod-originated connection), and TCP
// 443 egress to the operator-provided forge CIDRs — the pod's only 443
// destinations once the flag is set, so the operator's list must cover every
// 443 destination the pod needs, the forge hosts and the cluster API server
// the guard and observer read their credentials document through. Callers
// must provide at least one parsed CIDR; the exclusion lists carry only the
// entries contained in the admitted CIDR, because the API server rejects an
// except outside it.
func GitGuardEgressPolicy(run *courier.CoderRun, cidrs []*net.IPNet) *networkingv1.NetworkPolicy {
	if run == nil || len(cidrs) == 0 {
		return nil
	}
	protocolTCP := corev1.ProtocolTCP
	protocolUDP := corev1.ProtocolUDP
	dnsPort := intstr.FromInt32(53)
	selector := coordinatorSelector(run.Name)
	egress := []networkingv1.NetworkPolicyEgressRule{dnsEgressRule(protocolTCP, protocolUDP, dnsPort)}
	for _, cidr := range cidrs {
		egress = append(egress, networkingv1.NetworkPolicyEgressRule{
			To: []networkingv1.NetworkPolicyPeer{{
				IPBlock: &networkingv1.IPBlock{CIDR: cidr.String(), Except: gitGuardExceptions(cidr)},
			}},
			Ports: []networkingv1.NetworkPolicyPort{{Protocol: &protocolTCP, Port: intOr(443)}},
		})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: objectMeta(run, GitGuardNetworkPolicyName(run.Name), ComponentCoordinator),
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress, networkingv1.PolicyTypeIngress},
			// The pod dials out; the reply to a pod-originated connection
			// lands on an ephemeral port and is judged as fresh ingress,
			// which a deny-all ingress drops on datapaths that do not
			// recognize it as established (#711). The peer is the run's own
			// pods, so the rest of the cluster still cannot connect in.
			Ingress: []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{LabelRun: RunLabelValue(run.Name)}},
			}}}},
			Egress: egress,
		},
	}
}

// gitGuardExceptions returns the non-routable ranges the admitted CIDR
// contains: the link-local metadata and loopback exclusions for IPv4, the
// link-local and AWS IPv6 metadata exclusions for IPv6, each intersected
// with the CIDR itself so the rendered policy never carries an except
// outside its own range — the API server rejects that outright.
func gitGuardExceptions(cidr *net.IPNet) []string {
	var candidates []string
	ip4 := cidr.IP.To4() != nil
	if ip4 {
		candidates = []string{
			"169.254.0.0/16",     // link-local metadata (AWS/GCP/Azure)
			"127.0.0.0/8",        // loopback
			"100.100.100.200/32", // Alibaba Cloud metadata
		}
	} else {
		candidates = []string{
			"fe80::/10",         // link-local
			"fd00:ec2::254/128", // AWS IPv6 metadata
		}
	}
	var except []string
	base := &net.IPNet{IP: cidr.IP.Mask(cidr.Mask), Mask: cidr.Mask}
	for _, candidate := range candidates {
		_, candidateNet, err := net.ParseCIDR(candidate)
		if err != nil {
			continue
		}
		if contained(candidateNet, base) {
			except = append(except, candidate)
		}
	}
	return except
}

// contained reports whether the whole candidate network lies inside base.
func contained(candidate, base *net.IPNet) bool {
	if candidate.IP.To4() == nil != (base.IP.To4() == nil) {
		return false
	}
	baseOnes, baseBits := base.Mask.Size()
	candOnes, candBits := candidate.Mask.Size()
	return baseBits == candBits && candOnes >= baseOnes && base.Contains(candidate.IP)
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
