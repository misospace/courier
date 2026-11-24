package topology

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/misospace/courier/internal/executor"
)

func TestGitGuardEgressPolicy(t *testing.T) {
	run := testRun("git-guard")
	// A CIDR containing the link-local range carries the IPv4 exclusions; a
	// narrow CIDR that cannot contain them carries none, because the API
	// server rejects an except outside the admitted range. 0.0.0.0/0 spans
	// every IPv4 range, so it keeps all three; the IPv6 forge CIDR
	// 2606:50c0::/32 spans only itself, so it keeps none of its candidates.
	cidrs, err := ParseGitGuardCIDRs([]string{"0.0.0.0/0", "2606:50c0::/32"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	policy := GitGuardEgressPolicy(run, cidrs)
	if policy.Name != "git-guard-git-guard-egress" {
		t.Fatalf("policy name = %q", policy.Name)
	}
	if GitGuardNetworkPolicyName(run.Name) != policy.Name {
		t.Fatalf("name helper = %q, policy name = %q", GitGuardNetworkPolicyName(run.Name), policy.Name)
	}
	wantSelector := coordinatorSelector(run.Name)
	if !reflect.DeepEqual(policy.Spec.PodSelector.MatchLabels, wantSelector) {
		t.Fatalf("pod selector = %+v, want %+v", policy.Spec.PodSelector.MatchLabels, wantSelector)
	}
	// The selector must select an executor-built legacy coordinator pod and
	// never a worker or broker pod of the same run.
	coordinatorPod := map[string]string{
		executor.LabelRun:                 executor.RunLabelValue(run.Name),
		"courier.misospace.dev/component": ComponentCoordinator,
	}
	for key, value := range wantSelector {
		if coordinatorPod[key] != value {
			t.Fatalf("selector key %s=%q does not match an executor-built coordinator pod", key, value)
		}
	}
	for _, component := range []string{ComponentWorker, ComponentBroker} {
		if selectorSelectsComponent(wantSelector, component) {
			t.Fatalf("selector matches %s pods", component)
		}
	}
	if !reflect.DeepEqual(policy.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeEgress, networkingv1.PolicyTypeIngress}) {
		t.Fatalf("policy types = %v", policy.Spec.PolicyTypes)
	}
	if len(policy.Spec.Egress) != 3 {
		t.Fatalf("egress rules = %d, want DNS and one rule per CIDR", len(policy.Spec.Egress))
	}
	dns := policy.Spec.Egress[0]
	if len(dns.To) != 1 || dns.To[0].NamespaceSelector == nil || dns.To[0].PodSelector == nil ||
		dns.To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "kube-system" ||
		dns.To[0].PodSelector.MatchLabels["k8s-app"] != "kube-dns" || len(dns.Ports) != 2 {
		t.Fatalf("DNS rule = %+v", dns)
	}
	if *dns.Ports[0].Protocol != corev1.ProtocolUDP || dns.Ports[0].Port.IntValue() != 53 ||
		*dns.Ports[1].Protocol != corev1.ProtocolTCP || dns.Ports[1].Port.IntValue() != 53 {
		t.Fatalf("DNS ports = %+v", dns.Ports)
	}
	// A CIDR containing the link-local range carries the IPv4 exclusions; a
	// narrow CIDR that cannot contain them carries none, because the API
	// server rejects an except outside the admitted range.
	// A CIDR containing the metadata ranges carries those exclusions; a
	// narrow CIDR that cannot contain them carries none (nil slice), because
	// the API server rejects an except outside the admitted range.
	wantExcept := [][]string{
		{"169.254.0.0/16", "127.0.0.0/8", "100.100.100.200/32"},
		nil,
	}
	for i, cidr := range cidrs {
		rule := policy.Spec.Egress[i+1]
		if len(rule.To) != 1 || rule.To[0].IPBlock == nil || rule.To[0].IPBlock.CIDR != cidr.String() ||
			!reflect.DeepEqual(rule.To[0].IPBlock.Except, wantExcept[i]) {
			t.Fatalf("CIDR rule %q = %+v", cidr, rule)
		}
		if len(rule.Ports) != 1 || rule.Ports[0].Protocol == nil || *rule.Ports[0].Protocol != corev1.ProtocolTCP ||
			rule.Ports[0].Port == nil || *rule.Ports[0].Port != intstr.FromInt32(443) {
			t.Fatalf("CIDR rule ports = %+v", rule.Ports)
		}
	}
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].From) != 1 || len(policy.Spec.Ingress[0].Ports) != 0 {
		t.Fatalf("ingress = %+v", policy.Spec.Ingress)
	}
	peer := policy.Spec.Ingress[0].From[0]
	if peer.PodSelector == nil || peer.NamespaceSelector != nil || !reflect.DeepEqual(peer.PodSelector.MatchLabels, map[string]string{LabelRun: RunLabelValue(run.Name)}) {
		t.Fatalf("ingress peer = %+v", peer)
	}
	if !reflect.DeepEqual(policy, GitGuardEgressPolicy(run, cidrs)) {
		t.Fatal("policy render is not deterministic")
	}
}

// The #908 acceptance condition at the rendered-spec level: a direct forge
// connection from the coordinator container is refused on every port the
// policy does not name — SSH 22, the git protocol 9418, and plain HTTP (the
// evidence intake's service and intake ports) are admitted by no rule —
// while guard-routed git still works: the guard's loopback listener is
// intra-pod and outside NetworkPolicy scope, and TCP 443 to the forge CIDRs
// carries the guard's upstream TLS. The policy cannot distinguish containers
// inside one pod, so a direct forge connection on 443 itself remains
// fabric-admitted (DESIGN.md records that reduced scope); what the spec must
// refuse is every other forge path. A rule with no ports, a port with no
// number, or a named port matches every port or a port set — either would
// reopen the refused paths — so every rule is pinned to named integer ports.
func TestGitGuardEgressPolicyRefusesDirectForgePaths(t *testing.T) {
	run := testRun("git-guard-direct")
	cidrs, err := ParseGitGuardCIDRs([]string{"140.82.112.0/20", "185.199.108.0/22"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	policy := GitGuardEgressPolicy(run, cidrs)
	if len(policy.Spec.Egress) != 1+len(cidrs) {
		t.Fatalf("egress rules = %d, want DNS plus one rule per CIDR", len(policy.Spec.Egress))
	}
	forge := map[string]bool{}
	for _, cidr := range cidrs {
		forge[cidr.String()] = true
	}
	for _, rule := range policy.Spec.Egress {
		if len(rule.Ports) == 0 {
			t.Fatalf("egress rule %+v carries no ports: that matches every port", rule)
		}
		for _, port := range rule.Ports {
			if port.Port == nil || port.Port.Type != intstr.Int {
				t.Fatalf("egress rule %+v carries port %v: only named integer ports are pinned", rule, port.Port)
			}
			switch port.Port.IntValue() {
			case 22, 9418, 80, 8082:
				t.Fatalf("egress rule admits port %d: a direct forge path or the evidence intake must be refused", port.Port.IntValue())
			}
		}
		if len(rule.To) == 1 && rule.To[0].IPBlock != nil && forge[rule.To[0].IPBlock.CIDR] {
			if len(rule.Ports) != 1 || *rule.Ports[0].Protocol != corev1.ProtocolTCP ||
				*rule.Ports[0].Port != intstr.FromInt32(443) {
				t.Fatalf("forge CIDR %q admits %+v, want TCP 443 only: the guard's upstream", rule.To[0].IPBlock.CIDR, rule.Ports)
			}
		}
	}
}

func TestGitGuardEgressPolicyNarrowCIDRDropsInapplicableExcepts(t *testing.T) {
	run := testRun("git-guard-narrow")
	cidrs, err := ParseGitGuardCIDRs([]string{"140.82.112.0/20"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	policy := GitGuardEgressPolicy(run, cidrs)
	block := policy.Spec.Egress[1].To[0].IPBlock
	if block.CIDR != "140.82.112.0/20" || len(block.Except) != 0 {
		t.Fatalf("narrow-CIDR except = %+v, want none: an except outside the admitted range is refused by the API server", block.Except)
	}
}

func TestGitGuardEgressPolicyMetadataSpanningCIDRKeepsExcepts(t *testing.T) {
	run := testRun("git-guard-wide")
	cidrs, err := ParseGitGuardCIDRs([]string{"0.0.0.0/0"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	policy := GitGuardEgressPolicy(run, cidrs)
	except := policy.Spec.Egress[1].To[0].IPBlock.Except
	want := []string{"169.254.0.0/16", "127.0.0.0/8", "100.100.100.200/32"}
	if !reflect.DeepEqual(except, want) {
		t.Fatalf("wide-CIDR except = %v, want %v", except, want)
	}
}

func TestGitGuardEgressPolicyIPv6MetadataSpanningCIDRKeepsExcepts(t *testing.T) {
	run := testRun("git-guard-wide6")
	cidrs, err := ParseGitGuardCIDRs([]string{"::/0"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	policy := GitGuardEgressPolicy(run, cidrs)
	except := policy.Spec.Egress[1].To[0].IPBlock.Except
	want := []string{"fe80::/10", "fd00:ec2::254/128"}
	if !reflect.DeepEqual(except, want) {
		t.Fatalf("wide-IPv6-CIDR except = %v, want %v", except, want)
	}
}

func TestParseGitGuardCIDRsRejectsMalformed(t *testing.T) {
	if _, err := ParseGitGuardCIDRs([]string{"140.82.112.0/20", "not-a-cidr"}); err == nil {
		t.Fatal("ParseGitGuardCIDRs accepted a malformed CIDR")
	}
	if parsed, err := ParseGitGuardCIDRs(nil); err != nil || parsed != nil {
		t.Fatalf("ParseGitGuardCIDRs(nil) = %v, %v", parsed, err)
	}
}

func TestGitGuardEgressPolicyNilInputs(t *testing.T) {
	run := testRun("git-guard-nil")
	cidrs, err := ParseGitGuardCIDRs([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatalf("ParseGitGuardCIDRs: %v", err)
	}
	if GitGuardEgressPolicy(nil, cidrs) != nil {
		t.Fatal("GitGuardEgressPolicy(nil run) must return nil")
	}
	if GitGuardEgressPolicy(run, nil) != nil {
		t.Fatal("GitGuardEgressPolicy with no CIDRs must return nil")
	}
}

// selectorSelectsComponent reports whether a selector that names the run's
// component key would select a pod stamped with the given component value.
func selectorSelectsComponent(selector map[string]string, component string) bool {
	return selector["courier.misospace.dev/component"] == component
}
