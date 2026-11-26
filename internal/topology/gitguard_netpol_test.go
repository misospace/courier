package topology

import (
	"net"
	"reflect"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/misospace/courier/internal/executor"
)

func testAPIIP() *net.IPNet {
	_, ip, _ := net.ParseCIDR("10.96.0.1/32")
	return ip
}

func TestGitGuardServiceNameParity(t *testing.T) {
	for _, runName := range []string{"simple", "name.with.dots", strings.Repeat("long-name-", 8), strings.Repeat("a", 63) + ".tail", "-leading", "trailing-", "9digits", "a." + strings.Repeat("b", 70)} {
		got, want := executor.GitGuardServiceName(runName), serviceResourceName(runName, "-git-guard")
		if got != want {
			t.Errorf("GitGuardServiceName(%q)=%q, want %q", runName, got, want)
		}
	}
}

func TestGitGuardIngressOnlyPolicyWithoutCIDRs(t *testing.T) {
	policy := GitGuardPodNetworkPolicy(testRun("guard-no-cidrs"), nil, nil)
	if policy == nil || !reflect.DeepEqual(policy.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) || policy.Spec.Egress != nil {
		t.Fatalf("ingress-only policy = %+v", policy)
	}
	if len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].From) != 1 || len(policy.Spec.Ingress[0].Ports) != 1 {
		t.Fatalf("ingress rules = %+v", policy.Spec.Ingress)
	}
}

func TestGitGuardPoliciesAndService(t *testing.T) {
	run := testRun("guard-run")
	cidrs, err := ParseGitGuardCIDRs([]string{"140.82.112.0/20", "2606:50c0::/32"})
	if err != nil {
		t.Fatal(err)
	}
	apiIP := testAPIIP()
	coordinator := GitGuardEgressPolicy(run, cidrs, apiIP)
	guard := GitGuardPodNetworkPolicy(run, cidrs, apiIP)
	if !reflect.DeepEqual(coordinator, GitGuardEgressPolicy(run, cidrs, apiIP)) || !reflect.DeepEqual(guard, GitGuardPodNetworkPolicy(run, cidrs, apiIP)) {
		t.Fatal("policy rendering is not deterministic")
	}
	if coordinator.Name != GitGuardNetworkPolicyName(run.Name) || guard.Name != GitGuardPodNetworkPolicyName(run.Name) {
		t.Fatalf("policy names: coordinator=%q guard=%q", coordinator.Name, guard.Name)
	}
	if len(coordinator.Spec.Egress) != 5 {
		t.Fatalf("coordinator egress rules=%d, want DNS, guard, API and two broad rules", len(coordinator.Spec.Egress))
	}
	if coordinator.Spec.Egress[1].To[0].PodSelector.MatchLabels[executor.LabelComponent] != executor.GitGuardComponentName || coordinator.Spec.Egress[1].Ports[0].Port.IntValue() != executor.GitEgressPort {
		t.Fatalf("guard peer rule=%+v", coordinator.Spec.Egress[1])
	}
	if coordinator.Spec.Egress[2].To[0].IPBlock.CIDR != apiIP.String() {
		t.Fatalf("API peer rule=%+v", coordinator.Spec.Egress[2])
	}
	for i, cidr := range cidrs {
		rule := coordinator.Spec.Egress[3+i]
		if rule.To[0].IPBlock.CIDR != map[bool]string{true: "0.0.0.0/0", false: "::/0"}[cidr.IP.To4() != nil] {
			t.Fatalf("broad family rule=%+v", rule)
		}
		if !hasCIDR(rule.To[0].IPBlock.Except, cidr.String()) {
			t.Fatalf("broad rule %s does not except forge CIDR %s", rule.To[0].IPBlock.CIDR, cidr)
		}
	}
	if len(guard.Spec.Ingress) != 1 || len(guard.Spec.Ingress[0].From) != 1 || guard.Spec.Ingress[0].From[0].PodSelector.MatchLabels[LabelRun] != RunLabelValue(run.Name) || guard.Spec.Ingress[0].Ports[0].Port.IntValue() != executor.GitEgressPort {
		t.Fatalf("guard ingress=%+v", guard.Spec.Ingress)
	}
	if len(guard.Spec.Egress) != 4 || guard.Spec.Egress[1].To[0].IPBlock.CIDR != cidrs[0].String() || guard.Spec.Egress[2].To[0].IPBlock.CIDR != cidrs[1].String() || guard.Spec.Egress[3].To[0].IPBlock.CIDR != apiIP.String() {
		t.Fatalf("guard egress=%+v", guard.Spec.Egress)
	}
	service := GitGuardService(run)
	if service.Name != executor.GitGuardServiceName(run.Name) || service.Spec.Ports[0].Port != executor.GitEgressPort || service.Spec.Ports[0].TargetPort.IntValue() != executor.GitEgressPort || service.Spec.Selector[executor.LabelComponent] != executor.GitGuardComponentName {
		t.Fatalf("guard Service=%+v", service)
	}
	for _, policy := range []*networkingv1.NetworkPolicy{coordinator, guard} {
		for _, rule := range policy.Spec.Egress {
			for _, port := range rule.Ports {
				if port.Port == nil || port.Port.Type != intstr.Int {
					t.Fatalf("unbounded port: %+v", port)
				}
			}
		}
	}
}

func TestGitGuardPolicyExceptBounds(t *testing.T) {
	run := testRun("guard-wide")
	cidrs, _ := ParseGitGuardCIDRs([]string{"0.0.0.0/0", "::/0"})
	policy := GitGuardEgressPolicy(run, cidrs, testAPIIP())
	if !reflect.DeepEqual(policy.Spec.Egress[3].To[0].IPBlock.Except, []string{"169.254.0.0/16", "127.0.0.0/8", "100.100.100.200/32", "0.0.0.0/0"}) {
		t.Fatalf("IPv4 except=%v", policy.Spec.Egress[3].To[0].IPBlock.Except)
	}
	if !reflect.DeepEqual(policy.Spec.Egress[4].To[0].IPBlock.Except, []string{"fe80::/10", "fd00:ec2::254/128", "::/0"}) {
		t.Fatalf("IPv6 except=%v", policy.Spec.Egress[4].To[0].IPBlock.Except)
	}
}

func TestParseGitGuardCIDRs(t *testing.T) {
	if _, err := ParseGitGuardCIDRs([]string{"bad"}); err == nil {
		t.Fatal("malformed CIDR accepted")
	}
	if got, err := ParseGitGuardCIDRs(nil); err != nil || got != nil {
		t.Fatalf("empty parse=%v, %v", got, err)
	}
}

func hasCIDR(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}
