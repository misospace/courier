package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

func TestEnvtestLaunchGitGuardTopologyAndServiceConverge(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	const laneName, runName = "gitguard-topology-lane", "gitguard-topology"
	if err := c.Create(ctx, envtestLane(laneName)); err != nil {
		t.Fatal(err)
	}
	run := envtestRun(runName, laneName)
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}

	cidrs := []string{"140.82.112.0/20"}
	apiIP := parseGitGuardAPIIP("10.96.0.1")
	launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "envtest", expiresAt: time.Now().Add(time.Hour)}, time.Now(), plainExecutor{})
	launcher.GitGuardEgressCIDRs = cidrs
	launcher.GitGuardKubernetesAPIIP = "10.96.0.1"
	if err := launcher.Launch(ctx, run); !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("first Launch: %v, want guard running gate", err)
	}
	var guardStarting corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &guardStarting); err != nil {
		t.Fatal(err)
	}
	guardStarting.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: executor.GitEgressContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := c.Status().Update(ctx, &guardStarting); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(ctx, run); err != nil {
		t.Fatalf("second Launch: %v", err)
	}

	parsedCIDRs, err := topology.ParseGitGuardCIDRs(cidrs)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]*networkingv1.NetworkPolicy{
		topology.GitGuardNetworkPolicyName(run.Name):    topology.GitGuardEgressPolicy(run, parsedCIDRs, apiIP),
		topology.GitGuardPodNetworkPolicyName(run.Name): topology.GitGuardPodNetworkPolicy(run, parsedCIDRs, apiIP),
	} {
		var got networkingv1.NetworkPolicy
		key := client.ObjectKey{Namespace: run.Namespace, Name: name}
		if err := c.Get(ctx, key, &got); err != nil {
			t.Fatalf("get policy %s: %v", name, err)
		}
		if !apiequality.Semantic.DeepEqual(got.Spec, want.Spec) {
			t.Fatalf("policy %s spec = %+v, want %+v", name, got.Spec, want.Spec)
		}
		if name == topology.GitGuardNetworkPolicyName(run.Name) {
			forgeExcluded := false
			for _, rule := range got.Spec.Egress {
				for _, peer := range rule.To {
					if peer.IPBlock == nil {
						continue
					}
					if peer.IPBlock.CIDR == cidrs[0] {
						t.Fatalf("coordinator policy admits forge CIDR %s directly", cidrs[0])
					}
					if peer.IPBlock.CIDR == "0.0.0.0/0" && gitGuardContainsString(peer.IPBlock.Except, cidrs[0]) {
						forgeExcluded = true
					}
				}
			}
			if !forgeExcluded {
				t.Fatalf("coordinator policy does not exclude forge CIDR %s in its broad rule", cidrs[0])
			}
		} else {
			admitted := false
			for _, rule := range got.Spec.Egress {
				for _, peer := range rule.To {
					if peer.IPBlock != nil && peer.IPBlock.CIDR == cidrs[0] {
						admitted = true
					}
				}
			}
			if !admitted {
				t.Fatalf("guard egress policy does not admit forge CIDR %s", cidrs[0])
			}
		}
	}

	var service corev1.Service
	serviceKey := client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}
	if err := c.Get(ctx, serviceKey, &service); err != nil {
		t.Fatal(err)
	}
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != executor.GitEgressPort || service.Spec.Ports[0].TargetPort.IntValue() != executor.GitEgressPort {
		t.Fatalf("guard Service ports = %+v; want one port %d -> %d", service.Spec.Ports, executor.GitEgressPort, executor.GitEgressPort)
	}
	serviceVersion := service.ResourceVersion
	if err := launcher.ensureGitGuardService(ctx, run); err != nil {
		t.Fatalf("second ensureGitGuardService: %v", err)
	}
	if err := c.Get(ctx, serviceKey, &service); err != nil {
		t.Fatal(err)
	}
	if service.ResourceVersion != serviceVersion {
		t.Fatalf("steady-state ensure rewrote Service (resourceVersion %q -> %q)", serviceVersion, service.ResourceVersion)
	}

	var guardPod, coordinator corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &guardPod); err != nil {
		t.Fatal(err)
	}
	if !podOwnedByRunUID(&guardPod, run.UID) || guardPod.Spec.ServiceAccountName != executor.GitCredentialsServiceAccountName(run.Name) {
		t.Fatalf("guard Pod ownership/service account = %q/%q, want run-owned and %q", guardPod.UID, guardPod.Spec.ServiceAccountName, executor.GitCredentialsServiceAccountName(run.Name))
	}
	var serviceAccount corev1.ServiceAccount
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitCredentialsServiceAccountName(run.Name)}, &serviceAccount); err != nil {
		t.Fatalf("get restored git credentials ServiceAccount: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: run.Name + "-coordinator"}, &coordinator); err != nil {
		t.Fatal(err)
	}
	wantRemote := "http://" + executor.GitGuardServiceDNS(run.Namespace, run.Name) + ":18443/acme/widgets.git"
	if got := controllerEnvValue(coordinator.Spec.Containers[0].Env, "COURIER_REPO_URL"); got != wantRemote {
		t.Fatalf("coordinator COURIER_REPO_URL = %q, want %q", got, wantRemote)
	}
}

func TestEnvtestTeardownLegacyGitGuardIsIdempotentAndOwnershipSafe(t *testing.T) {
	if envtestServer == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := networkingv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(envtestServer.Config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	const laneName, runName = "gitguard-teardown-lane", "gitguard-teardown"
	if err := c.Create(ctx, envtestLane(laneName)); err != nil {
		t.Fatal(err)
	}
	run := envtestRun(runName, laneName)
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, envtestKey(runName), run); err != nil {
		t.Fatal(err)
	}
	launcher := installationLauncher(c, &fixedInstallationTokenMinter{token: "teardown", expiresAt: time.Now().Add(time.Hour)}, time.Now(), plainExecutor{})
	launcher.GitGuardEgressCIDRs = []string{"140.82.112.0/20"}
	launcher.GitGuardKubernetesAPIIP = "10.96.0.1"
	if err := launcher.Launch(ctx, run); !errors.Is(err, ErrProvisioningInFlight) {
		t.Fatalf("first Launch: %v, want guard running gate", err)
	}
	var guardStarting corev1.Pod
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}, &guardStarting); err != nil {
		t.Fatal(err)
	}
	guardStarting.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: executor.GitEgressContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if err := c.Status().Update(ctx, &guardStarting); err != nil {
		t.Fatal(err)
	}
	if err := launcher.Launch(ctx, run); err != nil {
		t.Fatalf("second Launch: %v", err)
	}
	var guardPod corev1.Pod
	guardKey := client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardPodName(run.Name)}
	if err := c.Get(ctx, guardKey, &guardPod); err != nil {
		t.Fatal(err)
	}
	guardPod.Finalizers = append(guardPod.Finalizers, "courier.misospace.dev/test-hold")
	if err := c.Update(ctx, &guardPod); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := launcher.TeardownLegacyGitGuard(ctx, run); !errors.Is(err, ErrGuardTeardownPending) {
			t.Fatalf("teardown[%d]: %v, want ErrGuardTeardownPending", i, err)
		}
		if err := c.Get(ctx, guardKey, &guardPod); err != nil || guardPod.DeletionTimestamp == nil {
			t.Fatalf("guard Pod after teardown[%d]: err=%v deletionTimestamp=%v", i, err, guardPod.DeletionTimestamp)
		}
		if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: executor.GitGuardServiceName(run.Name)}, &corev1.Service{}); !apierrors.IsNotFound(err) {
			t.Fatalf("guard Service after teardown[%d]: %v, want NotFound", i, err)
		}
		for _, name := range []string{topology.GitGuardNetworkPolicyName(run.Name), topology.GitGuardPodNetworkPolicyName(run.Name)} {
			if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: name}, &networkingv1.NetworkPolicy{}); err != nil {
				t.Fatalf("policy %s after teardown[%d]: %v, want present", name, i, err)
			}
		}
	}
	guardPod.Finalizers = nil
	if err := c.Update(ctx, &guardPod); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := c.Get(ctx, guardKey, &corev1.Pod{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("wait for guard Pod absence: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("guard Pod did not disappear after finalizer removal")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := launcher.TeardownLegacyGitGuard(ctx, run); err != nil {
		t.Fatalf("teardown after guard absence: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardPodNetworkPolicyName(run.Name)}, &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
		t.Fatalf("guard ingress policy after teardown: %v, want NotFound", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatalf("coordinator egress policy was not retained: %v", err)
	}
	if err := launcher.TeardownLegacyGitGuard(ctx, run); err != nil {
		t.Fatalf("repeated teardown: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: run.Namespace, Name: topology.GitGuardNetworkPolicyName(run.Name)}, &networkingv1.NetworkPolicy{}); err != nil {
		t.Fatalf("coordinator egress policy after repeated teardown: %v", err)
	}

	foreignRun := envtestRun("gitguard-foreign", laneName)
	foreignRun.UID = "foreign-run-uid"
	if err := c.Create(ctx, foreignRun); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, envtestKey(foreignRun.Name), foreignRun); err != nil {
		t.Fatal(err)
	}
	foreignService := topology.GitGuardService(foreignRun)
	foreignService.OwnerReferences = nil
	if err := c.Create(ctx, foreignService); err != nil {
		t.Fatal(err)
	}
	if err := launcher.TeardownLegacyGitGuard(ctx, foreignRun); err != nil {
		t.Fatalf("teardown with foreign Service: %v", err)
	}
	var preserved corev1.Service
	if err := c.Get(ctx, client.ObjectKeyFromObject(foreignService), &preserved); err != nil {
		t.Fatalf("foreign Service was removed: %v", err)
	}
}

func gitGuardContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
