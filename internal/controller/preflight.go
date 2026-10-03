package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	courier "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/topology"
)

// defaultProbeTimeout bounds one live-probe phase. It is a preflight
// resource bound, not a run-duration limit.
const defaultProbeTimeout = 2 * time.Minute

// preflightProbesImageNote documents the probe contract: probe pods carry the
// worker's labels so the worker's own NetworkPolicy is what the probes
// exercise. Allowed control-to-broker/worker paths are proven post-launch by
// the control binary's capability health; the preflight probes prove the deny
// paths and the worker-to-cache allow path.

type probeCheck struct {
	Name   string `json:"name"`
	IP     string `json:"ip"`
	Port   int32  `json:"port"`
	Expect string `json:"expect"` // "deny" or "allow"
}

type probeOutcome struct {
	Name string `json:"name"`
	Got  string `json:"got"`
}

// Preflight runs the secure-mode preflight for one run. It assumes the static
// topology objects (SAs, RBAC, secrets, services, policies) are already
// provisioned. relaunch abbreviates the check: replacement launches re-verify
// the static guarantees but do not repeat the multi-node live probes, which
// ran before the first workload exposure.
//
// Permanent deployment misconfiguration returns a *SecureNeedsHumanError;
// transient API or probe failures return an ordinary error so the run requeues
// without launching.
func (s *SecureControl) Preflight(ctx context.Context, run *courier.CoderRun, policy *courier.PublicationPolicy, relaunch bool) error {
	if err := s.checkNamespaceBoundary(ctx); err != nil {
		return err
	}
	if err := s.checkOperatorAccess(ctx); err != nil {
		return err
	}
	if err := s.checkBrokerRBAC(ctx, run); err != nil {
		return err
	}
	cache, err := s.cachePin(ctx)
	if err != nil {
		return err
	}
	if err := s.checkDryRuns(ctx, run, cache); err != nil {
		return err
	}
	if relaunch || !s.Config.LiveProbes {
		return nil
	}
	return s.runLiveProbes(ctx, run, cache)
}

// checkNamespaceBoundary verifies the dedicated run namespace exists and
// enforces the restricted Pod Security standard. A cluster that cannot prove
// restricted admission on the run namespace cannot run secure mode.
func (s *SecureControl) checkNamespaceBoundary(ctx context.Context) error {
	namespace := &corev1.Namespace{}
	if err := s.Client.Get(ctx, types.NamespacedName{Name: s.Config.RunNamespace}, namespace); err != nil {
		return fmt.Errorf("secure preflight: read run namespace: %w", err)
	}
	labels := namespace.Labels
	enforce := labels["pod-security.kubernetes.io/enforce"]
	if enforce == "" {
		return secureNeedsHuman("PodSecurity",
			"run namespace %q does not set pod-security.kubernetes.io/enforce; secure mode requires the restricted Pod Security standard",
			s.Config.RunNamespace)
	}
	if enforce != "restricted" {
		return secureNeedsHuman("PodSecurity",
			"run namespace %q enforces Pod Security level %q, not restricted", s.Config.RunNamespace, enforce)
	}
	return nil
}

type accessReview struct {
	group    string
	resource string
	verb     string
	name     string
}

// checkOperatorAccess verifies the operator's own provisioning grants with
// access reviews. A negative answer proves a grant is missing, which is
// permanent deployment misconfiguration.
func (s *SecureControl) checkOperatorAccess(ctx context.Context) error {
	core := ""
	reviews := []accessReview{
		{core, "secrets", "create", ""},
		{core, "secrets", "delete", ""},
		{core, "serviceaccounts", "create", ""},
		{core, "serviceaccounts", "delete", ""},
		{core, "pods", "create", ""},
		{core, "pods", "delete", ""},
		{core, "services", "create", ""},
		{core, "services", "delete", ""},
		{"networking.k8s.io", "networkpolicies", "create", ""},
		{"networking.k8s.io", "networkpolicies", "delete", ""},
		{"rbac.authorization.k8s.io", "roles", "create", ""},
		{"rbac.authorization.k8s.io", "rolebindings", "create", ""},
		{"rbac.authorization.k8s.io", "clusterrolebindings", "create", ""},
		{core, "nodes", "list", ""},
		{core, "namespaces", "get", ""},
		{"authorization.k8s.io", "selfsubjectaccessreviews", "create", ""},
	}
	for _, review := range reviews {
		allowed, err := s.selfSubjectAccessReview(ctx, review)
		if err != nil {
			return fmt.Errorf("secure preflight: access review for %s/%s %s: %w", review.group, review.resource, review.verb, err)
		}
		if !allowed {
			return secureNeedsHuman("OperatorRBAC",
				"the operator lacks %s on %s/%s required to provision the secure topology", review.verb, review.group, review.resource)
		}
	}
	return nil
}

// checkBrokerRBAC verifies the broker's named grants two ways: the Role
// object's rule set is compared against the intended rules, and a
// SubjectAccessReview as the broker service account proves the named grants
// exist. Neither proves the absence of extra grants; that rests on the
// namespace/RBAC boundary assumption (HARNESS.md §3).
func (s *SecureControl) checkBrokerRBAC(ctx context.Context, run *courier.CoderRun) error {
	role := topology.BrokerRole(run)
	live := role.DeepCopy()
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: role.Name}, live); err != nil {
		return fmt.Errorf("secure preflight: read broker Role: %w", err)
	}
	if !reflect.DeepEqual(live.Rules, role.Rules) {
		return secureNeedsHuman("BrokerRBAC",
			"broker Role %s/%s does not match the intended named grant set", run.Namespace, role.Name)
	}
	reviews := []accessReview{
		{courier.GroupVersion.Group, "coderruns", "get", run.Name},
		{courier.GroupVersion.Group, "coderruns/status", "get", run.Name},
		{courier.GroupVersion.Group, "coderruns/status", "patch", run.Name},
		{"", "pods", "get", ""},
		{"", "pods", "list", ""},
		{"authentication.k8s.io", "tokenreviews", "create", ""},
	}
	for _, review := range reviews {
		allowed, err := s.subjectAccessReviewAs(ctx, topology.BrokerSAName(run.Name), review)
		if err != nil {
			return fmt.Errorf("secure preflight: broker access review for %s/%s %s: %w", review.group, review.resource, review.verb, err)
		}
		if !allowed {
			return secureNeedsHuman("BrokerRBAC",
				"the broker service account lacks %s on %s/%s", review.verb, review.group, review.resource)
		}
	}
	return nil
}

func (s *SecureControl) selfSubjectAccessReview(ctx context.Context, review accessReview) (bool, error) {
	if s.Config.Kubernetes == nil {
		return false, errors.New("secure preflight: no Kubernetes clientset configured")
	}
	sar, err := s.Config.Kubernetes.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: s.Config.RunNamespace,
				Verb:      review.verb,
				Group:     review.group,
				Resource:  review.resource,
				Name:      review.name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return sar.Status.Allowed && !sar.Status.Denied, nil
}

func (s *SecureControl) subjectAccessReviewAs(ctx context.Context, serviceAccount string, review accessReview) (bool, error) {
	if s.Config.Kubernetes == nil {
		return false, errors.New("secure preflight: no Kubernetes clientset configured")
	}
	sar, err := s.Config.Kubernetes.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: "system:serviceaccount:" + s.Config.RunNamespace + ":" + serviceAccount,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: s.Config.RunNamespace,
				Verb:      review.verb,
				Group:     review.group,
				Resource:  review.resource,
				Name:      review.name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return false, err
	}
	return sar.Status.Allowed && !sar.Status.Denied, nil
}

// checkDryRuns proves what the cluster's API admits: the operator's rendered
// pod specs create under dry-run, and known-bad specs (host path, privileged,
// host networking) are rejected. A bad spec that the API admits is permanent
// evidence that the configured admission cannot hold the boundary.
func (s *SecureControl) checkDryRuns(ctx context.Context, run *courier.CoderRun, cache *topology.CachePin) error {
	registration, err := s.Config.Registry.Select(run.Spec.Repo)
	if err != nil {
		return secureNeedsHuman("ProviderSelection", "provider selection failed: %v", err)
	}
	publicKey, err := s.renderedWorkerPublicKey(ctx, run)
	if err != nil {
		return err
	}
	pods := []*corev1.Pod{}
	worker, err := topology.WorkerPod(run, s.Config.HarnessImage, "dry-run-incarnation", publicKey, cache)
	if err != nil {
		return fmt.Errorf("secure preflight: render worker: %w", err)
	}
	pods = append(pods, worker)
	control, err := topology.ControlPod(run, topology.ControlInputs{
		Image:                 s.Config.HarnessImage,
		ControlSAUID:          "dry-run-sa",
		WorkerPodUID:          "dry-run-worker",
		ControlIncarnationUID: "dry-run-incarnation",
		WorkerURL:             "http://dry-run:8080",
		BrokerURL:             "https://dry-run:8443",
	})
	if err != nil {
		return fmt.Errorf("secure preflight: render control: %w", err)
	}
	pods = append(pods, control)
	broker, err := topology.BrokerPod(run, s.Config.HarnessImage, registration.Projection(), "dry-run-sa")
	if err != nil {
		return fmt.Errorf("secure preflight: render broker: %w", err)
	}
	pods = append(pods, broker)

	for _, pod := range pods {
		dry := pod.DeepCopy()
		dry.Name = dry.Name + "-dryrun"
		dry.GenerateName = ""
		if err := s.Client.Create(ctx, dry, ctrlclient.DryRunAll); err != nil {
			return fmt.Errorf("secure preflight: dry-run create of %s failed: %w", pod.Name, err)
		}
	}

	bad := func(mutate func(*corev1.Pod)) *corev1.Pod {
		p := worker.DeepCopy()
		p.Name = p.Name + "-bad-dryrun"
		p.Spec.Volumes = append([]corev1.Volume(nil), p.Spec.Volumes...)
		p.Spec.Containers[0].VolumeMounts = append([]corev1.VolumeMount(nil), p.Spec.Containers[0].VolumeMounts...)
		mutate(p)
		return p
	}
	badSpecs := []*corev1.Pod{
		bad(func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name:         "host-etc",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/etc"}},
			})
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{Name: "host-etc", MountPath: "/host-etc"})
		}),
		bad(func(p *corev1.Pod) {
			privileged := true
			p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
				Privileged: &privileged,
			}
		}),
		bad(func(p *corev1.Pod) {
			p.Spec.HostNetwork = true
		}),
	}
	for _, pod := range badSpecs {
		err := s.Client.Create(ctx, pod, ctrlclient.DryRunAll)
		if err == nil {
			return secureNeedsHuman("AdmissionBroken",
				"the cluster API admitted a known-bad pod spec (%s); secure mode requires enforced restricted admission", pod.Name)
		}
	}
	return nil
}

// renderedWorkerPublicKey returns the base64 public key the current signing
// incarnation would provision, for dry-run rendering only.
func (s *SecureControl) renderedWorkerPublicKey(ctx context.Context, run *courier.CoderRun) ([]byte, error) {
	secret := &corev1.Secret{}
	err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: topology.SigningSecretName(run.Name)}, secret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// No incarnation yet: render with a placeholder key; the dry-run
			// checks admission, not identity.
			return make([]byte, 32), nil
		}
		return nil, fmt.Errorf("secure preflight: read signing secret: %w", err)
	}
	raw, ok := secret.Data["signing.key"]
	if !ok || len(raw) != 64 {
		return nil, fmt.Errorf("secure preflight: signing secret has no usable key")
	}
	return []byte(raw[32:]), nil
}

// runLiveProbes executes the multi-node live deny/allow probe phase. Probe
// pods carry the worker's labels, so the worker's own NetworkPolicy — not an
// exception — is what the probes exercise.
func (s *SecureControl) runLiveProbes(ctx context.Context, run *courier.CoderRun, cache *topology.CachePin) error {
	nodes := &corev1.NodeList{}
	if err := s.Client.List(ctx, nodes); err != nil {
		return fmt.Errorf("secure preflight: list nodes: %w", err)
	}
	eligible := eligibleNodes(nodes)
	if len(eligible) == 0 {
		return errors.New("secure preflight: no eligible nodes to prove network enforcement")
	}

	apiIP, err := s.serviceClusterIP(ctx, "default", "kubernetes")
	if err != nil {
		return fmt.Errorf("secure preflight: locate the API server service: %w", err)
	}
	dnsIP, err := s.serviceClusterIP(ctx, "kube-system", "kube-dns")
	if err != nil {
		return fmt.Errorf("secure preflight: locate the cluster DNS service: %w", err)
	}
	brokerIP, err := s.serviceClusterIP(ctx, run.Namespace, topology.BrokerServiceName(run.Name))
	if err != nil {
		return fmt.Errorf("secure preflight: locate the broker service: %w", err)
	}

	checks := []probeCheck{
		{Name: "api-server", IP: apiIP, Port: 443, Expect: "deny"},
		{Name: "cluster-dns", IP: dnsIP, Port: 53, Expect: "deny"},
		{Name: "cloud-metadata", IP: "169.254.169.254", Port: 80, Expect: "deny"},
		{Name: "broker-service", IP: brokerIP, Port: 8443, Expect: "deny"},
	}
	if cache != nil && cache.IP != "" {
		checks = append(checks, probeCheck{Name: "dependency-cache", IP: cache.IP, Port: cache.Port, Expect: "allow"})
	}

	script := probeScript(checks)
	timeout := s.Config.ProbeTimeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, node := range eligible {
		pod := topology.ProbePod(run, node, s.Config.ProbeImage, script)
		if err := s.Client.Delete(ctx, pod); err == nil {
			// A leftover probe from an earlier attempt: wait for its
			// termination below instead of failing on a name conflict.
		}
		if err := s.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("secure preflight: create probe pod on %s: %w", node, err)
		}
	}

	deadline := time.Now().Add(timeout)
	for {
		outcomes, pending, err := s.collectProbeResults(ctx, run, eligible)
		if err != nil {
			return err
		}
		if !pending {
			for _, node := range eligible {
				pod := topology.ProbePod(run, node, s.Config.ProbeImage, "")
				_ = ctrlclient.IgnoreNotFound(s.Client.Delete(ctx, pod))
			}
			for _, outcome := range outcomes {
				check := findCheck(checks, outcome.Name)
				if check == nil {
					return fmt.Errorf("secure preflight: probe reported unknown check %q", outcome.Name)
				}
				if outcome.Got != check.Expect {
					if check.Expect == "allow" {
						// An unreachable allowed path is a transient
						// dependency failure, not broken isolation.
						return fmt.Errorf("secure preflight: probe %q expected %s but observed %s",
							check.Name, check.Expect, outcome.Got)
					}
					return secureNeedsHuman("IsolationBroken",
						"live probe %q expected %s but observed %s: worker network isolation is not enforced",
						check.Name, check.Expect, outcome.Got)
				}
			}
			return nil
		}
		if probeCtx.Err() != nil || time.Now().After(deadline) {
			return errors.New("secure preflight: live probes did not complete in time")
		}
		select {
		case <-probeCtx.Done():
			return errors.New("secure preflight: live probes did not complete in time")
		case <-time.After(2 * time.Second):
		}
	}
}

func findCheck(checks []probeCheck, name string) *probeCheck {
	for i := range checks {
		if checks[i].Name == name {
			return &checks[i]
		}
	}
	return nil
}

// collectProbeResults reads every probe pod's termination message. It returns
// the outcomes and whether any probe is still pending.
func (s *SecureControl) collectProbeResults(ctx context.Context, run *courier.CoderRun, nodes []string) ([]probeOutcome, bool, error) {
	var outcomes []probeOutcome
	pending := false
	for _, node := range nodes {
		pod := &corev1.Pod{}
		name := topology.ProbePod(run, node, s.Config.ProbeImage, "").Name
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				pending = true
				continue
			}
			return nil, false, fmt.Errorf("secure preflight: read probe pod on %s: %w", node, err)
		}
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			pending = true
			continue
		}
		message := ""
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.Name == "probe" && cs.State.Terminated != nil {
				message = cs.State.Terminated.Message
			}
		}
		for _, line := range strings.Split(strings.TrimSpace(message), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var outcome probeOutcome
			if err := json.Unmarshal([]byte(line), &outcome); err != nil {
				return nil, false, fmt.Errorf("secure preflight: probe on %s produced an unreadable result: %w", node, err)
			}
			outcomes = append(outcomes, outcome)
		}
	}
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Name < outcomes[j].Name })
	return outcomes, pending, nil
}

func (s *SecureControl) serviceClusterIP(ctx context.Context, namespace, name string) (string, error) {
	service := &corev1.Service{}
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, service); err != nil {
		return "", err
	}
	if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == "None" {
		return "", fmt.Errorf("service %s/%s has no ClusterIP", namespace, name)
	}
	return service.Spec.ClusterIP, nil
}

// eligibleNodes returns schedulable, ready node names.
func eligibleNodes(nodes *corev1.NodeList) []string {
	var out []string
	for i := range nodes.Items {
		node := &nodes.Items[i]
		ready := false
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if !ready {
			continue
		}
		unschedulable := false
		for _, taint := range node.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
				unschedulable = true
			}
		}
		if unschedulable {
			continue
		}
		out = append(out, node.Name)
	}
	sort.Strings(out)
	return out
}

// probeScript renders the busybox shell script one probe pod runs. Each
// result line is a compact JSON object written to the termination log.
func probeScript(checks []probeCheck) string {
	var b strings.Builder
	b.WriteString("results=/tmp/probe-results; : > $results\n")
	b.WriteString("check() { name=$1; ip=$2; port=$3; expect=$4; ")
	b.WriteString("if nc -z -w 2 \"$ip\" \"$port\" >/dev/null 2>&1; then got=allow; else got=deny; fi; ")
	b.WriteString(`printf '{"name":"%s","got":"%s"}\n' "$name" "$got" >> $results; }` + "\n")
	for _, check := range checks {
		fmt.Fprintf(&b, "check %s %s %d %s\n", check.Name, check.IP, check.Port, check.Expect)
	}
	b.WriteString("cp $results /dev/termination-log\n")
	return b.String()
}
