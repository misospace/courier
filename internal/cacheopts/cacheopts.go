// Package cacheopts builds the manager's informer-cache configuration.
//
// The configuration lives here rather than in the manager's entrypoint so
// the controller tests can exercise the same cache the manager runs: the
// bounded-memory evidence for #642 — a provisioning pass that starts no
// informer for a kind the chart denies list/watch — is only meaningful
// against the production options, not a copy of them.
package cacheopts

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// Options scopes the manager's informers to the watched namespaces (#632):
// CoderRun, LaneProfile and Pod are the only kinds the cache
// informer-caches, listed and watched only there, so a side-by-side
// release neither observes nor can be woken by another deployment's runs,
// lanes, and run pods. Every other kind must be read point-wise through
// the uncached API reader (SecureControl.reader), never through the cached
// client: the chart grants list and watch for the three cached kinds, only
// list for Secrets, Services, ServiceAccounts, NetworkPolicies, and the
// RBAC objects, get and list for Nodes, and only get for Namespaces. A
// cached read of any other kind therefore lazily starts an informer whose
// initial list pulls every object of that kind into the operator's memory
// and which cannot watch, so it relists forever — the #642 OOM.
//
// DefaultNamespaces bounds such an accidental informer for a namespaced kind
// to the watched namespaces instead of the whole cluster. Cluster-scoped
// kinds (Node, Namespace) stay cluster-wide in any case: the API server
// holds them outside every namespace.
//
// An empty watched set leaves the cache unrestricted — the single-controller
// default.
func Options(watched []string) cache.Options {
	if len(watched) == 0 {
		return cache.Options{}
	}
	namespaces := make(map[string]cache.Config, len(watched))
	for _, namespace := range watched {
		namespaces[namespace] = cache.Config{}
	}
	byNamespace := cache.ByObject{Namespaces: namespaces}
	return cache.Options{
		DefaultNamespaces: namespaces,
		ByObject: map[client.Object]cache.ByObject{
			&courierv1alpha1.CoderRun{}:    byNamespace,
			&courierv1alpha1.LaneProfile{}: byNamespace,
			&corev1.Pod{}:                  byNamespace,
		},
	}
}
