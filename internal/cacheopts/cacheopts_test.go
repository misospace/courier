package cacheopts

import (
	"reflect"
	"testing"
)

func TestOptionsScopeRunKinds(t *testing.T) {
	if opts := Options(nil); len(opts.ByObject) != 0 {
		t.Fatalf("empty watch set restricted %d object kinds, want unrestricted default", len(opts.ByObject))
	}
	opts := Options([]string{"runs-a", "courier-system"})
	want := map[string]bool{
		"CoderRun":    false,
		"LaneProfile": false,
		"Pod":         false,
	}
	for object, scoped := range opts.ByObject {
		kind := reflect.TypeOf(object).Elem().Name()
		if _, ok := want[kind]; !ok {
			t.Fatalf("cache restricted unexpected kind %s; the cache informer-caches only CoderRun, LaneProfile, and Pod", kind)
		}
		want[kind] = true
		if len(scoped.Namespaces) != 2 {
			t.Fatalf("kind %s watches %d namespaces, want watched set", kind, len(scoped.Namespaces))
		}
		for _, namespace := range []string{"runs-a", "courier-system"} {
			if _, ok := scoped.Namespaces[namespace]; !ok {
				t.Fatalf("kind %s does not watch %q", kind, namespace)
			}
		}
	}
	for kind, seen := range want {
		if !seen {
			t.Fatalf("cache did not restrict %s", kind)
		}
	}
	if len(opts.DefaultNamespaces) != 2 {
		t.Fatalf("DefaultNamespaces has %d entries, want watched set bounding any accidental informer for an unregistered kind", len(opts.DefaultNamespaces))
	}
	for _, namespace := range []string{"runs-a", "courier-system"} {
		if _, ok := opts.DefaultNamespaces[namespace]; !ok {
			t.Fatalf("DefaultNamespaces does not bound %q", namespace)
		}
	}
}

// TestUncachedReadObjectsCoverNoWatchKinds guards the invariant that aligns
// the manager client's reads with the shipped chart RBAC (#641): every kind
// read by-name during secure admission, preflight or provisioning must be
// excluded from the informer cache (the chart grants those kinds get/list but
// not watch), while the three kinds the chart does grant watch on must stay
// cached and out of the uncached set.
func TestUncachedReadObjectsCoverNoWatchKinds(t *testing.T) {
	// Kinds the chart grants get/list but never watch, all point-read by the
	// secure path. A read of any of these through the cache starts a forbidden
	// cluster-wide informer and wedges, so each must appear here.
	wantUncached := map[string]bool{
		"Secret":         false,
		"Service":        false,
		"ServiceAccount": false,
		"Namespace":      false,
		"Node":           false,
		"Role":           false,
	}
	// Kinds the chart grants watch on and the cache legitimately holds. They
	// must never be forced uncached: doing so would drop the watch-driven
	// reconciliation that wake-on-event correctness depends on.
	watched := map[string]bool{"CoderRun": true, "LaneProfile": true, "Pod": true}
	for _, object := range UncachedReadObjects() {
		kind := reflect.TypeOf(object).Elem().Name()
		if watched[kind] {
			t.Fatalf("%s is watched and cached but also listed uncached; the cache and uncached-read sets must stay disjoint", kind)
		}
		if _, ok := wantUncached[kind]; !ok {
			t.Fatalf("unexpected uncached kind %s; only chart get/list-only kinds read by the secure path belong here", kind)
		}
		wantUncached[kind] = true
	}
	for kind, seen := range wantUncached {
		if !seen {
			t.Fatalf("kind %s is read by-name on the secure path but is not disabled from the cache; its first read would wedge a forbidden informer", kind)
		}
	}
}
