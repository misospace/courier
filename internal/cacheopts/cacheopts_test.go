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
