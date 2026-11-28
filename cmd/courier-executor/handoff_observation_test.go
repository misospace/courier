package main

import "testing"

// TestNoChangeMergeStateRejection pins #946: a false mergeability answer
// names no reason by itself, so each forge merge state must be worded by
// what it proves — only "dirty" may be reported as a conflict.
func TestNoChangeMergeStateRejection(t *testing.T) {
	cases := []struct {
		name  string
		state string
		want  string
	}{
		{"conflict", "dirty", "acme/widgets#7 still conflicts with base main"},
		{"blocked", "blocked", "acme/widgets#7 is blocked against base main"},
		{"behind names its state", "behind", `acme/widgets#7 is not mergeable into base main (merge state "behind")`},
		{"unstable names its state", "unstable", `acme/widgets#7 is not mergeable into base main (merge state "unstable")`},
		{"draft names its state", "draft", `acme/widgets#7 is not mergeable into base main (merge state "draft")`},
		{"empty names no state", "", "acme/widgets#7 is not mergeable into base main"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := noChangeMergeStateRejection("acme/widgets", 7, "main", tc.state)
			want := "opencode declared no change needed but pull request " + tc.want + "; the assignment is not satisfied"
			if got != want {
				t.Fatalf("rejection = %q, want %q", got, want)
			}
		})
	}
}
