package rebase

import (
	"errors"
	"testing"
)

// validateConflictedPaths is the guard every conflicted path passes before
// anything reads or writes through it. The rejection set is exactly the
// shapes that would resolve outside the workspace; the accept set pins that
// legitimate repository-controlled names — including a colon-leading,
// glob-bearing, or spaced name — are never falsely refused.
func TestValidateConflictedPaths(t *testing.T) {
	rejected := []string{
		"",
		"/x",
		"a/../b",
		"a/./b",
		"a//b",
		"./a",
	}
	for _, path := range rejected {
		if err := validateConflictedPaths([]string{path}); err == nil {
			t.Fatalf("validateConflictedPaths(%q) = nil, want refusal", path)
		} else {
			var semantic *semanticError
			if !errors.As(err, &semantic) {
				t.Fatalf("validateConflictedPaths(%q) error = %T, want a semantic stop-for-human conflict", path, err)
			}
		}
	}
	accepted := []string{"a/b.go", ":odd", "glob*.txt", "sp ace.txt"}
	if err := validateConflictedPaths(accepted); err != nil {
		t.Fatalf("validateConflictedPaths(%q) = %v, want no false refusal", accepted, err)
	}
}
