package grammar

import (
	"strings"
	"testing"
)

// TestSubstitutionKeysAreUnambiguous pins the invariant derivedGrammar
// depends on: at the moment a key is applied it must occur exactly once in
// the template the earlier substitutions have left, and no key may be a
// substring of another key. A key an earlier substitution already consumed
// occurs zero times and would be substituted away silently; `(\d+)` is a
// substring of the header's source-reference key, so adding it ahead of that
// key corrupts the header template. Both halves fail loudly here.
func TestSubstitutionKeysAreUnambiguous(t *testing.T) {
	cases := []struct {
		name          string
		template      string
		substitutions []string
	}{
		{"header", strings.TrimSuffix(strings.TrimPrefix(HeaderPattern.String(), "^"), "$"), headerSubstitutions},
		{"row", strings.TrimSuffix(strings.TrimPrefix(RowPattern.String(), "^"), "$"), rowSubstitutions},
	}
	for _, tc := range cases {
		if len(tc.substitutions)%2 != 0 {
			t.Fatalf("%s substitutions: %d entries, want key/placeholder pairs", tc.name, len(tc.substitutions))
		}
		for i := 0; i+1 < len(tc.substitutions); i += 2 {
			for j := 0; j+1 < len(tc.substitutions); j += 2 {
				if i != j && strings.Contains(tc.substitutions[j], tc.substitutions[i]) {
					t.Fatalf("%s: key %d (%q) is a substring of key %d (%q); the substitutions would consume the wrong text", tc.name, i, tc.substitutions[i], j, tc.substitutions[j])
				}
			}
		}
		template := tc.template
		for i := 0; i+1 < len(tc.substitutions); i += 2 {
			key := tc.substitutions[i]
			if got := strings.Count(template, key); got != 1 {
				t.Fatalf("%s: substitution key %d (%q) occurs %d times after the earlier substitutions; want exactly 1: %q", tc.name, i, key, got, template)
			}
			template = strings.ReplaceAll(template, key, tc.substitutions[i+1])
		}
	}
}

// TestReferenceWithLocationIsComposed pins that the validator's reference
// pattern is built from the exported URL and path alternatives rather than
// spelled as an independent literal. A consumer that must probe exactly the
// references the validator accepts — the executor's cited-evidence path
// check (#367, #375 review) — takes ReferencePathPattern itself; editing
// either side without the other fails here instead of drifting silently.
func TestReferenceWithLocationIsComposed(t *testing.T) {
	want := `(?:` + ReferenceURLPattern.String() + `|` + ReferencePathPattern.String() + `)`
	if got := ReferenceWithLocation.String(); got != want {
		t.Fatalf("ReferenceWithLocation = %q, want the composition of the exported alternatives %q", got, want)
	}
}
