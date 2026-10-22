package executor_test

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/topology"
)

// prefixSharedNames are two supported CoderRun names of 64+ characters that
// share their first 63 characters: the legacy truncating scheme rendered the
// same run label for both, so one run's pods were attributable to the other.
var (
	labelOverflowPrefix = strings.Repeat("a", 63)
	overflowRunOne      = labelOverflowPrefix + "-one"
	overflowRunTwo      = labelOverflowPrefix + "-two"
)

func TestRunLabelValueDistinguishesRunsSharingAPrefix(t *testing.T) {
	one := executor.RunLabelValue(overflowRunOne)
	two := executor.RunLabelValue(overflowRunTwo)
	if one == two {
		t.Fatalf("runs sharing a 63-character prefix collide on the run label: %q", one)
	}
	for name, value := range map[string]string{overflowRunOne: one, overflowRunTwo: two} {
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			t.Fatalf("RunLabelValue(%q) = %q is not a valid label value: %v", name, value, errs)
		}
		if again := executor.RunLabelValue(name); again != value {
			t.Fatalf("RunLabelValue(%q) is not stable: %q then %q", name, value, again)
		}
	}
	if executor.LegacyRunLabelValue(overflowRunOne) != executor.LegacyRunLabelValue(overflowRunTwo) {
		t.Fatal("test precondition: the legacy scheme must still truncate without a digest")
	}
}

func TestRunLabelValueMatchesTopologyScheme(t *testing.T) {
	// The two packages hold the bounded scheme in parallel (neither can
	// import the other); controller readers rely on the values agreeing.
	for _, name := range []string{
		"short", "run.test", "Run_1.2", "Bad Name!", "__", "",
		labelOverflowPrefix,
		labelOverflowPrefix + "x",
		labelOverflowPrefix + "y",
		overflowRunOne,
		overflowRunTwo,
		strings.Repeat("z", 253),
	} {
		if got, want := executor.RunLabelValue(name), topology.RunLabelValue(name); got != want {
			t.Fatalf("RunLabelValue(%q): executor %q != topology %q", name, got, want)
		}
	}
}

func TestRunLabelValuePassesShortValidNamesThrough(t *testing.T) {
	for _, name := range []string{"short", "run.1_2-x", labelOverflowPrefix} {
		if got := executor.RunLabelValue(name); got != name {
			t.Fatalf("valid bounded names must pass through unchanged: %q -> %q", name, got)
		}
	}
}
