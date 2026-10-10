package broker

import (
	"errors"
	"strings"
	"testing"
)

const (
	linkageOwner = "misospace"
	linkageName  = "miso-gallery"
	linkageNum   = 502
)

func trustedIssue() SourceIssue {
	return SourceIssue{Owner: linkageOwner, Name: linkageName, Number: linkageNum}
}

func TestValidateLinkageAcceptsStandardClosingKeywords(t *testing.T) {
	source := trustedIssue()
	cases := []string{
		"Closes #502",
		"closes #502",
		"CLOSES #502",
		"Fixes #502",
		"fixes #502",
		"Resolve #502",
		"resolves #502",
		"Resolved #502",
		"Closed #502",
		"Fixed #502",
		// A PR body the model will typically write: prose around the
		// supported keyword, with the keyword not at the start of the
		// body.
		"This change ships the icon assets and updates the manifest. Closes #502.\n",
		"  Fixes #502  \n\nThis addresses the icon regression; see the diff for the test fixture.",
		// A body that mentions the keyword more than once must still
		// pass when every reference matches.
		"Closes #502. Also fixes #502 with a follow-up test.",
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			if err := ValidateLinkage(body, source); err != nil {
				t.Fatalf("ValidateLinkage(%q) = %v, want nil", body, err)
			}
		})
	}
}

func TestValidateLinkageAcceptsCrossRepoFormWhenOwnerMatches(t *testing.T) {
	source := trustedIssue()
	cases := []string{
		"Closes misospace/miso-gallery#502",
		"closes misospace/miso-gallery#502",
		"Fixes misospace/miso-gallery#502",
		"Closes  misospace/miso-gallery  #502",
		"Resolves misospace/miso-gallery#502",
		"Closes misospace/miso-gallery#502.\nFollowed by other body content.",
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			if err := ValidateLinkage(body, source); err != nil {
				t.Fatalf("ValidateLinkage(%q) = %v, want nil", body, err)
			}
		})
	}
}

// TestValidateLinkageRejectsVagueAndSubstringMatchesDogfood502PR505 is
// the regression fixture for the miso-gallery dogfood: issue #502 (the
// trusted source) and the published PR #505 whose vague body was the
// original bug.
func TestValidateLinkageRejectsVagueAndSubstringMatchesDogfood502PR505(t *testing.T) {
	source := trustedIssue()
	cases := []string{
		// Vague references that the original dogfood PR used
		// (dogfood: misospace/miso-gallery issue #502, published PR #505).
		"Addresses #502",
		"addresses #502",
		"See also #502",
		"Refs #502",
		"Related to #502",
		"Part of #502",
		// Substring matches that share tokens with supported keywords
		// but are not a closing reference.
		"discloses #502",
		"refixes #502",
		"unresolved #502",
		"closer #502",
		// No issue reference at all.
		"This change ships the icon assets and updates the manifest.",
		"",
		// Reference that is not a digit (forged to test the regex).
		"Closes #five-oh-two",
		"Closes #502a",
		// Empty body with a stray keyword.
		"Closes",
		"Fixes",
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			err := ValidateLinkage(body, source)
			if err == nil {
				t.Fatalf("ValidateLinkage(%q) = nil, want error", body)
			}
			// Every case in this table is a "no match found" or a
			// "no match at all" failure. We don't claim the precise
			// reason here; the structured-error table below covers
			// that.
		})
	}
}

func TestValidateLinkageRejectsWrongIssueNumber(t *testing.T) {
	source := trustedIssue()
	err := ValidateLinkage("Closes #999", source)
	if err == nil {
		t.Fatal("wrong issue number accepted")
	}
	var le *LinkageError
	if !errors.As(err, &le) {
		t.Fatalf("expected *LinkageError, got %T", err)
	}
	if len(le.Observed) != 1 || le.Observed[0].Number != 999 {
		t.Fatalf("observations = %+v, want one observation with #999", le.Observed)
	}
	if le.Observed[0].Match {
		t.Fatal("wrong-number observation marked as a match")
	}
	if !strings.Contains(le.Reason, "#999") {
		t.Fatalf("reason does not name the observed number: %q", le.Reason)
	}
}

func TestValidateLinkageRejectsWrongSourceRepository(t *testing.T) {
	source := trustedIssue()
	err := ValidateLinkage("Closes other-org/miso-gallery#502", source)
	if err == nil {
		t.Fatal("wrong source repository accepted")
	}
	var le *LinkageError
	if !errors.As(err, &le) {
		t.Fatalf("expected *LinkageError, got %T", err)
	}
	if len(le.Observed) != 1 || le.Observed[0].Owner != "other-org" {
		t.Fatalf("observations = %+v", le.Observed)
	}
	if !strings.Contains(le.Reason, "other-org") {
		t.Fatalf("reason does not name the wrong repository: %q", le.Reason)
	}
}

func TestValidateLinkageRejectsAmbiguousMultipleReferences(t *testing.T) {
	source := trustedIssue()
	body := "Closes #502. Closes #503."
	err := ValidateLinkage(body, source)
	if err == nil {
		t.Fatal("ambiguous body accepted")
	}
	var le *LinkageError
	if !errors.As(err, &le) {
		t.Fatalf("expected *LinkageError, got %T", err)
	}
	if len(le.Observed) != 2 {
		t.Fatalf("observations = %+v, want 2", le.Observed)
	}
	if le.Observed[0].Match != true || le.Observed[1].Match != false {
		t.Fatalf("expected exactly one matching observation, got %+v", le.Observed)
	}
	if !strings.Contains(le.Reason, "multiple") {
		t.Fatalf("reason does not call out the ambiguity: %q", le.Reason)
	}
}

func TestValidateLinkageRejectsEmptyBody(t *testing.T) {
	err := ValidateLinkage("", trustedIssue())
	if err == nil {
		t.Fatal("empty body accepted")
	}
	if !errors.Is(err, ErrLinkageMissing) {
		t.Fatalf("err = %v, want ErrLinkageMissing", err)
	}
}

func TestValidateLinkageRejectsUnsetSourceIdentity(t *testing.T) {
	if err := ValidateLinkage("Closes #502", SourceIssue{}); err == nil {
		t.Fatal("unset source identity accepted")
	}
	if err := ValidateLinkage("Closes #502", SourceIssue{Owner: "x", Name: "y", Number: 0}); err == nil {
		t.Fatal("zero issue number accepted")
	}
}

func TestValidateLinkageTreatsBareAndCrossRepoAsEquivalentWhenOwnerMatches(t *testing.T) {
	source := trustedIssue()
	// A body that mixes both forms of the same trusted reference must
	// be accepted: the cross-repo form is redundant but not
	// conflicting.
	body := "Closes #502. (Same as Closes misospace/miso-gallery#502.)"
	if err := ValidateLinkage(body, source); err != nil {
		t.Fatalf("redundant cross-repo reference rejected: %v", err)
	}
}

func TestValidateLinkageRejectsBodyWithNoClosingReference(t *testing.T) {
	source := trustedIssue()
	err := ValidateLinkage("This change ships the icon assets and updates the manifest. No closing reference here.", source)
	if err == nil {
		t.Fatal("body with no closing reference accepted")
	}
	if !errors.Is(err, ErrLinkageMissing) {
		t.Fatalf("err = %v, want ErrLinkageMissing", err)
	}
}
