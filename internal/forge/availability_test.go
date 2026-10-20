package forge

import (
	"errors"
	"fmt"
	"testing"
)

// transientStub is a typed error the availability tests mark or leave
// unmarked, standing in for a provider's typed availability errors.
type transientStub struct {
	transient bool
}

func (e *transientStub) Error() string               { return "transient stub" }
func (e *transientStub) TransientAvailability() bool { return e.transient }

// TestIsTransientAvailability pins the predicate contract: nil and plain
// errors are not transient, a marked typed error is, an unmarked typed
// error is not, and wrapping preserves the classification so boundary
// wrappers carrying the original error through still classify.
func TestIsTransientAvailability(t *testing.T) {
	if IsTransientAvailability(nil) {
		t.Fatal("nil error classified as transient availability")
	}
	if IsTransientAvailability(errors.New("plain forge failure")) {
		t.Fatal("untyped error classified as transient availability")
	}
	if IsTransientAvailability(&transientStub{transient: false}) {
		t.Fatal("unmarked typed error classified as transient availability")
	}
	if !IsTransientAvailability(&transientStub{transient: true}) {
		t.Fatal("marked typed error not classified as transient availability")
	}
	wrapped := fmt.Errorf("forge read: %w", &transientStub{transient: true})
	if !IsTransientAvailability(wrapped) {
		t.Fatal("wrapped marked error not classified as transient availability")
	}
	wrappedPlain := fmt.Errorf("forge read: %w", &transientStub{transient: false})
	if IsTransientAvailability(wrappedPlain) {
		t.Fatal("wrapped unmarked error classified as transient availability")
	}
}
