package main

import (
	"context"
	"testing"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/controller"
)

type observerWithHeadResolver struct{}

func (observerWithHeadResolver) Observe(context.Context, string, string) (controller.PRObservation, error) {
	return controller.PRObservation{}, nil
}

func (observerWithHeadResolver) ResolveHead(context.Context, *courierv1alpha1.CoderRun) (string, error) {
	return "feature/existing", nil
}

func TestExistingPRHeadResolver(t *testing.T) {
	observer := observerWithHeadResolver{}
	if got := existingPRHeadResolver(observer); got == nil {
		t.Fatal("existingPRHeadResolver() returned nil for a compatible observer")
	}
	if got := existingPRHeadResolver(nil); got != nil {
		t.Fatal("existingPRHeadResolver(nil) returned a resolver")
	}
}
