package broker

import (
	"errors"
	"fmt"

	"github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
)

// PolicyFromRun binds broker policy to the live run and to the registered
// provider configuration selected by trusted startup configuration. Provider is
// the registered configuration name, not its credential reference. This function
// validates persisted pins against the immutable spec where derivation is
// possible; its caller must use the trusted provider to resolve spec.repo and
// verify that its canonical identity matches the pinned BaseRepo. Do not use
// mutable status.branch/headRepo as policy input: live provider observations
// determine world state, while these fields are neither authority nor fences.
func PolicyFromRun(run *v1alpha1.CoderRun, providerConfigRef string) (Policy, error) {
	if run == nil {
		return Policy{}, errors.New("broker binding: run is required")
	}
	if providerConfigRef == "" {
		return Policy{}, errors.New("broker binding: trusted provider config ref is required")
	}
	persisted := run.Status.PublicationPolicy
	if persisted == nil {
		return Policy{}, errors.New("broker binding: persisted publication policy is required")
	}
	if string(run.UID) == "" || persisted.RunUID != string(run.UID) {
		return Policy{}, errors.New("broker binding: publication policy run UID does not match live run")
	}
	if persisted.ProviderConfigRef != providerConfigRef {
		return Policy{}, errors.New("broker binding: publication policy provider does not match trusted config")
	}

	mode := Mode(run.Spec.Mode)
	if mode != ModeResolveIssue && mode != ModeFixPR {
		return Policy{}, fmt.Errorf("broker binding: unsupported run mode %q", run.Spec.Mode)
	}
	if run.Spec.Repo == "" || run.Spec.Ref < 1 {
		return Policy{}, errors.New("broker binding: run spec repository and positive ref are required")
	}
	if persisted.BaseRepo == "" || persisted.BaseRef == "" || persisted.BaseOID == "" ||
		persisted.WorkRepo == "" || persisted.WorkRef == "" {
		return Policy{}, errors.New("broker binding: persisted base and work identities are incomplete")
	}

	policy := Policy{
		RunUID:              persisted.RunUID,
		Mode:                mode,
		Provider:            persisted.ProviderConfigRef,
		BaseRepo:            persisted.BaseRepo,
		BaseRef:             persisted.BaseRef,
		BaseOID:             persisted.BaseOID,
		WorkRepo:            persisted.WorkRepo,
		WorkRef:             persisted.WorkRef,
		WorkInitiallyAbsent: persisted.WorkInitiallyAbsent,
		WorkAnchorOID:       persisted.WorkOID,
		PRNumber:            persisted.PRNumber,
		HeadAnchorOID:       persisted.HeadAnchorOID,
	}

	switch mode {
	case ModeResolveIssue:
		derived := branch.ResolveIssue(persisted.BaseRepo, run.Spec.Ref)
		if derived == "" || persisted.WorkRepo != persisted.BaseRepo || persisted.WorkRef != derived {
			return Policy{}, errors.New("broker binding: resolve-issue work identity does not match derived destination")
		}
		if persisted.PRNumber != 0 || persisted.HeadAnchorOID != "" {
			return Policy{}, errors.New("broker binding: resolve-issue policy contains fix-pr anchors")
		}
		if persisted.WorkInitiallyAbsent && persisted.WorkOID != "" ||
			!persisted.WorkInitiallyAbsent && persisted.WorkOID == "" {
			return Policy{}, errors.New("broker binding: resolve-issue policy has inconsistent initial work tip")
		}
	case ModeFixPR:
		if persisted.PRNumber != run.Spec.Ref || persisted.HeadAnchorOID == "" {
			return Policy{}, errors.New("broker binding: fix-pr policy does not match the requested PR and head anchor")
		}
		if persisted.WorkInitiallyAbsent || persisted.WorkOID == "" || persisted.WorkOID != persisted.HeadAnchorOID {
			return Policy{}, errors.New("broker binding: fix-pr work tip must equal the full PR head anchor")
		}
	}

	if err := validatePolicy(policy); err != nil {
		return Policy{}, fmt.Errorf("broker binding: %w", err)
	}
	return policy, nil
}
