package broker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/branch"
)

// splitOwnerRepo splits a "owner/name" repository identity on the
// final separator. Empty parts, a leading separator, or a trailing
// separator fail. The function is local to the broker package to keep
// the helper near the binding code that needs it.
func splitOwnerRepo(value string) (owner, name string, ok bool) {
	value = strings.TrimSpace(value)
	idx := strings.LastIndex(value, "/")
	if idx <= 0 || idx == len(value)-1 {
		return "", "", false
	}
	return value[:idx], value[idx+1:], true
}

// PolicyFromRun binds broker policy to the live run and to the registered
// provider configuration selected by trusted startup configuration. Provider is
// the registered configuration name, not its credential reference. This function
// validates persisted pins against the immutable spec where derivation is
// possible; its caller supplies the trusted provider's canonical identity for
// spec.repo, which must match the persisted BaseRepo. Do not use mutable
// status.branch/headRepo as policy input: live provider observations determine
// world state, while these fields are neither authority nor fences.
func PolicyFromRun(run *v1alpha1.CoderRun, providerConfigRef, canonicalSpecRepo string) (Policy, error) {
	if run == nil {
		return Policy{}, errors.New("broker binding: run is required")
	}
	if providerConfigRef == "" || canonicalSpecRepo == "" {
		return Policy{}, errors.New("broker binding: trusted provider config ref and canonical spec repository are required")
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
	if persisted.BaseRepo == "" || persisted.BaseRepo != canonicalSpecRepo {
		return Policy{}, errors.New("broker binding: persisted base repository does not match canonical spec repository")
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
		// The trusted source-issue identity comes from the immutable
		// spec, never from a model input. Spec.Repo is the canonical
		// source repository (already validated against the provider
		// identity above) and Spec.Ref is the issue number. Wiring
		// it here means the broker is the only component that
		// reasons about PR-body linkage against this identity.
		owner, name, ok := splitOwnerRepo(persisted.BaseRepo)
		if !ok {
			return Policy{}, fmt.Errorf("broker binding: persisted base repository %q is not an owner/name", persisted.BaseRepo)
		}
		policy.SourceIssue = SourceIssue{Owner: owner, Name: name, Number: run.Spec.Ref}
		// A persisted source-issue pin is cross-checked against the
		// spec-derived identity: the pin describes the same issue the
		// run's immutable spec names, or the policy is corrupt.
		if persisted.SourceIssue != nil {
			if persisted.SourceIssue.Owner != owner || persisted.SourceIssue.Name != name || persisted.SourceIssue.Number != run.Spec.Ref {
				return Policy{}, errors.New("broker binding: persisted source-issue identity does not match the spec-derived identity")
			}
		}
		// CriterionSetFromPolicy owns the pin-tuple integrity: only the
		// fully absent tuple is a pre-contract policy; anything partial or
		// tampered fails the binding here.
		set, pinned, err := CriterionSetFromPolicy(persisted)
		if err != nil {
			return Policy{}, err
		}
		if pinned {
			policy.Criteria = set
		}
	case ModeFixPR:
		if persisted.PRNumber != run.Spec.Ref || persisted.HeadAnchorOID == "" {
			return Policy{}, errors.New("broker binding: fix-pr policy does not match the requested PR and head anchor")
		}
		if persisted.WorkInitiallyAbsent || persisted.WorkOID == "" || persisted.WorkOID != persisted.HeadAnchorOID {
			return Policy{}, errors.New("broker binding: fix-pr work tip must equal the full PR head anchor")
		}
		// A fix-pr run pins nothing: its contract is the existing PR, and a
		// policy that carries resolve-issue criterion pins is corrupt.
		if persisted.SourceIssue != nil || len(persisted.AcceptanceCriteria) > 0 || persisted.AcceptanceCriteriaDigest != "" {
			return Policy{}, errors.New("broker binding: fix-pr policy carries resolve-issue criterion pins")
		}
	}

	if err := validatePolicy(policy); err != nil {
		return Policy{}, fmt.Errorf("broker binding: %w", err)
	}
	return policy, nil
}

// CriterionSetFromPolicy reconstructs the pinned acceptance-criterion set
// from a persisted publication policy. pinned is false only when the pin
// tuple is fully absent — no source identity, no criteria, no set digest —
// which is the legal pre-contract state for fix-pr runs and policies
// admitted before #280. Any orphaned combination (criteria or identity
// without a digest, digest without an identity) is corruption and fails
// closed rather than silently downgrading to an unpinned policy. A pinned
// set is verified like every other policy field: each criterion's digest is
// recomputed from its persisted text, IDs must remain the extraction
// ordinals, and the set digest is recomputed over the trusted issue
// identity plus every criterion digest in order. A tampered or corrupted
// pin fails closed instead of becoming the target a publication boundary
// validates against.
func CriterionSetFromPolicy(persisted *v1alpha1.PublicationPolicy) (set CriterionSet, pinned bool, err error) {
	if persisted == nil {
		return CriterionSet{}, false, nil
	}
	if persisted.AcceptanceCriteriaDigest == "" {
		if persisted.SourceIssue != nil || len(persisted.AcceptanceCriteria) > 0 {
			return CriterionSet{}, false, errors.New("broker binding: orphaned criterion pin fields: a pre-contract policy must carry no source identity, no criteria, and no set digest")
		}
		return CriterionSet{}, false, nil
	}
	if persisted.SourceIssue == nil {
		return CriterionSet{}, false, errors.New("broker binding: pinned criterion set carries no source-issue identity")
	}
	if len(persisted.AcceptanceCriteria) > MaxCriteria {
		return CriterionSet{}, false, fmt.Errorf("broker binding: pinned criterion set carries %d criteria; the bound is %d", len(persisted.AcceptanceCriteria), MaxCriteria)
	}
	set = CriterionSet{
		Issue: SourceIssue{
			Owner:  persisted.SourceIssue.Owner,
			Name:   persisted.SourceIssue.Name,
			Number: persisted.SourceIssue.Number,
		},
		Criteria: make([]Criterion, 0, len(persisted.AcceptanceCriteria)),
	}
	for i, criterion := range persisted.AcceptanceCriteria {
		if criterion.ID != fmt.Sprintf("AC-%d", i+1) {
			return CriterionSet{}, false, fmt.Errorf("broker binding: pinned criterion %d carries ID %q; extraction ordinals are immutable", i+1, criterion.ID)
		}
		if len(criterion.Text) > MaxCriterionTextBytes {
			return CriterionSet{}, false, fmt.Errorf("broker binding: pinned criterion %s exceeds the %d-byte text bound", criterion.ID, MaxCriterionTextBytes)
		}
		if digestString(criterion.Text) != criterion.Digest {
			return CriterionSet{}, false, fmt.Errorf("broker binding: pinned criterion %s text does not match its digest", criterion.ID)
		}
		set.Criteria = append(set.Criteria, Criterion{ID: criterion.ID, Text: criterion.Text, Digest: criterion.Digest})
	}
	computed := set.digest()
	if computed != persisted.AcceptanceCriteriaDigest {
		return CriterionSet{}, false, errors.New("broker binding: pinned criterion set digest does not cover the pinned criteria")
	}
	set.Digest = computed
	return set, true, nil
}
