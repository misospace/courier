// Package forge contains the forge-agnostic contract the core uses to read
// issues and work items and pull/merge requests and to create, update, and
// comment on them. Concrete forge implementations belong in sibling packages
// (for example, internal/github).
//
// The contract exposes no merge or raw-passthrough verb by design: merging is
// a human gate, and a raw passthrough would let a provider smuggle in
// forge-specific behavior the core must never see.
package forge

import (
	"context"
	"errors"
	"sort"
)

// ErrUnsupported is returned when a provider does not support an operation it
// was asked to perform. It lets a provider decline cleanly rather than fake a
// result, and it is the only error the contract defines for capability misses.
var ErrUnsupported = errors.New("forge: operation not supported by provider")

// Capability names one operation a forge provider can register support for.
//
// The vocabulary deliberately contains no merge and no raw-passthrough
// capability. A capability cannot be registered for them, so those verbs are
// unrepresentable.
type Capability string

const (
	CapabilityReadWorkItem      Capability = "read-work-item"
	CapabilityReadPullRequest   Capability = "read-pull-request"
	CapabilityListReviews       Capability = "list-reviews"
	CapabilityListComments      Capability = "list-comments"
	CapabilityReadChecks        Capability = "read-checks"
	CapabilityCreatePullRequest Capability = "create-pull-request"
	CapabilityUpdatePullRequest Capability = "update-pull-request"
	CapabilityComment           Capability = "comment"
)

// Availability is the safe, redacted result of asking whether a capability is
// usable. Detail explains unavailability in one short line and must never
// contain a credential or endpoint secret; empty when available.
type Availability struct {
	Available bool   `json:"available"`
	Detail    string `json:"detail,omitempty"`
}

// available is the diagnostic for a usable capability.
func available() Availability { return Availability{Available: true} }

// unavailable is the diagnostic for a capability that cannot be used now.
// detail is passed through RedactDetail before it is returned so a caller can
// never learn a secret from an unavailability reason.
func unavailable(detail string) Availability {
	return Availability{Available: false, Detail: RedactDetail(detail)}
}

// Capabilities is the set of operations a provider registers, each with a
// safe availability diagnostic. A capability that is registered but not
// currently usable reports Available: false with a redacted detail rather than
// being removed, so callers can distinguish "not offered" from "offered but
// down".
//
// The zero value is usable: Has reports false and Available reports an
// unavailable diagnostic for every capability.
type Capabilities struct {
	entries map[Capability]Availability
}

// NewCapabilities registers the given capabilities as available.
func NewCapabilities(caps ...Capability) Capabilities {
	c := Capabilities{entries: make(map[Capability]Availability, len(caps))}
	for _, capability := range caps {
		c.Register(capability, available())
	}
	return c
}

// Register adds or replaces the diagnostic for one capability. Registering a
// capability does not make an operation exist on the interface; it only
// advertises support and readiness.
func (c *Capabilities) Register(capability Capability, a Availability) {
	if c.entries == nil {
		c.entries = make(map[Capability]Availability)
	}
	if !a.Available {
		a.Detail = RedactDetail(a.Detail)
	}
	c.entries[capability] = a
}

// Clone returns a copy of the capability set that shares no state with the
// original, so a provider can hand out its capabilities without letting a
// caller mutate the provider's internal registration through Register.
func (c Capabilities) Clone() Capabilities {
	out := Capabilities{entries: make(map[Capability]Availability, len(c.entries))}
	for capability, a := range c.entries {
		out.entries[capability] = a
	}
	return out
}

// Has reports whether a capability is registered at all, regardless of whether
// it is currently available.
func (c Capabilities) Has(capability Capability) bool {
	_, ok := c.entries[capability]
	return ok
}

// Available returns the diagnostic for a capability: unavailable when the
// capability is not registered.
func (c Capabilities) Available(capability Capability) Availability {
	if a, ok := c.entries[capability]; ok {
		return a
	}
	return unavailable("capability not registered")
}

// List returns the registered capabilities sorted by name, for stable output.
func (c Capabilities) List() []Capability {
	caps := make([]Capability, 0, len(c.entries))
	for capability := range c.entries {
		caps = append(caps, capability)
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i] < caps[j] })
	return caps
}

// WorkItemRef identifies a work item (issue/ticket) in a repository.
type WorkItemRef struct {
	Repo string
	ID   string
}

// PullRequestRef identifies a pull/merge request in a repository.
type PullRequestRef struct {
	Repo   string
	Number int
}

// WorkItem is a provider-neutral issue or work item read from a forge.
type WorkItem struct {
	ID     string
	Repo   string
	Title  string
	Body   string
	State  string
	Labels []string
	URL    string
}

// PullRequest is a provider-neutral pull/merge request.
type PullRequest struct {
	Number  int
	Repo    string
	Title   string
	Body    string
	State   string
	Draft   bool
	HeadSHA string
	HeadRef string
	BaseRef string
	URL     string
	Merged  bool
}

// Review is a provider-neutral review on a pull request.
type Review struct {
	ID          string
	Author      string
	State       string
	Body        string
	SubmittedAt string
}

// Comment is a provider-neutral comment on a pull request or work item.
type Comment struct {
	ID     string
	Author string
	Body   string
	URL    string
}

// Check is a provider-neutral CI/check result attached to a commit.
type Check struct {
	Name       string
	State      string
	Conclusion string
	URL        string
}

// CreatePullRequestInput is the provider-neutral request to open a pull
// request.
type CreatePullRequestInput struct {
	Repo  string
	Title string
	Head  string
	Base  string
	Body  string
	Draft bool
}

// UpdatePullRequestInput is the provider-neutral request to update a pull
// request. A field left at its zero value is left unchanged by the provider.
type UpdatePullRequestInput struct {
	Title string
	Body  string
	State string
	Draft bool
}

// ProviderConfig is the forge-agnostic configuration for a provider. It is
// deliberately independent of LaneProfile: LaneProfile describes hardware and
// model lanes, never the forge or its credentials.
//
// CredentialRef is a reference that the broker resolves into a real
// credential. It must not itself be a secret, and a resolved credential is
// never returned through this package.
type ProviderConfig struct {
	Name          string
	Endpoint      string
	CredentialRef string
}

// Provider is the forge boundary used by the core. It is the only way the core
// reads or writes forge state. The contract deliberately omits any merge or
// raw-passthrough verb: merging is a human gate, and a raw request would let a
// provider smuggle in forge-specific behavior the core must never see.
//
// A provider implements the operations it supports and returns ErrUnsupported
// for the rest; Capabilities advertises which are usable up front.
type Provider interface {
	// Config returns the provider's forge-agnostic configuration. It never
	// includes a resolved credential.
	Config() ProviderConfig
	// Capabilities reports the operations this provider registers and whether
	// each is currently available.
	Capabilities() Capabilities

	// ReadWorkItem reads a work item (issue/ticket) by reference.
	ReadWorkItem(context.Context, WorkItemRef) (WorkItem, error)
	// ReadPullRequest reads a pull/merge request by reference.
	ReadPullRequest(context.Context, PullRequestRef) (PullRequest, error)
	// ListReviews lists the reviews on a pull/merge request.
	ListReviews(context.Context, PullRequestRef) ([]Review, error)
	// ListComments lists the comments on a pull/merge request.
	ListComments(context.Context, PullRequestRef) ([]Comment, error)
	// ReadChecks lists the CI/check results attached to the given head commit
	// of a pull/merge request.
	ReadChecks(context.Context, PullRequestRef, string /* headSHA */) ([]Check, error)

	// CreatePullRequest opens a new pull/merge request.
	CreatePullRequest(context.Context, CreatePullRequestInput) (PullRequest, error)
	// UpdatePullRequest applies the non-zero fields of the input to a pull
	// request and returns the updated request.
	UpdatePullRequest(context.Context, PullRequestRef, UpdatePullRequestInput) (PullRequest, error)
	// CommentPullRequest adds a comment to a pull/merge request.
	CommentPullRequest(context.Context, PullRequestRef, string /* body */) (Comment, error)
}
