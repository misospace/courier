// Package adhoc implements the source lifecycle for ad-hoc contributions
// (#596, docs/external-contributions.md, "Work identity and admission").
// An ad-hoc contribution is work submitted directly by a frontend — no
// prior Dispatch inventory, no ownership of an upstream issue: an approved
// base repository, an approved fork, and a natural-language goal. The
// adapter materializes that admission as an ordinary contribute-mode
// CoderRun. It is push-based with no external lifecycle: there is nothing
// to discover, claim, or transition, and a duplicate submission carrying
// the same idempotency key resumes the existing run instead of creating a
// second one.
package adhoc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source"
)

// SourceName is the source identity stamped into the run's immutable spec.
const SourceName = "adhoc"

var (
	// ErrClientRequired indicates that Admit was called on an adapter
	// constructed without a Kubernetes client. Keeping it an error (rather
	// than a construction-time panic) makes startup misconfiguration
	// observable at the call site.
	ErrClientRequired = errors.New("adhoc: kubernetes client is required")
	// ErrInvalidAdmission indicates that an admission input failed
	// server-side validation before any CoderRun was read or created.
	// Rejected admissions have no cluster-side effect.
	ErrInvalidAdmission = errors.New("adhoc: invalid admission")
	// ErrForeignRun indicates that a CoderRun already occupies the name
	// derived for this admission's key but is owned by another source or
	// key. The collision is permanent for that name: retrying the same
	// admission can never succeed, and the foreign object is never
	// touched.
	ErrForeignRun = errors.New("adhoc: coder run belongs to a different admission")
)

var (
	// repoPattern is the owner/name shape both repository identities must
	// take; it mirrors the CRD pattern on spec.repo and spec.workRepo.
	repoPattern = regexp.MustCompile(`^[^/]+/[^/]+$`)
	// keyPattern bounds the character set of a caller-supplied
	// idempotency key: it starts with an alphanumeric and may continue
	// with alphanumerics, dot, underscore, colon, and dash.
	keyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

const (
	// maxBaseRefBytes is the admission bound on the pinned base ref;
	// branch names are short, and anything larger is a client bug.
	maxBaseRefBytes = 255
	// maxKeyBytes is the admission bound on a caller-supplied
	// idempotency key.
	maxKeyBytes = 256

	// contextKey is the single top-level key under which the adapter
	// writes its SourceContext payload. It is source-owned: core copies
	// the context verbatim and never interprets it.
	contextKey = "adhoc.courier.misospace.dev/v1"
)

// contextPayload is the deterministic object stored under contextKey.
// UpstreamIssue is omitted from the JSON when empty.
type contextPayload struct {
	Goal          string `json:"goal"`
	BaseRef       string `json:"baseRef"`
	UpstreamIssue string `json:"upstreamIssue,omitempty"`
}

// Admission is the server-side validated input for one ad-hoc
// contribution. Every string field is trimmed before validation. Goal
// and UpstreamIssue are untrusted data: they are carried into the run as
// context only and can never change the approved fork, base, or
// publication destination.
type Admission struct {
	// Namespace is the Kubernetes namespace the CoderRun is created in.
	// Required.
	Namespace string
	// Lane is the LaneProfile name that supplies the model ensemble and
	// framing. Required.
	Lane string
	// BaseRepo is the upstream repository in "owner/name" form — the PR
	// base. It becomes spec.repo. Required.
	BaseRepo string
	// BaseRef is the pinned base branch name; an explicit admission input
	// that never falls back to a remote default. Required.
	BaseRef string
	// WorkRepo is the approved fork in "owner/name" form that the run may
	// push to. It becomes spec.workRepo. Required.
	WorkRepo string
	// Goal is the natural-language objective (untrusted data). Required.
	Goal string
	// UpstreamIssue is an optional issue URL or number carried as
	// untrusted context. Supplying one is not an ownership claim.
	UpstreamIssue string
	// IdempotencyKey is the optional durable identity of the
	// contribution. When empty, admission mints "adhoc-<12 hex>" from
	// (BaseRepo, BaseRef, Goal, WorkRepo).
	IdempotencyKey string
	// Debug, when true, raises the pod log level for this run only.
	Debug bool
}

// Admitted is the identity of the CoderRun a contribution was admitted
// to. RunName is the created-or-found object name; WorkItemID is the
// idempotency key persisted in spec.workItemID; Resumed is true when an
// existing run already owned the key.
type Admitted struct {
	RunName    string
	WorkItemID string
	Resumed    bool
}

// Adapter is the ad-hoc contribution source adapter. It is push-based
// with no external lifecycle: discovery, claim, release, transition, and
// resolution are all no-ops, and Admit is the only verb with effects.
// Value receivers keep the adapter safely shareable.
type Adapter struct {
	client client.Client
	// apiReader serves the reads that decide create-vs-resume. A cached
	// client can answer NotFound for an object a concurrent admission
	// just created, which would turn a duplicate submission into a
	// transient error instead of a resume; reading straight from the API
	// server keeps resume immediate. The durable guarantee is unchanged
	// either way: the deterministic name plus API-server uniqueness.
	apiReader client.Reader
}

// New creates the ad-hoc adapter around a Kubernetes client and a direct
// API-server reader for the create-vs-resume lookups (for example,
// manager.GetAPIReader). c may be nil; the adapter then fails Admit with
// ErrClientRequired, keeping startup misconfiguration observable at the
// call site. A nil apiReader falls back to c.
func New(c client.Client, apiReader client.Reader) Adapter {
	return Adapter{client: c, apiReader: apiReader}
}

// reader is the strongest-consistency reader the adapter was given.
func (a Adapter) reader() client.Reader {
	if a.apiReader != nil {
		return a.apiReader
	}
	return a.client
}

var _ source.Adapter = Adapter{}

// Admit validates the admission, derives the idempotency key and run
// name, and materializes — or, for a repeated key, resumes — the
// contribute-mode CoderRun. It makes no forge calls and no other external
// contact: Kubernetes only. A name collision with a run owned by another
// source or key fails closed without touching the foreign object.
func (a Adapter) Admit(ctx context.Context, in Admission) (Admitted, error) {
	if a.client == nil {
		return Admitted{}, ErrClientRequired
	}

	hadKey := in.IdempotencyKey != ""
	in.Namespace = strings.TrimSpace(in.Namespace)
	in.Lane = strings.TrimSpace(in.Lane)
	in.BaseRepo = strings.TrimSpace(in.BaseRepo)
	in.BaseRef = strings.TrimSpace(in.BaseRef)
	in.WorkRepo = strings.TrimSpace(in.WorkRepo)
	in.Goal = strings.TrimSpace(in.Goal)
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)

	if msg := validateAdmission(in, hadKey); msg != "" {
		return Admitted{}, fmt.Errorf("%w: %s", ErrInvalidAdmission, msg)
	}

	key := in.IdempotencyKey
	if key == "" {
		key = mintKey(in.BaseRepo, in.BaseRef, in.Goal, in.WorkRepo)
	}

	// The context size bound is checked before any cluster read or write,
	// so an oversized goal is rejected with no object side effects.
	contextJSON, err := encodeContext(in)
	if err != nil {
		return Admitted{}, err
	}

	name := source.RunName(SourceName, key)

	existing := &courierv1alpha1.CoderRun{}
	switch err := a.reader().Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: name}, existing); {
	case err == nil:
		if err := verifyOwnership(existing, key); err != nil {
			return Admitted{}, err
		}
		return Admitted{RunName: existing.Name, WorkItemID: key, Resumed: true}, nil
	case apierrors.IsNotFound(err):
		// Fall through to creation.
	default:
		return Admitted{}, fmt.Errorf("get coder run %s/%s: %w", in.Namespace, name, err)
	}

	run := &courierv1alpha1.CoderRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: in.Namespace,
		},
		Spec: courierv1alpha1.CoderRunSpec{
			Mode:          courierv1alpha1.ModeContribute,
			Source:        SourceName,
			WorkItemID:    key,
			SourceContext: contextJSON,
			Repo:          in.BaseRepo,
			WorkRepo:      in.WorkRepo,
			Lane:          in.Lane,
			Debug:         in.Debug,
		},
	}
	if err := a.client.Create(ctx, run); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return Admitted{}, fmt.Errorf("create coder run %s/%s: %w", in.Namespace, name, err)
		}
		// Lost the create race: the winner owns the key. Re-read and
		// verify like the resume path instead of mutating the object.
		winner := &courierv1alpha1.CoderRun{}
		if err := a.reader().Get(ctx, types.NamespacedName{Namespace: in.Namespace, Name: name}, winner); err != nil {
			return Admitted{}, fmt.Errorf("get coder run %s/%s after create race: %w", in.Namespace, name, err)
		}
		if err := verifyOwnership(winner, key); err != nil {
			return Admitted{}, err
		}
		return Admitted{RunName: winner.Name, WorkItemID: key, Resumed: true}, nil
	}

	return Admitted{RunName: run.Name, WorkItemID: key, Resumed: false}, nil
}

// Discover is a no-op: ad-hoc work is submitted through Admit, never
// discovered from an external queue.
func (a Adapter) Discover(context.Context) ([]source.WorkItem, error) { return nil, nil }

// Claim is a no-op: the ad-hoc source keeps no source-side claim state.
func (a Adapter) Claim(context.Context, source.WorkItem) error { return nil }

// Release is a no-op: there is no source-side claim to roll back.
func (a Adapter) Release(context.Context, source.WorkItem) error { return nil }

// Transition is a no-op: the ad-hoc source has no queue state to publish
// lifecycle changes into.
func (a Adapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}

// Resolve is a no-op: the ad-hoc source owns nothing to close out.
func (a Adapter) Resolve(context.Context, source.WorkItem) error { return nil }

// validateAdmission returns a specific rejection reason for an invalid
// admission, or an empty string when the admission is well-formed. It is
// evaluated before any cluster access, so a rejected admission has no
// side effects. hadKey distinguishes a supplied key that trimmed to
// empty (rejected) from an omitted one (minted).
func validateAdmission(in Admission, hadKey bool) string {
	switch {
	case in.Namespace == "":
		return "namespace is required"
	case in.Lane == "":
		return "lane is required"
	case in.Goal == "":
		return "goal is required"
	case !repoPattern.MatchString(in.BaseRepo):
		return fmt.Sprintf("base repo %q must be of the form owner/name", in.BaseRepo)
	case !repoPattern.MatchString(in.WorkRepo):
		return fmt.Sprintf("work repo %q must be of the form owner/name", in.WorkRepo)
	case in.BaseRef == "":
		return "base ref is required"
	case len(in.BaseRef) > maxBaseRefBytes:
		return fmt.Sprintf("base ref exceeds %d bytes", maxBaseRefBytes)
	case hasWhitespace(in.BaseRef):
		return "base ref must not contain whitespace"
	case strings.HasPrefix(in.BaseRef, "/") || strings.HasSuffix(in.BaseRef, "/"):
		return "base ref must not start or end with /"
	case hadKey && in.IdempotencyKey == "":
		return "idempotency key must not be blank"
	case in.IdempotencyKey != "" && len(in.IdempotencyKey) > maxKeyBytes:
		return fmt.Sprintf("idempotency key exceeds %d bytes", maxKeyBytes)
	case in.IdempotencyKey != "" && !keyPattern.MatchString(in.IdempotencyKey):
		return "idempotency key must match " + keyPattern.String()
	}
	return ""
}

// hasWhitespace reports whether s contains any Unicode whitespace.
func hasWhitespace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// mintKey derives the deterministic idempotency key for a contribution
// without a caller-supplied one: "adhoc-" plus the first 12 hex
// characters of sha256 over (baseRepo, baseRef, goal, workRepo), NUL-
// separated and already trimmed. Identical admissions mint identical
// keys, so a duplicate submission resumes the existing run.
func mintKey(baseRepo, baseRef, goal, workRepo string) string {
	digest := sha256.Sum256([]byte(baseRepo + "\x00" + baseRef + "\x00" + goal + "\x00" + workRepo))
	return "adhoc-" + hex.EncodeToString(digest[:])[:12]
}

// encodeContext renders the run's SourceContext as deterministic JSON:
// one top-level source-owned key mapping to the admission payload. It
// fails with ErrInvalidAdmission when the payload exceeds the CRD's
// context bound; callers check the result before any cluster access.
func encodeContext(in Admission) (string, error) {
	raw, err := json.Marshal(map[string]contextPayload{contextKey: {
		Goal:          in.Goal,
		BaseRef:       in.BaseRef,
		UpstreamIssue: in.UpstreamIssue,
	}})
	if err != nil {
		return "", fmt.Errorf("%w: encode source context: %v", ErrInvalidAdmission, err)
	}
	if len(raw) > source.MaxSourceContextBytes {
		return "", fmt.Errorf("%w: source context exceeds %d bytes", ErrInvalidAdmission, source.MaxSourceContextBytes)
	}
	return string(raw), nil
}

// verifyOwnership fails closed when a CoderRun at the derived name is
// not owned by this admission: a different source or a different
// idempotency key means the name belongs to a foreign object, which must
// never be adopted or mutated.
func verifyOwnership(run *courierv1alpha1.CoderRun, key string) error {
	if run.Spec.Source != SourceName {
		return fmt.Errorf("%w: coder run %s/%s is owned by source %q, not %q",
			ErrForeignRun, run.Namespace, run.Name, run.Spec.Source, SourceName)
	}
	if run.Spec.WorkItemID != key {
		return fmt.Errorf("%w: coder run %s/%s is owned by work item %q, not %q",
			ErrForeignRun, run.Namespace, run.Name, run.Spec.WorkItemID, key)
	}
	return nil
}
