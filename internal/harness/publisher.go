package harness

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	couriergit "github.com/misospace/courier/internal/git"
)

// SnapshotProvider supplies the sanitized read-only snapshot for the next
// dispatch (HARNESS.md §5). The first call of an incarnation seeds control's
// integration tree from the broker's live observation; later calls render
// from that tree, so every snapshot carries all integrated — and possibly
// still unpublished — brief work.
type Snapshot struct {
	// Data is the snapshot bundle: a git bundle whose fixed work ref the
	// worker's unpack fetches.
	Data []byte
	// Tip is the exact commit the snapshot was built from. It is the
	// dispatched base tip every returned artifact's ancestry must reach.
	Tip string
	// BaseTip is the pinned base ref's tip at snapshot time.
	BaseTip string
}

type SnapshotProvider func(context.Context) (Snapshot, error)

// Publisher is the trusted publication seam: only the coordinator's own
// control path may integrate and publish, through the run's broker, under
// the resolved publication policy. It is deliberately unreachable from a
// BriefResult, a worker result, or any delegate: subagents return untrusted
// artifacts and cannot publish (HARNESS.md §5).
type Publisher interface {
	// Integrate validates one completed brief's untrusted bundle against the
	// dispatched snapshot tip and commits one local integration commit.
	Integrate(ctx context.Context, req IntegrationRequest) (Integration, error)
	// Publish fast-forwards the pinned work ref to the integration head
	// through the broker and returns the broker-confirmed publication.
	Publish(ctx context.Context) (Publication, error)
}

// PublicationEngine is the trusted control-side implementation of the
// snapshot and publication seams: it seeds and renders snapshots from its
// private integration tree, validates and integrates returned artifacts,
// and publishes through the run's broker, whose independent §4 enforcement
// and post-push observation decide every outcome. This side classifies the
// world: idempotent recovery after a crash, re-sync when the base advances,
// and NeedsHuman when the work tip is foreign.
type PublicationEngine struct {
	broker *BrokerClient
	tree   *Integrator

	seeded           bool
	seedTip          string
	seedWorkRefExist bool
	seedAtAnchor     bool
	lastConfirmed    string
}

// NewPublicationEngine wires the publication engine to one broker client
// and one integration tree.
func NewPublicationEngine(broker *BrokerClient, tree *Integrator) (*PublicationEngine, error) {
	if broker == nil || strings.TrimSpace(broker.BaseURL) == "" {
		return nil, errors.New("harness: publication requires a broker client")
	}
	if tree == nil || !filepath.IsAbs(tree.Dir) {
		return nil, errors.New("harness: publication requires an integration tree")
	}
	return &PublicationEngine{broker: broker, tree: tree}, nil
}

// PrepareSnapshot implements the SnapshotProvider seam.
func (e *PublicationEngine) PrepareSnapshot(ctx context.Context) (Snapshot, error) {
	if !e.seeded {
		snap, err := retried(ctx, func(ctx context.Context) (BrokerSnapshot, error) {
			return e.broker.Snapshot(ctx)
		})
		if err != nil {
			return Snapshot{}, e.classify(err, "the seed snapshot read")
		}
		tip, err := e.tree.Seed(ctx, snap.Data)
		if err != nil {
			return Snapshot{}, err
		}
		if tip != snap.Tip {
			return Snapshot{}, errors.New("harness: seeded tree does not match the observed snapshot tip")
		}
		e.seeded = true
		e.seedTip = tip
		e.seedWorkRefExist = snap.WorkRefExists
		e.seedAtAnchor = snap.AtAnchor
		// The seed is the world at incarnation start: nothing this
		// incarnation published is confirmed yet.
		e.lastConfirmed = ""
		return Snapshot{Data: snap.Data, Tip: tip, BaseTip: e.tree.BaseTip()}, nil
	}
	return e.tree.Render(ctx)
}

// Integrate implements the Publisher seam.
func (e *PublicationEngine) Integrate(ctx context.Context, req IntegrationRequest) (Integration, error) {
	return e.tree.Integrate(ctx, req)
}

// Publish publishes the current integration head through the broker
// (HARNESS.md §4): full-closure bundle import against the broker's own fresh
// observation, then publication that the broker confirms by post-push
// observation. When the world has moved, the engine re-observes and
// classifies once per round: its own previously published state is
// fast-forwarded, a base advance is re-synced with a merge, and a foreign
// tip is never rebased onto or adopted.
func (e *PublicationEngine) Publish(ctx context.Context) (Publication, error) {
	if !e.seeded {
		return Publication{}, ErrNothingToPublish
	}
	expected := e.expectedTip()
	for round := 0; round <= worldReconcileRounds; round++ {
		head, bundle, err := e.tree.PublishBundle(ctx)
		if err != nil {
			return Publication{}, err
		}
		if head == e.seedTip {
			// Nothing was integrated beyond the seeded tip. What this head
			// could publish is decided by the world's own evidence, never by
			// the live OID: a seed at the admission anchor (or a work ref
			// that did not exist) is ordinary pre-existing state and no
			// publication of this run; a seed beyond the anchor may be this
			// run's own unconfirmed publication, which the broker confirms
			// idempotently — without a duplicate push — while it holds the
			// trusted evidence, and refuses otherwise (§4).
			if !e.seedWorkRefExist || e.seedAtAnchor {
				return Publication{}, ErrNothingToPublish
			}
			if err := retriedCall(ctx, func(ctx context.Context) error {
				return e.broker.ImportBundle(ctx, bundle, head, expected)
			}); err != nil {
				return Publication{}, e.classify(err, "the seeded publication confirmation import")
			}
			result, pubErr := retried(ctx, func(ctx context.Context) (Publication, error) {
				return e.broker.Publish(ctx, expected, head)
			})
			if pubErr != nil {
				return Publication{}, e.classify(pubErr, "the seeded publication confirmation")
			}
			e.lastConfirmed = result.OID
			return result, nil
		}
		if err := retriedCall(ctx, func(ctx context.Context) error {
			return e.broker.ImportBundle(ctx, bundle, head, expected)
		}); err == nil {
			result, pubErr := retried(ctx, func(ctx context.Context) (Publication, error) {
				return e.broker.Publish(ctx, expected, head)
			})
			if pubErr == nil {
				e.lastConfirmed = result.OID
				return result, nil
			}
		}
		// Either the import or the publication was refused by the broker:
		// re-observe the world once and classify before retrying.
		reconciled, err := e.reconcile(ctx, head, expected)
		if err != nil {
			return Publication{}, err
		}
		expected = reconciled.expected
	}
	return Publication{}, &PublicationBlockedError{Reason: "the live work ref did not settle across reconciliation rounds; refusing to race it"}
}

// expectedTip derives the expected work tip for this incarnation's first
// publication attempt: the last broker-confirmed OID when this process has
// one, the admission anchor when the work ref existed at seed time, and
// absent otherwise — the run's first commit creates the ref.
func (e *PublicationEngine) expectedTip() string {
	if e.lastConfirmed != "" {
		return e.lastConfirmed
	}
	if e.seedWorkRefExist {
		return e.seedTip
	}
	return ""
}

// reconciledWorld is one fresh-observation classification: the expected tip
// the next attempt must cite. The loop re-renders the bundle from the tree
// afterward, so a base re-sync merge is picked up automatically.
type reconciledWorld struct {
	expected string
}

// reconcile re-observes the live world once and classifies it against this
// run's publication evidence. It never rebases onto or adopts a foreign tip.
func (e *PublicationEngine) reconcile(ctx context.Context, head, expected string) (*reconciledWorld, error) {
	snap, err := retried(ctx, func(ctx context.Context) (BrokerSnapshot, error) {
		return e.broker.Snapshot(ctx)
	})
	if err != nil {
		return nil, e.classify(err, "the world observation")
	}
	// Absorb the fresh snapshot bundle and resolve its tips locally: the
	// world facts classification runs on are objects git verified, and the
	// broker's claimed tips are used only for the cross-check that the
	// bundle carries the world it reported.
	live, baseTip, err := e.tree.Absorb(ctx, snap.Data)
	if err != nil {
		return nil, err
	}
	if live != snap.Tip || baseTip != snap.BaseTip {
		return nil, errors.New("harness: the world snapshot does not match the broker's claimed observation")
	}
	// The exact proposal is already live: the crash-after-push recovery
	// path. Publication confirms it idempotently while this broker holds
	// the trusted evidence; a broker that cannot vouch for the OID refuses,
	// and that refusal is terminal (§4).
	if live == head {
		return &reconciledWorld{expected: live}, nil
	}
	behind, err := couriergit.IsAncestor(ctx, e.tree.Dir, live, head)
	if err != nil {
		return nil, fmt.Errorf("harness: world ancestry check failed: %w", err)
	}
	baseMoved := snap.BaseTip != e.tree.BaseTip()
	// Base advance: re-sync trusted integration with the live base and let
	// the caller retry against the merged head — but never on top of a
	// work tip this engine cannot vouch for, whatever its ancestry: a
	// rewind to an unadoptable tip blocks before any merge is attempted.
	if baseMoved {
		if snap.WorkRefExists && live != expected && live != e.seedTip {
			return nil, &PublicationBlockedError{Reason: fmt.Sprintf("the live work tip advanced beyond this run's publication evidence (live %s, expected %s); a foreign tip is never adopted", live, expected)}
		}
		baseTip, err := e.tree.RefreshBase(ctx, snap.Data)
		if err != nil {
			return nil, err
		}
		if err := e.tree.MergeBase(ctx, baseTip); err != nil {
			// The merge state never outlives this decision: a left-behind
			// MERGE_HEAD would silently turn the next integration commit
			// into a bogus two-parent merge.
			if abortErr := e.tree.AbortMerge(ctx); abortErr != nil {
				return nil, fmt.Errorf("harness: conflicted base re-sync could not be aborted: %w", abortErr)
			}
			var conflict *couriergit.MergeConflictError
			if errors.As(err, &conflict) {
				return nil, &PublicationBlockedError{Reason: fmt.Sprintf("base re-sync merge conflicts in %d path(s); resolving them is human work", len(conflict.Paths))}
			}
			return nil, fmt.Errorf("harness: base re-sync merge failed: %w", err)
		}
		return &reconciledWorld{expected: expected}, nil
	}
	if behind {
		// The live tip is an ancestor of this run's head. Only a tip this
		// engine can vouch for — its own last confirmed publication or the
		// seed-time world — is adopted as the next expected tip; a local
		// ancestry fact alone is not publication evidence (§4).
		if live == e.lastConfirmed || live == e.seedTip {
			return &reconciledWorld{expected: live}, nil
		}
	}
	// The work tip moved with no explanation this engine can vouch for: a
	// foreign tip is never adopted.
	return nil, &PublicationBlockedError{Reason: fmt.Sprintf("the live work tip advanced beyond this run's publication evidence (live %s, expected %s); a foreign tip is never adopted", live, expected)}
}

// retriedCall is retried for calls that report only an error.
func retriedCall(ctx context.Context, call func(context.Context) error) error {
	_, err := retried(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, call(ctx)
	})
	return err
}

// ErrNothingToPublish reports a declared "changes" outcome with no work
// behind it: the integration head is still the seeded snapshot tip and
// nothing was published this incarnation. Publishing it would push the
// snapshot tip itself — for an initially-absent work ref, the base tip — so
// it is refused instead.
var ErrNothingToPublish = errors.New("harness: no work was integrated or published in this incarnation")

// retried runs one broker call with bounded transport-class retries. A
// definite denial returns immediately.
func retried[T any](ctx context.Context, call func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if err := sleepForRetry(ctx); err != nil {
				return zero, err
			}
		}
		result, err := call(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isBrokerRetryable(err) || attempt >= publishRetryRounds {
			return zero, lastErr
		}
	}
}

// classify maps a broker failure to the publication error taxonomy. A
// definite denial is a blocked world; only transport-class failure that
// persisted past the bounded rounds is an infrastructure failure.
func (e *PublicationEngine) classify(err error, what string) error {
	if err == nil {
		return nil
	}
	var blocked *PublicationBlockedError
	if errors.As(err, &blocked) {
		return blocked
	}
	var callErr *BrokerCallError
	if errors.As(err, &callErr) && !callErr.Retryable {
		return &PublicationBlockedError{Reason: fmt.Sprintf("the broker denied %s (%s)", what, callErr.Category)}
	}
	return &TransientPublicationError{Err: err}
}

func sleepForRetry(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(publishRetryDelay):
		return nil
	}
}
