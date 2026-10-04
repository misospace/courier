package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// OwnerLabelKey marks a LaneProfile whose spec is owned by the manager's
// bootstrap reconciler. A LaneProfile carrying this label has its spec
// reconciled toward the bootstrap config; profiles without it are never
// adopted or overwritten.
const (
	OwnerLabelKey   = "courier.misospace.dev/bootstrap-managed"
	OwnerLabelValue = "true"
)

// ErrProfileUnmanaged is a short sentinel returned when a same-name
// LaneProfile exists without the owner label. The actionable guidance is
// added by the wrap at the detection site.
var ErrProfileUnmanaged = errors.New("LaneProfile is not bootstrap-managed")

const defaultReconcileInterval = 60 * time.Second

// +kubebuilder:rbac:groups=courier.misospace.dev,resources=laneprofiles,verbs=get;list;watch;create;update

// Reconciler creates and updates deployment-managed LaneProfiles on a fixed
// interval. It is a manager.Runnable: Start runs one immediate pass, then
// ticker-driven passes, logging (never exiting on) pass errors. There is no
// delete verb and no deletion logic: removal from config intentionally leaves
// the previously managed CR in place (conservative MVP, because active runs
// reference lanes).
type Reconciler struct {
	client.Client
	Namespace string
	Provider  Provider
	Interval  time.Duration

	// collisionMu guards collidingNames and passCollidingNames. A collision
	// (an unmanaged same-name profile) is a steady-state condition, so the
	// transition is logged once per direction (the source runner's laneWaiting
	// precedent) rather than on every pass.
	collisionMu        sync.Mutex
	collidingNames     map[string]bool
	passCollidingNames map[string]bool
}

// Start validates the reconciler, runs one immediate pass, then runs a pass
// on every tick until the context is done. Pass errors are logged and the
// loop continues.
func (r *Reconciler) Start(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	interval := r.Interval
	if interval <= 0 {
		interval = defaultReconcileInterval
	}

	if err := r.ReconcileAll(ctx); err != nil {
		logPassErrors(ctx, err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.ReconcileAll(ctx); err != nil {
				logPassErrors(ctx, err)
			}
		}
	}
}

// logPassErrors logs a pass error from Start, skipping collision errors.
// Start previously logged the joined pass error wholesale on every 60s tick,
// which re-emitted the unmanaged-name collision that ensure/logCollision
// already dedupe at the detection site. Skipping ErrProfileUnmanaged
// components keeps that dedup; only other pass errors are logged.
func logPassErrors(ctx context.Context, err error) {
	if err == nil {
		return
	}
	var components []error
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		components = joined.Unwrap()
	} else {
		components = []error{err}
	}
	for _, component := range components {
		if errors.Is(component, ErrProfileUnmanaged) {
			// Collision, already deduped in ensure/logCollision.
			continue
		}
		log.FromContext(ctx).Error(component, "bootstrap reconciliation failed")
	}
}

// ReconcileAll runs one pass: fetch the desired profiles from the Provider and
// ensure each one exists (or is updated) in the cluster. Per-profile errors
// are combined and returned; an empty desired list is a no-op.
func (r *Reconciler) ReconcileAll(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	profiles, err := r.Provider.Profiles(ctx)
	if err != nil {
		return fmt.Errorf("bootstrap provider: %w", err)
	}
	if len(profiles) == 0 {
		r.settleCollisions(ctx)
		return nil
	}
	var passErrors []error
	for i := range profiles {
		if err := r.ensure(ctx, &profiles[i]); err != nil {
			passErrors = append(passErrors, err)
		}
	}
	r.settleCollisions(ctx)
	return errors.Join(passErrors...)
}

// ensure creates or updates one deployment-managed LaneProfile. Transient
// write races (AlreadyExists on create, Conflict on update) are absorbed and
// retried on the next pass. A same-name profile without the owner label is
// never modified.
func (r *Reconciler) ensure(ctx context.Context, profile *Profile) error {
	existing := &courierv1alpha1.LaneProfile{}
	err := r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: profile.Name}, existing)
	if apierrors.IsNotFound(err) {
		desired := &courierv1alpha1.LaneProfile{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: r.Namespace,
				Name:      profile.Name,
				Labels:    map[string]string{OwnerLabelKey: OwnerLabelValue},
			},
			Spec: desiredSpec(profile),
		}
		if err := r.Create(ctx, desired); err != nil {
			if apierrors.IsAlreadyExists(err) {
				log.FromContext(ctx).Info("LaneProfile appeared between get and create; applying next pass", "name", profile.Name, "namespace", r.Namespace)
				return nil
			}
			return fmt.Errorf("create LaneProfile %s/%s: %w", r.Namespace, profile.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get LaneProfile %s/%s: %w", r.Namespace, profile.Name, err)
	}
	if existing.Labels[OwnerLabelKey] != OwnerLabelValue {
		err := fmt.Errorf("LaneProfile %s/%s exists but %w; rename or remove the existing profile or drop it from the bootstrap config — the bootstrap reconciler never adopts or overwrites unmanaged profiles", r.Namespace, profile.Name, ErrProfileUnmanaged)
		r.logCollision(ctx, profile.Name, err)
		return err
	}
	desired := desiredSpec(profile)
	if !apiequality.Semantic.DeepEqual(existing.Spec, desired) {
		existing.Spec = desired
		if err := r.Update(ctx, existing); err != nil {
			if apierrors.IsConflict(err) {
				log.FromContext(ctx).Info("LaneProfile update conflict; retrying next pass", "name", existing.Name, "namespace", r.Namespace)
				return nil
			}
			return fmt.Errorf("update LaneProfile %s/%s: %w", r.Namespace, profile.Name, err)
		}
	}
	return nil
}

// desiredSpec maps a bootstrap Profile onto a LaneProfileSpec.
func desiredSpec(profile *Profile) courierv1alpha1.LaneProfileSpec {
	return courierv1alpha1.LaneProfileSpec{
		RuntimeImage: profile.RuntimeImage,
		Concurrency:  profile.Concurrency,
		Roles:        profile.Roles,
		Framing:      profile.Framing,
	}
}

// logCollision records that name is in the unmanaged-collision state. The
// first pass for a name logs at Error; subsequent passes log at V(1).
func (r *Reconciler) logCollision(ctx context.Context, name string, err error) {
	r.collisionMu.Lock()
	if r.collidingNames == nil {
		r.collidingNames = map[string]bool{}
	}
	if r.passCollidingNames == nil {
		r.passCollidingNames = map[string]bool{}
	}
	first := !r.collidingNames[name]
	r.collidingNames[name] = true
	r.passCollidingNames[name] = true
	r.collisionMu.Unlock()
	if first {
		log.FromContext(ctx).Error(err, "LaneProfile is blocked by an unmanaged same-name profile", "name", name, "namespace", r.Namespace)
	} else {
		log.FromContext(ctx).V(1).Info("LaneProfile is still blocked by an unmanaged same-name profile", "name", name, "namespace", r.Namespace)
	}
}

// settleCollisions logs Info for names that were in the collision state on a
// previous pass but no longer are (they stopped erroring or disappeared from
// config) and clears their state. It runs at the end of every successful
// pass.
func (r *Reconciler) settleCollisions(ctx context.Context) {
	r.collisionMu.Lock()
	if r.passCollidingNames == nil {
		r.passCollidingNames = map[string]bool{}
	}
	resolved := make([]string, 0)
	for name := range r.collidingNames {
		if !r.passCollidingNames[name] {
			resolved = append(resolved, name)
			delete(r.collidingNames, name)
		}
	}
	for name := range r.passCollidingNames {
		r.collidingNames[name] = true
	}
	r.passCollidingNames = map[string]bool{}
	r.collisionMu.Unlock()
	for _, name := range resolved {
		log.FromContext(ctx).Info("LaneProfile collision resolved", "name", name, "namespace", r.Namespace)
	}
}

// validate checks that the reconciler has the fields it needs to run.
func (r *Reconciler) validate() error {
	if r == nil || r.Client == nil {
		return errors.New("bootstrap reconciler: client is required")
	}
	if strings.TrimSpace(r.Namespace) == "" {
		return errors.New("bootstrap reconciler: namespace is required")
	}
	if r.Provider == nil {
		return errors.New("bootstrap reconciler: provider is required")
	}
	return nil
}
