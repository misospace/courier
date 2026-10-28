package label

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/misospace/courier/internal/connector"
	"github.com/misospace/courier/internal/forge"
	"github.com/misospace/courier/internal/source"
)

// connectorName is the bounded connector label value this adapter reports
// under, and the source identity every discovered run carries in
// spec.source. It matches the name DESIGN.md and the observability signal
// catalogue already use for this adapter.
const connectorName = "github-label"

// Connector-declared reason vocabulary. The label adapter has no protocol
// exchange, so its vocabulary is minimal and closed.
const (
	reasonAvailable   = connector.AvailabilityAvailable
	reasonUnreachable = connector.AvailabilityUnreachable
)

// Defaults for the configured trigger labels. A deployment overrides them to
// match its forge's existing label vocabulary.
const (
	DefaultTriggerLabel = "courier"
	DefaultFixLabel     = "changes-requested"
)

// assignmentMaxBytes bounds one assignment reason; the single formatted
// reason this adapter emits is untrusted task data, never instructions.
const assignmentMaxBytes = 1024

// Config is the label adapter's deployment configuration. Repos are
// canonical "owner/repo" identities; at least one is required.
type Config struct {
	Repos        []string
	TriggerLabel string
	FixLabel     string
	Lane         string
}

// Adapter discovers label-driven forge work and settles it by removing the
// trigger label. The zero value is not usable; New attaches the provider and
// validated configuration.
type Adapter struct {
	lister  forge.LabeledWorkItemLister
	mutator forge.WorkItemLabelMutator
	config  Config
	// diagnostics is optional; nil leaves the adapter diagnostic-free.
	diagnostics *connector.Emitter
}

var (
	_ source.Adapter           = Adapter{}
	_ source.WorkIdentifier    = Adapter{}
	_ source.DiscoveryExcluder = Adapter{}
)

// New creates the label adapter over one forge provider. Construction fails
// fast when the configuration is unusable or the provider cannot serve the
// two typed seams the adapter needs: discovery without a listing can never
// offer work, and settlement without label mutation can never retire a
// trigger label, so a run whose source could not be resolved would retry
// forever. A nil emitter leaves the adapter diagnostic-free.
func New(provider forge.Provider, config Config, diagnostics *connector.Emitter) (Adapter, error) {
	if provider == nil {
		return Adapter{}, errors.New("label source: forge provider is required")
	}
	repos := make([]string, 0, len(config.Repos))
	seen := make(map[string]struct{}, len(config.Repos))
	for _, repo := range config.Repos {
		repo = strings.TrimSpace(repo)
		if err := validRepository(repo); err != nil {
			return Adapter{}, fmt.Errorf("label source: %w", err)
		}
		if _, dup := seen[repo]; dup {
			continue
		}
		seen[repo] = struct{}{}
		repos = append(repos, repo)
	}
	if len(repos) == 0 {
		return Adapter{}, errors.New("label source: at least one repository is required")
	}
	config.Repos = repos
	config.TriggerLabel = strings.TrimSpace(config.TriggerLabel)
	if config.TriggerLabel == "" {
		config.TriggerLabel = DefaultTriggerLabel
	}
	config.FixLabel = strings.TrimSpace(config.FixLabel)
	if config.FixLabel == "" {
		config.FixLabel = DefaultFixLabel
	}
	for name, label := range map[string]string{"trigger": config.TriggerLabel, "fix": config.FixLabel} {
		if err := validLabel(label); err != nil {
			return Adapter{}, fmt.Errorf("label source: %s label: %w", name, err)
		}
	}
	lister, ok := provider.(forge.LabeledWorkItemLister)
	if !ok {
		return Adapter{}, fmt.Errorf("label source: provider %q does not list work items by label", provider.Config().Name)
	}
	mutator, ok := provider.(forge.WorkItemLabelMutator)
	if !ok {
		return Adapter{}, fmt.Errorf("label source: provider %q does not remove work item labels", provider.Config().Name)
	}
	a := Adapter{
		lister:      lister,
		mutator:     mutator,
		config:      config,
		diagnostics: diagnostics,
	}
	if diagnostics == nil {
		return a, nil
	}
	diagnostics.RegisterConnector(connectorName, map[string][]string{
		connector.SignalAvailability: {reasonAvailable, reasonUnreachable},
	})
	diagnostics.Capability(connectorName, connector.CapabilityDiscovery, connector.CapabilitySupported)
	diagnostics.Capability(connectorName, connector.CapabilityDiscoveryExclusions, connector.CapabilitySupported)
	diagnostics.Capability(connectorName, connector.CapabilityWorkIdentity, connector.CapabilitySupported)
	diagnostics.Capability(connectorName, connector.CapabilityAvailability, connector.CapabilitySupported)
	diagnostics.Capability(connectorName, connector.CapabilityLifecycleReport, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityPreLaunchRevalidation, connector.CapabilityUnsupported)
	diagnostics.Capability(connectorName, connector.CapabilityCIParkLifecycle, connector.CapabilityUnsupported)
	return a, nil
}

// Discover lists the label-driven work each configured repository currently
// offers. Issues carrying the trigger label become resolve-issue items and
// pull requests carrying the fix label become fix-pr items; each item's
// opaque ID is "<repo>#<number>", so identities never collide across
// repositories and carry no incidental metadata that changes between offers.
// A listing failure for any repository fails the whole poll: partial
// discovery could silently starve one repository while another advances.
func (a Adapter) Discover(ctx context.Context) ([]source.WorkItem, error) {
	return a.discoverExcluding(ctx, nil)
}

// DiscoverExcluding discovers work while dropping retained same-source items,
// so a retained run's work is not re-offered while its retention window
// covers it. It shares Discover's single poll path — and its availability
// diagnostics — because the runner prefers it whenever the adapter offers
// exclusions, so emission must not live on Discover alone.
func (a Adapter) DiscoverExcluding(ctx context.Context, excluded []source.WorkItem) ([]source.WorkItem, error) {
	drop := make(map[string]struct{}, len(excluded))
	for _, item := range excluded {
		drop[item.ID] = struct{}{}
	}
	return a.discoverExcluding(ctx, drop)
}

func (a Adapter) discoverExcluding(ctx context.Context, drop map[string]struct{}) ([]source.WorkItem, error) {
	items, err := a.poll(ctx, drop)
	if err != nil {
		if a.diagnostics != nil {
			a.diagnostics.Availability(connectorName, reasonUnreachable)
		}
		return nil, err
	}
	if a.diagnostics != nil {
		a.diagnostics.Availability(connectorName, reasonAvailable)
	}
	return items, nil
}

func (a Adapter) poll(ctx context.Context, drop map[string]struct{}) ([]source.WorkItem, error) {
	items := make([]source.WorkItem, 0, len(drop)+1)
	for _, repo := range a.config.Repos {
		labeled, err := a.lister.ListWorkItemsByLabel(ctx, repo, a.config.TriggerLabel, forge.WorkItemKindIssue)
		if err != nil {
			return nil, fmt.Errorf("label source: list %q issues labeled %q: %w", repo, a.config.TriggerLabel, err)
		}
		for _, item := range labeled {
			number, err := itemNumber(item.ID)
			if err != nil {
				return nil, fmt.Errorf("label source: repository %q offered malformed work item ID %q", repo, item.ID)
			}
			items = append(items, source.WorkItem{
				ID:      itemIdentity(repo, number),
				Repo:    repo,
				Ref:     number,
				Mode:    string(resolveIssueMode),
				Lane:    a.config.Lane,
				Context: boundedContext(item),
				Assignment: source.Assignment{
					Kind: source.AssignmentOther,
				},
			})
		}
		labeled, err = a.lister.ListWorkItemsByLabel(ctx, repo, a.config.FixLabel, forge.WorkItemKindPullRequest)
		if err != nil {
			return nil, fmt.Errorf("label source: list %q pull requests labeled %q: %w", repo, a.config.FixLabel, err)
		}
		for _, item := range labeled {
			number, err := itemNumber(item.ID)
			if err != nil {
				return nil, fmt.Errorf("label source: repository %q offered malformed work item ID %q", repo, item.ID)
			}
			items = append(items, source.WorkItem{
				ID:      itemIdentity(repo, number),
				Repo:    repo,
				Ref:     number,
				Mode:    string(fixPRMode),
				Lane:    a.config.Lane,
				Context: boundedContext(item),
				Assignment: source.Assignment{
					Kind: source.AssignmentReviewFeedback,
					// Untrusted task data, bounded before it reaches a
					// coordinator as context.
					Reasons: []string{boundReason(fmt.Sprintf("fix label %q on %s#%d", a.config.FixLabel, repo, number))},
				},
			})
		}
	}
	filtered := items[:0]
	for _, item := range items {
		if _, dropped := drop[item.ID]; dropped {
			continue
		}
		filtered = append(filtered, item)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Repo != filtered[j].Repo {
			return filtered[i].Repo < filtered[j].Repo
		}
		return filtered[i].Ref < filtered[j].Ref
	})
	return filtered, nil
}

// Claim and Release are no-ops: the label state is the source truth and the
// runner's source-plus-work-identity dedupe is the claim, so there is no
// compare-and-set endpoint to race.
func (a Adapter) Claim(context.Context, source.WorkItem) error   { return nil }
func (a Adapter) Release(context.Context, source.WorkItem) error { return nil }

// Transition is a no-op: review state is carried by the pull request itself
// for fix-pr work and by the forge for issues; the adapter has no label
// vocabulary for in-progress states and must not invent one.
func (a Adapter) Transition(context.Context, source.WorkItem, source.State) error {
	return nil
}

// Resolve retires the label that offered the work, matched by the item's
// mode: the trigger label for a settled resolve-issue run, the fix label for
// a settled fix-pr run. The mutator's removal is idempotent, so a retried
// settlement after a lost response succeeds without fabricating an error.
// Until Resolve succeeds a Done run is never reaped, so the label is only
// removed once the run has genuinely settled.
func (a Adapter) Resolve(ctx context.Context, item source.WorkItem) error {
	label, err := a.resolveLabel(item.Mode)
	if err != nil {
		return err
	}
	repo, number, err := parseIdentity(item.ID)
	if err != nil {
		return fmt.Errorf("label source: resolve work %q: %w", item.ID, err)
	}
	if item.Repo != "" && item.Repo != repo {
		return fmt.Errorf("label source: resolve work %q: repository %q does not match the identity", item.ID, item.Repo)
	}
	return a.mutator.RemoveWorkItemLabel(ctx, forge.WorkItemRef{Repo: repo, ID: strconv.Itoa(number)}, label)
}

// WorkIdentity returns the work item ID unchanged: "<repo>#<number>" already
// names the work without incidental metadata that could change between
// offers, so the runner's dedupe key is stable across rediscoveries.
func (a Adapter) WorkIdentity(workItemID string) string { return workItemID }

func (a Adapter) resolveLabel(mode string) (string, error) {
	switch mode {
	case string(resolveIssueMode):
		return a.config.TriggerLabel, nil
	case string(fixPRMode):
		return a.config.FixLabel, nil
	default:
		return "", fmt.Errorf("label source: unsupported work mode %q", mode)
	}
}

// validRepository accepts exactly the canonical "owner/name" identity the
// forge layer parses: exactly one path separator and no empty segment. A
// typo that reaches a request instead fails discovery every poll, so the
// shape is settled once at startup.
func validRepository(repo string) error {
	owner, name, found := strings.Cut(repo, "/")
	if !found || owner == "" || name == "" || strings.Contains(name, "/") {
		return fmt.Errorf("repository %q must be a canonical owner/name identity", repo)
	}
	return nil
}

// validLabel refuses the shapes that could silently wedge settlement or
// discovery: "." and ".." are cleaned away by the endpoint path builder (the
// removal would 404 and read as idempotent success while the label stays), and
// a comma splits GitHub's label filter query into two labels the listing
// matches but the removal never targets.
func validLabel(label string) error {
	if label == "." || label == ".." || strings.Contains(label, ",") {
		return fmt.Errorf("%q is not a usable label: refuse \".\", \"..\", and any label containing a comma", label)
	}
	return nil
}

// Run modes are the same literal vocabulary the runner accepts and the
// Dispatch adapter already uses in its task mapping.
const (
	resolveIssueMode = "resolve-issue"
	fixPRMode        = "fix-pr"
)

func itemIdentity(repo string, number int) string {
	return repo + "#" + strconv.Itoa(number)
}

func itemNumber(id string) (int, error) {
	number, err := strconv.Atoi(strings.TrimSpace(id))
	if err != nil || number < 1 {
		return 0, fmt.Errorf("want a positive forge-native number, got %q", id)
	}
	return number, nil
}

func parseIdentity(id string) (string, int, error) {
	repo, suffix, found := strings.Cut(id, "#")
	if !found || repo == "" {
		return "", 0, fmt.Errorf("want \"<repo>#<number>\", got %q", id)
	}
	number, err := strconv.Atoi(suffix)
	if err != nil || number < 1 {
		return "", 0, fmt.Errorf("want \"<repo>#<number>\", got %q", id)
	}
	return repo, number, nil
}

// boundedContext renders one forge work item as bounded run context: title,
// body, and a source link, truncated to the source contract's context budget.
// The budget is byte-based (the constant's contract), cut on a rune boundary
// so a multibyte body never splits a character or exceeds the budget. The
// body is untrusted task data and is presented as context only.
func boundedContext(item forge.WorkItem) string {
	var b strings.Builder
	if title := strings.TrimSpace(item.Title); title != "" {
		b.WriteString(title)
		b.WriteString("\n\n")
	}
	if body := strings.TrimSpace(item.Body); body != "" {
		b.WriteString(body)
		b.WriteString("\n\n")
	}
	if item.URL != "" {
		b.WriteString("Source: ")
		b.WriteString(item.URL)
		b.WriteString("\n")
	}
	return truncateUTF8Bytes(strings.TrimRight(b.String(), "\n"), source.MaxSourceContextBytes)
}

func boundReason(reason string) string {
	return truncateUTF8Bytes(reason, assignmentMaxBytes)
}

// truncateUTF8Bytes cuts s to at most max bytes without splitting a rune.
func truncateUTF8Bytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := s[:max]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
