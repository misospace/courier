package harness

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Capability states (HARNESS.md §5): configured (declared and in use, not
// probed — never emitted for a required capability), healthy (probed and
// working), unavailable (probed and not usable).
const (
	StateConfigured  = "configured"
	StateHealthy     = "healthy"
	StateUnavailable = "unavailable"
)

// Capability is one probed semantic capability with a redacted reason.
// Detail never carries a secret value: probes report fixed categories and
// status codes, and broker-provided diagnostics are redacted server-side.
type Capability struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
	// Required marks a capability the run cannot proceed without. Required
	// and unavailable fails the gate; optional capabilities are named and
	// the run proceeds degraded — capability health never silently drops a
	// degraded state, and never hard-gates an optional tool.
	Required bool `json:"required"`
	// Retryable marks an unavailable state caused by a transient transport
	// or availability failure (a broker still starting, one gateway hiccup):
	// re-probing can fix it. Non-retryable unavailability is configuration.
	Retryable bool `json:"retryable,omitempty"`
}

// healthy/unavailable helpers keep state spellings in one place.
func healthy(name string, required bool) Capability {
	return Capability{Name: name, State: StateHealthy, Required: required}
}

func unavailable(name string, required bool, detail string) Capability {
	return Capability{Name: name, State: StateUnavailable, Detail: detail, Required: required}
}

func unavailableRetryable(name string, required bool, detail string) Capability {
	return Capability{Name: name, State: StateUnavailable, Detail: detail, Required: required, Retryable: true}
}

// unavailableClassified emits an unavailable required capability carrying the
// probe's retryability class.
func unavailableClassified(name, detail string, retryable bool) Capability {
	capability := unavailable(name, true, detail)
	capability.Retryable = retryable
	return capability
}

// BrokerProbeError carries a capability-read failure class. Transport-class
// failures are retryable; identity and request-shape failures are not.
type BrokerProbeError struct {
	Msg       string
	Retryable bool
}

func (e *BrokerProbeError) Error() string { return e.Msg }

// ProbeRetryable marks a probe error as transient: re-probing can succeed.
type ProbeRetryable struct{ Err error }

func (e *ProbeRetryable) Error() string { return e.Err.Error() }
func (e *ProbeRetryable) Unwrap() error { return e.Err }

// isProbeRetryable classifies a probe error. A transient GatewayError or an
// explicit ProbeRetryable wrapper is retryable; everything else is
// configuration the operator must fix.
func isProbeRetryable(err error) bool {
	if err == nil {
		return false
	}
	var retryable *ProbeRetryable
	if errors.As(err, &retryable) {
		return true
	}
	var gatewayErr *GatewayError
	if errors.As(err, &gatewayErr) {
		return isTransientGatewayStatus(gatewayErr.Status)
	}
	var brokerErr *BrokerProbeError
	if errors.As(err, &brokerErr) {
		return brokerErr.Retryable
	}
	return false
}

// BrokerProbeClient reaches the run's broker's typed API over TLS with the
// control pod's projected broker-audience token.
type BrokerProbeClient struct {
	BaseURL string
	Token   []byte
	CA      []byte
	HTTP    *http.Client
}

// BrokerCapabilities is the redacted capability report served by the
// broker's GET /v1/capabilities.
type BrokerCapabilities struct {
	Provider struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"provider"`
	Operations []struct {
		Name      string `json:"name"`
		Available bool   `json:"available"`
		Detail    string `json:"detail,omitempty"`
	} `json:"operations"`
	Git struct {
		Endpoint string `json:"endpoint"`
		Ready    bool   `json:"ready"`
	} `json:"git"`
}

// Capabilities fetches the broker's capability report. Every response class
// maps to a fixed, safe diagnostic; transport-class failures are marked
// retryable.
func (b *BrokerProbeClient) Capabilities(ctx context.Context) (*BrokerCapabilities, error) {
	transport := b.HTTP
	if transport == nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b.CA) {
			return nil, &BrokerProbeError{Msg: "broker CA is unreadable"}
		}
		serverName := hostOnly(b.BaseURL)
		transport = &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS13}},
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trimSuffix(b.BaseURL, "/")+"/v1/capabilities", nil)
	if err != nil {
		return nil, &BrokerProbeError{Msg: "broker capabilities request is malformed"}
	}
	req.Header.Set("Authorization", "Bearer "+string(b.Token))
	resp, err := transport.Do(req)
	if err != nil {
		return nil, &BrokerProbeError{Msg: "broker is unreachable", Retryable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &BrokerProbeError{
			Msg:       fmt.Sprintf("broker rejected the authenticated capabilities read with status %d", resp.StatusCode),
			Retryable: resp.StatusCode >= 500,
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, &BrokerProbeError{Msg: "broker capabilities response is unreadable", Retryable: true}
	}
	var report BrokerCapabilities
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, &BrokerProbeError{Msg: "broker capabilities response is malformed"}
	}
	return &report, nil
}

func trimSuffix(s, suffix string) string {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)]
	}
	return s
}

func hostOnly(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return rawURL
	}
	return parsed.Hostname()
}

// ProbeDeps carries the probed endpoints. Every probe is best-effort: an
// error becomes an unavailable capability, never a crash.
type ProbeDeps struct {
	// Gateway probes the model bindings per role. Optional roles probe as
	// optional capabilities.
	Gateway *Gateway
	// Bindings names the roles and their model identifiers.
	Bindings Bindings
	// Broker reaches the run's broker capabilities report.
	Broker *BrokerProbeClient
	// Mode selects which forge operations the run's terminal contract needs.
	Mode string
	// PublicationPresent reports whether this build ships a publisher. The
	// native harness reports the seam honestly rather than pretending.
	PublicationPresent bool
	// SnapshotPresent reports whether the workspace snapshot seam is wired.
	// Without it, worker execution has no workspace to run against.
	SnapshotPresent bool
	// ProbeWorker optionally probes the signed worker protocol end to end.
	// The probe task is inert; a nil probe skips the capability.
	ProbeWorker func(context.Context) Capability
	// ModelProbe performs one cheap model call for a role binding.
	ModelProbe func(context.Context, string) error
}

// requiredForgeOperations maps a run mode to the forge operations its
// terminal contract needs. The vocabulary is the forge package's closed
// capability set; merge does not exist anywhere in it.
func requiredForgeOperations(mode string) []string {
	switch mode {
	case "resolve-issue":
		return []string{"read-work-item", "create-pull-request", "update-pull-request", "comment"}
	case "fix-pr":
		return []string{"read-pull-request", "list-reviews", "list-comments", "read-checks", "update-pull-request", "comment"}
	default:
		return nil
	}
}

// ProbeCapabilities probes each semantic capability the harness depends on
// and returns the health table. It never fails: an unusable probe is an
// unavailable capability with a safe reason.
func ProbeCapabilities(ctx context.Context, deps ProbeDeps) []Capability {
	caps := make([]Capability, 0, 6)
	if deps.ProbeWorker != nil {
		caps = append(caps, deps.ProbeWorker(ctx))
	}
	caps = append(caps, probeForge(ctx, deps)...)
	caps = append(caps, probeModelBindings(ctx, deps)...)
	caps = append(caps, probeSnapshot(deps), probePublication(deps))
	return caps
}

// probeSnapshot reports the workspace-snapshot seam honestly: without a
// snapshot provider, worker execution has no workspace, and the table must
// not say the worker path is usable when it is not.
func probeSnapshot(deps ProbeDeps) Capability {
	if deps.SnapshotPresent {
		return healthy("workspace-snapshot", true)
	}
	return unavailable("workspace-snapshot", true, "the snapshot provider is provisioned with #125 and is not part of this build")
}

// probeForge probes broker forge operations and the git remote through one
// authenticated broker read. A healthy report names the provider; an
// unavailable report names the provider when the broker could say so, plus a
// safe reason when it could not.
func probeForge(ctx context.Context, deps ProbeDeps) []Capability {
	if deps.Broker == nil {
		return []Capability{
			unavailable("forge-operations", true, "no broker is configured in this build"),
			unavailable("git-remote", true, "no broker is configured in this build"),
		}
	}
	report, err := deps.Broker.Capabilities(ctx)
	if err != nil {
		retryable := isProbeRetryable(err)
		return []Capability{
			unavailableClassified("forge-operations", err.Error(), retryable),
			unavailableClassified("git-remote", err.Error(), retryable),
		}
	}
	required := requiredForgeOperations(deps.Mode)
	byName := make(map[string]struct {
		Available bool
		Detail    string
	}, len(report.Operations))
	for _, op := range report.Operations {
		byName[op.Name] = struct {
			Available bool
			Detail    string
		}{op.Available, op.Detail}
	}
	forgeState := StateHealthy
	forgeDetail := ""
	for _, op := range required {
		entry, ok := byName[op]
		if !ok {
			forgeState = StateUnavailable
			forgeDetail = fmt.Sprintf("provider %q does not register operation %q", report.Provider.Name, op)
			break
		}
		if !entry.Available {
			forgeState = StateUnavailable
			forgeDetail = fmt.Sprintf("provider %q operation %q is unavailable: %s", report.Provider.Name, op, entry.Detail)
			break
		}
	}
	gitState := StateHealthy
	gitDetail := ""
	if !report.Git.Ready {
		gitState = StateUnavailable
		gitDetail = fmt.Sprintf("provider %q did not verify its git endpoint at broker startup", report.Provider.Name)
	}
	return []Capability{
		{Name: "forge-operations", State: forgeState, Detail: forgeDetail, Required: true},
		{Name: "git-remote", State: gitState, Detail: gitDetail, Required: true},
	}
}

// probeModelBindings probes every bound role. The coordinator binding is
// required; other roles are optional and their unavailability is named so a
// degraded run is a visible decision, not a silent drop.
func probeModelBindings(ctx context.Context, deps ProbeDeps) []Capability {
	caps := make([]Capability, 0, len(deps.Bindings.Roles()))
	if deps.Gateway == nil || deps.ModelProbe == nil {
		for _, role := range deps.Bindings.Roles() {
			caps = append(caps, unavailable("model-binding:"+role, role == RoleCoordinator,
				"no model gateway is configured in this build"))
		}
		return caps
	}
	for _, role := range deps.Bindings.Roles() {
		if err := deps.ModelProbe(ctx, deps.Bindings.Model(role)); err != nil {
			capability := unavailable("model-binding:"+role, role == RoleCoordinator, err.Error())
			capability.Retryable = isProbeRetryable(err)
			caps = append(caps, capability)
			continue
		}
		caps = append(caps, healthy("model-binding:"+role, role == RoleCoordinator))
	}
	return caps
}

// probePublication reports the publication seam honestly. Publication is
// required: the coordinator owns the terminal contract, and a build without
// the publisher fails closed instead of burning model work it cannot land.
func probePublication(deps ProbeDeps) Capability {
	if deps.PublicationPresent {
		return healthy("publication", true)
	}
	return unavailable("publication", true, "publication is implemented with #125 and is not part of this build")
}

// GateError reports the required capabilities that are unavailable. The
// message is the actionable, redacted capability table.
type GateError struct {
	Failed []Capability
}

func (e *GateError) Error() string {
	detail := ""
	for _, capability := range e.Failed {
		if detail != "" {
			detail += "; "
		}
		detail += fmt.Sprintf("%s is %s", capability.Name, capability.State)
		if capability.Detail != "" {
			detail += ": " + capability.Detail
		}
	}
	return "required capabilities are unavailable: " + detail
}

// AllRetryable reports whether every failed capability's unavailability is
// transient (transport, availability): re-probing after a short wait can
// clear the gate. A single permanent failure makes the whole gate permanent.
func (e *GateError) AllRetryable() bool {
	for _, capability := range e.Failed {
		if !capability.Retryable {
			return false
		}
	}
	return true
}

// Gate fails closed when a required capability is unavailable and reports
// which optional capabilities the run will proceed without. A run proceeds
// degraded whenever only optional capabilities are down (HARNESS.md §5:
// "a run that can proceed does, degraded capabilities and all"). A required
// capability that is merely configured — declared but never probed — never
// exists: probes either mark required capabilities healthy or unavailable.
func Gate(caps []Capability) ([]Capability, error) {
	var failed []Capability
	for _, capability := range caps {
		if capability.Required && capability.State == StateUnavailable {
			failed = append(failed, capability)
		}
	}
	if len(failed) > 0 {
		return caps, &GateError{Failed: failed}
	}
	return caps, nil
}
