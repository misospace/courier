// Package skillcanary proves, by execution, that the native harness loads a
// deployment-provided skill bundle into a run and that the model uses it.
//
// The canary drives the production loading chain end to end — registry
// registration, lane selection, the run-context environment round trip, the
// control-side load, and a native coordinator run — against a bundle the
// deployment provides, and reports one named check per claim the chain
// makes. Every check is read from recorded evidence: the registry's digest,
// the round-tripped pin envelope, the model requests the harness sent, and
// the model's own final declaration. The canary reimplements no production
// step: it calls the functions a run's control pod calls, so a passing
// canary is evidence about the shipped chain rather than about the canary.
//
// The default mode writes its own deployment tree — a bundle whose SKILL.md
// carries a per-run unique marker and one supporting asset — into a
// temporary directory and drives it through the hermetic stub gateway, so
// the canary is deterministic and needs no credentials and no network. The
// stub answers with the marker it read in the request the harness sent, so a
// passing canary shows the harness delivered the instruction body into the
// model's context. Against a real deployment the operator points the canary
// at the deployment's bundle config and, optionally, at a real gateway; the
// assertions are the same in both modes.
//
// A deployment-provided test bundle must carry a marker token of the form
// CANARY-MARKER-<hex> in its SKILL.md body and at least one supporting
// asset: the marker is what proves the instruction body reached the model,
// and the asset listing is what proves the bundle's material rode with it.
package skillcanary

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/skills"
)

const (
	// canaryBundleName is the logical name the canary's own synthetic
	// deployment tree registers.
	canaryBundleName = "canary-skill"
	// canaryDescription is the synthetic bundle's frontmatter description.
	canaryDescription = "canary test skill for the native skill-bundle canary"
	// canaryAsset is the synthetic bundle's supporting asset, relative to
	// the bundle root.
	canaryAsset = "assets/checklist.md"
	// canaryModel is the model identifier the canary binds to the
	// coordinator role. A real gateway routes it like any other model name.
	canaryModel = "courier-skill-canary"
	// canaryRunName and canaryNamespace identify the canary's own run
	// context; the run never reaches a cluster.
	canaryRunName   = "skill-canary"
	canaryNamespace = "courier-skill-canary"
	// markerPrefix is the token prefix a skill body must carry for the
	// canary to prove the instruction reached the model.
	markerPrefix = "CANARY-MARKER-"
	// maxExcerptBytes bounds every excerpt the report prints.
	maxExcerptBytes = 400
	// maxRecordedBodyBytes bounds one recorded request or response body.
	maxRecordedBodyBytes = 4 << 20
	// maxRecordedExchanges bounds the model exchanges the canary keeps as
	// evidence, so a long run cannot grow the canary's memory without
	// limit.
	maxRecordedExchanges = 64
	// defaultTimeout bounds one canary run. A gateway that never answers
	// must not hold the canary open.
	defaultTimeout = 5 * time.Minute
)

// The claims the canary tests, one check each, in the order the chain
// makes them.
const (
	// CheckRegistered: the deployment config registered the bundle with a
	// sha256: content digest.
	CheckRegistered = "the deployment config registered the bundle with a sha256: content digest"
	// CheckSelected: lane selection pinned the bundle with that digest.
	CheckSelected = "lane selection pinned the bundle with that same digest"
	// CheckRoundTrip: the pin envelope survived the run-context
	// environment round trip.
	CheckRoundTrip = "the pin envelope survived the run-context environment round trip"
	// CheckControlLoad: the control-side load verified the pinned content
	// and produced a visible view whose origin is the bundle.
	CheckControlLoad = "the control-side load verified the pinned content and produced a visible bundle view"
	// CheckSkillIndex: the pinned system message carried the skill index.
	CheckSkillIndex = "the pinned system message carried the skill index"
	// CheckToolOffered: the skill tool was offered to the coordinator.
	CheckToolOffered = "the skill tool was offered to the coordinator role"
	// CheckToolCall: the recorded tool call was exactly the skill tool.
	CheckToolCall = "the recorded tool call was exactly the skill tool"
	// CheckToolResult: the skill tool result carried the instruction body
	// and the supporting asset.
	CheckToolResult = "the skill tool result carried the instruction body and the supporting asset"
	// CheckModelUsed: the model quoted the marker in its answer.
	CheckModelUsed = "the model's answer quoted the marker the instruction body carries"
	// CheckNoPublication: the run ended as no_change_needed, published
	// nothing.
	CheckNoPublication = "the run ended as no_change_needed with no publication"
	// CheckUnchanged: reloading the registry yields the same digest.
	CheckUnchanged = "the deployment content did not change"
	// CheckDigestReacts: the content digest changes when the content
	// changes.
	CheckDigestReacts = "the content digest reacts to a changed byte"
	// CheckCheckout: the target checkout was not modified.
	CheckCheckout = "the target checkout was not modified"
)

// digestShape is the shape of a registered bundle's content identity.
var digestShape = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// markerShape is the shape of the marker token a skill body carries.
var markerShape = regexp.MustCompile(markerPrefix + `[0-9a-f]{8,64}`)

// Options configures one canary run.
type Options struct {
	// BundleConfig is the path of the deployment's skill bundle config —
	// the file the manager registered bundles from. Empty means the canary
	// writes its own synthetic deployment tree into WorkDir and runs
	// against that.
	BundleConfig string
	// RunConfig is the path of the per-run bundle config the operator
	// rendered for a run's pods — the document the control pod reads at
	// `executor.SkillBundlesConfigPath`. Empty means the control-side load
	// resolves the deployment's own config, which is the tree form. Set it
	// to check the materialization form the run's pods actually receive.
	RunConfig string
	// Skills names the lane's selected skill bundles. Empty means every
	// bundle the config registers.
	Skills []string
	// CheckoutDir is the run's checkout, the directory repository-local
	// skills are discovered against. Empty means a fresh empty directory.
	CheckoutDir string
	// WorkDir is the scratch root the canary's synthetic deployment tree
	// is written into. Empty means a fresh temporary directory.
	WorkDir string
	// GatewayURL is the base URL of a real OpenAI-compatible gateway.
	// Empty means the built-in hermetic stub answers the model.
	GatewayURL string
	// GatewayKeyFile is the file holding the gateway's API key. Empty means
	// the gateway is called unauthenticated.
	GatewayKeyFile string
	// Model is the model identifier bound to the coordinator role. Empty
	// means the canary's own model name.
	Model string
	// Pins is the run's admitted pin set — the name+digest pairs a run's
	// persisted status carries from admission to the control pod. Empty
	// means the canary admits the run itself, pinning the lane's selection
	// against the registry. An operator replays a run whose admitted pins
	// no longer match the registered content with it: the control-side
	// load must then fail closed, exactly as it does in a run whose
	// registry drifted.
	Pins []skills.Pin
	// Timeout bounds the canary run. Zero means the default bound. A
	// gateway that never answers must not hold the canary open.
	Timeout time.Duration
}

// Check is one claim the canary tested, with the evidence it read.
type Check struct {
	// Name is the claim.
	Name string
	// Passed reports whether the evidence supported the claim.
	Passed bool
	// Detail is the bounded evidence: what was observed, not a verdict
	// restated.
	Detail string
}

// Report is the canary's outcome: the checks it ran, in order, and the
// transcript lines an operator reads.
type Report struct {
	// Checks are the claims the canary tested, in order.
	Checks []Check
	// Transcript are the bounded lines the operator reads: the skill index
	// as pinned into the system message, the skill tool result, and the
	// final declaration.
	Transcript []string
	// Skill is the bundle the canary exercised.
	Skill string
	// Digest is the pinned content identity of that bundle.
	Digest string
	// Marker is the unique token the canary proved travelled from the
	// bundle to the model.
	Marker string
	// Gateway names the endpoint that answered the model: "stub" for the
	// canary's own hermetic gateway, otherwise the gateway's base URL. The
	// stub answers with the marker it read in the request the harness sent,
	// so a run against it shows the harness delivered the instruction into
	// the model's context. A run against a real gateway shows a real model
	// quoting the instruction.
	Gateway string
	// Truncated reports that the canary kept only the first
	// maxRecordedExchanges model exchanges as evidence.
	Truncated bool
}

// claims are the checks every canary run makes, in order. A report that
// carries fewer of them proved less than the canary promises.
var claims = []string{
	CheckRegistered,
	CheckSelected,
	CheckRoundTrip,
	CheckControlLoad,
	CheckSkillIndex,
	CheckToolOffered,
	CheckToolCall,
	CheckToolResult,
	CheckModelUsed,
	CheckNoPublication,
	CheckUnchanged,
	CheckDigestReacts,
	CheckCheckout,
}

// Passed reports whether every claim the canary makes was made and held. A
// report that carries fewer checks never passes: a canary that proved
// nothing is not a passing canary.
func (r *Report) Passed() bool {
	if r == nil || len(r.Checks) != len(claims) {
		return false
	}
	for i, check := range r.Checks {
		if check.Name != claims[i] || !check.Passed {
			return false
		}
	}
	return true
}

// Run executes the canary and returns its report. A non-nil error means the
// chain could not be driven to the end — the deployment config would not
// register, the pinned content would not verify, or the coordinator could
// not start — and the report names the check that failed.
func Run(ctx context.Context, opts Options) (*Report, error) {
	c := &canary{opts: opts}
	defer c.cleanup()
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := c.run(runCtx)
	return c.report(), err
}

// canary holds one run's state.
type canary struct {
	opts Options

	checks     []Check
	transcript []string
	owned      []string
	// ownedFiles are the files the canary wrote into a directory the
	// operator supplied. The canary removes them and leaves the directory.
	ownedFiles []string

	workDir      string
	checkoutDir  string
	bundleConfig string
	runConfig    string
	synthetic    bool
	// checkoutBefore is the checkout's content digest before the run
	// started.
	checkoutBefore string

	registry *skills.Registry
	runPins  []skills.Pin
	digests  map[string]string

	invocation executor.Invocation
	skillSet   *harness.SkillSet
	view       skills.SkillView
	skill      string
	marker     string
	digest     string

	gateway  *harness.Gateway
	stub     *stubGateway
	recorder *recorder
	// gatewayName names the endpoint that answered the model.
	gatewayName string
}

// run drives the chain in the production order.
func (c *canary) run(ctx context.Context) error {
	if err := c.prepare(); err != nil {
		return err
	}
	if err := c.register(); err != nil {
		return err
	}
	if err := c.selectPins(); err != nil {
		return err
	}
	if err := c.roundTrip(); err != nil {
		return err
	}
	if err := c.load(); err != nil {
		return err
	}
	if err := c.openGateway(); err != nil {
		return err
	}
	result, err := c.runCoordinator(ctx)
	if err != nil {
		return err
	}
	c.evidence()
	c.ending(result)
	c.unchanged()
	return nil
}

// check records one claim and the evidence it was read from.
func (c *canary) check(name string, passed bool, detail string) {
	c.checks = append(c.checks, Check{Name: name, Passed: passed, Detail: detail})
}

// note records one transcript line the operator reads.
func (c *canary) note(format string, args ...any) {
	c.transcript = append(c.transcript, fmt.Sprintf(format, args...))
}

// report renders the run's outcome.
func (c *canary) report() *Report {
	report := &Report{
		Checks:     c.checks,
		Transcript: c.transcript,
		Skill:      c.skill,
		Digest:     c.digest,
		Marker:     c.marker,
		Gateway:    c.gatewayName,
	}
	if c.recorder != nil {
		report.Truncated = c.recorder.truncated()
	}
	return report
}

// cleanup removes the directories the canary created for itself and stops
// the stub gateway.
func (c *canary) cleanup() {
	if c.stub != nil {
		c.stub.close()
	}
	for _, file := range c.ownedFiles {
		os.RemoveAll(file)
	}
	for _, dir := range c.owned {
		os.RemoveAll(dir)
	}
}

// prepare resolves the canary's directories and, in the default mode,
// writes the synthetic deployment tree the canary runs against.
func (c *canary) prepare() error {
	workDir := strings.TrimSpace(c.opts.WorkDir)
	if workDir == "" {
		dir, err := os.MkdirTemp("", "courier-skill-canary-")
		if err != nil {
			return fmt.Errorf("skillcanary: create the work directory: %w", err)
		}
		c.owned = append(c.owned, dir)
		workDir = dir
	} else if err := os.MkdirAll(workDir, 0o755); err != nil {
		return fmt.Errorf("skillcanary: create the work directory %s: %w", workDir, err)
	}
	c.workDir = workDir

	checkout := strings.TrimSpace(c.opts.CheckoutDir)
	if checkout == "" {
		dir, err := os.MkdirTemp("", "courier-skill-canary-checkout-")
		if err != nil {
			return fmt.Errorf("skillcanary: create the checkout directory: %w", err)
		}
		c.owned = append(c.owned, dir)
		checkout = dir
	} else if err := os.MkdirAll(checkout, 0o755); err != nil {
		return fmt.Errorf("skillcanary: create the checkout directory %s: %w", checkout, err)
	}
	c.checkoutDir = checkout
	digest, err := checkoutDigest(c.checkoutDir)
	if err != nil {
		return err
	}
	c.checkoutBefore = digest

	if config := strings.TrimSpace(c.opts.BundleConfig); config != "" {
		c.bundleConfig = config
	} else if err := c.writeSyntheticBundle(); err != nil {
		return err
	}
	// The control pod reads the per-run bundle config the operator rendered
	// for the run's pods. A canary run without one resolves the
	// deployment's own config, which is the tree form.
	c.runConfig = strings.TrimSpace(c.opts.RunConfig)
	if c.runConfig == "" {
		c.runConfig = c.bundleConfig
	}
	return nil
}

// writeSyntheticBundle writes the canary's own deployment tree: one bundle
// whose SKILL.md carries a marker unique to this run plus one supporting
// asset, and the bundle config that registers it. The marker makes the
// later assertion meaningful: only the bundle body carries it, so a
// declaration that quotes it proves the body reached the model.
func (c *canary) writeSyntheticBundle() error {
	marker, err := newMarker()
	if err != nil {
		return err
	}
	c.marker = marker
	root := filepath.Join(c.workDir, "bundles", canaryBundleName)
	if err := os.MkdirAll(filepath.Join(root, "assets"), 0o755); err != nil {
		return fmt.Errorf("skillcanary: create the synthetic bundle directory: %w", err)
	}
	document := fmt.Sprintf(`---
name: %s
description: %s
---

# Canary test skill

This bundle is the deployment-provided test skill for Courier's native
skill-bundle canary. Its instruction body carries the unique marker

    %s

A model that quotes the marker proves it read this body through the skill
tool. The checklist in %s is the bundle's supporting asset.
`, canaryBundleName, canaryDescription, marker, canaryAsset)
	if err := os.WriteFile(filepath.Join(root, "SKILL.md"), []byte(document), 0o644); err != nil {
		return fmt.Errorf("skillcanary: write the synthetic SKILL.md: %w", err)
	}
	asset := fmt.Sprintf(`# Canary checklist

- [ ] Load the skill with the skill tool.
- [ ] Quote the marker %s in the outcome evidence.
`, marker)
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(canaryAsset)), []byte(asset), 0o644); err != nil {
		return fmt.Errorf("skillcanary: write the synthetic bundle asset: %w", err)
	}
	config := fmt.Sprintf("bundles:\n- name: %s\n  source: %s\n", canaryBundleName, yamlScalar(root))
	path := filepath.Join(c.workDir, "bundles.yaml")
	if err := os.WriteFile(path, []byte(config), 0o644); err != nil {
		return fmt.Errorf("skillcanary: write the synthetic bundle config: %w", err)
	}
	c.bundleConfig = path
	c.synthetic = true
	// The canary removes what it wrote into a directory the operator
	// supplied. The directory itself is the operator's, so it stays.
	if strings.TrimSpace(c.opts.WorkDir) != "" {
		c.ownedFiles = append(c.ownedFiles, filepath.Join(c.workDir, "bundles"), path)
	}
	return nil
}

// newMarker returns the run's unique marker token.
func newMarker() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("skillcanary: generate the run marker: %w", err)
	}
	return markerPrefix + hex.EncodeToString(buf), nil
}

// register loads the deployment's skill registry — the manager's startup
// step — and checks that every registered bundle carries a content digest.
func (c *canary) register() error {
	registry, err := skills.Load(c.bundleConfig)
	if err != nil {
		c.check(CheckRegistered, false, err.Error())
		return fmt.Errorf("skillcanary: register the deployment skill registry: %w", err)
	}
	c.registry = registry
	c.digests = map[string]string{}
	bundles := registry.Bundles()
	details := make([]string, 0, len(bundles))
	passed := len(bundles) > 0
	for _, bundle := range bundles {
		if !digestShape.MatchString(bundle.Digest) {
			passed = false
			details = append(details, bundle.Name+" carries no sha256 content digest")
			continue
		}
		c.digests[bundle.Name] = bundle.Digest
		details = append(details, bundle.Name+" "+bundle.Digest)
	}
	if len(bundles) == 0 {
		details = append(details, "the config registers no bundle")
	}
	c.check(CheckRegistered, passed, strings.Join(details, "; "))
	if !passed {
		return fmt.Errorf("skillcanary: the deployment config registered no usable bundle: %s", strings.Join(details, "; "))
	}
	return nil
}

// selectedNames returns the lane's selected bundle names, or every name the
// registry holds when the lane selected none.
func (c *canary) selectedNames() []string {
	names := make([]string, 0, len(c.opts.Skills))
	for _, name := range c.opts.Skills {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	if len(names) == 0 {
		return c.registry.Names()
	}
	return names
}

// selectPins resolves the lane's selection against the registry — the
// admission step — and fixes the run's admitted pin set.
func (c *canary) selectPins() error {
	pins, err := skills.Select(c.registry, c.selectedNames())
	if err != nil {
		c.check(CheckSelected, false, err.Error())
		return fmt.Errorf("skillcanary: select the lane's skills: %w", err)
	}
	details := make([]string, 0, len(pins))
	passed := len(pins) > 0
	for _, pin := range pins {
		bundle, found := c.registry.Lookup(pin.Name)
		if !found || bundle.Digest != pin.Digest {
			passed = false
			details = append(details, pin.Name+" did not keep the registered digest")
			continue
		}
		details = append(details, pin.Name+" "+pin.Digest)
	}
	// The admission-side verification the operator runs before it lets a run
	// carry a pin set: every pin must still resolve to the registered
	// content. A selection that names a bundle the registry does not hold,
	// or a digest the registry no longer carries, fails here.
	if err := skills.Verify(c.registry, pins); err != nil {
		passed = false
		details = append(details, err.Error())
	}
	if len(c.opts.Pins) > 0 {
		details = append(details, "the run carries an explicit admitted pin set")
	}
	c.check(CheckSelected, passed, strings.Join(details, "; "))
	if !passed {
		return fmt.Errorf("skillcanary: lane selection did not pin the registered content: %s", strings.Join(details, "; "))
	}
	c.runPins = pins
	if len(c.opts.Pins) > 0 {
		c.runPins = c.opts.Pins
	}
	return nil
}

// roundTrip carries the run's pin envelope and bundle config path through
// the run-context environment and back — the boundary the operator renders
// and the control pod reconstructs.
func (c *canary) roundTrip() error {
	envelope, err := json.Marshal(c.runPins)
	if err != nil {
		return fmt.Errorf("skillcanary: encode the pin envelope: %w", err)
	}
	model := c.model()
	invocation := executor.Invocation{
		RunName:          canaryRunName,
		Namespace:        canaryNamespace,
		Mode:             courierv1alpha1.ModeResolveIssue,
		Repo:             "courier/skill-canary",
		Ref:              1,
		Branch:           "courier/skill-canary",
		Goal:             canaryGoal(),
		Model:            model,
		Roles:            map[string]string{harness.RoleCoordinator: model},
		Workspace:        c.checkoutDir,
		Skills:           string(envelope),
		SkillBundlesFile: c.runConfig,
	}
	values := map[string]string{}
	for _, value := range executor.RunContextEnvironment(invocation) {
		values[value.Name] = value.Value
	}
	recovered, err := executor.InvocationFromEnv(func(name string) string { return values[name] })
	if err != nil {
		c.check(CheckRoundTrip, false, err.Error())
		return fmt.Errorf("skillcanary: reconstruct the invocation from the run context: %w", err)
	}
	passed := recovered.SkillBundlesFile == c.runConfig && samePins(recovered.Skills, c.runPins)
	detail := fmt.Sprintf("%d pin(s) %s; bundle config %s",
		len(c.runPins), excerpt(recovered.Skills), recovered.SkillBundlesFile)
	c.check(CheckRoundTrip, passed, detail)
	if !passed {
		return fmt.Errorf("skillcanary: the pin envelope did not survive the run-context round trip: the recovered invocation carries %q and %q", recovered.Skills, recovered.SkillBundlesFile)
	}
	c.invocation = recovered
	return nil
}

// load resolves the run's pins into the harness's skill view — the
// control-side load — and checks that the loaded content is the pinned
// content.
func (c *canary) load() error {
	pins, err := executor.SkillPins(c.invocation)
	if err != nil {
		c.check(CheckControlLoad, false, err.Error())
		return fmt.Errorf("skillcanary: decode the pinned skill envelope: %w", err)
	}
	loaded, err := skills.LoadForRun(pins, c.invocation.SkillBundlesFile, c.checkoutDir)
	if err != nil {
		c.check(CheckControlLoad, false, err.Error())
		return fmt.Errorf("skillcanary: load the pinned skills: %w", err)
	}
	view, marker, found := markedView(loaded)
	if !found {
		c.check(CheckControlLoad, false, "no loaded skill carries a "+markerPrefix+" token, so the canary cannot prove the instruction reached the model")
		return errors.New("skillcanary: no loaded skill carries a canary marker token")
	}
	set, err := harness.NewSkillSet(loaded)
	if err != nil {
		c.check(CheckControlLoad, false, err.Error())
		return fmt.Errorf("skillcanary: compose the skill set: %w", err)
	}
	c.skillSet = set
	c.view = view
	c.skill = view.Name
	c.marker = marker
	c.digest = view.Digest

	pinned := ""
	for _, pin := range c.runPins {
		if pin.Name == view.Name {
			pinned = pin.Digest
		}
	}
	passed := view.Origin == skills.OriginBundle &&
		view.Digest == pinned &&
		len(view.Files) > 0
	if c.synthetic && !contains(view.Files, canaryAsset) {
		passed = false
	}
	detail := fmt.Sprintf("%s origin %s digest %s; marker %s; assets %s",
		view.Name, view.Origin, view.Digest, marker, strings.Join(view.Files, ", "))
	c.check(CheckControlLoad, passed, detail)
	if !passed {
		return fmt.Errorf("skillcanary: the control-side load did not produce the pinned bundle view for %q", view.Name)
	}
	return nil
}

// openGateway wires the model endpoint the coordinator run drives: the
// hermetic stub, or the operator's real gateway.
func (c *canary) openGateway() error {
	gateway := &harness.Gateway{}
	if url := strings.TrimSpace(c.opts.GatewayURL); url != "" {
		gateway.BaseURL = url
		c.gatewayName = url
		if file := strings.TrimSpace(c.opts.GatewayKeyFile); file != "" {
			key, err := os.ReadFile(file)
			if err != nil {
				return fmt.Errorf("skillcanary: read the gateway key file %s: %w", file, err)
			}
			gateway.APIKey = strings.TrimSpace(string(key))
		}
	} else {
		stub, err := startStub(c.skill)
		if err != nil {
			return err
		}
		c.stub = stub
		gateway.BaseURL = stub.baseURL()
		c.gatewayName = "the built-in stub gateway"
	}
	recorder, err := record(gateway)
	if err != nil {
		return err
	}
	c.recorder = recorder
	c.gateway = gateway
	return nil
}

// runCoordinator drives the native coordinator over the loaded skill set.
func (c *canary) runCoordinator(ctx context.Context) (executor.HarnessResult, error) {
	bindings, err := harness.BindRoles(map[string]string{harness.RoleCoordinator: c.model()})
	if err != nil {
		return executor.HarnessResult{}, fmt.Errorf("skillcanary: bind the coordinator role: %w", err)
	}
	coordinator, err := harness.NewCoordinator(harness.CoordinatorConfig{
		Gateway:  c.gateway,
		Bindings: bindings,
		Skills:   c.skillSet,
	})
	if err != nil {
		return executor.HarnessResult{}, fmt.Errorf("skillcanary: build the coordinator: %w", err)
	}
	return coordinator.Run(ctx, c.invocation), nil
}

// evidence reads the recorded model traffic and checks what the harness
// offered, what the model asked for, what the tool returned, and what the
// model declared.
func (c *canary) evidence() {
	exchanges := c.recorder.exchanges()
	if len(exchanges) == 0 {
		c.check(CheckSkillIndex, false, "the coordinator sent no model request")
		c.check(CheckToolOffered, false, "the coordinator sent no model request")
		c.check(CheckToolCall, false, "the coordinator sent no model request")
		c.check(CheckToolResult, false, "the coordinator sent no model request")
		c.check(CheckModelUsed, false, "the coordinator sent no model request")
		return
	}
	first, err := decodeRequest(exchanges[0].Request)
	if err != nil {
		c.check(CheckSkillIndex, false, err.Error())
		c.check(CheckToolOffered, false, err.Error())
	} else {
		index, found := skillIndexMessage(first.Messages, c.skillSet)
		c.check(CheckSkillIndex, found, indexDetail(index, found))
		offered := toolNames(first.Tools)
		c.check(CheckToolOffered, contains(offered, harness.ToolSkill), "tools offered: "+strings.Join(offered, ", "))
		if found {
			if c.synthetic {
				c.note("skill index: %s", excerpt(index))
			} else {
				c.note("skill index: %d bytes of the composed index", len(index))
			}
		}
	}
	c.evidenceToolCall(exchanges)
	c.evidenceToolResult(exchanges)
	c.evidenceDeclaration(exchanges)
}

// evidenceToolCall checks what the model asked for: exactly one tool
// request, and it is the skill tool.
func (c *canary) evidenceToolCall(exchanges []recordedExchange) {
	var asked []recordedCall
	for _, exchange := range exchanges {
		turn, err := decodeStream(exchange.Response)
		if err != nil {
			continue
		}
		asked = append(asked, turn.calls...)
	}
	var carried []recordedCall
	for _, exchange := range exchanges {
		request, err := decodeRequest(exchange.Request)
		if err != nil {
			continue
		}
		carried = append(carried, transcriptCalls(request)...)
	}
	passed := len(asked) == 1 &&
		asked[0].Name == harness.ToolSkill &&
		callArgumentName(asked[0].Arguments) == c.skill &&
		len(carried) == 1 &&
		carried[0].Name == harness.ToolSkill
	detail := "no tool call was recorded"
	if len(asked) > 0 {
		detail = fmt.Sprintf("%d tool call(s): %s", len(asked), strings.Join(calledTools(asked), ", "))
	}
	c.check(CheckToolCall, passed, detail)
}

// evidenceToolResult checks what the skill tool returned to the model: the
// verified instruction body and the supporting asset's path.
func (c *canary) evidenceToolResult(exchanges []recordedExchange) {
	result := ""
	for _, exchange := range exchanges {
		request, err := decodeRequest(exchange.Request)
		if err != nil {
			continue
		}
		for _, message := range request.Messages {
			if message.Role == "tool" {
				result = message.Content
			}
		}
	}
	asset := ""
	if len(c.view.Files) > 0 {
		asset = c.view.Source + "/" + c.view.Files[0]
	}
	passed := result != "" && strings.Contains(result, c.marker) && asset != "" && strings.Contains(result, asset)
	detail := "the skill tool returned no result"
	if result != "" {
		detail = fmt.Sprintf("%d bytes; marker %s; asset %s", len(result), c.marker, asset)
	}
	c.check(CheckToolResult, passed, detail)
	if result == "" {
		return
	}
	if c.synthetic {
		c.note("skill tool result: %s", excerpt(result))
		return
	}
	c.note("skill tool result: %d bytes of verified instruction content", len(result))
}

// evidenceDeclaration checks the model's final message: it declared the
// run's ending and quoted the marker only the skill body carries. The
// detail names the endpoint that answered, because the built-in stub quotes
// the marker it read in the request the harness sent, while a real gateway
// answers with what its model wrote.
func (c *canary) evidenceDeclaration(exchanges []recordedExchange) {
	last := exchanges[len(exchanges)-1]
	turn, err := decodeStream(last.Response)
	if err != nil {
		c.check(CheckModelUsed, false, err.Error())
		return
	}
	declaration, err := decodeDeclaration(turn.content)
	if err != nil {
		c.check(CheckModelUsed, false, err.Error())
		return
	}
	passed := declaration.Outcome == string(executor.OutcomeNoChangeNeeded) &&
		strings.Contains(declaration.Evidence, c.marker)
	detail := fmt.Sprintf("%s answered: %s", c.gatewayName, excerpt(turn.content))
	c.check(CheckModelUsed, passed, detail)
	c.note("final declaration: %s", excerpt(turn.content))
}

// ending checks the run's own ending: a declared no_change_needed with no
// publication. The canary wires no publication path, so the run could not
// have published anything; the ending is what it declares.
func (c *canary) ending(result executor.HarnessResult) {
	passed := result.Err == nil &&
		result.Outcome == executor.OutcomeNoChangeNeeded &&
		result.Kind == courierv1alpha1.NativeResultNoChange
	detail := fmt.Sprintf("outcome %q, kind %q, no publication path configured", result.Outcome, result.Kind)
	if result.Err != nil {
		detail = result.Err.Error()
	}
	c.check(CheckNoPublication, passed, detail)
}

// unchanged checks that neither the deployment's content nor the target
// checkout moved while the run happened.
func (c *canary) unchanged() {
	registry, err := skills.Load(c.bundleConfig)
	if err != nil {
		c.check(CheckUnchanged, false, err.Error())
	} else {
		details := make([]string, 0, len(c.digests))
		names := make([]string, 0, len(c.digests))
		for name := range c.digests {
			names = append(names, name)
		}
		sort.Strings(names)
		passed := len(names) > 0
		for _, name := range names {
			digest := c.digests[name]
			bundle, found := registry.Lookup(name)
			switch {
			case !found:
				passed = false
				details = append(details, name+" is no longer registered")
			case bundle.Digest != digest:
				passed = false
				details = append(details, name+" now digests "+bundle.Digest)
			default:
				details = append(details, name+" "+bundle.Digest)
			}
		}
		c.check(CheckUnchanged, passed, strings.Join(details, "; "))
	}
	c.digestReacts()
	after, err := checkoutDigest(c.checkoutDir)
	if err != nil {
		c.check(CheckCheckout, false, err.Error())
		return
	}
	passed := after == c.checkoutBefore
	detail := fmt.Sprintf("%s at %s", after, c.checkoutDir)
	if !passed {
		detail = fmt.Sprintf("the checkout changed: %s before, %s after", c.checkoutBefore, after)
	}
	c.check(CheckCheckout, passed, detail)
}

// digestReacts checks that the registered digest is a content identity: a
// bundle whose content changed digests differently. The check copies the
// registered bundle, changes one byte of the copy, and registers the copy.
// A digest that ignores the content would pass the whole chain while
// carrying no content identity at all.
func (c *canary) digestReacts() {
	name := ""
	source := ""
	for _, candidate := range c.syntheticNames() {
		bundle, found := c.registry.Lookup(candidate)
		if found {
			name = candidate
			source = bundle.Path
			break
		}
	}
	if source == "" {
		c.check(CheckDigestReacts, false, "the registry holds no bundle to copy")
		return
	}
	dir, err := os.MkdirTemp("", "courier-skill-canary-mutated-")
	if err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	c.owned = append(c.owned, dir)
	copyRoot := filepath.Join(dir, "bundle")
	if err := copyTree(source, copyRoot); err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	document := filepath.Join(copyRoot, "SKILL.md")
	content, err := os.ReadFile(document)
	if err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	if err := os.WriteFile(document, append(content, '\n'), 0o644); err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	config := fmt.Sprintf("bundles:\n- name: %s\n  source: %s\n", name, yamlScalar(copyRoot))
	configPath := filepath.Join(dir, "bundles.yaml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	mutated, err := skills.Load(configPath)
	if err != nil {
		c.check(CheckDigestReacts, false, err.Error())
		return
	}
	mutatedBundle, found := mutated.Lookup(name)
	if !found {
		c.check(CheckDigestReacts, false, "the mutated copy did not register")
		return
	}
	original, found := c.registry.Lookup(name)
	if !found {
		c.check(CheckDigestReacts, false, "the original bundle is no longer registered")
		return
	}
	passed := mutatedBundle.Digest != original.Digest
	detail := fmt.Sprintf("the original digests %s, the changed copy digests %s", original.Digest, mutatedBundle.Digest)
	c.check(CheckDigestReacts, passed, detail)
}

// syntheticNames lists the registered bundle names the canary can copy for
// the digest check.
func (c *canary) syntheticNames() []string {
	if c.synthetic {
		return []string{canaryBundleName}
	}
	return c.registry.Names()
}

// copyTree copies a directory tree, file by file.
func copyTree(source, target string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, content, 0o644)
	})
}

// model returns the model identifier bound to the coordinator role.
func (c *canary) model() string {
	if model := strings.TrimSpace(c.opts.Model); model != "" {
		return model
	}
	return canaryModel
}

// canaryGoal is the goal the canary's coordinator run carries. It asks the
// model to load the pinned skill and to quote the marker the instruction
// body carries. The marker itself is deliberately absent: only the skill
// body carries it, so a declaration that quotes it proves the body reached
// the model.
func canaryGoal() string {
	return fmt.Sprintf("This run verifies the deployment's pinned skill bundle. Load the pinned skill with the %s tool and follow its instructions. The skill's instruction body carries a unique marker token; quote that marker verbatim in the evidence of your final declaration. There is no work to do: your final message must be exactly one JSON object, {\"outcome\":\"no_change_needed\",\"evidence\":\"what you observed, including the marker\"}.",
		harness.ToolSkill)
}

// stubDeclaration is the declaration the built-in stub sends: the
// no_change_needed ending whose evidence quotes the marker the stub read in
// the request it answered. A request that carried no marker produces a
// declaration without one, so the claim that the model used the instruction
// fails when the harness never delivered the instruction body.
func stubDeclaration(skill, marker string) string {
	evidence := fmt.Sprintf("the pinned skill %s was loaded", skill)
	if marker != "" {
		evidence += fmt.Sprintf(" and its instruction body carries the marker %s", marker)
	}
	encoded, err := json.Marshal(map[string]string{
		"outcome":  string(executor.OutcomeNoChangeNeeded),
		"evidence": evidence,
	})
	if err != nil {
		return `{"outcome":"no_change_needed","evidence":"the pinned skill was loaded"}`
	}
	return string(encoded)
}

// markedView returns the loaded view whose instruction body carries the
// canary marker, and the marker itself.
func markedView(loaded skills.LoadedSkills) (skills.SkillView, string, bool) {
	for _, view := range loaded.Visible {
		if view.Origin != skills.OriginBundle {
			continue
		}
		if marker := markerIn(view.Instructions); marker != "" {
			return view, marker, true
		}
	}
	return skills.SkillView{}, "", false
}

// markerIn returns the marker token one instruction body carries.
func markerIn(instructions string) string {
	match := markerShape.FindString(instructions)
	return match
}

// samePins reports whether the carried envelope names the same pins as the
// expected set.
func samePins(envelope string, expected []skills.Pin) bool {
	var carried []skills.Pin
	if err := json.Unmarshal([]byte(envelope), &carried); err != nil {
		return false
	}
	if len(carried) != len(expected) {
		return false
	}
	for i, pin := range carried {
		if pin.Name != expected[i].Name || pin.Digest != expected[i].Digest {
			return false
		}
	}
	return true
}

// checkoutEntries lists the entries the checkout directory holds.
func checkoutEntries(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("skillcanary: read the checkout directory %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

// checkoutDigest is the content identity of a checkout tree: every regular
// file's relative path and its bytes, in path order. A change to a file's
// content changes the digest, so the canary can tell an edited checkout
// from an untouched one.
func checkoutDigest(dir string) (string, error) {
	hash := sha256.New()
	var paths []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("skillcanary: walk the checkout directory %s: %w", dir, err)
	}
	sort.Strings(paths)
	for _, relative := range paths {
		content, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(relative)))
		if err != nil {
			return "", fmt.Errorf("skillcanary: read the checkout file %s: %w", relative, err)
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", relative, len(content))
		hash.Write(content)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// contains reports whether the list holds the value.
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// excerpt bounds a text the report prints.
func excerpt(text string) string {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) <= maxExcerptBytes {
		return trimmed
	}
	return trimmed[:maxExcerptBytes] + "... (truncated)"
}

// yamlScalar renders a path as a double-quoted YAML scalar. The path comes
// from the operator's work directory, so the quoting keeps a path holding a
// colon or a quote out of the config's syntax.
func yamlScalar(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(encoded)
}

// Render writes the report for an operator: every check with the evidence
// it was read from, then the transcript lines the operator reads.
func (r *Report) Render(w io.Writer) error {
	if r == nil {
		return errors.New("skillcanary: there is no report to render")
	}
	if _, err := fmt.Fprintf(w, "courier native skill-bundle canary\n\nskill:  %s\ndigest: %s\nmarker: %s\ngateway: %s\n\nchecks:\n", r.Skill, r.Digest, r.Marker, r.Gateway); err != nil {
		return err
	}
	for _, check := range r.Checks {
		status := "pass"
		if !check.Passed {
			status = "FAIL"
		}
		if _, err := fmt.Fprintf(w, "  [%s] %s\n", status, check.Name); err != nil {
			return err
		}
		if check.Detail != "" {
			if _, err := fmt.Fprintf(w, "         %s\n", excerpt(check.Detail)); err != nil {
				return err
			}
		}
	}
	if len(r.Transcript) > 0 {
		if _, err := fmt.Fprint(w, "\ntranscript:\n"); err != nil {
			return err
		}
		for _, line := range r.Transcript {
			if _, err := fmt.Fprintf(w, "  %s\n", line); err != nil {
				return err
			}
		}
	}
	if r.Truncated {
		if _, err := fmt.Fprint(w, "\nThe canary kept only the first model exchanges as evidence.\n"); err != nil {
			return err
		}
	}
	return nil
}

// --- the hermetic stub gateway ---------------------------------------------

// stubGateway is the canary's OpenAI-compatible model endpoint. It serves
// the two turns a canary run makes — a request for the skill tool, then the
// run's final declaration — and nothing else. It is deterministic and
// hermetic: no network, no credentials, no model. A canary run against it
// proves the harness's chain, not a model's judgment.
//
// The stub's final declaration quotes the marker it read in the request it
// answered, never a marker it was told to quote: the harness delivered the
// skill tool's result into the model's context, so the declaration can carry
// the marker only when that delivery happened.
type stubGateway struct {
	listener net.Listener
	server   *http.Server
	skill    string

	mu    sync.Mutex
	turns int
}

// startStub binds the stub gateway on a local listener.
func startStub(skill string) (*stubGateway, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("skillcanary: listen for the stub gateway: %w", err)
	}
	stub := &stubGateway{listener: listener, skill: skill}
	mux := http.NewServeMux()
	mux.HandleFunc("/", stub.serve)
	stub.server = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go stub.server.Serve(listener)
	return stub, nil
}

// baseURL is the gateway root the coordinator drives: the same shape a
// deployment's OpenAI-compatible gateway exposes.
func (s *stubGateway) baseURL() string {
	return "http://" + s.listener.Addr().String() + "/v1"
}

// close stops the stub gateway.
func (s *stubGateway) close() {
	s.server.Close()
}

// serve answers one chat completion.
func (s *stubGateway) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
		http.Error(w, "the stub gateway serves POST /chat/completions", http.StatusNotFound)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRecordedBodyBytes+1))
	r.Body.Close()
	if err != nil {
		http.Error(w, "unreadable request", http.StatusBadRequest)
		return
	}
	if len(body) > maxRecordedBodyBytes {
		http.Error(w, "the request exceeds the recording bound", http.StatusRequestEntityTooLarge)
		return
	}
	s.mu.Lock()
	turn := s.turns
	s.turns++
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, s.response(turn, body))
}

// response renders the stub's answer for one turn: the skill tool request
// first, the final declaration after it.
func (s *stubGateway) response(turn int, request []byte) string {
	var b strings.Builder
	if turn == 0 {
		b.WriteString(sseFrame(streamChunk{Choices: []streamChoice{{
			Index: 0,
			Delta: streamDelta{ToolCalls: []streamToolCall{{
				Index:    0,
				ID:       "call-skill-1",
				Type:     "function",
				Function: streamFunction{Name: harness.ToolSkill, Arguments: skillArguments(s.skill)},
			}}},
		}}}))
		b.WriteString(sseFrame(streamChunk{Choices: []streamChoice{{
			Index:        0,
			FinishReason: "tool_calls",
		}}}))
	} else {
		b.WriteString(sseFrame(streamChunk{Choices: []streamChoice{{
			Index: 0,
			Delta: streamDelta{Content: stubDeclaration(s.skill, markerFromRequest(request))},
		}}}))
		b.WriteString(sseFrame(streamChunk{Choices: []streamChoice{{
			Index:        0,
			FinishReason: "stop",
		}}}))
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

// markerFromRequest returns the marker the request carried in the skill
// tool's result. It is the only source the stub's declaration draws on, so
// the declaration can quote the marker only when the harness delivered the
// instruction body into the model's context.
func markerFromRequest(body []byte) string {
	request, err := decodeRequest(body)
	if err != nil {
		return ""
	}
	for _, message := range request.Messages {
		if message.Role != "tool" {
			continue
		}
		if marker := markerIn(message.Content); marker != "" {
			return marker
		}
	}
	return ""
}

// skillArguments renders the skill tool's arguments.
func skillArguments(skill string) string {
	encoded, err := json.Marshal(map[string]string{"name": skill})
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// --- the recorded model traffic --------------------------------------------

// streamChunk is one OpenAI-compatible chat-completion chunk.
type streamChunk struct {
	Choices []streamChoice `json:"choices"`
}

// streamChoice is one choice of a chat-completion chunk.
type streamChoice struct {
	Index        int         `json:"index"`
	Delta        streamDelta `json:"delta"`
	FinishReason string      `json:"finish_reason,omitempty"`
}

// streamDelta is one choice's delta.
type streamDelta struct {
	Content   string           `json:"content,omitempty"`
	ToolCalls []streamToolCall `json:"tool_calls,omitempty"`
}

// streamToolCall is one tool-call fragment.
type streamToolCall struct {
	Index    int            `json:"index"`
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Function streamFunction `json:"function"`
}

// streamFunction is the function a tool-call fragment names.
type streamFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// sseFrame renders one SSE data frame.
func sseFrame(chunk streamChunk) string {
	encoded, err := json.Marshal(chunk)
	if err != nil {
		return ""
	}
	return "data: " + string(encoded) + "\n\n"
}

// recordedCall is one tool request the model made.
type recordedCall struct {
	Name      string
	Arguments string
}

// streamTurn is one recorded model turn.
type streamTurn struct {
	content string
	calls   []recordedCall
	finish  string
}

// decodeStream reads one recorded SSE body: the content it carried and the
// tool requests it asked for.
func decodeStream(body []byte) (streamTurn, error) {
	var (
		turn      streamTurn
		fragments = map[int]*recordedCall{}
	)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return turn, fmt.Errorf("skillcanary: decode the recorded stream chunk: %w", err)
		}
		for _, choice := range chunk.Choices {
			turn.content += choice.Delta.Content
			for _, call := range choice.Delta.ToolCalls {
				fragment, ok := fragments[call.Index]
				if !ok {
					fragment = &recordedCall{}
					fragments[call.Index] = fragment
				}
				if call.Function.Name != "" {
					fragment.Name = call.Function.Name
				}
				fragment.Arguments += call.Function.Arguments
			}
			if choice.FinishReason != "" {
				turn.finish = choice.FinishReason
			}
		}
	}
	indexes := make([]int, 0, len(fragments))
	for index := range fragments {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		turn.calls = append(turn.calls, *fragments[index])
	}
	return turn, nil
}

// recordedRequest is one model request the harness sent.
type recordedRequest struct {
	Model    string            `json:"model"`
	Messages []recordedMessage `json:"messages"`
	Tools    []recordedTool    `json:"tools"`
}

// recordedMessage is one message of a recorded request.
type recordedMessage struct {
	Role       string             `json:"role"`
	Content    string             `json:"content"`
	ToolCallID string             `json:"tool_call_id"`
	ToolCalls  []recordedToolCall `json:"tool_calls"`
}

// recordedToolCall is one tool request the model made in a recorded turn.
type recordedToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function recordedFunction `json:"function"`
}

// recordedFunction is the function a recorded tool request names.
type recordedFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// recordedTool is one tool declaration a recorded request offered.
type recordedTool struct {
	Type     string           `json:"type"`
	Function recordedFunction `json:"function"`
}

// decodeRequest reads one recorded model request.
func decodeRequest(body []byte) (recordedRequest, error) {
	var request recordedRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return recordedRequest{}, fmt.Errorf("skillcanary: decode the recorded model request: %w", err)
	}
	return request, nil
}

// toolNames lists the tools a recorded request offered.
func toolNames(tools []recordedTool) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Function.Name)
	}
	return names
}

// skillIndexMessage returns the system message that carried the composed
// skill index. The index the harness composed is the whole message, so the
// check compares against that composition rather than against a phrase the
// check itself chose.
func skillIndexMessage(messages []recordedMessage, set *harness.SkillSet) (string, bool) {
	if set == nil {
		return "", false
	}
	index := set.Index()
	for _, message := range messages {
		if message.Role != "system" {
			continue
		}
		if strings.Contains(message.Content, index) {
			return message.Content, true
		}
	}
	return "", false
}

// indexDetail renders the evidence of the skill index check.
func indexDetail(index string, found bool) string {
	if !found {
		return "no system message carried the composed skill index"
	}
	return fmt.Sprintf("%d bytes of system message carry the composed index", len(index))
}

// callArgumentName returns the skill name a skill tool request named.
func callArgumentName(arguments string) string {
	var call struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(arguments), &call); err != nil {
		return ""
	}
	return call.Name
}

// transcriptCalls returns the tool requests the recorded transcript carries.
func transcriptCalls(request recordedRequest) []recordedCall {
	var calls []recordedCall
	for _, message := range request.Messages {
		for _, call := range message.ToolCalls {
			calls = append(calls, recordedCall{Name: call.Function.Name, Arguments: call.Function.Arguments})
		}
	}
	return calls
}

// calledTools lists the tools the recorded turns called.
func calledTools(calls []recordedCall) []string {
	names := make([]string, 0, len(calls))
	for _, call := range calls {
		names = append(names, call.Name)
	}
	return names
}

// outcomeDeclaration is the run's terminal declaration.
type outcomeDeclaration struct {
	Outcome  string `json:"outcome"`
	Evidence string `json:"evidence"`
}

// decodeDeclaration reads the model's final message as a declaration.
func decodeDeclaration(content string) (outcomeDeclaration, error) {
	var declaration outcomeDeclaration
	if err := json.Unmarshal([]byte(strings.TrimSpace(content)), &declaration); err != nil {
		return outcomeDeclaration{}, fmt.Errorf("the final message is not an outcome declaration: %w", err)
	}
	return declaration, nil
}

// --- the recording transport -----------------------------------------------

// recorder captures the model traffic the harness produced.
type recorder struct {
	mu       sync.Mutex
	captured []recordedExchange
	// dropped reports that the canary kept fewer exchanges than the
	// harness made, so the report says the evidence is partial.
	dropped bool
}

// recordedExchange is one model request and the response it drew.
type recordedExchange struct {
	Request  []byte
	Response []byte
}

// record installs the recording transport on the gateway's client, so every
// request and response the harness exchanged is available as evidence.
func record(gateway *harness.Gateway) (*recorder, error) {
	client := gateway.HTTP
	if client == nil {
		client = &http.Client{}
	}
	clone := *client
	base := clone.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	captured := &recorder{}
	clone.Transport = &recordingTransport{base: base, recorder: captured}
	gateway.HTTP = &clone
	return captured, nil
}

// exchanges returns the recorded model traffic, in order.
func (r *recorder) exchanges() []recordedExchange {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordedExchange, len(r.captured))
	copy(out, r.captured)
	return out
}

// add records one exchange. The recorder keeps a bounded number of
// exchanges, so a long run cannot grow the canary's memory without limit.
func (r *recorder) add(request, response []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.captured) >= maxRecordedExchanges {
		r.dropped = true
		return
	}
	r.captured = append(r.captured, recordedExchange{Request: request, Response: response})
}

// truncated reports that the recorder dropped model exchanges.
func (r *recorder) truncated() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// recordingTransport records the bodies that pass through it and hands the
// harness the unchanged exchange.
type recordingTransport struct {
	base     http.RoundTripper
	recorder *recorder
}

// RoundTrip performs one model call and records what it carried.
func (t *recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var requestBody []byte
	if request.Body != nil {
		body, err := io.ReadAll(io.LimitReader(request.Body, maxRecordedBodyBytes+1))
		if err != nil {
			return nil, fmt.Errorf("skillcanary: read the model request: %w", err)
		}
		request.Body.Close()
		if len(body) > maxRecordedBodyBytes {
			return nil, fmt.Errorf("skillcanary: the model request exceeds the %d byte recording bound", maxRecordedBodyBytes)
		}
		requestBody = body
		request.Body = io.NopCloser(bytes.NewReader(body))
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRecordedBodyBytes+1))
	response.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("skillcanary: read the model response: %w", err)
	}
	if len(body) > maxRecordedBodyBytes {
		return nil, fmt.Errorf("skillcanary: the model response exceeds the %d byte recording bound", maxRecordedBodyBytes)
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	t.recorder.add(requestBody, body)
	return response, nil
}
