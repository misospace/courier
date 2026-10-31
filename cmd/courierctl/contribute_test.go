package main

import (
	"context"
	"io"
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source/adhoc"
)

// runLineRe extracts the namespace, run name, and idempotency key the CLI
// reports on stdout, so tests can compare identities across submissions
// without re-deriving the adapter's digests.
var runLineRe = regexp.MustCompile(`(?:created coder run|resuming existing coder run) (\S+)/(\S+) \(work item ([^)]+)\)`)

// freshKeyRe is the settled shape of a --fresh minted key.
var freshKeyRe = regexp.MustCompile(`^adhoc-cli-[0-9a-f]{16}$`)

// newTestClient builds a fresh controller-runtime fake client with the
// Courier API types registered, the same shape the adapter's own tests
// use, so the CLI is exercised against the identical surface.
func newTestClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := courierv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add courier scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// validOptions is the baseline well-formed flag set; tests mutate a copy
// per case. It mirrors the adapter's validAdmission baseline.
func validOptions() contributeOptions {
	return contributeOptions{
		baseRepo:  "upstream/project",
		baseRef:   "main",
		goal:      "fix the login flow",
		headRepo:  "contributor/project",
		lane:      "local",
		namespace: "contrib",
	}
}

// runOptions runs runContribute directly against a fake client and
// returns the exit code and captured output, keeping the core-path tests
// free of flag parsing.
func runOptions(t *testing.T, opts contributeOptions, c client.Client) (int, string, string) {
	t.Helper()
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := runContribute(context.Background(), opts, c, stdout, stderr)
	return code, stdout.String(), stderr.String()
}

// TestContributeCreatesRun is the happy path: one well-formed invocation
// creates exactly one run and reports the created form of the message
// with the run name and work item the adapter minted.
func TestContributeCreatesRun(t *testing.T) {
	c := newTestClient(t)

	code, stdout, stderr := runOptions(t, validOptions(), c)
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr %q; want %d", code, stderr, exitOK)
	}
	if !strings.Contains(stdout, "created coder run contrib/") {
		t.Errorf("stdout %q; want a created coder run line", stdout)
	}
	if strings.Contains(stdout, "resuming") {
		t.Errorf("stdout %q; a first submission must not claim a resume", stdout)
	}
	m := runLineRe.FindStringSubmatch(stdout)
	if m == nil {
		t.Fatalf("stdout %q carries no run line", stdout)
	}
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: m[1], Name: m[2]}, &run); err != nil {
		t.Fatalf("get created run: %v", err)
	}
	if run.Spec.WorkItemID != m[3] {
		t.Errorf("spec.workItemID = %q; want the reported %q", run.Spec.WorkItemID, m[3])
	}
}

// TestContributeResumesDuplicate drives the duplicate-submission path:
// the same explicit key twice must create one run and surface the
// existing one as a resume, with the hint pointing at --fresh.
func TestContributeResumesDuplicate(t *testing.T) {
	c := newTestClient(t)
	opts := validOptions()
	opts.key = "caller-key-1"

	code1, stdout1, stderr1 := runOptions(t, opts, c)
	if code1 != exitOK {
		t.Fatalf("first submission exit code = %d, stderr %q; want %d", code1, stderr1, exitOK)
	}
	code2, stdout2, stderr2 := runOptions(t, opts, c)
	if code2 != exitOK {
		t.Fatalf("second submission exit code = %d, stderr %q; want %d", code2, stderr2, exitOK)
	}
	if !strings.Contains(stdout2, "resuming existing coder run contrib/") {
		t.Errorf("second stdout %q; want a resuming line", stdout2)
	}
	if first, second := runLineRe.FindStringSubmatch(stdout1)[3], runLineRe.FindStringSubmatch(stdout2)[3]; first != second {
		t.Errorf("work items diverged across a duplicate submission: %q vs %q", first, second)
	}
	if !strings.Contains(stdout2, "--fresh") {
		t.Errorf("second stdout %q; want the --fresh hint", stdout2)
	}
	var list courierv1alpha1.CoderRunList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("runs after duplicate submission = %d; want 1", len(list.Items))
	}
}

// TestContributeFreshCreatesNewRun pins the --fresh contract: identical
// content resubmitted with --fresh must land on a distinct minted key and
// therefore a second run, where the deterministic mint would have resumed.
func TestContributeFreshCreatesNewRun(t *testing.T) {
	c := newTestClient(t)
	if code, _, stderr := runOptions(t, validOptions(), c); code != exitOK {
		t.Fatalf("first submission exit code = %d, stderr %q", code, stderr)
	}
	opts := validOptions()
	opts.fresh = true
	code, stdout, stderr := runOptions(t, opts, c)
	if code != exitOK {
		t.Fatalf("fresh submission exit code = %d, stderr %q", code, stderr)
	}
	if !strings.Contains(stdout, "created coder run contrib/") {
		t.Errorf("fresh stdout %q; want a created run", stdout)
	}
	key := runLineRe.FindStringSubmatch(stdout)[3]
	if !freshKeyRe.MatchString(key) {
		t.Errorf("fresh key %q; want the adhoc-cli-<16 hex> shape", key)
	}
	var list courierv1alpha1.CoderRunList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("runs after fresh submission = %d; want 2", len(list.Items))
	}
	for _, run := range list.Items {
		if run.Spec.WorkItemID != key && strings.HasPrefix(run.Spec.WorkItemID, "adhoc-cli-") {
			t.Errorf("run %s carries another fresh key %q", run.Name, run.Spec.WorkItemID)
		}
	}
}

// TestContributeRejectsFreshWithKey pins the flag-conflict rejection: the
// two idempotency strategies are mutually exclusive, the rejection is a
// usage error, and no client is ever built.
func TestContributeRejectsFreshWithKey(t *testing.T) {
	built := false
	newClient := func(string) (client.Client, error) {
		built = true
		return newTestClient(t), nil
	}
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	args := []string{
		"--base-repo", "upstream/project", "--base-ref", "main",
		"--goal", "fix the login flow", "--head-repo", "contributor/project",
		"--lane", "local", "--namespace", "contrib",
		"--key", "caller-key-1", "--fresh",
	}
	code := runContributeArgs(context.Background(), args, newClient, stdout, stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d; want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "--fresh cannot be combined with --key") {
		t.Errorf("stderr %q; want the conflict explanation", stderr.String())
	}
	if built {
		t.Error("client was built for a usage rejection")
	}
}

// TestContributeArgsMapIntoAdmission drives the full flag-parsing path —
// runContributeArgs with a real argument vector and an injected fake
// client — and asserts the parsed flags land on the right CoderRun spec
// fields. It is the proof that a swapped base and head repo, a lost lane,
// or a dropped goal cannot slip through parsing unnoticed.
func TestContributeArgsMapIntoAdmission(t *testing.T) {
	c := newTestClient(t)
	stdout := &strings.Builder{}
	args := []string{
		"--base-repo", "upstream/project", "--base-ref", "release-1",
		"--goal", "fix the login flow", "--head-repo", "contributor/project",
		"--lane", "local", "--upstream-issue", "https://example.com/issues/7",
		"--namespace", "contrib", "--debug", "--key", "caller-key-1",
	}
	code := runContributeArgs(context.Background(), args, func(string) (client.Client, error) { return c, nil }, stdout, io.Discard)
	if code != exitOK {
		t.Fatalf("exit code = %d; want %d", code, exitOK)
	}
	m := runLineRe.FindStringSubmatch(stdout.String())
	if m == nil {
		t.Fatalf("stdout %q carries no run line", stdout.String())
	}
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: m[1], Name: m[2]}, &run); err != nil {
		t.Fatalf("get created run: %v", err)
	}
	spec := run.Spec
	if spec.Repo != "upstream/project" {
		t.Errorf("spec.repo = %q; want the --base-repo value", spec.Repo)
	}
	if spec.WorkRepo != "contributor/project" {
		t.Errorf("spec.workRepo = %q; want the --head-repo value", spec.WorkRepo)
	}
	if spec.Lane != "local" {
		t.Errorf("spec.lane = %q; want the --lane value", spec.Lane)
	}
	if !spec.Debug {
		t.Error("spec.debug = false; want the --debug value")
	}
	if spec.WorkItemID != "caller-key-1" {
		t.Errorf("spec.workItemID = %q; want the --key value", spec.WorkItemID)
	}
	if spec.Mode != courierv1alpha1.ModeContribute || spec.Source != adhoc.SourceName {
		t.Errorf("spec mode/source = %q/%q; want contribute/adhoc", spec.Mode, spec.Source)
	}
	if !strings.Contains(spec.SourceContext, "fix the login flow") ||
		!strings.Contains(spec.SourceContext, "https://example.com/issues/7") {
		t.Errorf("spec.sourceContext %q lacks the goal or upstream issue", spec.SourceContext)
	}
}

// TestContributeFreshWithBlankKeyIsUsage pins the raw-key conflict check:
// a whitespace-only --key alongside --fresh is rejected as a usage error,
// not silently resolved in --fresh's favor.
func TestContributeFreshWithBlankKeyIsUsage(t *testing.T) {
	built := false
	newClient := func(string) (client.Client, error) {
		built = true
		return newTestClient(t), nil
	}
	stderr := &strings.Builder{}
	args := []string{
		"--base-repo", "upstream/project", "--base-ref", "main",
		"--goal", "fix the login flow", "--head-repo", "contributor/project",
		"--lane", "local", "--key", "  ", "--fresh",
	}
	code := runContributeArgs(context.Background(), args, newClient, io.Discard, stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d; want %d", code, exitUsage)
	}
	if built {
		t.Error("client was built for a usage rejection")
	}
}

// TestContributeRequiresFlags proves missing required flags fail as a
// usage error naming every missing flag, before any client is built and
// therefore with no cluster contact at all.
func TestContributeRequiresFlags(t *testing.T) {
	built := false
	newClient := func(string) (client.Client, error) {
		built = true
		return newTestClient(t), nil
	}
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := runContributeArgs(context.Background(), nil, newClient, stdout, stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d; want %d", code, exitUsage)
	}
	stderrText := stderr.String()
	for _, flagName := range []string{"--base-repo", "--base-ref", "--goal", "--head-repo", "--lane"} {
		if !strings.Contains(stderrText, flagName) {
			t.Errorf("stderr %q does not name missing %s", stderrText, flagName)
		}
	}
	if built {
		t.Error("client was built for a usage rejection")
	}
}

// TestContributeSurfacesInvalidAdmission proves a malformed value (here a
// base repo without the owner/name shape) is surfaced as the adapter's
// ErrInvalidAdmission message and mapped to the documented validation exit
// code, with no run created.
func TestContributeSurfacesInvalidAdmission(t *testing.T) {
	c := newTestClient(t)
	opts := validOptions()
	opts.baseRepo = "not-a-repo"

	code, stdout, stderr := runOptions(t, opts, c)
	if code != exitUsage {
		t.Fatalf("exit code = %d; want %d", code, exitUsage)
	}
	if !strings.Contains(stderr, "invalid admission") {
		t.Errorf("stderr %q; want the adapter's invalid-admission wording", stderr)
	}
	if !strings.Contains(stderr, "must be of the form owner/name") {
		t.Errorf("stderr %q; want the adapter's base-repo detail", stderr)
	}
	if stdout != "" {
		t.Errorf("stdout = %q; a rejected admission prints nothing", stdout)
	}
	var list courierv1alpha1.CoderRunList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("runs after rejected admission = %d; want 0", len(list.Items))
	}
}

// TestContributeForeignRunIsError pins the foreign-run mapping: a run
// already occupying the derived name under another source or key fails
// closed with the documented cluster-failure exit code, and the foreign
// object stays untouched.
func TestContributeForeignRunIsError(t *testing.T) {
	c := newTestClient(t)
	opts := validOptions()
	opts.key = "colliding-key"
	// The adapter derives the run name from the key alone; place the
	// foreign object exactly where the adapter will look, without
	// duplicating the adapter's digest logic, by admitting through a
	// throwaway client and taking the reported name.
	foreignName := derivedRunName(t, opts.key)
	foreign := &courierv1alpha1.CoderRun{}
	foreign.Name = foreignName
	foreign.Namespace = "contrib"
	foreign.Spec = courierv1alpha1.CoderRunSpec{
		Mode:       courierv1alpha1.ModeContribute,
		Source:     "manual",
		WorkItemID: "someone-elses-key",
	}
	if err := c.Create(context.Background(), foreign); err != nil {
		t.Fatalf("seed foreign run: %v", err)
	}

	code, _, stderr := runOptions(t, opts, c)
	if code != exitError {
		t.Fatalf("exit code = %d, stderr %q; want %d", code, stderr, exitError)
	}
	if !strings.Contains(stderr, "belongs to a different admission") {
		t.Errorf("stderr %q; want the adapter's foreign-run wording", stderr)
	}
	var run courierv1alpha1.CoderRun
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "contrib", Name: foreignName}, &run); err != nil {
		t.Fatalf("get foreign run: %v", err)
	}
	if run.Spec.Source != "manual" {
		t.Errorf("foreign run source = %q; the foreign object must be untouched", run.Spec.Source)
	}
}

// derivedRunName reports the run name the adapter will derive for a key,
// by admitting through a throwaway fake client and reading back the
// created object's name. Foreign-run placement stays in lockstep with the
// adapter's naming even if the digest convention changes.
func derivedRunName(t *testing.T, key string) string {
	t.Helper()
	c := newTestClient(t)
	opts := validOptions()
	opts.key = key
	code, stdout, stderr := runOptions(t, opts, c)
	if code != exitOK {
		t.Fatalf("derive run name for %q: exit %d, stderr %q", key, code, stderr)
	}
	m := runLineRe.FindStringSubmatch(stdout)
	if m == nil {
		t.Fatalf("derive run name for %q: stdout %q carries no run line", key, stdout)
	}
	return m[2]
}

// TestContributeHelpGoesToStdout proves -h prints the contribute usage
// (with its AUTHORITY section) to stdout and exits cleanly, per the
// settled interface.
func TestContributeHelpGoesToStdout(t *testing.T) {
	built := false
	newClient := func(string) (client.Client, error) {
		built = true
		return newTestClient(t), nil
	}
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	code := runContributeArgs(context.Background(), []string{"-h"}, newClient, stdout, stderr)
	if code != exitOK {
		t.Fatalf("exit code = %d; want %d", code, exitOK)
	}
	out := stdout.String()
	if !strings.Contains(out, "AUTHORITY") {
		t.Errorf("help text %q lacks the AUTHORITY section", out)
	}
	if !strings.Contains(out, "--fresh") {
		t.Errorf("help text %q does not document --fresh", out)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q; help must not write to stderr", stderr.String())
	}
	if built {
		t.Error("client was built for a help request")
	}
}

// TestContributeUnknownFlagIsUsage proves an unrecognized flag is a
// usage error, not a cluster round trip.
func TestContributeUnknownFlagIsUsage(t *testing.T) {
	built := false
	newClient := func(string) (client.Client, error) {
		built = true
		return newTestClient(t), nil
	}
	stderr := &strings.Builder{}
	code := runContributeArgs(context.Background(), []string{"--nope"}, newClient, io.Discard, stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d; want %d", code, exitUsage)
	}
	if built {
		t.Error("client was built for a usage rejection")
	}
}

// TestRunDispatchRejectsUnknown pins the top-level dispatch contract:
// an unknown or absent subcommand is a usage error with usage on stderr,
// while help exits cleanly on stdout.
func TestRunDispatchRejectsUnknown(t *testing.T) {
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	if code := run(context.Background(), []string{"frobnicate"}, stdout, stderr); code != exitUsage {
		t.Fatalf("unknown subcommand exit code = %d; want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "unknown subcommand") || !strings.Contains(stderr.String(), "contribute") {
		t.Errorf("stderr %q; want the unknown-subcommand note plus usage", stderr.String())
	}
	if code := run(context.Background(), nil, stdout, stderr); code != exitUsage {
		t.Fatalf("no-subcommand exit code = %d; want %d", code, exitUsage)
	}
	if code := run(context.Background(), []string{"help"}, stdout, stderr); code != exitOK {
		t.Fatalf("help exit code = %d; want %d", code, exitOK)
	}
	if !strings.Contains(stdout.String(), "contribute") {
		t.Errorf("usage %q does not name the contribute subcommand", stdout.String())
	}
}

// TestContributeSchemeRegistersCourier proves the CLI's scheme agrees
// with the manager's: the Courier API types decode through it.
func TestContributeSchemeRegistersCourier(t *testing.T) {
	scheme := contributeScheme()
	if _, err := scheme.New(courierv1alpha1.GroupVersion.WithKind("CoderRun")); err != nil {
		t.Errorf("scheme cannot build a CoderRun: %v", err)
	}
}

// TestMintFreshKeyIsShapedAndDistinct pins the fresh key mint: it matches
// the adapter's key shape, and successive mints never collide (the whole
// reason --fresh exists).
func TestMintFreshKeyIsShapedAndDistinct(t *testing.T) {
	seen := make(map[string]struct{})
	for range 64 {
		key, err := mintFreshKey()
		if err != nil {
			t.Fatalf("mintFreshKey: %v", err)
		}
		if !freshKeyRe.MatchString(key) {
			t.Errorf("key %q; want the adhoc-cli-<16 hex> shape", key)
		}
		if _, dup := seen[key]; dup {
			t.Fatalf("key %q minted twice", key)
		}
		seen[key] = struct{}{}
	}
}
