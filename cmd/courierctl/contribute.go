// The contribute subcommand is the settled V1 admission surface for
// ad-hoc contributions (#601, docs/external-contributions.md, "Admission
// surface"). It is intentionally dumb: every authority decision — which
// base repositories are approved, which forks may receive pushes, which
// lanes exist, what publication looks like — lives server-side in the
// controller admission path and the publication policy. This file owns
// only ergonomics: flag parsing, help text, error mapping to exit codes,
// and forwarding the admission to internal/source/adhoc.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/source/adhoc"
)

// Exit codes for the courierctl subcommands: 0 admits a run (created or
// resumed), 1 reports a cluster, kube-config, or foreign-run failure, and
// 2 reports a usage or validation rejection the caller can fix without
// touching the cluster (#601). The codes are part of the documented
// contract in contributeUsage, so scripts can branch on them safely.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// contributeUsage is the full help text for the contribute subcommand.
// The AUTHORITY section is part of the issue's done-when: a contributor
// reading the help must learn, without reading any source, that the CLI
// decides nothing and that goal or issue text can never redirect the
// approved fork, base, or publication destination. Exit codes are
// documented here rather than left to discovery, so scripts can branch
// on them safely.
const contributeUsage = `contribute creates (or resumes) an ad-hoc contribution as a contribute-mode
CoderRun through the Kubernetes API (#601; docs/external-contributions.md,
"Admission surface"). It is a thin frontend: it performs no authority
validation of its own and makes no forge calls.

AUTHORITY

  courierctl performs no authority validation. The approved base
  repository (--base-repo), the pinned base ref (--base-ref), the
  approved fork (--head-repo), and the lane (--lane) are validated
  server-side in the controller admission path (allowlist and registry)
  and validated again at publication. --goal and --upstream-issue are
  untrusted data carried as run context only: they cannot change the
  approved fork, the base, or the publication destination. Publication
  semantics — including the non-closing pull-request body wording — are
  set by server-side policy, never by client input. Submitting the same
  contribution with the same idempotency key resumes the existing run;
  an intentional new contribution requires --fresh or an explicit
  distinct --key.

Usage:

  courierctl contribute [flags]

Flags:

  --base-repo string      Required. Upstream repository "owner/name"; the
                          pull-request base. Validated server-side.
  --base-ref string       Required. Pinned base branch name. Validated
                          server-side.
  --goal string           Required. Natural-language objective; untrusted
                          context.
  --head-repo string      Required. Approved fork "owner/name" the run may
                          push to. Validated server-side.
  --lane string           Required. LaneProfile name supplying the model
                          ensemble. Validated server-side.
  --upstream-issue string Optional. Issue URL or number, carried as
                          untrusted context; not an ownership claim.
  --key string            Optional. Caller idempotency key; the same key
                          resumes the existing run instead of creating a
                          second one.
  --fresh                 Optional. Mint a fresh unique idempotency key
                          for an intentional new contribution, even when
                          the content is identical to a previous
                          submission. Cannot be combined with --key.
  --namespace string      Target namespace (default "courier").
  --debug                 Raise this run's pod log level only.
  --kubeconfig string     Explicit kubeconfig path; otherwise the standard
                          lookup applies (KUBECONFIG, default locations,
                          in-cluster).

Exit codes:

  0  the CoderRun was created or an existing one was resumed
  1  cluster, kube-config, or admission failure (for example a foreign run
     already owns the derived name, or no cluster is reachable)
  2  usage or validation rejection: missing or conflicting flags, or an
     admission shape the server rejected before any run was touched
`

// contributeOptions is the parsed flag state of one contribute
// invocation. Field names mirror the flags; the mapping onto
// adhoc.Admission happens in runContribute so parsing stays free of
// adapter knowledge.
type contributeOptions struct {
	baseRepo      string
	baseRef       string
	goal          string
	headRepo      string
	lane          string
	upstreamIssue string
	key           string
	fresh         bool
	namespace     string
	debug         bool
	kubeconfig    string
}

// newContributeClient builds the Kubernetes client one admission goes
// through: an explicit --kubeconfig path wins, and otherwise the standard
// controller-runtime lookup applies (KUBECONFIG, default locations,
// in-cluster). The scheme is the same one the manager registers, so the
// CLI and the operator agree on the CoderRun shape. The adapter is handed
// this client as its direct reader too: a CLI holds no cache, so a
// cached-versus-API distinction would be meaningless, and the adapter
// falls back to the write client when no separate reader is given.
func newContributeClient(kubeconfig string) (client.Client, error) {
	var config *rest.Config
	var err error
	if path := strings.TrimSpace(kubeconfig); path != "" {
		config, err = clientcmd.BuildConfigFromFlags("", path)
	} else {
		config, err = ctrl.GetConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("load kube config: %w", err)
	}
	return client.New(config, client.Options{Scheme: contributeScheme()})
}

// contributeScheme builds the scheme the CLI speaks: client-go's types
// plus Courier's, mirroring the manager's registration in cmd/main.go so
// a type the operator accepts is never invisible to the frontend.
func contributeScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(courierv1alpha1.AddToScheme(scheme))
	return scheme
}

// runContributeArgs parses the contribute flags and runs one admission.
// The client constructor is injected so tests can prove that a usage
// rejection never reaches the cluster: a missing or conflicting flag
// fails before newClient is called. The exit-code contract is the one
// documented in contributeUsage.
func runContributeArgs(ctx context.Context, args []string, newClient func(kubeconfig string) (client.Client, error), stdout, stderr io.Writer) int {
	var opts contributeOptions
	fs := flag.NewFlagSet("courierctl contribute", flag.ContinueOnError)
	fs.SetOutput(stderr)
	// Usage is silenced here and printed explicitly below: flag would
	// otherwise write the usage to stderr even for -h, which should print
	// to stdout and exit cleanly.
	fs.Usage = func() {}
	fs.StringVar(&opts.baseRepo, "base-repo", "", "upstream repository owner/name (required)")
	fs.StringVar(&opts.baseRef, "base-ref", "", "pinned base branch name (required)")
	fs.StringVar(&opts.goal, "goal", "", "natural-language objective (required)")
	fs.StringVar(&opts.headRepo, "head-repo", "", "approved fork owner/name (required)")
	fs.StringVar(&opts.lane, "lane", "", "LaneProfile name (required)")
	fs.StringVar(&opts.upstreamIssue, "upstream-issue", "", "optional issue URL or number, untrusted context")
	fs.StringVar(&opts.key, "key", "", "optional caller idempotency key")
	fs.BoolVar(&opts.fresh, "fresh", false, "mint a fresh idempotency key for an intentional new contribution")
	fs.StringVar(&opts.namespace, "namespace", "courier", "target namespace")
	fs.BoolVar(&opts.debug, "debug", false, "raise the run's pod log level only")
	fs.StringVar(&opts.kubeconfig, "kubeconfig", "", "explicit kubeconfig path")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stdout, contributeUsage)
			return exitOK
		}
		// flag already wrote the specific parse error to stderr.
		fmt.Fprint(stderr, "\n"+contributeUsage)
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "courierctl contribute: unexpected argument %q\n\n", fs.Arg(0))
		fmt.Fprint(stderr, contributeUsage)
		return exitUsage
	}
	if opts.fresh && opts.key != "" {
		// The raw key is checked, not a trimmed one: a whitespace-only
		// --key alongside --fresh is the same contradiction, and letting
		// the adapter silently discard the blank key while --fresh wins
		// would hide the caller's mistake.
		fmt.Fprintf(stderr, "courierctl contribute: --fresh cannot be combined with --key; pick one idempotency strategy\n")
		return exitUsage
	}
	// Namespace is trimmed at parse time so what the CLI prints always
	// matches what the adapter validates and persists, which trims its
	// own inputs again.
	opts.namespace = strings.TrimSpace(opts.namespace)
	var missing []string
	if strings.TrimSpace(opts.baseRepo) == "" {
		missing = append(missing, "--base-repo")
	}
	if strings.TrimSpace(opts.baseRef) == "" {
		missing = append(missing, "--base-ref")
	}
	if strings.TrimSpace(opts.goal) == "" {
		missing = append(missing, "--goal")
	}
	if strings.TrimSpace(opts.headRepo) == "" {
		missing = append(missing, "--head-repo")
	}
	if strings.TrimSpace(opts.lane) == "" {
		missing = append(missing, "--lane")
	}
	if len(missing) > 0 {
		fmt.Fprintf(stderr, "courierctl contribute: missing required flags: %s\n", strings.Join(missing, ", "))
		return exitUsage
	}
	kubeClient, err := newClient(opts.kubeconfig)
	if err != nil {
		fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
		return exitError
	}
	return runContribute(ctx, opts, kubeClient, stdout, stderr)
}

// runContribute performs one admission against the given client and maps
// the outcome onto stdout, stderr, and an exit code. The context payload
// for goal and upstream issue is built entirely by the adapter — the CLI
// never writes SourceContext itself — and every error branch leaves the
// cluster untouched unless the adapter already acted.
func runContribute(ctx context.Context, opts contributeOptions, kubeClient client.Client, stdout, stderr io.Writer) int {
	admission := adhoc.Admission{
		Namespace:      opts.namespace,
		Lane:           opts.lane,
		BaseRepo:       opts.baseRepo,
		BaseRef:        opts.baseRef,
		WorkRepo:       opts.headRepo,
		Goal:           opts.goal,
		UpstreamIssue:  opts.upstreamIssue,
		IdempotencyKey: opts.key,
		Debug:          opts.debug,
	}
	if opts.fresh {
		key, err := mintFreshKey()
		if err != nil {
			fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
			return exitError
		}
		admission.IdempotencyKey = key
	}
	admitted, err := adhoc.New(kubeClient, nil).Admit(ctx, admission)
	if err != nil {
		switch {
		case errors.Is(err, adhoc.ErrInvalidAdmission):
			fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
			return exitUsage
		case errors.Is(err, adhoc.ErrForeignRun):
			fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
			return exitError
		case errors.Is(err, adhoc.ErrClientRequired):
			// Unreachable through this frontend (the client is always
			// built), but the mapping stays explicit so a wiring mistake
			// is named rather than rendered as a generic cluster error.
			fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
			return exitError
		default:
			fmt.Fprintf(stderr, "courierctl contribute: %v\n", err)
			return exitError
		}
	}
	if admitted.Resumed {
		fmt.Fprintf(stdout, "resuming existing coder run %s/%s (work item %s)\n", opts.namespace, admitted.RunName, admitted.WorkItemID)
		fmt.Fprintf(stdout, "hint: an intentional new contribution needs --fresh (or an explicit distinct --key)\n")
	} else {
		fmt.Fprintf(stdout, "created coder run %s/%s (work item %s)\n", opts.namespace, admitted.RunName, admitted.WorkItemID)
	}
	return exitOK
}

// mintFreshKey mints the idempotency key --fresh requests: an "adhoc-cli-"
// prefix plus random hex, well within the adapter's key shape and length
// bound. Randomness rather than a content digest is the point: --fresh
// must guarantee a new run even for byte-identical content, which a
// deterministic mint can never do.
func mintFreshKey() (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("mint fresh idempotency key: %w", err)
	}
	return "adhoc-cli-" + hex.EncodeToString(nonce), nil
}
