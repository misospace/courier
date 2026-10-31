// Command courierctl is the thin admission frontend for external
// contributions (#601, docs/external-contributions.md, "Admission
// surface"). It is a dumb frontend by construction: it validates inputs
// only for ergonomics, forwards every decision to the server-side
// admission path through the ad-hoc source adapter, and makes no forge
// calls of its own. The contribute subcommand is its first and only
// surface; HTTP, web, and Dispatch frontends follow the same contract
// later and must consume it, not reimplement it.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches one courierctl invocation over the given arguments and
// returns the process exit code instead of exiting itself, so tests can
// exercise the whole dispatch surface in process. No or unknown
// subcommands print usage and fail; there is no default action, because
// an admission frontend that guessed at intent would be unsafe.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "contribute":
		return runContributeArgs(ctx, args[1:], newContributeClient, stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "courierctl: unknown subcommand %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
}

// usage is the top-level courierctl help text. It stays deliberately
// short and points at the subcommand help, which carries the authority
// model the issue's done-when requires.
const usage = `courierctl is the thin admission frontend for Courier ad-hoc contributions
(#601; docs/external-contributions.md, "Admission surface"). It creates or
resumes contribute-mode CoderRuns through the Kubernetes API and performs
no authority validation of its own; all validation is server-side.

Usage:

  courierctl <subcommand> [flags]

Subcommands:

  contribute   Create or resume an ad-hoc contribution.
  help         Print this usage.

Run "courierctl contribute -h" for the contribute flags and the AUTHORITY
section that documents what the CLI does and does not decide.
`
