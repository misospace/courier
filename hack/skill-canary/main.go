// Command skill-canary proves, by execution, that the native harness loads
// a deployment-provided skill bundle into a run and that the model uses it.
//
// The canary runs the production loading chain — registry registration, lane
// selection, the run-context environment round trip, the control-side load,
// and a native coordinator run — against a bundle the deployment provides,
// and reports one check per claim the chain makes. It exits non-zero when
// any check fails.
//
// By default it writes its own deployment tree into a temporary directory
// and drives it through a hermetic stub gateway, so a run needs no
// credentials and no network. Against a real deployment, point it at the
// deployment's bundle config with --bundle-config, at the per-run bundle
// config the operator rendered with --run-config, and, to exercise a real
// model, at a real gateway with --gateway-url.
//
// A deployment-provided test bundle must carry a CANARY-MARKER-<hex> token
// in its SKILL.md body and at least one supporting asset: the marker is what
// proves the instruction body reached the model.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/misospace/courier/internal/skillcanary"
)

// stringList collects a repeatable string flag.
type stringList []string

// String renders the collected values.
func (l *stringList) String() string { return strings.Join(*l, ",") }

// Set appends one collected value.
func (l *stringList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

func main() {
	opts := skillcanary.Options{}
	var names stringList
	flag.StringVar(&opts.BundleConfig, "bundle-config", "", "path of the deployment's skill bundle config; empty runs the canary's own synthetic deployment tree")
	flag.StringVar(&opts.RunConfig, "run-config", "", "path of the per-run bundle config the operator rendered for a run's pods; empty resolves the deployment's own config")
	flag.Var(&names, "skill", "lane's selected skill bundle name; repeatable; empty selects every bundle the config registers")
	flag.StringVar(&opts.CheckoutDir, "checkout", "", "the run's checkout directory; the canary asserts the target checkout was not modified")
	flag.StringVar(&opts.WorkDir, "work-dir", "", "scratch directory for the canary's synthetic deployment tree; empty uses a temporary directory")
	flag.StringVar(&opts.GatewayURL, "gateway-url", "", "base URL of a real OpenAI-compatible gateway; empty uses the built-in hermetic stub")
	flag.StringVar(&opts.GatewayKeyFile, "gateway-key-file", "", "file holding the gateway's API key")
	flag.DurationVar(&opts.Timeout, "timeout", 0, "bound on one canary run; empty uses the default bound")
	flag.Parse()
	opts.Skills = names

	report, err := skillcanary.Run(context.Background(), opts)
	if report != nil {
		if renderErr := report.Render(os.Stdout); renderErr != nil {
			fmt.Fprintf(os.Stderr, "skill-canary: %v\n", renderErr)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "skill-canary: %v\n", err)
		os.Exit(1)
	}
	if !report.Passed() {
		fmt.Fprintln(os.Stderr, "skill-canary: the canary failed")
		os.Exit(1)
	}
}
