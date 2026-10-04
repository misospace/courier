// Command courier-control is the trusted control pod's entrypoint: the
// native coordinator harness (HARNESS.md §5). It bootstraps the control
// identity, probes the semantic capabilities the harness depends on, and —
// when the required capabilities are healthy — runs the coordinator loop:
// model sessions normalized in trusted control, delegation to the untrusted
// worker over the signed protocol, and the terminal outcome declaration.
// A capability gate failure declares the run NeedsHuman with the redacted
// capability table. There is no silent fallback to the legacy OpenCode shim.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

// termination mirrors the operator's terminationMessage contract: the
// phase/result pair must satisfy validTerminationPhaseResult for the exit
// code, and the declared outcome travels in Outcome.
type termination struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int32  `json:"exit_code"`
	Reason   string `json:"reason"`
	Outcome  string `json:"outcome,omitempty"`
}

// Probe retry rounds and delay are bounded infrastructure retries covering a
// dependency that is still starting (the broker pod is created before
// control). They are not run-duration limits: persistent failure after the
// rounds declares NeedsHuman with the capability table.
const (
	probeRetryRounds = 5
	probeRetryDelay  = 10 * time.Second
)

// maxTerminationReasonBytes bounds the termination reason. The kubelet caps
// termination message files, and an oversized payload would truncate the
// structured result; the reason carries names, categories, and short
// diagnostics, never full model output.
const maxTerminationReasonBytes = 1200

func boundReason(reason string) string {
	if len(reason) <= maxTerminationReasonBytes {
		return reason
	}
	return reason[:maxTerminationReasonBytes] + "…[truncated]"
}

func main() {
	if err := runArgs(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "courier-control: %v\n", err)
		os.Exit(1)
	}
}

func runArgs(args []string) error {
	flags := flag.NewFlagSet("courier-control", flag.ContinueOnError)
	var probeTimeout time.Duration
	flags.DurationVar(&probeTimeout, "probe-timeout", 2*time.Minute, "per-capability probe bound")
	if err := flags.Parse(args); err != nil {
		return err
	}

	identity, err := loadIdentity()
	if err != nil {
		return err
	}
	invocation, err := executor.InvocationFromEnv(os.Getenv)
	if err != nil {
		return fmt.Errorf("courier-control: run context: %w", err)
	}
	bindings, err := harness.BindRoles(invocation.Roles)
	if err != nil {
		return fmt.Errorf("courier-control: lane roles: %w", err)
	}
	gateway := &harness.Gateway{BaseURL: os.Getenv(topology.EnvGatewayURL)}
	if keyFile := os.Getenv(topology.EnvGatewayKeyFile); keyFile != "" {
		key, err := os.ReadFile(keyFile)
		if err != nil || len(key) == 0 {
			return errors.New("courier-control: projected model gateway key is missing")
		}
		gateway.APIKey = string(key)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Startup capability health (§5): every semantic capability the harness
	// depends on is probed and named. Required and unavailable fails closed
	// before any model work starts; optional capabilities proceed degraded.
	// Transient probe failures (a broker pod still starting, one gateway
	// hiccup) are re-probed for bounded rounds before declaring, so a
	// slow-to-start dependency does not park the run with a human.
	deps := harness.ProbeDeps{
		Gateway:            gateway,
		Bindings:           bindings,
		Broker:             identity.brokerProber(),
		Mode:               string(invocation.Mode),
		PublicationPresent: false, // the publisher lands with #125
		SnapshotPresent:    false, // the snapshot provider lands with #125
		ProbeWorker: func(probeCtx context.Context) harness.Capability {
			return identity.probeWorker(probeCtx)
		},
		ModelProbe: func(probeCtx context.Context, model string) error {
			return probeModel(probeCtx, gateway, model)
		},
	}
	var caps []harness.Capability
	for attempt := 0; ; attempt++ {
		probeCtx, cancelProbe := context.WithTimeout(ctx, probeTimeout)
		caps = harness.ProbeCapabilities(probeCtx, deps)
		cancelProbe()
		_, err := harness.Gate(caps)
		if err == nil {
			break
		}
		var gateErr *harness.GateError
		if !errors.As(err, &gateErr) || !gateErr.AllRetryable() || attempt >= probeRetryRounds {
			return declareNeedsHuman(gateErr.Error(), caps)
		}
		fmt.Printf("transient capability failure, re-probing: %s\n", gateErr)
		select {
		case <-ctx.Done():
			return declareNeedsHuman(gateErr.Error(), caps)
		case <-time.After(probeRetryDelay):
		}
	}
	fmt.Printf("capability health: %s\n", mustJSON(caps))

	// The coordinator loop. It is interrupted by pod termination (context),
	// not by any wall-clock run limit.
	coordinator, err := harness.NewCoordinator(harness.CoordinatorConfig{
		Gateway:  gateway,
		Bindings: bindings,
		Worker:   identity.delegator(),
		Activity: nil, // earned-activity status wiring is #126's seam
	})
	if err != nil {
		return fmt.Errorf("courier-control: %w", err)
	}
	result := coordinator.Run(ctx, invocation)
	return declare(result)
}

// declare maps the harness result onto the operator's exit contract:
// changes exits 0 (Verifying), no_change_needed exits 3 (AwaitingReview),
// needs_decision and blocked_external exit 2 (NeedsHuman), and an
// infrastructure failure or undeclared ending exits 1 so the operator's
// relaunch and crashloop backstop own the recovery. The structured result is
// written to the termination file in every case — the exit code alone is
// never the classification.
func declare(result executor.HarnessResult) error {
	return handoff(terminationFor(result))
}

// terminationFor renders the operator handoff payload. Its phase/result pair
// must satisfy validTerminationPhaseResult for the exit code — the operator
// discards the trusted reason and outcome otherwise — and the coordinator's
// own declared outcome travels in Outcome.
func terminationFor(result executor.HarnessResult) termination {
	reason := result.Reason
	exit := int32(1)
	phase := "Failed"
	handoffResult := "failure"
	outcome := ""
	switch {
	case result.Err != nil && result.Outcome == executor.OutcomeChanges:
		// Declared changes, publication failed: infrastructure failure that
		// a relaunch reconciles against the world.
		outcome = string(result.Outcome)
		reason = result.Err.Error()
	case result.Err != nil:
		reason = result.Err.Error()
	case result.Outcome == executor.OutcomeChanges:
		exit, phase, handoffResult, outcome = 0, "Verifying", "success", string(result.Outcome)
	case result.Outcome == executor.OutcomeNoChangeNeeded:
		exit, phase, handoffResult, outcome = 3, "NoChangeNeeded", "success", string(result.Outcome)
	case result.Outcome == executor.OutcomeNeedsDecision, result.Outcome == executor.OutcomeBlockedExternal:
		exit, phase, handoffResult, outcome = 2, "NeedsHuman", "needs-human", string(result.Outcome)
	}
	return termination{
		Phase:    phase,
		Result:   handoffResult,
		ExitCode: exit,
		Reason:   boundReason(reason),
		Outcome:  outcome,
	}
}

// handoff writes the structured termination payload to stdout and the
// termination file, then exits with the payload's exit code.
func handoff(payload termination) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	fmt.Printf("COURIER_TERMINATION %s\n", data)
	if path := os.Getenv("COURIER_TERMINATION_FILE"); path != "" {
		prefixed := append([]byte("COURIER_TERMINATION "), data...)
		if err := os.WriteFile(path, prefixed, 0o600); err != nil {
			return fmt.Errorf("courier-control: write termination file: %w", err)
		}
	}
	os.Exit(int(payload.ExitCode))
	return nil
}

// declareNeedsHuman writes the structured NeedsHuman handoff with the
// capability table as the reason, then exits 2 — the operator's fail-closed
// contract for a run that cannot proceed.
func declareNeedsHuman(reason string, caps []harness.Capability) error {
	table, _ := json.Marshal(caps)
	fmt.Printf("capability health: %s\n", table)
	return handoff(termination{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: 2,
		Reason:   boundReason(reason),
	})
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

type controlIdentity struct {
	runUID      string
	controlUID  string
	workerUID   string
	workerURL   string
	brokerURL   string
	signingKey  []byte
	brokerToken []byte
	brokerCA    []byte
}

func loadIdentity() (*controlIdentity, error) {
	out := &controlIdentity{
		runUID:     os.Getenv(topology.EnvRunUID),
		controlUID: os.Getenv(topology.EnvControlPodUID),
		workerUID:  os.Getenv(topology.EnvWorkerPodUID),
		workerURL:  os.Getenv(topology.EnvWorkerURL),
		brokerURL:  os.Getenv(topology.EnvBrokerURL),
	}
	if out.runUID == "" || out.controlUID == "" || out.workerUID == "" || out.workerURL == "" || out.brokerURL == "" {
		return nil, errors.New("courier-control: identity environment is incomplete")
	}
	key, err := os.ReadFile(os.Getenv(topology.EnvSigningKeyFile))
	if err != nil || len(key) != 64 {
		return nil, errors.New("courier-control: signing key is missing or unusable")
	}
	out.signingKey = key
	out.brokerToken, err = os.ReadFile(os.Getenv(topology.EnvBrokerTokenFile))
	if err != nil || len(out.brokerToken) == 0 {
		return nil, errors.New("courier-control: projected broker token is missing")
	}
	out.brokerCA, err = os.ReadFile(os.Getenv(topology.EnvBrokerCAFile))
	if err != nil || len(out.brokerCA) == 0 {
		return nil, errors.New("courier-control: broker CA certificate is missing")
	}
	return out, nil
}

// delegator wires the signed worker protocol for the harness.
func (i *controlIdentity) delegator() *harness.Delegator {
	key := make(ed25519.PrivateKey, ed25519.PrivateKeySize)
	copy(key, i.signingKey)
	delegator, err := harness.NewDelegator(&protocol.Client{BaseURL: i.workerURL}, harness.WorkerIdentity{
		RunUID:        i.runUID,
		ControlPodUID: i.controlUID,
		WorkerPodUID:  i.workerUID,
		Key:           key,
	})
	if err != nil {
		// Identity was validated at load; this is unreachable in practice
		// and failing closed here is correct if it ever happens.
		panic("courier-control: worker delegator rejected identity: " + err.Error())
	}
	return delegator
}

// brokerProber reaches the broker's typed capability report over TLS.
func (i *controlIdentity) brokerProber() *harness.BrokerProbeClient {
	return &harness.BrokerProbeClient{BaseURL: i.brokerURL, Token: i.brokerToken, CA: i.brokerCA}
}

// probeWorker exercises the signed worker protocol end to end: snapshot
// upload, dispatch, and a verified result. The probe task is inert.
// probeWorker exercises the signed worker protocol end to end: snapshot
// upload, dispatch, and a verified result. The probe task is inert. Every
// failure is transient-class: the worker pod is fresh infrastructure the
// operator fences on death, so re-probing after a wait can succeed.
func (i *controlIdentity) probeWorker(ctx context.Context) harness.Capability {
	unavailable := func(detail string) harness.Capability {
		return harness.Capability{Name: "worker-protocol", State: harness.StateUnavailable, Detail: detail, Required: true, Retryable: true}
	}
	client := &protocol.Client{BaseURL: i.workerURL}
	if err := client.UploadSnapshot(ctx, []byte("courier-control capability probe")); err != nil {
		return unavailable("snapshot upload failed")
	}
	task, err := json.Marshal(protocol.Task{Command: []string{"git", "--version"}})
	if err != nil {
		return unavailable("task encoding failed")
	}
	env := protocol.NewEnvelope(time.Now(), 5*time.Minute, task)
	env.Kind = protocol.KindDispatch
	env.RunUID = i.runUID
	env.ControlPodUID = i.controlUID
	env.WorkerPodUID = i.workerUID
	env.BriefID = "capability-probe"
	env.OpID = "capability-probe"
	if _, err := client.Dispatch(ctx, ed25519.PrivateKey(i.signingKey), env, task); err != nil {
		return unavailable("dispatch failed")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, err := client.Result(ctx, env.OpID)
		if err != nil {
			return unavailable("result poll failed")
		}
		if state.Status == protocol.ResultCompleted {
			return harness.Capability{Name: "worker-protocol", State: harness.StateHealthy, Required: true}
		}
		if state.Status == protocol.ResultFailed || state.Status == protocol.ResultCancelled {
			return unavailable("probe task did not complete")
		}
		if time.Now().After(deadline) {
			return unavailable("probe task timed out")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// probeModel performs one cheap completion probe for a role binding. The
// diagnostic is a fixed category: provider response bodies are never echoed.
func probeModel(ctx context.Context, gateway *harness.Gateway, model string) error {
	events, err := gateway.StreamChat(ctx, harness.ChatRequest{
		Model:    model,
		Messages: []harness.Message{{Role: "user", Content: "Reply with the single word: ready"}},
	})
	if err != nil {
		return errors.New("model probe request could not be built")
	}
	for _, event := range events {
		if event.Kind == harness.KindError {
			return event.Err
		}
	}
	return nil
}
