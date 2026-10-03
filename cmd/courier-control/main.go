// Command courier-control is the trusted control pod's entrypoint. In the
// #123 build it bootstraps the control identity, probes the semantic
// capabilities the harness depends on (the worker protocol, the broker
// identity chain), and declares the run NeedsHuman with an actionable reason:
// the native model client — planning, delegation, integration, publication —
// is the #124 build and is deliberately absent here. There is no silent
// fallback to the legacy OpenCode shim.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/misospace/courier/internal/protocol"
	"github.com/misospace/courier/internal/topology"
)

// termination is the operator's exit-contract handoff (the same shape the
// legacy executor writes to its termination message path).
type termination struct {
	Phase    string `json:"phase"`
	Result   string `json:"result"`
	ExitCode int32  `json:"exit_code"`
	Reason   string `json:"reason"`
}

type capability struct {
	Name   string `json:"name"`
	State  string `json:"state"` // configured | healthy | unavailable
	Detail string `json:"detail,omitempty"`
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
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	caps := probeCapabilities(ctx, identity)
	caps = append(caps, capability{
		Name:   "model-bindings",
		State:  "unavailable",
		Detail: "the native model harness is not part of this build; secure runs require the #124 coordinator",
	})

	// The honest terminal declaration: without the model client the run
	// cannot proceed, and failing closed is the contract. A human sees the
	// capability table in the termination reason.
	reason := "secure topology is healthy but the native model harness is not implemented in this build"
	if detail := unavailableDetails(caps); detail != "" {
		reason = detail
	}
	return declareNeedsHuman(reason, caps)
}

type controlIdentity struct {
	runUID          string
	controlUID      string
	workerUID       string
	workerURL       string
	brokerURL       string
	brokerStatusURL string
	signingKey      ed25519.PrivateKey
	brokerToken     []byte
	brokerCA        []byte
}

func loadIdentity() (*controlIdentity, error) {
	out := &controlIdentity{
		runUID:          os.Getenv(topology.EnvRunUID),
		controlUID:      os.Getenv(topology.EnvControlPodUID),
		workerUID:       os.Getenv(topology.EnvWorkerPodUID),
		workerURL:       os.Getenv(topology.EnvWorkerURL),
		brokerURL:       os.Getenv(topology.EnvBrokerURL),
		brokerStatusURL: os.Getenv(topology.EnvBrokerStatusURL),
	}
	if out.runUID == "" || out.controlUID == "" || out.workerUID == "" || out.workerURL == "" || out.brokerURL == "" {
		return nil, errors.New("courier-control: identity environment is incomplete")
	}
	key, err := os.ReadFile(os.Getenv(topology.EnvSigningKeyFile))
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("courier-control: signing key is missing or unusable")
	}
	out.signingKey = ed25519.PrivateKey(key)
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

func probeCapabilities(ctx context.Context, identity *controlIdentity) []capability {
	caps := make([]capability, 0, 2)
	caps = append(caps, probeWorker(ctx, identity))
	caps = append(caps, probeBroker(ctx, identity))
	return caps
}

// probeWorker exercises the signed worker protocol end to end: snapshot
// upload, dispatch, and a verified result. The probe task is inert.
func probeWorker(ctx context.Context, identity *controlIdentity) capability {
	client := &protocol.Client{BaseURL: identity.workerURL}
	if err := client.UploadSnapshot(ctx, []byte("courier-control capability probe")); err != nil {
		return capability{Name: "worker-protocol", State: "unavailable", Detail: "snapshot upload failed"}
	}
	task, err := json.Marshal(protocol.Task{Command: []string{"git", "--version"}})
	if err != nil {
		return capability{Name: "worker-protocol", State: "unavailable", Detail: "task encoding failed"}
	}
	env := protocol.NewEnvelope(time.Now(), 5*time.Minute, task)
	env.Kind = protocol.KindDispatch
	env.RunUID = identity.runUID
	env.ControlPodUID = identity.controlUID
	env.WorkerPodUID = identity.workerUID
	env.BriefID = "capability-probe"
	env.OpID = "capability-probe"
	if _, err := client.Dispatch(ctx, identity.signingKey, env, task); err != nil {
		return capability{Name: "worker-protocol", State: "unavailable", Detail: "dispatch failed"}
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		state, err := client.Result(ctx, env.OpID)
		if err != nil {
			return capability{Name: "worker-protocol", State: "unavailable", Detail: "result poll failed"}
		}
		if state.Status == protocol.ResultCompleted {
			return capability{Name: "worker-protocol", State: "healthy"}
		}
		if state.Status == protocol.ResultFailed || state.Status == protocol.ResultCancelled {
			return capability{Name: "worker-protocol", State: "unavailable", Detail: "probe task did not complete"}
		}
		if time.Now().After(deadline) {
			return capability{Name: "worker-protocol", State: "unavailable", Detail: "probe task timed out"}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// probeBroker verifies the broker identity chain: a TLS connection anchored
// in the run's CA plus a TokenReview-authenticated call to the trusted status
// path. A 400 (schema rejection) proves authentication passed; a 401 means
// the identity chain failed.
func probeBroker(ctx context.Context, identity *controlIdentity) capability {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(identity.brokerCA) {
		return capability{Name: "broker-identity", State: "unavailable", Detail: "broker CA is unreadable"}
	}
	host := hostOnly(identity.brokerStatusURL)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS13}}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, identity.brokerStatusURL+"/trusted/v1/status", nil)
	if err != nil {
		return capability{Name: "broker-identity", State: "unavailable", Detail: "request build failed"}
	}
	req.Header.Set("Authorization", "Bearer "+string(identity.brokerToken))
	resp, err := client.Do(req)
	if err != nil {
		return capability{Name: "broker-identity", State: "unavailable", Detail: "broker unreachable"}
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		// Authenticated, then rejected on schema — the identity chain works.
		return capability{Name: "broker-identity", State: "healthy"}
	default:
		return capability{Name: "broker-identity", State: "unavailable",
			Detail: fmt.Sprintf("broker rejected the authenticated probe with status %d", resp.StatusCode)}
	}
}

func unavailableDetails(caps []capability) string {
	for _, c := range caps {
		if c.Name == "model-bindings" {
			continue
		}
		if c.State != "healthy" {
			return fmt.Sprintf("capability %q is %s: %s", c.Name, c.State, c.Detail)
		}
	}
	return ""
}

func hostOnly(rawURL string) string {
	for i := 0; i < len(rawURL); i++ {
		if rawURL[i] == '/' {
			return rawURL[:i]
		}
	}
	return rawURL
}

func declareNeedsHuman(reason string, caps []capability) error {
	payload, err := json.Marshal(termination{
		Phase:    "NeedsHuman",
		Result:   "needs-human",
		ExitCode: 2,
		Reason:   reason,
	})
	if err != nil {
		return err
	}
	// The operator's contract: the structured handoff on stdout and in the
	// termination message file, exit code 2.
	fmt.Printf("COURIER_TERMINATION %s\n", payload)
	if path := os.Getenv("COURIER_TERMINATION_FILE"); path != "" {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			return fmt.Errorf("courier-control: write termination file: %w", err)
		}
	}
	table, _ := json.Marshal(caps)
	fmt.Printf("capability health: %s\n", table)
	os.Exit(2)
	return nil
}
