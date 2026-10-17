package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
	"github.com/misospace/courier/internal/executor"
	"github.com/misospace/courier/internal/git"
	"github.com/misospace/courier/internal/harness"
)

type exhaustionSignals = harness.ExhaustionSignals

func defaultExhaustionSignals() exhaustionSignals { return harness.DefaultExhaustionSignals() }
func parseExhaustionSignals(codes, types string) exhaustionSignals {
	return harness.ParseExhaustionSignals(codes, types)
}

// OpenCode v1.18.31 run.ts emits serialized NamedError objects. Only APIError
// responseBody is provider evidence; data.message, text parts and stderr are not.
func classifyProviderExhaustion(line []byte, signals exhaustionSignals) (bool, string, string) {
	if len(line) > tapMaxLine {
		return false, "", ""
	}
	var event struct {
		Type      string `json:"type"`
		SessionID string `json:"sessionID"`
		Error     struct {
			Name string `json:"name"`
			Data struct {
				StatusCode   int    `json:"statusCode"`
				ResponseBody string `json:"responseBody"`
			} `json:"data"`
		} `json:"error"`
	}
	if json.Unmarshal(line, &event) != nil || event.Type != "error" || event.SessionID == "" || event.Error.Name != "APIError" {
		return false, "", ""
	}
	data := event.Error.Data
	if len(data.ResponseBody) == 0 || len(data.ResponseBody) > 64<<10 {
		return false, "", ""
	}
	classified := harness.ClassifyProviderError(data.StatusCode, nil, []byte(data.ResponseBody), signals)
	// A quota verdict must be carried by the body's structured signal, never
	// by status code alone: the harness treats bare 402 as allowance-shaped,
	// but the stop path kills a working child and ends the run as exhausted,
	// which #327 reserves for authoritative evidence.
	if classified.Availability == harness.ProviderAllowanceExhausted && classified.Code != "" {
		return true, string(classified.Availability), classified.Code
	}
	// Some Anthropic-compatible responses expose their numeric plan code only as
	// a suffix of error.message. Require the typed provider envelope, HTTP 429,
	// and an explicit deployment-configured code; never read the runtime summary.
	if data.StatusCode != 429 {
		return false, "", ""
	}
	var body struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(data.ResponseBody), &body) != nil || body.Error.Type != "rate_limit_error" {
		return false, "", ""
	}
	message := strings.TrimSpace(body.Error.Message)
	if len(message) > 4096 || !strings.HasSuffix(message, ")") {
		return false, "", ""
	}
	start := strings.LastIndexByte(message, '(')
	if start < 0 {
		return false, "", ""
	}
	code := message[start+1 : len(message)-1]
	if len(code) == 0 || len(code) > 9 {
		return false, "", ""
	}
	for _, c := range code {
		if c < '0' || c > '9' {
			return false, "", ""
		}
	}
	for _, configured := range signals.Codes {
		if code == configured {
			return true, string(harness.ProviderAllowanceExhausted), code
		}
	}
	return false, "", ""
}

// capacityExhaustionReason renders the bounded, allowlisted exhaustion
// explanation. class and code are fixed vocabulary tokens extracted from
// structured evidence; provider response text never reaches the reason.
func capacityExhaustionReason(class, code string) string {
	reason := "model provider capacity confirmed exhausted (" + class + ")"
	if code != "" {
		reason += ", provider code " + code
	}
	return reason + "; stopping without further provider requests"
}

// capacityExhaustedTermination builds the recoverable capacity-handoff
// ending (#320/#327): exit 2 keeps the handoff vocabulary valid, the
// outcome routes the operator to the fenced capacity-recovery path, the
// category is the durable diagnostic, and the workspace snapshot is the
// executor's own git-verified evidence.
func (r reporter) capacityExhaustedTermination(ctx context.Context, workspace *git.Workspace, cfg config, startCommit, class, code string) termination {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return termination{
		Phase:           "NeedsHuman",
		Result:          "needs-human",
		ExitCode:        exitNeedsHuman,
		Reason:          capacityExhaustionReason(class, code),
		Outcome:         string(executor.OutcomeCapacityExhausted),
		Category:        terminalCategoryCapacityExhaustion,
		WorkspaceSafety: capacityWorkspaceSafety(ctx, workspace, cfg.RemoteURL, cfg.Branch, startCommit),
	}
}

// availabilityRecoveryTermination builds the recoverable availability-handoff
// ending (#327 Scenario D): the provider or the child kept failing without
// ever producing a structured allowance-exhaustion signal, so the run ends on
// the same fenced capacity-recovery channel — exit 2, the capacity-handoff
// outcome — but with the honest provider-unavailable category, never claiming
// a quota verdict the evidence does not support. `detail` carries why this
// attempt stream ended (outage retries or a spent crash budget).
func (r reporter) availabilityRecoveryTermination(ctx context.Context, workspace *git.Workspace, cfg config, startCommit string, detail string) termination {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return termination{
		Phase:           "NeedsHuman",
		Result:          "needs-human",
		ExitCode:        exitNeedsHuman,
		Reason:          fmt.Sprintf("%s; releasing work for another capacity without a structured exhaustion signal", detail),
		Outcome:         string(executor.OutcomeCapacityExhausted),
		Category:        terminalCategoryProviderUnavailable,
		WorkspaceSafety: capacityWorkspaceSafety(ctx, workspace, cfg.RemoteURL, cfg.Branch, startCommit),
	}
}

// capacityWorkspaceSafety computes the executor-owned workspace snapshot at
// the exhaustion stop, using the same bounded vocabulary as the native
// trusted observer (PublicationEngine.CapacityWorkspaceSafety): uncommitted
// or untracked changes → dirty; no work beyond the start commit on the run
// branch → empty; committed work the remote run branch holds → pushed;
// anything unverifiable or ambiguous → unknown. Only the executor's own
// git reads feed it — no model-authored text decides a state — and the
// operator must independently validate evidence before ownership moves (#327).
func capacityWorkspaceSafety(ctx context.Context, workspace *git.Workspace, remoteURL, branch, startCommit string) *courierv1alpha1.WorkspaceSafetyEvidence {
	unknown := func() *courierv1alpha1.WorkspaceSafetyEvidence {
		return &courierv1alpha1.WorkspaceSafetyEvidence{State: "unknown"}
	}
	head, err := workspace.Head(ctx)
	if err != nil {
		return unknown()
	}
	current, err := workspace.CurrentBranch(ctx)
	if err != nil {
		return unknown()
	}
	dirty, err := workspace.StatusPorcelain(ctx)
	if err != nil {
		return unknown()
	}
	if len(dirty) > 0 {
		return &courierv1alpha1.WorkspaceSafetyEvidence{State: "dirty", Branch: current, Head: head}
	}
	state, err := workspace.WorkState(ctx, startCommit)
	if err != nil {
		return unknown()
	}
	if current != branch {
		// A workspace off the run branch is ambiguous even when it holds no
		// work: the snapshot describes a place the recovery path does not
		// know, so it proves nothing.
		return unknown()
	}
	switch state {
	case git.WorkStateDirty:
		return &courierv1alpha1.WorkspaceSafetyEvidence{State: "dirty", Branch: current, Head: head}
	case git.WorkStateNone:
		if head != startCommit {
			return unknown()
		}
		// No commits beyond the start and a clean worktree: nothing exists
		// to strand, so the branch/head identity is safe by absence.
		return &courierv1alpha1.WorkspaceSafetyEvidence{State: "empty", Branch: current, Head: head}
	default:
		// Committed work: only a successful fetch-and-compare proves the
		// remote run branch holds the local head.
		if err := fetchRunBranch(ctx, workspace.Directory, remoteURL, branch); err != nil {
			return unknown()
		}
		remoteHead, err := revParse(ctx, workspace.Directory, "FETCH_HEAD")
		if err != nil || remoteHead == "" {
			return unknown()
		}
		if remoteHead != head {
			return unknown()
		}
		return &courierv1alpha1.WorkspaceSafetyEvidence{State: "pushed", Branch: current, Head: head, RemoteHead: remoteHead}
	}
}
