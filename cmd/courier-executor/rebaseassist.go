package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/misospace/courier/internal/harness"
	"github.com/misospace/courier/internal/rebase"
)

// The model-assisted rebase adapter (#732): the executor-side implementation
// of rebase.ModelResolver over the harness gateway. It is deliberately thin
// transport: the trusted engine owns the prompt and the validation, and this
// file only carries the request to the configured coordinator model, collects
// the streamed answer, and parses it into the resolution shape the engine
// validates. The gateway API key lives here and in the HTTP header alone —
// it never enters a prompt, a log line, or a resolution.

const (
	// rebaseAssistMaxResponseBytes bounds the assembled model response. It
	// is a transport resource bound, not a work limit: a response larger
	// than this is a broken or hostile turn, and the engine's own content
	// bound (rebase.maxConflictFileBytes) still governs what may be written.
	rebaseAssistMaxResponseBytes = 4 << 20
	// rebaseAssistMaxOutputTokens is the output ceiling one resolution turn
	// reserves: a full-file resolution answer for one conflict round.
	rebaseAssistMaxOutputTokens = 8192
)

// gatewayResolver implements rebase.ModelResolver over the harness Gateway.
// Model is the configured coordinator model: the rebase assist borrows the
// run's model binding rather than introducing a second one to configure and
// route.
type gatewayResolver struct {
	gateway *harness.Gateway
	model   string
}

// rebaseAssistEnabled parses COURIER_REBASE_MODEL_ASSIST: enabled only by an
// explicit affirmative, so an unset or unrecognized value is off.
func rebaseAssistEnabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "enabled":
		return true
	}
	return false
}

// newRebaseAssistResolver builds the resolver when model-assisted conflict
// resolution is enabled, nil when it is not. Enabled without a gateway URL
// is a configuration error, never a silent fallback: a deployment that asked
// for model assistance and did not configure its transport must fail closed
// instead of quietly running mechanical-only and publishing differently than
// it was configured to.
func newRebaseAssistResolver(cfg config) (rebase.ModelResolver, error) {
	if !rebaseAssistEnabled(cfg.RebaseAssist) {
		return nil, nil
	}
	if strings.TrimSpace(cfg.ModelGatewayURL) == "" {
		return nil, errors.New("COURIER_REBASE_MODEL_ASSIST is enabled but COURIER_MODEL_GATEWAY_URL is not set")
	}
	return &gatewayResolver{
		gateway: &harness.Gateway{BaseURL: cfg.ModelGatewayURL, APIKey: cfg.ModelGatewayAPIKey},
		model:   cfg.Model,
	}, nil
}

// Resolve sends one chat request for the engine's resolution turn and maps
// the streamed answer into an AssistResolution. One call, one turn, no
// retry: the engine owns retry policy by re-asking in a later round, and a
// transport that retried here would spend unbounded model turns outside the
// engine's accounting.
func (g *gatewayResolver) Resolve(ctx context.Context, req rebase.AssistRequest) (*rebase.AssistResolution, error) {
	assistCtx, cancel := context.WithTimeout(ctx, rebase.DefaultAssistTimeout)
	defer cancel()
	events, err := g.gateway.StreamChat(assistCtx, harness.ChatRequest{
		Model:     g.model,
		Messages:  []harness.Message{{Role: "user", Content: req.Prompt()}},
		MaxTokens: rebaseAssistMaxOutputTokens,
	})
	if err != nil {
		return nil, err
	}
	var response strings.Builder
	var usage *harness.Usage
	overCap := false
	for _, event := range events {
		switch event.Kind {
		case harness.KindDelta:
			if response.Len()+len(event.Text) > rebaseAssistMaxResponseBytes {
				// The cap stops the assembly, not the drain: the stream's
				// terminal usage still rides back, so a turn the bound cut
				// short is reported with the spend it actually cost.
				overCap = true
				continue
			}
			response.WriteString(event.Text)
		case harness.KindFinal:
			usage = event.Usage
		case harness.KindError:
			// The gateway's error is already redacted and classified; it is
			// the resolver-unavailable verdict the engine classifies as a
			// failed attempt, never a semantic one.
			return nil, event.Err
		}
	}
	if overCap {
		resolution := &rebase.AssistResolution{
			Refusal: fmt.Sprintf("the model response exceeded the %d-byte response bound", rebaseAssistMaxResponseBytes),
			Model:   g.model,
		}
		if usage != nil {
			resolution.InputTokens = usage.InputTokens
			resolution.OutputTokens = usage.OutputTokens
		}
		return resolution, nil
	}
	resolution := parseAssistResolution(response.String())
	resolution.Model = g.model
	if usage != nil {
		resolution.InputTokens = usage.InputTokens
		resolution.OutputTokens = usage.OutputTokens
	}
	return resolution, nil
}

// assistWire is the JSON shape the prompt instructs the model to answer
// with. Both fields are optional by decode; the engine's validation decides
// what an answer that names neither is worth.
type assistWire struct {
	Files []struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	} `json:"files"`
	Refusal string `json:"refusal"`
}

// parseAssistResolution extracts the last JSON object from the model's
// response and maps it to a resolution. Models routinely frame JSON in
// prose, so the scan tolerates surrounding text: it walks the response once
// tracking string literals and brace depth, keeps the last complete
// top-level object, and treats a response that holds none — or one that
// will not decode, or one past the response bound — as the model failing to
// answer in the agreed shape. That is a decision failure, not a down
// gateway, so it maps to a refusal the engine classifies as the model
// declining: a genuine transport error returns from Resolve as an error.
func parseAssistResolution(response string) *rebase.AssistResolution {
	if len(response) > rebaseAssistMaxResponseBytes {
		return &rebase.AssistResolution{Refusal: fmt.Sprintf("the model response exceeded the %d-byte response bound", rebaseAssistMaxResponseBytes)}
	}
	span, ok := lastJSONObject(response)
	if !ok {
		return &rebase.AssistResolution{Refusal: "the model response contained no JSON object"}
	}
	var wire assistWire
	if err := json.Unmarshal([]byte(response[span.start:span.end]), &wire); err != nil {
		return &rebase.AssistResolution{Refusal: "the model's JSON answer was undecodable"}
	}
	if strings.TrimSpace(wire.Refusal) != "" {
		// An honest decline stands even beside file content: the engine
		// treats a refusal as the whole answer.
		return &rebase.AssistResolution{Refusal: wire.Refusal}
	}
	files := make(map[string]string, len(wire.Files))
	for _, file := range wire.Files {
		files[file.Path] = file.Content
	}
	return &rebase.AssistResolution{Files: files}
}

type jsonSpan struct {
	start, end int
}

// lastJSONObject returns the byte span of the last complete top-level JSON
// object in s. The scan is bounded by the caller's response bound and
// string-aware, so braces inside JSON string literals never confuse depth.
func lastJSONObject(s string) (jsonSpan, bool) {
	var last jsonSpan
	found := false
	depth, start := 0, -1
	inString, escaped := false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inString:
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{':
			if depth == 0 {
				start = i
			}
			depth++
		case c == '}':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					last, found = jsonSpan{start: start, end: i + 1}, true
					start = -1
				}
			}
		}
	}
	return last, found
}
