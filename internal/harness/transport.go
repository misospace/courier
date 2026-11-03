package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ModelTransport is the harness's model boundary (HARNESS.md §5, #716): one
// normalized streaming turn plus the metadata read the capability contract
// needs. Implementations own their provider wire format — request shape,
// streaming events, tool blocks, thinking blocks, stop reasons, and error
// shapes all stay below this interface — and normalize into the fixed event
// vocabulary everything above it already speaks. Nothing above the transport
// may parse, log, or render provider wire data.
type ModelTransport interface {
	// StreamChat performs one streaming model turn and returns the
	// normalized event sequence, with the same ordering and terminal
	// guarantees every transport shares: deltas first, then assembled tool
	// requests, then exactly one terminal event (final, tool requests, or
	// error). A failed turn is a KindError, never a partial success.
	StreamChat(ctx context.Context, req ChatRequest) ([]Event, error)
	// ModelInputLimit returns the input limit the transport can verify for
	// one bound model. A transport whose provider exposes no equivalent
	// metadata returns an explicit error — the role's declared ceiling then
	// stays `configured`, never healthy, and is never silently invented.
	ModelInputLimit(ctx context.Context, model string) (int, error)
}

// Model wire transports, named by the lane's declared transport (#716). The
// lane names one explicitly; the values are the whole set, and anything else
// fails closed at transport construction.
const (
	// WireOpenAI is the LiteLLM/OpenAI-compatible transport — the original
	// harness transport and the default for lanes that name none, preserved
	// for old invocations and profiles.
	WireOpenAI = "openai"
	// WireAnthropic is the native Anthropic Messages transport: the Messages
	// API wire format, streaming content blocks, native tool-use and
	// thinking blocks, and Anthropic stop reasons, none of it routed through
	// OpenAI translation.
	WireAnthropic = "anthropic"
)

// ParseWire validates one lane-declared transport name. An empty value is
// the default OpenAI-compatible transport; an unknown value is an error, so
// a mistyped lane fails closed at transport construction instead of falling
// back to a wire format the model may not speak.
func ParseWire(declared string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(declared)) {
	case "":
		return WireOpenAI, nil
	case WireOpenAI:
		return WireOpenAI, nil
	case WireAnthropic:
		return WireAnthropic, nil
	default:
		return "", fmt.Errorf("harness: unknown model transport %q (want %q or %q)", declared, WireOpenAI, WireAnthropic)
	}
}

// NewModelTransport constructs the transport one lane declared. The endpoint
// and key are deployment configuration shared by every transport — the same
// gateway serves the OpenAI-compatible and Anthropic Messages surfaces — and
// an unknown declared name is an error, never a fallback.
func NewModelTransport(declared, baseURL, apiKey string, client *http.Client, signals ExhaustionSignals) (ModelTransport, error) {
	wire, err := ParseWire(declared)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("harness: model gateway URL is not configured")
	}
	switch wire {
	case WireAnthropic:
		return &AnthropicTransport{
			BaseURL:           baseURL,
			APIKey:            apiKey,
			HTTP:              client,
			ExhaustionSignals: signals,
		}, nil
	default:
		return &Gateway{
			BaseURL:           baseURL,
			APIKey:            apiKey,
			HTTP:              client,
			ExhaustionSignals: signals,
		}, nil
	}
}
