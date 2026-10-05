package broker

import (
	"encoding/json"
	"net/http"
)

// PathCapabilities is the read-only capability report endpoint. It exists so
// trusted control can probe the semantic capabilities the pinned provider
// serves (HARNESS.md §5) without holding any credential itself: the report
// is configuration data, redacted server-side, and carries no credential
// material — references and diagnostics only.
const PathCapabilities = "/v1/capabilities"

// CapabilityReport is the broker's pinned provider capability summary.
type CapabilityReport struct {
	Provider   ProviderReport    `json:"provider"`
	Operations []OperationReport `json:"operations"`
	Git        GitReport         `json:"git"`
}

// ProviderReport names the one registration the broker pod projects.
type ProviderReport struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

// OperationReport is one semantic operation in the forge package's closed
// capability vocabulary. Detail is the provider's redacted diagnostic.
type OperationReport struct {
	Name      string `json:"name"`
	Available bool   `json:"available"`
	Detail    string `json:"detail,omitempty"`
}

// GitReport summarizes the git push surface. Ready records that the broker's
// own startup preflight verified the credential against the pinned
// destinations; a broker that did not never serves this report.
type GitReport struct {
	Endpoint string `json:"endpoint"`
	Ready    bool   `json:"ready"`
}

// CapabilityReporter supplies the immutable capability report of the pinned
// provider. It is captured at broker startup from the projected registration
// and the provider's own typed surface; no request can change it.
type CapabilityReporter interface {
	CapabilityReport() CapabilityReport
}

// capabilityHandler serves GET /v1/capabilities behind the same
// TokenReview-authenticated identity chain as every other typed route.
func (s *Server) capabilityHandler(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.authenticate(w, r); !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.capabilities.CapabilityReport()); err != nil {
		http.Error(w, "capability report encoding failed", http.StatusInternalServerError)
		return
	}
}
