package connector

import (
	"slices"

	"github.com/prometheus/client_golang/prometheus"
)

// Capability names (§3). The vocabulary is closed and mirrors the optional
// interfaces Courier already defines, so a connector cannot advertise a
// capability Courier has no seam for.
const (
	CapabilityDiscovery             = "discovery"
	CapabilityDiscoveryExclusions   = "discovery-exclusions"
	CapabilityLifecycleReport       = "lifecycle-report"
	CapabilityPreLaunchRevalidation = "pre-launch-revalidation"
	CapabilityWorkIdentity          = "work-identity"
	CapabilityAvailability          = "availability"
)

// Capabilities is the closed capability catalogue, the single source of
// truth for the capability label values of the info gauge.
var Capabilities = []string{
	CapabilityDiscovery,
	CapabilityDiscoveryExclusions,
	CapabilityLifecycleReport,
	CapabilityPreLaunchRevalidation,
	CapabilityWorkIdentity,
	CapabilityAvailability,
}

// Capability states (§4): every capability resolves to exactly one of
// supported, unsupported, or unknown; unsupported and unknown are
// first-class states, never summed into a success count.
const (
	CapabilitySupported   = "supported"
	CapabilityUnsupported = "unsupported"
	CapabilityUnknown     = "unknown"
)

// CapabilityStates is the closed capability state vocabulary.
var CapabilityStates = []string{
	CapabilitySupported,
	CapabilityUnsupported,
	CapabilityUnknown,
}

// MetricCapabilityInfo is the info-style gauge through which a dashboard
// discovers which optional capabilities a connector offers (§5).
const MetricCapabilityInfo = "courier_connector_capability_info"

func newCapabilityInfo() *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricCapabilityInfo,
		Help: "Connector capability catalogue: 1 for the negotiated state of each declared capability.",
	}, []string{"connector", "capability", "state"})
}

// Capability records the negotiated state of one of the connector's
// capabilities in the capability catalogue (§5). The gauge value is always 1,
// and switching a capability's state overwrites the previous state's series so
// the catalogue never shows two states for the same (connector, capability).
// A capability outside the closed catalogue is not advertised at all; a state
// outside the closed state vocabulary is reported as unknown.
func (e *Emitter) Capability(connector, capability, state string) {
	if e == nil {
		return
	}
	if !slices.Contains(Capabilities, capability) {
		return
	}
	if !slices.Contains(CapabilityStates, state) {
		state = CapabilityUnknown
	}
	labels := prometheus.Labels{"connector": connector, "capability": capability, "state": state}
	if err := RejectCorrelation(labels); err != nil {
		return
	}
	for _, existing := range CapabilityStates {
		if existing != state {
			e.capabilityInfo.DeleteLabelValues(connector, capability, existing)
		}
	}
	e.capabilityInfo.WithLabelValues(connector, capability, state).Set(1)
}
