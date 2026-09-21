package metrics

import (
	"strings"

	dto "github.com/prometheus/client_model/go"
)

// vLLM's gauges, per its /metrics exposition. The queue gauge is the
// saturation signal; see DESIGN.md, "Contention and concurrency".
const (
	vllmPrefix = "vllm:"

	vllmNumRunning    = "vllm:num_requests_running"
	vllmNumWaiting    = "vllm:num_requests_waiting"
	vllmGPUCacheUsage = "vllm:gpu_cache_usage_perc"
)

// vllmMapping is the first concrete backend mapping. Later backends
// (sglang, llama.cpp, TGI, ollama) add their own file and a For case, never
// fields on Load.
type vllmMapping struct{}

func (vllmMapping) Name() string { return BackendVLLM }

func (vllmMapping) Map(families []*dto.MetricFamily) (Signals, bool) {
	gauges := gaugeValues(families)
	signals := Signals{
		Running:      gauges[vllmNumRunning],
		Waiting:      gauges[vllmNumWaiting],
		KVCacheUsage: gauges[vllmGPUCacheUsage],
	}
	if signals.Running == nil && signals.Waiting == nil && signals.KVCacheUsage == nil {
		return Signals{}, false
	}
	return signals, true
}

func vllmExposed(families []*dto.MetricFamily) bool {
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), vllmPrefix) {
			return true
		}
	}
	return false
}
