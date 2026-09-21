package metrics

import (
	"reflect"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func mustParse(t *testing.T, text string) []*dto.MetricFamily {
	t.Helper()
	families, err := parsePrometheus(strings.NewReader(text))
	if err != nil {
		t.Fatalf("parsePrometheus(%q): %v", text, err)
	}
	return families
}

const vllmMetrics = `# HELP vllm:num_requests_running Number of requests currently running on GPU.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 1.0
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting 3.0
# HELP vllm:gpu_cache_usage_perc GPU KV-cache usage. 1 means 100 percent usage.
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.72
# HELP vllm:request_success_total Count of successfully processed requests.
# TYPE vllm:request_success_total counter
vllm:request_success_total 42.0
`

func TestVLLMMappingReadsAllGauges(t *testing.T) {
	signals, ok := (vllmMapping{}).Map(mustParse(t, vllmMetrics))
	if !ok {
		t.Fatal("vllmMapping.Map did not recognize vLLM metrics")
	}
	running, waiting, cache := 1.0, 3.0, 0.72
	want := Signals{Running: &running, Waiting: &waiting, KVCacheUsage: &cache}
	if !reflect.DeepEqual(signals, want) {
		t.Fatalf("signals = %+v, want %+v", signals, want)
	}
}

func TestVLLMMappingToleratesPartialGauges(t *testing.T) {
	running := 2.0
	text := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 2.0
`
	signals, ok := (vllmMapping{}).Map(mustParse(t, text))
	if !ok {
		t.Fatal("vllmMapping.Map did not recognize partial vLLM metrics")
	}
	if !reflect.DeepEqual(signals, Signals{Running: &running}) {
		t.Fatalf("signals = %+v, want only running=%v", signals, running)
	}
}

func TestVLLMMappingTakesWorstCaseAcrossLabeledSeries(t *testing.T) {
	text := `# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="small"} 2.0
vllm:num_requests_waiting{model_name="large"} 7.0
`
	signals, ok := (vllmMapping{}).Map(mustParse(t, text))
	if !ok {
		t.Fatal("vllmMapping.Map did not recognize labeled series")
	}
	if signals.Waiting == nil || *signals.Waiting != 7.0 {
		t.Fatalf("waiting = %v, want 7", signals.Waiting)
	}
}

func TestVLLMMappingRejectsUnknownMetrics(t *testing.T) {
	unknown := `# TYPE http_requests_total counter
http_requests_total 12.0
`
	if _, ok := (vllmMapping{}).Map(mustParse(t, unknown)); ok {
		t.Fatal("vllmMapping.Map recognized unrelated metrics")
	}
}

func TestAutoMappingSniffsVLLM(t *testing.T) {
	families := mustParse(t, vllmMetrics)
	if !vllmExposed(families) {
		t.Fatal("vllmExposed = false for vLLM metrics")
	}
	signals, ok := (autoMapping{}).Map(families)
	if !ok || signals.Waiting == nil || *signals.Waiting != 3.0 {
		t.Fatalf("autoMapping signals = %+v, ok = %v", signals, ok)
	}
}

func TestAutoMappingNeverGuesses(t *testing.T) {
	other := `# TYPE sglang:num_reqs gauge
sglang:num_reqs 1.0
`
	if _, ok := (autoMapping{}).Map(mustParse(t, other)); ok {
		t.Fatal("autoMapping recognized a backend it has no mapping for")
	}
	vllmNamed := `# TYPE vllm:some_other_metric counter
vllm:some_other_metric 1.0
`
	if _, ok := (autoMapping{}).Map(mustParse(t, vllmNamed)); ok {
		t.Fatal("autoMapping claimed vLLM load with none of its load gauges present")
	}
	if vllmExposed(mustParse(t, other)) {
		t.Fatal("vllmExposed matched a foreign namespace")
	}
}

// Non-finite gauges are broken instrumentation, not load: they must never
// reach Load, and when nothing usable remains the mapping reports
// unrecognized so the caller degrades normally.
func TestVLLMMappingIgnoresNonFiniteGauges(t *testing.T) {
	nonFinite := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running 2.0
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting NaN
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc +Inf
`
	signals, ok := (vllmMapping{}).Map(mustParse(t, nonFinite))
	if !ok {
		t.Fatal("vllmMapping.Map discarded a backend with a usable running gauge")
	}
	running := 2.0
	if !reflect.DeepEqual(signals, Signals{Running: &running}) {
		t.Fatalf("signals = %+v, want only running=2", signals)
	}
}

func TestVLLMMappingUnusableWhenAllGaugesNonFinite(t *testing.T) {
	allBad := `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running -Inf
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting NaN
`
	if _, ok := (vllmMapping{}).Map(mustParse(t, allBad)); ok {
		t.Fatal("vllmMapping.Map claimed usable signals from only non-finite gauges")
	}
}

func TestVLLMMappingKeepsFiniteSeriesBesideNonFinite(t *testing.T) {
	mixed := `# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="broken"} NaN
vllm:num_requests_waiting{model_name="healthy"} 7.0
`
	signals, ok := (vllmMapping{}).Map(mustParse(t, mixed))
	if !ok || signals.Waiting == nil || *signals.Waiting != 7.0 {
		t.Fatalf("signals = %+v, ok = %v, want waiting=7", signals, ok)
	}
}

func TestFor(t *testing.T) {
	tests := []struct {
		backend string
		want    string
		wantErr bool
	}{
		{backend: "", want: BackendAuto},
		{backend: "auto", want: BackendAuto},
		{backend: "vllm", want: BackendVLLM},
		{backend: "sglang", wantErr: true},
	}
	for _, tt := range tests {
		t.Run("backend="+tt.backend, func(t *testing.T) {
			mapping, err := For(tt.backend)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("For(%q) succeeded, want error", tt.backend)
				}
				return
			}
			if err != nil {
				t.Fatalf("For(%q): %v", tt.backend, err)
			}
			if mapping.Name() != tt.want {
				t.Fatalf("For(%q).Name() = %q, want %q", tt.backend, mapping.Name(), tt.want)
			}
		})
	}
}
