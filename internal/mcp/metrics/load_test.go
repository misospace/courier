package metrics

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNormalizeDerivesBackpressureFromWaiting(t *testing.T) {
	running, waiting, cache := 1.0, 3.0, 0.72
	zero, long := 0.0, 5.0

	tests := []struct {
		name    string
		signals Signals
		want    Load
	}{
		{
			name:    "a queue is backpressure",
			signals: Signals{Running: &running, Waiting: &waiting, KVCacheUsage: &cache},
			want: Load{
				Available: true, Running: &running, Waiting: &waiting,
				Backpressured: boolPtr(true), KVCacheUsage: &cache,
			},
		},
		{
			name:    "running alone proves nothing either way",
			signals: Signals{Running: &long},
			want:    Load{Available: true, Running: &long},
		},
		{
			name:    "running with an empty queue is healthy",
			signals: Signals{Running: &running, Waiting: &zero},
			want:    Load{Available: true, Running: &running, Waiting: &zero, Backpressured: boolPtr(false)},
		},
		{
			name:    "unknown waiting leaves backpressure unknown",
			signals: Signals{KVCacheUsage: &cache},
			want:    Load{Available: true, KVCacheUsage: &cache},
		},
		{
			name:    "no signals at all",
			signals: Signals{},
			want:    Load{Available: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalize(tt.signals); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("normalize() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestUnavailableIsNeverBackpressured(t *testing.T) {
	load := unavailable("endpoint down")
	if load.Available || load.Backpressured != nil || load.Detail == "" {
		t.Fatalf("unavailable() = %+v, want available=false, no backpressure claim, a detail", load)
	}
}

// The JSON is the tool contract; lock its exact shape so renames break here
// and not on some coordinator's prompt.
func TestLoadJSONContract(t *testing.T) {
	running, waiting, cache := 1.0, 3.0, 0.72
	full, err := json.Marshal(Load{
		Available: true, Running: &running, Waiting: &waiting,
		Backpressured: boolPtr(true), KVCacheUsage: &cache,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"available":true,"running":1,"waiting":3,"backpressured":true,"kvCacheUsage":0.72}`; string(full) != want {
		t.Fatalf("full load JSON = %s, want %s", full, want)
	}

	degraded, err := json.Marshal(unavailable("endpoint down"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"available":false,"detail":"endpoint down"}`; string(degraded) != want {
		t.Fatalf("degraded load JSON = %s, want %s", degraded, want)
	}
}

func boolPtr(b bool) *bool { return &b }
