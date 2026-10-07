package evidence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func sampleManifest() Manifest {
	return Manifest{
		SchemaVersion: SchemaVersion,
		Run: RunIdentity{
			Name:      "run-42",
			Namespace: "courier",
			RunUID:    "run-uid-1",
			PodUID:    "pod-uid-1",
		},
		Workspace: WorkspaceIdentity{
			BaseRepo: "org/base",
			HeadRepo: "org/fork",
			Branch:   "courier/run-42",
			StartSHA: "0000000000000000000000000000000000000000",
			HeadSHA:  "1111111111111111111111111111111111111111",
		},
		CapturedAt: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC),
		Trigger:    TriggerTerminal,
		Entries: []Entry{
			{
				Path:        "internal/evidence/scan.go",
				Class:       ClassModified,
				Disposition: DispositionStored,
				StoredPath:  "internal/evidence/scan.go",
				Bytes:       2048,
			},
			{
				Path:        "secrets.env",
				Class:       ClassUntracked,
				Disposition: DispositionWithheld,
			},
			{
				Path:        "link",
				Class:       ClassUntracked,
				Disposition: DispositionSymlink,
				LinkTarget:  "../outside",
				Escaping:    true,
			},
		},
		Totals: Totals{
			Files:            3,
			Commits:          1,
			Stored:           1,
			Withheld:         1,
			OmittedBinary:    0,
			OmittedOverLimit: 0,
			OmittedByBudget:  0,
			Deleted:          0,
			Symlinks:         1,
			StoredBytes:      2048,
		},
	}
}

func TestMarshalCanonicalRoundTrip(t *testing.T) {
	want := sampleManifest()

	data, err := want.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("MarshalCanonical produced invalid JSON: %s", data)
	}

	var got Manifest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip mismatch:\ngot  %+v\nwant %+v", got, want)
	}
}

func TestMarshalCanonicalTopLevelKeys(t *testing.T) {
	m := sampleManifest()
	data, err := m.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	wantKeys := map[string]struct{}{
		"schemaVersion": {},
		"run":           {},
		"workspace":     {},
		"capturedAt":    {},
		"trigger":       {},
		"entries":       {},
		"totals":        {},
	}
	if len(raw) != len(wantKeys) {
		t.Errorf("top-level key count = %d, want %d (keys: %v)", len(raw), len(wantKeys), keysOf(raw))
	}
	for k := range wantKeys {
		if _, ok := raw[k]; !ok {
			t.Errorf("missing top-level key %q", k)
		}
	}
}

func TestMarshalCanonicalTotalsKeys(t *testing.T) {
	m := sampleManifest()
	data, err := m.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}

	var raw struct {
		Totals map[string]json.RawMessage `json:"totals"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	wantKeys := map[string]struct{}{
		"files":            {},
		"commits":          {},
		"stored":           {},
		"withheld":         {},
		"omittedBinary":    {},
		"omittedOverLimit": {},
		"omittedByBudget":  {},
		"deleted":          {},
		"symlinks":         {},
		"storedBytes":      {},
	}
	if len(raw.Totals) != len(wantKeys) {
		t.Errorf("totals key count = %d, want %d (keys: %v)", len(raw.Totals), len(wantKeys), keysOf(raw.Totals))
	}
	for k := range wantKeys {
		if _, ok := raw.Totals[k]; !ok {
			t.Errorf("missing totals key %q", k)
		}
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
