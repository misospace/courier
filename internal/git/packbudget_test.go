package git

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// assertEmptyDirectory fails when dir holds any entry at all: a rejected
// bundle must leave nothing behind in the quarantine.
func assertEmptyDirectory(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("quarantine holds %d entries, want none", len(entries))
	}
}

// requireLimit fails unless err reports the named bound.
func requireLimit(t *testing.T, err error, limit string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want the %q bound", limit)
	}
	var limitErr *LimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("error = %v, want a LimitError", err)
	}
	if limitErr.Limit != limit {
		t.Fatalf("limit = %q, want %q", limitErr.Limit, limit)
	}
}

// TestUnbundleRejectsOversizedBlobBeforeInflation proves the budget is
// enforced before the pack is inflated: a bundle whose single blob is over
// the per-object bound is refused while it is still compressed, and no
// object lands in the quarantine.
func TestUnbundleRejectsOversizedBlobBeforeInflation(t *testing.T) {
	ctx := context.Background()
	// Twenty mebibytes of one repeated byte: a few tens of kibibytes on
	// the wire, twenty mebibytes once inflated.
	repo, _ := seedRepo(t, t.TempDir(), "big.bin", string(bytes.Repeat([]byte("A"), 20<<20)))
	bundle := filepath.Join(t.TempDir(), "big.bundle")
	if err := BundleCreate(ctx, repo, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	limits := generousLimits()
	limits.MaxObjectBytes = 1 << 20
	requireLimit(t, Unbundle(ctx, quarantine, bundle, limits), LimitObjectBytes)
	assertEmptyDirectory(t, quarantine)
}

// TestUnbundleRejectsTooManyObjects proves the object-count bound is read
// from the pack header before any object is inflated.
func TestUnbundleRejectsTooManyObjects(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedRepo(t, t.TempDir(), "a.txt", "a")
	commitFile(t, dir, "b.txt", "b", "second")
	commitFile(t, dir, "c.txt", "c", "third")
	bundle := filepath.Join(t.TempDir(), "many.bundle")
	if err := BundleCreate(ctx, dir, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	limits := generousLimits()
	limits.MaxObjects = 3
	requireLimit(t, Unbundle(ctx, quarantine, bundle, limits), LimitObjects)
	assertEmptyDirectory(t, quarantine)
}

// TestUnbundleRejectsOversizedTotal proves the total unpacked-byte bound is
// enforced across the whole object set, not only per object.
func TestUnbundleRejectsOversizedTotal(t *testing.T) {
	ctx := context.Background()
	repo, _ := seedRepo(t, t.TempDir(), "data.bin", string(bytes.Repeat([]byte("B"), 1<<20)))
	bundle := filepath.Join(t.TempDir(), "data.bundle")
	if err := BundleCreate(ctx, repo, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	limits := generousLimits()
	limits.MaxTotalBytes = 64 << 10
	limits.MaxObjectBytes = 4 << 20
	requireLimit(t, Unbundle(ctx, quarantine, bundle, limits), LimitBytes)
	assertEmptyDirectory(t, quarantine)
}

// TestUnbundleAndInventoryWithinLimits is the positive control: a bundle
// inside the budget unpacks and inventories exactly, including the delta
// objects a pack carries for an unchanged file.
func TestUnbundleAndInventoryWithinLimits(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	git(t, dir, "init", "--quiet", "--initial-branch", "main")
	// The same file committed twice with a small change: the pack stores
	// the second version as a delta against the first.
	payload := bytes.Repeat([]byte("payload"), 64*1024)
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", "big.bin")
	git(t, dir, "commit", "--quiet", "-m", "big")
	changed := append([]byte(nil), payload...)
	copy(changed[len(changed)-8:], []byte("modified"))
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), changed, 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", "big.bin")
	git(t, dir, "commit", "--quiet", "-m", "big again")

	bundle := filepath.Join(t.TempDir(), "delta.bundle")
	if err := BundleCreate(ctx, dir, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	if err := Unbundle(ctx, quarantine, bundle, generousLimits()); err != nil {
		t.Fatal(err)
	}
	// The delta path of the scan is only exercised when the pack really
	// carries a delta.
	if count := countPackDeltas(t, quarantine); count == 0 {
		t.Fatal("pack carries no delta object, want the delta path exercised")
	}
	fresh := t.TempDir()
	git(t, fresh, "init", "--quiet", "--initial-branch", "main")
	inventory, err := InventoryObjects(ctx, fresh, quarantine, generousLimits())
	if err != nil {
		t.Fatal(err)
	}
	// Two commits, two trees, two blobs: the delta is a storage form of
	// the second blob, not a replacement for the first.
	if inventory.Count != 6 {
		t.Fatalf("inventory count = %d, want 6", inventory.Count)
	}
	if inventory.MaxBlobBytes != int64(len(payload)) {
		t.Fatalf("inventory max blob = %d, want %d", inventory.MaxBlobBytes, len(payload))
	}
	if inventory.TotalBytes <= inventory.MaxBlobBytes {
		t.Fatalf("inventory bytes = %d, want more than the blob", inventory.TotalBytes)
	}
}

// TestUnbundleRejectsMalformedBundle proves a file that is not a bundle is
// an ordinary failure, not a budget violation.
func TestUnbundleRejectsMalformedBundle(t *testing.T) {
	ctx := context.Background()
	quarantine := t.TempDir()
	cases := map[string][]byte{
		"not a bundle":          []byte("this is not a git bundle\n"),
		"truncated pack":        []byte("# v2 git bundle\n0000000000000000000000000000000000000000 HEAD\n\n"),
		"header without a pack": []byte("# v2 git bundle\n\nNOTAPACK"),
	}
	for name, content := range cases {
		bundle := filepath.Join(t.TempDir(), "broken.bundle")
		if err := os.WriteFile(bundle, content, 0o600); err != nil {
			t.Fatal(err)
		}
		err := Unbundle(ctx, quarantine, bundle, generousLimits())
		if err == nil {
			t.Fatalf("%s: error = nil, want a malformed-bundle error", name)
		}
		var limitErr *LimitError
		if errors.As(err, &limitErr) {
			t.Fatalf("%s: error = %v, want a plain error, not a LimitError", name, err)
		}
		assertEmptyDirectory(t, quarantine)
	}
}

// TestUnbundleRejectsUnsetLimits proves the budget fails closed: an unset
// bound is never read as "no bound".
func TestUnbundleRejectsUnsetLimits(t *testing.T) {
	ctx := context.Background()
	dir, _ := seedRepo(t, t.TempDir(), "a.txt", "a")
	bundle := filepath.Join(t.TempDir(), "w.bundle")
	if err := BundleCreate(ctx, dir, bundle, "HEAD"); err != nil {
		t.Fatal(err)
	}
	quarantine := t.TempDir()
	for name, limits := range map[string]ObjectLimits{
		"no objects":      {MaxObjects: 0, MaxTotalBytes: 1, MaxObjectBytes: 1},
		"no bytes":        {MaxObjects: 1, MaxTotalBytes: 0, MaxObjectBytes: 1},
		"no object bytes": {MaxObjects: 1, MaxTotalBytes: 1, MaxObjectBytes: 0},
	} {
		err := Unbundle(ctx, quarantine, bundle, limits)
		if err == nil {
			t.Fatalf("%s: error = nil, want an invalid-limits error", name)
		}
		var limitErr *LimitError
		if errors.As(err, &limitErr) {
			t.Fatalf("%s: error = %v, want a plain error, not a LimitError", name, err)
		}
		assertEmptyDirectory(t, quarantine)
	}
}

// countPackDeltas reports how many objects of the unpacked pack are deltas.
func countPackDeltas(t *testing.T, quarantine string) int {
	t.Helper()
	indexes, err := filepath.Glob(filepath.Join(quarantine, "pack", "*.idx"))
	if err != nil || len(indexes) == 0 {
		t.Fatalf("no pack index in the quarantine: %v", err)
	}
	out, err := gitOutput(quarantine, "verify-pack", "-v", indexes[0])
	if err != nil {
		t.Fatalf("verify-pack: %v", err)
	}
	deltas := 0
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		// A delta object carries its depth and its base object; a whole
		// object does not.
		if len(fields) >= 7 && fields[5] != "0" {
			deltas++
		}
	}
	return deltas
}
