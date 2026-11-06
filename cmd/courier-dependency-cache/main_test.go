package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunArgsRequiresRoot(t *testing.T) {
	err := runArgs([]string{})
	if err == nil || !strings.Contains(err.Error(), "--root") {
		t.Fatalf("missing --root must be rejected, got %v", err)
	}
}

func TestRunArgsRejectsUnusableStoreRoot(t *testing.T) {
	if err := runArgs([]string{"--root=" + filepath.Join(t.TempDir(), "absent")}); err == nil {
		t.Fatal("an absent store root must refuse to start")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runArgs([]string{"--root=" + file}); err == nil {
		t.Fatal("a non-directory store root must refuse to start")
	}
}

func TestRunArgsRejectsUnusableBounds(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{
		{"--root=" + root, "--burst=0"},
		{"--root=" + root, "--max-tracked-addrs=0"},
	} {
		if err := runArgs(args); err == nil {
			t.Fatalf("args %v must be rejected", args)
		}
	}
}
