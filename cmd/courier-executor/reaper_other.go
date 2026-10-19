//go:build !linux

package main

import (
	"os/exec"
	"sync"
)

// adoptedChildReaper is inert off Linux: descendant enumeration needs procfs,
// so there is nothing to reap and nothing to arm. run is still the funnel a
// supervised child goes through, and it is the child's own Wait that produces
// its exit status, exactly as on Linux.
type adoptedChildReaper struct {
	gate       sync.Mutex
	mu         sync.Mutex
	supervised map[int]struct{}
	armed      bool
}

var (
	reaperMu      sync.Mutex
	processReaper = &adoptedChildReaper{supervised: make(map[int]struct{})}
)

func installAdoptedChildReaper() *adoptedChildReaper {
	reaperMu.Lock()
	defer reaperMu.Unlock()
	processReaper.armed = true
	return processReaper
}

func childReaper() *adoptedChildReaper {
	reaperMu.Lock()
	defer reaperMu.Unlock()
	return processReaper
}

func (r *adoptedChildReaper) drain() {}

func (r *adoptedChildReaper) run(cmd *exec.Cmd) error {
	return cmd.Run()
}
