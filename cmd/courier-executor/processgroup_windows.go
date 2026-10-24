//go:build windows

package main

import (
	"errors"
	"syscall"
)

// Supervised children are contained through POSIX process groups; Windows
// has none of that ABI. These stand-ins exist for the build graph only and
// fail closed, mirroring the inert reaper off Linux: a child runs in the
// default group, and a cancellation that cannot signal a group reports the
// failure instead of claiming a verified tree kill.

var errProcessGroupUnsupported = errors.New("courier: process-group control is unavailable on this platform")

func processGroupSysProcAttr() *syscall.SysProcAttr { return nil }

func killProcessGroup(int, syscall.Signal) error { return errProcessGroupUnsupported }
