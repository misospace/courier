//go:build unix

package main

import "syscall"

// processGroupSysProcAttr puts a supervised child in a process group it
// leads: the group is what makes the child's descendants recognizable by the
// adopted-child reaper and what lets cancellation reach the whole tree.
func processGroupSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup delivers sig to the whole process group pid leads.
func killProcessGroup(pid int, sig syscall.Signal) error {
	return syscall.Kill(-pid, sig)
}
