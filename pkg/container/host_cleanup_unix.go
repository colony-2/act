//go:build linux || darwin

package container

import (
	"os"
	"syscall"
)

func configureHostPty(file *os.File) error {
	// Required by creack/pty for Close and deadlines to interrupt I/O.
	return syscall.SetNonblock(int(file.Fd()), true)
}

func hostProcessGroup(process *os.Process) int {
	if process == nil || process.Pid <= 0 || process.Pid == syscall.Getpgrp() {
		return 0
	}
	return process.Pid
}

func cleanupHostProcessGroup(process *os.Process) {
	group := hostProcessGroup(process)
	if group == 0 {
		return
	}
	// Commands start a new session (PTY) or process group (pipe). Never
	// signal group zero or the caller's group, including after failed Start.
	_ = syscall.Kill(-group, syscall.SIGKILL)
}
