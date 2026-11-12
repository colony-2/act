//go:build !linux && !darwin

package container

import "os"

func configureHostPty(_ *os.File) error { return nil }

func cleanupHostProcessGroup(_ *os.Process) {}
