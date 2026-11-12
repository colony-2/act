//go:build linux || darwin

package container

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hostTestEnv() map[string]string {
	env := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		env[key] = value
	}
	return env
}

type hostTrackingWriter struct {
	buffer   bytes.Buffer
	returned atomic.Bool
	late     atomic.Bool
}

func (w *hostTrackingWriter) Write(p []byte) (int, error) {
	if w.returned.Load() {
		w.late.Store(true)
	}
	return w.buffer.Write(p)
}

func (w *hostTrackingWriter) String() string { return w.buffer.String() }

// These wrappers expose the actual helpers' completion, not goroutine counts.
func hostObservedOperations(t *testing.T) hostExecOperations {
	t.Helper()
	ops := defaultHostExecOperations()
	copyOutput, keepAlive := ops.copyOutput, ops.keepAlive
	outputDone, keepDone := make(chan struct{}), make(chan struct{})
	ops.copyOutput = func(w io.Writer, r io.Reader) error {
		defer close(outputDone)
		return copyOutput(w, r)
	}
	ops.keepAlive = func(ctx context.Context, w io.Writer) {
		defer close(keepDone)
		keepAlive(ctx, w)
	}
	t.Cleanup(func() {
		for name, done := range map[string]<-chan struct{}{"output": outputDone, "keepalive": keepDone} {
			select {
			case <-done:
			default:
				t.Errorf("%s helper was not joined", name)
			}
		}
	})
	return ops
}

func hostAssertExit(t *testing.T, err error, code int) {
	t.Helper()
	if code == 0 {
		assert.NoError(t, err)
		return
	}
	var exit *exec.ExitError
	if assert.ErrorAs(t, err, &exit) {
		assert.Equal(t, code, exit.ExitCode())
	}
}

func TestHostDrain(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			for repeat := 0; repeat < 10; repeat++ {
				var output hostTrackingWriter
				e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
				ops := hostObservedOperations(t)
				err := e.execWithOperations(t.Context(), []string{"bash", "-c", fmt.Sprintf("for i in {1..100}; do echo line-$i; done; printf FINAL-TRAILING-LINE; exit %d", code)}, "", hostTestEnv(), "", ops)
				output.returned.Store(true)
				hostAssertExit(t, err, code)
				var want strings.Builder
				for i := 1; i <= 100; i++ {
					fmt.Fprintf(&want, "line-%d\n", i)
				}
				want.WriteString("FINAL-TRAILING-LINE\n")
				assert.Equal(t, want.String(), output.String(), "iteration %d", repeat)
				assert.False(t, output.late.Load())
			}
		})
	}
}

// Inspect all group members; Z (zombie) and X (dead) are no longer running.
// ps works on both Linux and macOS, unlike /proc-only PID checks.
func hostRunningGroup(group int) ([]int, error) {
	raw, err := exec.Command("ps", "-axo", "pid=,pgid=,stat=").Output()
	if err != nil {
		return nil, err
	}
	var running []int
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		pid, _ := strconv.Atoi(fields[0])
		pgid, _ := strconv.Atoi(fields[1])
		if pgid == group && !strings.HasPrefix(fields[2], "Z") && !strings.HasPrefix(fields[2], "X") {
			running = append(running, pid)
		}
	}
	return running, nil
}

func hostWaitFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(path)
		if len(raw) > 0 {
			return strings.TrimSpace(string(raw))
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fixture not ready: %s", path)
	return ""
}

func hostNoPty(*exec.Cmd, string) (*os.File, *os.File, error) {
	return nil, nil, errors.New("forced PTY unavailability")
}

func hostCleanupFixture(t *testing.T, cancel context.CancelFunc, dir string, done <-chan error, group *int) {
	t.Helper()
	t.Cleanup(func() {
		cancel()
		if *group == 0 {
			raw, _ := os.ReadFile(filepath.Join(dir, "group"))
			*group, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		if *group > 0 && *group != syscall.Getpgrp() {
			_ = syscall.Kill(-*group, syscall.SIGKILL)
		}
		select {
		case <-done:
		case <-time.After(7 * time.Second):
			t.Error("fixture executor did not join")
		}
	})
}

func TestHostDescendants(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, mode := range []string{"success", "failure", "cancel"} {
			t.Run(fmt.Sprintf("pipe=%t/%s", fallback, mode), func(t *testing.T) {
				dir := t.TempDir()
				var output hostTrackingWriter
				e := HostEnvironment{Path: dir, StdOut: &output}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				group := 0
				// Register cancellation, group kill and executor join before assertions.
				hostCleanupFixture(t, cancel, dir, done, &group)
				script := `echo $$ > group
bash -c 'trap "" HUP; bash -c '\''trap "" HUP; echo ready > grand.ready; exec sleep 60'\'' & echo $! > grand.pid; echo ready > child.ready; wait' &
echo $! > child.pid
while test ! -s child.ready || test ! -s grand.ready; do sleep 0.01; done
printf 'READY\n'
echo ready > output.ready
while test ! -f finish; do sleep 0.01; done
`
				if mode == "failure" {
					script += "exit 7\n"
				}
				ops := defaultHostExecOperations()
				if fallback {
					ops.setupPty = hostNoPty
				}
				go func() {
					var err error
					if fallback {
						err = e.execWithOperations(ctx, []string{"bash", "-c", script}, "", hostTestEnv(), "", ops)
					} else {
						err = e.Exec([]string{"bash", "-c", script}, hostTestEnv(), "", "")(ctx)
					}
					done <- err
				}()
				group, _ = strconv.Atoi(hostWaitFile(t, filepath.Join(dir, "group")))
				require.Greater(t, group, 0)
				hostWaitFile(t, filepath.Join(dir, "output.ready"))
				for _, file := range []string{"child.pid", "grand.pid"} {
					pid, err := strconv.Atoi(hostWaitFile(t, filepath.Join(dir, file)))
					require.NoError(t, err)
					pgid, err := syscall.Getpgid(pid)
					require.NoError(t, err)
					require.Equal(t, group, pgid)
				}
				start := time.Now()
				if mode == "cancel" {
					cancel()
				} else {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "finish"), nil, 0600))
				}
				select {
				case err := <-done:
					done <- err // cleanup always owns a join
					output.returned.Store(true)
					if mode == "cancel" {
						var exit *exec.ExitError
						assert.ErrorAs(t, err, &exit)
						if !fallback {
							assert.ErrorContains(t, err, "this step has been cancelled")
						}
					} else if mode == "failure" {
						hostAssertExit(t, err, 7)
					} else {
						assert.NoError(t, err)
					}
				case <-time.After(6 * time.Second):
					t.Fatal("command completion exceeded six seconds")
				}
				assert.Less(t, time.Since(start), 6*time.Second)
				assert.Equal(t, "READY\n", output.String())
				deadline := time.Now().Add(500 * time.Millisecond)
				for {
					running, err := hostRunningGroup(group)
					require.NoError(t, err)
					if len(running) == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("running members after completion: %v", running)
					}
					time.Sleep(10 * time.Millisecond)
				}
				assert.False(t, output.late.Load())
			})
		}
	}
}

func TestHostStartFailures(t *testing.T) {
	for _, mode := range []string{"lookup", "start", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			var output hostTrackingWriter
			e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
			ops := defaultHostExecOperations()
			command := []string{"bash", "-c", "echo should-not-start"}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "lookup" {
				command[0] = "act-command-that-does-not-exist"
			} else {
				ops = hostObservedOperations(t)
				if mode == "start" {
					e.Path = filepath.Join(e.Path, "missing")
				} else {
					cancel()
				}
			}
			ops.cleanupGroup = func(p *os.Process) {
				assert.Nil(t, p, "no process/group is owned on failed startup")
				cleanupHostProcessGroup(p)
			}
			start := time.Now()
			err := e.execWithOperations(ctx, command, "", hostTestEnv(), "", ops)
			output.returned.Store(true)
			if mode == "lookup" {
				assert.ErrorContains(t, err, "Cannot find:")
			} else if mode == "cancelled" {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				var pathErr *os.PathError
				assert.ErrorAs(t, err, &pathErr)
			}
			assert.Less(t, time.Since(start), time.Second)
			assert.NotContains(t, output.String(), "should-not-start")
			assert.False(t, output.late.Load())
		})
	}
	for _, p := range []*os.Process{nil, {Pid: 0}, {Pid: -1}, {Pid: syscall.Getpgrp()}} {
		assert.Zero(t, hostProcessGroup(p))
	}
	otherGroup := syscall.Getpgrp() + 1
	assert.Equal(t, otherGroup, hostProcessGroup(&os.Process{Pid: otherGroup}))
}

func TestHostPtyConfigurationFailure(t *testing.T) {
	ops := defaultHostExecOperations()
	var master, slave *os.File
	t.Cleanup(func() {
		if master != nil {
			_ = master.Close()
		}
		if slave != nil {
			_ = slave.Close()
		}
	})
	setup := ops.setupPty
	ops.setupPty = func(cmd *exec.Cmd, line string) (*os.File, *os.File, error) {
		var err error
		master, slave, err = setup(cmd, line)
		return master, slave, err
	}
	fault := errors.New("forced nonblocking configuration failure")
	ops.configurePty = func(*os.File) error { return fault }
	var output bytes.Buffer
	e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
	err := e.execWithOperations(t.Context(), []string{"bash", "-c", "touch started"}, "", hostTestEnv(), "", ops)
	assert.ErrorIs(t, err, fault)
	_, err = os.Stat(filepath.Join(e.Path, "started"))
	assert.True(t, os.IsNotExist(err))
	require.NotNil(t, master)
	require.NotNil(t, slave)
	_, err = master.Read(make([]byte, 1))
	assert.ErrorIs(t, err, os.ErrClosed)
	_, err = slave.Write([]byte{4})
	assert.ErrorIs(t, err, os.ErrClosed)
}

func TestHostPtyDrainTimeout(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			var output hostTrackingWriter
			e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
			ops := hostObservedOperations(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// Hold the real slave open and omit EOT. The real master must stall
			// until the production timeout closes its interruptible read.
			ops.writeEOT = func(*os.File) error { return io.ErrClosedPipe }
			var completed time.Time
			cleanup := ops.cleanupGroup
			ops.cleanupGroup = func(p *os.Process) { completed = time.Now(); cleanup(p) }
			script := "echo final; exit 0"
			if mode == "failure" {
				script = "echo final; exit 7"
			}
			if mode == "cancel" {
				script = "echo final; echo ready > ready; exec sleep 60"
				joined := make(chan struct{})
				t.Cleanup(func() { cancel(); <-joined })
				go func() {
					defer close(joined)
					for {
						if _, err := os.Stat(filepath.Join(e.Path, "ready")); err == nil {
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Millisecond):
						}
					}
				}()
			}
			err := e.execWithOperations(ctx, []string{"bash", "-c", script}, "", hostTestEnv(), "", ops)
			output.returned.Store(true)
			elapsed := time.Since(completed)
			assert.GreaterOrEqual(t, elapsed, 5*time.Second)
			assert.Less(t, elapsed, 6*time.Second)
			if mode == "success" {
				assert.ErrorContains(t, err, "PTY drain timed out")
			} else if mode == "failure" {
				hostAssertExit(t, err, 7)
			} else {
				var exit *exec.ExitError
				assert.ErrorAs(t, err, &exit)
				assert.NotContains(t, err.Error(), "timed out")
			}
			assert.Equal(t, "final\n", output.String())
			assert.False(t, output.late.Load())
			t.Logf("%s: %v after %s", mode, err, elapsed)
		})
	}
}

func TestHostFallbackOutput(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var output hostTrackingWriter
			e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
			ops := defaultHostExecOperations()
			ops.setupPty = hostNoPty
			copied := make(chan struct{})
			copyOutput := ops.copyOutput
			ops.copyOutput = func(w io.Writer, r io.Reader) error { defer close(copied); return copyOutput(w, r) }
			err := e.execWithOperations(t.Context(), []string{"bash", "-c", fmt.Sprintf("printf stdout; printf stderr >&2; printf '\\004'; exit %d", code)}, "", hostTestEnv(), "", ops)
			output.returned.Store(true)
			hostAssertExit(t, err, code)
			select {
			case <-copied:
			default:
				t.Error("pipe copier not joined")
			}
			assert.Equal(t, "stdoutstderr\x04", output.String())
			assert.False(t, output.late.Load())
		})
	}
}

func TestHostKeepAliveBlockedWrite(t *testing.T) {
	var output hostTrackingWriter
	e := HostEnvironment{Path: t.TempDir(), StdOut: &output}
	ops := hostObservedOperations(t)
	keepAlive := ops.keepAlive
	started := make(chan struct{})
	ops.keepAlive = func(ctx context.Context, w io.Writer) {
		keepAlive(ctx, hostFullInputWriter{writer: w, started: started})
	}
	// Wait until the helper starts a write larger than the terminal input
	// queue, then finish a command that never reads stdin.
	dir := e.Path
	joined := make(chan struct{})
	t.Cleanup(func() { <-joined })
	go func() {
		defer close(joined)
		select {
		case <-started:
			_ = os.WriteFile(filepath.Join(dir, "finish"), nil, 0600)
		case <-t.Context().Done():
		}
	}()
	start := time.Now()
	err := e.execWithOperations(t.Context(), []string{"bash", "-c", "echo final; while test ! -f finish; do sleep 0.01; done"}, "", hostTestEnv(), "", ops)
	output.returned.Store(true)
	assert.ErrorContains(t, err, "PTY drain timed out")
	assert.Less(t, time.Since(start), 6*time.Second)
	assert.Equal(t, "final\n", output.String())
	assert.False(t, output.late.Load())
}

type hostFullInputWriter struct {
	writer  io.Writer
	started chan struct{}
}

func (w hostFullInputWriter) Write(_ []byte) (int, error) {
	close(w.started)
	return w.writer.Write(make([]byte, 1024*1024))
}
