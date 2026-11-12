//go:build linux || darwin

package container

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hostFailingWriter struct {
	err      error
	written  chan struct{}
	called   atomic.Bool
	returned atomic.Bool
	late     atomic.Bool
}

func (w *hostFailingWriter) Write(_ []byte) (int, error) {
	if w.returned.Load() {
		w.late.Store(true)
	}
	if w.called.CompareAndSwap(false, true) {
		close(w.written)
	}
	return 0, w.err
}

func TestHostFallbackWriterFailure(t *testing.T) {
	for _, mode := range []string{"small", "large", "nonzero", "start", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			fault := errors.New("host output sink failed")
			writer := &hostFailingWriter{err: fault, written: make(chan struct{})}
			e := HostEnvironment{Path: dir, StdOut: writer}
			ops := defaultHostExecOperations()
			ops.setupPty = hostNoPty
			copied := make(chan struct{})
			copyOutput := ops.copyOutput
			ops.copyOutput = func(w io.Writer, r io.Reader) error {
				defer close(copied)
				return copyOutput(w, r)
			}
			ops.keepAlive = func(context.Context, io.Writer) { t.Error("fallback must not start PTY keepalive") }
			var process *os.Process
			cleanup := ops.cleanupGroup
			ops.cleanupGroup = func(p *os.Process) { process = p; cleanup(p) }
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			group := 0
			// Always cancel, retire the fixture group and join, even if a bound
			// or readiness assertion fails. Cancellation is only a safety guard.
			hostCleanupFixture(t, cancel, dir, done, &group)
			script := `echo $$ > group
bash -c 'trap "" HUP; echo ready > child.ready; exec sleep 60' &
while test ! -s child.ready; do sleep 0.01; done
`
			switch mode {
			case "large":
				script += "printf '%1000000s' x"
			case "nonzero":
				script += "printf payload; exit 7"
			case "cancel":
				script += "printf payload; exec sleep 60"
			default:
				script += "printf payload"
			}
			if mode == "start" {
				e.Path = filepath.Join(dir, "missing")
			}
			start := time.Now()
			go func() { done <- e.execWithOperations(ctx, []string{"bash", "-c", script}, "", hostTestEnv(), "", ops) }()
			if mode == "cancel" {
				select {
				case <-writer.written:
					cancel()
				case <-time.After(3 * time.Second):
					t.Fatal("failed-sink output was not ready")
				}
			}
			var err error
			select {
			case err = <-done:
				done <- err // cleanup owns a join on every path
			case <-time.After(3 * time.Second):
				t.Fatal("failed sink stranded the command")
			}
			writer.returned.Store(true)
			elapsed := time.Since(start)
			assert.Less(t, elapsed, 3*time.Second)
			select {
			case <-copied:
			default:
				t.Error("failed-sink collector was not joined")
			}
			assert.False(t, writer.late.Load())
			switch mode {
			case "small":
				assert.ErrorIs(t, err, fault)
			case "nonzero":
				hostAssertExit(t, err, 7)
			case "start":
				var pathErr *os.PathError
				assert.ErrorAs(t, err, &pathErr)
				assert.Nil(t, process)
				assert.False(t, writer.called.Load())
			default:
				var exit *exec.ExitError
				assert.ErrorAs(t, err, &exit)
				assert.NotErrorIs(t, err, fault, "command error must take precedence")
			}
			if mode != "cancel" {
				assert.NoError(t, ctx.Err(), "completion must not depend on cancellation")
			}
			if mode != "start" {
				require.NotNil(t, process)
				group = process.Pid
				hostAssertStoppedGroup(t, group)
			}
			t.Logf("%s: %v after %s, context=%v", mode, err, elapsed, ctx.Err())
		})
	}
}

func hostAssertStoppedGroup(t *testing.T, group int) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		running, err := hostRunningGroup(group)
		require.NoError(t, err)
		if len(running) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("running members after failed sink: %v", running)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
