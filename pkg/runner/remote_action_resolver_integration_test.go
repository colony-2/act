package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/common/git"
	"github.com/nektos/act/pkg/container"
	"github.com/nektos/act/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/yaml.v3"
)

type resolverExecution struct {
	command []string
	ref     string
}
type resolverContainer struct {
	containerMock
	files      map[string]string
	executions []resolverExecution
	copies     []string
}

func newResolverContainer(t *testing.T) *resolverContainer {
	t.Helper()
	c := &resolverContainer{files: map[string]string{}}
	noop := func(context.Context) error { return nil }
	c.On("Copy", mock.Anything, mock.Anything).Return(noop)
	c.On("UpdateFromEnv", mock.Anything, mock.Anything).Return(noop)
	return c
}

func (c *resolverContainer) Exec(command []string, env map[string]string, _, _ string) common.Executor {
	return func(context.Context) error {
		c.executions = append(c.executions, resolverExecution{append([]string(nil), command...), env["GITHUB_ACTION_REF"]})
		return nil
	}
}
func (c *resolverContainer) CopyDir(dest, _ string, _ bool) common.Executor {
	return func(context.Context) error { c.copies = append(c.copies, dest); return nil }
}
func (c *resolverContainer) CopyTarStream(_ context.Context, dest string, r io.Reader) error {
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		c.files[path.Join(dest, h.Name)] = string(b)
	}
}
func (c *resolverContainer) GetContainerArchive(_ context.Context, name string) (io.ReadCloser, error) {
	if body, ok := c.files[name]; ok {
		return resolverTar(map[string]string{name: body}, name)
	}
	if strings.HasPrefix(name, "/var/run/act/workflow/") {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return nil, os.ErrNotExist
}
func (*resolverContainer) ReplaceLogWriter(_, _ io.Writer) (io.Writer, io.Writer) {
	return io.Discard, io.Discard
}

func resolverFixture(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	root := filepath.Join("testdata", "remote-action-resolver")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return files
}

func resolverFixtureCache(files map[string]string) *resolverCache {
	node := map[string]string{}
	for _, name := range []string{"action.yml", "pre.js", "main.js", "post.js"} {
		node["sub/"+name] = files["node/"+name]
	}
	return &resolverCache{files: map[string]map[string]string{
		"example/action@" + resolverSHA1:    node,
		"example/composite@" + resolverSHA1: {"action.yml": files["downloaded/action.yml"]},
		"example/deeper@" + resolverSHA1:    {"action.yml": files["deeper/action.yml"]},
	}}
}

func resolverRunStages(t *testing.T, s step) error {
	t.Helper()
	ctx := common.WithJobErrorContainer(context.Background())
	if err := s.pre()(ctx); err != nil {
		return err
	}
	err := s.main()(ctx)
	if postErr := s.post()(ctx); err == nil {
		err = postErr
	}
	if err == nil {
		err = common.JobError(ctx)
	}
	return err
}

func TestRemoteActionResolverRecursiveStages(t *testing.T) {
	// A15, A18–A21, A24–A25: real YAML, factory and composite stage executors.
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint("reject=", reject), func(t *testing.T) {
			files := resolverFixture(t)
			before := maps.Clone(files)
			cache := resolverFixtureCache(files)
			seen := []string{}
			sentinel := errors.New("nested lock rejected")
			config := &Config{ActionCache: cache, ResolveRemoteAction: func(_ context.Context, authored string) (string, error) {
				seen = append(seen, authored)
				if reject && authored == "example/action/sub@v1" {
					return "", sentinel
				}
				return strings.TrimSuffix(authored, "@v1") + "@" + resolverSHA1, nil
			}}
			rc := resolverContext(t, config)
			cm := newResolverContainer(t)
			rc.JobContainer = cm
			cm.files[path.Join(config.Workdir, "local/action.yml")] = files["local/action.yml"]
			workflow, err := model.ReadWorkflow(strings.NewReader(files["push.yml"]), false)
			if !assert.NoError(t, err) {
				return
			}
			rc.Run.Workflow = workflow
			for _, sm := range workflow.Jobs["job"].Steps {
				// Isolate each path from earlier sibling failures.
				rc.StepResults = map[string]*model.StepResult{}
				authored, label := sm.Uses, sm.String()
				s, err := (&stepFactoryImpl{}).newStep(sm, rc)
				if !assert.NoError(t, err) {
					return
				}
				err = resolverRunStages(t, s)
				if reject {
					assert.ErrorIs(t, err, sentinel)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, authored, sm.Uses)
				assert.Equal(t, label, sm.String())
				if remote, ok := s.(*stepActionRemote); ok && !reject {
					assert.Equal(t, resolverSHA1, remote.getGithubContext(context.Background()).ActionRef)
				}
			}
			assert.Equal(t, []string{"example/action/sub@v1", "example/action/sub@v1", "example/composite@v1", "example/deeper@v1", "example/action/sub@v1"}, seen)
			// Local composites load during main, so their children have no pre
			// stage. Top-level and downloaded children exercise all three stages.
			if reject {
				assert.Empty(t, cm.executions)
			} else {
				assert.Len(t, cm.executions, 8)
				stages := []string{"pre.js", "main.js", "post.js", "main.js", "post.js", "pre.js", "main.js", "post.js"}
				for i, e := range cm.executions {
					assert.Equal(t, resolverSHA1, e.ref)
					assert.Equal(t, []string{"node", path.Join("/var/run/act/actions/example-action-sub@v1/sub", stages[i])}, e.command)
				}
			}
			for _, f := range cache.fetches {
				assert.NotEqual(t, "v1", f.ref)
				if reject {
					assert.NotEqual(t, "example/action", f.dir)
				}
			}
			assert.Equal(t, before, resolverFixture(t))
			assert.Equal(t, before["local/action.yml"], cm.files[path.Join(config.Workdir, "local/action.yml")])
			assert.Equal(t, resolverFixtureCache(before).files, cache.files)
		})
	}
}

func TestRemoteActionResolverCheckoutExecution(t *testing.T) {
	// A28–A30: both transport configurations, copy/bind, identity/lock/rejection.
	original := stepActionRemoteNewCloneExecutor
	stepActionRemoteNewCloneExecutor = func(git.NewGitCloneExecutorInput) common.Executor { t.Fatal("checkout cloned"); return nil }
	t.Cleanup(func() { stepActionRemoteNewCloneExecutor = original })
	for _, legacy := range []bool{false, true} {
		for _, bind := range []bool{false, true} {
			for _, mode := range []string{"identity", "locked", "rejected"} {
				t.Run(fmt.Sprintf("%t/%t/%s", legacy, bind, mode), func(t *testing.T) {
					cache := &resolverCache{}
					calls := 0
					sentinel := errors.New("checkout rejected")
					config := &Config{ActionCache: cache, BindWorkdir: bind, ResolveRemoteAction: func(_ context.Context, s string) (string, error) {
						calls++
						switch mode {
						case "locked":
							return "actions/checkout@" + resolverSHA1, nil
						case "rejected":
							return "", sentinel
						default:
							return s, nil
						}
					}}
					if legacy {
						config.ActionCache = nil
					}
					s := resolverStep(t, config, "actions/checkout@v4")
					cm := newResolverContainer(t)
					s.RunContext.JobContainer = cm
					s.Step.With = map[string]string{"path": "prepared"}
					err := resolverRunStages(t, s)
					if mode == "rejected" {
						assert.ErrorIs(t, err, sentinel)
						assert.Empty(t, cm.copies)
					} else {
						assert.NoError(t, err)
						if bind {
							assert.Empty(t, cm.copies)
						} else {
							assert.Equal(t, []string{path.Join(config.Workdir, "prepared")}, cm.copies)
						}
					}
					assert.Equal(t, 1, calls)
					assert.Empty(t, cache.fetches)
					assert.Empty(t, cache.reads)
					assert.Empty(t, s.resolvedSha)
					assert.Nil(t, s.action)
					assert.Empty(t, cm.executions)
				})
			}
		}
	}
}

func TestRemoteActionResolverConditionFalse(t *testing.T) {
	// A26: preparation may resolve a skipped step; it does not certify execution.
	files := resolverFixture(t)
	cache := resolverFixtureCache(files)
	calls := 0
	s := resolverStep(t, &Config{ActionCache: cache, ResolveRemoteAction: func(_ context.Context, a string) (string, error) {
		calls++
		return strings.TrimSuffix(a, "@v1") + "@" + resolverSHA1, nil
	}}, "example/action/sub@v1")
	s.Step.If = yaml.Node{Value: "false"}
	s.env = map[string]string{}
	cm := newResolverContainer(t)
	s.RunContext.JobContainer = cm
	// main prepares before the condition, without running a pre hook.
	assert.NoError(t, s.main()(context.Background()))
	assert.Equal(t, 1, calls)
	assert.Len(t, cache.fetches, 1)
	assert.Empty(t, cm.executions)
	assert.Equal(t, model.StepStatusSkipped, s.RunContext.StepResults[s.Step.ID].Conclusion)
	assert.NoError(t, s.post()(context.Background()))
	assert.Equal(t, 1, calls)
}

func TestRemoteActionResolverParallelJobs(t *testing.T) {
	// A27: shared Config, independent job contexts, synchronized overlap without
	// product serialization or sleep-based scheduling.
	cache := &resolverCache{files: map[string]map[string]string{
		"example/action@" + resolverSHA1: resolverNodeFiles("allowed"),
		"example/action@" + resolverSHA2: resolverNodeFiles("other"),
	}}
	entered := make(chan string, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	config := &Config{Workdir: t.TempDir(), ActionCacheDir: t.TempDir(), GitHubInstance: "github.com", ActionCache: cache, ResolveRemoteAction: func(ctx context.Context, a string) (string, error) {
		entered <- a
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		ref := resolverSHA1
		if strings.Contains(a, "/other@") {
			ref = resolverSHA2
		}
		return strings.TrimSuffix(a, "@v1") + "@" + ref, nil
	}}
	steps := []*stepActionRemote{resolverStep(t, config, "example/action/allowed@v1"), resolverStep(t, config, "example/action/other@v1")}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range steps {
		s.RunContext.JobContainer = newResolverContainer(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, s.pre()(ctx))
			assert.NoError(t, s.main()(ctx))
			assert.NoError(t, s.post()(ctx))
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Error("resolver calls did not overlap")
		}
	}
	close(release)
	wg.Wait()
	assert.Equal(t, resolverSHA1, steps[0].remoteAction.Ref)
	assert.Equal(t, resolverSHA2, steps[1].remoteAction.Ref)
	assert.Len(t, cache.fetches, 2)
	assert.Equal(t, []resolverExecution{{[]string{"node", "/var/run/act/actions/example-action-allowed@v1/allowed/main.js"}, resolverSHA1}}, steps[0].RunContext.JobContainer.(*resolverContainer).executions)
}

func TestRemoteActionResolverNonRemoteSteps(t *testing.T) {
	// A22: ordinary local and Docker execution bypasses the action resolver.
	config := &Config{ResolveRemoteAction: func(context.Context, string) (string, error) { t.Fatal("non-remote selector resolved"); return "", nil }}
	rc := resolverContext(t, config)
	cm := newResolverContainer(t)
	rc.JobContainer = cm
	cm.files[path.Join(config.Workdir, "local/action.yml")] = "name: local\nruns:\n  using: composite\n  steps:\n    - id: skipped\n      if: false\n      shell: sh\n      run: exit 1\n"
	created := 0
	original := ContainerNewContainer
	t.Cleanup(func() { ContainerNewContainer = original })
	ContainerNewContainer = func(input *container.NewContainerInput) container.ExecutionsEnvironment {
		created++
		assert.Equal(t, "alpine:3", input.Image)
		return cm
	}
	noop := func(context.Context) error { return nil }
	cm.On("Pull", false).Return(noop).Once()
	cm.On("Remove").Return(noop).Twice()
	cm.On("Create", mock.Anything, mock.Anything).Return(noop).Once()
	cm.On("Start", true).Return(noop).Once()
	cm.On("Close").Return(noop).Once()
	for _, uses := range []string{"./local", "docker://alpine:3"} {
		s, err := (&stepFactoryImpl{}).newStep(&model.Step{ID: "action", Uses: uses}, rc)
		assert.NoError(t, err)
		switch uses {
		case "./local":
			assert.IsType(t, &stepActionLocal{}, s)
		default:
			assert.IsType(t, &stepDocker{}, s)
		}
		assert.NoError(t, resolverRunStages(t, s))
	}
	assert.Equal(t, 1, created)
	cm.AssertExpectations(t)
}

// Keep the mock's existing container interface in use; only transport/content
// operations above gain recording behavior for these connected runner tests.
var _ container.Container = (*resolverContainer)(nil)

func TestRemoteActionResolverReusableWorkflow(t *testing.T) {
	// Node discovery is optional; this fixture needs no external runtime.
	t.Setenv("PATH", t.TempDir())
	// A22–A23: the resource fetch bypasses the action hook, but a remote step
	// in that workflow resolves during ordinary runner execution on the host.
	files := resolverFixture(t)
	for _, remote := range []bool{false, true} {
		t.Run(fmt.Sprint("remote=", remote), func(t *testing.T) {
			cache := &resolverCache{files: map[string]map[string]string{
				"example/empty@" + resolverSHA1: {"action.yml": "name: composite\nruns:\n  using: composite\n  steps:\n    - id: skipped\n      if: false\n      shell: sh\n      run: exit 1\n"},
				"example/workflows@v1@v1":       {".github/workflows/reusable.yml": files[".github/workflows/reusable.yml"]},
			}}
			seen := []string{}
			config := &Config{ActionCache: cache, Platforms: map[string]string{"fixture": "-self-hosted"}, ResolveRemoteAction: func(_ context.Context, a string) (string, error) {
				seen = append(seen, a)
				return strings.TrimSuffix(a, "@v1") + "@" + resolverSHA1, nil
			}}
			rc := resolverContext(t, config)
			rc.Run.Job().Uses = "./.github/workflows/reusable.yml"
			p := filepath.Join(config.Workdir, ".github", "workflows")
			assert.NoError(t, os.MkdirAll(p, 0o755))
			assert.NoError(t, os.WriteFile(filepath.Join(p, "reusable.yml"), []byte(files[".github/workflows/reusable.yml"]), 0o600))
			ctx := common.WithJobErrorContainer(context.Background())
			var err error
			if remote {
				rc.Run.Job().Uses = "example/workflows/.github/workflows/reusable.yml@v1"
				err = newRemoteReusableWorkflowExecutor(rc)(ctx)
			} else {
				err = newLocalReusableWorkflowExecutor(rc)(ctx)
			}
			assert.NoError(t, err)
			assert.Equal(t, []string{"example/empty@v1"}, seen)
			if remote {
				assert.Equal(t, "v1", cache.fetches[0].ref)
				assert.Equal(t, "example/workflows@v1", cache.fetches[0].dir)
			}
			assert.Equal(t, resolverSHA1, cache.fetches[len(cache.fetches)-1].ref)
			after, readErr := os.ReadFile(filepath.Join(p, "reusable.yml"))
			assert.NoError(t, readErr)
			assert.Equal(t, files[".github/workflows/reusable.yml"], string(after))
		})
	}
}
