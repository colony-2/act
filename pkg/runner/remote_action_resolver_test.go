package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/common/git"
	"github.com/nektos/act/pkg/model"
	"github.com/stretchr/testify/assert"
)

const resolverSHA1 = "1111111111111111111111111111111111111111"
const resolverSHA2 = "2222222222222222222222222222222222222222"

type resolverFetch struct{ dir, url, ref, token, handle string }
type resolverRead struct{ dir, handle, prefix string }
type resolverCache struct {
	mu      sync.Mutex
	fetches []resolverFetch
	reads   []resolverRead
	// Immutable once preparation begins; selectors at different refs can serve
	// different content even when their repository and subpath coincide.
	files   map[string]map[string]string
	handles map[string]map[string]string
}

var _ ActionCache = (*resolverCache)(nil) // Existing two-method API, A38.

func (c *resolverCache) Fetch(_ context.Context, dir, url, ref, token string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	handle := fmt.Sprintf("fetch-handle-%d", len(c.fetches))
	c.fetches = append(c.fetches, resolverFetch{dir, url, ref, token, handle})
	if c.handles == nil {
		c.handles = map[string]map[string]string{}
	}
	files, ok := c.files[dir+"@"+ref]
	if !ok {
		return "", fmt.Errorf("unexpected fetch %s@%s", dir, ref)
	}
	c.handles[handle] = files
	return handle, nil
}

func resolverTar(files map[string]string, prefix string) (io.ReadCloser, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	found := false
	for name, body := range files {
		if prefix != "" && name != prefix && !strings.HasPrefix(name, prefix+"/") {
			continue
		}
		found = true
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			return nil, err
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, os.ErrNotExist
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

func (c *resolverCache) GetTarArchive(_ context.Context, dir, handle, prefix string) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, resolverRead{dir, handle, prefix})
	files, ok := c.handles[handle]
	if !ok {
		return nil, fmt.Errorf("unknown fetch handle %q", handle)
	}
	return resolverTar(files, prefix)
}

func resolverContext(t *testing.T, config *Config) *RunContext {
	t.Helper()
	if config.Workdir == "" {
		config.Workdir = t.TempDir()
	}
	if config.ActionCacheDir == "" {
		config.ActionCacheDir = t.TempDir()
	}
	if config.GitHubInstance == "" {
		config.GitHubInstance = "github.com"
	}
	rc := &RunContext{Config: config, Run: &model.Run{JobID: "job", Workflow: &model.Workflow{Jobs: map[string]*model.Job{"job": {}}}}, StepResults: map[string]*model.StepResult{}, Env: map[string]string{}, EventJSON: "{}", nodeToolFullPath: "node"}
	rc.ExprEval = rc.NewExpressionEvaluator(context.Background())
	return rc
}

func resolverStep(t *testing.T, config *Config, uses string) *stepActionRemote {
	t.Helper()
	s, err := (&stepFactoryImpl{}).newStep(&model.Step{ID: "action", Name: "Authored label", Uses: uses}, resolverContext(t, config))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return s.(*stepActionRemote)
}

func resolverNodeFiles(subpath string) map[string]string {
	return map[string]string{path.Join(subpath, "action.yml"): "name: node\nruns:\n  using: node20\n  main: main.js\n", path.Join(subpath, "main.js"): "// fixture\n"}
}

func TestRemoteActionResolverExactSelectors(t *testing.T) {
	// A01–A03, A09, A17: exact bytes and per-instance handles, not ref aliases.
	cache := &resolverCache{files: map[string]map[string]string{"Example/Action@" + resolverSHA1: resolverNodeFiles("allowed"), "Example/Action@" + resolverSHA2: resolverNodeFiles("other")}}
	seen := []string{}
	config := &Config{ActionCache: cache, ResolveRemoteAction: func(_ context.Context, authored string) (string, error) {
		seen = append(seen, authored)
		if strings.Contains(authored, "/allowed@") {
			return "Example/Action/allowed@" + resolverSHA1, nil
		}
		return "Example/Action/other@" + resolverSHA2, nil
	}}
	for _, subpath := range []string{"allowed", "other"} {
		authored := "Example/Action/" + subpath + "@v1"
		s := resolverStep(t, config, authored)
		for range 2 {
			assert.NoError(t, s.prepareActionExecutor()(context.Background()))
		}
		assert.Equal(t, authored, s.Step.Uses)
		assert.Equal(t, "Authored label", s.Step.Name)
		assert.Equal(t, subpath, s.remoteAction.Path)
		assert.NotEqual(t, s.remoteAction.Ref, s.resolvedSha)
		assert.Equal(t, s.remoteAction.Ref, s.getGithubContext(context.Background()).ActionRef)
		// Exercise the production whole-action archive path as well as the manifest.
		cm := newResolverContainer(t)
		s.RunContext.JobContainer = cm
		assert.NoError(t, maybeCopyToActionDir(context.Background(), s, "unused", subpath, "/actions/"+subpath))
	}
	assert.Equal(t, []string{"Example/Action/allowed@v1", "Example/Action/other@v1"}, seen)
	assert.Len(t, cache.fetches, 2)
	assert.Equal(t, []resolverRead{{"Example/Action", "fetch-handle-0", "allowed/action.yml"}, {"Example/Action", "fetch-handle-0", ""}, {"Example/Action", "fetch-handle-1", "other/action.yml"}, {"Example/Action", "fetch-handle-1", ""}}, cache.reads)
	assert.Equal(t, resolverSHA1, cache.fetches[0].ref)
	assert.Equal(t, resolverSHA2, cache.fetches[1].ref)
	// A separate instance of the same selector resolves again.
	s := resolverStep(t, config, "Example/Action/allowed@v1")
	assert.NoError(t, s.prepareActionExecutor()(context.Background()))
	assert.Len(t, seen, 3)
}

func TestRemoteActionResolverFailures(t *testing.T) {
	sentinel := errors.New("missing exact lock")
	tests := []struct {
		name, authored, result string
		callbackErr            error
		cancelled, during      bool
		wantCalls              int
	}{
		{name: "reject allowed", authored: "example/action/allowed@v1", callbackErr: sentinel, wantCalls: 1},
		{name: "reject other", authored: "example/action/other@v1", callbackErr: sentinel, wantCalls: 1},
		{name: "missing ref", authored: "example/action"},
		{name: "escaping authored", authored: "example/action/../../out@v1"},
		{name: "empty", result: "", wantCalls: 1},
		{name: "malformed", result: "invalid", wantCalls: 1},
		{name: "escaping", result: "example/action/../../out@v1", wantCalls: 1},
		{name: "local", result: "./action@v1", wantCalls: 1},
		{name: "docker", result: "docker://alpine:3", wantCalls: 1},
		{name: "owner", result: "other/action/sub@v1", wantCalls: 1},
		{name: "repo", result: "example/other/sub@v1", wantCalls: 1},
		{name: "subpath", result: "example/action/other@v1", wantCalls: 1},
		{name: "owner case", result: "Example/action/sub@v1", wantCalls: 1},
		{name: "repo case", result: "example/Action/sub@v1", wantCalls: 1},
		{name: "path case", result: "example/action/Sub@v1", wantCalls: 1},
		{name: "path cleanup", authored: "example/action/sub/./child@v1", result: "example/action/sub/child@v2", wantCalls: 1},
		{name: "already cancelled", cancelled: true},
		{name: "cancel during callback", during: true, result: "example/action/sub@v2", wantCalls: 1},
	}
	// A04–A08, A10–A12, A16: exercise both transport routes and remember errors.
	original := stepActionRemoteNewCloneExecutor
	stepActionRemoteNewCloneExecutor = func(_ git.NewGitCloneExecutorInput) common.Executor {
		t.Fatal("clone constructed after rejection")
		return nil
	}
	t.Cleanup(func() { stepActionRemoteNewCloneExecutor = original })
	for _, legacy := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("%t/%s", legacy, tt.name), func(t *testing.T) {
				if tt.authored == "" {
					tt.authored = "example/action/sub@v1"
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tt.cancelled {
					cancel()
				}
				calls := 0
				cache := &resolverCache{}
				config := &Config{ActionCache: cache, ResolveRemoteAction: func(_ context.Context, authored string) (string, error) {
					calls++
					assert.Equal(t, tt.authored, authored)
					if tt.during {
						cancel()
					}
					return tt.result, tt.callbackErr
				}}
				if legacy {
					config.ActionCache = nil
				}
				s := resolverStep(t, config, tt.authored)
				err := s.prepareActionExecutor()(ctx)
				if !assert.Error(t, err) {
					return
				}
				assert.Contains(t, err.Error(), tt.authored)
				if tt.callbackErr != nil {
					assert.ErrorIs(t, err, sentinel)
				}
				if tt.cancelled || tt.during {
					assert.ErrorIs(t, err, context.Canceled)
				}
				assert.Same(t, err, s.prepareActionExecutor()(context.Background()))
				assert.Equal(t, tt.wantCalls, calls)
				assert.Empty(t, cache.fetches)
				assert.Empty(t, cache.reads)
				assert.Nil(t, s.remoteAction)
				assert.Empty(t, s.resolvedSha)
				assert.Equal(t, tt.authored, s.Step.Uses)
			})
		}
	}
}

func TestRemoteActionResolverRefAndRoutingCompatibility(t *testing.T) {
	// A09, A13–A14, A34, A36: nil and opted-in hooks on both transports.
	for _, legacy := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			for _, replace := range []bool{false, true} {
				for _, ref := range []string{"v1", resolverSHA1} {
					t.Run(fmt.Sprintf("legacy=%t/resolver=%t/replace=%t/%s", legacy, enabled, replace, ref), func(t *testing.T) {
						effective := ref
						calls := 0
						config := &Config{GitHubInstance: "enterprise.example", Token: "enterprise-token"}
						if enabled {
							effective = "refs/tags/locked"
							config.ResolveRemoteAction = func(_ context.Context, s string) (string, error) {
								calls++
								return strings.TrimSuffix(s, "@"+ref) + "@" + effective, nil
							}
						}
						url, token := "https://enterprise.example/example/action", "enterprise-token"
						if replace {
							config.ReplaceGheActionWithGithubCom = []string{"EXAMPLE/ACTION"}
							config.ReplaceGheActionTokenWithGithubCom = "replacement-token"
							url = "https://github.com/example/action"
							token = "replacement-token"
						}
						cache := &resolverCache{files: map[string]map[string]string{"example/action@" + effective: resolverNodeFiles("sub")}}
						config.ActionCache = cache
						s := resolverStep(t, config, "example/action/sub@"+ref)
						if legacy {
							config.ActionCache = nil
							original := stepActionRemoteNewCloneExecutor
							t.Cleanup(func() { stepActionRemoteNewCloneExecutor = original })
							stepActionRemoteNewCloneExecutor = func(input git.NewGitCloneExecutorInput) common.Executor {
								assert.Equal(t, url, input.URL)
								assert.Equal(t, effective, input.Ref)
								assert.Equal(t, token, input.Token)
								assert.Equal(t, filepath.Join(config.ActionCacheDir, safeFilename(s.Step.Uses)), input.Dir)
								return func(_ context.Context) error {
									assert.NoError(t, os.MkdirAll(filepath.Join(input.Dir, "sub"), 0o755))
									return os.WriteFile(filepath.Join(input.Dir, "sub", "action.yml"), []byte(resolverNodeFiles("sub")["sub/action.yml"]), 0o600)
								}
							}
						}
						assert.NoError(t, s.prepareActionExecutor()(context.Background()))
						if !legacy {
							assert.Equal(t, resolverFetch{"example/action", url, effective, token, "fetch-handle-0"}, cache.fetches[0])
						}
						assert.Equal(t, effective, s.remoteAction.Ref)
						if enabled {
							assert.Equal(t, 1, calls)
						} else {
							assert.Zero(t, calls)
						}
					})
				}
			}
		}
	}
	// Identity and a non-SHA replacement are both supported.
	for _, result := range []string{"example/action@v1", "example/action@branch"} {
		cache := &resolverCache{files: map[string]map[string]string{"example/action@" + newRemoteAction(result).Ref: resolverNodeFiles("")}}
		s := resolverStep(t, &Config{ActionCache: cache, ResolveRemoteAction: func(context.Context, string) (string, error) { return result, nil }}, "example/action@v1")
		assert.NoError(t, s.prepareActionExecutor()(context.Background()))
	}
}

func TestRemoteActionResolverCheckoutPreparation(t *testing.T) {
	// A31–A33, A35: shortcut decisions use authored checkout inputs.
	for _, mode := range []string{"nil", "identity", "locked"} {
		for _, input := range []string{"skip", "no-skip", "repository", "ref"} {
			for _, legacy := range []bool{false, true} {
				t.Run(mode+"/"+input+fmt.Sprint(legacy), func(t *testing.T) {
					ref := "v4"
					calls := 0
					config := &Config{}
					if mode != "nil" {
						config.ResolveRemoteAction = func(_ context.Context, s string) (string, error) {
							calls++
							if mode == "locked" {
								return "actions/checkout@" + resolverSHA1, nil
							}
							return s, nil
						}
					}
					if mode == "locked" {
						ref = resolverSHA1
					}
					cache := &resolverCache{files: map[string]map[string]string{"actions/checkout@" + ref: resolverNodeFiles("")}}
					config.ActionCache = cache
					s := resolverStep(t, config, "actions/checkout@v4")
					switch input {
					case "no-skip":
						config.NoSkipCheckout = true
					case "repository":
						s.Step.With = map[string]string{"repository": "other/repo"}
					case "ref":
						s.Step.With = map[string]string{"ref": "other-ref"}
					}
					clones := 0
					if legacy {
						config.ActionCache = nil
						original := stepActionRemoteNewCloneExecutor
						t.Cleanup(func() { stepActionRemoteNewCloneExecutor = original })
						stepActionRemoteNewCloneExecutor = func(i git.NewGitCloneExecutorInput) common.Executor {
							clones++
							assert.Equal(t, ref, i.Ref)
							return func(context.Context) error { return nil }
						}
						s.readAction = func(context.Context, *model.Step, string, string, actionYamlReader, fileWriter) (*model.Action, error) {
							return &model.Action{}, nil
						}
					}
					for range 2 {
						assert.NoError(t, s.prepareActionExecutor()(context.Background()))
					}
					if input == "skip" {
						assert.Empty(t, cache.fetches)
						assert.Zero(t, clones)
						assert.Nil(t, s.action)
						assert.Empty(t, s.resolvedSha)
					} else if legacy {
						assert.Equal(t, 1, clones)
					} else {
						assert.Len(t, cache.fetches, 1)
					}
					if mode != "nil" {
						assert.Equal(t, 1, calls)
					}
				})
			}
		}
	}
}

func TestRemoteActionResolverConfigJSON(t *testing.T) {
	// A37–A38: keyed configuration and both nil/non-nil JSON remain compatible.
	for _, r := range []RemoteActionResolver{nil, func(_ context.Context, s string) (string, error) { return s, nil }} {
		b, err := json.Marshal(&Config{Actor: "actor", ResolveRemoteAction: r})
		assert.NoError(t, err)
		assert.NotContains(t, string(b), "ResolveRemoteAction")
		var config Config
		assert.NoError(t, json.Unmarshal(b, &config))
		assert.Equal(t, "actor", config.Actor)
		assert.Nil(t, config.ResolveRemoteAction)
	}
}
