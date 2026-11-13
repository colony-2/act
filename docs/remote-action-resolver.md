# Resolve remote action refs in an embedded runner

An embedding application can resolve exact authored action selectors before act
skips a prepared-source checkout or starts action transport:

```go
type RemoteActionResolver func(ctx context.Context, authoredSelector string) (effectiveSelector string, err error)
```

Set `runner.Config.ResolveRemoteAction` to enable it. A nil callback preserves
existing behavior. The callback receives the exact `owner/repository[/subpath]@ref`
from the authored step, including case and subpath spelling. Act validates the
original selector first; the callback cannot repair invalid input.

The returned selector must be valid and have the same owner, repository and
subpath, compared byte for byte. Only the ref may change. Returning the original
selector succeeds; returning an empty string without an error fails. Act accepts
its existing ref forms, including tags and branches. A consumer that requires
full commit SHAs must enforce that policy itself.

For example, install a stable map before running the plan:

```go
locks := map[string]string{
    "example/action/allowed@v1": "example/action/allowed@1111111111111111111111111111111111111111",
    "example/action/other@v1":   "example/action/other@2222222222222222222222222222222222222222",
}
config := &runner.Config{
    // Set the remaining runner options for your application.
    ResolveRemoteAction: func(ctx context.Context, authored string) (string, error) {
        if err := ctx.Err(); err != nil {
            return "", err
        }
        if effective, ok := locks[authored]; ok {
            return effective, nil
        }
        // Explicitly permit identity resolution for prepared-source checkout.
        // Validate checkout inputs separately before executing the workflow.
        if authored == "actions/checkout@v4" {
            return authored, nil
        }
        return "", fmt.Errorf("missing exact action lock for %q", authored)
    },
}
```

This example requires `context`, `fmt` and `github.com/nektos/act/pkg/runner`.
Keep the map immutable during execution. Separate jobs may call the shared
callback concurrently; synchronize any mutable recording state in the callback.
Act does not serialize jobs around resolution or retry the callback.

Each constructed remote step remembers one resolution result or error across
preparation and pre/main/post execution. Repeated selectors in separate instances
resolve independently. Errors include the authored selector and retain their
identity for `errors.Is`. Cancellation before or during resolution prevents
subsequent action transport and is remembered on that instance.

Resolution leaves authored models and source files unchanged. Labels and action
directories retain the authored selector. Cache and legacy clone requests use the
effective ref, as do runtime `github.action_ref` and `GITHUB_ACTION_REF`. Archive
reads use the handle returned by that instance's cache fetch.

The same callback reaches remote children of local and downloaded composites,
including deeper composites, and remote action steps inside reusable workflows.
Local action selectors, Docker URLs and job-level reusable-workflow resources do
not themselves invoke it. Reusable-workflow fetching retains its separate path.

Resolution occurs before the checkout shortcut. A valid checkout lock or explicit
identity result can preserve copying or binding the prepared workspace without
fetching the action. A rejected result fails before that shortcut. Existing
`NoSkipCheckout` and authored checkout repository/ref input checks still apply.
The callback sees no checkout inputs: callers must separately validate supported
repository, ref, path and history requests, including inside downloaded composites.

Preparation can precede condition evaluation; checkout can skip transport.
Callback invocation therefore establishes resolution only, not a fetched SHA or
execution receipt. The callback is not a general network sandbox.

`ActionCache` keeps its existing two methods. Ordinary Config JSON omits the
function field. Existing keyed Config literals remain compatible; external
unkeyed literals may need updating for the new field. This extension adds no CLI
flag, lock-file format, automatic tag resolution or lock-map API.
