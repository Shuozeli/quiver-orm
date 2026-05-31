# CI Verification

1. **Verify CI After Push:** After every `git push` to a public GitHub repository, verify the GitHub Actions CI run passes before considering the task complete. Run `gh run list --limit 1` to find the run ID, then `gh run watch <id> --exit-status` to wait for results. If any job fails, fix the issue, commit, push again, and re-verify.

2. **Always Use `gh` for GitHub CI:** When reading GitHub CI status, logs, or job results, always use the `gh` CLI (`gh run list`, `gh run view`, `gh run watch`). Never use the browser or suggest the user check a URL manually -- use `gh` to read and report the results directly.

3. **Phased Launches:** When implementing a complex system, use different phases: dark launch, 1% launch. Always have the system run a partial flow first to validate correctness before scaling to the full dataset.

4. **Circular Dependencies:** Strictly avoid circular dependencies to prevent architectural degradation and tight coupling.
