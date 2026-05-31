# Dependency Management

## No Path Dependencies for Cross-Repo References

Never use `path = "../../other-repo/crate"` in Cargo.toml to reference crates in other repositories. Path dependencies:
- Break CI (requires cloning sibling repos)
- Tie the project to a specific local directory layout
- Create implicit coupling between repos

**Instead, use git dependencies:**
```toml
# Correct: git dependency with branch
my-dep = { git = "https://github.com/shuozeli/other-repo.git", branch = "main" }

# Correct: git dependency with specific revision
my-dep = { git = "https://github.com/shuozeli/other-repo.git", rev = "abc123" }
```

Path dependencies are only acceptable **within the same workspace** (e.g., `path = "../grpc-core"` when both crates are in the same repo).
