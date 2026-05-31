# Shuozeli Codex Rules

This file is generated from the central Shuozeli rules repo.
Do not edit downstream copies by hand; update the source rule files here and regenerate.

## Common Rules

### common/ci-verification.md

Full rule file: `.codex/rules/shared/common/ci-verification.md`

- After every push to a public GitHub repo, verify GitHub Actions with `gh run list --limit 1` and `gh run watch <id> --exit-status`.
- Use the `gh` CLI for CI status, logs, and job results; do not ask the user to check a browser URL manually.
- For complex launches, validate through phased rollout such as dark launch and small-percentage rollout before full scale.
- Avoid circular dependencies.

### common/code-standards.md

Full rule file: `.codex/rules/shared/common/code-standards.md`

- Keep controllers/services isolated in their own modules. REST APIs must follow Google AIP standard methods and generate OpenAPI docs.
- Use exhaustive enum handling, avoid `any`, avoid vague `util` names, prefer composition, and fail fast instead of swallowing errors.
- Do not use emoji in code, comments, or commits. Encode invariants in types when possible.
- Test public behavior with real implementations or fakes; avoid mocks when practical.
- Wrap all database reads and writes in transactions. Prefer typed schema/protobuf structs over free-form JSON payloads.
- Use `pnpm` for Node, `uv` for Python, `@google/genai` for GenAI, generic `BullMQ Job<Data, Return, Name>`, no default config fallbacks, Guice-style constructor DI, `npx tsx` for TypeScript execution, strict typed async Python, Playwright exploration notes before crawler implementation, and structured event logs.
- Do not commit unless explicitly instructed. Ask concise clarification when ambiguity would make implementation risky.

### common/dependency-management.md

Full rule file: `.codex/rules/shared/common/dependency-management.md`

- Never use cross-repo Cargo `path` dependencies; they break CI and couple repos to local layout.
- Use Git dependencies with `branch` or pinned `rev` for cross-repo references.
- Path dependencies are acceptable only inside the same workspace.

### common/di-non-deterministic.md

Full rule file: `.codex/rules/shared/common/di-non-deterministic.md`

- Do not call non-deterministic operations directly in core business logic.
- Inject traits for time, ID generation, randomness, environment, and process identity so tests can control them.
- Provide production implementations and deterministic test implementations, then inject through constructors.
- Avoid globals and optional fallbacks that still call `Utc::now()`, `Uuid::new_v4()`, random, env, or process APIs internally.
- Database/audit timestamps and file mtimes can remain external-system truth where appropriate.

### common/large-refactor.md

Full rule file: `.codex/rules/shared/common/large-refactor.md`

- For broad edits in one file, read the full file, rewrite it coherently, then verify it compiles.
- Avoid fragile `sed` hacks or many line-by-line edits for large structural changes.

### common/rust-quality.md

Full rule file: `.codex/rules/shared/common/rust-quality.md`

- Do not suppress Clippy warnings; fix the code. The only allowed exception is `#[allow(dead_code, unused_imports)]` on `mod common;` in integration tests.
- Avoid redundant casts, implement standard traits instead of lookalike inherent methods, remove unused imports, and remove dead code instead of allowing it.

### common/spanner-schemas.md

Full rule file: `.codex/rules/shared/common/spanner-schemas.md`

- Use primary keys for every GoogleSQL table and avoid hotspotting from monotonically increasing keys.
- Use interleaving for parent-child locality when the child primary key begins with the parent key; use foreign keys for logical integrity without co-location.
- Use named schemas to organize objects and support fine-grained access control.

### common/testing-patterns.md

Full rule file: `.codex/rules/shared/common/testing-patterns.md`

- Use Arrange, Act, Assert structure in tests.
- Keep one Act per test, use minimal setup, write behavior-focused names, mark phases with comments, keep tests isolated and deterministic, and always assert outcomes.

## API Rules

### api/aipdev.md

Full rule file: `.codex/rules/shared/api/aipdev.md`

- Design APIs as resource hierarchies with plural collections and `name` string resource identifiers.
- Implement standard Get/List/Create/Update/Delete methods with AIP request and response shapes.
- Use field masks for PATCH updates, opaque pagination tokens, CEL-style filters, stable sorting, and long-running operations for work over roughly 10 seconds.
- Use singleton resources where appropriate, version APIs with `v1`/`v1beta`/`v2`, avoid breaking changes inside a major version, and deprecate before removal.

### api/docsguide.md

Full rule file: `.codex/rules/shared/api/docsguide.md`

- After any code change, check `docs/MANIFEST.md` when a project has `docs/` and update docs whose triggers match.
- Every agent-created or updated markdown doc needs `<!-- agent-updated: YYYY-MM-DDTHH:MM:SSZ -->` immediately after frontmatter.
- Maintain `MANIFEST.md` when adding or removing docs. Common docs include README, tasks, API, CHANGELOG, architecture, and ADRs.

## Workflow Rules

### workflows/agent-driven-learning.md

Full rule file: `.codex/rules/shared/workflows/agent-driven-learning.md`

- For learning a project, use a structured pipeline: explore, plan, set up or reuse persistent tracking, execute tasks, and capture knowledge.
- Prefer persistent SQLite-backed task tracking such as `beu`, with goals, milestones, tasks, progress dashboard, and resumable lessons.
- Avoid shallow summaries, human-driven sequencing, excessive task granularity, and marking work done without teaching a distilled lesson.

### workflows/beu-workflow.md

Full rule file: `.codex/rules/shared/workflows/beu-workflow.md`

- When a project has beu memory configured, use the resume command at start and the pause command with a checkpoint before stopping.
- Use `beu task`, `state`, `debug`, `artifact`, `journal`, `progress`, `events`, `health`, and `check` to track work, decisions, blockers, investigations, deliverables, and compliance.
- Use `-p <project>` for project scoping inside shared `.beu` databases.

### workflows/session-management.md

Full rule file: `.codex/rules/shared/workflows/session-management.md`

- Treat each Codex conversation as a session that should be resumable and attributable.
- At session start, check recent session files if present, inspect `git status`, and attribute existing changes to prior sessions when possible.
- During work, track changed files and deployments. On explicit wrap-up/save, write a concise `.codex/sessions/YYYY-MM-DDTHH-MM-<slug>.md` summary.
- Session summaries are append-only and do not replace commits.
