# Documentation Guide

## Doc Update Protocol

**After ANY code change**, check if the affected project has a `docs/` directory. If it does:

1. **Read `docs/MANIFEST.md`** -- it lists all doc files with "Covers" and "Update When" columns.
2. **Match your changes** against the "Update When" triggers in the manifest.
3. **If a trigger matches**, update that doc file and bump its `<!-- agent-updated: YYYY-MM-DDTHH:MM:SSZ -->` timestamp.
4. **If you add a new doc file**, add a row to `MANIFEST.md`.
5. **If you remove a doc file**, remove its row from `MANIFEST.md`.

**Quick reference -- common triggers:**

| You changed... | Update this doc |
|----------------|----------------|
| Env vars, ports, database | README.md |
| gRPC proto, REST endpoints | API.md |
| Module structure, data flow | architecture.md |
| Completed a task, found a bug | tasks.md |
| Any significant change | CHANGELOG.md |

## Doc Freshness Rule

Every markdown document created or updated by the agent **MUST** include a freshness line at the very top of the file (after any YAML frontmatter), in exactly this format:

```
<!-- agent-updated: YYYY-MM-DDTHH:MM:SSZ -->
```

* The timestamp **MUST** use UTC (Z suffix) and ISO 8601 format.
* The agent **MUST** update this line every time it writes to or rewrites the document.
* If no `agent-updated` line is present, the document is considered **unreviewed**.

## Doc File Formats

#### 1. `MANIFEST.md` (Doc Index)

* **Purpose:** Machine-readable index of all doc files. Tells agents what docs exist and when to update them.
* **Format:** Markdown table:

```
# Doc Manifest

| File | Covers | Update When |
|------|--------|-------------|
| README.md | Service overview, env vars, ports | Architecture changes, new env vars |
| tasks.md | Roadmap, pending work, known bugs | Task completion, new bugs |
| API.md | gRPC/REST endpoints, schemas | Proto changes, new endpoints |
```

#### 2. `README.md` (Entry Point)

* **Purpose:** "What is this?" and "How do I run it?"
* **Sections:** Title, description, dependencies, quick start commands.

#### 3. `tasks.md` (Roadmap & Status)

* **Purpose:** Tracks to-dos and technical debt.
* **Format:** `- [x] Completed Item (YYYY-MM-DD)` / `- [ ] Pending Item` / `- [ ] **Bug**: Description`

#### 4. `API.md` (Interface Reference)

* **Purpose:** Public interfaces, endpoints, exported functions.
* **Sections:** Endpoint signature, inputs, outputs, error codes.

#### 5. `CHANGELOG.md` (History)

* **Purpose:** Chronological log of changes.
* **Format:** `## [Version] - YYYY-MM-DD` with `### Added` / `### Changed` / `### Fixed`

#### 6. `architecture.md` (System Design)

* **Purpose:** High-level design and data flow.
* **Format:** Diagrams (Mermaid), key components, data flow.

#### 7. `ADR/` (Architecture Decision Records)

* **Purpose:** Numbered files recording architectural decisions.
* **Format:** Title, Status (Proposed/Accepted/Deprecated), Context, Decision, Consequences.
* **Update When:** Never (historical records).
