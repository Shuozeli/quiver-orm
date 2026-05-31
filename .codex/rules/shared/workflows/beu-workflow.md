# beu -- Session Memory for AI Agents

You have access to `beu`, a persistent session-memory CLI. Use it to track work
across sessions and survive conversation compaction.

## When to Use beu

- Any project with a project-memory directory should use beu for tracking
- At session start: run the resume command for beu to restore context
- At session end or before pausing: run the pause command for beu with a checkpoint message
- When starting multi-step work: create tasks with `beu task add`
- When making architectural decisions: record with `beu state set --category decision`
- When hitting blockers: record with `beu state set --category blocker`
- When debugging: use `beu debug open` to track investigations

## Session Protocol

```bash
beu <resume-command>    # Check checkpoint, blockers, focus items
beu journal open        # Start interaction ledger
# ... do work ...
beu check               # Verify docs are up to date (if configured)
beu <pause-command> "state desc"  # Save checkpoint before stopping
```

## Module Quick Reference

| Module | Purpose | Key Commands |
|--------|---------|--------------|
| journal | Interaction ledger | `open`, `log`, `note`, `summary`, `close` |
| artifact | Deliverable tracking | `add`, `status`, `list`, `show`, `changelog` |
| task | Work items + sprint view | `add`, `list`, `update`, `done`, `sprint` |
| state | Persistent memory | `set --category <C> <key> <value>`, `get`, `list`, `remove` |
| idea | Lightweight capture | `add`, `list`, `done`, `archive` |
| debug | Investigation tracking | `open`, `log`, `symptom`, `cause`, `resolve` |

## Common Patterns

### Record a decision
```bash
beu state set --category decision "db-choice" "SQLite for simplicity"
```

### Track a blocker
```bash
beu state set --category blocker "ci-flaky" "Tests timeout on CI"
```

### Task workflow
```bash
beu task add "implement auth" --priority high --tag feature
beu task sprint                    # View sprint board
beu task update 1 --status in-progress
beu task done 1
```

### Debug investigation
```bash
beu debug open "connection timeout"
beu debug symptom conn-timeout "errors after 30s idle"
beu debug log conn-timeout "pool max_idle is 10s"
beu debug cause conn-timeout "idle timeout shorter than keepalive"
beu debug resolve conn-timeout
```

### Artifact tracking
```bash
beu artifact add design --type doc
beu artifact status design in-progress
beu artifact changelog design "added auth section"
```

## System Commands

```bash
beu status              # Project overview + enabled modules
beu progress            # Cross-module summary
beu events              # Recent event log
beu health              # Database integrity check
beu check               # Compliance gate (required docs)
beu export --all        # Export all data as JSON
```

## Configuration

The project-memory config file controls which modules are active:

```yaml
modules:
  journal: true
  artifact: true
  task: true
  state: true
  idea: true
  debug: true
```

## Project Scoping

Use `-p <project>` to scope commands to a specific project within a shared
`.beu` database. Without the flag, the default project is used.

## Global Flags

- `--beu-dir <PATH>`: Override .beu directory location
- `-p, --project <ID>`: Project scope
- `-v, --verbose`: Detailed output
- `-q, --quiet`: Suppress non-essential output
