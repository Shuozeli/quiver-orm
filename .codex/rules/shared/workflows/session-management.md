# Codex Session Management

Every Codex conversation in this project is a **session**. Sessions must be tracked so the user can understand what happened across conversations.

## Session Lifecycle

**On session start:**
1. Read the last 3 session files from `.codex/sessions/` to understand recent context.
2. Check for uncommitted changes with `git status` and attribute them to prior sessions using the session log.
3. If resuming work from a prior session, say so explicitly.

**During session:**
- Track what files are created/modified/deployed.
- Track what was deployed to production or other infrastructure.

**On session end (when the user says goodbye, wraps up, or asks to save):**
1. Write a session summary to `.codex/sessions/YYYY-MM-DDTHH-MM-<slug>.md`.
2. The slug should be 2-4 words describing the primary work (e.g., `digest-curator-panic-fix`).

## Session File Format

```markdown
# Session: <slug>
Date: YYYY-MM-DDTHH:MM UTC
Duration: ~Xh (approximate)

## Summary
1-3 sentence overview of what was accomplished.

## Work Done
- [ ] or [x] for each task (checked = completed, unchecked = pending/blocked)

## Files Changed
Group by project area:
- `project/path/file.rs` -- what changed

## Deployed
- What was deployed to production, if anything

## Uncommitted
- Files that were changed but not committed (attribute to THIS session)

## Blocked / Next Steps
- What's pending for a future session
```

## Rules
- Session files are append-only. Never modify a previous session's file.
- If multiple topics were covered, use the primary topic as the slug.
- Session files live in `.codex/sessions/` (gitignored -- these are local working state, not repo history).
- Keep session files concise. This is a log, not documentation.
- The session file is NOT a substitute for committing. It tracks what happened between commits.
