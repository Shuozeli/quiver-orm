#!/usr/bin/env bash
# Prevent committing files that should be gitignored.
# This catches cases where someone uses `git add -f` to bypass .gitignore.

set -e

BLOCKED_FILES="CLAUDE.md skills-lock.json"
BLOCKED_DIRS=".claude .beu docs/internal skills"

staged=$(git diff --cached --name-only 2>/dev/null || true)
exit_code=0

for f in $BLOCKED_FILES; do
    if echo "$staged" | grep -q "^${f}$"; then
        echo "BLOCKED: ${f} should not be committed (listed in .gitignore)"
        exit_code=1
    fi
done

for d in $BLOCKED_DIRS; do
    if echo "$staged" | grep -q "^${d}/"; then
        echo "BLOCKED: ${d}/ should not be committed (listed in .gitignore)"
        exit_code=1
    fi
done

exit $exit_code
