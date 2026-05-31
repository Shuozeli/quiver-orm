# Large File Refactors

When a refactor touches many places in a file (e.g., adding a field to a struct used in 20 constructions), do not use sed hacks or line-by-line edits. Instead:

1. Read the full file
2. Rewrite it completely using the Write tool
3. Verify it compiles

This is faster, less error-prone, and produces a clean result. Do not be afraid of rewriting a 400-line file to add a field everywhere.
