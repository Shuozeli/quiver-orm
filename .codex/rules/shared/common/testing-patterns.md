# Testing: Arrange-Act-Assert Pattern

## The Pattern

Arrange-Act-Assert (AAA) is a simple but powerful pattern for structuring tests. It forces tests to focus on independent, individual behaviors.

### Structure

1. **Arrange** — Set up inputs and targets. Create objects, prep data, configure mocks.
2. **Act** — Exercise the behavior under test. Call the function/method/API.
3. **Assert** — Verify expected outcomes. Check return values, side effects, state changes.

### Example (Python/pytest)

```python
def test_abs_for_negative_number():
    # Arrange
    negative = -5

    # Act
    answer = abs(negative)

    # Assert
    assert answer == 5
```

### Example (Rust)

```rust
#[tokio::test]
async fn test_create_session() {
    // Arrange
    let store = SqliteEventStore::in_memory().unwrap();
    let config = create_test_config("test");

    // Act
    let session_id = store.create_session(&config).await.unwrap();

    // Assert
    assert_eq!(session_id.as_str(), "test");
    let session = store.get_session(session_id).await.unwrap().unwrap();
    assert_eq!(session.status, SessionStatus::Initializing);
}
```

## Why AAA?

- **Focused tests** — Each test verifies one behavior
- **Readable** — Comments mark each phase, reviewers can quickly understand test intent
- **Debuggable** — Failures clearly indicate which phase failed (setup vs. action vs. verification)
- **Maintainable** — Clear separation makes it easy to update tests when behavior changes

## Guidelines

1. **One Act per test** — Multiple Act steps usually mean multiple tests
2. **Minimal Arrange** — Only set up what's needed for the specific behavior
3. **Descriptive names** — Test names should describe the behavior, not the implementation: `test_session_returns_none_for_nonexistent_id` not `test_get_session`
4. **Comments** — Use `// Arrange`, `// Act`, `// Assert` comments to mark phases
5. **Isolated** — Tests must not depend on each other or execution order
6. **Deterministic** — Same result every time; no randomness or time dependencies

## Anti-Patterns to Avoid

❌ No Act in Assert:
```rust
// BAD - assertion mixes with action
assert!(store.create_session(&config).await.is_ok());
```

❌ Multiple Acts without separate assertions:
```rust
// BAD - should be two tests
store.create_session(&config).await;
store.create_session(&config).await; // second act?
```

❌ Arrange too much:
```rust
// BAD - setting up entire database for one test
let db = setup_full_database_with_10_sessions();
```

❌ No assertions:
```rust
// BAD - test without verification
store.create_session(&config).await;
```

## Related

- Given-When-Then (BDD) is the same pattern under another name
- See also: `~/.claude/rules/feedback_coding_habits.md` for test helper patterns
