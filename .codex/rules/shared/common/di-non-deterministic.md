# Decoupling Non-Deterministic Operations via Dependency Injection

## Principle

Non-deterministic operations (time, random IDs, system clocks) must never be called directly in core business logic. They must be injected as dependencies, enabling deterministic testing and isolation from system calls.

## Why

- **Testability**: Direct calls to `Utc::now()` or `uuid::Uuid::new_v4()` make tests non-deterministic and hard to reproduce
- **Isolation**: Business logic should not depend on system state
- **Replayability**: Tests must be able to control time, IDs, and other non-deterministic inputs
- **Coverage**: External calls cannot be tracked by coverage tools, making 100% coverage impossible

## The Pattern

### 1. Define a Trait for the Non-Deterministic Operation

```rust
// Good: Trait-based abstraction
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub trait IdGenerator: Send + Sync {
    fn generate_id(&self) -> String;
}
```

### 2. Provide Production Implementations

```rust
// Good: Production implementation
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct UuidGenerator;

impl IdGenerator for UuidGenerator {
    fn generate_id(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}
```

### 3. Provide Test Implementations

```rust
// Good: Controllable test implementation
#[derive(Debug, Clone)]
pub struct FakeClock {
    time: Arc<Mutex<DateTime<Utc>>>,
}

impl FakeClock {
    pub fn new(time: DateTime<Utc>) -> Self {
        Self { time: Arc::new(Mutex::new(time)) }
    }

    pub fn advance(&self, duration: Duration) {
        *self.time.lock().unwrap() += duration;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.time.lock().unwrap()
    }
}
```

### 4. Inject via Constructor

```rust
// Good: Inject via constructor
pub struct SessionStateMachineInner<E: EventStore> {
    // ...
    clock: Arc<dyn Clock>,
}

impl<E: EventStore> SessionStateMachineInner<E> {
    pub async fn wake(
        session_id: SessionId,
        store: Arc<E>,
        runner: Arc<dyn Runner>,
        llm_provider: Arc<dyn LlmProvider>,
        clock: Arc<dyn Clock>,  // Injected
    ) -> anyhow::Result<Self> {
        // Use clock.now() instead of Utc::now()
        last_event_timestamp: clock.now(),
    }
}
```

## Anti-Patterns

### Bad: Direct System Calls in Business Logic

```rust
// BAD: Non-deterministic call embedded in logic
impl Agent {
    pub fn create_event(&self) -> Event {
        Event {
            timestamp: Utc::now(),  // Called directly
            // ...
        }
    }
}
```

### Bad: Hidden Dependencies via Globals

```rust
// BAD: Global state
static CURRENT_TIME: OnceCell<DateTime<Utc>> = OnceCell::new();

pub fn now() -> DateTime<Utc> {
    *CURRENT_TIME.get_or_init(Utc::now())
}
```

### Bad: Optional Override

```rust
// BAD: Optional injection defeats the purpose
impl CompressionManager {
    pub fn new(strategy: Box<dyn CompressionStrategy>) -> Self {
        Self {
            strategy,
            clock: None,  // Falls back to Utc::now() internally
        }
    }

    fn get_time(&self) -> DateTime<Utc> {
        self.clock.unwrap_or_else(|| Utc::now())  // Still non-deterministic!
    }
}
```

## What Must Be Injected

| Category | Examples | Why |
|----------|----------|-----|
| Time | `Utc::now()`, `SystemTime::now()` | Tests must control time |
| ID Generation | `uuid::Uuid::new_v4()`, `ulid::Ulid::new()` | Deterministic test IDs |
| Random | `rand::random()`, `ThreadRng` | Reproducible test scenarios |
| Environment | `std::env::var()`, `getenv()` | Controlled test environments |
| Process ID | `getpid()`, `process::id()` | Cross-platform consistency |

## What Should NOT Be Injected

| Category | Examples | Why |
|----------|----------|-----|
| Database timestamps | `INSERT` timestamps | These record actual persistence time |
| Audit logs | `created_at`, `updated_at` | Database-level truth |
| File system ops | File mtime | External system truth |

## Implementation Checklist

When adding non-deterministic code to any module:

- [ ] Identify the non-deterministic call (`Utc::now()`, `Uuid::new_v4()`, etc.)
- [ ] Create a trait in `core/` or appropriate module
- [ ] Implement production version using actual system call
- [ ] Implement test version with controllable behavior
- [ ] Add trait as field to struct (using `Arc<dyn Trait>`)
- [ ] Update constructor to accept injected dependency
- [ ] Replace all internal calls with `self.dependency.method()`
- [ ] Update test call sites to inject test doubles

## Example: Refactoring a Manager

Before:
```rust
pub struct CompressionManager {
    strategy: Box<dyn CompressionStrategy>,
    config: CompressionConfig,
}

impl CompressionManager {
    pub fn build_event(&self, result: &CompressionResult) -> Event {
        Event {
            timestamp: Utc::now(),  // BAD: Direct call
            compression_id: format!("comp-{}", uuid::Uuid::new_v4()),  // BAD
        }
    }
}
```

After:
```rust
pub struct CompressionManager {
    strategy: Box<dyn CompressionStrategy>,
    config: CompressionConfig,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
}

impl CompressionManager {
    pub fn with_dependencies(
        strategy: Box<dyn CompressionStrategy>,
        config: CompressionConfig,
        clock: Arc<dyn Clock>,
        id_generator: Arc<dyn IdGenerator>,
    ) -> Self {
        Self { strategy, config, clock, id_generator }
    }

    pub fn build_event(&self, result: &CompressionResult) -> Event {
        Event {
            timestamp: self.clock.now(),  // GOOD: Injected
            compression_id: format!("comp-{}", self.id_generator.generate_id()),  // GOOD
        }
    }
}
```

## Core Module Location

For shared abstractions used across the codebase, place traits in `core/`:
```
src/
  core/
    mod.rs       # Exports
    clock.rs     # Clock trait + implementations
    id.rs        # IdGenerator trait + implementations
```

For domain-specific abstractions, co-locate with the module that owns them.
