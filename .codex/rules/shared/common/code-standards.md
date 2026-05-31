# Code Standards

## Project Structure

1. **Module Isolation:** Each controller or service must be under a sub-folder with its own module.
2. **Google AIP Compliance:** RESTful APIs must be consistent with the Google AIP (API Improvement Proposal) standard. At minimum, follow AIP-131 (Get), AIP-132 (List), AIP-133 (Create), AIP-134 (Update), and AIP-135 (Delete).
3. **Documentation:** RESTful APIs must generate Swagger (OpenAPI) documentation.

## Code Style & Integrity

1. **Enum Safety:** Always use exhaustive switch statements to handle all possible cases for enums. Enforce this with a `never` default case to get compile-time errors on unhandled cases.
2. **No Any:** The use of `any` is strictly prohibited. If absolutely necessary, use `unknown` and perform explicit type checking.
   * **Agent Note:** Do not bypass this by casting to `any`.
3. **Naming:** Avoid using `util` as a file name or function name. Use a name that describes the specific functionality (e.g., `${functionName}Helper`).
4. **Architecture:** Prefer composition over inheritance.
5. **Error Handling:** Prefer fail-fast over fail-safe. Never silently swallow errors by logging a warning and returning a default value -- always propagate errors via `Result`/exceptions to the caller. If a code path is not yet implemented, use a `TODO` comment with a `panic!`/`throw` rather than silently producing incorrect data.
6. **No Emoji:** Do not use Emoji in code, comments, or commit messages.
7. **If It Compiles, It Works:** Encode invariants in the type system wherever possible (branded types, discriminated unions, const assertions). Rely on the compiler to catch errors rather than runtime checks.
8. **Error Response Format:** All APIs must return errors using the Google AIP error model (status code, message, details).

## Testing

1. **Real Implementations:** Favor real implementations in unit tests. If a real implementation is unavailable, use a Fake instead. Avoid using Mocks whenever possible.
2. **Public Behavior:** Test only public behavior. Do not hack into classes to access private functions for testing.

## Database

1. **Strict Transactions:** All database interactions must be within a transaction. **This applies to READ operations as well** (wrap reads in a transaction).

## Data Serialization

1. **Typed Structs Over Free-Form JSON:** When constructing JSON payloads (e.g., for database updates, API requests), prefer using protobuf-defined or schema-defined typed structs with serde serialization instead of free-form `json!()` macros. This ensures compile-time type safety and correct field naming (e.g., camelCase via `serde(rename_all)`).

## Dependencies & Tech Stack

1. **NodeJS:** Use `pnpm` to manage dependencies.
   * All NestJS projects must integrate Swagger and include explicit Request/Response DTOs.
2. **Python:** Use `uv` to manage dependencies.
3. **GenAI:** Always use `@google/genai` (deprecated: `@google/generative-ai`).
4. **BullMQ:** For Job types, use generic typing: `Job<DataType, ReturnType, JobName>`.
5. **Config:** Don't use any default config value. If the config is missed in .env, just fail the server.
6. **Dependency Injection:** All projects should follow the Dependency Injection code style similar to Guice (constructor injection, explicit bindings, no service locators).
7. **NodeJS & Typescript:** Always use `npx tsx` to run TypeScript code in NodeJS.
8. **Python:** Always enforce strict Python types.
9. **Python:** Always use asyncio. Do not use any sync code.
10. **Playwright:** When performing crawling tasks, first run an exploration script which uses Playwright to access the webpage, dump the HTML to a local file. Process the HTML files for exploration. Write down the structure of the HTML and plans for crawling in a local markdown.
11. **EventLog:** Every system should have an event log which logs time-series events of the system for debugging. Events must use structured logging (JSON format with timestamp, event type, and payload).

## Execution

1. **Golden Rule:** DO NOT commit until explicitly instructed.
2. Always seek clarification if the user's request is ambiguous.
