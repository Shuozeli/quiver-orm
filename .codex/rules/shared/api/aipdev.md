# Google AIP Standards (Comprehensive)

## 1. Resource Oriented Design (AIP-121 & 122)

### Resource Hierarchy

* **Structure:** APIs must be structured as a hierarchy of resources and collections.
* **Pattern:** `//api.example.com/v1/publishers/{publisher}/books/{book}`
* **Collections:** Must be plural nouns (e.g., `books`, not `book`).
* **Resources:** Must be singular nouns (conceptually), but addressable via the collection.

### Resource Names (AIP-122)

* **The `name` Field:** Every resource **MUST** have a field named `name` of type string.
* This field contains the relative URI path (e.g., `publishers/123/books/456`).
* It acts as the primary key. Do not use integer IDs (like `id: 456`) as the primary identifier in API responses.
* **Format:** Use `kebab-case` for ID segments inside the URI path, but `snake_case` for field names in the JSON body.

## 2. Standard Methods (AIP-131 -- 135)

### Get (AIP-131)

* **Signature:** `GetBookRequest` -> `Book`
* **Behavior:**
  * Must not modify state (Safe method).
  * Must not support request body data.
  * If the resource is Soft Deleted, `Get` must return `NOT_FOUND` unless a specific flag (e.g., `show_deleted=true`) is passed.

### List (AIP-132)

* **Signature:** `ListBooksRequest` -> `ListBooksResponse`
* **Request Fields:**
  * `parent` (string, required): The collection to list from.
  * `page_size` (int, optional): Max results.
  * `page_token` (string, optional): Opaque pagination token.
* **Response Fields:**
  * `books` (repeated Book): The list of resources.
  * `next_page_token` (string): Token for the next page.
* **Consistency:** `List` results should be consistent with `Get`. If a user can `Get` a resource, it should appear in `List`.

### Create (AIP-133)

* **Signature:** `CreateBookRequest` -> `Book`
* **Request Fields:**
  * `parent` (string, required).
  * `book` (Book, required): The resource body.
  * `book_id` (string, optional): Allow the client to pick the ID.
* **Behavior:**
  * If the ID already exists, return `ALREADY_EXISTS`.
  * Fields marked as `Output Only` (like `create_time`) must be ignored if present in the request.

### Update (AIP-134)

* **Signature:** `UpdateBookRequest` -> `Book`
* **Method:** MUST use `PATCH` (not `PUT`).
* **Request Fields:**
  * `book` (Book, required).
  * `update_mask` (FieldMask, required): Explicitly lists the fields to modify.
* **Behavior:**
  * If `update_mask` is missing, the API should treat it as a full replacement (or strict error, depending on policy).
  * Partial updates are required.

### Delete (AIP-135)

* **Signature:** `DeleteBookRequest` -> `Empty` (or `Operation`)
* **Behavior:**
  * Return `google.protobuf.Empty` on success.
  * If the resource is missing, return `NOT_FOUND`.
  * **Soft Delete:** If implemented, `Delete` marks the resource as deleted but does not remove it immediately.

## 3. Common Design Patterns

### Field Masks (AIP-161)

* **Usage:** Used primarily in `Update` (PATCH) requests to support partial updates.
* **Format:** Comma-separated paths (e.g., `displayName,accountConfig.notificationSettings`).
* **Safety:** When a client sends data in the body but excludes it from the `update_mask`, the server **MUST** ignore that data (do not update it).

### Pagination (AIP-158)

* **Bidirectional:** If reverse pagination is needed, use standard `page_token` logic but traverse backwards. Do not add `prev_page_token` to the response unless strictly necessary (AIP prefers strict forward-only cursors for performance).
* **Stability:** Tokens should be opaque (base64 encoded protobufs) so schema changes don't break clients.

### Filtering (AIP-160)

* **Syntax:** Filter strings should follow the Common Expression Language (CEL) subset.
  * `a = 1`
  * `b = "foo"`
  * `c > 5 AND (d < 10 OR e != "bar")`
* **Traversal:** Filtering can traverse relationships (e.g., `author.name = "Dickens"`).

### Sorting (AIP-132)

* **Syntax:** Comma-separated fields with optional `desc` suffix.
  * `create_time desc, last_name` (Default is ascending).
* **Defaults:** APIs should define a default ordering if none is provided (usually `name` or `create_time desc`).

## 4. Advanced Patterns

### Long-Running Operations (AIP-151)

* **Usage:** For methods that take >10 seconds or involve external asynchronous processing (e.g., `ProvisionDatabase`).
* **Response:** Return a `google.longrunning.Operation` resource immediately.
  * Contains `name` (operation ID), `done` (bool), and `metadata` (progress info).
* **Polling:** Clients poll `GetOperation` to check status.
* **Completion:** When `done=true`, the `response` field contains the final resource or `error`.

### Singleton Resources (AIP-156)

* **Definition:** Resources where only one instance exists within a parent (e.g., `users/123/settings`).
* **Naming:** The name acts as the ID (e.g., `settings`).
* **Methods:**
  * No `Create` or `Delete`.
  * Only `Get` and `Update`.
  * If it doesn't exist yet, `Get` returns a default object or `NOT_FOUND` (design choice), and `Update` creates it upsert-style.

## 5. Versioning & Compatibility

### API Versioning (AIP-168)

* **Strategy:** Use Semantic Versioning in the URL path (`v1`, `v1beta`, `v2`).
* **Breaking Changes:** Strictly forbidden within a major version (`v1`).
  * renaming fields.
  * changing field types.
  * changing resource structure.
* **Non-Breaking Changes:** Allowed.
  * Adding new fields.
  * Adding new methods.

### Deprecation (AIP-192)

* **Mechanism:** Do not remove fields immediately. Mark them as `deprecated` in the definition.
* **Timeline:** Provide a migration window (usually 12+ months) before complete removal in the next major version.
