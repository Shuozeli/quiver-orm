# Spanner Schema Design

## Core Concepts

A schema in Spanner acts as a namespace for database objects (tables, views, indexes, functions) to organize data, prevent naming collisions, and apply access controls. Spanner data is **strongly typed**, supporting both scalar and complex data types.

## Primary Keys and Storage

Every table (in GoogleSQL dialect) should have a primary key to uniquely identify rows. Spanner physically stores rows sorted by primary key values.

* **Hotspotting:** Because data is divided among servers by key ranges, using monotonically increasing keys (like timestamps) causes all inserts to hit a single server ("hotspot").
* **Best Practices:** To distribute load across servers, use techniques like hashing keys, swapping column order, or using Version 4 UUIDs.

## Parent-Child Relationships

There are two methods to define relationships between tables:

### 1. Table Interleaving (Physical Co-location)

Interleaving physically stores child rows immediately after their associated parent row. This optimizes query performance for related data.

* **Requirement:** The child table's primary key must start with the parent table's primary key.
* **Interleave in Parent:** Enforces referential integrity (the parent row must exist).
* **Interleave In:** Defines locality without strict referential integrity (child rows can exist if the parent is deleted).
* **Hierarchy:** You can nest interleaving up to seven layers deep (e.g., Singer > Album > Song).

### 2. Foreign Keys (Logical Relationship)

Foreign keys ensure referential integrity but do **not** co-locate tables in storage. They are a general solution when physical co-location is not required or when tables need multiple relationships.

## Named Schemas

Named schemas (e.g., `marketing.customers`) allow you to group similar data logically. This supports **Fine-Grained Access Control**, allowing you to grant permissions to entire groups of objects within a specific namespace rather than individually.
