# deltat

A time-allocation database.

## The idea

Every scheduling problem is the same problem.

A flight seat from 6am to 12pm. A dentist appointment from 2pm to 3pm. A hotel room from Monday to Friday. Strip away the domain and what's left is a segment on a timeline. A booking is just a line segment placed on the number line of time.

Two bookings conflict when their segments overlap — when one starts before the other ends. That's it. That's the entire conflict model. It's 1D collision detection: the same idea as checking whether two objects on a line touch each other.

Everything else builds on this single primitive:

- **Capacity** is how many segments can stack on the same point. A conference room fits 1 meeting at a time. A parking lot fits 200 cars.
- **Buffers** are forced gaps between segments. A 45-minute turnaround between flights. A 15-minute cleaning window between hotel guests.
- **Rules** are regions of the timeline that are open or closed. A restaurant is available 11am–11pm. A theater is blocked on Mondays.
- **Holds** are segments that fade away after a timer. A user adds a seat to their cart — the system places a segment that disappears in 15 minutes if they don't confirm.
- **Availability** is the answer to: given everything already placed on the line, where are the gaps?

All scheduling complexity reduces to intervals on a line, collisions between them, and rules about where you can place new ones.

deltat is a database built entirely around this insight.

## What it is

A single Rust binary. No PostgreSQL underneath — deltat speaks the PostgreSQL wire protocol directly, so any Postgres client can connect, but the engine is a purpose-built in-memory state machine backed by an append-only write-ahead log.

- Sub-millisecond availability queries
- Hierarchical resources (Flight > Cabin > Seat)
- Multi-slot capacity with sweep-line conflict detection
- Configurable buffer times between allocations
- Temporary holds with automatic expiration
- Atomic batch bookings (all-or-nothing across multiple resources)
- Real-time change notifications via LISTEN/NOTIFY

## Quick start

```bash
# Install
cargo install --git https://github.com/open-tap/deltat.git

# Run
DELTAT_PASSWORD=secret deltat
```

deltat is now listening on port 5433. Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5433 -U deltat
```

## Core concepts

### Resources

Anything bookable. Resources form a tree — a parent groups its children, and children inherit availability from their parent.

```sql
-- Create a root resource
INSERT INTO resources (id, parent_id, name, capacity, buffer_after)
VALUES ('01J...', NULL, 'Flight AA-100', 1, 2700000);

-- Create a child
INSERT INTO resources (id, parent_id, name, capacity)
VALUES ('01J...', '01J_PARENT...', 'Seat 1A', 1);
```

| Column | Description |
|---|---|
| `id` | ULID (client-generated) |
| `parent_id` | Parent resource, or NULL for root |
| `name` | Optional display name |
| `capacity` | Max concurrent allocations (default 1) |
| `buffer_after` | Forced gap in ms after each allocation ends |

```sql
-- Get root resources
SELECT * FROM resources WHERE parent_id IS NULL;

-- Get children of a resource
SELECT * FROM resources WHERE parent_id = '01J...';
```

### Rules

Define when a resource is open or closed. A non-blocking rule opens an availability window. A blocking rule closes one.

```sql
-- This resource is available from 9am to 5pm on a given day
INSERT INTO rules (id, resource_id, start, "end", blocking)
VALUES ('01J...', '01J_RESOURCE...', 1706000000000, 1706028800000, false);

-- This resource is blocked for maintenance
INSERT INTO rules (id, resource_id, start, "end", blocking)
VALUES ('01J...', '01J_RESOURCE...', 1706028800000, 1706032400000, true);
```

Children inherit their parent's rules. If a child has its own non-blocking rules, those override the parent's availability windows entirely. Blocking rules accumulate — both parent and child blocking rules apply.

### Bookings

Permanent allocations. A booking occupies a segment of time on a resource.

```sql
-- Book a single resource
INSERT INTO bookings (id, resource_id, start, "end", label)
VALUES ('01J...', '01J_RESOURCE...', 1706000000000, 1706003600000, 'Team Meeting');

-- Atomic batch: book multiple resources at once (all or nothing)
INSERT INTO bookings (id, resource_id, start, "end")
VALUES ('01J_A...', '01J_SEAT1...', 1706000000000, 1706003600000),
       ('01J_B...', '01J_SEAT2...', 1706000000000, 1706003600000);

-- Cancel a booking
DELETE FROM bookings WHERE id = '01J...';
```

If any booking in a batch conflicts, the entire batch fails. No partial bookings.

### Holds

Temporary reservations that auto-expire. Place a hold while a user is checking out — if they don't confirm in time, the hold disappears and the slot opens back up.

```sql
-- Hold a resource for 15 minutes
INSERT INTO holds (id, resource_id, start, "end", expires_at)
VALUES ('01J...', '01J_RESOURCE...', 1706000000000, 1706003600000, 1706000900000);

-- Release a hold early
DELETE FROM holds WHERE id = '01J...';
```

Active holds block availability just like bookings. Once `expires_at` passes, the hold is automatically released.

### Availability

Query the gaps. Given all rules, bookings, and active holds on a resource, what time windows are free?

```sql
-- Find free slots for a resource within a time range
SELECT * FROM availability
WHERE resource_id = '01J...'
  AND start >= 1706000000000
  AND "end" <= 1706086400000;

-- Only return slots at least 1 hour long
SELECT * FROM availability
WHERE resource_id = '01J...'
  AND start >= 1706000000000
  AND "end" <= 1706086400000
  AND min_duration = 3600000;
```

For multi-resource scheduling — find times when multiple resources are simultaneously free:

```sql
-- Find times when ALL of these resources are free
SELECT * FROM availability
WHERE resource_id IN ('01J_A...', '01J_B...', '01J_C...')
  AND start >= 1706000000000
  AND "end" <= 1706086400000;

-- Find times when at least 2 out of 3 are free
SELECT * FROM availability
WHERE resource_id IN ('01J_A...', '01J_B...', '01J_C...')
  AND start >= 1706000000000
  AND "end" <= 1706086400000
  AND min_available = 2;
```

### Events

Real-time notifications when anything changes. Subscribe to a resource's channel to get notified of bookings, holds, rule changes, etc.

```sql
LISTEN resource_01J...;

UNLISTEN resource_01J...;
```

Events are JSON payloads delivered via PostgreSQL's NOTIFY mechanism. Event types: `ResourceCreated`, `ResourceUpdated`, `ResourceDeleted`, `RuleAdded`, `RuleUpdated`, `RuleRemoved`, `HoldPlaced`, `HoldReleased`, `BookingConfirmed`, `BookingCancelled`.

## All times are Unix milliseconds

Every `start`, `end`, `expires_at`, and `buffer_after` value is in Unix milliseconds. Intervals are half-open: `[start, end)` — a booking from 1000 to 2000 occupies 1000 and 1999, but not 2000. This means two adjacent bookings [1000, 2000) and [2000, 3000) don't conflict.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `DELTAT_PORT` | `5433` | Listen port |
| `DELTAT_BIND` | `0.0.0.0` | Bind address |
| `DELTAT_DATA_DIR` | `./data` | WAL storage directory |
| `DELTAT_PASSWORD` | `deltat` | Connection password |

## Architecture

```
src/
  engine/       State machine: availability computation, conflict detection, mutations
  sql.rs        SQL parser (sqlparser) → internal command enum
  wire.rs       pgwire protocol handler (simple + extended query)
  model.rs      Core types: Span, Interval, ResourceState, Event
  wal.rs        Append-only WAL with group-commit
  tenant.rs     Multi-tenant manager (per-database engine + WAL)
  notify.rs     Broadcast channels for LISTEN/NOTIFY
  reaper.rs     Background hold expiration
  auth.rs       Password authentication
  main.rs       TCP listener
```

## Client libraries

- **TypeScript**: [@open-tap/client](https://github.com/open-tap/tap) — SDK wrapping deltat's SQL interface with typed domain classes

## License

[AGPL-3.0](LICENSE)
