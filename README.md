<p align="center">
  <h1 align="center">deltat</h1>
  <p align="center">A time-allocation database built on 1D collision detection.</p>
</p>

---

## The idea

In a video game, a collision engine answers one question: do these two objects overlap?

deltat does the same thing — but on a single axis: **time**.

```
 ──────[  Flight 6am–12pm  ]──────────────────────────────
 ────────────[  Dentist 2–3pm  ]──────────────────────────
 ──────────────────────[  Hotel Mon–Fri  ]────────────────
```

A booking is a segment on the timeline. Two bookings conflict when their segments collide. That's the entire model.

Everything in scheduling is just an extension of this:

| Concept | What it means |
|---|---|
| **Booking** | A segment placed on the line |
| **Conflict** | Two segments that overlap |
| **Capacity** | How many segments can stack on the same point |
| **Buffer** | A forced gap after each segment |
| **Rule** | A region of the line marked open or closed |
| **Hold** | A segment that fades away after a timer |
| **Availability** | The gaps between everything already placed |

deltat is a database built entirely around this.

## What it is

A single Rust binary that speaks PostgreSQL. No Postgres underneath — it's a purpose-built in-memory engine backed by an append-only WAL. Connect with any Postgres client.

- Sub-millisecond availability queries
- Hierarchical resources (Flight → Cabin → Seat)
- Capacity, buffers, holds with auto-expiry
- Atomic batch bookings (all-or-nothing)
- Real-time LISTEN/NOTIFY events

## Quick start

```bash
cargo install --git https://github.com/open-tap/deltat.git

DELTAT_PASSWORD=secret deltat
```

Connect on port 5433:

```bash
psql -h localhost -p 5433 -U deltat
```

---

## Usage

### Resources

Anything bookable. Resources form a tree — children inherit availability from their parent.

```sql
INSERT INTO resources (id, parent_id, name, capacity, buffer_after)
VALUES ('01J...', NULL, 'Flight AA-100', 1, 2700000);

INSERT INTO resources (id, parent_id, name)
VALUES ('01J...', '01J_FLIGHT...', 'Seat 1A');
```

```sql
SELECT * FROM resources WHERE parent_id IS NULL;       -- roots
SELECT * FROM resources WHERE parent_id = '01J...';    -- children
```

### Rules

When a resource is open or closed.

```sql
-- Available 9am–5pm
INSERT INTO rules (id, resource_id, start, "end", blocking)
VALUES ('01J...', '01J...', 1706000000000, 1706028800000, false);

-- Blocked for maintenance
INSERT INTO rules (id, resource_id, start, "end", blocking)
VALUES ('01J...', '01J...', 1706028800000, 1706032400000, true);
```

### Bookings

Place a segment on the line.

```sql
INSERT INTO bookings (id, resource_id, start, "end", label)
VALUES ('01J...', '01J...', 1706000000000, 1706003600000, 'Team Meeting');

-- Batch: all or nothing
INSERT INTO bookings (id, resource_id, start, "end")
VALUES ('01J_A...', '01J_SEAT1...', 1706000000000, 1706003600000),
       ('01J_B...', '01J_SEAT2...', 1706000000000, 1706003600000);

DELETE FROM bookings WHERE id = '01J...';
```

### Holds

Temporary segments that auto-expire.

```sql
-- Hold for 15 minutes
INSERT INTO holds (id, resource_id, start, "end", expires_at)
VALUES ('01J...', '01J...', 1706000000000, 1706003600000, 1706000900000);

DELETE FROM holds WHERE id = '01J...';
```

### Availability

Find the gaps.

```sql
SELECT * FROM availability
WHERE resource_id = '01J...'
  AND start >= 1706000000000
  AND "end" <= 1706086400000;

-- Minimum 1-hour slots
SELECT * FROM availability
WHERE resource_id = '01J...'
  AND start >= 1706000000000
  AND "end" <= 1706086400000
  AND min_duration = 3600000;

-- Multi-resource: when are all 3 free?
SELECT * FROM availability
WHERE resource_id IN ('01J_A...', '01J_B...', '01J_C...')
  AND start >= 1706000000000
  AND "end" <= 1706086400000;

-- When are at least 2 of 3 free?
SELECT * FROM availability
WHERE resource_id IN ('01J_A...', '01J_B...', '01J_C...')
  AND start >= 1706000000000
  AND "end" <= 1706086400000
  AND min_available = 2;
```

### Events

Real-time notifications via LISTEN/NOTIFY.

```sql
LISTEN resource_01J...;
UNLISTEN resource_01J...;
```

---

## Reference

All times are **Unix milliseconds**. Intervals are half-open `[start, end)` — adjacent segments don't collide.

### Configuration

| Env var | Default | Description |
|---|---|---|
| `DELTAT_PORT` | `5433` | Listen port |
| `DELTAT_BIND` | `0.0.0.0` | Bind address |
| `DELTAT_DATA_DIR` | `./data` | WAL storage directory |
| `DELTAT_PASSWORD` | `deltat` | Connection password |

### Architecture

```
src/
  engine/       Availability computation, conflict detection, state mutations
  model.rs      Core types: Span, Interval, ResourceState
  sql.rs        SQL parser → command enum
  wire.rs       pgwire protocol (simple + extended query)
  wal.rs        Append-only write-ahead log
  notify.rs     LISTEN/NOTIFY broadcast
  reaper.rs     Hold expiration
  main.rs       TCP listener
```

### Client libraries

**TypeScript** — [@open-tap/client](https://github.com/open-tap/tap)

### Demos

The [tap](https://github.com/open-tap/tap) repo includes a Next.js demo app with interactive examples: airline seat booking, theater reservations, stadium events, calendar scheduling, multi-resource availability, and temporary holds.

## License

[AGPL-3.0](LICENSE)
