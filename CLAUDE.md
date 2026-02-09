# deltat

## Code Principles

These apply to ALL code in this repo. No exceptions.

- **First principles** — understand the problem before writing code. Ask "why" before "how."
- **Occam's razor / KISS** — the simplest solution that works is the right one. Don't add complexity.
- **DRY** — don't repeat yourself, but don't abstract prematurely either. Three similar lines beats a premature abstraction.
- **SOLID** — single responsibility, open/closed, Liskov substitution, interface segregation, dependency inversion.
- **Composable** — small, focused functions that combine. No god functions.
- **TDD** — tests pass at every step. Never leave tests broken between changes.
- **No over-engineering** — only build what's needed now. No feature flags, no "just in case" abstractions, no hypothetical future requirements.
- **No unnecessary comments** — code should be self-documenting. Only comment the "why", never the "what."

## What is deltat

A custom time-allocation database written in Rust. Speaks PostgreSQL wire protocol (pgwire). No Postgres underneath — it's a single binary with an in-memory state machine backed by an append-only WAL.

**Core capability:** Given resources and time rules, compute availability with sub-millisecond latency. Supports hierarchical resources, multi-slot capacity, buffer times, holds with auto-expiry, atomic batch bookings, and real-time LISTEN/NOTIFY.

## Architecture

```
src/
  engine/          State machine: availability, conflict detection, mutations, queries
  sql.rs           SQL parsing (sqlparser) → Command enum
  wire.rs          pgwire protocol handler (simple + extended query)
  model.rs         Core types: Span, Interval, ResourceState, Event
  wal.rs           Append-only WAL with group-commit
  tenant.rs        Multi-tenant manager (per-database Engine + WAL)
  notify.rs        Broadcast channels for LISTEN/NOTIFY
  reaper.rs        Background hold expiration
  auth.rs          Cleartext password auth
  main.rs          TCP listener

web/               Next.js demo app (separate from the DB)
```

## Commands

### Rust (database)

```bash
# Run tests (skip slow limit tests)
cargo test --lib -- --skip create_resource_too_many --skip interval_limit

# Integration tests
cargo test --test listen_notify

# Clippy (must be clean)
cargo clippy -- -D warnings

# Release build
cargo build --release

# Run server
DELTAT_PASSWORD=secret cargo run
```

### Web (demo app)

```bash
cd web
bun dev          # Starts deltat server in background + Next.js dev server
bun run build    # Production build
```

## Environment

- Rust toolchain: `/Users/sorzel/.rustup/toolchains/stable-aarch64-apple-darwin/bin`
- Default port: 5433
- Default password: `deltat` (web demos use `secret`)
- Data dir: `./data/`

## Config (env vars)

- `DELTAT_PORT` (default 5433)
- `DELTAT_BIND` (default 0.0.0.0)
- `DELTAT_DATA_DIR` (default ./data)
- `DELTAT_PASSWORD` (default deltat)
