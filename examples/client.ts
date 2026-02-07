/**
 * deltat — Bun SQL client example
 *
 * Start the server:
 *   DELTAT_PASSWORD=secret cargo run --release
 *
 * Run this script:
 *   bun run examples/client.ts
 */

import { SQL } from "bun";

const db = new SQL({
  hostname: "localhost",
  port: 5433,
  database: "demo",
  username: "user",
  password: "secret",
});

// Helper to generate ULID-like IDs (26 chars, Crockford base32)
function id(): string {
  const t = Date.now().toString(36).padStart(10, "0");
  const r = Math.random().toString(36).substring(2, 18).padStart(16, "0");
  return (t + r).substring(0, 26).toUpperCase();
}

async function main() {
  const now = Date.now();
  const HOUR = 3_600_000;
  const DAY = 86_400_000;

  // ─── 1. Create resource hierarchy ──────────────────────────────
  const theaterId = id();
  const screenId = id();
  const seat1Id = id();
  const seat2Id = id();

  console.log("Creating resources...");
  await db`INSERT INTO resources (id, parent_id) VALUES (${theaterId}, NULL)`;
  await db`INSERT INTO resources (id, parent_id) VALUES (${screenId}, ${theaterId})`;
  await db`INSERT INTO resources (id, parent_id, capacity) VALUES (${seat1Id}, ${screenId}, 1)`;
  await db`INSERT INTO resources (id, parent_id, capacity) VALUES (${seat2Id}, ${screenId}, 1)`;
  console.log(`  theater: ${theaterId}`);
  console.log(`  screen:  ${screenId}`);
  console.log(`  seat1:   ${seat1Id}`);
  console.log(`  seat2:   ${seat2Id}`);

  // ─── 2. Add availability rules ────────────────────────────────
  const ruleId = id();
  const tomorrow = now + DAY;
  const open = tomorrow;
  const close = tomorrow + 12 * HOUR;

  console.log("\nAdding availability rule (12-hour window)...");
  await db`INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES (${ruleId}, ${theaterId}, ${open}, ${close}, false)`;

  // ─── 3. Query availability ─────────────────────────────────────
  console.log("\nQuerying seat1 availability...");
  const slots = await db`SELECT * FROM availability WHERE resource_id = ${seat1Id} AND start >= ${open} AND "end" <= ${close}`;
  console.log("  Available slots:");
  for (const row of slots) {
    const start = new Date(Number(row.start)).toISOString();
    const end = new Date(Number(row.end)).toISOString();
    console.log(`    ${start} → ${end}`);
  }

  // ─── 4. Book a seat ────────────────────────────────────────────
  const bookingId = id();
  const showStart = tomorrow + 2 * HOUR;
  const showEnd = tomorrow + 4 * HOUR;

  console.log("\nBooking seat1 for 2-hour show...");
  await db`INSERT INTO bookings (id, resource_id, start, "end") VALUES (${bookingId}, ${seat1Id}, ${showStart}, ${showEnd})`;
  console.log(`  booking: ${bookingId}`);

  // ─── 5. Check availability after booking ───────────────────────
  console.log("\nAvailability after booking:");
  const slotsAfter = await db`SELECT * FROM availability WHERE resource_id = ${seat1Id} AND start >= ${open} AND "end" <= ${close}`;
  for (const row of slotsAfter) {
    const start = new Date(Number(row.start)).toISOString();
    const end = new Date(Number(row.end)).toISOString();
    console.log(`    ${start} → ${end}`);
  }

  // ─── 6. Try double-booking (should fail) ───────────────────────
  console.log("\nAttempting double-booking on same seat...");
  try {
    await db`INSERT INTO bookings (id, resource_id, start, "end") VALUES (${id()}, ${seat1Id}, ${showStart}, ${showEnd})`;
    console.log("  ERROR: should have failed!");
  } catch (e: any) {
    console.log(`  Correctly rejected: ${e.message}`);
  }

  // ─── 7. Book seat2 on same time (different resource) ──────────
  const booking2Id = id();
  console.log("\nBooking seat2 for same show time...");
  await db`INSERT INTO bookings (id, resource_id, start, "end") VALUES (${booking2Id}, ${seat2Id}, ${showStart}, ${showEnd})`;
  console.log(`  booking: ${booking2Id}`);

  // ─── 8. Cancel booking ─────────────────────────────────────────
  console.log("\nCancelling seat1 booking...");
  await db`DELETE FROM bookings WHERE id = ${bookingId}`;

  // ─── 9. Verify availability restored ───────────────────────────
  const slotsRestored = await db`SELECT * FROM availability WHERE resource_id = ${seat1Id} AND start >= ${open} AND "end" <= ${close}`;
  console.log("Availability restored:");
  for (const row of slotsRestored) {
    const start = new Date(Number(row.start)).toISOString();
    const end = new Date(Number(row.end)).toISOString();
    console.log(`    ${start} → ${end}`);
  }

  // ─── 10. Cleanup ───────────────────────────────────────────────
  await db`DELETE FROM bookings WHERE id = ${booking2Id}`;
  await db`DELETE FROM rules WHERE id = ${ruleId}`;
  await db`DELETE FROM resources WHERE id = ${seat1Id}`;
  await db`DELETE FROM resources WHERE id = ${seat2Id}`;
  await db`DELETE FROM resources WHERE id = ${screenId}`;
  await db`DELETE FROM resources WHERE id = ${theaterId}`;

  console.log("\nDone! All resources cleaned up.");
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
