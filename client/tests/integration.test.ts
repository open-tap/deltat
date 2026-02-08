import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { startServer, stopServer, createClient } from "./helpers";
import type { DeltaTEvent } from "../src/index";

// Simple ULID generator for tests
function ulid(): string {
  const ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
  const now = Date.now();
  let id = "";
  let ts = now;
  const timePart = [];
  for (let i = 0; i < 10; i++) {
    timePart.unshift(ENCODING[ts % 32]);
    ts = Math.floor(ts / 32);
  }
  id = timePart.join("");
  for (let i = 0; i < 16; i++) {
    id += ENCODING[Math.floor(Math.random() * 32)];
  }
  return id;
}

let port: number;

beforeAll(async () => {
  port = await startServer();
}, 60000);

afterAll(() => {
  stopServer();
});

describe("Resource CRUD", () => {
  test("full_resource_lifecycle", async () => {
    const client = createClient(port);
    try {
      const id = ulid();

      const created = await client.createResource({ id, name: "Test Room", capacity: 5 });
      expect(created).toBe(id);

      const all = await client.listResources();
      const found = all.find((r) => r.id === id);
      expect(found).toBeDefined();
      expect(found!.name).toBe("Test Room");
      expect(found!.capacity).toBe(5);

      await client.updateResource(id, { name: "Updated Room", capacity: 10 });
      const updated = await client.listResources();
      const updatedRes = updated.find((r) => r.id === id);
      expect(updatedRes!.name).toBe("Updated Room");
      expect(updatedRes!.capacity).toBe(10);

      await client.deleteResource(id);
      const afterDelete = await client.listResources();
      expect(afterDelete.find((r) => r.id === id)).toBeUndefined();
    } finally {
      await client.close();
    }
  });
});

describe("Rules CRUD", () => {
  test("rules_crud", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({
        id: ruleId,
        resourceId: rid,
        start: 1000,
        end: 2000,
        blocking: false,
      });

      const rules = await client.getRules(rid);
      expect(rules.length).toBe(1);
      expect(rules[0].id).toBe(ruleId);
      expect(rules[0].start).toBe(1000);
      expect(rules[0].end).toBe(2000);
      expect(rules[0].blocking).toBe(false);

      await client.updateRule(ruleId, { start: 3000, end: 4000, blocking: true });
      const updated = await client.getRules(rid);
      expect(updated[0].start).toBe(3000);
      expect(updated[0].end).toBe(4000);
      expect(updated[0].blocking).toBe(true);

      await client.deleteRule(ruleId);
      const afterDelete = await client.getRules(rid);
      expect(afterDelete.length).toBe(0);
    } finally {
      await client.close();
    }
  });
});

describe("Bookings CRUD", () => {
  test("bookings_crud", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 100000 });

      const bookingId = ulid();
      await client.book({
        id: bookingId,
        resourceId: rid,
        start: 1000,
        end: 2000,
        label: "Team Standup",
      });

      const bookings = await client.getBookings(rid);
      expect(bookings.length).toBe(1);
      expect(bookings[0].id).toBe(bookingId);
      expect(bookings[0].label).toBe("Team Standup");

      await client.cancelBooking(bookingId);
      const after = await client.getBookings(rid);
      expect(after.length).toBe(0);
    } finally {
      await client.close();
    }
  });

  test("batch_bookings", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 100000 });

      const ids = [ulid(), ulid(), ulid()];
      await client.batchBook([
        { id: ids[0], resourceId: rid, start: 1000, end: 2000, label: "A" },
        { id: ids[1], resourceId: rid, start: 3000, end: 4000, label: "B" },
        { id: ids[2], resourceId: rid, start: 5000, end: 6000, label: "C" },
      ]);

      const bookings = await client.getBookings(rid);
      expect(bookings.length).toBe(3);
      const bookingIds = bookings.map((b) => b.id).sort();
      expect(bookingIds).toEqual(ids.sort());
    } finally {
      await client.close();
    }
  });
});

describe("Holds CRUD", () => {
  test("holds_crud", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 100000 });

      const holdId = ulid();
      const expiresAt = Date.now() + 60000;
      await client.placeHold({
        id: holdId,
        resourceId: rid,
        start: 1000,
        end: 2000,
        expiresAt,
      });

      const holds = await client.getHolds(rid);
      expect(holds.length).toBe(1);
      expect(holds[0].id).toBe(holdId);
      expect(holds[0].expires_at).toBe(expiresAt);

      await client.releaseHold(holdId);
      const after = await client.getHolds(rid);
      expect(after.length).toBe(0);
    } finally {
      await client.close();
    }
  });
});

describe("Availability", () => {
  test("availability_query", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 10000 });

      const bookingId = ulid();
      await client.book({ id: bookingId, resourceId: rid, start: 3000, end: 5000 });

      const slots = await client.getAvailability(rid, 0, 10000);
      expect(slots.length).toBe(2);
      expect(slots[0].start).toBe(0);
      expect(slots[0].end).toBe(3000);
      expect(slots[1].start).toBe(5000);
      expect(slots[1].end).toBe(10000);
    } finally {
      await client.close();
    }
  });

  test("multi_availability_query", async () => {
    const client = createClient(port);
    try {
      const rid1 = ulid();
      const rid2 = ulid();
      await client.createResource({ id: rid1 });
      await client.createResource({ id: rid2 });

      await client.addRule({ id: ulid(), resourceId: rid1, start: 0, end: 10000 });
      await client.addRule({ id: ulid(), resourceId: rid2, start: 0, end: 10000 });

      await client.book({ id: ulid(), resourceId: rid1, start: 2000, end: 4000 });
      await client.book({ id: ulid(), resourceId: rid2, start: 6000, end: 8000 });

      // min_available = 1 (union/pool)
      const poolSlots = await client.getMultiAvailability(
        [rid1, rid2],
        0,
        10000,
        { minAvailable: 1 },
      );
      expect(poolSlots.length).toBeGreaterThan(0);

      // min_available = 2 (intersection)
      const bothSlots = await client.getMultiAvailability(
        [rid1, rid2],
        0,
        10000,
        { minAvailable: 2 },
      );
      expect(bothSlots.length).toBeGreaterThan(0);
      for (const slot of bothSlots) {
        expect(slot.start >= 4000 || slot.end <= 2000).toBe(true);
        expect(slot.start >= 8000 || slot.end <= 6000).toBe(true);
      }
    } finally {
      await client.close();
    }
  });
});

describe("Subscriptions", () => {
  test("subscribe_receives_events", async () => {
    const client = createClient(port);
    const client2 = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 100000 });

      const events: DeltaTEvent[] = [];
      const sub = await client.subscribe(rid, (event) => {
        events.push(event);
      });

      await new Promise((r) => setTimeout(r, 300));

      const bookingId = ulid();
      await client2.book({ id: bookingId, resourceId: rid, start: 1000, end: 2000 });

      await new Promise((r) => setTimeout(r, 1000));

      expect(events.length).toBeGreaterThan(0);

      await sub.unsubscribe();
    } finally {
      await client.close();
      await client2.close();
    }
  }, 10000);

  test("unsubscribe_stops_events", async () => {
    const client = createClient(port);
    const client2 = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });

      const ruleId = ulid();
      await client.addRule({ id: ruleId, resourceId: rid, start: 0, end: 100000 });

      const events: DeltaTEvent[] = [];
      const sub = await client.subscribe(rid, (event) => {
        events.push(event);
      });

      await new Promise((r) => setTimeout(r, 300));
      await sub.unsubscribe();
      await new Promise((r) => setTimeout(r, 300));

      await client2.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 });

      await new Promise((r) => setTimeout(r, 1000));

      expect(events.length).toBe(0);
    } finally {
      await client.close();
      await client2.close();
    }
  }, 10000);

  test("subscribe_correct_channel_only", async () => {
    const client = createClient(port);
    const client2 = createClient(port);
    try {
      const ridA = ulid();
      const ridB = ulid();
      await client.createResource({ id: ridA });
      await client.createResource({ id: ridB });

      await client.addRule({ id: ulid(), resourceId: ridA, start: 0, end: 100000 });
      await client.addRule({ id: ulid(), resourceId: ridB, start: 0, end: 100000 });

      const eventsA: DeltaTEvent[] = [];
      const sub = await client.subscribe(ridA, (event) => {
        eventsA.push(event);
      });

      await new Promise((r) => setTimeout(r, 300));

      // Mutate B — should NOT trigger callback for A
      await client2.book({ id: ulid(), resourceId: ridB, start: 1000, end: 2000 });

      await new Promise((r) => setTimeout(r, 500));
      expect(eventsA.length).toBe(0);

      // Mutate A — SHOULD trigger callback
      await client2.book({ id: ulid(), resourceId: ridA, start: 3000, end: 4000 });

      await new Promise((r) => setTimeout(r, 1000));
      expect(eventsA.length).toBeGreaterThan(0);

      await sub.unsubscribe();
    } finally {
      await client.close();
      await client2.close();
    }
  }, 10000);
});

describe("Error paths", () => {
  test("duplicate_resource_rejected", async () => {
    const client = createClient(port);
    try {
      const id = ulid();
      await client.createResource({ id });
      await expect(client.createResource({ id })).rejects.toThrow(/already exists/);
    } finally {
      await client.close();
    }
  });

  test("double_booking_conflict", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid });
      await client.addRule({ id: ulid(), resourceId: rid, start: 0, end: 100000 });

      await client.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 });
      await expect(
        client.book({ id: ulid(), resourceId: rid, start: 1500, end: 2500 }),
      ).rejects.toThrow(/conflict with allocation/);
    } finally {
      await client.close();
    }
  });

  test("capacity_exceeded", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      await client.createResource({ id: rid, capacity: 2 });
      await client.addRule({ id: ulid(), resourceId: rid, start: 0, end: 100000 });

      // Fill both slots
      await client.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 });
      await client.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 });

      // Third booking should fail
      await expect(
        client.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 }),
      ).rejects.toThrow(/capacity 2 exceeded/);
    } finally {
      await client.close();
    }
  });

  test("buffer_after_prevents_adjacent_booking", async () => {
    const client = createClient(port);
    try {
      const rid = ulid();
      // 500ms buffer after each booking
      await client.createResource({ id: rid, bufferAfter: 500 });
      await client.addRule({ id: ulid(), resourceId: rid, start: 0, end: 100000 });

      await client.book({ id: ulid(), resourceId: rid, start: 1000, end: 2000 });

      // Adjacent booking at 2000 should conflict (buffer extends to 2500)
      await expect(
        client.book({ id: ulid(), resourceId: rid, start: 2000, end: 3000 }),
      ).rejects.toThrow(/conflict with allocation/);

      // Booking after the buffer should succeed
      await client.book({ id: ulid(), resourceId: rid, start: 2500, end: 3500 });
    } finally {
      await client.close();
    }
  });

  test("delete_resource_with_children_fails", async () => {
    const client = createClient(port);
    try {
      const parentId = ulid();
      const childId = ulid();
      await client.createResource({ id: parentId });
      await client.createResource({ id: childId, parentId });

      await expect(client.deleteResource(parentId)).rejects.toThrow(/has children/);

      // Deleting child first, then parent should work
      await client.deleteResource(childId);
      await client.deleteResource(parentId);
    } finally {
      await client.close();
    }
  });
});

describe("Tenant isolation", () => {
  test("databases_are_isolated", async () => {
    const clientA = createClient(port, "tenant_a");
    const clientB = createClient(port, "tenant_b");
    try {
      const id = ulid();
      await clientA.createResource({ id, name: "Tenant A Room" });

      // Tenant B should not see tenant A's resource
      const bResources = await clientB.listResources();
      expect(bResources.find((r) => r.id === id)).toBeUndefined();

      // Tenant B can create same ID independently
      await clientB.createResource({ id, name: "Tenant B Room" });

      const aResources = await clientA.listResources();
      const bResources2 = await clientB.listResources();

      expect(aResources.find((r) => r.id === id)!.name).toBe("Tenant A Room");
      expect(bResources2.find((r) => r.id === id)!.name).toBe("Tenant B Room");
    } finally {
      await clientA.close();
      await clientB.close();
    }
  });
});
