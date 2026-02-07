"use server";

import * as db from "@/lib/db";
import * as store from "@/lib/store";
import type { Resource, Rule } from "@/lib/schemas";

export async function seed(): Promise<{
  resources: Resource[];
  rules: Rule[];
}> {
  if (store.isSeeded()) {
    return { resources: store.getResources(), rules: store.getRules() };
  }

  const now = new Date();
  now.setHours(0, 0, 0, 0);
  const DAY = 86_400_000;

  // Create resource hierarchy
  const theaterId = await db.createResource(null);
  const screen1Id = await db.createResource(theaterId);
  const seatA1Id = await db.createResource(screen1Id);
  const seatA2Id = await db.createResource(screen1Id);
  const seatA3Id = await db.createResource(screen1Id);
  const screen2Id = await db.createResource(theaterId);
  const seatB1Id = await db.createResource(screen2Id);
  const seatB2Id = await db.createResource(screen2Id);

  const resources: Resource[] = [
    { id: theaterId, parentId: null, name: "Theater", slotMinutes: 60, bufferMinutes: 0 },
    { id: screen1Id, parentId: theaterId, name: "Screen 1", slotMinutes: 60, bufferMinutes: 0 },
    { id: seatA1Id, parentId: screen1Id, name: "Seat A1", slotMinutes: 60, bufferMinutes: 0 },
    { id: seatA2Id, parentId: screen1Id, name: "Seat A2", slotMinutes: 60, bufferMinutes: 0 },
    { id: seatA3Id, parentId: screen1Id, name: "Seat A3", slotMinutes: 60, bufferMinutes: 0 },
    { id: screen2Id, parentId: theaterId, name: "Screen 2", slotMinutes: 60, bufferMinutes: 0 },
    { id: seatB1Id, parentId: screen2Id, name: "Seat B1", slotMinutes: 60, bufferMinutes: 0 },
    { id: seatB2Id, parentId: screen2Id, name: "Seat B2", slotMinutes: 60, bufferMinutes: 0 },
  ];

  for (const r of resources) {
    store.setResource(r);
  }

  // Add availability rule: 9am-9pm for next 30 days on theater (inherits to children)
  const rules: Rule[] = [];
  for (let i = 0; i < 30; i++) {
    const dayStart = now.getTime() + i * DAY + 9 * 3_600_000; // 9am
    const dayEnd = now.getTime() + i * DAY + 21 * 3_600_000; // 9pm
    const ruleId = await db.addRule(theaterId, dayStart, dayEnd, false);
    const rule: Rule = {
      id: ruleId,
      resourceId: theaterId,
      start: dayStart,
      end: dayEnd,
      blocking: false,
    };
    store.setRule(rule);
    rules.push(rule);
  }

  // ── Plane: 10 rows × 6 columns (A-F), 3-3 layout ──
  const planeId = await db.createResource(null);
  const planeResource: Resource = {
    id: planeId, parentId: null, name: "Flight AA-1234",
    slotMinutes: 180, bufferMinutes: 30,
  };
  resources.push(planeResource);
  store.setResource(planeResource);

  const cols = ["A", "B", "C", "D", "E", "F"];
  for (let row = 1; row <= 10; row++) {
    for (const col of cols) {
      const seatId = await db.createResource(planeId);
      const seat: Resource = {
        id: seatId, parentId: planeId, name: `${row}${col}`,
        slotMinutes: 180, bufferMinutes: 30,
      };
      resources.push(seat);
      store.setResource(seat);
    }
  }

  // Add availability for the plane: daily 6am-11pm for next 30 days
  for (let i = 0; i < 30; i++) {
    const dayStart = now.getTime() + i * DAY + 6 * 3_600_000;
    const dayEnd = now.getTime() + i * DAY + 23 * 3_600_000;
    const ruleId = await db.addRule(planeId, dayStart, dayEnd, false);
    const rule: Rule = {
      id: ruleId, resourceId: planeId,
      start: dayStart, end: dayEnd, blocking: false,
    };
    store.setRule(rule);
    rules.push(rule);
  }

  store.markSeeded();
  return { resources, rules };
}

export async function checkSeeded(): Promise<boolean> {
  return store.isSeeded();
}
