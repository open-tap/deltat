import type { Resource, Rule, Booking } from "./schemas";

// Server-side in-memory store for metadata that deltat doesn't track
// (resource names, local booking/rule references for display)

const resources = new Map<string, Resource>();
const rules = new Map<string, Rule>();
const bookings = new Map<string, Booking>();
let seeded = false;

export function getResources(): Resource[] {
  return Array.from(resources.values());
}

export function getResource(id: string): Resource | undefined {
  return resources.get(id);
}

export function setResource(resource: Resource): void {
  resources.set(resource.id, resource);
}

export function removeResource(id: string): void {
  resources.delete(id);
  // Also remove child resources
  for (const [childId, r] of resources) {
    if (r.parentId === id) {
      removeResource(childId);
    }
  }
  // Remove associated rules and bookings
  for (const [ruleId, rule] of rules) {
    if (rule.resourceId === id) rules.delete(ruleId);
  }
  for (const [bookingId, booking] of bookings) {
    if (booking.resourceId === id) bookings.delete(bookingId);
  }
}

export function getRules(): Rule[] {
  return Array.from(rules.values());
}

export function getRulesForResource(resourceId: string): Rule[] {
  return Array.from(rules.values()).filter((r) => r.resourceId === resourceId);
}

export function setRule(rule: Rule): void {
  rules.set(rule.id, rule);
}

export function removeRule(id: string): void {
  rules.delete(id);
}

export function getBookings(): Booking[] {
  return Array.from(bookings.values());
}

export function getBookingsForResource(resourceId: string): Booking[] {
  return Array.from(bookings.values()).filter(
    (b) => b.resourceId === resourceId
  );
}

export function setBooking(booking: Booking): void {
  bookings.set(booking.id, booking);
}

export function removeBooking(id: string): void {
  bookings.delete(id);
}

export function isSeeded(): boolean {
  return seeded;
}

export function markSeeded(): void {
  seeded = true;
}
