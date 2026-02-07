"use client";

import { useEffect, useState, useCallback, useTransition } from "react";
import { toast } from "sonner";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SeatMap } from "@/components/seat-map";
import { CancelBookingDialog } from "@/components/booking-dialog";
import type { Resource, AvailabilitySlot, Booking } from "@/lib/schemas";
import { cn } from "@/lib/utils";

import { seed } from "../actions/seed";
import { getResources } from "../actions/resources";
import { getMultiResourceAvailability } from "../actions/availability";
import { getMultiResourceBookings, batchBookSlots, cancelBooking } from "../actions/bookings";

function toLocalDateString(date: Date): string {
  const offset = date.getTimezoneOffset();
  const local = new Date(date.getTime() - offset * 60000);
  return local.toISOString().slice(0, 10);
}

export default function SeatsPage() {
  const [resources, setResources] = useState<Resource[]>([]);
  const [loading, setLoading] = useState(true);
  const [isPending, startTransition] = useTransition();

  // Which parent resource (plane/screen) is selected
  const [parentId, setParentId] = useState<string | null>(null);

  // Time slot selection
  const [date, setDate] = useState(toLocalDateString(new Date()));
  const [time, setTime] = useState("09:00");

  // Seat data
  const [availability, setAvailability] = useState<Map<string, AvailabilitySlot[]>>(new Map());
  const [bookings, setBookings] = useState<Map<string, Booking[]>>(new Map());
  const [selectedSeats, setSelectedSeats] = useState<Set<string>>(new Set());
  const [bookingLabel, setBookingLabel] = useState("");

  // Cancel dialog
  const [cancelDialogOpen, setCancelDialogOpen] = useState(false);
  const [cancelTarget, setCancelTarget] = useState<Booking | null>(null);

  const parentResource = resources.find((r) => r.id === parentId) ?? null;
  const seats = resources.filter((r) => r.parentId === parentId);
  const slotMinutes = parentResource?.slotMinutes ?? 60;

  // Compute slot times
  const slotStart = new Date(`${date}T${time}`).getTime();
  const slotEnd = slotStart + slotMinutes * 60_000;

  // All parent resources that have children (potential seat maps)
  const parentResources = resources.filter((r) =>
    resources.some((c) => c.parentId === r.id)
  );

  // Load data
  const loadSeatData = useCallback(
    async (seatIds: string[], start: number, end: number) => {
      if (seatIds.length === 0) return;
      try {
        const [availMap, bookMap] = await Promise.all([
          getMultiResourceAvailability(seatIds, start, end),
          getMultiResourceBookings(seatIds),
        ]);
        setAvailability(new Map(Object.entries(availMap)));
        const filtered = new Map<string, Booking[]>();
        for (const [id, bks] of Object.entries(bookMap)) {
          filtered.set(id, (bks as Booking[]).filter((b) => b.start < end && b.end > start));
        }
        setBookings(filtered);
      } catch (err) {
        console.error("Failed to load seat data:", err);
        toast.error("Failed to load seat data");
      }
    },
    []
  );

  // Seed on mount
  useEffect(() => {
    async function init() {
      try {
        await seed();
        const all = await getResources();
        setResources(all);
        // Auto-select first parent that has children
        const parents = all.filter((r) =>
          all.some((c) => c.parentId === r.id)
        );
        // Prefer "Flight" parent over "Theater"
        const plane = parents.find((p) => p.name.toLowerCase().includes("flight"));
        if (plane) {
          setParentId(plane.id);
        } else if (parents.length > 0) {
          setParentId(parents[0].id);
        }
      } catch (err) {
        console.error("Failed to seed:", err);
        toast.error("Failed to connect to deltat. Is it running?");
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  // Reload when parent/date/time changes
  useEffect(() => {
    if (!parentId || !date || !time) return;
    const seatIds = resources.filter((r) => r.parentId === parentId).map((r) => r.id);
    if (seatIds.length === 0) return;
    const start = new Date(`${date}T${time}`).getTime();
    const end = start + (parentResource?.slotMinutes ?? 60) * 60_000;
    if (isNaN(start)) return;
    loadSeatData(seatIds, start, end);
    setSelectedSeats(new Set());
  }, [parentId, date, time, resources]); // eslint-disable-line react-hooks/exhaustive-deps

  function handleToggleSeat(seatId: string) {
    setSelectedSeats((prev) => {
      const next = new Set(prev);
      if (next.has(seatId)) next.delete(seatId);
      else next.add(seatId);
      return next;
    });
  }

  async function handleBookSelected() {
    if (selectedSeats.size === 0) return;
    startTransition(async () => {
      try {
        const slots = Array.from(selectedSeats).map((seatId) => ({
          resourceId: seatId,
          start: slotStart,
          end: slotEnd,
          label: bookingLabel,
        }));
        await batchBookSlots(slots);
        toast.success(`Booked ${slots.length} seat${slots.length > 1 ? "s" : ""}`);
        setSelectedSeats(new Set());
        setBookingLabel("");
        // Reload
        const seatIds = seats.map((r) => r.id);
        await loadSeatData(seatIds, slotStart, slotEnd);
      } catch (err: any) {
        toast.error(err.message ?? "Booking failed — seats may be taken");
      }
    });
  }

  function handleBookingClick(booking: Booking) {
    setCancelTarget(booking);
    setCancelDialogOpen(true);
  }

  async function handleCancelConfirm() {
    if (!cancelTarget) return;
    setCancelDialogOpen(false);
    startTransition(async () => {
      try {
        await cancelBooking(cancelTarget.id);
        toast.success("Booking cancelled");
        const seatIds = seats.map((r) => r.id);
        await loadSeatData(seatIds, slotStart, slotEnd);
      } catch (err: any) {
        toast.error(err.message ?? "Failed to cancel booking");
      }
    });
  }

  function prevDay() {
    const d = new Date(date);
    d.setDate(d.getDate() - 1);
    setDate(toLocalDateString(d));
  }

  function nextDay() {
    const d = new Date(date);
    d.setDate(d.getDate() + 1);
    setDate(toLocalDateString(d));
  }

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center">
        <div className="text-sm text-muted-foreground">Connecting to deltat...</div>
      </div>
    );
  }

  return (
    <div className="flex h-screen flex-col">
      {/* Top bar */}
      <div className="flex items-center justify-between border-b px-6 py-3">
        <div className="flex items-center gap-4">
          <a href="/" className="text-sm text-muted-foreground hover:text-foreground">
            Calendar
          </a>
          <h1 className="text-sm font-semibold">Seat Map</h1>
        </div>

        {/* Parent selector */}
        <div className="flex items-center gap-2">
          {parentResources.map((r) => (
            <Button
              key={r.id}
              variant={r.id === parentId ? "default" : "outline"}
              size="sm"
              onClick={() => setParentId(r.id)}
            >
              {r.name}
            </Button>
          ))}
        </div>
      </div>

      {/* Controls + map */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left: controls */}
        <div className="w-64 shrink-0 border-r p-4 space-y-6">
          <div className="space-y-2">
            <Label className="text-xs font-medium">Date</Label>
            <div className="flex items-center gap-1">
              <Button variant="ghost" size="icon" className="h-8 w-8" onClick={prevDay}>
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <Input
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className="text-sm"
              />
              <Button variant="ghost" size="icon" className="h-8 w-8" onClick={nextDay}>
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>

          <div className="space-y-2">
            <Label className="text-xs font-medium">Start Time</Label>
            <Input
              type="time"
              value={time}
              onChange={(e) => setTime(e.target.value)}
              className="text-sm"
            />
          </div>

          <div className="rounded-md bg-muted/50 p-3 text-xs text-muted-foreground space-y-1">
            <div>Slot: {slotMinutes} min</div>
            <div>
              {new Date(slotStart).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}
              {" – "}
              {new Date(slotEnd).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}
            </div>
          </div>

          {selectedSeats.size > 0 && (
            <div className="space-y-3 border-t pt-4">
              <div className="text-sm font-medium">
                {selectedSeats.size} seat{selectedSeats.size > 1 ? "s" : ""} selected
              </div>
              <div className="flex flex-wrap gap-1">
                {Array.from(selectedSeats)
                  .map((id) => seats.find((s) => s.id === id)?.name ?? id)
                  .sort()
                  .map((name) => (
                    <span
                      key={name}
                      className="rounded bg-emerald-100 px-1.5 py-0.5 text-xs font-medium text-emerald-700"
                    >
                      {name}
                    </span>
                  ))}
              </div>
              <div className="space-y-2">
                <Label htmlFor="seat-label" className="text-xs">Label (optional)</Label>
                <Input
                  id="seat-label"
                  placeholder="e.g. John Smith"
                  value={bookingLabel}
                  onChange={(e) => setBookingLabel(e.target.value)}
                  className="text-sm"
                />
              </div>
              <Button className="w-full" onClick={handleBookSelected} disabled={isPending}>
                Book {selectedSeats.size} Seat{selectedSeats.size > 1 ? "s" : ""}
              </Button>
            </div>
          )}
        </div>

        {/* Right: seat map */}
        <div className="flex-1 flex items-center justify-center overflow-auto p-8">
          {!parentId ? (
            <div className="text-sm text-muted-foreground">
              Select a resource to view its seat map
            </div>
          ) : seats.length === 0 ? (
            <div className="text-sm text-muted-foreground">
              No seats found for this resource
            </div>
          ) : (
            <SeatMap
              seats={seats.map((s) => ({ id: s.id, name: s.name }))}
              availabilityByResource={availability}
              bookingsByResource={bookings}
              slotStart={slotStart}
              slotEnd={slotEnd}
              selectedIds={selectedSeats}
              onToggle={handleToggleSeat}
              onBookingClick={handleBookingClick}
            />
          )}
        </div>
      </div>

      {/* Cancel dialog */}
      {cancelTarget && (
        <CancelBookingDialog
          open={cancelDialogOpen}
          onOpenChange={setCancelDialogOpen}
          resourceName={
            seats.find((s) => s.id === cancelTarget.resourceId)?.name ?? ""
          }
          start={cancelTarget.start}
          end={cancelTarget.end}
          onConfirm={handleCancelConfirm}
        />
      )}

      {isPending && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/50">
          <div className="text-sm text-muted-foreground">Working...</div>
        </div>
      )}
    </div>
  );
}
