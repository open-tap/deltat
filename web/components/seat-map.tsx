"use client";

import { useMemo } from "react";
import { cn } from "@/lib/utils";
import type { AvailabilitySlot, Booking } from "@/lib/schemas";

interface SeatInfo {
  id: string;
  name: string;
}

interface SeatMapProps {
  seats: SeatInfo[];
  availabilityByResource: Map<string, AvailabilitySlot[]>;
  bookingsByResource: Map<string, Booking[]>;
  slotStart: number;
  slotEnd: number;
  selectedIds: Set<string>;
  onToggle: (seatId: string) => void;
  onBookingClick: (booking: Booking) => void;
}

/** Parse seat name like "1A" into { row: 1, col: "A" }.
 *  Falls back to sequential grid for non-standard names. */
function parseSeatName(name: string): { row: number; col: string } | null {
  const m = name.match(/^(\d+)([A-Za-z]+)$/);
  if (m) return { row: parseInt(m[1], 10), col: m[2].toUpperCase() };
  return null;
}

export function SeatMap({
  seats,
  availabilityByResource,
  bookingsByResource,
  slotStart,
  slotEnd,
  selectedIds,
  onToggle,
  onBookingClick,
}: SeatMapProps) {
  // Parse grid structure from seat names
  const grid = useMemo(() => {
    const parsed = seats.map((s) => ({ ...s, parsed: parseSeatName(s.name) }));
    const hasParsed = parsed.every((p) => p.parsed !== null);

    if (!hasParsed) {
      // Fallback: flat grid, 6 columns
      const cols = 6;
      const rows: SeatInfo[][] = [];
      for (let i = 0; i < parsed.length; i += cols) {
        rows.push(parsed.slice(i, i + cols));
      }
      return { rows, columns: Array.from({ length: cols }, (_, i) => String(i + 1)), rowLabels: rows.map((_, i) => String(i + 1)) };
    }

    // Build structured grid
    const allCols = [...new Set(parsed.map((p) => p.parsed!.col))].sort();
    const allRows = [...new Set(parsed.map((p) => p.parsed!.row))].sort((a, b) => a - b);

    const seatByPos = new Map<string, SeatInfo>();
    for (const p of parsed) {
      seatByPos.set(`${p.parsed!.row}-${p.parsed!.col}`, { id: p.id, name: p.name });
    }

    const gridRows: (SeatInfo | null)[][] = allRows.map((row) =>
      allCols.map((col) => seatByPos.get(`${row}-${col}`) ?? null)
    );

    return {
      rows: gridRows,
      columns: allCols,
      rowLabels: allRows.map(String),
    };
  }, [seats]);

  // Compute per-seat status
  const seatStatus = useMemo(() => {
    const status = new Map<string, "available" | "booked" | "unavailable">();
    for (const seat of seats) {
      const avail = availabilityByResource.get(seat.id) ?? [];
      const bks = bookingsByResource.get(seat.id) ?? [];

      // Check if any booking overlaps with our slot
      const hasBooking = bks.some(
        (b) => b.start < slotEnd && b.end > slotStart
      );
      if (hasBooking) {
        status.set(seat.id, "booked");
        continue;
      }

      // Check if availability covers the slot
      const isAvailable = avail.some(
        (a) => a.start <= slotStart && a.end >= slotEnd
      );
      status.set(seat.id, isAvailable ? "available" : "unavailable");
    }
    return status;
  }, [seats, availabilityByResource, bookingsByResource, slotStart, slotEnd]);

  // Detect aisle position (between C and D for 6-col, or middle)
  const aisleAfter = useMemo(() => {
    const cols = grid.columns;
    if (cols.length === 6) return 2; // after 3rd column (index 2)
    if (cols.length === 4) return 1;
    return Math.floor(cols.length / 2) - 1;
  }, [grid.columns]);

  return (
    <div className="flex flex-col items-center gap-1">
      {/* Column headers */}
      <div className="flex items-center gap-1">
        <div className="w-8" /> {/* Row label spacer */}
        {grid.columns.map((col, ci) => (
          <div key={col} className={cn("w-10 text-center text-xs font-medium text-muted-foreground", ci === aisleAfter && "mr-6")}>
            {col}
          </div>
        ))}
      </div>

      {/* Seat rows */}
      {grid.rows.map((row, ri) => (
        <div key={ri} className="flex items-center gap-1">
          <div className="w-8 text-right text-xs font-medium text-muted-foreground pr-1">
            {grid.rowLabels[ri]}
          </div>
          {row.map((seat, ci) => {
            if (!seat) {
              return (
                <div
                  key={`empty-${ri}-${ci}`}
                  className={cn("w-10 h-10", ci === aisleAfter && "mr-6")}
                />
              );
            }

            const st = seatStatus.get(seat.id) ?? "unavailable";
            const isSelected = selectedIds.has(seat.id);
            const booking = (bookingsByResource.get(seat.id) ?? []).find(
              (b) => b.start < slotEnd && b.end > slotStart
            );

            return (
              <button
                key={seat.id}
                className={cn(
                  "w-10 h-10 rounded-md text-[11px] font-medium transition-all border",
                  st === "unavailable" && "bg-muted/40 text-muted-foreground/50 border-transparent cursor-not-allowed",
                  st === "available" && !isSelected && "bg-emerald-50 text-emerald-700 border-emerald-200 hover:bg-emerald-100 hover:border-emerald-300 cursor-pointer",
                  st === "available" && isSelected && "bg-emerald-500 text-white border-emerald-600 ring-2 ring-emerald-300 cursor-pointer",
                  st === "booked" && "bg-blue-100 text-blue-700 border-blue-200 hover:bg-blue-200 cursor-pointer",
                  ci === aisleAfter && "mr-6"
                )}
                disabled={st === "unavailable"}
                onClick={() => {
                  if (st === "booked" && booking) {
                    onBookingClick(booking);
                  } else if (st === "available") {
                    onToggle(seat.id);
                  }
                }}
                title={
                  st === "booked"
                    ? `${seat.name} — ${booking?.label || "Booked"}`
                    : st === "available"
                    ? `${seat.name} — Available`
                    : `${seat.name} — Unavailable`
                }
              >
                {seat.name}
              </button>
            );
          })}
        </div>
      ))}

      {/* Legend */}
      <div className="flex items-center gap-4 mt-4 text-xs text-muted-foreground">
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-emerald-50 border border-emerald-200" />
          <span>Available</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-emerald-500 border border-emerald-600" />
          <span>Selected</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-blue-100 border border-blue-200" />
          <span>Booked</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-muted/40 border border-transparent" />
          <span>Unavailable</span>
        </div>
      </div>
    </div>
  );
}
