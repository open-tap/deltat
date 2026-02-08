import postgres from "postgres";

// ── Types ────────────────────────────────────────────────────────

export interface Resource {
  id: string;
  parent_id: string | null;
  name: string | null;
  capacity: number;
  buffer_after: number | null;
}

export interface Rule {
  id: string;
  resource_id: string;
  start: number;
  end: number;
  blocking: boolean;
}

export interface Booking {
  id: string;
  resource_id: string;
  start: number;
  end: number;
  label: string | null;
}

export interface Hold {
  id: string;
  resource_id: string;
  start: number;
  end: number;
  expires_at: number;
}

export interface Slot {
  start: number;
  end: number;
}

/** Matches serde default externally-tagged enum serialization. */
export type DeltaTEvent = { [key: string]: unknown };

// ── Client ───────────────────────────────────────────────────────

export interface DeltaTOptions {
  host?: string;
  port?: number;
  database?: string;
  password?: string;
  username?: string;
}

export class DeltaT {
  private sql: postgres.Sql;

  constructor(opts: DeltaTOptions = {}) {
    this.sql = postgres({
      host: opts.host ?? "localhost",
      port: opts.port ?? 5433,
      database: opts.database ?? "default",
      password: opts.password ?? "deltat",
      username: opts.username ?? "deltat",
      fetch_types: false,
    });
  }

  // ── Resources ────────────────────────────────────────────────

  async createResource(opts: {
    id: string;
    parentId?: string | null;
    name?: string | null;
    capacity?: number;
    bufferAfter?: number | null;
  }): Promise<string> {
    const cols = ["id"];
    const vals = [opts.id];

    if (opts.parentId !== undefined) {
      cols.push("parent_id");
      vals.push(opts.parentId as string);
    }
    if (opts.name !== undefined) {
      cols.push("name");
      vals.push(opts.name as string);
    }
    if (opts.capacity !== undefined) {
      cols.push("capacity");
      vals.push(String(opts.capacity));
    }
    if (opts.bufferAfter !== undefined) {
      cols.push("buffer_after");
      vals.push(opts.bufferAfter === null ? null! : String(opts.bufferAfter));
    }

    await this.sql.unsafe(
      `INSERT INTO resources (${cols.join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})`,
      vals,
    );
    return opts.id;
  }

  async updateResource(
    id: string,
    opts: { name?: string; capacity?: number; bufferAfter?: number | null },
  ): Promise<void> {
    const sets: string[] = [];
    const vals: (string | number | null)[] = [];
    let idx = 1;

    if (opts.name !== undefined) {
      sets.push(`name = $${idx++}`);
      vals.push(opts.name);
    }
    if (opts.capacity !== undefined) {
      sets.push(`capacity = $${idx++}`);
      vals.push(opts.capacity);
    }
    if (opts.bufferAfter !== undefined) {
      sets.push(`buffer_after = $${idx++}`);
      vals.push(opts.bufferAfter);
    }

    vals.push(id);
    await this.sql.unsafe(
      `UPDATE resources SET ${sets.join(", ")} WHERE id = $${idx}`,
      vals,
    );
  }

  async deleteResource(id: string): Promise<void> {
    await this.sql.unsafe(`DELETE FROM resources WHERE id = $1`, [id]);
  }

  async listResources(parentId?: string | null): Promise<Resource[]> {
    let rows: postgres.Row[];
    if (parentId === undefined) {
      rows = await this.sql.unsafe(`SELECT * FROM resources`);
    } else if (parentId === null) {
      rows = await this.sql.unsafe(
        `SELECT * FROM resources WHERE parent_id IS NULL`,
      );
    } else {
      rows = await this.sql.unsafe(
        `SELECT * FROM resources WHERE parent_id = $1`,
        [parentId],
      );
    }
    return rows.map(toResource);
  }

  // ── Rules ────────────────────────────────────────────────────

  async addRule(opts: {
    id: string;
    resourceId: string;
    start: number;
    end: number;
    blocking?: boolean;
  }): Promise<string> {
    await this.sql.unsafe(
      `INSERT INTO rules (id, resource_id, start, "end", blocking) VALUES ($1, $2, $3, $4, $5)`,
      [opts.id, opts.resourceId, opts.start, opts.end, opts.blocking ?? false],
    );
    return opts.id;
  }

  async updateRule(
    id: string,
    opts: { start: number; end: number; blocking?: boolean },
  ): Promise<void> {
    await this.sql.unsafe(
      `UPDATE rules SET start = $1, "end" = $2, blocking = $3 WHERE id = $4`,
      [opts.start, opts.end, opts.blocking ?? false, id],
    );
  }

  async deleteRule(id: string): Promise<void> {
    await this.sql.unsafe(`DELETE FROM rules WHERE id = $1`, [id]);
  }

  async getRules(resourceId: string): Promise<Rule[]> {
    const rows = await this.sql.unsafe(
      `SELECT * FROM rules WHERE resource_id = $1`,
      [resourceId],
    );
    return rows.map(toRule);
  }

  // ── Bookings ─────────────────────────────────────────────────

  async book(opts: {
    id: string;
    resourceId: string;
    start: number;
    end: number;
    label?: string | null;
  }): Promise<string> {
    if (opts.label !== undefined) {
      await this.sql.unsafe(
        `INSERT INTO bookings (id, resource_id, start, "end", label) VALUES ($1, $2, $3, $4, $5)`,
        [opts.id, opts.resourceId, opts.start, opts.end, opts.label],
      );
    } else {
      await this.sql.unsafe(
        `INSERT INTO bookings (id, resource_id, start, "end") VALUES ($1, $2, $3, $4)`,
        [opts.id, opts.resourceId, opts.start, opts.end],
      );
    }
    return opts.id;
  }

  async batchBook(
    bookings: Array<{
      id: string;
      resourceId: string;
      start: number;
      end: number;
      label?: string | null;
    }>,
  ): Promise<string[]> {
    const hasLabels = bookings.some((b) => b.label !== undefined);
    const cols = hasLabels
      ? `(id, resource_id, start, "end", label)`
      : `(id, resource_id, start, "end")`;

    const placeholders: string[] = [];
    const vals: (string | number | null)[] = [];
    let idx = 1;

    for (const b of bookings) {
      if (hasLabels) {
        placeholders.push(
          `($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`,
        );
        vals.push(b.id, b.resourceId, b.start, b.end, b.label ?? null);
      } else {
        placeholders.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++})`);
        vals.push(b.id, b.resourceId, b.start, b.end);
      }
    }

    await this.sql.unsafe(
      `INSERT INTO bookings ${cols} VALUES ${placeholders.join(", ")}`,
      vals,
    );
    return bookings.map((b) => b.id);
  }

  async cancelBooking(id: string): Promise<void> {
    await this.sql.unsafe(`DELETE FROM bookings WHERE id = $1`, [id]);
  }

  async getBookings(resourceId: string): Promise<Booking[]> {
    const rows = await this.sql.unsafe(
      `SELECT * FROM bookings WHERE resource_id = $1`,
      [resourceId],
    );
    return rows.map(toBooking);
  }

  // ── Holds ────────────────────────────────────────────────────

  async placeHold(opts: {
    id: string;
    resourceId: string;
    start: number;
    end: number;
    expiresAt: number;
  }): Promise<string> {
    await this.sql.unsafe(
      `INSERT INTO holds (id, resource_id, start, "end", expires_at) VALUES ($1, $2, $3, $4, $5)`,
      [opts.id, opts.resourceId, opts.start, opts.end, opts.expiresAt],
    );
    return opts.id;
  }

  async releaseHold(id: string): Promise<void> {
    await this.sql.unsafe(`DELETE FROM holds WHERE id = $1`, [id]);
  }

  async getHolds(resourceId: string): Promise<Hold[]> {
    const rows = await this.sql.unsafe(
      `SELECT * FROM holds WHERE resource_id = $1`,
      [resourceId],
    );
    return rows.map(toHold);
  }

  // ── Availability ─────────────────────────────────────────────

  async getAvailability(
    resourceId: string,
    start: number,
    end: number,
    minDuration?: number,
  ): Promise<Slot[]> {
    let sql = `SELECT * FROM availability WHERE resource_id = $1 AND start >= $2 AND "end" <= $3`;
    const vals: (string | number)[] = [resourceId, start, end];

    if (minDuration !== undefined) {
      sql += ` AND min_duration = $4`;
      vals.push(minDuration);
    }

    const rows = await this.sql.unsafe(sql, vals);
    return rows.map(toSlot);
  }

  async getMultiAvailability(
    resourceIds: string[],
    start: number,
    end: number,
    opts?: { minAvailable?: number; minDuration?: number },
  ): Promise<Slot[]> {
    const inList = resourceIds.map((_, i) => `$${i + 1}`).join(", ");
    let idx = resourceIds.length + 1;

    let sql = `SELECT * FROM availability WHERE resource_id IN (${inList}) AND start >= $${idx++} AND "end" <= $${idx++}`;
    const vals: (string | number)[] = [...resourceIds, start, end];

    if (opts?.minAvailable !== undefined) {
      sql += ` AND min_available = $${idx++}`;
      vals.push(opts.minAvailable);
    }
    if (opts?.minDuration !== undefined) {
      sql += ` AND min_duration = $${idx++}`;
      vals.push(opts.minDuration);
    }

    const rows = await this.sql.unsafe(sql, vals);
    return rows.map(toSlot);
  }

  // ── Subscriptions (LISTEN/NOTIFY) ────────────────────────────

  async subscribe(
    resourceId: string,
    callback: (event: DeltaTEvent) => void,
  ): Promise<{ unsubscribe: () => Promise<void> }> {
    const channel = `resource_${resourceId}`;
    const meta = await this.sql.listen(channel, (payload) => {
      try {
        callback(JSON.parse(payload));
      } catch {
        // ignore malformed payloads
      }
    });
    return { unsubscribe: () => meta.unlisten() };
  }

  // ── Lifecycle ────────────────────────────────────────────────

  async close(): Promise<void> {
    await this.sql.end();
  }
}

// ── Row mappers ──────────────────────────────────────────────────

function toResource(row: postgres.Row): Resource {
  return {
    id: row.id,
    parent_id: row.parent_id ?? null,
    name: row.name ?? null,
    capacity: Number(row.capacity),
    buffer_after: row.buffer_after != null ? Number(row.buffer_after) : null,
  };
}

function toRule(row: postgres.Row): Rule {
  return {
    id: row.id,
    resource_id: row.resource_id,
    start: Number(row.start),
    end: Number(row.end),
    blocking: row.blocking === true || row.blocking === "t",
  };
}

function toBooking(row: postgres.Row): Booking {
  return {
    id: row.id,
    resource_id: row.resource_id,
    start: Number(row.start),
    end: Number(row.end),
    label: row.label ?? null,
  };
}

function toHold(row: postgres.Row): Hold {
  return {
    id: row.id,
    resource_id: row.resource_id,
    start: Number(row.start),
    end: Number(row.end),
    expires_at: Number(row.expires_at),
  };
}

function toSlot(row: postgres.Row): Slot {
  return {
    start: Number(row.start),
    end: Number(row.end),
  };
}
