use crate::model::*;

// ── Availability Algorithm ────────────────────────────────────────

/// Compute raw free intervals for a resource using its unified interval list
/// plus inherited rules from ancestors.
///
/// Non-blocking: OVERRIDE — if resource has own non-blocking rules, use those;
/// otherwise fall back to inherited_non_blocking.
/// Blocking: ACCUMULATE — own blocking + inherited_blocking are all subtracted.
pub fn availability(
    resource: &ResourceState,
    query: &Span,
    inherited_non_blocking: &[Span],
    inherited_blocking: &[Span],
    now: Ms,
) -> Vec<Span> {
    let buffer = resource.buffer_after.unwrap_or(0);
    let capacity = resource.capacity;

    // Step 1: Determine base non-blocking spans (using binary search)
    let mut own_non_blocking: Vec<Span> = Vec::new();
    let mut own_blocking: Vec<Span> = Vec::new();
    let mut active_allocs: Vec<Span> = Vec::new();

    for interval in resource.overlapping(query) {
        let clamped = Span::new(
            interval.span.start.max(query.start),
            interval.span.end.min(query.end),
        );
        match &interval.kind {
            IntervalKind::NonBlocking => own_non_blocking.push(clamped),
            IntervalKind::Blocking => own_blocking.push(clamped),
            IntervalKind::Hold { expires_at } if *expires_at > now => {
                let effective_end = interval.span.end + buffer;
                active_allocs.push(Span::new(interval.span.start, effective_end));
            }
            IntervalKind::Booking { .. } => {
                let effective_end = interval.span.end + buffer;
                active_allocs.push(Span::new(interval.span.start, effective_end));
            }
            _ => {} // expired hold
        }
    }

    let mut free = if own_non_blocking.is_empty() {
        inherited_non_blocking.to_vec()
    } else {
        own_non_blocking
    };

    free.sort_by_key(|s| s.start);
    free = merge_overlapping(&free);

    // Step 2: Collect ALL blocking rules (own + inherited)
    let mut blocked = own_blocking;
    blocked.extend_from_slice(inherited_blocking);
    blocked.sort_by_key(|s| s.start);

    if !blocked.is_empty() {
        free = subtract_intervals(&free, &blocked);
    }

    // Step 3: Subtract active allocations (with capacity awareness)
    if !active_allocs.is_empty() {
        active_allocs.sort_by_key(|s| s.start);
        if capacity <= 1 {
            free = subtract_intervals(&free, &active_allocs);
        } else {
            let saturated = compute_saturated_spans(&active_allocs, capacity);
            if !saturated.is_empty() {
                free = subtract_intervals(&free, &saturated);
            }
        }
    }

    free
}

/// Merge sorted overlapping/adjacent intervals into disjoint intervals.
pub fn merge_overlapping(sorted: &[Span]) -> Vec<Span> {
    let mut merged: Vec<Span> = Vec::new();
    for &span in sorted {
        if let Some(last) = merged.last_mut()
            && span.start <= last.end {
                last.end = last.end.max(span.end);
                continue;
            }
        merged.push(span);
    }
    merged
}

pub fn subtract_intervals(base: &[Span], to_remove: &[Span]) -> Vec<Span> {
    let mut result = Vec::new();
    let mut ri = 0;

    for &b in base {
        let mut current_start = b.start;
        let current_end = b.end;

        while ri < to_remove.len() && to_remove[ri].end <= current_start {
            ri += 1;
        }

        let mut j = ri;
        while j < to_remove.len() && to_remove[j].start < current_end {
            let r = &to_remove[j];
            if r.start > current_start {
                result.push(Span::new(current_start, r.start));
            }
            current_start = current_start.max(r.end);
            j += 1;
        }

        if current_start < current_end {
            result.push(Span::new(current_start, current_end));
        }
    }

    result
}

/// Sweep-line algorithm: find time ranges where allocation count >= capacity.
/// Returns sorted, merged spans representing fully-saturated time ranges.
pub fn compute_saturated_spans(allocs: &[Span], capacity: u32) -> Vec<Span> {
    if allocs.is_empty() || capacity == 0 {
        return Vec::new();
    }
    if capacity == 1 {
        return merge_overlapping(allocs);
    }

    // Build sweep-line events: +1 at start, -1 at end
    let mut events: Vec<(Ms, i32)> = Vec::with_capacity(allocs.len() * 2);
    for a in allocs {
        events.push((a.start, 1));
        events.push((a.end, -1));
    }
    events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut result = Vec::new();
    let mut count: u32 = 0;
    let mut saturated_start: Option<Ms> = None;

    for (time, delta) in &events {
        if *delta > 0 {
            count += *delta as u32;
        } else {
            count -= (-*delta) as u32;
        }

        if count >= capacity && saturated_start.is_none() {
            saturated_start = Some(*time);
        } else if count < capacity
            && let Some(start) = saturated_start.take()
            && *time > start {
                result.push(Span::new(start, *time));
            }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: Ms = 3_600_000;
    const M: Ms = 60_000;

    fn make_resource(intervals: Vec<Interval>) -> ResourceState {
        make_resource_with_capacity(intervals, 1, None)
    }

    fn make_resource_with_capacity(intervals: Vec<Interval>, capacity: u32, buffer_after: Option<Ms>) -> ResourceState {
        let mut rs = ResourceState::new(ulid::Ulid::new(), None, None, capacity, buffer_after);
        for i in intervals {
            rs.insert_interval(i);
        }
        rs
    }

    fn rule(start: Ms, end: Ms, blocking: bool) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: if blocking {
                IntervalKind::Blocking
            } else {
                IntervalKind::NonBlocking
            },
        }
    }

    fn booking(start: Ms, end: Ms) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: IntervalKind::Booking { label: None },
        }
    }

    fn hold(start: Ms, end: Ms, expires_at: Ms) -> Interval {
        Interval {
            id: ulid::Ulid::new(),
            span: Span::new(start, end),
            kind: IntervalKind::Hold { expires_at },
        }
    }

    // ── subtract_intervals ────────────────────────────────

    #[test]
    fn subtract_no_overlap() {
        let base = vec![Span::new(100, 200), Span::new(300, 400)];
        let remove = vec![Span::new(200, 300)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, base);
    }

    #[test]
    fn subtract_full_overlap() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(50, 250)];
        let result = subtract_intervals(&base, &remove);
        assert!(result.is_empty());
    }

    #[test]
    fn subtract_partial_left() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(50, 150)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(150, 200)]);
    }

    #[test]
    fn subtract_partial_right() {
        let base = vec![Span::new(100, 200)];
        let remove = vec![Span::new(150, 250)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(100, 150)]);
    }

    #[test]
    fn subtract_middle_punch() {
        let base = vec![Span::new(100, 300)];
        let remove = vec![Span::new(150, 200)];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(result, vec![Span::new(100, 150), Span::new(200, 300)]);
    }

    #[test]
    fn subtract_multiple_punches() {
        let base = vec![Span::new(0, 1000)];
        let remove = vec![
            Span::new(100, 200),
            Span::new(400, 500),
            Span::new(800, 900),
        ];
        let result = subtract_intervals(&base, &remove);
        assert_eq!(
            result,
            vec![
                Span::new(0, 100),
                Span::new(200, 400),
                Span::new(500, 800),
                Span::new(900, 1000),
            ]
        );
    }

    // ── merge_overlapping ────────────────────────────────

    #[test]
    fn merge_overlapping_basic() {
        let spans = vec![
            Span::new(100, 300),
            Span::new(200, 400),
            Span::new(500, 600),
        ];
        let merged = merge_overlapping(&spans);
        assert_eq!(merged, vec![Span::new(100, 400), Span::new(500, 600)]);
    }

    #[test]
    fn merge_overlapping_adjacent() {
        let spans = vec![Span::new(100, 200), Span::new(200, 300)];
        let merged = merge_overlapping(&spans);
        assert_eq!(merged, vec![Span::new(100, 300)]);
    }

    // ── availability (pure function, no hierarchy) ────────

    #[test]
    fn availability_basic_raw_intervals() {
        let nine = 9 * H;
        let twelve = 12 * H;
        let ten = 10 * H;
        let ten_thirty = ten + 30 * M;

        let rs = make_resource(vec![
            rule(nine, twelve, false),
            booking(ten, ten_thirty),
        ]);
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &[], &[], 0);
        assert_eq!(free.len(), 2);
        assert_eq!(free[0], Span::new(nine, ten));
        assert_eq!(free[1], Span::new(ten_thirty, twelve));
    }

    #[test]
    fn availability_with_inherited_non_blocking() {
        let rs = make_resource(vec![]);
        let inherited = vec![Span::new(9 * H, 17 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &inherited, &[], 0);
        assert_eq!(free, vec![Span::new(9 * H, 17 * H)]);
    }

    #[test]
    fn availability_own_overrides_inherited() {
        let rs = make_resource(vec![rule(14 * H, 16 * H, false)]);
        let inherited = vec![Span::new(9 * H, 17 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &inherited, &[], 0);
        assert_eq!(free, vec![Span::new(14 * H, 16 * H)]);
    }

    #[test]
    fn availability_inherited_blocking_accumulates() {
        let rs = make_resource(vec![rule(9 * H, 17 * H, false)]);
        let inherited_blocking = vec![Span::new(12 * H, 13 * H)];
        let query = Span::new(0, 24 * H);
        let free = availability(&rs, &query, &[], &inherited_blocking, 0);
        assert_eq!(
            free,
            vec![Span::new(9 * H, 12 * H), Span::new(13 * H, 17 * H)]
        );
    }

    #[test]
    fn expired_hold_not_counted() {
        let nine = 9 * H;
        let ten = 10 * H;

        let rs = make_resource(vec![
            rule(nine, ten, false),
            hold(nine, ten, 1), // expired
        ]);
        let query = Span::new(0, ten + H);
        let now = 1000;
        let free = availability(&rs, &query, &[], &[], now);
        assert_eq!(free, vec![Span::new(nine, ten)]);
    }

    #[test]
    fn blocking_rule_subtracts() {
        let nine = 9 * H;
        let ten = 10 * H;
        let eleven = 11 * H;
        let twelve = 12 * H;

        let rs = make_resource(vec![
            rule(nine, twelve, false),
            rule(ten, eleven, true),
        ]);
        let query = Span::new(0, twelve + H);
        let free = availability(&rs, &query, &[], &[], 0);
        assert_eq!(
            free,
            vec![Span::new(nine, ten), Span::new(eleven, twelve)]
        );
    }

    // ── compute_saturated_spans ────────────────────────────

    #[test]
    fn saturated_spans_basic() {
        let allocs = vec![Span::new(0, 100), Span::new(50, 150)];
        let sat = compute_saturated_spans(&allocs, 2);
        assert_eq!(sat, vec![Span::new(50, 100)]);
    }

    #[test]
    fn saturated_spans_no_overlap() {
        let allocs = vec![Span::new(0, 100), Span::new(200, 300)];
        let sat = compute_saturated_spans(&allocs, 2);
        assert!(sat.is_empty());
    }

    #[test]
    fn saturated_spans_capacity_one() {
        let allocs = vec![Span::new(0, 100), Span::new(200, 300)];
        let sat = compute_saturated_spans(&allocs, 1);
        assert_eq!(sat, vec![Span::new(0, 100), Span::new(200, 300)]);
    }

    #[test]
    fn saturated_spans_three_overlap_capacity_three() {
        let allocs = vec![
            Span::new(0, 100),
            Span::new(25, 75),
            Span::new(50, 150),
        ];
        let sat = compute_saturated_spans(&allocs, 3);
        assert_eq!(sat, vec![Span::new(50, 75)]);
    }

    #[test]
    fn saturated_spans_empty() {
        let sat = compute_saturated_spans(&[], 5);
        assert!(sat.is_empty());
    }
}
