use crate::model::*;

use super::availability::compute_saturated_spans;
use super::EngineError;

pub(crate) fn now_ms() -> Ms {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as Ms
}

pub(crate) fn validate_span(span: &Span) -> Result<(), EngineError> {
    use crate::limits::*;
    if span.start < MIN_VALID_TIMESTAMP_MS || span.end > MAX_VALID_TIMESTAMP_MS {
        return Err(EngineError::LimitExceeded("timestamp out of range"));
    }
    if span.duration_ms() > MAX_SPAN_DURATION_MS {
        return Err(EngineError::LimitExceeded("span too wide"));
    }
    Ok(())
}

pub(crate) fn check_no_conflict(rs: &ResourceState, span: &Span, now: Ms) -> Result<(), EngineError> {
    let buffer = rs.buffer_after.unwrap_or(0);
    // Expand the search window to catch:
    // - Existing allocations whose end + buffer > span.start (search backwards by buffer)
    // - Our allocation's end + buffer reaching into existing allocations
    let search_start = (span.start - buffer).max(0);
    let search_end = span.end + buffer;
    let search_span = Span::new(search_start, search_end);

    if rs.capacity <= 1 {
        // Fast path: any overlapping active allocation (with buffer) is a conflict
        for interval in rs.overlapping(&search_span) {
            match &interval.kind {
                IntervalKind::Hold { expires_at } if *expires_at <= now => continue,
                IntervalKind::Hold { .. } | IntervalKind::Booking { .. } => {
                    let effective_end = interval.span.end + buffer;
                    let effective = Span::new(interval.span.start, effective_end);
                    if effective.overlaps(span) {
                        return Err(EngineError::Conflict(interval.id));
                    }
                }
                _ => {}
            }
        }
    } else {
        // Capacity > 1: count overlapping active allocations using sweep line
        let allocs = collect_active_allocs_with_buffer(rs, &search_span, now, buffer);
        let saturated = compute_saturated_spans(&allocs, rs.capacity);
        for sat in &saturated {
            if sat.overlaps(span) {
                return Err(EngineError::CapacityExceeded(rs.capacity));
            }
        }
    }
    Ok(())
}

/// Collect active allocation spans extended by buffer_after.
fn collect_active_allocs_with_buffer(
    rs: &ResourceState,
    query: &Span,
    now: Ms,
    buffer: Ms,
) -> Vec<Span> {
    let mut allocs = Vec::new();
    for interval in rs.overlapping(query) {
        match &interval.kind {
            IntervalKind::Hold { expires_at } if *expires_at <= now => continue,
            IntervalKind::Hold { .. } | IntervalKind::Booking { .. } => {
                let effective_end = interval.span.end + buffer;
                allocs.push(Span::new(interval.span.start, effective_end));
            }
            _ => {}
        }
    }
    allocs.sort_by_key(|s| s.start);
    allocs
}
