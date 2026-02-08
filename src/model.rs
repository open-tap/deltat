use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Unix milliseconds — the only time type.
pub type Ms = i64;

/// Half-open interval `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: Ms,
    pub end: Ms,
}

impl Span {
    pub fn new(start: Ms, end: Ms) -> Self {
        debug_assert!(start < end, "Span start must be before end");
        Self { start, end }
    }

    pub fn duration_ms(&self) -> Ms {
        self.end - self.start
    }

    pub fn overlaps(&self, other: &Span) -> bool {
        self.start < other.end && other.start < self.end
    }

    #[allow(dead_code)]
    pub fn contains_instant(&self, t: Ms) -> bool {
        self.start <= t && t < self.end
    }

    /// Returns true if `self` fully contains `other`.
    #[allow(dead_code)]
    pub fn contains_span(&self, other: &Span) -> bool {
        self.start <= other.start && other.end <= self.end
    }
}

/// What an interval represents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntervalKind {
    /// Opens availability for this time range.
    NonBlocking,
    /// Closes availability for this time range.
    Blocking,
    /// Temporary reservation with expiration.
    Hold { expires_at: Ms },
    /// Permanent reservation with optional label.
    Booking { label: Option<String> },
}

/// A single interval on a resource — rules, holds, and bookings are all just intervals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Interval {
    pub id: Ulid,
    pub span: Span,
    pub kind: IntervalKind,
}

impl Interval {
    #[allow(dead_code)]
    pub fn is_rule(&self) -> bool {
        matches!(self.kind, IntervalKind::NonBlocking | IntervalKind::Blocking)
    }

    #[allow(dead_code)]
    pub fn is_allocation(&self) -> bool {
        matches!(self.kind, IntervalKind::Hold { .. } | IntervalKind::Booking { .. })
    }
}

#[derive(Debug, Clone)]
pub struct ResourceState {
    pub id: Ulid,
    pub parent_id: Option<Ulid>,
    pub name: Option<String>,
    /// Max concurrent allocations (default 1).
    pub capacity: u32,
    /// Buffer time in ms after each allocation ends (e.g. cleaning time).
    pub buffer_after: Option<Ms>,
    /// All intervals (rules + allocations), sorted by `span.start`.
    pub intervals: Vec<Interval>,
}

impl ResourceState {
    pub fn new(id: Ulid, parent_id: Option<Ulid>, name: Option<String>, capacity: u32, buffer_after: Option<Ms>) -> Self {
        Self {
            id,
            parent_id,
            name,
            capacity,
            buffer_after,
            intervals: Vec::new(),
        }
    }

    /// Insert interval maintaining sort order by span.start.
    pub fn insert_interval(&mut self, interval: Interval) {
        let pos = self
            .intervals
            .binary_search_by_key(&interval.span.start, |i| i.span.start)
            .unwrap_or_else(|e| e);
        self.intervals.insert(pos, interval);
    }

    /// Remove interval by id.
    pub fn remove_interval(&mut self, id: Ulid) -> Option<Interval> {
        if let Some(pos) = self.intervals.iter().position(|i| i.id == id) {
            Some(self.intervals.remove(pos))
        } else {
            None
        }
    }

    /// Return only intervals whose span overlaps the query window.
    /// Uses binary search to skip intervals starting at or after `query.end`.
    pub fn overlapping(&self, query: &Span) -> impl Iterator<Item = &Interval> {
        // Everything at index >= right_bound starts at or after query.end → can't overlap.
        let right_bound = self
            .intervals
            .partition_point(|i| i.span.start < query.end);
        self.intervals[..right_bound]
            .iter()
            .filter(move |i| i.span.end > query.start)
    }
}

/// The event types — flat, no nesting. This is the WAL record format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    ResourceCreated {
        id: Ulid,
        parent_id: Option<Ulid>,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    },
    ResourceUpdated {
        id: Ulid,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    },
    ResourceDeleted {
        id: Ulid,
    },
    RuleAdded {
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        blocking: bool,
    },
    RuleUpdated {
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        blocking: bool,
    },
    RuleRemoved {
        id: Ulid,
        resource_id: Ulid,
    },
    HoldPlaced {
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        expires_at: Ms,
    },
    HoldReleased {
        id: Ulid,
        resource_id: Ulid,
    },
    BookingConfirmed {
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        label: Option<String>,
    },
    BookingCancelled {
        id: Ulid,
        resource_id: Ulid,
    },
}

// ── Query result types ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceInfo {
    pub id: Ulid,
    pub parent_id: Option<Ulid>,
    pub name: Option<String>,
    pub capacity: u32,
    pub buffer_after: Option<Ms>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleInfo {
    pub id: Ulid,
    pub resource_id: Ulid,
    pub start: Ms,
    pub end: Ms,
    pub blocking: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookingInfo {
    pub id: Ulid,
    pub resource_id: Ulid,
    pub start: Ms,
    pub end: Ms,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldInfo {
    pub id: Ulid,
    pub resource_id: Ulid,
    pub start: Ms,
    pub end: Ms,
    pub expires_at: Ms,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_basics() {
        let s = Span::new(100, 200);
        assert_eq!(s.duration_ms(), 100);
        assert!(s.contains_instant(100));
        assert!(s.contains_instant(199));
        assert!(!s.contains_instant(200)); // half-open
    }

    #[test]
    fn span_overlap() {
        let a = Span::new(100, 200);
        let b = Span::new(150, 250);
        let c = Span::new(200, 300);
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c)); // adjacent, not overlapping
    }

    #[test]
    fn span_contains_span() {
        let outer = Span::new(100, 400);
        let inner = Span::new(150, 300);
        let partial = Span::new(50, 200);
        assert!(outer.contains_span(&inner));
        assert!(outer.contains_span(&outer)); // self-containment
        assert!(!outer.contains_span(&partial));
    }

    #[test]
    fn interval_ordering() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(300, 400),
            kind: IntervalKind::Booking { label: None },
        });
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(100, 200),
            kind: IntervalKind::NonBlocking,
        });
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(200, 300),
            kind: IntervalKind::Hold { expires_at: 9999 },
        });
        assert_eq!(rs.intervals[0].span.start, 100);
        assert_eq!(rs.intervals[1].span.start, 200);
        assert_eq!(rs.intervals[2].span.start, 300);
    }

    #[test]
    fn interval_remove() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        let id = Ulid::new();
        rs.insert_interval(Interval {
            id,
            span: Span::new(100, 200),
            kind: IntervalKind::Booking { label: None },
        });
        assert_eq!(rs.intervals.len(), 1);
        rs.remove_interval(id);
        assert!(rs.intervals.is_empty());
    }

    #[test]
    fn overlapping_skips_past() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        // Past interval
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(100, 200),
            kind: IntervalKind::Booking { label: None },
        });
        // Overlapping interval
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(450, 600),
            kind: IntervalKind::NonBlocking,
        });
        // Future interval (starts after query end)
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(1000, 1100),
            kind: IntervalKind::Booking { label: None },
        });

        let query = Span::new(500, 800);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].span, Span::new(450, 600));
    }

    #[test]
    fn overlapping_adjacent_not_included() {
        // Interval ending exactly at query.start is NOT overlapping (half-open)
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(100, 200),
            kind: IntervalKind::Booking { label: None },
        });
        let query = Span::new(200, 300);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert!(hits.is_empty());
    }

    #[test]
    fn overlapping_all_past() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        for i in 0..5 {
            rs.insert_interval(Interval {
                id: Ulid::new(),
                span: Span::new(i * 100, i * 100 + 50),
                kind: IntervalKind::Booking { label: None },
            });
        }
        // All intervals end before 1000
        let query = Span::new(1000, 2000);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert!(hits.is_empty());
    }

    #[test]
    fn overlapping_all_future() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        for i in 10..15 {
            rs.insert_interval(Interval {
                id: Ulid::new(),
                span: Span::new(i * 100, i * 100 + 50),
                kind: IntervalKind::Booking { label: None },
            });
        }
        // All intervals start at 1000+, query ends at 500
        let query = Span::new(0, 500);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert!(hits.is_empty());
    }

    #[test]
    fn overlapping_large_interval_spanning_query() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        // One huge interval that starts before and ends after the query
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(0, 10000),
            kind: IntervalKind::NonBlocking,
        });
        let query = Span::new(500, 600);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn overlapping_empty_resource() {
        let rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        let query = Span::new(0, 1000);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert!(hits.is_empty());
    }

    #[test]
    fn overlapping_single_ms_overlap() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        // Interval [100, 201) overlaps query [200, 300) by exactly 1ms
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(100, 201),
            kind: IntervalKind::Booking { label: None },
        });
        let query = Span::new(200, 300);
        let hits: Vec<_> = rs.overlapping(&query).collect();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        rs.insert_interval(Interval {
            id: Ulid::new(),
            span: Span::new(100, 200),
            kind: IntervalKind::Booking { label: None },
        });
        let result = rs.remove_interval(Ulid::new());
        assert!(result.is_none());
        assert_eq!(rs.intervals.len(), 1); // original still there
    }

    #[test]
    fn remove_middle_preserves_order() {
        let mut rs = ResourceState::new(Ulid::new(), None, None, 1, None);
        let ids: Vec<Ulid> = (0..3).map(|_| Ulid::new()).collect();
        for (i, &id) in ids.iter().enumerate() {
            rs.insert_interval(Interval {
                id,
                span: Span::new((i as Ms) * 100, (i as Ms) * 100 + 50),
                kind: IntervalKind::Booking { label: None },
            });
        }
        rs.remove_interval(ids[1]); // remove middle
        assert_eq!(rs.intervals.len(), 2);
        assert_eq!(rs.intervals[0].id, ids[0]);
        assert_eq!(rs.intervals[1].id, ids[2]);
    }

    #[test]
    fn interval_kind_helpers() {
        let nb = Interval {
            id: Ulid::new(),
            span: Span::new(0, 100),
            kind: IntervalKind::NonBlocking,
        };
        assert!(nb.is_rule());
        assert!(!nb.is_allocation());

        let b = Interval {
            id: Ulid::new(),
            span: Span::new(0, 100),
            kind: IntervalKind::Blocking,
        };
        assert!(b.is_rule());

        let h = Interval {
            id: Ulid::new(),
            span: Span::new(0, 100),
            kind: IntervalKind::Hold { expires_at: 999 },
        };
        assert!(h.is_allocation());
        assert!(!h.is_rule());

        let bk = Interval {
            id: Ulid::new(),
            span: Span::new(0, 100),
            kind: IntervalKind::Booking { label: None },
        };
        assert!(bk.is_allocation());
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = Event::ResourceCreated {
            id: Ulid::new(),
            parent_id: None,
            name: Some("Test".into()),
            capacity: 1,
            buffer_after: None,
        };
        let bytes = bincode::serialize(&event).unwrap();
        let decoded: Event = bincode::deserialize(&bytes).unwrap();
        assert_eq!(event, decoded);
    }
}
