use std::collections::HashSet;

use ulid::Ulid;

use crate::limits::*;
use crate::model::*;

use super::availability::availability;
use super::conflict::now_ms;
use super::{Engine, EngineError};

impl Engine {
    /// Walk up from a resource collecting inherited rules from ancestors.
    ///
    /// Non-blocking: OVERRIDE — first ancestor with non-blocking rules wins.
    /// Blocking: ACCUMULATE — all ancestors' blocking rules are collected.
    ///
    /// Returns `(inherited_non_blocking, inherited_blocking)` clamped to query.
    pub(super) async fn collect_inherited_rules(
        &self,
        resource: &ResourceState,
        query: &Span,
    ) -> Result<(Vec<Span>, Vec<Span>), EngineError> {
        let mut inherited_non_blocking: Vec<Span> = Vec::new();
        let mut inherited_blocking: Vec<Span> = Vec::new();
        let mut found_non_blocking = false;

        let mut current_parent_id = resource.parent_id;
        let mut visited = HashSet::new();
        visited.insert(resource.id);
        let mut depth = 0usize;

        while let Some(pid) = current_parent_id {
            depth += 1;
            if depth > MAX_HIERARCHY_DEPTH {
                return Err(EngineError::LimitExceeded("hierarchy too deep"));
            }
            if !visited.insert(pid) {
                return Err(EngineError::CycleDetected(pid));
            }
            let parent_rs = self
                .get_resource(&pid)
                .ok_or(EngineError::NotFound(pid))?;
            let parent_guard = parent_rs.read().await;

            for interval in parent_guard.overlapping(query) {
                match &interval.kind {
                    IntervalKind::Blocking => {
                        inherited_blocking.push(Span::new(
                            interval.span.start.max(query.start),
                            interval.span.end.min(query.end),
                        ));
                    }
                    IntervalKind::NonBlocking if !found_non_blocking => {
                        inherited_non_blocking.push(Span::new(
                            interval.span.start.max(query.start),
                            interval.span.end.min(query.end),
                        ));
                    }
                    _ => {}
                }
            }

            if !found_non_blocking && !inherited_non_blocking.is_empty() {
                found_non_blocking = true;
            }

            current_parent_id = parent_guard.parent_id;
        }

        inherited_non_blocking.sort_by_key(|s| s.start);
        inherited_blocking.sort_by_key(|s| s.start);

        Ok((inherited_non_blocking, inherited_blocking))
    }

    pub async fn compute_availability(
        &self,
        resource_id: Ulid,
        query_start: Ms,
        query_end: Ms,
        min_duration_ms: Option<Ms>,
    ) -> Result<Vec<Span>, EngineError> {
        if query_end - query_start > MAX_QUERY_WINDOW_MS {
            return Err(EngineError::LimitExceeded("query window too wide"));
        }
        let rs = match self.get_resource(&resource_id) {
            Some(rs) => rs,
            None => return Ok(vec![]),
        };
        let guard = rs.read().await;

        let query = Span::new(query_start, query_end);
        let (inherited_non_blocking, inherited_blocking) =
            self.collect_inherited_rules(&guard, &query).await?;

        let now = now_ms();
        let mut free = availability(
            &guard,
            &query,
            &inherited_non_blocking,
            &inherited_blocking,
            now,
        );

        if let Some(min_dur) = min_duration_ms {
            free.retain(|span| span.duration_ms() >= min_dur);
        }

        Ok(free)
    }

    /// Compute combined availability across multiple independent resources.
    pub async fn compute_multi_availability(
        &self,
        resource_ids: &[Ulid],
        query_start: Ms,
        query_end: Ms,
        min_available: usize,
        min_duration_ms: Option<Ms>,
    ) -> Result<Vec<Span>, EngineError> {
        if query_end - query_start > MAX_QUERY_WINDOW_MS {
            return Err(EngineError::LimitExceeded("query window too wide"));
        }
        if resource_ids.len() > MAX_IN_CLAUSE_IDS {
            return Err(EngineError::LimitExceeded("too many resource IDs"));
        }
        if resource_ids.is_empty() || min_available == 0 {
            return Ok(Vec::new());
        }

        let mut all_events: Vec<(Ms, i32)> = Vec::new();
        for &rid in resource_ids {
            let spans = self
                .compute_availability(rid, query_start, query_end, None)
                .await?;
            for s in spans {
                all_events.push((s.start, 1));
                all_events.push((s.end, -1));
            }
        }

        all_events.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut result = Vec::new();
        let mut count: i32 = 0;
        let mut seg_start: Option<Ms> = None;
        let threshold = min_available as i32;

        for (time, delta) in &all_events {
            let prev = count;
            count += delta;

            if prev < threshold && count >= threshold {
                seg_start = Some(*time);
            } else if prev >= threshold && count < threshold
                && let Some(start) = seg_start.take()
                && *time > start {
                    let span = Span::new(start, *time);
                    if min_duration_ms.is_none_or(|d| span.duration_ms() >= d) {
                        result.push(span);
                    }
                }
        }

        Ok(result)
    }

    pub fn list_resources(&self) -> Vec<ResourceInfo> {
        self.state
            .iter()
            .map(|entry| {
                let rs = entry.value().clone();
                let guard = rs.try_read().expect("list_resources: uncontended read");
                ResourceInfo {
                    id: guard.id,
                    parent_id: guard.parent_id,
                    name: guard.name.clone(),
                    capacity: guard.capacity,
                    buffer_after: guard.buffer_after,
                }
            })
            .collect()
    }

    pub async fn get_rules(&self, resource_id: Ulid) -> Result<Vec<RuleInfo>, EngineError> {
        let rs = match self.get_resource(&resource_id) {
            Some(rs) => rs,
            None => return Ok(vec![]),
        };
        let guard = rs.read().await;
        Ok(guard
            .intervals
            .iter()
            .filter_map(|i| match &i.kind {
                IntervalKind::NonBlocking => Some(RuleInfo {
                    id: i.id,
                    resource_id,
                    start: i.span.start,
                    end: i.span.end,
                    blocking: false,
                }),
                IntervalKind::Blocking => Some(RuleInfo {
                    id: i.id,
                    resource_id,
                    start: i.span.start,
                    end: i.span.end,
                    blocking: true,
                }),
                _ => None,
            })
            .collect())
    }

    pub async fn get_bookings(&self, resource_id: Ulid) -> Result<Vec<BookingInfo>, EngineError> {
        let rs = match self.get_resource(&resource_id) {
            Some(rs) => rs,
            None => return Ok(vec![]),
        };
        let guard = rs.read().await;
        Ok(guard
            .intervals
            .iter()
            .filter_map(|i| match &i.kind {
                IntervalKind::Booking { label } => Some(BookingInfo {
                    id: i.id,
                    resource_id,
                    start: i.span.start,
                    end: i.span.end,
                    label: label.clone(),
                }),
                _ => None,
            })
            .collect())
    }

    pub async fn get_holds(&self, resource_id: Ulid) -> Result<Vec<HoldInfo>, EngineError> {
        let rs = match self.get_resource(&resource_id) {
            Some(rs) => rs,
            None => return Ok(vec![]),
        };
        let guard = rs.read().await;
        Ok(guard
            .intervals
            .iter()
            .filter_map(|i| match &i.kind {
                IntervalKind::Hold { expires_at } => Some(HoldInfo {
                    id: i.id,
                    resource_id,
                    start: i.span.start,
                    end: i.span.end,
                    expires_at: *expires_at,
                }),
                _ => None,
            })
            .collect())
    }
}
