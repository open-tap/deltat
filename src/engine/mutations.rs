use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{oneshot, RwLock};
use ulid::Ulid;

use crate::limits::*;
use crate::model::*;

use super::availability::subtract_intervals;
use super::conflict::{check_no_conflict, now_ms, validate_span};
use super::{apply_to_resource, Engine, EngineError, SharedResourceState, WalCommand};

impl Engine {
    pub async fn create_resource(
        &self,
        id: Ulid,
        parent_id: Option<Ulid>,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    ) -> Result<(), EngineError> {
        if self.state.len() >= MAX_RESOURCES_PER_TENANT {
            return Err(EngineError::LimitExceeded("too many resources"));
        }
        if let Some(ref n) = name
            && n.len() > MAX_NAME_LEN {
                return Err(EngineError::LimitExceeded("resource name too long"));
            }
        if let Some(pid) = parent_id {
            let mut depth = 0usize;
            let mut cur = Some(pid);
            while let Some(cid) = cur {
                depth += 1;
                if depth > MAX_HIERARCHY_DEPTH {
                    return Err(EngineError::LimitExceeded("hierarchy too deep"));
                }
                cur = self.get_resource(&cid).and_then(|rs| {
                    rs.try_read().ok().and_then(|g| g.parent_id)
                });
            }
        }
        if self.state.contains_key(&id) {
            return Err(EngineError::AlreadyExists(id));
        }
        if let Some(pid) = parent_id {
            if pid == id {
                return Err(EngineError::CycleDetected(id));
            }
            if !self.state.contains_key(&pid) {
                return Err(EngineError::NotFound(pid));
            }
        }

        let event = Event::ResourceCreated { id, parent_id, name: name.clone(), capacity, buffer_after };
        self.wal_append(&event).await?;
        let rs = ResourceState::new(id, parent_id, name, capacity, buffer_after);
        self.state.insert(id, Arc::new(RwLock::new(rs)));
        if let Some(pid) = parent_id {
            self.children.entry(pid).or_default().push(id);
        }
        self.notify.send(id, &event);
        Ok(())
    }

    pub async fn delete_resource(&self, id: Ulid) -> Result<(), EngineError> {
        if !self.state.contains_key(&id) {
            return Err(EngineError::NotFound(id));
        }
        if let Some(kids) = self.children.get(&id)
            && !kids.is_empty() {
                return Err(EngineError::HasChildren(id));
            }

        let rs = self.get_resource(&id).unwrap();
        let guard = rs.read().await;
        if let Some(pid) = guard.parent_id
            && let Some(mut kids) = self.children.get_mut(&pid) {
                kids.retain(|c| c != &id);
            }
        drop(guard);

        let event = Event::ResourceDeleted { id };
        self.wal_append(&event).await?;
        self.state.remove(&id);
        self.notify.send(id, &event);
        Ok(())
    }

    pub async fn add_rule(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        blocking: bool,
    ) -> Result<(), EngineError> {
        validate_span(&span)?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let mut guard = rs.write().await;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource"));
        }

        if !blocking
            && let Some(parent_id) = guard.parent_id {
                let parent_free = self
                    .compute_availability(parent_id, span.start, span.end, None)
                    .await?;
                let rule_as_slice = [span];
                let uncovered = subtract_intervals(&rule_as_slice, &parent_free);
                if !uncovered.is_empty() {
                    return Err(EngineError::NotCoveredByParent {
                        rule_span: span,
                        uncovered,
                    });
                }
            }

        let event = Event::RuleAdded { id, resource_id, span, blocking };
        self.persist_and_apply(resource_id, &mut guard, &event).await
    }

    pub async fn remove_rule(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let event = Event::RuleRemoved { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub async fn place_hold(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        expires_at: Ms,
    ) -> Result<(), EngineError> {
        validate_span(&span)?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let mut guard = rs.write().await;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource"));
        }

        check_no_conflict(&guard, &span, now_ms())?;

        let event = Event::HoldPlaced { id, resource_id, span, expires_at };
        self.persist_and_apply(resource_id, &mut guard, &event).await
    }

    pub async fn release_hold(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let event = Event::HoldReleased { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub async fn confirm_booking(
        &self,
        id: Ulid,
        resource_id: Ulid,
        span: Span,
        label: Option<String>,
    ) -> Result<(), EngineError> {
        validate_span(&span)?;
        if let Some(ref l) = label
            && l.len() > MAX_LABEL_LEN {
                return Err(EngineError::LimitExceeded("label too long"));
            }
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let mut guard = rs.write().await;
        if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
            return Err(EngineError::LimitExceeded("too many intervals on resource"));
        }

        check_no_conflict(&guard, &span, now_ms())?;

        let event = Event::BookingConfirmed { id, resource_id, span, label };
        self.persist_and_apply(resource_id, &mut guard, &event).await
    }

    /// Atomically book multiple slots. All-or-nothing: if any booking conflicts,
    /// none are committed. Bookings may span different resources.
    pub async fn batch_confirm_bookings(
        &self,
        bookings: Vec<(Ulid, Ulid, Span, Option<String>)>,
    ) -> Result<(), EngineError> {
        if bookings.is_empty() {
            return Ok(());
        }
        if bookings.len() > MAX_BATCH_SIZE {
            return Err(EngineError::LimitExceeded("batch too large"));
        }
        for (_, _, span, label) in &bookings {
            validate_span(span)?;
            if let Some(l) = label
                && l.len() > MAX_LABEL_LEN {
                    return Err(EngineError::LimitExceeded("label too long"));
                }
        }

        // Acquire write locks in sorted order to prevent deadlocks.
        let mut resource_ids: Vec<Ulid> = bookings.iter().map(|(_, rid, _, _)| *rid).collect();
        resource_ids.sort();
        resource_ids.dedup();

        let mut guards = Vec::with_capacity(resource_ids.len());
        let mut rs_map = HashMap::new();

        for rid in &resource_ids {
            let rs = self
                .get_resource(rid)
                .ok_or(EngineError::NotFound(*rid))?;
            let guard = rs.write_owned().await;
            if guard.intervals.len() >= MAX_INTERVALS_PER_RESOURCE {
                return Err(EngineError::LimitExceeded("too many intervals on resource"));
            }
            rs_map.insert(*rid, guards.len());
            guards.push(guard);
        }

        // Phase 1: Validate all bookings against current state + intra-batch.
        let now = now_ms();

        let mut by_resource: HashMap<Ulid, Vec<(Ulid, Span)>> = HashMap::new();
        for (id, rid, span, _) in &bookings {
            by_resource.entry(*rid).or_default().push((*id, *span));
        }

        for (rid, batch) in &by_resource {
            let guard = &guards[rs_map[rid]];

            for (_, span) in batch {
                check_no_conflict(guard, span, now)?;
            }

            if batch.len() > 1 {
                let buffer = guard.buffer_after.unwrap_or(0);
                for i in 0..batch.len() {
                    for j in (i + 1)..batch.len() {
                        let effective_i = Span::new(batch[i].1.start, batch[i].1.end + buffer);
                        if effective_i.overlaps(&batch[j].1) {
                            return Err(EngineError::Conflict(batch[i].0));
                        }
                        let effective_j = Span::new(batch[j].1.start, batch[j].1.end + buffer);
                        if effective_j.overlaps(&batch[i].1) {
                            return Err(EngineError::Conflict(batch[j].0));
                        }
                    }
                }
            }
        }

        // Phase 2: All validated — commit all bookings.
        for (id, resource_id, span, label) in bookings {
            let event = Event::BookingConfirmed { id, resource_id, span, label };
            self.wal_append(&event).await?;
            let guard_idx = rs_map[&resource_id];
            apply_to_resource(&mut guards[guard_idx], &event, &self.entity_to_resource);
            self.notify.send(resource_id, &event);
        }

        Ok(())
    }

    pub async fn cancel_booking(&self, id: Ulid) -> Result<Ulid, EngineError> {
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let event = Event::BookingCancelled { id, resource_id };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub async fn update_resource(
        &self,
        id: Ulid,
        name: Option<String>,
        capacity: u32,
        buffer_after: Option<Ms>,
    ) -> Result<(), EngineError> {
        if let Some(ref n) = name
            && n.len() > MAX_NAME_LEN {
                return Err(EngineError::LimitExceeded("resource name too long"));
            }
        let rs = self
            .get_resource(&id)
            .ok_or(EngineError::NotFound(id))?;
        let mut guard = rs.write().await;

        let event = Event::ResourceUpdated { id, name, capacity, buffer_after };
        self.persist_and_apply(id, &mut guard, &event).await
    }

    pub async fn update_rule(
        &self,
        id: Ulid,
        span: Span,
        blocking: bool,
    ) -> Result<Ulid, EngineError> {
        validate_span(&span)?;
        let (resource_id, mut guard) = self.resolve_entity_write(&id).await?;
        let event = Event::RuleUpdated { id, resource_id, span, blocking };
        self.persist_and_apply(resource_id, &mut guard, &event).await?;
        Ok(resource_id)
    }

    pub fn collect_expired_holds(&self, now: Ms) -> Vec<(Ulid, Ulid)> {
        let mut expired = Vec::new();
        for entry in self.state.iter() {
            let rs = entry.value().clone();
            if let Ok(guard) = rs.try_read() {
                for interval in &guard.intervals {
                    if let IntervalKind::Hold { expires_at } = interval.kind
                        && expires_at <= now {
                            expired.push((interval.id, guard.id));
                        }
                }
            }
        }
        expired
    }

    /// Compact the WAL by rewriting it with only the events needed to recreate the current state.
    pub async fn compact_wal(&self) -> Result<(), EngineError> {
        let mut events = Vec::new();
        let mut visited = HashSet::new();

        fn emit_resource(
            id: Ulid,
            state: &dashmap::DashMap<Ulid, SharedResourceState>,
            events: &mut Vec<Event>,
            visited: &mut HashSet<Ulid>,
        ) {
            if !visited.insert(id) {
                return;
            }
            let entry = match state.get(&id) {
                Some(e) => e,
                None => return,
            };
            let rs = entry.value().clone();
            let guard = rs.try_read().expect("compact: uncontended read");

            if let Some(pid) = guard.parent_id {
                emit_resource(pid, state, events, visited);
            }

            events.push(Event::ResourceCreated {
                id: guard.id,
                parent_id: guard.parent_id,
                name: guard.name.clone(),
                capacity: guard.capacity,
                buffer_after: guard.buffer_after,
            });

            for interval in &guard.intervals {
                match &interval.kind {
                    IntervalKind::NonBlocking => events.push(Event::RuleAdded {
                        id: interval.id,
                        resource_id: guard.id,
                        span: interval.span,
                        blocking: false,
                    }),
                    IntervalKind::Blocking => events.push(Event::RuleAdded {
                        id: interval.id,
                        resource_id: guard.id,
                        span: interval.span,
                        blocking: true,
                    }),
                    IntervalKind::Hold { expires_at } => events.push(Event::HoldPlaced {
                        id: interval.id,
                        resource_id: guard.id,
                        span: interval.span,
                        expires_at: *expires_at,
                    }),
                    IntervalKind::Booking { label } => events.push(Event::BookingConfirmed {
                        id: interval.id,
                        resource_id: guard.id,
                        span: interval.span,
                        label: label.clone(),
                    }),
                }
            }
        }

        let resource_ids: Vec<Ulid> = self.state.iter().map(|e| *e.key()).collect();
        for id in resource_ids {
            emit_resource(id, &self.state, &mut events, &mut visited);
        }

        let (tx, rx) = oneshot::channel();
        self.wal_tx
            .send(WalCommand::Compact { events, response: tx })
            .await
            .map_err(|_| EngineError::WalError("WAL writer shut down".into()))?;
        rx.await
            .map_err(|_| EngineError::WalError("WAL writer dropped response".into()))?
            .map_err(|e| EngineError::WalError(e.to_string()))
    }

    pub async fn wal_appends_since_compact(&self) -> u64 {
        let (tx, rx) = oneshot::channel();
        if self
            .wal_tx
            .send(WalCommand::AppendsSinceCompact { response: tx })
            .await
            .is_err()
        {
            return 0;
        }
        rx.await.unwrap_or(0)
    }
}
