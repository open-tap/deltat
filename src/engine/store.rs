use dashmap::DashMap;
use ulid::Ulid;

use crate::model::*;

use super::SharedResourceState;

pub struct InMemoryStore {
    resources: DashMap<Ulid, SharedResourceState>,
    entity_to_resource: DashMap<Ulid, Ulid>,
    children: DashMap<Ulid, Vec<Ulid>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            resources: DashMap::new(),
            entity_to_resource: DashMap::new(),
            children: DashMap::new(),
        }
    }

    // ── Resource CRUD ────────────────────────────────────────

    pub fn resource_count(&self) -> usize {
        self.resources.len()
    }

    pub fn contains_resource(&self, id: &Ulid) -> bool {
        self.resources.contains_key(id)
    }

    pub fn get_resource(&self, id: &Ulid) -> Option<SharedResourceState> {
        self.resources.get(id).map(|e| e.value().clone())
    }

    pub fn insert_resource(&self, id: Ulid, state: SharedResourceState) {
        self.resources.insert(id, state);
    }

    pub fn remove_resource(&self, id: &Ulid) -> Option<(Ulid, SharedResourceState)> {
        self.resources.remove(id)
    }

    pub fn resource_ids(&self) -> Vec<Ulid> {
        self.resources.iter().map(|e| *e.key()).collect()
    }

    // ── Entity index ─────────────────────────────────────────

    pub fn get_resource_for_entity(&self, entity_id: &Ulid) -> Option<Ulid> {
        self.entity_to_resource.get(entity_id).map(|e| *e.value())
    }

    pub fn map_entity(&self, entity_id: Ulid, resource_id: Ulid) {
        self.entity_to_resource.insert(entity_id, resource_id);
    }

    pub fn unmap_entity(&self, entity_id: &Ulid) {
        self.entity_to_resource.remove(entity_id);
    }

    // ── Children index ───────────────────────────────────────

    pub fn add_child(&self, parent_id: Ulid, child_id: Ulid) {
        self.children.entry(parent_id).or_default().push(child_id);
    }

    pub fn remove_child(&self, parent_id: &Ulid, child_id: &Ulid) {
        if let Some(mut kids) = self.children.get_mut(parent_id) {
            kids.retain(|c| c != child_id);
        }
    }

    pub fn get_children(&self, parent_id: &Ulid) -> Vec<Ulid> {
        self.children
            .get(parent_id)
            .map(|e| e.value().clone())
            .unwrap_or_default()
    }

    pub fn has_children(&self, parent_id: &Ulid) -> bool {
        self.children
            .get(parent_id)
            .is_some_and(|kids| !kids.is_empty())
    }

    // ── Event application ────────────────────────────────────

    pub fn apply_event(&self, rs: &mut ResourceState, event: &Event) {
        match event {
            Event::RuleAdded {
                id,
                resource_id,
                span,
                blocking,
            } => {
                let kind = if *blocking {
                    IntervalKind::Blocking
                } else {
                    IntervalKind::NonBlocking
                };
                rs.insert_interval(Interval {
                    id: *id,
                    span: *span,
                    kind,
                });
                self.map_entity(*id, *resource_id);
            }
            Event::RuleUpdated {
                id,
                resource_id,
                span,
                blocking,
            } => {
                rs.remove_interval(*id);
                let kind = if *blocking {
                    IntervalKind::Blocking
                } else {
                    IntervalKind::NonBlocking
                };
                rs.insert_interval(Interval {
                    id: *id,
                    span: *span,
                    kind,
                });
                self.map_entity(*id, *resource_id);
            }
            Event::RuleRemoved { id, .. } => {
                rs.remove_interval(*id);
                self.unmap_entity(id);
            }
            Event::HoldPlaced {
                id,
                resource_id,
                span,
                expires_at,
            } => {
                rs.insert_interval(Interval {
                    id: *id,
                    span: *span,
                    kind: IntervalKind::Hold {
                        expires_at: *expires_at,
                    },
                });
                self.map_entity(*id, *resource_id);
            }
            Event::HoldReleased { id, .. } => {
                rs.remove_interval(*id);
                self.unmap_entity(id);
            }
            Event::BookingConfirmed {
                id,
                resource_id,
                span,
                label,
            } => {
                rs.insert_interval(Interval {
                    id: *id,
                    span: *span,
                    kind: IntervalKind::Booking { label: label.clone() },
                });
                self.map_entity(*id, *resource_id);
            }
            Event::BookingCancelled { id, .. } => {
                rs.remove_interval(*id);
                self.unmap_entity(id);
            }
            Event::ResourceUpdated {
                name,
                capacity,
                buffer_after,
                ..
            } => {
                rs.name = name.clone();
                rs.capacity = *capacity;
                rs.buffer_after = *buffer_after;
            }
            Event::ResourceCreated { .. } | Event::ResourceDeleted { .. } => {}
        }
    }
}
