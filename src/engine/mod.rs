mod availability;
mod conflict;
mod error;
mod mutations;
mod queries;
mod store;
#[cfg(test)]
mod tests;

pub use availability::{availability, compute_saturated_spans, merge_overlapping, subtract_intervals};
pub use error::EngineError;
pub use store::InMemoryStore;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};
use ulid::Ulid;

use crate::model::*;
use crate::notify::NotifyHub;
use crate::wal::Wal;

pub type SharedResourceState = Arc<RwLock<ResourceState>>;

// ── Group-commit WAL channel ─────────────────────────────

pub(super) enum WalCommand {
    Append {
        event: Event,
        response: oneshot::Sender<io::Result<()>>,
    },
    Compact {
        events: Vec<Event>,
        response: oneshot::Sender<io::Result<()>>,
    },
    AppendsSinceCompact {
        response: oneshot::Sender<u64>,
    },
}

/// Background task that owns the WAL and batches appends for group commit.
/// 1. Block until the first Append arrives.
/// 2. Buffer it (no fsync).
/// 3. Drain all immediately available Appends (the batch window).
/// 4. Single flush_sync for the whole batch.
/// 5. Respond Ok to all senders.
async fn wal_writer_loop(mut wal: Wal, mut rx: mpsc::Receiver<WalCommand>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            WalCommand::Append { event, response } => {
                let mut batch = vec![(event, response)];

                // Drain all immediately available appends
                loop {
                    match rx.try_recv() {
                        Ok(WalCommand::Append { event, response }) => {
                            batch.push((event, response));
                        }
                        Ok(other) => {
                            // Flush current batch first, then handle the non-append command
                            metrics::histogram!(crate::observability::WAL_FLUSH_BATCH_SIZE)
                                .record(batch.len() as f64);
                            let flush_start = std::time::Instant::now();
                            let result = flush_batch(&mut wal, &mut batch);
                            metrics::histogram!(crate::observability::WAL_FLUSH_DURATION_SECONDS)
                                .record(flush_start.elapsed().as_secs_f64());
                            respond_batch(&mut batch, &result);
                            handle_non_append(&mut wal, other);
                            break;
                        }
                        Err(_) => break, // channel empty — flush batch
                    }
                }

                if !batch.is_empty() {
                    metrics::histogram!(crate::observability::WAL_FLUSH_BATCH_SIZE)
                        .record(batch.len() as f64);
                    let flush_start = std::time::Instant::now();
                    let result = flush_batch(&mut wal, &mut batch);
                    metrics::histogram!(crate::observability::WAL_FLUSH_DURATION_SECONDS)
                        .record(flush_start.elapsed().as_secs_f64());
                    respond_batch(&mut batch, &result);
                }
            }
            other => handle_non_append(&mut wal, other),
        }
    }
}

fn flush_batch(wal: &mut Wal, batch: &mut [(Event, oneshot::Sender<io::Result<()>>)]) -> io::Result<()> {
    let mut append_err: Option<io::Error> = None;
    for (event, _) in batch.iter() {
        if let Err(e) = wal.append_buffered(event) {
            append_err = Some(e);
            break;
        }
    }
    // Always flush — even on append error — so partially buffered bytes
    // don't leak into the next batch (callers were told this batch failed).
    let flush_err = wal.flush_sync().err();
    if let Some(e) = append_err {
        return Err(e);
    }
    if let Some(e) = flush_err {
        return Err(e);
    }
    Ok(())
}

fn respond_batch(batch: &mut Vec<(Event, oneshot::Sender<io::Result<()>>)>, result: &io::Result<()>) {
    for (_, tx) in batch.drain(..) {
        let r = match result {
            Ok(()) => Ok(()),
            Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
        };
        let _ = tx.send(r);
    }
}

fn handle_non_append(wal: &mut Wal, cmd: WalCommand) {
    match cmd {
        WalCommand::Compact { events, response } => {
            let result = Wal::write_compact_file(wal.path(), &events)
                .and_then(|()| wal.swap_compact_file());
            let _ = response.send(result);
        }
        WalCommand::AppendsSinceCompact { response } => {
            let _ = response.send(wal.appends_since_compact());
        }
        WalCommand::Append { .. } => unreachable!(),
    }
}

pub struct Engine {
    pub(super) store: InMemoryStore,
    pub(super) wal_tx: mpsc::Sender<WalCommand>,
    pub notify: Arc<NotifyHub>,
}

impl Engine {
    pub fn new(wal_path: PathBuf, notify: Arc<NotifyHub>) -> std::io::Result<Self> {
        let events = Wal::replay(&wal_path)?;
        let wal = Wal::open(&wal_path)?;
        let (wal_tx, wal_rx) = mpsc::channel(4096);
        tokio::spawn(wal_writer_loop(wal, wal_rx));

        let store = InMemoryStore::new();
        let engine = Self {
            store,
            wal_tx,
            notify,
        };

        // Replay events — we're the sole owner of these Arcs, so try_read/try_write
        // always succeed instantly (no contention). Never use blocking_read/blocking_write
        // here because this may run inside an async context (e.g. lazy tenant creation).
        for event in &events {
            match event {
                Event::ResourceCreated { id, parent_id, name, capacity, buffer_after } => {
                    let rs = ResourceState::new(*id, *parent_id, name.clone(), *capacity, *buffer_after);
                    engine.store.insert_resource(*id, Arc::new(RwLock::new(rs)));
                    if let Some(pid) = parent_id {
                        engine.store.add_child(*pid, *id);
                    }
                }
                Event::ResourceDeleted { id } => {
                    if let Some(rs) = engine.store.get_resource(id) {
                        let guard = rs.try_read().expect("replay: uncontended read");
                        if let Some(pid) = guard.parent_id {
                            engine.store.remove_child(&pid, id);
                        }
                    }
                    engine.store.remove_resource(id);
                }
                other => {
                    let resource_id = event_resource_id(other);
                    if let Some(resource_id) = resource_id
                        && let Some(rs) = engine.store.get_resource(&resource_id) {
                            let mut guard = rs.try_write().expect("replay: uncontended write");
                            engine.store.apply_event(&mut guard, other);
                        }
                }
            }
        }

        Ok(engine)
    }

    /// Write event to WAL via the background group-commit writer.
    async fn wal_append(&self, event: &Event) -> Result<(), EngineError> {
        let (tx, rx) = oneshot::channel();
        self.wal_tx
            .send(WalCommand::Append {
                event: event.clone(),
                response: tx,
            })
            .await
            .map_err(|_| EngineError::WalError("WAL writer shut down".into()))?;
        rx.await
            .map_err(|_| EngineError::WalError("WAL writer dropped response".into()))?
            .map_err(|e| EngineError::WalError(e.to_string()))
    }

    pub fn get_resource(&self, id: &Ulid) -> Option<SharedResourceState> {
        self.store.get_resource(id)
    }

    pub fn get_resource_for_entity(&self, entity_id: &Ulid) -> Option<Ulid> {
        self.store.get_resource_for_entity(entity_id)
    }

    /// WAL-append + apply + notify in one call. Eliminates the repeated 3-line pattern.
    pub(super) async fn persist_and_apply(
        &self,
        resource_id: Ulid,
        rs: &mut ResourceState,
        event: &Event,
    ) -> Result<(), EngineError> {
        self.wal_append(event).await?;
        self.store.apply_event(rs, event);
        self.notify.send(resource_id, event);
        Ok(())
    }

    /// Lookup entity → resource, get resource, acquire write lock.
    pub(super) async fn resolve_entity_write(
        &self,
        entity_id: &Ulid,
    ) -> Result<(Ulid, tokio::sync::OwnedRwLockWriteGuard<ResourceState>), EngineError> {
        let resource_id = self
            .get_resource_for_entity(entity_id)
            .ok_or(EngineError::NotFound(*entity_id))?;
        let rs = self
            .get_resource(&resource_id)
            .ok_or(EngineError::NotFound(resource_id))?;
        let guard = rs.write_owned().await;
        Ok((resource_id, guard))
    }
}

/// Extract the resource_id from an event (for non-Create/Delete events).
fn event_resource_id(event: &Event) -> Option<Ulid> {
    match event {
        Event::RuleAdded { resource_id, .. }
        | Event::RuleUpdated { resource_id, .. }
        | Event::RuleRemoved { resource_id, .. }
        | Event::HoldPlaced { resource_id, .. }
        | Event::HoldReleased { resource_id, .. }
        | Event::BookingConfirmed { resource_id, .. }
        | Event::BookingCancelled { resource_id, .. } => Some(*resource_id),
        Event::ResourceUpdated { id, .. } => Some(*id),
        Event::ResourceCreated { .. } | Event::ResourceDeleted { .. } => None,
    }
}
