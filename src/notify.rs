use dashmap::DashMap;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::model::Event;

#[allow(dead_code)]
const CHANNEL_CAPACITY: usize = 256;

/// Broadcast hub for LISTEN/NOTIFY per resource.
pub struct NotifyHub {
    channels: DashMap<Ulid, broadcast::Sender<Event>>,
}

impl NotifyHub {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
        }
    }

    /// Subscribe to notifications for a resource. Creates the channel if needed.
    #[allow(dead_code)]
    pub fn subscribe(&self, resource_id: Ulid) -> broadcast::Receiver<Event> {
        let sender = self
            .channels
            .entry(resource_id)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }

    /// Send a notification. No-op if nobody is listening.
    pub fn send(&self, resource_id: Ulid, event: &Event) {
        if let Some(sender) = self.channels.get(&resource_id) {
            let _ = sender.send(event.clone());
        }
    }

    /// Remove a channel (e.g. when resource is deleted).
    #[allow(dead_code)]
    pub fn remove(&self, resource_id: &Ulid) {
        self.channels.remove(resource_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_and_receive() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        let mut rx = hub.subscribe(rid);

        let event = Event::ResourceCreated {
            id: rid,
            parent_id: None,
            capacity: 1,
            buffer_after: None,
        };
        hub.send(rid, &event);

        let received = rx.recv().await.unwrap();
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn send_without_subscribers_is_noop() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        // No subscriber — should not panic
        hub.send(
            rid,
            &Event::ResourceDeleted { id: rid },
        );
    }
}
