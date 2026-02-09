use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::engine::Engine;

/// Background task that periodically cleans up expired holds.
pub async fn run_reaper(engine: Arc<Engine>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let expired = engine.collect_expired_holds(now);
        for (hold_id, _resource_id) in expired {
            match engine.release_hold(hold_id).await {
                Ok(_) => info!("reaped expired hold {hold_id}"),
                Err(e) => {
                    // May already have been released — that's fine
                    tracing::debug!("reaper skip {hold_id}: {e}");
                }
            }
        }
    }
}

/// Background task that periodically collects past bookings and expired holds.
pub async fn run_gc(engine: Arc<Engine>, retention_ms: i64) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let collected = engine.gc_past_intervals(now, retention_ms);
        if collected > 0 {
            info!("GC collected {collected} intervals");
            match engine.compact_wal().await {
                Ok(()) => info!("WAL compacted after GC"),
                Err(e) => tracing::warn!("WAL compaction after GC failed: {e}"),
            }
        }
    }
}

/// Background task that compacts the WAL after a threshold of mutations.
/// Checks every 10 seconds, only compacts when appends since last compaction exceed `threshold`.
pub async fn run_compactor(engine: Arc<Engine>, threshold: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        if engine.wal_appends_since_compact().await >= threshold {
            match engine.compact_wal().await {
                Ok(()) => info!("WAL compacted"),
                Err(e) => tracing::warn!("WAL compaction failed: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::notify::NotifyHub;
    use std::path::PathBuf;
    use ulid::Ulid;

    fn test_wal_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_reaper");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = std::fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn reaper_collects_expired_holds() {
        let path = test_wal_path("reaper_collect.wal");
        let notify = Arc::new(NotifyHub::new());
        let engine = Arc::new(Engine::new(path, notify).unwrap());

        let rid = Ulid::new();
        engine
            .create_resource(rid, None, None, 1, None)
            .await
            .unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let hold_id = Ulid::new();

        // Place a hold that expires immediately
        engine
            .place_hold(hold_id, rid, Span::new(1000, 2000), now - 1000)
            .await
            .unwrap();

        let expired = engine.collect_expired_holds(now);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, hold_id);

        // Release it
        engine.release_hold(hold_id).await.unwrap();

        let expired_after = engine.collect_expired_holds(now);
        assert!(expired_after.is_empty());
    }
}
