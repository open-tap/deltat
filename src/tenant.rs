use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;

use crate::engine::Engine;
use crate::limits::*;
use crate::notify::NotifyHub;
use crate::reaper;

/// Manages per-tenant engines. Each tenant gets its own Engine + WAL + reaper.
/// Tenant = database name from the pgwire connection.
pub struct TenantManager {
    engines: DashMap<String, Arc<Engine>>,
    data_dir: PathBuf,
    compact_threshold: u64,
}

impl TenantManager {
    pub fn new(data_dir: PathBuf, compact_threshold: u64) -> Self {
        Self {
            engines: DashMap::new(),
            data_dir,
            compact_threshold,
        }
    }

    /// Get or lazily create an engine for the given tenant.
    pub fn get_or_create(&self, tenant: &str) -> std::io::Result<Arc<Engine>> {
        if let Some(engine) = self.engines.get(tenant) {
            return Ok(engine.value().clone());
        }
        if tenant.len() > MAX_TENANT_NAME_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "tenant name too long",
            ));
        }
        if self.engines.len() >= MAX_TENANTS {
            return Err(std::io::Error::other("too many tenants"));
        }

        // Sanitize tenant name to prevent path traversal
        let safe_name: String = tenant
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
            .collect();
        if safe_name.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty tenant name",
            ));
        }

        let wal_path = self.data_dir.join(format!("{safe_name}.wal"));
        let notify = Arc::new(NotifyHub::new());
        let engine = Arc::new(Engine::new(wal_path, notify)?);

        // Spawn reaper + compactor for this tenant
        let reaper_engine = engine.clone();
        tokio::spawn(async move {
            reaper::run_reaper(reaper_engine).await;
        });
        let compactor_engine = engine.clone();
        let threshold = self.compact_threshold;
        tokio::spawn(async move {
            reaper::run_compactor(compactor_engine, threshold).await;
        });

        self.engines.insert(tenant.to_string(), engine.clone());
        metrics::gauge!(crate::observability::TENANTS_ACTIVE).set(self.engines.len() as f64);
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use ulid::Ulid;
    use crate::model::*;

    fn test_data_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_tenant").join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let dir = test_data_dir("isolation");
        let tm = TenantManager::new(dir, 1000);

        let eng_a = tm.get_or_create("tenant_a").unwrap();
        let eng_b = tm.get_or_create("tenant_b").unwrap();

        let rid = Ulid::new();

        // Create same resource ID in both tenants
        eng_a.create_resource(rid, None, None, 1, None).await.unwrap();
        eng_b.create_resource(rid, None, None, 1, None).await.unwrap();

        // Add rule in tenant A
        eng_a
            .add_rule(Ulid::new(), rid, Span::new(0, 10000), false)
            .await
            .unwrap();

        // Tenant B's resource should have no rules
        let avail_b = eng_b.compute_availability(rid, 0, 10000, None).await.unwrap();
        assert!(avail_b.is_empty()); // no rules → no availability

        // Tenant A should have availability
        let avail_a = eng_a.compute_availability(rid, 0, 10000, None).await.unwrap();
        assert_eq!(avail_a, vec![Span::new(0, 10000)]);
    }

    #[tokio::test]
    async fn tenant_lazy_creation() {
        let dir = test_data_dir("lazy");
        let tm = TenantManager::new(dir.clone(), 1000);

        // No WAL files should exist yet
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert!(entries.is_empty());

        // Create a tenant
        let _eng = tm.get_or_create("my_db").unwrap();

        // WAL file should now exist
        assert!(dir.join("my_db.wal").exists());
    }

    #[tokio::test]
    async fn tenant_same_engine_returned() {
        let dir = test_data_dir("same_eng");
        let tm = TenantManager::new(dir, 1000);

        let eng1 = tm.get_or_create("foo").unwrap();
        let eng2 = tm.get_or_create("foo").unwrap();

        // Should be the same Arc
        assert!(Arc::ptr_eq(&eng1, &eng2));
    }

    #[tokio::test]
    async fn tenant_name_sanitized() {
        let dir = test_data_dir("sanitize");
        let tm = TenantManager::new(dir.clone(), 1000);

        // Path traversal attempt
        let _eng = tm.get_or_create("../evil").unwrap();
        // Should create "evil.wal", not "../evil.wal"
        assert!(dir.join("evil.wal").exists());

        // Empty after sanitization
        let result = tm.get_or_create("../..");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tenant_name_too_long() {
        let dir = test_data_dir("name_too_long");
        let tm = TenantManager::new(dir, 1000);

        let long_name = "x".repeat(MAX_TENANT_NAME_LEN + 1);
        let result = tm.get_or_create(&long_name);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("tenant name too long"));
    }

    #[tokio::test]
    async fn tenant_name_at_limit() {
        let dir = test_data_dir("name_at_limit");
        let tm = TenantManager::new(dir, 1000);

        // Use a name just under the OS filename limit (256 - ".wal" = 252 is safe)
        // but at our MAX_TENANT_NAME_LEN (256). Since the WAL appends ".wal" (4 chars),
        // the actual test verifies the *length check passes*, then the name gets
        // used as a WAL filename. Use a shorter name that still hits the boundary.
        let name = "x".repeat(MAX_TENANT_NAME_LEN);
        // The length check itself should pass
        assert!(name.len() <= MAX_TENANT_NAME_LEN);
        // But creating the WAL file may fail on OS filename limits, so just verify
        // the length check doesn't reject it — the actual io::Error would be from
        // the OS, not our limit.
        let result = tm.get_or_create(&name);
        // Either succeeds or fails with an OS error (not our "tenant name too long" error)
        if let Err(ref e) = result {
            assert!(!e.to_string().contains("tenant name too long"));
        }
    }

    #[tokio::test]
    async fn tenant_count_limit() {
        let dir = test_data_dir("count_limit");
        let tm = TenantManager::new(dir, 1000);

        for i in 0..MAX_TENANTS {
            tm.get_or_create(&format!("t{i}")).unwrap();
        }
        let result = tm.get_or_create("one_more");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("too many tenants"));
    }
}
