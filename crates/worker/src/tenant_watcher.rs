//! Polls `list_tenants()` every 30s and spawns a Temporal worker for
//! each new tenant. Already-spawned tenants are tracked in a `HashSet`
//! under a `Mutex` so the diff is atomic against concurrent polls.

use catalog::Catalog;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub type SpawnFn = Box<
    dyn Fn(common_types::ids::TenantId) -> tokio::task::JoinHandle<()> + Send + Sync,
>;

pub async fn run(
    catalog: Arc<Catalog>,
    initial: Vec<common_types::ids::TenantId>,
    spawn: SpawnFn,
) {
    let known = Arc::new(Mutex::new(initial.into_iter().collect::<HashSet<_>>()));
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.tick().await; // skip the immediate first tick
    loop {
        tick.tick().await;
        let tenants = match catalog.list_tenants().await {
            Ok(ts) => ts,
            Err(e) => {
                tracing::warn!(error = %e, "tenant_watcher list_tenants");
                continue;
            }
        };
        let mut g = known.lock().await;
        for t in tenants {
            if g.insert(t.tenant_id) {
                tracing::info!(tenant = %t.name, "tenant_watcher: spawning new worker");
                let _ = spawn(t.tenant_id);
            }
        }
    }
}
