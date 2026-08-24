//! mini-dsh: a tiny harness assembled entirely from plugins — the DSH way.
//!
//! Nothing in this binary knows about databases or HTTP; it only composes.
//! Swap any plugin, hot-update config, or isolate a tenant scope and the
//! dependency graph re-converges on its own.
//!
//! Run: `cargo run -p cordis --example mini_dsh`

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use cordis::{
    plugin, plugin_with, Context, FiberHandle, Injection, Plugin,
};

// ---------------------------------------------------------------------------
// Plugin 1: settings — provides the "config" service from raw JSON.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Settings(Value);

fn settings_plugin(raw: Value) -> Arc<dyn Plugin> {
    plugin("settings", move |ctx: Context, _config: Value| {
        let raw = raw.clone();
        async move {
            ctx.provide("config", Settings(raw)).await?;
            Ok(())
        }
    })
}

// ---------------------------------------------------------------------------
// Plugin 2: database — requires "config", provides "database".
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct DbSection {
    url: String,
    pool: u32,
}

#[derive(Debug)]
struct Database {
    url: String,
    pool: u32,
}

impl Drop for Database {
    fn drop(&mut self) {
        eprintln!("      [database] connection pool closed ({})", self.url);
    }
}

fn database_plugin() -> Arc<dyn Plugin> {
    plugin_with(
        "database",
        vec![Injection::from("config")],
        |ctx: Context, _config: ()| async move {
        let settings = ctx.require::<Settings>("config")?;
        let db: DbSection = serde_json::from_value(settings.0.get("database").cloned().unwrap_or(json!({})))
            .map_err(|e| cordis::Error::msg(format!("bad database section: {e}")))?;

        ctx.effect("database pool", async move {
            // Pretend to open min connections here...
            let close: cordis::fiber::Disposer = Box::new(|| Box::pin(async {}));
            Ok(Some(close))
        })
        .await?;

        eprintln!("      [database] opened {} (pool={})", db.url, db.pool);
        ctx.provide("database", Database { url: db.url, pool: db.pool }).await?;
        Ok(())
    },
    )
}

// ---------------------------------------------------------------------------
// Plugin 3: api server — requires "database"; serves until disposed.
// ---------------------------------------------------------------------------

fn api_plugin() -> Arc<dyn Plugin> {
    plugin_with(
        "api",
        vec![Injection::from("database")],
        |ctx: Context, _config: ()| async move {
            let db = ctx.require::<Database>("database")?;
            eprintln!("      [api] serving requests against {} (pool={})", db.url, db.pool);
            Ok(())
        },
    )
}

// ---------------------------------------------------------------------------
// The harness: compose, start, observe, hot-update, stop.
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = Context::new();

    // Register dependents BEFORE providers: they wait (PENDING) reactively.
    let api: FiberHandle = ctx.plugin(api_plugin(), None);
    let database = ctx.plugin(database_plugin(), None);

    let config = json!({
        "database": { "url": "postgres://localhost/app", "pool": 8 }
    });
    let settings = ctx.plugin(settings_plugin(config.clone()), None);

    settings.join().await?;
    database.join().await?;
    api.join().await?;
    eprintln!("-- all plugins converged --");

    // Hot update: change the config section; the graph reflows by itself.
    eprintln!("-- updating settings (HMR-style) --");
    let updated = json!({
        "database": { "url": "postgres://replica/app", "pool": 16 }
    });
    settings.update(updated).await?;
    api.join().await?;
    eprintln!("   api state after reflow: {:?}", api.state());

    // Observe internal lifecycle events, the extension point inventory/HMR
    // plugins build on.
    ctx.on_global("internal/status", |_ctx, payload, _next| {
        Box::pin(async move {
            eprintln!(
                "   [status] {} -> {}",
                payload["fiber"].as_str().unwrap_or("?"),
                payload["state"].as_str().unwrap_or("?")
            );
            Ok(Value::Null)
        })
    })
    .await?;

    // Graceful shutdown: newest registrations unwind first, LIFO effects.
    eprintln!("-- stopping harness --");
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.stop().await;
    eprintln!("-- stopped cleanly --");

    Ok(())
}

// anyhow is intentionally not a dependency of cordis itself; this example
// uses a minimal shim to keep the workspace dependency-free of extras.
mod anyhow {
    pub type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;
}
