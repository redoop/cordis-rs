//! Accessor (computed) services, scope identity, and effect introspection.

use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use cordis::{plugin, Context, FiberState, Plugin};

#[derive(Debug, Clone, PartialEq)]
struct Database {
    url: String,
}

fn database_plugin() -> Arc<dyn Plugin> {
    plugin("database", |ctx: Context, _config: Value| async move {
        ctx.provide("database", Database { url: "provided".into() }).await?;
        Ok(())
    })
}

fn summary_plugin(reads: Arc<AtomicUsize>) -> Arc<dyn Plugin> {
    plugin("summary", move |ctx: Context, _config: Value| {
        let counter = reads.clone();
        async move {
            ctx.accessor::<Database, _>("summary", move |ctx| {
                counter.fetch_add(1, Ordering::SeqCst);
                let db = ctx.get::<Database>("database")?;
                Some(Database { url: db.url.to_uppercase() })
            })
            .await?;
            Ok(())
        }
    })
}

#[tokio::test]
async fn accessor_computes_per_read_from_live_sources() {
    let ctx = Context::new();
    let reads = Arc::new(AtomicUsize::new(0));

    ctx.plugin(database_plugin(), None).join().await.unwrap();
    let summary = ctx.plugin(summary_plugin(reads.clone()), None);
    summary.join().await.unwrap();

    assert_eq!(ctx.get::<Database>("summary").unwrap().url, "PROVIDED");
    let after_first = reads.load(Ordering::SeqCst);
    assert_eq!(after_first, 1);

    // A second read recomputes: dynamic entries are never cached.
    assert_eq!(ctx.get::<Database>("summary").unwrap().url, "PROVIDED");
    assert_eq!(reads.load(Ordering::SeqCst), after_first + 1);

    // Disposing the accessor owner removes the derived service.
    summary.dispose().await;
    assert!(ctx.get::<Database>("summary").is_none());

    ctx.stop().await;
}

#[tokio::test]
async fn accessor_unavailable_when_source_missing() {
    let ctx = Context::new();

    let derived = plugin("derived", |ctx: Context, _c: Value| async move {
        ctx.accessor::<Database, _>("summary", |ctx| {
            let db = ctx.get::<Database>("database")?;
            Some(Database { url: format!("view of {}", db.url) })
        })
        .await?;
        Ok(())
    });
    let fiber = ctx.plugin(derived, None);
    fiber.join().await.unwrap();

    // No provider yet: strict resolution hides the computed entry entirely,
    // but the accessor's own fiber stays healthy.
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(ctx.get::<Database>("summary").is_none());

    // The source appears later: the view resolves without re-registration.
    ctx.plugin(database_plugin(), None).join().await.unwrap();
    assert_eq!(ctx.get::<Database>("summary").unwrap().url, "view of provided");

    ctx.stop().await;
}

#[tokio::test]
async fn duplicate_accessor_rejected_like_provide() {
    let ctx = Context::new();
    let make = || {
        plugin("derived", |ctx: Context, _c: Value| async move {
            ctx.accessor::<String, _>("computed", |_ctx| Some("x".to_string())).await?;
            Ok(())
        })
    };
    let first = ctx.plugin(make(), None);
    first.join().await.unwrap();

    let second = ctx.plugin(make(), None);
    second.join().await.expect_err("same name in same scope must collide");

    // An isolated scope may host its own accessor under the same name.
    let iso = ctx.isolate("computed");
    iso.plugin(make(), None).join().await.unwrap();

    ctx.stop().await;
}

#[tokio::test]
async fn scope_identity_and_effect_introspection() {
    let ctx = Context::new();
    assert!(ctx.is(&ctx));
    let scoped = ctx.isolate("cache");
    assert!(!ctx.is(&scoped), "isolate creates a distinct scope");
    assert!(!scoped.is(&ctx));

    let fiber = ctx.plugin(database_plugin(), None);
    fiber.join().await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);

    // The provide() registration shows up as a named live effect.
    let metas = fiber.effect_metas();
    assert!(metas.iter().any(|m| m.contains("provide")), "effects: {metas:?}");

    ctx.stop().await;
}

#[tokio::test]
async fn accessor_reads_are_stable_under_high_frequency() {
    // The hot path for accessor reads must stay lock-light and exact: every
    // read recomputes, and repeated reads observe a stable value.
    let ctx = Context::new();
    let reads = Arc::new(AtomicUsize::new(0));
    let db = ctx.plugin(database_plugin(), None);
    let summary = ctx.plugin(summary_plugin(reads.clone()), None);
    db.join().await.unwrap();
    summary.join().await.unwrap();

    for i in 0..1000usize {
        let value = ctx.get::<Database>("summary").expect("accessor live");
        assert_eq!(value.url, "PROVIDED");
        let _ = i;
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1000, "every read recomputes");
    ctx.stop().await;
}
