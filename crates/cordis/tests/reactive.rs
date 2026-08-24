//! The crown-jewel test: reactive dependency lifecycle.
//!
//! A plugin stays PENDING until its required services go live, unloads when a
//! provider disappears, and reloads when service returns — no manual wiring.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use cordis::{plugin, plugin_with, Context, FiberState, Injection, Plugin};

#[derive(Debug, Clone)]
struct Database {
    url: String,
}

#[derive(Deserialize)]
struct DbConfig {
    url: String,
}

fn database_plugin() -> Arc<dyn Plugin> {
    plugin("database", |ctx: Context, config: DbConfig| async move {
        ctx.effect("db connection", async move {
            Ok(Some(Box::new(|| {
                Box::pin(async {}) as cordis::plugin::BoxFuture<()>
            }) as cordis::fiber::Disposer))
        })
        .await?;
        ctx.provide("database", Database { url: config.url }).await?;
        Ok(())
    })
}

fn consumer_plugin(log: Arc<std::sync::Mutex<Vec<String>>>) -> Arc<dyn Plugin> {
    plugin_with(
        "api",
        vec![Injection::from("database")],
        move |ctx: Context, _config: ()| {
            let log = log.clone();
            async move {
                let db = ctx.require::<Database>("database")?;
                log.lock().unwrap().push(format!("api started with {}", db.url));
                Ok(())
            }
        },
    )
}

#[tokio::test]
async fn pending_until_dependency_provided() {
    let ctx = Context::new();
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Consumer first: nothing provides "database" yet.
    let api = ctx.inject(vec!["database"], {
        let log = log.clone();
        move |ctx: Context| {
            let log = log.clone();
            Box::pin(async move {
                let db = ctx.require::<Database>("database")?;
                log.lock().unwrap().push(format!("anon started with {}", db.url));
                Ok(())
            }) as cordis::plugin::BoxFuture<cordis::Result<()>>
        }
    });
    assert_eq!(api.state(), FiberState::Pending);
    assert!(ctx.get::<Database>("database").is_none());

    // Now provide the dependency via a provider plugin.
    let db_fiber = ctx.plugin(database_plugin(), Some(json!({ "url": "postgres://x" })));
    db_fiber.join().await.expect("provider starts");

    api.join().await.expect("consumer activates once dep appears");
    assert_eq!(api.state(), FiberState::Active);
    assert_eq!(log.lock().unwrap().as_slice(), ["anon started with postgres://x"]);

    ctx.stop().await;
}

#[tokio::test]
async fn dependent_unloads_when_provider_disappears_and_reloads_on_return() {
    let ctx = Context::new();
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let consumer = ctx.plugin(consumer_plugin(log.clone()), None);

    let provider_a = ctx.plugin(database_plugin(), Some(json!({ "url": "db-a" })));
    provider_a.join().await.unwrap();
    consumer.join().await.unwrap();
    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(log.lock().unwrap().as_slice(), ["api started with db-a"]);

    // Withdraw the provider: cascade unload of the consumer.
    provider_a.dispose().await;
    consumer.join().await.unwrap();
    assert_eq!(consumer.state(), FiberState::Pending);
    assert!(ctx.get::<Database>("database").is_none());

    // Provide again from a different plugin instance: reload.
    let provider_b = ctx.plugin(database_plugin(), Some(json!({ "url": "db-b" })));
    provider_b.join().await.unwrap();
    consumer.join().await.unwrap();
    assert_eq!(consumer.state(), FiberState::Active);
    assert_eq!(
        log.lock().unwrap().as_slice(),
        ["api started with db-a", "api started with db-b"]
    );

    ctx.stop().await;
}

#[tokio::test]
async fn restart_reloads_with_same_config() {
    let ctx = Context::new();
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let consumer = ctx.plugin(consumer_plugin(log.clone()), None);
    let provider = ctx.plugin(database_plugin(), Some(json!({ "url": "same" })));
    let _ = (provider.join().await, consumer.join().await);
    let runs_before = log.lock().unwrap().len();

    consumer.restart().await.expect("restart succeeds");
    assert!(log.lock().unwrap().len() > runs_before);
    assert_eq!(consumer.state(), FiberState::Active);

    ctx.stop().await;
}

#[tokio::test]
async fn update_applies_new_config_through_waterfall() {
    #[derive(Deserialize)]
    struct Greeting {
        name: String,
    }

    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let greeter = plugin("greeter", {
        let seen = seen.clone();
        move |ctx: Context, config: Greeting| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(config.name);
                let _ = ctx;
                Ok(())
            }
        }
    });
    let ctx = Context::new();
    let fiber = ctx.plugin(greeter, Some(json!({ "name": "v1" })));
    fiber.join().await.unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), ["v1"]);

    fiber.update(json!({ "name": "v2" })).await.expect("hot update");
    assert_eq!(seen.lock().unwrap().as_slice(), ["v1", "v2"]);
    assert_eq!(fiber.state(), FiberState::Active);

    ctx.stop().await;
}

#[tokio::test]
async fn failed_config_marks_failed_and_recovers_on_update() {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct NeedsPort {
        port: u16,
    }

    let server = plugin("server", |_ctx: Context, _config: NeedsPort| async move {
        Ok(())
    });
    let ctx = Context::new();
    let fiber = ctx.plugin(server, Some(json!({})));

    let err = fiber.join().await.expect_err("validation must fail");
    assert!(matches!(err, cordis::Error::Validation(_)), "got: {err}");
    assert_eq!(fiber.state(), FiberState::Failed);

    // Recovery path: update with valid config re-arms the driver.
    fiber.update(json!({ "port": 8080 })).await.expect("valid update");
    assert_eq!(fiber.state(), FiberState::Active);

    ctx.stop().await;
}

#[tokio::test]
async fn effects_dispose_in_lifo_order() {
    let order = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let order_for_plugin = order.clone();
    let effects_plugin = plugin("effects", move |ctx: Context, _c: ()| {
        let order = order_for_plugin.clone();
        async move {
                for tag in ["a", "b", "c"] {
                    let order = order.clone();
                    let tag = tag.to_string();
                    ctx.effect(format!("effect {tag}"), async move {
                        let order = order.clone();
                        let tag2 = tag.clone();
                        Ok(Some(Box::new(move || {
                            let order = order.clone();
                            let tag3 = tag2.clone();
                            Box::pin(async move {
                                order.lock().unwrap().push(tag3);
                            }) as cordis::plugin::BoxFuture<()>
                        }) as cordis::fiber::Disposer))
                    })
                    .await?;
                }
                Ok(())
            }
    });

    let ctx = Context::new();
    let fiber = ctx.plugin(effects_plugin, None);
    fiber.join().await.unwrap();
    assert_eq!(order.lock().unwrap().len(), 0, "nothing disposed yet");
    assert_eq!(fiber.effect_count(), 3);

    fiber.dispose().await;
    assert_eq!(order.lock().unwrap().as_slice(), ["c", "b", "a"], "LIFO");
}

#[tokio::test]
async fn internal_status_events_track_transitions() {
    let ctx = Context::new();
    let states = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = states.clone();
    ctx.on_global("internal/status", move |_ctx: Context, payload: serde_json::Value, _next: cordis::Next| {
        let sink = sink.clone();
        Box::pin(async move {
            if let Some(state) = payload.get("state").and_then(|v: &serde_json::Value| v.as_str()) {
                sink.lock().unwrap().push(state.to_string());
            }
            Ok(serde_json::Value::Null)
        })
    })
    .await
    .unwrap();

    let provider = ctx.plugin(database_plugin(), Some(json!({ "url": "s" })));
    provider.join().await.unwrap();

    // Give detached emit tasks a beat to record.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let recorded = states.lock().unwrap().clone();
    assert!(recorded.contains(&"loading".to_string()), "{recorded:?}");
    assert!(recorded.contains(&"active".to_string()), "{recorded:?}");

    ctx.stop().await;
}

#[tokio::test]
async fn state_cache_tracks_loading_then_active_then_disposed() {
    // The atomic state mirror must be observable lock-free and consistent with
    // every transition point (P1: state() used to chain four mutexes).
    let gate = Arc::new(tokio::sync::Notify::new());

    let g1 = gate.clone();
    let gated = plugin("gated", move |_ctx: Context, _c: ()| {
        let g = g1.clone();
        async move {
            g.notified().await;
            Ok(())
        }
    });

    let ctx = Context::new();
    let fiber = ctx.plugin(gated, None);

    // The driver is parked inside apply() → converged state is Loading.
    assert_eq!(fiber.state(), FiberState::Loading);

    gate.notify_one();
    fiber.join().await.expect("converges after release");
    assert_eq!(fiber.state(), FiberState::Active);

    ctx.stop().await;
    assert_eq!(fiber.state(), FiberState::Disposed);
}

#[tokio::test]
async fn join_with_timeout_bounds_a_never_finishing_apply() {
    // A plugin whose apply() never completes used to wedge join() forever;
    // the bounded join returns a timeout error while the driver keeps running.
    let gate = Arc::new(tokio::sync::Notify::new());

    let g1 = gate.clone();
    let stuck = plugin("stuck", move |_ctx: Context, _c: ()| {
        let g = g1.clone();
        async move {
            g.notified().await;
            Ok(())
        }
    });

    let ctx = Context::new();
    let fiber = ctx.plugin(stuck, None);
    assert_eq!(fiber.state(), FiberState::Loading);

    let err = fiber
        .join_with_timeout(Duration::from_millis(100))
        .await
        .expect_err("join must time out while apply is parked");
    assert!(err.to_string().contains("timed out"), "got: {err}");

    // The fiber is still healthy: releasing the gate lets it converge, and a
    // later unbounded join succeeds.
    gate.notify_one();
    fiber.join().await.expect("converges after release");
    assert_eq!(fiber.state(), FiberState::Active);
    ctx.stop().await;
}
