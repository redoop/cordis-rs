//! Isolation scopes and intercept config — the `isolate`/`intercept` port.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;

use cordis::{plugin, Context, FiberState, Plugin};

#[derive(Debug, Clone, PartialEq)]
struct Cache {
    region: String,
}

#[derive(Deserialize)]
struct CacheConfig {
    region: String,
}

fn cache_plugin() -> Arc<dyn Plugin> {
    plugin("cache", |ctx: Context, config: CacheConfig| async move {
        ctx.provide("cache", Cache { region: config.region }).await?;
        Ok(())
    })
}

#[tokio::test]
async fn isolated_scopes_host_independent_implementations() {
    let ctx = Context::new();

    // Default scope: one implementation.
    let global = ctx.plugin(cache_plugin(), Some(json!({ "region": "eu" })));
    global.join().await.unwrap();
    assert_eq!(ctx.get::<Cache>("cache").unwrap().region, "eu");

    // Isolated scope: a DIFFERENT implementation of the same service name.
    let tenant_a = ctx.isolate("cache");
    let scoped = tenant_a.plugin(cache_plugin(), Some(json!({ "region": "us" })));
    scoped.join().await.unwrap();
    assert_eq!(tenant_a.get::<Cache>("cache").unwrap().region, "us");
    // Parent scope untouched:
    assert_eq!(ctx.get::<Cache>("cache").unwrap().region, "eu");

    ctx.stop().await;
}

#[tokio::test]
async fn intercept_layers_merge_outermost_first() {
    let base = Context::new()
        .intercept("llm", json!({ "model": "base-model", "temperature": 0.7 }))
        .intercept("llm", json!({ "temperature": 0.2 }))
        .intercept("llm", json!({ "api_key": "sk-test" }));

    // Later (inner) layers win on conflicts; non-conflicting keys persist.
    let merged = base.service_config("llm");
    assert_eq!(
        merged,
        json!({ "model": "base-model", "temperature": 0.2, "api_key": "sk-test" })
    );

    // Unrelated service unaffected.
    assert_eq!(base.service_config("other"), json!({}));
}

#[tokio::test]
async fn services_in_other_scopes_are_invisible() {
    let ctx = Context::new().intercept("cache", json!({ "x": 1 }));
    let scoped = ctx.isolate("cache");
    let fiber = scoped.plugin(cache_plugin(), Some(json!({ "region": "ap" })));
    fiber.join().await.unwrap();

    assert_eq!(scoped.get::<Cache>("cache").unwrap().region, "ap");
    assert!(ctx.get::<Cache>("cache").is_none(), "parent must not see child scope");

    ctx.stop().await;
}

#[tokio::test]
async fn duplicate_provision_rejected_per_scope() {
    let ctx = Context::new();
    let first = ctx.plugin(cache_plugin(), Some(json!({ "region": "a" })));
    first.join().await.unwrap();

    let second = ctx.plugin(cache_plugin(), Some(json!({ "region": "b" })));
    second.join().await.expect_err("same scope + same name must fail");

    // But an isolated scope may host its own.
    let iso = ctx.isolate("cache");
    let third = iso.plugin(cache_plugin(), Some(json!({ "region": "b" })));
    third.join().await.unwrap();
    assert_eq!(third.state(), FiberState::Active);

    ctx.stop().await;
}
