//! Event dispatch modes: emit, parallel, serial/bail, waterfall.

use std::sync::Arc;

use serde_json::{json, Value};

use cordis::Context;

#[tokio::test]
async fn parallel_aggregates_listener_errors() {
    let ctx = Context::new();
    let hits = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

    for name in ["a", "b"] {
        let hits = hits.clone();
        ctx.on("ping", move |_ctx, _p, _n| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.lock().unwrap().push(name);
                if name == "b" {
                    Err(cordis::Error::msg("boom"))
                } else {
                    Ok(Value::Null)
                }
            })
        })
        .await
        .unwrap();
    }

    let err = ctx.parallel("ping", json!(null)).await.expect_err("must aggregate");
    assert!(matches!(err, cordis::Error::Aggregate(_)), "{err}");
    // Every listener still ran despite the failure.
    assert_eq!(hits.lock().unwrap().to_vec(), ["a", "b"]);
}

#[tokio::test]
async fn serial_stops_at_first_bail() {
    let ctx = Context::new();
    let hits = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

    for (name, bail) in [("first", true), ("second", false)] {
        let hits = hits.clone();
        ctx.on("gate", move |_ctx, mut payload, _n| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.lock().unwrap().push(name);
                if bail {
                    return Ok(json!({ "bailed_by": name }));
                }
                if let Value::Object(map) = &mut payload {
                    map.insert("touched".into(), json!(name));
                }
                Ok(payload)
            })
        })
        .await
        .unwrap();
    }

    let out = ctx.serial("gate", json!({})).await.unwrap();
    assert_eq!(out, json!({ "bailed_by": "first" }));
    assert_eq!(hits.lock().unwrap().to_vec(), ["first"], "second must not run");
}

#[tokio::test]
async fn any_object_result_bails_serial() {
    // Cordis semantics: a listener returning ANY non-null/non-false value
    // bails. Chaining transforms belongs to waterfall, not serial.
    let ctx = Context::new();
    ctx.on("flow", |_ctx, mut p, _n| {
        Box::pin(async move {
            if let Value::Object(m) = &mut p {
                m.insert("one".into(), json!(true));
            }
            Ok(p)
        })
    })
    .await
    .unwrap();

    let out = ctx.serial("flow", json!({ "seed": 1 })).await.unwrap();
    assert_eq!(out, json!({ "seed": 1, "one": true }));
}

#[tokio::test]
async fn null_listeners_let_serial_continue() {
    let ctx = Context::new();
    ctx.on("flow", |_ctx, _p, _n| {
        Box::pin(async move { Ok(Value::Null) })
    })
    .await
    .unwrap();
    ctx.on("flow", |_ctx, _p, _n| {
        Box::pin(async move { Ok(json!(false)) })
    })
    .await
    .unwrap();

    // Neither null nor false bails: dispatch falls through to the payload.
    let out = ctx.serial("flow", json!({ "reached": true })).await.unwrap();
    assert_eq!(out, json!({ "reached": true }));
}

#[tokio::test]
async fn waterfall_composes_and_can_veto() {
    let ctx = Context::new();

    // Outermost: prepend listener adds a prefix then delegates.
    ctx.on_global("config", |_ctx, mut p, next| {
        Box::pin(async move {
            if let Value::Object(m) = &mut p {
                m.insert("prefix".into(), json!("global"));
            }
            next.run(p).await
        })
    })
    .await
    .unwrap();

    // Innermost: normal listener appends a suffix and delegates to fallback.
    ctx.on("config", |_ctx, mut p, next| {
        Box::pin(async move {
            if let Value::Object(m) = &mut p {
                m.insert("suffix".into(), json!("local"));
            }
            next.run(p).await
        })
    })
    .await
    .unwrap();

    let final_value = ctx
        .waterfall(
            "config",
            json!({}),
            |mut payload| {
                Box::pin(async move {
                    if let Value::Object(m) = &mut payload {
                        m.insert("builtin".into(), json!(true));
                    }
                    Ok(payload)
                }) as cordis::plugin::BoxFuture<cordis::Result<Value>>
            },
        )
        .await
        .unwrap();
    assert_eq!(
        final_value,
        json!({ "prefix": "global", "suffix": "local", "builtin": true })
    );
}

#[tokio::test]
async fn waterfall_veto_blocks_builtin() {
    let ctx = Context::new();
    let builtin_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let flag = builtin_ran.clone();
    ctx.on("auth", move |_ctx, _p, _next| {
        let flag = flag.clone();
        Box::pin(async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            // VETO: never call next.
            Ok(json!({ "allowed": false }))
        })
    })
    .await
    .unwrap();

    let out = ctx
        .waterfall("auth", json!({ "user": "root" }), |_p| {
            Box::pin(async { Ok(json!({ "allowed": true })) })
                as cordis::plugin::BoxFuture<cordis::Result<Value>>
        })
        .await
        .unwrap();
    assert_eq!(out, json!({ "allowed": false }));
    assert!(builtin_ran.load(std::sync::atomic::Ordering::SeqCst), "listener ran");
}

#[tokio::test]
async fn once_fires_exactly_once() {
    let ctx = Context::new();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let c = count.clone();
    ctx.once("boot", move |_ctx, _p, _n| {
        let c = c.clone();
        Box::pin(async move {
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Value::Null)
        })
    })
    .await
    .unwrap();

    ctx.emit("boot", json!(null));
    // Detached tasks need a yield to run; serial is synchronous enough.
    let _ = ctx.serial("boot", json!(null)).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
}
