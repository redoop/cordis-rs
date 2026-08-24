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

#[tokio::test]
async fn emit_sync_runs_sync_listeners_inline_before_returning() {
    // Inline execution is observable as STRICT ORDERING: a sync-slot listener
    // that awaits a yield still completes before emit_sync returns, so the
    // recorded sequence is ["hear", "after"]. A spawned listener could be
    // reordered (["after", "hear"]), which is exactly the overhead/coupling
    // the sync slot removes.
    let ctx = Context::new();
    let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

    ctx.on_sync("ordered", {
        let order = order.clone();
        move |_ctx, _p, _n| {
            let order = order.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                order.lock().unwrap().push("hear");
                Ok(Value::Null)
            })
        }
    })
    .await
    .unwrap();

    ctx.emit_sync("ordered", json!(null)).await.expect("sync dispatch succeeds");
    order.lock().unwrap().push("after");
    assert_eq!(
        order.lock().unwrap().as_slice(),
        ["hear", "after"],
        "sync listener must complete inline before emit_sync returns"
    );

    // Fire-and-forget emit STILL spawns sync-slot listeners: the same
    // listener may observe "after" first (no ordering guarantee).
    let order2 = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
    let order2_for_listener = order2.clone();
    ctx.on_sync("unordered", move |_ctx, _p, _n| {
        let order = order2_for_listener.clone();
        Box::pin(async move {
            order.lock().unwrap().push("hear");
            Ok(Value::Null)
        })
    })
    .await
    .unwrap();
    ctx.emit("unordered", json!(null));
    order2.lock().unwrap().push("after");
    // Give the detached listener a beat so "hear" is usually recorded too.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let recorded = order2.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "spawned listener must still fire, got {recorded:?}");
    ctx.stop().await;
}

#[tokio::test]
async fn emit_sync_aggregates_sync_failures_while_ordinary_listeners_run() {
    let ctx = Context::new();
    let hits = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

    ctx.on_sync("mixed", {
        let hits = hits.clone();
        move |_ctx, _p, _n| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.lock().unwrap().push("sync-bad");
                Err(cordis::Error::msg("sync boom"))
            })
        }
    })
    .await
    .unwrap();
    ctx.on("mixed", {
        let hits = hits.clone();
        move |_ctx, _p, _n| {
            let hits = hits.clone();
            Box::pin(async move {
                hits.lock().unwrap().push("spawned-ok");
                Ok(Value::Null)
            })
        }
    })
    .await
    .unwrap();

    // Sync-slot failure surfaces as an aggregate; the spawned listener still
    // runs (its error/failure rule is unchanged: fire-and-forget).
    let err = ctx.emit_sync("mixed", json!(null)).await.expect_err("aggregate");
    assert!(matches!(err, cordis::Error::Aggregate(_)), "{err}");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let recorded = hits.lock().unwrap().clone();
    assert!(recorded.contains(&"sync-bad"));
    assert!(recorded.contains(&"spawned-ok"));
    ctx.stop().await;
}

#[tokio::test]
async fn parallel_timeout_bounds_hung_listeners() {
    let ctx = Context::new();
    // A listener that never answers; parallel would wait forever.
    let gate = Arc::new(tokio::sync::Notify::new());
    ctx.on("hung", {
        let gate = gate.clone();
        move |_ctx, _p, _n| {
            let gate = gate.clone();
            Box::pin(async move {
                gate.notified().await;
                Ok(Value::Null)
            })
        }
    })
    .await
    .unwrap();

    let err = ctx
        .parallel_timeout("hung", json!(null), std::time::Duration::from_millis(100))
        .await
        .expect_err("must time out");
    assert!(
        matches!(err, cordis::Error::Coded(ref coded) if coded.code == cordis::CordisCode::Timeout),
        "expected coded Timeout, got: {err}"
    );
    gate.notify_one();
    ctx.stop().await;
}

#[tokio::test]
async fn parallel_without_timeout_still_waits_for_all() {
    let ctx = Context::new();
    let hits = Arc::new(std::sync::Mutex::new(0usize));
    for _ in 0..3 {
        let hits = hits.clone();
        ctx.on("all", move |_ctx, _p, _n| {
            let hits = hits.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                *hits.lock().unwrap() += 1;
                Ok(Value::Null)
            })
        })
        .await
        .unwrap();
    }
    let out = ctx.parallel("all", json!(7)).await.unwrap();
    assert_eq!(out, json!(7));
    assert_eq!(*hits.lock().unwrap(), 3);
    ctx.stop().await;
}
