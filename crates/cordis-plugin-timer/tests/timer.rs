//! Timer plugin tests: fiber-owned timers cancel on unload.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cordis::{Context, Injection};
use cordis_plugin_timer::timer;

#[tokio::test]
async fn service_is_visible_without_inject() {
    let ctx = Context::new();
    ctx.plugin(timer(), None).join().await.unwrap();

    // The providing fiber is active, so strict resolution succeeds.
    assert!(ctx.get::<cordis_plugin_timer::TimerService>("timer").is_some());
    ctx.stop().await;
}

#[tokio::test]
async fn injected_fiber_uses_mixin_timeout() {
    let ctx = Context::new();
    ctx.plugin(timer(), None).join().await.unwrap();

    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();

    let fiber = ctx.inject(vec![Injection::from("timer")], move |ctx| {
        let flag = flag.clone();
        Box::pin(async move {
            use cordis_plugin_timer::TimerContextExt as _;
            ctx.timeout(Duration::from_millis(20)).await?.await?;
            flag.store(true, Ordering::SeqCst);
            Ok(())
        }) as cordis::plugin::BoxFuture<cordis::Result<()>>
    });
    fiber.join().await.unwrap();
    assert!(ran.load(Ordering::SeqCst));

    ctx.stop().await;
}

#[tokio::test]
async fn interval_ticks_and_stops_on_dispose() {
    let ctx = Context::new();
    ctx.plugin(timer(), None).join().await.unwrap();

    let ticks = Arc::new(AtomicUsize::new(0));
    let counter = ticks.clone();

    let fiber = ctx.inject(vec!["timer"], move |ctx| {
        let counter = counter.clone();
        Box::pin(async move {
            use cordis_plugin_timer::TimerContextExt as _;
            let mut stream = ctx.interval(Duration::from_millis(10)).await?;
            for _ in 0..3 {
                stream.next().await.expect("tick")?;
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }) as cordis::plugin::BoxFuture<cordis::Result<()>>
    });
    fiber.join().await.unwrap();
    assert_eq!(ticks.load(Ordering::SeqCst), 3);

    fiber.dispose().await;
}

#[tokio::test]
async fn pending_timeout_resolves_as_disposed_when_owner_unloads() {
    let ctx = Context::new();
    ctx.plugin(timer(), None).join().await.unwrap();

    // The waiter records what the timeout future resolved to after the
    // owning fiber unloads mid-sleep.
    let outcome: Arc<Mutex<Option<Result<(), cordis::Error>>>> =
        Arc::new(Mutex::new(None));

    let outcome_for_body = outcome.clone();
    let fiber = ctx.inject(vec!["timer"], move |ctx| {
        let outcome = outcome_for_body.clone();
        Box::pin(async move {
            use cordis_plugin_timer::TimerContextExt as _;
            let fut = ctx.timeout(Duration::from_secs(60)).await?;
            let handle = tokio::spawn(async move {
                let result = fut.await;
                *outcome.lock().unwrap() = Some(result);
            });
            // Give the waiter a beat to start parking on the timer.
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(handle); // detached: survives this body, dies with runtime
            Ok(())
        }) as cordis::plugin::BoxFuture<cordis::Result<()>>
    });

    // Body done: in cordis the fiber stays ACTIVE with its effects live.
    // Disposing it must cancel the still-pending timer.
    fiber.join().await.unwrap();
    fiber.dispose().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let recorded = outcome.lock().unwrap().clone();
    let result = recorded.expect("waiter must have finished");
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        cordis_plugin_timer::DISPOSED_MESSAGE
    );

    ctx.stop().await;
}
