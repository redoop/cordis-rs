//! Timer service plugin for cordis-rs — the analogue of `@deepseek-ai/cordis-plugin-timer`.
//!
//! Provides the `timer` service: timeout and interval primitives whose
//! underlying tasks are fiber effects, so every pending timer is cancelled
//! automatically when the owning plugin unloads.
//!
//! Ergonomics mirror cordis' mixin pattern via the extension trait
//! `TimerContextExt`: call `ctx.timeout(...)` / `ctx.interval(...)` on any
//! context whose fiber injected the `timer` service.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use serde_json::Value;

use cordis::error::{Error, Result};
use cordis::{plugin, Context, Plugin};

/// Error surfaced when a timer is cancelled by context disposal.
pub const DISPOSED_MESSAGE: &str = "context has been disposed";

fn disposed_error() -> Error {
    Error::msg(DISPOSED_MESSAGE)
}

fn no_timer_error() -> Error {
    Error::msg("cannot get property \"timer\" without inject")
}

/// The `timer` service value.
#[derive(Clone)]
pub struct TimerService {
    ctx: Context,
}

impl TimerService {
    /// Sleep for `duration`, tied to the calling fiber's lifetime.
    ///
    /// The returned future resolves `Err` when the fiber unloads first.
    pub async fn timeout(&self, duration: Duration) -> Result<TimeoutFuture> {
        self.timeout_in(&self.ctx.clone(), duration).await
    }

    /// Like `timeout`, but the effect is owned by `scope`'s fiber — the
    /// calling fiber for mixin usage, so timers die with their user.
    pub async fn timeout_in(&self, scope: &Context, duration: Duration) -> Result<TimeoutFuture> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<()>>();
        let ctx = scope.clone();
        let guard = ctx.effect("ctx.timeout()", async move {
            let task = tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                let _ = tx.send(Ok(()));
            });
            let disposer: cordis::Disposer = Box::new(move || {
                task.abort();
                Box::pin(async move {})
            });
            Ok(Some(disposer))
        })
        .await?;
        Ok(TimeoutFuture { rx: Some(rx), _guard: Some(guard) })
    }

    /// Tick every `period`; ticks stop arriving once the fiber unloads.
    pub async fn interval(&self, period: Duration) -> Result<IntervalStream> {
        self.interval_in(&self.ctx.clone(), period).await
    }

    /// Like `interval`, but owned by `scope`'s fiber.
    pub async fn interval_in(
        &self,
        scope: &Context,
        period: Duration,
    ) -> Result<IntervalStream> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<()>>();
        let guard = scope.effect("ctx.interval()", async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let task = tokio::spawn(async move {
                loop {
                    ticker.tick().await;
                    if tx.send(Ok(())).is_err() {
                        break;
                    }
                }
            });
            let disposer: cordis::Disposer = Box::new(move || {
                task.abort();
                Box::pin(async move {})
            });
            Ok(Some(disposer))
        })
        .await?;
        Ok(IntervalStream { rx, _guard: Some(guard), closed_seen: false })
    }
}

/// Resolves once after `duration`, or errors when the fiber disposes it.
pub struct TimeoutFuture {
    rx: Option<tokio::sync::oneshot::Receiver<Result<()>>>,
    _guard: Option<cordis::EffectGuard>,
}

impl Future for TimeoutFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let Some(rx) = self.rx.as_mut() else {
            return Poll::Ready(Err(disposed_error()));
        };
        match Pin::new(rx).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(result)) => {
                self._guard = None;
                self.rx = None;
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                self._guard = None;
                self.rx = None;
                Poll::Ready(Err(disposed_error()))
            }
        }
    }
}

/// Tick stream; after disposal every `next()` yields a disposed error once,
/// then `None`.
pub struct IntervalStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<Result<()>>,
    _guard: Option<cordis::EffectGuard>,
    closed_seen: bool,
}

impl IntervalStream {
    /// Receive the next tick. Yields one final disposed error when the owning
    /// fiber unloaded, then `None` forever.
    pub async fn next(&mut self) -> Option<Result<()>> {
        if self.closed_seen {
            return None;
        }
        match self.rx.recv().await {
            Some(item) => Some(item),
            None => {
                self.closed_seen = true;
                Some(Err(disposed_error()))
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.rx.is_closed()
    }

    /// Drop the underlying effect guard eagerly (cancels ticking).
    pub fn cancel(&mut self) {
        self._guard = None;
        self.rx.close();
    }
}

struct TimerPlugin;

impl Plugin for TimerPlugin {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("timer")
    }

    fn resolve_config(&self, raw: &Value) -> Result<Value> {
        Ok(raw.clone())
    }

    fn apply(&self, ctx: Context, _config: Value) -> plugin::BoxFuture<Result<()>> {
        Box::pin(async move {
            ctx.provide("timer", TimerService { ctx: ctx.extend() }).await?;
            Ok(())
        })
    }
}

/// Build the timer plugin: `ctx.plugin(cordis_plugin_timer::timer(), None)`.
pub fn timer() -> Arc<dyn Plugin> {
    Arc::new(TimerPlugin)
}

/// Mixin-style extension trait mirroring cordis' `ctx.mixin("timer", ...)`.
///
/// Implemented for `Context` so plugin bodies can write `ctx.timeout(..)`
/// directly once their fiber injects `timer`. On a context without the
/// service, `timeout` fails immediately and `interval` returns an error.
pub trait TimerContextExt {
    fn timeout(&self, duration: Duration)
        -> impl Future<Output = Result<TimeoutFuture>> + Send;
    fn interval(&self, period: Duration) -> impl Future<Output = Result<IntervalStream>> + Send;
    fn timer(&self) -> Result<TimerService>;
}

impl TimerContextExt for Context {
    async fn timeout(&self, duration: Duration) -> Result<TimeoutFuture> {
        match self.timer() {
            Ok(service) => service.timeout_in(self, duration).await,
            Err(_) => Err(no_timer_error()),
        }
    }

    async fn interval(&self, period: Duration) -> Result<IntervalStream> {
        self.timer()?.interval_in(self, period).await
    }

    fn timer(&self) -> Result<TimerService> {
        self.require::<TimerService>("timer").map(|arc| (*arc).clone())
    }
}
