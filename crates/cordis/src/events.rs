//! Event bus installed on every context, mirroring `EventsService`.
//!
//! Listeners are fiber effects: registering one on a context owned by a fiber
//! disposes it automatically when that fiber unloads. Dispatch supports the
//! five cordis modes — emit, parallel, serial, bail, waterfall — plus scoped
//! filtering used by internal service-availability events.

use std::collections::VecDeque;
use std::sync::Arc;

use serde_json::Value;

use crate::context::Context;
use crate::error::{Error, Result};
use crate::fiber::{Disposer, EffectGuard};
use crate::plugin::BoxFuture;

/// A listener receives the dispatching context, the JSON payload, and a
/// `Next` handle (only meaningful in waterfall dispatch).
pub type Listener = Arc<
    dyn Fn(Context, Value, Next) -> BoxFuture<Result<Value>> + Send + Sync,
>;

#[derive(Clone)]
pub(crate) struct Hook {
    pub ctx: Context,
    pub callback: Listener,
    pub prepend: bool,
    /// Global listeners ignore scope filters during dispatch.
    pub global: bool,
}

/// Continuation handle for waterfall composition.
///
/// A listener that never calls `Next::run` vetoes the rest of the chain,
/// including the built-in fallback — exactly like cordis waterfalls.
#[derive(Clone)]
pub struct Next {
    steps: Arc<VecDeque<Listener>>,
    fallback: Option<Arc<dyn Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync>>,
    ctx: Context,
}

impl Next {
    #[allow(dead_code)]
    pub(crate) fn terminal(
        ctx: Context,
        fallback: impl Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync + 'static,
    ) -> Self {
        Self { steps: Arc::new(VecDeque::new()), fallback: Some(Arc::new(fallback)), ctx }
    }

    pub(crate) fn with_steps(
        ctx: Context,
        steps: VecDeque<Listener>,
        fallback: Option<Arc<dyn Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync>>,
    ) -> Self {
        Self { steps: Arc::new(steps), fallback, ctx }
    }

    /// Continue the chain with an updated payload.
    pub async fn run(self, payload: Value) -> Result<Value> {
        let mut steps = (*self.steps).clone();
        match steps.pop_front() {
            Some(listener) => {
                let rest = Next::with_steps(self.ctx.clone(), steps, self.fallback);
                (listener)(self.ctx, payload, rest).await
            }
            None => match &self.fallback {
                Some(fallback) => (fallback)(payload).await,
                None => Ok(payload),
            },
        }
    }
}

/// Whether a value stops serial/bail dispatch: anything but `null`/`false`.
pub fn is_bailed(value: &Value) -> bool {
    !matches!(value, Value::Null | Value::Bool(false))
}

/// Event store shared at the root; hooks are keyed by event name.
#[derive(Default)]
pub(crate) struct EventStore {
    hooks: std::collections::HashMap<String, Vec<Hook>>,
}

impl EventStore {
    pub(crate) fn dispatch(
        &self,
        name: &str,
        filter: Option<&(dyn Fn(&Context) -> bool + Send + Sync)>,
    ) -> Vec<(Context, Listener)> {
        self.hooks
            .get(name)
            .map(|hooks| {
                hooks
                    .iter()
                    .filter(|hook| hook.global || filter.map_or(true, |f| f(&hook.ctx)))
                    .map(|hook| (hook.ctx.clone(), hook.callback.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn register(&mut self, name: &str, hook: Hook) {
        let list = self.hooks.entry(name.to_string()).or_default();
        if hook.prepend {
            list.insert(0, hook);
        } else {
            list.push(hook);
        }
    }

    pub(crate) fn unregister(&mut self, name: &str, callback: &Listener) -> bool {
        if let Some(list) = self.hooks.get_mut(name) {
            if let Some(pos) =
                list.iter().position(|hook| Arc::ptr_eq(&hook.callback, callback))
            {
                list.remove(pos);
                return true;
            }
        }
        false
    }
}

pub(crate) struct EventsService;

impl EventsService {
    /// Register a listener as a fiber effect of `ctx`.
    pub(crate) async fn on(
        ctx: &Context,
        name: &str,
        callback: Listener,
        prepend: bool,
        global: bool,
    ) -> Result<EffectGuard> {
        let label = format!("ctx.on({name})");
        let event_name = name.to_string();
        let ctx_for_effect = ctx.clone();
        let cb_for_effect = callback.clone();
        ctx.effect(label, async move {
            let hook = Hook {
                ctx: ctx_for_effect.clone(),
                callback: cb_for_effect.clone(),
                prepend,
                global,
            };
            ctx_for_effect
                .root
                .events
                .lock()
                .unwrap()
                .register(&event_name, hook);
            let ctx2 = ctx_for_effect.clone();
            let name2 = event_name.clone();
            let disposer: Disposer = Box::new(move || {
                let removed = ctx2.root.events.lock().unwrap().unregister(&name2, &cb_for_effect);
                let _ = removed;
                Box::pin(async move {})
            });
            Ok(Some(disposer))
        })
        .await
    }

    fn listeners(ctx: &Context, name: &str) -> Vec<(Context, Listener)> {
        ctx.root.events.lock().unwrap().dispatch(name, None)
    }

    /// Concurrent dispatch; waits for every listener and aggregates failures
    /// into `Error::Aggregate` (mirrors JS `Promise.allSettled` + `AggregateError`).
    pub async fn parallel(ctx: &Context, name: &str, payload: Value) -> Result<Value> {
        let listeners = Self::listeners(ctx, name);
        if listeners.is_empty() {
            return Ok(payload);
        }
        let mut set = tokio::task::JoinSet::new();
        for (hook_ctx, callback) in listeners {
            let payload = payload.clone();
            let next = Next::with_steps(hook_ctx.clone(), VecDeque::new(), None);
            set.spawn(async move { (callback)(hook_ctx, payload, next).await });
        }
        let mut errors = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => errors.push(err),
                Err(join_err) => errors.push(Error::msg(join_err.to_string())),
            }
        }
        if errors.is_empty() {
            Ok(payload)
        } else {
            Err(Error::aggregate(errors))
        }
    }

    /// Fire-and-forget dispatch: spawn listeners detached, logging failures.
    pub fn emit(ctx: &Context, name: &str, payload: Value) {
        let event_name = name.to_string();
        let listeners = Self::listeners(ctx, name);
        for (hook_ctx, callback) in listeners {
            let payload = payload.clone();
            let next = Next::with_steps(hook_ctx.clone(), VecDeque::new(), None);
            let logger_ctx = ctx.clone();
            let label = event_name.clone();
            tokio::spawn(async move {
                if let Err(err) = (callback)(hook_ctx, payload, next).await {
                    logger_ctx
                        .logger()
                        .log(
                            crate::logger::LogLevel::Warn,
                            format!("[{label}] listener failed: {err}"),
                        );
                }
            });
        }
    }

    /// Scoped variant: only listeners whose context satisfies `filter` fire.
    pub(crate) fn emit_filtered(
        ctx: &Context,
        name: &str,
        payload: Value,
        filter: impl Fn(&Context) -> bool + Send + Sync + 'static,
    ) {
        let event_name = name.to_string();
        let listeners = ctx.root.events.lock().unwrap().dispatch(name, Some(&filter));
        for (hook_ctx, callback) in listeners {
            let payload = payload.clone();
            let next = Next::with_steps(hook_ctx.clone(), VecDeque::new(), None);
            let logger_ctx = ctx.clone();
            let label = event_name.clone();
            tokio::spawn(async move {
                if let Err(err) = (callback)(hook_ctx, payload, next).await {
                    logger_ctx
                        .logger()
                        .log(
                            crate::logger::LogLevel::Warn,
                            format!("[{label}] listener failed: {err}"),
                        );
                }
            });
        }
    }

    /// Run listeners in order until one returns a bail value.
    pub async fn serial(ctx: &Context, name: &str, payload: Value) -> Result<Value> {
        let listeners = Self::listeners(ctx, name);
        // Mirrors cordis: listener arguments are never rewritten by non-bail
        // returns; only a bail value short-circuits and becomes the result.
        let payload = payload;
        for (hook_ctx, callback) in listeners {
            let next = Next::with_steps(hook_ctx.clone(), VecDeque::new(), None);
            let returned = (callback)(hook_ctx, payload.clone(), next).await?;
            if is_bailed(&returned) {
                return Ok(returned);
            }
        }
        Ok(payload)
    }

    /// First-bail-wins dispatch (synchronous-intent alias of `serial`).
    pub async fn bail(ctx: &Context, name: &str, payload: Value) -> Result<Value> {
        Self::serial(ctx, name, payload).await
    }

    /// Compose listeners around a built-in fallback, outermost first.
    ///
    /// The final argument is the innermost default behavior; each listener may
    /// transform the payload before delegating, or veto by never calling `next`.
    pub async fn waterfall<F>(
        ctx: &Context,
        name: &str,
        payload: Value,
        fallback: F,
    ) -> Result<Value>
    where
        F: Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync + 'static,
    {
        let listeners = Self::listeners(ctx, name);
        let fallback: Arc<dyn Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync> =
            Arc::new(fallback);
        let mut steps: VecDeque<Listener> = listeners.into_iter().map(|(_, cb)| cb).collect();
        match steps.pop_front() {
            Some(first) => {
                // Every continuation keeps the same terminal fallback so the
                // built-in behavior runs once the chain is exhausted.
                let rest = Next::with_steps(ctx.clone(), steps, Some(fallback));
                (first)(ctx.clone(), payload, rest).await
            }
            None => {
                let next = Next::with_steps(ctx.clone(), VecDeque::new(), Some(fallback));
                next.run(payload).await
            }
        }
    }
}
