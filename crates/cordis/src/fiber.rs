//! Plugin fiber lifecycle, effects, and cleanup — the reactive heart of cordis.
//!
//! One fiber is one running instance of a plugin application. It tracks its
//! required services (`inject`), an *epoch* fingerprint of the providers that
//! currently satisfy them, and every effect (disposable) created while the
//! plugin body ran. When the provider set changes, the epoch changes and the
//! fiber converges: unload (LIFO disposers) and/or load (run `apply`) again.
//!
//! States mirror cordis exactly:
//! PENDING -> LOADING -> ACTIVE -> UNLOADING -> (PENDING|LOADING|DISPOSED),
//! with FAILED retained when startup errored while inactive.
//!
//! ## Lock ordering
//!
//! Global order (never acquire a later lock while holding an earlier one):
//! `root.reflect` < `root.registry` < `root.fibers` < fiber-internal locks
//! (`uid`, `error`, `loaded`, `driver`, `disposables`, `store`).
//! In particular: `state()` NEVER touches the mutexes — it reads the atomic
//! [`Fiber::state_cache`] mirror, which transition points republish via
//! [`Fiber::cache_state`] (which itself takes the mutexes; call it only when
//! no fiber-internal lock is held). Notifications run in two phases (snapshot
//! under one lock, re-check + refresh outside it) because `std::sync::Mutex`
//! is not re-entrant.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use crate::context::Context;
use crate::error::{CordisCode, CordisError, Error, Result};
use crate::events::EventsService;
use crate::plugin::{BoxFuture, Injection};
use crate::registry::{RegistryService, Runtime};

/// A boxed asynchronous cleanup routine collected as a fiber effect.
pub type Disposer = Box<dyn FnOnce() -> BoxFuture<()> + Send>;

/// Sentinel epoch meaning "required services are not all available".
pub const EPOCH_INACTIVE: &str = "";

/// Lifecycle state of a fiber.
///
/// Kept as one `repr(u8)` because the converged state is mirrored into an
/// atomic cache ([`Fiber::state_cache`]): the hot path (`fiber_is_active`
/// during every typed service read) must observe it WITHOUT taking the
/// lifecycle mutexes, while each transition point re-derives and republishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FiberState {
    /// Registered but waiting for required services.
    Pending = 0,
    /// Running the plugin body right now.
    Loading = 1,
    /// The plugin body completed and all effects are live.
    Active = 2,
    /// Startup failed; the fiber waits for dependency changes to retry.
    Failed = 3,
    /// Draining effects (LIFO) after deactivation.
    Unloading = 4,
    /// Fully disposed; uid cleared.
    Disposed = 5,
}

impl FiberState {
    pub fn as_str(self) -> &'static str {
        match self {
            FiberState::Pending => "pending",
            FiberState::Loading => "loading",
            FiberState::Active => "active",
            FiberState::Failed => "failed",
            FiberState::Unloading => "unloading",
            FiberState::Disposed => "disposed",
        }
    }

    /// Decode the atomic cache value. Unknown bytes are treated as
    /// [`FiberState::Pending`] by the caller.
    pub(crate) fn from_repr(value: u8) -> Option<FiberState> {
        Some(match value {
            0 => FiberState::Pending,
            1 => FiberState::Loading,
            2 => FiberState::Active,
            3 => FiberState::Failed,
            4 => FiberState::Unloading,
            5 => FiberState::Disposed,
            _ => return None,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ProviderSnapshot {
    pub provider_uid: u64,
    /// Cached value; meaningless for accessor (computed) services.
    pub value: Arc<dyn std::any::Any + Send + Sync>,
    /// Accessor-style entries are recomputed on every read.
    pub dynamic: bool,
}

struct DriveState {
    desired: String,
    dirty: bool,
    /// Set by `restart`: guarantees a full unload->load round trip even when
    /// the recomputed epoch equals the currently loaded one.
    force: bool,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// Runtime instance of one plugin application.
pub struct Fiber {
    /// Unique id within the registry; 0 for the root fiber, `None` once disposed.
    pub uid: Mutex<Option<u64>>,
    pub(crate) runtime: Option<Arc<Runtime>>,
    pub(crate) inject: Vec<Injection>,
    /// The context this fiber's plugin runs in; bound post-construction.
    ctx_slot: OnceLock<Context>,

    pub(crate) raw_config: Mutex<Value>,
    pub(crate) config: Mutex<Value>,
    pub(crate) error: Mutex<Option<Error>>,

    /// Snapshots of required-service implementations while loaded.
    pub(crate) store: Mutex<HashMap<String, ProviderSnapshot>>,
    loaded: Mutex<String>,
    /// Live effects: (id, label, disposer); None slots are drained guards.
    disposables: Mutex<Vec<Option<(u64, String, Disposer)>>>,
    next_effect_id: AtomicU64,
    driver: Mutex<DriveState>,
    settle: (tokio::sync::watch::Sender<u64>, tokio::sync::watch::Receiver<u64>),
    /// Atomic mirror of the derived [`FiberState`]: lock-free reads on the hot
    /// path ([`Fiber::state`]); republished at every transition point via
    /// [`Fiber::cache_state`].
    state_cache: AtomicU8,
}

impl Fiber {
    /// Create the root fiber: uid 0, always active, no runtime.
    pub(crate) fn new_root() -> Arc<Fiber> {
        Arc::new(Self::blank(Some(0), None, Vec::new(), Value::Null, ":root".to_string()))
    }

    /// Create an unbound child fiber; call `bind` right after `Arc::new`.
    pub(crate) fn new_child(
        runtime: Arc<Runtime>,
        inject: Vec<Injection>,
        config: Value,
        uid: u64,
    ) -> Arc<Fiber> {
        Arc::new(Self::blank(Some(uid), Some(runtime), inject, config, EPOCH_INACTIVE.to_string()))
    }

    fn blank(
        uid: Option<u64>,
        runtime: Option<Arc<Runtime>>,
        inject: Vec<Injection>,
        raw_config: Value,
        epoch: String,
    ) -> Self {
        let (settle_tx, settle_rx) = tokio::sync::watch::channel(0);
        Self {
            uid: Mutex::new(uid),
            runtime,
            inject,
            ctx_slot: OnceLock::new(),
            raw_config: Mutex::new(raw_config),
            config: Mutex::new(Value::Null),
            error: Mutex::new(None),
            store: Mutex::new(HashMap::new()),
            loaded: Mutex::new(epoch.clone()),
            disposables: Mutex::new(Vec::new()),
            next_effect_id: AtomicU64::new(1),
            driver: Mutex::new(DriveState {
                desired: epoch,
                dirty: false,
                force: false,
                task: None,
            }),
            settle: (settle_tx, settle_rx),
            state_cache: AtomicU8::new(FiberState::Pending as u8),
        }
    }

    /// Bind the owning context exactly once, right after `Arc::new`.
    pub(crate) fn bind(self: &Arc<Self>, base: &Context) {
        let _ = self.ctx_slot.set(Context {
            root: base.root.clone(),
            scope: base.scope.clone(),
            fiber: Arc::downgrade(self),
        });
    }

    #[inline]
    pub(crate) fn ctx(&self) -> &Context {
        self.ctx_slot.get().expect("fiber context bound at creation")
    }

    fn bump_settle(&self) {
        let (tx, _) = &self.settle;
        tx.send_modify(|v| *v += 1);
    }

    /// Current lifecycle state: a lock-free read of the atomic mirror.
    ///
    /// The mirror is republished at every transition point ([`Fiber::cache_state`]),
    /// so the hot path never takes the lifecycle mutexes (which themselves
    /// would deadlock when called from within a locked driver loop elsewhere).
    pub fn state(&self) -> FiberState {
        FiberState::from_repr(self.state_cache.load(Ordering::Acquire))
            .unwrap_or(FiberState::Pending)
    }

    /// Derive the converged lifecycle state from the live flags (mirroring
    /// cordis `_getState`). Reads the lifecycle mutexes; only transition
    /// points and tests call this.
    fn compute_state(&self) -> FiberState {
        if self.uid.lock().unwrap().is_none() {
            return FiberState::Disposed;
        }
        let error = self.error.lock().unwrap().is_some();
        let loaded = self.loaded.lock().unwrap().clone();
        let driver = self.driver.lock().unwrap();
        let desired = driver.desired.clone();
        // Converged as soon as the flags agree; the spawned driver task may
        // still be unwinding, which must not mask an already-live plugin.
        let busy = driver.dirty || loaded != desired;
        drop(driver);
        if error && loaded == EPOCH_INACTIVE && !busy {
            return FiberState::Failed;
        }
        if busy {
            return if desired == EPOCH_INACTIVE { FiberState::Unloading } else { FiberState::Loading };
        }
        if loaded != EPOCH_INACTIVE {
            return FiberState::Active;
        }
        FiberState::Pending
    }

    /// Republish the derived state into the atomic mirror. Called at every
    /// transition point (request/load/unload/fail/dispose) BEFORE any
    /// observability event, so `state()` and `internal/status` agree.
    fn cache_state(&self) -> FiberState {
        let state = self.compute_state();
        self.state_cache.store(state as u8, Ordering::Release);
        // Maintain the root's lock-free ACTIVE bitmask (bit `uid` set exactly
        // while ACTIVE) — the fast path of `RootState::fiber_is_active`.
        let uid = *self.uid.lock().unwrap();
        if let Some(uid) = uid {
            if uid < 64 {
                let root = &self.ctx().root;
                let bit = 1u64 << (uid as u32);
                if state == FiberState::Active {
                    root.active_fibers.fetch_or(bit, Ordering::SeqCst);
                } else {
                    root.active_fibers.fetch_and(!bit, Ordering::SeqCst);
                }
            }
        }
        state
    }

    /// Emit internal/status for observability plugins (inventory/HMR style).
    fn publish_state(&self) {
        let payload = serde_json::json!({
            "fiber": self.name(),
            "uid": *self.uid.lock().unwrap(),
            "state": self.state().as_str(),
        });
        EventsService::emit(self.ctx(), "internal/status", payload);
    }

    /// The plugin display name; the runtime's declared name, else `root`.
    pub fn name(&self) -> String {
        match &self.runtime {
            Some(runtime) => runtime.name.clone(),
            None => "root".to_string(),
        }
    }

    /// Throw if the fiber has already been disposed.
    pub fn assert_active(&self) -> Result<()> {
        if self.uid.lock().unwrap().is_some() {
            Ok(())
        } else {
            Err(CordisError::new(CordisCode::InactiveEffect).into())
        }
    }

    // ------------------------------------------------------------------
    // Effects
    // ------------------------------------------------------------------

    /// Register an effect owned by this fiber: run `setup`, collect its
    /// disposer, and return a guard for eager single-shot disposal.
    ///
    /// Creating an effect on an inactive/unloading fiber fails with
    /// `CordisCode::InactiveEffect`, mirroring cordis.
    pub(crate) async fn effect(
        self: &Arc<Self>,
        label: impl Into<String>,
        setup: impl std::future::Future<Output = Result<Option<Disposer>>> + Send + 'static,
    ) -> Result<EffectGuard> {
        self.assert_active()?;
        if matches!(self.state(), FiberState::Unloading | FiberState::Disposed) {
            return Err(CordisError::new(CordisCode::InactiveEffect).into());
        }
        let effect_label = label.into();
        let disposer = setup.await?;
        let id = self.next_effect_id.fetch_add(1, Ordering::SeqCst);
        if let Some(disposer) = disposer {
            let mut list = self.disposables.lock().unwrap();
            list.push(Some((id, effect_label, disposer)));
        }
        Ok(EffectGuard { fiber: Arc::downgrade(self), id })
    }

    /// Run one pending disposer by id, if still live; errors never propagate.
    async fn run_disposer(self: &Arc<Self>, id: u64) {
        let taken = {
            let mut list = self.disposables.lock().unwrap();
            let slot = list
                .iter_mut()
                .find(|slot| matches!(slot, Some((entry_id, _, _)) if *entry_id == id));
            slot.and_then(|slot| slot.take()).map(|(_, _, d)| d)
        };
        if let Some(disposer) = taken {
            // Disposers cannot fail by contract; cleanup errors would only be
            // observable, never actionable, matching cordis' logger swallow.
            (disposer)().await;
            self.bump_settle();
        }
    }

    async fn log_error(self: &Arc<Self>, err: Error) {
        let name = self.name();
        self.ctx().logger().log_event(
            crate::logger::LogLevel::Error,
            "fiber".to_string(),
            err.code().map(str::to_string),
            format!("{name}: {err}"),
        );
    }

    /// Drain every live effect LIFO, sequentially. Entries are taken out of
    /// the list and invoked DIRECTLY (guards calling run_disposer afterwards
    /// find nothing left, which is a no-op).
    async fn unload_effects(self: &Arc<Self>) {
        let disposers: Vec<Disposer> = {
            let mut list = self.disposables.lock().unwrap();
            let mut disposers = Vec::new();
            while let Some(slot) = list.pop() {
                if let Some((_, _, disposer)) = slot {
                    disposers.push(disposer);
                }
            }
            disposers
        };
        // pop() yields reverse insertion order already: exactly LIFO.
        for disposer in disposers {
            disposer().await;
            self.bump_settle();
        }
    }

    // ------------------------------------------------------------------
    // Reactive convergence
    // ------------------------------------------------------------------

    /// Recompute the desired epoch from current provider snapshots and
    /// request convergence. Mirrors cordis `_refresh`.
    pub(crate) fn refresh(self: &Arc<Self>) {

        // Epoch grammar: ":" marks an activatable fiber; each satisfied
        // dependency appends ":<provider-uid>". Any missing dependency
        // collapses the whole epoch to EPOCH_INACTIVE (empty string).
        let store = self.store.lock().unwrap();
        let mut epoch = String::from(":");
        for injection in &self.inject {
            match store.get(injection.service.as_ref()) {
                Some(snapshot) => {
                    epoch.push(':');
                    epoch.push_str(&snapshot.provider_uid.to_string());
                }
                None => {
                    epoch = EPOCH_INACTIVE.to_string();
                    break;
                }
            }
        }
        drop(store);
        self.request_epoch(epoch);
    }

    /// Update one dependency snapshot after a provide/withdraw notification.
    pub(crate) fn check_impl(self: &Arc<Self>, name: &str) -> bool {

        let resolved = self.ctx().resolve_impl(self, name);
        let mut store = self.store.lock().unwrap();
        match resolved {
            Some(snapshot) => {
                store.insert(name.to_string(), snapshot);
                true
            }
            None => {
                store.remove(name);
                false
            }
        }
    }

    fn request_epoch(self: &Arc<Self>, desired: String) {
        {
            let mut driver = self.driver.lock().unwrap();
            if driver.desired == desired && !driver.dirty {
                return;
            }
            driver.desired = desired;
            driver.dirty = true;
            if driver.task.as_ref().map_or(true, |t| t.is_finished()) {
                driver.task = Some(tokio::spawn(Self::drive_loop(self.clone())));
            }
        }
        
        self.cache_state();
        self.bump_settle();
        self.publish_state();
    }

    async fn drive_loop(self: Arc<Self>) {
        loop {
            let (target, force) = {
                let mut driver = self.driver.lock().unwrap();
                driver.dirty = false;
                (driver.desired.clone(), driver.force)
            };
            let loaded = self.loaded.lock().unwrap().clone();
            if target == loaded && !force {
                let mut driver = self.driver.lock().unwrap();
                if !driver.dirty {

                    driver.task = None;
                    drop(driver);
                    self.cache_state();
                    self.bump_settle();
                    self.publish_state();
                    return;
                }

                continue;
            }
            if loaded != EPOCH_INACTIVE {
                self.unload_once().await;
                if force {
                    // Round trip requested: after unloading, fall through to
                    // reload with whatever epoch is desired now.
                    let mut driver = self.driver.lock().unwrap();
                    driver.force = false;
                }
                continue;
            }
            if target == EPOCH_INACTIVE {
                // Nothing to load; converge as Pending.
                let mut driver = self.driver.lock().unwrap();
                driver.force = false;
                drop(driver);
                self.cache_state();
                continue;
            }
            self.load_once(target).await;
        }
    }

    async fn unload_once(self: &Arc<Self>) {
        self.publish_state();
        self.unload_effects().await;
        self.store.lock().unwrap().clear();
        *self.loaded.lock().unwrap() = EPOCH_INACTIVE.to_string();
        
        self.cache_state();
        self.bump_settle();
        self.publish_state();
    }

    async fn load_once(self: &Arc<Self>, target: String) {

        self.publish_state();
        let runtime = match &self.runtime {
            Some(runtime) => runtime.clone(),
            None => {
                *self.loaded.lock().unwrap() = target;
                return;
            }
        };

        // Resolve config: internal/config waterfall, then schema validation.
        let raw = self.raw_config.lock().unwrap().clone();
        let resolved = match EventsService::waterfall(
            self.ctx(),
            "internal/config",
            raw,
            |value| Box::pin(async move { Ok(value) }) as BoxFuture<Result<Value>>,
        )
        .await
        {
            Ok(value) => runtime.plugin.resolve_config(&value),
            Err(err) => Err(err),
        };
        let config = match resolved {
            Ok(value) => value,
            Err(err) => return self.fail_load(err).await,
        };
        *self.config.lock().unwrap() = config.clone();

        match runtime.plugin.apply(self.ctx().clone(), config).await {
            Ok(()) => {
                self.error.lock().unwrap().take();
                *self.loaded.lock().unwrap() = target;
                // Republish the state BEFORE notifying dependents: their
                // `resolve_impl`/`check_impl` consult `fiber_is_active`, and
                // that reads the atomic state mirror. With an eager derived
                // state this was implicit; with the cache it must be explicit
                // here or dependents never see this fiber as Active.
                self.cache_state();
                // Re-broadcast this fiber's provided services now that it is
                // ACTIVE: dependents whose first notification arrived while we
                // were still LOADING re-check here (cordis does the same on
                // the ACTIVE transition of the providing fiber).
                crate::service::notify(self.ctx(), &self.provided_names());

                self.bump_settle();
                self.publish_state();

            }
            Err(err) => {
                // Roll back anything the failed body managed to collect.
                self.unload_effects().await;
                self.fail_load(err).await;
            }
        }
    }

    /// Record startup failure and park the driver until dependencies change.
    async fn fail_load(self: &Arc<Self>, err: Error) {
        self.log_error(err.clone()).await;
        *self.error.lock().unwrap() = Some(err);
        *self.loaded.lock().unwrap() = EPOCH_INACTIVE.to_string();
        // Park: mirror cordis' epoch=INACTIVE so we do not spin-retry; the
        // next provide/withdraw notification re-arms convergence via refresh.
        let mut driver = self.driver.lock().unwrap();
        driver.desired = EPOCH_INACTIVE.to_string();
        drop(driver);
        
        self.cache_state();
        self.bump_settle();
        self.publish_state();
    }

    // ------------------------------------------------------------------
    // Public controls
    // ------------------------------------------------------------------

    /// Wait until the fiber settles into a stable state; surface startup errors.
    pub async fn join(&self) -> Result<()> {

        let mut rx = self.settle.1.clone();
        loop {
            let busy = {
                let driver = self.driver.lock().unwrap();
                driver.dirty
                    || driver.task.as_ref().map_or(false, |t| !t.is_finished())
                    || *self.loaded.lock().unwrap() != driver.desired
            };
            if !busy {
                break;
            }
            // borrow_and_update registers our snapshot; changed() awaits the
            // next bump, so no wakeups can be lost between check and await.
            let _ = rx.borrow_and_update().clone();
            if rx.changed().await.is_err() {
                break;
            }
        }
        match self.error.lock().unwrap().as_ref() {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    /// Like [`Fiber::join`], but gives up after `timeout` and returns a
    /// timeout error while the driver keeps converging in the background.
    ///
    /// A plugin whose `apply` never completes used to wedge `join` (and with
    /// it `FiberHandle::dispose`) forever; this bounds the wait so callers can
    /// decide whether to keep waiting, restart, or dispose.
    pub async fn join_with_timeout(&self, timeout: std::time::Duration) -> Result<()> {
        let mut rx = self.settle.1.clone();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let busy = {
                let driver = self.driver.lock().unwrap();
                driver.dirty
                    || driver.task.as_ref().map_or(false, |t| !t.is_finished())
                    || *self.loaded.lock().unwrap() != driver.desired
            };
            if !busy {
                break;
            }
            let _ = rx.borrow_and_update().clone();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::msg(format!("join(\"{}\") timed out", self.name())));
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return Err(Error::msg(format!("join(\"{}\") timed out", self.name())));
            }
        }
        match self.error.lock().unwrap().as_ref() {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }

    /// Dispose and immediately reload this plugin with its current config.
    pub async fn restart(self: &Arc<Self>) -> Result<()> {
        self.assert_active()?;
        self.error.lock().unwrap().take();
        {
            let mut driver = self.driver.lock().unwrap();
            driver.force = true;
            driver.dirty = true;
            if driver.task.as_ref().map_or(true, |t| t.is_finished()) {
                driver.task = Some(tokio::spawn(Self::drive_loop(self.clone())));
            }
        }
        self.bump_settle();
        self.refresh();
        self.join().await
    }

    /// Validate and apply new config, then restart the plugin.
    ///
    /// Runs the `internal/update` waterfall first so update hooks can veto or
    /// transform the change (the HMR extension point): a hook that never
    /// calls `next` vetoes the update entirely.
    pub async fn update(self: &Arc<Self>, config: Value) -> Result<Value> {
        self.assert_active()?;
        *self.raw_config.lock().unwrap() = config.clone();
        let fiber = self.clone();
        EventsService::waterfall(
            self.ctx(),
            "internal/update",
            config,
            move |_config| {
                let fiber = fiber.clone();
                Box::pin(async move {
                    fiber.restart().await?;
                    Ok(Value::Null)
                }) as BoxFuture<Result<Value>>
            },
        )
        .await?;
        Ok(self.config.lock().unwrap().clone())
    }

    /// Dispose this fiber: unload the plugin, then settle once cleanup finished.
    pub async fn dispose(self: &Arc<Self>) {
        let old_uid = { *self.uid.lock().unwrap() };
        if old_uid.is_none() {
            return;
        }
        // Clear the ACTIVE bit before wiping the uid: cache_state can no
        // longer identify this fiber afterwards, so a stale bit would make
        // fiber_is_active answer "active" for a disposed provider.
        if let Some(uid) = old_uid {
            if uid != 0 && uid < 64 {
                self.ctx()
                    .root
                    .active_fibers
                    .fetch_and(!(1u64 << (uid as u32)), Ordering::SeqCst);
            }
        }
        *self.uid.lock().unwrap() = None;
        self.cache_state();
        if old_uid != Some(0) {
            RegistryService::remove_fiber(&self.ctx().root, self);
            self.ctx().root.unregister_fiber(old_uid.unwrap());
        }
        EventsService::emit(
            self.ctx(),
            "internal/plugin",
            serde_json::json!({ "event": "disposed", "fiber": self.name(), "uid": old_uid }),
        );
        self.request_epoch(EPOCH_INACTIVE.to_string());
        let _ = self.join().await;
        self.publish_state();
    }

    /// Names of services currently registered by this fiber.
    fn provided_names(&self) -> Vec<String> {
        let uid = match *self.uid.lock().unwrap() {
            Some(uid) => uid,
            None => return Vec::new(),
        };
        let store = self.ctx().root.reflect.lock().unwrap();
        let mut names = Vec::new();
        for (name, map) in store.labels.iter() {
            if map.values().any(|impl_| impl_.provider_uid == uid) {
                names.push(name.clone());
            }
        }
        names
    }

    /// Snapshot of live effects for diagnostics.
    pub fn effect_count(&self) -> usize {
        self.disposables
            .lock()
            .unwrap()
            .iter()
            .filter(|slot| slot.is_some())
            .count()
    }

    /// Labels of every live effect, registration order. The Rust analogue of
    /// the cordis getEffects() introspection (metadata only).
    pub fn effect_metas(&self) -> Vec<String> {
        self.disposables
            .lock()
            .unwrap()
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_, label, _)| label.clone()))
            .collect()
    }
}

/// Handle returned by `ctx.plugin()`: a shared pointer plus controls.
///
/// Cloning is cheap; dropping it does not dispose the fiber — call
/// `dispose`/`Context::stop` explicitly, mirroring cordis.
#[derive(Clone)]
pub struct FiberHandle(pub(crate) Arc<Fiber>);

impl FiberHandle {
    pub fn uid(&self) -> Option<u64> {
        *self.0.uid.lock().unwrap()
    }

    pub fn name(&self) -> String {
        self.0.name()
    }

    pub fn state(&self) -> FiberState {
        self.0.state()
    }

    pub fn effect_count(&self) -> usize {
        self.0.effect_count()
    }

    /// Labels of the live effects owned by this plugin fiber.
    pub fn effect_metas(&self) -> Vec<String> {
        self.0.effect_metas()
    }

    /// Wait for the current lifecycle work and rethrow startup errors.
    pub async fn join(&self) -> Result<()> {

        self.0.join().await
    }

    /// Like [`FiberHandle::join`], bounded by `timeout` (see [`Fiber::join_with_timeout`]).
    pub async fn join_with_timeout(&self, timeout: std::time::Duration) -> Result<()> {
        self.0.join_with_timeout(timeout).await
    }

    /// Dispose and reload with the current config.
    pub async fn restart(&self) -> Result<()> {
        self.0.restart().await
    }

    /// Validate and apply new config, then restart (HMR-style hot update).
    pub async fn update(&self, config: Value) -> Result<Value> {
        self.0.update(config).await
    }

    /// Unload the plugin and settle after cleanup finished.
    pub async fn dispose(&self) {
        self.0.dispose().await
    }
}

/// Guard for eager disposal of one effect; disposing is single-shot.
#[derive(Clone)]
pub struct EffectGuard {
    fiber: std::sync::Weak<Fiber>,
    id: u64,
}

impl EffectGuard {
    /// Dispose eagerly. Idempotent: a second call finds nothing to run.
    pub async fn dispose(&self) -> Result<()> {
        let Some(fiber) = self.fiber.upgrade() else {
            return Ok(());
        };
        fiber.run_disposer(self.id).await;
        Ok(())
    }

    pub fn is_live(&self) -> bool {
        self.fiber.upgrade().map_or(false, |fiber| {
            fiber
                .disposables
                .lock()
                .unwrap()
                .iter()
                .any(|slot| matches!(slot, Some((id, _, _)) if *id == self.id))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::plugin;
    use crate::Plugin;

    #[tokio::test]
    async fn active_bitmask_tracks_lifecycle_locklessly() {
        let ctx = Context::new();
        // An immediately-completing plugin: uid is allocated sequentially (>=1).
        let ready = plugin("ready", |_ctx: Context, _c: serde_json::Value| async move { Ok(()) });
        let fiber = ctx.plugin(ready as Arc<dyn Plugin>, None);
        let uid = fiber.uid().expect("allocated");

        // Not yet converged: bit clear, locked path reports pending.
        assert_eq!(
            ctx.root.active_fibers.load(Ordering::SeqCst) & (1u64 << (uid as u32)),
            0,
            "bit must be clear while loading"
        );

        fiber.join().await.unwrap();
        assert_ne!(
            ctx.root.active_fibers.load(Ordering::SeqCst) & (1u64 << (uid as u32)),
            0,
            "bit must be set while Active"
        );
        // The lock-free fast path agrees with the locked look-up.
        assert!(ctx.root.fiber_is_active(uid));

        fiber.dispose().await;
        assert_eq!(
            ctx.root.active_fibers.load(Ordering::SeqCst) & (1u64 << (uid as u32)),
            0,
            "bit must be cleared after dispose (no stale provider liveness)"
        );
        assert!(!ctx.root.fiber_is_active(uid));
    }
}
