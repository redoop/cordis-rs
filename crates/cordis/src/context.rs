//! Root and child dependency containers for plugins — the `Context` port.
//!
//! A `Context` is a cheap-clonable handle: all roots share one `RootState`
//! (registry, service store, event bus), while each handle carries a *scope
//! chain* for isolation/interception and a weak link to its owning fiber.
//! `extend()`, `isolate()`, and `intercept()` derive child contexts without
//! mutating the parent, exactly like cordis.
//!
//! One documented deviation from JS cordis: lifecycle mutations (`provide`,
//! `on`) are `async` because effects run on the async runtime; reads stay
//! synchronous like the proxy path.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::Value;

use crate::error::{Error, Result};
use crate::events::EventsService;
use crate::fiber::{EffectGuard, Fiber, FiberHandle};
use crate::logger::{DefaultLogger, Logger, LoggerHandle};
use crate::plugin::{BoxFuture, Injection, IntoInjections, Plugin};
use crate::registry::RegistryService;
use crate::service as service_layer;
use crate::service::{merged_intercept, resolve_impl_for};

/// Isolation label of the default (root) scope.
pub(crate) const DEFAULT_LABEL: u64 = 0;

/// Immutable scope node forming a persistent linked list to the root.
pub(crate) struct ScopeNode {
    pub parent: Option<Arc<ScopeNode>>,
    /// Service name -> isolation label overrides introduced by this node.
    pub isolate: HashMap<String, u64>,
    /// Service name -> intercept config layers (root-most first).
    pub intercept: HashMap<String, VecDeque<Value>>,
}

impl ScopeNode {
    pub fn root() -> Arc<Self> {
        Arc::new(Self { parent: None, isolate: HashMap::new(), intercept: HashMap::new() })
    }
}

/// State shared by every context derived from one root.
pub struct RootState {
    pub(crate) registry: Mutex<crate::registry::RegistryMap>,
    pub(crate) reflect: Mutex<crate::service::ReflectStore>,
    pub(crate) events: Mutex<crate::events::EventStore>,
    /// Every live fiber by uid — powers notifications and provider checks.
    pub(crate) fibers: Mutex<HashMap<u64, Weak<Fiber>>>,
    /// The immortal root fiber (uid 0); owns built-in listeners/effects.
    pub(crate) root_fiber: std::sync::OnceLock<Arc<Fiber>>,
    pub(crate) counter: AtomicU64,
    label_seed: AtomicU64,
}

impl RootState {
    pub(crate) fn next_uid(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub(crate) fn fresh_label(&self) -> u64 {
        self.label_seed.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// The root fiber (uid 0) is always considered active; it owns built-ins.
    pub(crate) fn fiber_is_active(&self, uid: u64) -> bool {
        if uid == 0 {
            return true;
        }
        self.fibers
            .lock()
            .unwrap()
            .get(&uid)
            .and_then(|weak| weak.upgrade())
            .map_or(false, |fiber| fiber.state() == crate::fiber::FiberState::Active)
    }

    pub(crate) fn register_fiber(&self, fiber: &Arc<Fiber>) {
        let uid = match *fiber.uid.lock().unwrap() {
            Some(uid) => uid,
            None => return,
        };
        self.fibers.lock().unwrap().insert(uid, Arc::downgrade(fiber));
    }

    pub(crate) fn unregister_fiber(&self, uid: u64) {
        if uid != 0 {
            self.fibers.lock().unwrap().remove(&uid);
        }
    }
}

/// Dependency container handle; clone freely.
#[derive(Clone)]
pub struct Context {
    pub(crate) root: Arc<RootState>,
    pub(crate) scope: Arc<ScopeNode>,
    pub(crate) fiber: Weak<Fiber>,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// Create the root context and install built-in services.
    ///
    /// Mirrors the cordis constructor: the logger service is provided by the
    /// root fiber so every plugin can log before providing anything itself.
    pub fn new() -> Context {
        let root = Arc::new(RootState {
            registry: Mutex::new(HashMap::new()),
            reflect: Mutex::new(crate::service::ReflectStore::default()),
            events: Mutex::new(crate::events::EventStore::default()),
            fibers: Mutex::new(HashMap::new()),
            root_fiber: std::sync::OnceLock::new(),
            counter: AtomicU64::new(0),
            label_seed: AtomicU64::new(1),
        });
        let ctx = Context { root: root.clone(), scope: ScopeNode::root(), fiber: Weak::new() };

        // Root fiber: uid 0, always active; owns built-in effects/listeners.
        let root_fiber = Fiber::new_root();
        root_fiber.bind(&ctx);
        root.root_fiber.set(root_fiber.clone()).ok();
        root.register_fiber(&root_fiber);

        // Seed the built-in logger directly into the store (owned by root).
        let logger_value =
            Arc::new(LoggerHandle(Arc::new(DefaultLogger::new()) as Arc<dyn Logger>));
        let mut label_map = HashMap::new();
        label_map.insert(
            DEFAULT_LABEL,
            crate::service::ServiceImpl {
                name: "logger".to_string(),
                value: logger_value,
                provider_uid: 0,
                check: None,
                compute: None,
            },
        );
        ctx.root.reflect.lock().unwrap().labels.insert("logger".to_string(), label_map);

        Context { root, scope: ctx.scope, fiber: Arc::downgrade(&root_fiber) }
    }

    // ------------------------------------------------------------------
    // Scoping
    // ------------------------------------------------------------------

    /// Derive a child context bound to the same fiber and scope.
    pub fn extend(&self) -> Context {
        Context { root: self.root.clone(), scope: self.scope.clone(), fiber: self.fiber.clone() }
    }

    /// Create a child context where `name` resolves against an independent
    /// scope. Deriving with the same `label` twice joins both scopes.
    pub fn isolate_with_label(&self, name: &str, label: u64) -> Context {
        let mut isolate = HashMap::new();
        isolate.insert(name.to_string(), label);
        Context {
            root: self.root.clone(),
            scope: Arc::new(ScopeNode {
                parent: Some(self.scope.clone()),
                isolate,
                intercept: HashMap::new(),
            }),
            fiber: self.fiber.clone(),
        }
    }

    /// `isolate()` with a freshly allocated label.
    pub fn isolate(&self, name: &str) -> Context {
        let label = self.root.fresh_label();
        self.isolate_with_label(name, label)
    }

    /// Add an intercept-config layer for `name`; merged outermost-first.
    pub fn intercept(&self, name: &str, config: Value) -> Context {
        let mut intercept = HashMap::new();
        intercept.insert(name.to_string(), VecDeque::from(vec![config]));
        Context {
            root: self.root.clone(),
            scope: Arc::new(ScopeNode {
                parent: Some(self.scope.clone()),
                isolate: HashMap::new(),
                intercept,
            }),
            fiber: self.fiber.clone(),
        }
    }

    /// Merged intercept config visible from this context.
    pub fn service_config(&self, name: &str) -> Value {
        merged_intercept(&self.scope, name)
    }

    pub(crate) fn effective_label(&self, name: &str) -> u64 {
        let mut cursor: Option<&ScopeNode> = Some(&self.scope);
        while let Some(node) = cursor {
            if let Some(label) = node.isolate.get(name) {
                return *label;
            }
            cursor = node.parent.as_deref();
        }
        DEFAULT_LABEL
    }

    // ------------------------------------------------------------------
    // Fiber access
    // ------------------------------------------------------------------

    pub(crate) fn fiber(&self) -> Option<Arc<Fiber>> {
        self.fiber.upgrade()
    }

    pub(crate) fn require_fiber(&self, op: &str) -> Result<Arc<Fiber>> {
        self.fiber()
            .ok_or_else(|| Error::msg(format!("cannot {op} on this context: no active fiber is bound")))
    }

    /// Resolve a required-service implementation snapshot for a fiber.
    pub(crate) fn resolve_impl(
        &self,
        dependent: &Arc<Fiber>,
        name: &str,
    ) -> Option<crate::fiber::ProviderSnapshot> {
        resolve_impl_for(self, dependent, name)
    }

    // ------------------------------------------------------------------
    // Services
    // ------------------------------------------------------------------

    /// Register a service owned by the current fiber's lifetime.
    pub async fn provide<V>(&self, name: &str, value: V) -> Result<EffectGuard>
    where
        V: Any + Send + Sync,
    {
        service_layer::provide(self, name, Arc::new(value), None).await
    }

    /// Like `provide` with an availability predicate consulted by dependents.
    pub async fn provide_with_check<V>(
        &self,
        name: &str,
        value: V,
        check: impl Fn(&Context) -> bool + Send + Sync + 'static,
    ) -> Result<EffectGuard>
    where
        V: Any + Send + Sync,
    {
        service_layer::provide(self, name, Arc::new(value), Some(Arc::new(check))).await
    }

    /// Typed service read; prefers this fiber's injected snapshot, then the
    /// scoped store (provider must be active). Mirrors proxy resolution.
    ///
    /// Accessor (computed) entries are re-evaluated against THIS context on
    /// every read; their snapshots carry only liveness metadata.
    pub fn get<V: Any + Send + Sync>(&self, name: &str) -> Option<Arc<V>> {
        if let Some(fiber) = self.fiber() {
            let snapshot = fiber.store.lock().unwrap().get(name).cloned();
            if let Some(snapshot) = snapshot {
                if !snapshot.dynamic {
                    return snapshot.value.downcast::<V>().ok();
                }
                // fall through to the live computation below
            } else {
                // No dependency snapshot (root context or non-injected read).
            }
        }
        let label = self.effective_label(name);
        // The compute hook may read OTHER services, which re-locks reflect:
        // clone it out and invoke strictly after the guard is gone.
        let compute = {
            let store = self.root.reflect.lock().unwrap();
            let impl_ = store.get_impl(&self.root, name, label, true)?;
            match &impl_.compute {
                Some(compute) => Some(compute.clone()),
                None => {
                    let value = impl_.value.clone();
                    return value.downcast::<V>().ok();
                }
            }
        };
        let computed = compute?(self)?;
        computed.downcast::<V>().ok()
    }

    /// Scope identity: two handles refer to the same isolation scope.
    pub fn is(&self, other: &Context) -> bool {
        Arc::ptr_eq(&self.scope, &other.scope) && Arc::ptr_eq(&self.root, &other.root)
    }

    /// Register a computed service owned by the calling fiber.
    ///
    /// Every subsequent read of `name` invokes `compute` with the reading
    /// context, enabling derived services that track their sources without
    /// caching. Lifecycle mirrors provide(): withdrawal notifies dependents.
    pub async fn accessor<V, F>(&self, name: &str, compute: F) -> Result<crate::fiber::EffectGuard>
    where
        V: Any + Send + Sync,
        F: Fn(&Context) -> Option<V> + Send + Sync + 'static,
    {
        service_layer::accessor(self, name, Arc::new(move |ctx: &Context| {
            compute(ctx).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
        }))
        .await
    }

    /// Typed read or a descriptive error, mirroring proxy-miss diagnostics.
    pub fn require<V: Any + Send + Sync>(&self, name: &str) -> Result<Arc<V>> {
        self.get::<V>(name)
            .ok_or_else(|| Error::msg(format!("cannot get property \"{name}\" without inject")))
    }

    /// Overwrite the value of a service this fiber provided.
    pub fn set<V: Any + Send + Sync>(&self, name: &str, value: V) -> Result<()> {
        service_layer::set(self, name, Arc::new(value))
    }

    /// The logger service (built-in unless replaced in an isolated scope).
    pub fn logger(&self) -> Arc<dyn Logger> {
        match self.get::<LoggerHandle>("logger") {
            Some(handle) => handle.0.clone(),
            None => Arc::new(DefaultLogger::new()),
        }
    }

    // ------------------------------------------------------------------
    // Effects
    // ------------------------------------------------------------------

    /// Register a fiber-owned effect: run `setup`, collect its disposer.
    pub async fn effect<F>(&self, label: impl Into<String>, setup: F) -> Result<EffectGuard>
    where
        F: std::future::Future<Output = Result<Option<crate::fiber::Disposer>>> + Send + 'static,
    {
        let fiber = self.require_fiber("effect")?;
        fiber.effect(label, setup).await
    }

    // ------------------------------------------------------------------
    // Plugins
    // ------------------------------------------------------------------

    /// Start a plugin in the current context; returns its fiber handle.
    pub fn plugin(&self, plugin: Arc<dyn Plugin>, config: Option<Value>) -> FiberHandle {
        RegistryService::plugin(self, plugin, config)
    }

    /// Start an anonymous callback once the named services are available.
    pub fn inject<F>(&self, deps: impl IntoInjections, callback: F) -> FiberHandle
    where
        F: Fn(Context) -> BoxFuture<Result<()>> + Send + Sync + 'static,
    {
        let plugin: Arc<dyn Plugin> =
            Arc::new(AnonymousPlugin { inject: deps.into_injections(), callback });
        self.plugin(plugin, None)
    }

    /// Dispose every running plugin fiber, newest registration first.
    pub async fn stop(&self) {
        let fibers: Vec<(usize, Arc<Fiber>)> = {
            let registry = self.root.registry.lock().unwrap();
            let mut fibers = Vec::new();
            for runtime in registry.values() {
                for weak in runtime.fibers.lock().unwrap().iter().rev() {
                    if let Some(fiber) = weak.upgrade() {
                        fibers.push((runtime.key, fiber));
                    }
                }
            }
            fibers
        };
        for (_, fiber) in fibers {
            fiber.dispose().await;
        }
    }

    // ------------------------------------------------------------------
    // Events (mixed onto the context like cordis mixins)
    // ------------------------------------------------------------------

    /// Register a listener disposed with the owning fiber.
    pub async fn on<L>(&self, name: &str, listener: L) -> Result<EffectGuard>
    where
        L: Fn(Context, Value, crate::events::Next) -> BoxFuture<Result<Value>>
            + Send
            + Sync
            + 'static,
    {
        EventsService::on(self, name, Arc::new(listener), false, false).await
    }

    /// Register a global prepend listener (ignores scope filters).
    pub async fn on_global<L>(&self, name: &str, listener: L) -> Result<EffectGuard>
    where
        L: Fn(Context, Value, crate::events::Next) -> BoxFuture<Result<Value>>
            + Send
            + Sync
            + 'static,
    {
        EventsService::on(self, name, Arc::new(listener), true, true).await
    }

    /// Register a self-disposing one-shot listener.
    pub async fn once<L>(&self, name: &str, listener: L) -> Result<EffectGuard>
    where
        L: Fn(Context, Value, crate::events::Next) -> BoxFuture<Result<Value>>
            + Send
            + Sync
            + 'static,
    {
        use std::sync::atomic::AtomicBool;
        let fired = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(listener);
        let once_listener = move |ctx: Context, payload: Value, next: crate::events::Next| {
            if fired.swap(true, Ordering::SeqCst) {
                return Box::pin(async move { Ok(Value::Null) }) as BoxFuture<Result<Value>>;
            }
            let inner = inner.clone();
            Box::pin(async move { (*inner)(ctx, payload, next).await }) as BoxFuture<Result<Value>>
        };
        self.on(name, once_listener).await
    }

    /// Concurrent dispatch; aggregates listener failures.
    pub async fn parallel(&self, name: &str, payload: Value) -> Result<Value> {
        EventsService::parallel(self, name, payload).await
    }

    /// Fire-and-forget dispatch; failures are logged.
    pub fn emit(&self, name: &str, payload: Value) {
        EventsService::emit(self, name, payload);
    }

    /// Sequential dispatch until the first bail value.
    pub async fn serial(&self, name: &str, payload: Value) -> Result<Value> {
        EventsService::serial(self, name, payload).await
    }

    /// First-bail-wins dispatch (synchronous-intent alias of serial).
    pub async fn bail(&self, name: &str, payload: Value) -> Result<Value> {
        EventsService::bail(self, name, payload).await
    }

    /// Compose listeners around a fallback continuation.
    pub async fn waterfall<F>(&self, name: &str, payload: Value, fallback: F) -> Result<Value>
    where
        F: Fn(Value) -> BoxFuture<Result<Value>> + Send + Sync + 'static,
    {
        EventsService::waterfall(self, name, payload, fallback).await
    }

    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    /// Number of registered plugin runtimes.
    pub fn registry_size(&self) -> usize {
        self.root.registry.lock().unwrap().len()
    }

    /// Number of live fibers known to this root.
    pub fn live_fibers(&self) -> usize {
        self.root
            .fibers
            .lock()
            .unwrap()
            .values()
            .filter(|weak| weak.upgrade().is_some())
            .count()
    }
}

struct AnonymousPlugin<F> {
    inject: Vec<Injection>,
    callback: F,
}

impl<F> Plugin for AnonymousPlugin<F>
where
    F: Fn(Context) -> BoxFuture<Result<()>> + Send + Sync + 'static,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("anonymous")
    }

    fn inject(&self) -> Vec<Injection> {
        self.inject.clone()
    }

    fn apply(&self, ctx: Context, _config: Value) -> BoxFuture<Result<()>> {
        (self.callback)(ctx)
    }
}
