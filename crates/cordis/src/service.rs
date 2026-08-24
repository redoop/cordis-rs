//! Reflection and service-resolution layer — the Rust analogue of `ReflectService`.
//!
//! Services live in a two-level store keyed by `(service name, isolation
//! label)`. Every context resolves a name to one label through its scope
//! chain; `isolate()` rewrites exactly one name to a fresh (or joined) label,
//! so different subtrees can host independent implementations of the same
//! service — without touching their parent scopes.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::context::{Context, RootState, ScopeNode};
use crate::error::{CordisCode, CordisError, Error, Result};
use crate::events::EventsService;
use crate::fiber::{Disposer, EffectGuard, Fiber, ProviderSnapshot};

/// Availability predicate attached to a provided service.
pub type CheckFn = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

/// Public alias for plugin authors writing availability predicates.
pub type ServiceCheck = CheckFn;

/// Lazy value producer behind an accessor entry: recomputed per read from
/// the requesting context. Returning None means "currently unavailable".
pub(crate) type ComputeFn =
    Arc<dyn Fn(&Context) -> Option<Arc<dyn std::any::Any + Send + Sync>> + Send + Sync>;

pub(crate) struct ServiceImpl {
    #[allow(dead_code)]
    pub name: String,
    /// Stored value; a placeholder for accessor entries.
    pub value: Arc<dyn std::any::Any + Send + Sync>,
    pub provider_uid: u64,
    pub check: Option<CheckFn>,
    pub compute: Option<ComputeFn>,
}

/// Shared service store, mirroring `ReflectService.store`.
#[derive(Default)]
pub(crate) struct ReflectStore {
    /// service name -> isolation label -> implementation.
    pub labels: HashMap<String, HashMap<u64, ServiceImpl>>,
    /// Names ever declared, for duplicate-shape diagnostics.
    pub declared: std::collections::HashSet<String>,
}

impl ReflectStore {
    /// Read the implementation visible from `label`, optionally requiring
    /// its provider fiber to be active.
    pub(crate) fn get_impl(
        &self,
        root: &RootState,
        name: &str,
        label: u64,
        strict: bool,
    ) -> Option<&ServiceImpl> {
        let impl_ = self.labels.get(name)?.get(&label)?;
        if strict && !root.fiber_is_active(impl_.provider_uid) {
            return None;
        }
        Some(impl_)
    }
}

/// Register a service implementation owned by the calling fiber.
///
/// Mirrors `ctx.provide()`: the registration itself is an effect, so it is
/// removed automatically when the owning fiber unloads. The withdrawal
/// disposer removes the implementation synchronously and notifies dependents
/// immediately, driving the cascade unload of consumers.
pub(crate) async fn provide(
    ctx: &Context,
    name: &str,
    value: Arc<dyn std::any::Any + Send + Sync>,
    check: Option<CheckFn>,
) -> Result<EffectGuard> {
    let fiber = ctx.require_fiber("provide")?;
    let provider_uid = match *fiber.uid.lock().unwrap() {
        Some(uid) => uid,
        None => return Err(CordisError::new(CordisCode::InactiveEffect).into()),
    };
    let label = ctx.effective_label(name);

    // Validate before mutating so a failed effect leaves no residue.
    {
        let mut store = ctx.root.reflect.lock().unwrap();
        if store.labels.get(name).map_or(false, |map| map.contains_key(&label)) {
            return Err(CordisError::with_message(
                CordisCode::DuplicateService,
                format!("service \"{name}\" has been registered in this scope"),
            )
            .into());
        }
        store.declared.insert(name.to_string());
        store
            .labels
            .entry(name.to_string())
            .or_default()
            .insert(
                label,
                ServiceImpl { name: name.to_string(), value, provider_uid, check, compute: None },
            );
    }

    let name_owned = name.to_string();
    let ctx_for_effect = ctx.clone();
    let fiber_for_effect = fiber.clone();
    fiber
        .effect(
            format!("ctx.provide({name})"),
            async move {
                notify(&ctx_for_effect, &[name_owned.clone()]);
                let ctx2 = ctx_for_effect.clone();
                let name2 = name_owned.clone();
                let fiber2 = fiber_for_effect.clone();
                let disposer: Disposer = Box::new(move || {
                    let label = ctx2.effective_label(&name2);
                    {
                        let mut store = ctx2.root.reflect.lock().unwrap();
                        if let Some(map) = store.labels.get_mut(&name2) {
                            map.remove(&label);
                            if map.is_empty() {
                                store.labels.remove(&name2);
                            }
                        }
                    }
                    fiber2.store.lock().unwrap().remove(&name2);
                    // Synchronous notification: dependents re-check and
                    // request unloading before this disposer returns.
                    notify(&ctx2, &[name2]);
                    Box::pin(async move {})
                });
                Ok(Some(disposer))
            },
        )
        .await
}

/// Register a COMPUTED service owned by the calling fiber.
///
/// The Rust analogue of cordis' accessor(): every typed read invokes the
/// closure against the requesting context, so the value can be derived from
/// other services at read time. Lifecycle is identical to provide(): the
/// registration is an effect and dependents are notified on withdrawal.
pub(crate) async fn accessor(ctx: &Context, name: &str, compute: ComputeFn) -> Result<EffectGuard> {
    let fiber = ctx.require_fiber("accessor")?;
    let provider_uid = match *fiber.uid.lock().unwrap() {
        Some(uid) => uid,
        None => return Err(CordisError::new(CordisCode::InactiveEffect).into()),
    };
    let label = ctx.effective_label(name);

    {
        let mut store = ctx.root.reflect.lock().unwrap();
        if store.labels.get(name).map_or(false, |map| map.contains_key(&label)) {
            let q: char = '"';
            return Err(CordisError::with_message(
                CordisCode::DuplicateService,
                format!("service {q}{name}{q} has been registered in this scope"),
            )
            .into());
        }
        store.declared.insert(name.to_string());
        store
            .labels
            .entry(name.to_string())
            .or_default()
            .insert(
                label,
                ServiceImpl {
                    name: name.to_string(),
                    value: Arc::new(()),
                    provider_uid,
                    check: None,
                    compute: Some(compute),
                },
            );
    }

    let name_owned = name.to_string();
    let ctx_for_effect = ctx.clone();
    fiber
        .effect(
            format!("ctx.accessor({name})"),
            async move {
                notify(&ctx_for_effect, &[name_owned.clone()]);
                let ctx2 = ctx_for_effect.clone();
                let name2 = name_owned.clone();
                let disposer: Disposer = Box::new(move || {
                    let label = ctx2.effective_label(&name2);
                    {
                        let mut store = ctx2.root.reflect.lock().unwrap();
                        if let Some(map) = store.labels.get_mut(&name2) {
                            map.remove(&label);
                            if map.is_empty() {
                                store.labels.remove(&name2);
                            }
                        }
                    }
                    notify(&ctx2, &[name2]);
                    Box::pin(async move {})
                });
                Ok(Some(disposer))
            },
        )
        .await
}

/// Overwrite a provided service value owned by the calling fiber.
pub(crate) fn set(
    ctx: &Context,
    name: &str,
    value: Arc<dyn std::any::Any + Send + Sync>,
) -> Result<()> {
    let fiber_uid = ctx
        .fiber()
        .and_then(|f| *f.uid.lock().unwrap())
        .ok_or_else(|| CordisError::new(CordisCode::InactiveEffect))?;
    let label = ctx.effective_label(name);
    let mut store = ctx.root.reflect.lock().unwrap();
    let impl_ = store
        .labels
        .get_mut(name)
        .and_then(|map| map.get_mut(&label))
        .ok_or_else(|| Error::msg(format!("cannot set property \"{name}\" without provide")))?;
    if impl_.provider_uid != fiber_uid {
        return Err(CordisError::new(CordisCode::ForeignService).into());
    }
    impl_.value = value;
    Ok(())
}

/// Re-evaluate every fiber requiring any of `names` in matching scopes,
/// then emit scoped `internal/service` events (observability extension).
pub(crate) fn notify(ctx: &Context, names: &[String]) {
    for name in names {
        let source_label = ctx.effective_label(name);

        // Phase 1 (under lock): snapshot the interested fibers only.
        let interested: Vec<Arc<Fiber>> = {
            let fibers = ctx.root.fibers.lock().unwrap();
            fibers
                .values()
                .filter_map(|weak| weak.upgrade())
                .filter(|fiber| {
                    fiber.inject.iter().any(|inj| inj.service.as_ref() == name.as_str())
                        && fiber.ctx().effective_label(name) == source_label
                })
                .collect()
        };

        // Phase 2 (lock-free): re-check snapshots and re-request epochs.
        // check_impl resolves providers through fiber_is_active, which takes
        // root.fibers again — hence strictly outside phase 1 (std Mutex is
        // not re-entrant).
        for fiber in &interested {
            fiber.check_impl(name);
        }
        for fiber in &interested {
            fiber.refresh();
        }

        // Scoped observability event: listeners outside this isolation scope
        // do not see the service change.
        let filter_name = name.clone();
        EventsService::emit_filtered(
            ctx,
            "internal/service",
            serde_json::json!({ "service": name }),
            move |hook_ctx: &Context| hook_ctx.effective_label(&filter_name) == source_label,
        );
    }
}

/// Resolve the visible implementation of `name` for a dependent fiber,
/// enforcing provider activity and running the availability predicate.
pub(crate) fn resolve_impl_for(
    ctx: &Context,
    _dependent: &Arc<Fiber>,
    name: &str,
) -> Option<ProviderSnapshot> {
    let label = ctx.effective_label(name);
    let snapshot = {
        let store = ctx.root.reflect.lock().unwrap();
        let impl_ = store.get_impl(&ctx.root, name, label, true)?;
        ProviderSnapshot {
            value: impl_.value.clone(),
            provider_uid: impl_.provider_uid,
            dynamic: impl_.compute.is_some(),
        }
    };
    // Availability predicate runs with no locks held; it may consult other
    // services (ctx.get takes the reflect lock internally).
    let check = {
        let store = ctx.root.reflect.lock().unwrap();
        store
            .get_impl(&ctx.root, name, label, false)
            .and_then(|impl_| impl_.check.clone())
    };
    let check_ok = match check {
        Some(check) => check(ctx),
        None => true,
    };
    if check_ok {
        Some(snapshot)
    } else {
        None
    }
}

/// Merge intercept configs along the scope chain (outermost first).
///
/// Later entries win on conflicting keys, mirroring `Object.assign`.
pub(crate) fn merged_intercept(scope: &ScopeNode, name: &str) -> Value {
    // Walking innermost -> parent yields leaf-first; reverse so the merge
    // applies outermost layers first and inner layers win conflicts.
    let mut chain = Vec::new();
    let mut cursor: Option<&ScopeNode> = Some(scope);
    while let Some(node) = cursor {
        if let Some(queue) = node.intercept.get(name) {
            chain.extend(queue.iter().cloned());
        }
        cursor = node.parent.as_deref();
    }
    chain.reverse();
    let mut result = serde_json::Map::new();
    for layer in chain {
        if let Value::Object(map) = layer {
            for (key, val) in map {
                result.insert(key, val);
            }
        }
    }
    Value::Object(result)
}
