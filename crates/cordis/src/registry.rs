//! Plugin registry — tracks runtimes and starts fibers (`RegistryService`).
//!
//! Runtimes are keyed by plugin identity (`Arc` pointer), mirroring how
//! cordis keys by callback identity: loading the same `Arc<dyn Plugin>`
//! twice creates two fibers of one runtime; unloading the last fiber of a
//! runtime removes it from the map.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::context::Context;
use crate::events::EventsService;
use crate::fiber::{Fiber, FiberHandle};
use crate::plugin::{normalize_inject, Plugin};

pub(crate) type RegistryMap = HashMap<usize, Arc<Runtime>>;

/// Shared record for one plugin identity across all its fibers.
pub(crate) struct Runtime {
    /// Map key (`Arc<dyn Plugin>` pointer address).
    pub key: usize,
    pub name: String,
    pub plugin: Arc<dyn Plugin>,
    pub fibers: Mutex<Vec<std::sync::Weak<Fiber>>>,
}

impl Runtime {
    /// Drop one fiber entry from this runtime's list.
    pub fn detach_fiber(&self, fiber: &Arc<Fiber>) -> bool {
        let mut fibers = self.fibers.lock().unwrap();
        let before = fibers.len();
        fibers.retain(|weak| weak.upgrade().map_or(false, |f| !Arc::ptr_eq(&f, fiber)));
        before != fibers.len()
    }
}

pub(crate) struct RegistryService;

impl RegistryService {
    /// Start a plugin under `ctx`, creating/reusing its runtime record.
    ///
    /// Mirrors cordis ordering: create the PENDING fiber, publish it via
    /// `internal/plugin` so synchronous observers can react, seed dependency
    /// snapshots, then request convergence toward the first epoch.
    pub(crate) fn plugin(
        ctx: &Context,
        plugin: Arc<dyn Plugin>,
        config: Option<Value>,
    ) -> FiberHandle {
        let key = Arc::as_ptr(&plugin) as *const u8 as usize;
        let runtime = {
            let mut registry = ctx.root.registry.lock().unwrap();
            registry
                .entry(key)
                .or_insert_with(|| {
                    Arc::new(Runtime {
                        key,
                        name: plugin.name().to_string(),
                        plugin,
                        fibers: Mutex::new(Vec::new()),
                    })
                })
                .clone()
        };

        let uid = ctx.root.next_uid();
        let inject = normalize_inject(runtime.plugin.inject());
        let fiber =
            Fiber::new_child(runtime.clone(), inject, config.unwrap_or(Value::Null), uid);
        fiber.bind(ctx);
        ctx.root.register_fiber(&fiber);
        runtime.fibers.lock().unwrap().push(Arc::downgrade(&fiber));

        EventsService::emit(
            ctx,
            "internal/plugin",
            serde_json::json!({
                "event": "created",
                "fiber": fiber.name(),
                "uid": uid,
                "inject": fiber.inject.iter().map(|i| i.service.to_string()).collect::<Vec<String>>(),
            }),
        );
        for injection in &fiber.inject {
            fiber.check_impl(injection.service.as_ref());
        }
        fiber.refresh();

        FiberHandle(fiber)
    }

    /// Remove one fiber from the registry; drop empty runtimes.
    pub(crate) fn remove_fiber(root: &crate::context::RootState, fiber: &Arc<Fiber>) {
        let Some(runtime_key) = fiber.runtime.as_ref().map(|runtime| runtime.key) else {
            return;
        };
        let mut registry = root.registry.lock().unwrap();
        if let Some(runtime) = registry.get(&runtime_key) {
            if runtime.detach_fiber(fiber) && runtime.fibers.lock().unwrap().is_empty() {
                registry.remove(&runtime_key);
            }
        }
    }
}
