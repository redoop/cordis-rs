# cordis-rust — everything is a plugin, in Rust

[![crates.io](https://img.shields.io/crates/v/cordis-rust)](https://crates.io/crates/cordis-rust)
[![docs.rs](https://img.shields.io/docsrs/cordis-rust)](https://docs.rs/cordis-rust)

A Rust port of the cordis kernel that powers the DSH plugin system. Same
mental model, same lifecycle grammar, adapted to ownership: the runtime is
tokio, plugins are Send futures, and services live in a two-level store
keyed by (service name, isolation scope).

The crate publishes on crates.io as **`cordis-rust`**; the library target
keeps the name **`cordis`**, so consumers write `use cordis::…` unchanged:

```toml
[dependencies]
cordis-rust = "0.2"
```

```rust
use cordis::{Context, plugin, plugin_with, Injection};

// A plugin is just `(ctx, config) -> future`, optionally declaring
// required services; the kernel converges the dependency graph itself.
let api = ctx.plugin(plugin_with(
    "api",
    vec![Injection::new("database")],
    |ctx: Context, _config: ()| async move {
        let db = ctx.require::<Database>("database")?;
        // ... serve ...
        Ok(())
    },
), None);
```

## The one idea

Nothing wires anything by hand. A **plugin** declares what it *requires*
(inject) and what it *provides* (`ctx.provide`). The kernel computes the
dependency graph and converges every fiber toward its desired epoch:

    register consumer first (PENDING)
          |  provider appears
          v
    provider LOADING --> ACTIVE --notify--> consumer re-checks --> ACTIVE

Unload runs the other way: dispose the provider and dependents cascade to
Pending; bring it back (or hot-update it) and they reload with fresh
snapshots.

## Concepts: cordis (JS) vs cordis-rust

| cordis (JS) | cordis-rust | notes |
| --- | --- | --- |
| Context | Context (Clone, cheap) | extend / isolate / intercept chain |
| Plugin (ctx, config) | trait Plugin + plugin() / plugin_with() closures | config validated via serde before apply |
| inject: string[] | Injection::from("name") list | strict resolution + reactive notify keys |
| Fiber states | FiberState::{Pending, Loading, Active, Unloading, Disposed, Failed} | identical grammar |
| ctx.provide(name, value) | ctx.provide(name, value).await | async; registers withdrawal effect |
| ctx.effect(name, cb) | ctx.effect(name, future).await -> Option<Disposer> | disposers are infallible BoxFnOnce -> BoxFuture<()> |
| ctx.get(name) | ctx.get::<T>(name)? | typed store snapshot of the owning fiber |
| require without inject | Error: cannot get property ... | same guardrail message |
| app.scope(label) | ctx.isolate(label) | per-scope duplicate detection and visibility |
| config layers | ctx.intercept(service, json) | outermost-first merge, inner wins conflicts |
| events emit/parallel/bail/waterfall | same four modes | bail = non-null/non-false return |
| Next veto handle | next.run(payload).await | omit the call to override built-ins |
| internal events | internal/{status,plugin,service,config,update} | scoped filters for service/config |
| accessor(name, {get}) | ctx.accessor::<T>(name, closure).await | derived service recomputed per read; never cached |
| fiber.getEffects() | fiber.effect_metas() | labels of live effects, registration order |
| ctx.is(other) | ctx.is(&other) | scope identity |

Deliberate deviations: provide/on are async (lock-free snapshots), disposers
cannot fail, and epochs are explicit strings (`:` prefix plus `:uid` for each
satisfied dependency) instead of hidden fingerprints. JS mixins and trace/bind
proxies have no Rust equivalent: context extensions are expressed as extension
traits on Context (see `TimerContextExt`), which is static, typed, and zero-cost.

## Hardening since v0.2 (P1/P2)

Beyond the faithful port, the kernel carries a set of production hardening
in one release — all verified by the workspace suite:

- **Lock-free hot paths.** `FiberState` is mirrored into an atomic cache —
  `state()` no longer chains four lifecycle mutexes — and a root-level ACTIVE
  bitmask answers `fiber_is_active` without touching the fiber registry, so
  every typed service read stays lock-light. The full lock ordering
  (`reflect < registry < fibers < fiber-internal`) is documented in each
  module.
- **Bounded waits.** `FiberHandle::join_with_timeout()` lets a never-finishing
  `apply()` fail a bounded join instead of wedging `join`/`dispose`;
  `ctx.parallel_timeout()` does the same for hung listeners, returning a
  stable `CordisCode::Timeout`.
- **Zero-spawn event hot path.** `ctx.on_sync` + `ctx.emit_sync` await
  sync-slot listeners inline on the dispatching task (no per-event tokio
  spawn) — aimed at high-frequency streams such as per-token deltas; ordinary
  `emit` keeps its fire-and-forget semantics.
- **Structured logging.** `Logger::log_event(level, event, code, message)`
  with stable classifier `Error::code()` (`CordisCode` / `validation` /
  `aggregate`), folded into a single sink unless overridden.

## Layout

    crates/cordis/src/
      context.rs   Context, scopes, root fiber, typed accessors
      fiber.rs     lifecycle driver: epochs, load/unload, effects, restart/update
      registry.rs  plugin identity (Arc pointer key), registration bookkeeping
      events.rs    emit / parallel / serial(bail) / waterfall(+Next), sync slot
      service.rs   provide/set/notify, strict resolution, intercept merging
      plugin.rs    trait Plugin + FnPlugin adapter + injection lists
      error.rs     CordisCode, ValidationError, aggregate errors

    crates/cordis/examples/mini_dsh.rs   a tiny harness assembled only from plugins
    crates/cordis/tests/
      reactive.rs  dependency graph convergence scenarios
      events.rs    dispatch-mode semantics, sync slot, bounded parallel
      accessor.rs  derived services, hot-path stability
      isolate.rs   isolation + intercept layering

    crates/cordis-plugin-timer/  reference ecosystem plugin:
      timeout()/interval() as fiber-owned effects (cancelled on unload)

    crates/plugin-contract/  zero-dep C-ABI surface for dynamic plugins
    crates/greeter-plugin/   cdylib plugin loaded at RUNTIME by dynhost
    crates/dynhost/          dlopen host: adapts exports to trait Plugin

## Try it

    cd crates/cordis
    cargo run --example mini_dsh      # compose -> converge -> hot-update -> stop
    cargo test --workspace            # 38 tests, all green
    cargo run -p dynhost              # dlopen a .dylib/.so plugin at runtime
    cargo add cordis-rust             # or use it from crates.io directly

The example registers an API server *before* its database exists, watches
the graph fill in, hot-swaps settings, then shuts down LIFO - no manual
wiring anywhere.

## Consumed by

- **dsh-rs** (`github.com/redoop/dsh-rs`) — the interface-isolated agent
  harness; depends on `cordis = { version = "0.2", package = "cordis-rust" }`.