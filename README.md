# cordis-rs - everything is a plugin, in Rust

A Rust port of the cordis kernel that powers the DSH plugin system. Same
mental model, same lifecycle grammar, adapted to ownership: the runtime is
tokio, plugins are Send futures, and services live in a two-level store
keyed by (service name, isolation scope).

## The one idea

Nothing wires anything by hand. A **plugin** declares what it *requires*
(inject) and what it *provides* (ctx.provide). The kernel computes the
dependency graph and converges every fiber toward its desired epoch:

    register consumer first (PENDING)
          |  provider appears
          v
    provider LOADING --> ACTIVE --notify--> consumer re-checks --> ACTIVE

Unload runs the other way: dispose the provider and dependents cascade to
Pending; bring it back (or hot-update it) and they reload with fresh
snapshots.

## Concepts: cordis vs cordis-rs

| cordis (JS) | cordis-rs | notes |
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
cannot fail, and epochs are explicit strings (":" prefix plus ":uid" for each
satisfied dependency) instead of hidden fingerprints. JS mixins and trace/bind
proxies have no Rust equivalent: context extensions are expressed as extension
traits on Context (see TimerContextExt), which is static, typed, and zero-cost.

## Layout

    crates/cordis/src/
      context.rs   Context, scopes, root fiber, typed accessors
      fiber.rs     lifecycle driver: epochs, load/unload, effects, restart/update
      registry.rs  plugin identity (Arc pointer key), registration bookkeeping
      events.rs    emit / parallel / serial(bail) / waterfall(+Next)
      service.rs   provide/set/notify, strict resolution, intercept merging
      plugin.rs    trait Plugin + FnPlugin adapter + injection lists
      error.rs     CordisCode, ValidationError, aggregate errors

    crates/cordis-plugin-timer/   reference ecosystem plugin:
      timeout()/interval() as fiber-owned effects (cancelled on unload)

    examples/mini_dsh.rs          a tiny harness assembled only from plugins
    tests/reactive.rs             dependency graph convergence scenarios
    tests/events.rs               dispatch-mode semantics
    tests/isolate.rs              isolation + intercept layering

## Try it

    cargo run -p cordis --example mini_dsh   # compose -> converge -> hot-update -> stop
    cargo test --workspace                   # 23 tests, all green

The example registers an API server *before* its database exists, watches
the graph fill in, hot-swaps settings, then shuts down LIFO - no manual
wiring anywhere.
