//! # cordis-rust
//!
//! A Rust port of the cordis plugin kernel — the same kernel DSH uses to
//! compose its entire desktop harness out of ~200 small packages. The design
//! rule it borrows verbatim: **everything is a plugin**.
//!
//! ## The four moving parts
//!
//! - `Context` — a cheap-clonable dependency container. Derive children with
//!   `extend`, re-scope a single service with `isolate`, layer config with
//!   `intercept`.
//! - `Plugin` — `(ctx, config)` plus optional `inject` requirements and config
//!   validation. Adapt async closures via `plugin()` / `plugin_with()`.
//! - Fibers (`FiberState`) — one running instance of a plugin. A fiber stays
//!   PENDING until all required services are live, activates, and *unloads
//!   itself* when any provider disappears — then reloads when they return.
//!   Dependency graphs stay consistent without manual wiring.
//! - Effects — disposables collected while a plugin body runs; drained LIFO
//!   on unload, so cleanup always unwinds in reverse-startup order.
//!
//! Services themselves are just values provided by plugins into a two-level
//! store keyed by `(name, isolation label)`, retrieved typed.

pub mod context;
pub mod error;
pub mod events;
pub mod fiber;
pub mod logger;
pub mod plugin;
pub mod registry;
pub mod service;

pub use context::Context;
pub use error::{
    validation_error_from_json, CordisCode, CordisError, Error, Result, SharedError,
    ValidationIssue, ValidationError,
};
pub use events::{is_bailed, Listener, Next};
pub use fiber::{Disposer, EffectGuard, FiberHandle, FiberState};
pub use logger::{DefaultLogger, LevelFilter, LogLevel, Logger};
pub use plugin::{plugin, plugin_with, BoxFuture, Injection, IntoInjections, Plugin};
pub use service::ServiceCheck;
