//! Plugin entrypoint trait and adapters.
//!
//! A plugin in cordis is any callable `(ctx, config)` shape plus two optional
//! declarations: the services it requires (`inject`) and how to validate its
//! config. This port keeps that shape with a single Rust trait.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::context::Context;
use crate::error::{validation_error_from_json, Error, Result};

/// A boxed, sendable future — every plugin body and disposer is one of these.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A declared service dependency.
///
/// Mirrors one `inject` map entry in cordis: the service name plus an
/// optional intercept config merged into that service's view of the world.
#[derive(Debug, Clone)]
pub struct Injection {
    pub service: Cow<'static, str>,
    /// Intercept config for this dependency; see `Context::intercept`.
    pub config: Option<Value>,
}

impl Injection {
    pub fn new(service: impl Into<Cow<'static, str>>) -> Self {
        Self { service: service.into(), config: None }
    }

    pub fn with_config(service: impl Into<Cow<'static, str>>, config: Value) -> Self {
        Self { service: service.into(), config: Some(config) }
    }
}

impl From<&str> for Injection {
    fn from(value: &str) -> Self {
        Injection::new(value.to_string())
    }
}

/// Normalize inject declarations (like `Inject.resolve` in cordis).
pub fn normalize_inject(inject: impl Into<Vec<Injection>>) -> Vec<Injection> {
    inject.into()
}

/// The plugin trait — everything else in this crate is built from it.
///
/// Implement this directly for full control, or use `plugin()` to adapt an
/// async closure with a typed config.
pub trait Plugin: Send + Sync + 'static {
    /// Display name used in logs and diagnostics; defaults to the type path.
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(std::any::type_name::<Self>())
    }

    /// Required services. The fiber stays PENDING until all of them are
    /// provided by active fibers, and unloads again if any disappears.
    fn inject(&self) -> Vec<Injection> {
        Vec::new()
    }

    /// Validate and normalize raw user config before activation.
    ///
    /// The default implementation accepts the config unchanged; the typed
    /// adapter produced by `plugin()` deserializes into `C` here and reports
    /// failures as a `ValidationError`.
    fn resolve_config(&self, raw: &Value) -> Result<Value> {
        Ok(raw.clone())
    }

    /// The plugin body. Runs once per activation; disposers registered during
    /// the body are collected as fiber effects and run LIFO on unload.
    fn apply(&self, ctx: Context, config: Value) -> BoxFuture<Result<()>>;
}

struct FnPlugin<C, F> {
    name: Option<Cow<'static, str>>,
    inject: Vec<Injection>,
    callback: F,
    _marker: std::marker::PhantomData<fn() -> C>,
}

impl<C, F, Fut> Plugin for FnPlugin<C, F>
where
    C: DeserializeOwned + Send + 'static,
    F: Fn(Context, C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.name.clone().unwrap_or_else(|| Cow::Borrowed(std::any::type_name::<F>()))
    }

    fn inject(&self) -> Vec<Injection> {
        self.inject.clone()
    }

    fn resolve_config(&self, raw: &Value) -> Result<Value> {
        // Validate by round-tripping through the typed config; keep the raw
        // JSON so internal/config hooks stay JSON-stable across plugins.
        if let Err(err) = serde_json::from_value::<C>(raw.clone()) {
            return Err(Error::Validation(validation_error_from_json(err)));
        }
        Ok(raw.clone())
    }

    fn apply(&self, ctx: Context, config: Value) -> BoxFuture<Result<()>> {
        let typed: C = match serde_json::from_value(config) {
            Ok(value) => value,
            Err(err) => {
                let err = validation_error_from_json(err);
                return Box::pin(async move { Err(Error::Validation(err)) });
            }
        };
        Box::pin((self.callback)(ctx, typed))
    }
}

/// Adapt an async closure into a plugin with a typed, self-validating config.
///
/// ```
/// # use cordis::{plugin, Context};
/// # use serde::Deserialize;
/// # #[derive(Deserialize)]
/// # struct Conf { port: u16 }
/// let p = plugin("server", |ctx: Context, conf: Conf| async move {
///     println!("serving on {}", conf.port);
///     Ok(())
/// });
/// ```
pub fn plugin<C, F, Fut>(name: impl Into<Cow<'static, str>>, callback: F) -> Arc<dyn Plugin>
where
    C: DeserializeOwned + Send + 'static,
    F: Fn(Context, C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    plugin_with(name, Vec::<Injection>::new(), callback)
}

/// Like `plugin`, but also declares required services up front.
pub fn plugin_with<C, F, Fut>(
    name: impl Into<Cow<'static, str>>,
    inject: impl IntoInjections,
    callback: F,
) -> Arc<dyn Plugin>
where
    C: DeserializeOwned + Send + 'static,
    F: Fn(Context, C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Arc::new(FnPlugin {
        name: Some(name.into()),
        inject: inject.into_injections(),
        callback,
        _marker: std::marker::PhantomData,
    })
}

/// Flexible dependency-list declarations, mirroring cordis' array-or-map
/// `inject` shapes: strings are the common case, `Injection` adds intercepts.
pub trait IntoInjections {
    fn into_injections(self) -> Vec<Injection>;
}

impl IntoInjections for Vec<Injection> {
    fn into_injections(self) -> Vec<Injection> {
        self
    }
}

impl IntoInjections for Injection {
    fn into_injections(self) -> Vec<Injection> {
        vec![self]
    }
}

impl IntoInjections for &[&str] {
    fn into_injections(self) -> Vec<Injection> {
        self.iter().map(|name| Injection::new(name.to_string())).collect()
    }
}

impl IntoInjections for Vec<&str> {
    fn into_injections(self) -> Vec<Injection> {
        self.into_iter().map(|name| Injection::new(name.to_string())).collect()
    }
}

impl IntoInjections for &[String] {
    fn into_injections(self) -> Vec<Injection> {
        self.iter().map(|name| Injection::new(name.clone())).collect()
    }
}

impl IntoInjections for Vec<String> {
    fn into_injections(self) -> Vec<Injection> {
        self.into_iter().map(Injection::new).collect()
    }
}

impl<const N: usize> IntoInjections for [&str; N] {
    fn into_injections(self) -> Vec<Injection> {
        self.as_slice().into_injections()
    }
}
