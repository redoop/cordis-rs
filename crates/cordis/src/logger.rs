//! Logger facade and the built-in logger service.
//!
//! Mirrors cordis' `LoggerService`: a `logger` service is provided by the
//! root fiber at boot, so every plugin can log immediately. Plugins (or
//! isolated scopes) may provide their own implementation of the trait to
//! redirect or enrich output.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Log levels, ordered by increasing severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The logger service contract.
pub trait Logger: Send + Sync + 'static {
    fn log(&self, level: LogLevel, message: String);
}

/// Minimum level filter shared by default loggers.
#[derive(Debug)]
pub struct LevelFilter(pub AtomicU8);

impl LevelFilter {
    pub fn new(min: LogLevel) -> Self {
        Self(AtomicU8::new(min as u8))
    }

    pub fn allows(&self, level: LogLevel) -> bool {
        self.0.load(Ordering::Relaxed) <= level as u8
    }
}

/// Stderr logger used by the root context out of the box.
pub struct DefaultLogger {
    filter: LevelFilter,
}

impl DefaultLogger {
    pub fn new() -> Self {
        Self { filter: LevelFilter::new(LogLevel::Info) }
    }

    pub fn with_min_level(min: LogLevel) -> Self {
        Self { filter: LevelFilter::new(min) }
    }
}

impl Default for DefaultLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl Logger for DefaultLogger {
    fn log(&self, level: LogLevel, message: String) {
        if !self.filter.allows(level) {
            return;
        }
        eprintln!("[{}] {}", level.as_str(), message);
    }
}

/// Boxed trait object stored as a service value.
///
/// Trait objects cannot implement `Any` directly, so the store wraps them in
/// this newtype; `Context::logger()` downcasts to recover the trait object.
pub struct LoggerHandle(pub Arc<dyn Logger>);
