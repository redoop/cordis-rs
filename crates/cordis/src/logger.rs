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

    /// Structured write: attaches an originating event name and an optional
    /// stable machine-readable error code. The default implementation keeps a
    /// single-format sink by folding both into the message text
    /// (`[event=… code=…] …`); richer backends override this to serialize
    /// fields separately. Concrete `String` parameters keep the trait
    /// object-safe (no generic methods on `dyn Logger`).
    fn log_event(&self, level: LogLevel, event: String, code: Option<String>, message: String) {
        let prefix = match code {
            Some(code) => format!("[event={event} code={code}]"),
            None => format!("[event={event}]"),
        };
        self.log(level, format!("{prefix} {message}"));
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records raw `log` calls so the default `log_event` folding is observable.
    struct RecordingLogger {
        lines: Mutex<Vec<(LogLevel, String)>>,
    }
    impl Logger for RecordingLogger {
        fn log(&self, level: LogLevel, message: String) {
            self.lines.lock().unwrap().push((level, message));
        }
    }

    #[test]
    fn log_event_default_folds_event_and_code() {
        let logger = RecordingLogger { lines: Mutex::new(Vec::new()) };
        logger.log_event(
            LogLevel::Warn,
            "ping".to_string(),
            Some("timeout".to_string()),
            "listener failed: boom".to_string(),
        );
        assert_eq!(
            logger.lines.lock().unwrap().as_slice(),
            [(LogLevel::Warn, "[event=ping code=timeout] listener failed: boom".to_string())]
        );
    }

    #[test]
    fn log_event_omits_code_when_absent() {
        let logger = RecordingLogger { lines: Mutex::new(Vec::new()) };
        logger.log_event(
            LogLevel::Error,
            "fiber".to_string(),
            None,
            "startup failed".to_string(),
        );
        assert_eq!(
            logger.lines.lock().unwrap().as_slice(),
            [(LogLevel::Error, "[event=fiber] startup failed".to_string())]
        );
    }
}
