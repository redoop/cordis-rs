//! Framework error types with stable machine-readable codes.
//!
//! Mirrors cordis' `CordisError` / `ValidationError` pair: lifecycle misuse is
//! a coded programming error, config problems are aggregated validation issues.

/// Stable machine-readable error codes, mirroring `CordisError.Code`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CordisCode {
    /// Cannot create an effect on an inactive (disposed / unloading) context.
    InactiveEffect,
    /// The value passed to plugin registration is not a valid plugin shape.
    InvalidPlugin,
    /// A service of this name has already been provided in this scope.
    DuplicateService,
    /// Cannot mutate a service provided by another fiber.
    ForeignService,
    /// A required service was read while its providing fiber is inactive.
    ServiceUnavailable,
}

impl CordisCode {
    pub fn as_str(self) -> &'static str {
        match self {
            CordisCode::InactiveEffect => "cannot create effect on inactive context",
            CordisCode::InvalidPlugin => "invalid plugin",
            CordisCode::DuplicateService => "service has already been registered",
            CordisCode::ForeignService => "cannot set service provided by another fiber",
            CordisCode::ServiceUnavailable => "cannot get required service in inactive context",
        }
    }
}

/// Framework error with a stable code, mirroring cordis `CordisError`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct CordisError {
    pub code: CordisCode,
    pub message: String,
}

impl CordisError {
    pub fn new(code: CordisCode) -> Self {
        let message = code.as_str().to_string();
        Self { code, message }
    }

    pub fn with_message(code: CordisCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

/// One schema issue from plugin-config validation.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}{suffix}")]
pub struct ValidationIssue {
    /// Dot-joined path to the offending config fragment, if known.
    pub path: Option<String>,
    pub message: String,
    suffix: String,
}

impl ValidationIssue {
    pub fn new(path: Option<String>, message: impl Into<String>) -> Self {
        let suffix = match &path {
            Some(p) => format!(" (at {p})"),
            None => String::new(),
        };
        Self { path, message: message.into(), suffix }
    }
}

/// Aggregated plugin-config validation failure, mirroring cordis `ValidationError`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid config ({} issues): {}", .0.len(), render_issues(.0))]
pub struct ValidationError(pub Vec<ValidationIssue>);

fn render_issues(issues: &[ValidationIssue]) -> String {
    issues
        .iter()
        .map(|issue| format!("  - {issue}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Convert a `serde_json` error into a single-issue [`ValidationError`].
pub fn validation_error_from_json(error: serde_json::Error) -> ValidationError {
    // serde messages embed line/column info; surface them verbatim.
    ValidationError(vec![ValidationIssue::new(None, error.to_string())])
}

/// Unified result alias for framework operations and plugin bodies.
pub type Result<T, E = Error> = std::result::Result<T, E>;

use std::fmt;
use std::sync::Arc;

/// Clonable shared box for free-form errors (`Box<dyn Error>` analogue).
#[derive(Clone)]
pub struct SharedError(pub Arc<dyn std::error::Error + Send + Sync>);

impl fmt::Display for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for SharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for SharedError {}

/// The framework-wide error: either a coded lifecycle error or a free-form one
/// surfaced from plugin code, listener callbacks, or disposers.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Coded(#[from] CordisError),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    /// One or more concurrent listeners failed (mirrors JS `AggregateError`).
    #[error("aggregate error ({} failures)", .0.len())]
    Aggregate(Vec<Error>),
    #[error(transparent)]
    Other(#[from] SharedError),
}

impl Error {
    pub fn msg(message: impl Into<String>) -> Self {
        Error::Other(SharedError(Arc::new(string_error(message.into()))))
    }

    /// Build an aggregate error from concurrent listener failures.
    pub fn aggregate(errors: Vec<Error>) -> Self {
        Error::Aggregate(errors)
    }
}

struct StringError(String);

impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for StringError {}

fn string_error(message: String) -> StringError {
    StringError(message)
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Other(SharedError(Arc::new(value)))
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::msg(value)
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Error::msg(value)
    }
}
