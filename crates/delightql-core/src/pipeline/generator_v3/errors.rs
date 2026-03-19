#[derive(Debug)]
#[allow(dead_code)]
pub enum GeneratorError {
    Error(String),
    /// Preserves a typed error (e.g., ValidationError from predicate arity
    /// checks) so it can be propagated without losing its error category.
    Typed(crate::error::DelightQLError),
}

impl std::fmt::Display for GeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratorError::Error(msg) => write!(f, "Generator error: {}", msg),
            GeneratorError::Typed(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for GeneratorError {}

impl GeneratorError {
    /// Convert to DelightQLError, preserving typed errors.
    pub fn into_delightql_error(self, context: &str) -> crate::error::DelightQLError {
        match self {
            GeneratorError::Typed(e) => e,
            GeneratorError::Error(msg) => crate::error::DelightQLError::ParseError {
                message: format!("{}: {}", context, msg),
                source: None,
                subcategory: None,
            },
        }
    }
}
