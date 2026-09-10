// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
#[derive(Debug)]
pub enum GeneratorError {
    /// The generator was handed a SQL AST it has no spelling for. That is a
    /// compiler invariant: resolution and lowering should have refused or
    /// shaped the form before it reached the text renderer.
    Error(String),
    /// Preserves a typed error (e.g. a validation refusal from predicate
    /// arity checks) so it can be propagated without losing its identity.
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
    /// Convert to DelightQLError, preserving typed errors. A generator
    /// failure with no identity of its own is an internal invariant: the
    /// form it could not spell is one an earlier phase admitted.
    pub fn into_delightql_error(self, context: &str) -> crate::error::DelightQLError {
        match self {
            GeneratorError::Typed(e) => e,
            GeneratorError::Error(msg) => crate::diagnostic::Internal::invariant(context, msg),
        }
    }
}
