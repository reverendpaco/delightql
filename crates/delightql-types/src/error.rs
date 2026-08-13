// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// DelightQL Error Types
///
/// Comprehensive error handling for the DelightQL core library using thiserror.
use thiserror::Error;

/// Types of known limitations in the system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownLimitationType {
    /// Tree-sitter grammar ambiguity with qualified names ending queries
    QualifiedNameAmbiguity,
    /// Feature not yet implemented in the pipeline
    FeatureNotImplemented,
    // Other future limitations can be added here
}

/// The identifier badge scheme for compiler-minted error identities:
/// `delightql-error://<hierarchy>` ⇌
/// `https://delightql.org/uri/error/<hierarchy>`.
pub const ERROR_URI_SCHEME: &str = "delightql-error://";

#[derive(Error, Debug)]
pub enum DelightQLError {
    #[error("Parse error: {message}")]
    ParseError {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Explicit subcategory for URI classification. When `Some`, `error_uri()`
        /// uses this directly instead of inferring from message keywords.
        subcategory: Option<&'static str>,
    },

    #[error("Known limitation: {message}")]
    KnownLimitation {
        message: String,
        workaround: String,
        limitation_type: KnownLimitationType,
    },

    #[error("Transformation error: failed to convert CST to AST - {message}")]
    TransformationError {
        message: String,
        node_kind: String,
        position: Option<(usize, usize)>, // (start_byte, end_byte)
        subcategory: Option<&'static str>,
    },

    #[error("Transpilation error: failed to generate SQL - {message}")]
    TranspilationError {
        message: String,
        context: String,
        subcategory: Option<&'static str>,
    },

    #[error("Table not found: {table_name}")]
    TableNotFoundError { table_name: String, context: String },

    #[error("Column not found: {column}")]
    ColumnNotFoundError { column: String, context: String },

    #[error("Validation error: {message}")]
    ValidationError {
        message: String,
        context: String,
        subcategory: Option<&'static str>,
    },

    #[error("Database operation failed: {message} - {details}")]
    DatabaseOperationError {
        message: String,
        details: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
        /// Runtime subcategory for URI classification: "bug", "collision",
        /// "useafterfree", "assertion". When `None`, URI is `delightql-error://runtime`.
        subcategory: Option<&'static str>,
    },

    #[error("Connection lock poisoned: {message}")]
    ConnectionPoisonError {
        message: String,
        recovery_suggestion: String,
    },

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl DelightQLError {
    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::ParseError {
            message: message.into(),
            source: None,
            subcategory: None,
        }
    }

    pub fn parse_error_categorized(
        subcategory: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self::ParseError {
            message: message.into(),
            source: None,
            subcategory: Some(subcategory),
        }
    }

    pub fn parse_error_with_source(
        message: impl Into<String>,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::ParseError {
            message: message.into(),
            source: Some(source),
            subcategory: None,
        }
    }

    pub fn known_limitation(
        limitation_type: KnownLimitationType,
        message: impl Into<String>,
        workaround: impl Into<String>,
    ) -> Self {
        Self::KnownLimitation {
            message: message.into(),
            workaround: workaround.into(),
            limitation_type,
        }
    }

    pub fn transformation_error(message: impl Into<String>, node_kind: impl Into<String>) -> Self {
        Self::TransformationError {
            message: message.into(),
            node_kind: node_kind.into(),
            position: None,
            subcategory: None,
        }
    }

    pub fn transformation_error_categorized(
        subcategory: &'static str,
        message: impl Into<String>,
        node_kind: impl Into<String>,
    ) -> Self {
        Self::TransformationError {
            message: message.into(),
            node_kind: node_kind.into(),
            position: None,
            subcategory: Some(subcategory),
        }
    }

    pub fn transformation_error_with_position(
        message: impl Into<String>,
        node_kind: impl Into<String>,
        start_byte: usize,
        end_byte: usize,
    ) -> Self {
        Self::TransformationError {
            message: message.into(),
            node_kind: node_kind.into(),
            position: Some((start_byte, end_byte)),
            subcategory: None,
        }
    }

    pub fn transpilation_error(message: impl Into<String>, context: impl Into<String>) -> Self {
        Self::TranspilationError {
            message: message.into(),
            context: context.into(),
            subcategory: None,
        }
    }

    pub fn transpilation_error_categorized(
        subcategory: &'static str,
        message: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self::TranspilationError {
            message: message.into(),
            context: context.into(),
            subcategory: Some(subcategory),
        }
    }

    pub fn table_not_found_error(
        table_name: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self::TableNotFoundError {
            table_name: table_name.into(),
            context: context.into(),
        }
    }

    pub fn column_not_found_error(column: impl Into<String>, context: impl Into<String>) -> Self {
        Self::ColumnNotFoundError {
            column: column.into(),
            context: context.into(),
        }
    }

    pub fn validation_error(message: impl Into<String>, context: impl Into<String>) -> Self {
        Self::ValidationError {
            message: message.into(),
            context: context.into(),
            subcategory: None,
        }
    }

    pub fn validation_error_categorized(
        subcategory: &'static str,
        message: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self::ValidationError {
            message: message.into(),
            context: context.into(),
            subcategory: Some(subcategory),
        }
    }

    pub fn database_error(message: impl Into<String>, details: impl Into<String>) -> Self {
        Self::DatabaseOperationError {
            message: message.into(),
            details: details.into(),
            source: None,
            subcategory: None,
        }
    }

    pub fn database_error_categorized(
        subcategory: &'static str,
        message: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::DatabaseOperationError {
            message: message.into(),
            details: details.into(),
            source: None,
            subcategory: Some(subcategory),
        }
    }

    pub fn database_error_with_source(
        message: impl Into<String>,
        details: impl Into<String>,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self::DatabaseOperationError {
            message: message.into(),
            details: details.into(),
            source: Some(source),
            subcategory: None,
        }
    }

    pub fn connection_poison_error(
        message: impl Into<String>,
        recovery_suggestion: impl Into<String>,
    ) -> Self {
        Self::ConnectionPoisonError {
            message: message.into(),
            recovery_suggestion: recovery_suggestion.into(),
        }
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::NotImplemented(message.into())
    }

    /// Return a canonical error URI path for this error.
    ///
    /// The URI is hierarchical and domain-first:
    ///   `delightql-error://parse/...` — structural failures
    ///   `delightql-error://semantic/...` — name resolution, arity, constraints
    ///   `delightql-error://runtime/...` — execution-time failures
    ///
    /// Used by error hooks (`(~error://path ~)`) for prefix matching:
    /// hooks carry the bare hierarchy: expected `"semantic"` matches actual
    /// `delightql-error://semantic/arity` (the matcher strips the scheme).
    pub fn error_uri(&self) -> String {
        const S: &str = ERROR_URI_SCHEME;
        match self {
            Self::ParseError {
                subcategory,
                message,
                ..
            } => match subcategory {
                // A diagnosis may name the identity the SEMANTIC layer would
                // have used, had the form reached it: what the author needs
                // is the refusal's own name, not the phase that noticed.
                Some(sub) if sub.starts_with("dml/") || sub.starts_with("semantic/") => {
                    format!("{S}{}", sub)
                }
                Some(sub) => format!("{S}parse/{}", sub),
                None => format!("{S}parse/{}", Self::parse_subcategory(message)),
            },
            Self::KnownLimitation { limitation_type, .. } => match limitation_type {
                KnownLimitationType::QualifiedNameAmbiguity => {
                    format!("{S}semantic/limitation/qualified_name_ambiguity")
                }
                KnownLimitationType::FeatureNotImplemented => {
                    format!("{S}semantic/limitation/not_implemented")
                }
            },
            Self::TransformationError {
                subcategory,
                message,
                ..
            } => match subcategory {
                Some(sub) if sub.starts_with("dml/") => format!("{S}{}", sub),
                Some(sub) => format!("{S}semantic/{}", sub),
                None => format!(
                    "{S}semantic/constraint/{}",
                    Self::semantic_subcategory(message)
                ),
            },
            Self::TranspilationError {
                subcategory,
                message,
                ..
            } => match subcategory {
                Some(sub) if sub.starts_with("dml/") => format!("{S}{}", sub),
                Some(sub) => format!("{S}semantic/{}", sub),
                None => format!(
                    "{S}semantic/constraint/{}",
                    Self::semantic_subcategory(message)
                ),
            },
            Self::TableNotFoundError { .. } => format!("{S}semantic/resolution/table"),
            Self::ColumnNotFoundError { .. } => format!("{S}semantic/resolution/column"),
            Self::ValidationError {
                subcategory,
                message,
                ..
            } => match subcategory {
                Some(sub) if sub.starts_with("dml/") => format!("{S}{}", sub),
                Some(sub) if sub.starts_with("operational/") => format!("{S}{}", sub),
                // NB: effect-algebra discipline refusals (`effect/…`
                // subcategories: rule violations, registration,
                // effect-transformer limitations) deliberately take NO
                // carve-out — they fall to the default `semantic/` prefix
                // below (spelled `semantic/effect/…`).
                // Effect discipline IS a semantic error; the documented hierarchy
                // is parse/semantic/runtime and `error://semantic` must keep
                // matching them (pinned by the effects ball's
                // rules--79/80/81_r1_predicate_* hooks, `semantic/effect/…`).
                // imprint! lifecycle refusals (blueprint inertness) are their
                // own top segment, not a query-semantic error.
                Some(sub) if sub.starts_with("imprint/") => format!("{S}{}", sub),
                // namespace-creation name-guard refusals (the reserved
                // system name pool) are their own top segment — a
                // lifecycle-policy refusal, not a query semantic error.
                Some(sub) if sub.starts_with("namespace/") => format!("{S}{}", sub),
                Some(sub) => format!("{S}semantic/{}", sub),
                None => format!("{S}semantic/{}", Self::semantic_subcategory(message)),
            },
            Self::DatabaseOperationError { subcategory, .. } => match subcategory {
                // target-side failures are their own top segment (the
                // runtime/target double-spelling collapsed here)
                Some(sub) if sub.starts_with("target/") => format!("{S}{}", sub),
                Some(sub) => format!("{S}runtime/{}", sub),
                None => format!("{S}runtime"),
            },
            Self::ConnectionPoisonError { .. } => format!("{S}runtime/connection"),
            Self::NotImplemented(_) => format!("{S}semantic/limitation/not_implemented"),
            Self::IoError(_) => format!("{S}runtime/io"),
        }
    }

    /// Extract parse-phase subcategory from error message keywords.
    fn parse_subcategory(message: &str) -> &'static str {
        let lower = message.to_lowercase();
        if lower.contains("hex") {
            "literal"
        } else if lower.contains("octal") {
            "literal"
        } else if lower.contains("empty expression") {
            "expression"
        } else if lower.contains("missing operand") || lower.contains("missing operator") {
            "expression"
        } else if lower.contains("anonymous") || lower.contains("anon") {
            "anon"
        } else if lower.contains("pipe") {
            "pipe"
        } else if lower.contains("function") || lower.contains("lambda") {
            "function"
        } else if lower.contains("case") {
            "case"
        } else if lower.contains("window") || lower.contains("frame") {
            "window"
        } else if lower.contains("json") || lower.contains("path") {
            "json_path"
        } else if lower.contains("projection") || lower.contains("project") {
            "projection"
        } else if lower.contains("subquery") {
            "subquery"
        } else if lower.contains("pattern") {
            "pattern"
        } else {
            "general"
        }
    }

    /// Extract semantic-phase subcategory from error message keywords.
    ///
    /// Maps into: `arity/*`, `resolution/*`, `constraint/*`.
    fn semantic_subcategory(message: &str) -> &'static str {
        let lower = message.to_lowercase();
        // Arity errors
        if lower.contains("arity")
            || lower.contains("argument")
            || lower.contains("expects")
            || lower.contains("pattern incomplete")
        {
            "arity"
        // Resolution errors (from ValidationError — rare, usually caught by typed errors)
        } else if lower.contains("ambiguous") {
            "resolution/ambiguous"
        } else if lower.contains("not found") {
            "resolution"
        // Constraint subcategories
        } else if lower.contains("pivot") {
            "constraint/pivot"
        } else if lower.contains("destructur") {
            "constraint/destructuring"
        } else if lower.contains("join") || lower.contains("full outer") {
            "constraint/join"
        } else if lower.contains("context") {
            "constraint/context"
        } else if lower.contains("not supported") || lower.contains("unsupported") {
            "constraint/unsupported"
        } else if lower.contains("duplicate") {
            "constraint"
        } else if lower.contains("not implemented") {
            "limitation"
        } else {
            "constraint"
        }
    }
}

pub type Result<T> = std::result::Result<T, DelightQLError>;
