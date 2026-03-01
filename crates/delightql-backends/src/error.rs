/// Execution Error Types with Enhanced Context
///
/// This module provides rich error types for the Query Execution Engine
/// with source location information and helpful suggestions.
use serde::{Deserialize, Serialize};
use std::fmt;

/// Type of execution error
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorType {
    ParseError,
    TranspilationError,
    DatabaseError,
    ValidationError,
    TimeoutError,
    ConfigurationError,
}

/// Rich execution error with context and suggestions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionError {
    /// Type of error
    error_type: ErrorType,
    /// Primary error message
    message: String,
    /// Line number in source (1-based, optional)
    line: Option<usize>,
    /// Column number in source (1-based, optional)
    column: Option<usize>,
    /// Helpful suggestion for fixing the error
    suggestion: Option<String>,
    /// Additional context information
    context: Option<String>,
    /// Source location span (start_byte, end_byte)
    source_span: Option<(usize, usize)>,
}

impl ExecutionError {
    /// Create a new parse error with optional location
    pub fn parse_error<M: Into<String>>(
        message: M,
        line: usize,
        column: usize,
        suggestion: Option<String>,
    ) -> Self {
        Self {
            error_type: ErrorType::ParseError,
            message: message.into(),
            line: Some(line),
            column: Some(column),
            suggestion,
            context: None,
            source_span: None,
        }
    }

    /// Create a parse error without location information
    pub fn parse_error_simple<M: Into<String>>(message: M) -> Self {
        Self {
            error_type: ErrorType::ParseError,
            message: message.into(),
            line: None,
            column: None,
            suggestion: None,
            context: None,
            source_span: None,
        }
    }

    /// Create a transpilation error with optional context
    pub fn transpilation_error<M: Into<String>>(message: M, suggestion: Option<String>) -> Self {
        Self {
            error_type: ErrorType::TranspilationError,
            message: message.into(),
            line: None,
            column: None,
            suggestion,
            context: None,
            source_span: None,
        }
    }

    /// Create a database error with optional suggestion
    pub fn database_error<M: Into<String>>(message: M, suggestion: Option<String>) -> Self {
        Self {
            error_type: ErrorType::DatabaseError,
            message: message.into(),
            line: None,
            column: None,
            suggestion,
            context: None,
            source_span: None,
        }
    }

    /// Create a validation error
    pub fn validation_error<M: Into<String>>(message: M, suggestion: Option<String>) -> Self {
        Self {
            error_type: ErrorType::ValidationError,
            message: message.into(),
            line: None,
            column: None,
            suggestion,
            context: None,
            source_span: None,
        }
    }

    /// Create a timeout error
    pub fn timeout_error<M: Into<String>>(message: M) -> Self {
        Self {
            error_type: ErrorType::TimeoutError,
            message: message.into(),
            line: None,
            column: None,
            suggestion: Some("Try simplifying the query or increasing the timeout".to_string()),
            context: None,
            source_span: None,
        }
    }

    /// Create a configuration error
    pub fn configuration_error<M: Into<String>>(message: M, suggestion: Option<String>) -> Self {
        Self {
            error_type: ErrorType::ConfigurationError,
            message: message.into(),
            line: None,
            column: None,
            suggestion,
            context: None,
            source_span: None,
        }
    }

    /// Add context to an existing error
    pub fn with_context<C: Into<String>>(mut self, context: C) -> Self {
        self.context = Some(context.into());
        self
    }

    /// Add source span information
    pub fn with_source_span(mut self, start_byte: usize, end_byte: usize) -> Self {
        self.source_span = Some((start_byte, end_byte));
        self
    }

    /// Get the error type
    pub fn error_type(&self) -> ErrorType {
        self.error_type
    }

    /// Get the error message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Get the line number (1-based)
    pub fn line(&self) -> usize {
        self.line.unwrap_or(0)
    }

    /// Get the column number (1-based)
    pub fn column(&self) -> usize {
        self.column.unwrap_or(0)
    }

    /// Get the suggestion if available
    pub fn suggestion(&self) -> Option<&str> {
        self.suggestion.as_deref()
    }

    /// Get the context if available
    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }

    /// Check if this error has source location information
    pub fn has_source_location(&self) -> bool {
        self.line.is_some() && self.column.is_some()
    }

    /// Get source span if available
    pub fn source_span(&self) -> Option<(usize, usize)> {
        self.source_span
    }

    /// Create a user-friendly error message
    pub fn format_user_message(&self) -> String {
        let mut message = format!("{}: {}", self.error_type_name(), self.message);

        if self.has_source_location() {
            message.push_str(&format!(
                " at line {}, column {}",
                self.line(),
                self.column()
            ));
        }

        if let Some(suggestion) = &self.suggestion {
            message.push_str(&format!("\nSuggestion: {}", suggestion));
        }

        if let Some(context) = &self.context {
            message.push_str(&format!("\nContext: {}", context));
        }

        message
    }

    fn error_type_name(&self) -> &'static str {
        match self.error_type {
            ErrorType::ParseError => "Parse Error",
            ErrorType::TranspilationError => "Transpilation Error",
            ErrorType::DatabaseError => "Database Error",
            ErrorType::ValidationError => "Validation Error",
            ErrorType::TimeoutError => "Timeout Error",
            ErrorType::ConfigurationError => "Configuration Error",
        }
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_user_message())
    }
}

impl std::error::Error for ExecutionError {}

/// Convert from DelightQL core errors to execution errors
impl From<delightql_types::error::DelightQLError> for ExecutionError {
    fn from(error: delightql_types::error::DelightQLError) -> Self {
        use delightql_types::error::DelightQLError as CoreError;
        match error {
            CoreError::ParseError { message, .. } => {
                ExecutionError::parse_error_simple(message)
            }
            CoreError::KnownLimitation {
                message,
                workaround,
                ..
            } => ExecutionError::parse_error_simple(format!(
                "{}\nWorkaround: {}",
                message, workaround
            )),
            CoreError::TransformationError {
                message, position, ..
            } => {
                let mut exec_error = ExecutionError::parse_error_simple(message);
                if let Some((start, end)) = position {
                    exec_error = exec_error.with_source_span(start, end);
                }
                exec_error
            }
            CoreError::TranspilationError {
                message, context, ..
            } => {
                ExecutionError::transpilation_error(message, None).with_context(context)
            }
            CoreError::TableNotFoundError {
                table_name,
                context,
            } => {
                ExecutionError::validation_error(format!("Table '{}' not found", table_name), None)
                    .with_context(context)
            }
            CoreError::ColumnNotFoundError { column, context } => {
                ExecutionError::validation_error(format!("Column '{}' not found", column), None)
                    .with_context(context)
            }
            CoreError::ValidationError {
                message, context, ..
            } => {
                ExecutionError::validation_error(message, None).with_context(context)
            }
            CoreError::TreeSitterError(ts_error) => {
                ExecutionError::parse_error_simple(format!("Tree-sitter error: {}", ts_error))
            }
            CoreError::IoError(io_error) => ExecutionError::configuration_error(
                format!("IO error: {}", io_error),
                Some("Check file permissions and paths".to_string()),
            ),
            CoreError::DatabaseOperationError {
                message, details, ..
            } => ExecutionError::database_error(message, Some(details)),
            CoreError::ConnectionPoisonError {
                message,
                recovery_suggestion,
            } => ExecutionError::database_error(
                message,
                Some(format!("Recovery: {}", recovery_suggestion)),
            ),
            CoreError::NotImplemented(message) => {
                ExecutionError::parse_error_simple(format!("Not implemented: {}", message))
            }
        }
    }
}

/// Helper functions for creating common errors
impl ExecutionError {
    /// Create an error for incomplete syntax
    pub fn incomplete_syntax(context: &str, suggestion: &str) -> Self {
        ExecutionError::parse_error_simple(format!("Incomplete {}", context))
            .with_context("Syntax error")
            .with_suggestion(suggestion.to_string())
    }

    /// Create an error for unknown table
    pub fn unknown_table(table_name: &str) -> Self {
        ExecutionError::database_error(
            format!("Table '{}' doesn't exist", table_name),
            Some("Check table name spelling or create the table first".to_string()),
        )
    }

    /// Create an error for unknown column
    pub fn unknown_column(column_name: &str, table_name: &str) -> Self {
        ExecutionError::database_error(
            format!(
                "Column '{}' doesn't exist in table '{}'",
                column_name, table_name
            ),
            Some("Check column name spelling or table schema".to_string()),
        )
    }

    /// Create an error for unknown function
    pub fn unknown_function(function_name: &str) -> Self {
        ExecutionError::transpilation_error(
            format!("Unknown function '{}'", function_name),
            Some("Available functions: count, sum, max, min, avg".to_string()),
        )
    }

    fn with_suggestion(mut self, suggestion: String) -> Self {
        self.suggestion = Some(suggestion);
        self
    }
}
