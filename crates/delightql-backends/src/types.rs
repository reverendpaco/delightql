/// Core types for the Query Execution Engine
/// 
/// This module defines the fundamental types that control how queries are executed
/// and what results are returned.

use serde::{Deserialize, Serialize};
use crate::error::ExecutionError;

/// Execution mode determines how a DelightQL query is processed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Parse and transpile only - no execution. Returns generated SQL.
    Validate,
    /// Full execution pipeline - parse, transpile, and execute. Returns results.
    Execute,
    /// Parse, transpile, and analyze execution plan - no actual execution.
    Explain,
}

/// Status of query execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// Query executed successfully
    Success,
    /// Failed to parse DelightQL
    ParseError,
    /// Failed to transpile to SQL
    TranspilationError,
    /// Database error during execution
    DatabaseError,
    /// Validation error (e.g., schema mismatch)
    ValidationError,
    /// Query execution timed out
    Timeout,
}

/// Complete result of query execution including all metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    /// The execution mode that was used
    pub mode: ExecutionMode,
    /// Status of the execution
    pub status: ExecutionStatus,
    /// Original DelightQL query
    pub dql_query: String,
    /// Generated SQL (if transpilation succeeded)
    pub generated_sql: Option<String>,
    /// Column names in result set
    pub columns: Vec<String>,
    /// Result rows as vectors of string values
    pub rows: Vec<Vec<String>>,
    /// Number of rows in the result set
    pub row_count: usize,
    /// Execution time in milliseconds (for Execute mode only)
    pub execution_time_ms: Option<u64>,
    /// Error information (if any)
    pub error: Option<ExecutionError>,
}

impl QueryResult {
    /// Create a new successful result
    pub fn success(
        mode: ExecutionMode,
        dql_query: String,
        generated_sql: Option<String>,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    ) -> Self {
        let row_count = rows.len();
        Self {
            mode,
            status: ExecutionStatus::Success,
            dql_query,
            generated_sql,
            columns,
            rows,
            row_count,
            execution_time_ms: None,
            error: None,
        }
    }

    /// Create a new error result
    pub fn error(
        mode: ExecutionMode,
        status: ExecutionStatus,
        dql_query: String,
        error: ExecutionError,
    ) -> Self {
        Self {
            mode,
            status,
            dql_query,
            generated_sql: None,
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
            error: Some(error),
        }
    }

    /// Add execution timing to the result
    pub fn with_timing(mut self, execution_time_ms: u64) -> Self {
        self.execution_time_ms = Some(execution_time_ms);
        self
    }

    /// Get a specific value from the result set
    pub fn get_value(&self, row: usize, column: &str) -> Option<&String> {
        let col_index = self.columns.iter().position(|c| c == column)?;
        self.rows.get(row)?.get(col_index)
    }

    /// Check if the result is successful
    pub fn is_success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }

    /// Check if the result has an error
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Configuration for query execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Maximum execution time in milliseconds
    pub timeout_ms: Option<u64>,
    /// Maximum number of rows to return
    pub max_rows: Option<usize>,
    /// Enable EXPLAIN mode support
    pub enable_explain: bool,
    /// Enable performance timing
    pub enable_timing: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: Some(30000), // 30 seconds default timeout
            max_rows: None,          // No limit by default
            enable_explain: true,
            enable_timing: true,
        }
    }
}

impl ExecutionConfig {
    /// Create a new config with only timing enabled (for performance)
    pub fn performance_optimized() -> Self {
        Self {
            timeout_ms: Some(5000), // 5 second timeout for performance
            max_rows: Some(10000),  // Reasonable limit
            enable_explain: false,  // Disable for performance
            enable_timing: true,
        }
    }

    /// Create a config for development with all features enabled
    pub fn development() -> Self {
        Self {
            timeout_ms: Some(60000), // 1 minute for debugging
            max_rows: None,          // No limit for development
            enable_explain: true,
            enable_timing: true,
        }
    }
}