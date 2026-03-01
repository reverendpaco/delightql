//! Database backend implementations for DelightQL
//!
//! This crate provides concrete implementations of database backends including:
//! - Schema introspection (DatabaseSchema trait implementations)
//! - Connection management
//! - Query execution
//! - Value type conversions
//!
//! ## Features
//!
//! - `sqlite`: SQLite backend (default)
//! - `duckdb`: DuckDB backend (default)
//!
//! ## Architecture
//!
//! This crate follows the Dependency Inversion Principle:
//! - `delightql-core` defines traits and provides initialization
//! - This crate implements those traits for specific databases
//! - The CLI or other consumers choose which backend to use
//!
//! This allows adding new database backends without modifying core.

// Shared types and errors
pub mod error;
pub mod executor;
pub mod schema_base;
pub mod types;

// SQLite backend
#[cfg(feature = "sqlite")]
pub mod sqlite;

// DuckDB backend
#[cfg(feature = "duckdb")]
pub mod duckdb;

// Re-export commonly used types
pub use error::{ErrorType, ExecutionError};
pub use executor::{execute_sql, execute_sql_with_connection, QueryResults};
#[cfg(feature = "duckdb")]
pub use executor::execute_sql_with_duckdb_connection;
pub use schema_base::{ColumnInfo, DatabaseSchema, SchemaProvider, TableInfo};
pub use types::{ExecutionConfig, ExecutionMode, ExecutionStatus, QueryResult};

// SQLite re-exports
#[cfg(feature = "sqlite")]
pub use sqlite::{
    connection::{ConnectionInfo as SqliteConnectionInfo, SqliteConnectionManager},
    executor::{SqliteExecutor, SqliteExecutorImpl},
    value::SqlValue,
    DynamicSqliteSchema,
};

// DuckDB re-exports
#[cfg(feature = "duckdb")]
pub use duckdb::{
    connection::{ConnectionInfo as DuckDBConnectionInfo, DuckDBConnectionManager},
    executor::{DuckDBExecutor, DuckDBExecutorImpl},
    value::SqlValue as DuckDBSqlValue,
    DynamicDuckDBSchema, // This is now defined inline in duckdb/mod.rs
};
