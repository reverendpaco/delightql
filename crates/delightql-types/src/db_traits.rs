// Database abstraction traits for DelightQL core
//
// These traits decouple delightql-core from specific database implementations (rusqlite, DuckDB, WASM bridge, mocks).
// Core code uses these traits, while concrete implementations live in delightql-backends.

use crate::error::{DelightQLError, Result};
use std::fmt::Debug;

/// Value that can be bound to SQL parameters or returned from queries
#[derive(Debug, Clone)]
pub enum DbValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl DbValue {
    /// Try to extract an integer value
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            DbValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract a float value
    pub fn as_real(&self) -> Option<f64> {
        match self {
            DbValue::Real(f) => Some(*f),
            DbValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to extract a text value
    pub fn as_text(&self) -> Option<&str> {
        match self {
            DbValue::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to extract a blob value
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            DbValue::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, DbValue::Null)
    }
}

/// Trait for accessing column values from a database row
pub trait Row {
    /// Get value by column index (0-based)
    fn get_value(&self, idx: usize) -> Result<DbValue>;

    /// Get value by column name
    fn get_value_by_name(&self, name: &str) -> Result<DbValue>;

    /// Get number of columns in this row
    fn column_count(&self) -> usize;

    /// Get column name by index
    fn column_name(&self, idx: usize) -> Result<&str>;
}

/// Trait for database connections (object-safe)
///
/// Provides core database operations needed by DelightQL:
/// - Executing SQL statements (DDL/DML)
/// - Querying for single rows
/// - Querying for multiple rows
///
/// This trait is object-safe to allow `dyn DatabaseConnection` trait objects.
/// Generic query methods are provided via the `DatabaseConnectionExt` extension trait.
pub trait DatabaseConnection: Send + Sync {
    /// Execute a SQL statement that doesn't return rows (DDL/DML)
    ///
    /// Returns the number of rows affected
    fn execute(&self, sql: &str, params: &[DbValue]) -> Result<usize>;

    /// Get the last inserted row ID (for auto-increment columns)
    fn last_insert_rowid(&self) -> Result<i64>;

    /// Query for a single row and return values as Vec<DbValue>
    ///
    /// Returns None if no rows match
    fn query_row_values(&self, sql: &str, params: &[DbValue]) -> Result<Option<Vec<DbValue>>>;

    /// Query for all rows and return (column_names, rows) as string values.
    ///
    /// Used by the execution engine to route queries to imported connections.
    /// Default implementation returns an error; backends override as needed.
    fn query_all_string_rows(
        &self,
        _sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        Err(DelightQLError::validation_error(
            "query_all_string_rows not implemented for this connection type",
            "This connection does not support full result set queries",
        ))
    }

    /// Query for all rows with NULL fidelity preserved.
    ///
    /// Returns (column_names, rows) where None = SQL NULL.
    /// Default delegates to query_all_string_rows (no NULL distinction).
    fn query_all_nullable_rows(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        let (cols, rows) = self.query_all_string_rows(sql, params)?;
        let nullable_rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(Some).collect())
            .collect();
        Ok((cols, nullable_rows))
    }
}

/// Extension trait for database connections with generic methods
///
/// This trait is NOT object-safe due to generic methods, but provides
/// convenient query methods when you have a concrete type.
pub trait DatabaseConnectionExt: DatabaseConnection {
    /// Query for a single row, applying a function to extract the result
    ///
    /// Returns None if no rows match
    fn query_row<T, F>(&self, sql: &str, params: &[DbValue], f: F) -> Result<Option<T>>
    where
        F: FnOnce(&dyn Row) -> Result<T>;

    /// Query for multiple rows, calling a function for each row
    ///
    /// The function should return Ok(()) to continue or Err to stop iteration
    fn query<F>(&self, sql: &str, params: &[DbValue], f: F) -> Result<()>
    where
        F: FnMut(&dyn Row) -> Result<()>;
}

/// Blanket implementation of extension trait for all DatabaseConnection types
impl<T: DatabaseConnection + ?Sized> DatabaseConnectionExt for T {
    fn query_row<U, F>(&self, sql: &str, params: &[DbValue], f: F) -> Result<Option<U>>
    where
        F: FnOnce(&dyn Row) -> Result<U>,
    {
        // Default implementation using query_row_values
        match self.query_row_values(sql, params)? {
            Some(values) => {
                struct VecRow(Vec<DbValue>);
                impl Row for VecRow {
                    fn get_value(&self, idx: usize) -> Result<DbValue> {
                        self.0.get(idx).cloned().ok_or_else(|| {
                            DelightQLError::validation_error(
                                "Column index out of bounds",
                                format!("Index {} exceeds column count {}", idx, self.0.len()),
                            )
                        })
                    }

                    fn get_value_by_name(&self, _name: &str) -> Result<DbValue> {
                        Err(DelightQLError::validation_error(
                            "Cannot get value by name from Vec<DbValue>",
                            "Use get_value with index instead",
                        ))
                    }

                    fn column_count(&self) -> usize {
                        self.0.len()
                    }

                    fn column_name(&self, _idx: usize) -> Result<&str> {
                        Err(DelightQLError::validation_error(
                            "Column names not available from Vec<DbValue>",
                            "Use index-based access",
                        ))
                    }
                }

                let row = VecRow(values);
                Ok(Some(f(&row)?))
            }
            None => Ok(None),
        }
    }

    fn query<F>(&self, _sql: &str, _params: &[DbValue], mut _f: F) -> Result<()>
    where
        F: FnMut(&dyn Row) -> Result<()>,
    {
        // Not implemented in blanket impl - concrete types should override
        Err(DelightQLError::validation_error(
            "query() not implemented for trait object",
            "Use concrete type or implement DatabaseConnectionExt",
        ))
    }
}

/// Helper trait for converting Rust values to/from database values
pub trait ToDbValue {
    fn to_db_value(&self) -> DbValue;
}

pub trait FromDbValue: Sized {
    fn from_db_value(value: &DbValue) -> Result<Self>;
}

// Implementations for common types
impl ToDbValue for i64 {
    fn to_db_value(&self) -> DbValue {
        DbValue::Integer(*self)
    }
}

impl ToDbValue for f64 {
    fn to_db_value(&self) -> DbValue {
        DbValue::Real(*self)
    }
}

impl ToDbValue for String {
    fn to_db_value(&self) -> DbValue {
        DbValue::Text(self.clone())
    }
}

impl ToDbValue for &str {
    fn to_db_value(&self) -> DbValue {
        DbValue::Text(self.to_string())
    }
}

impl ToDbValue for Vec<u8> {
    fn to_db_value(&self) -> DbValue {
        DbValue::Blob(self.clone())
    }
}

impl<T: ToDbValue> ToDbValue for Option<T> {
    fn to_db_value(&self) -> DbValue {
        match self {
            Some(v) => v.to_db_value(),
            None => DbValue::Null,
        }
    }
}

impl FromDbValue for i64 {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Integer(i) => Ok(*i),
            _ => Err(DelightQLError::validation_error(
                "Expected integer",
                format!("Got {:?}", value),
            )),
        }
    }
}

impl FromDbValue for f64 {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Real(f) => Ok(*f),
            DbValue::Integer(i) => Ok(*i as f64),
            _ => Err(DelightQLError::validation_error(
                "Expected real",
                format!("Got {:?}", value),
            )),
        }
    }
}

impl FromDbValue for String {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Text(s) => Ok(s.clone()),
            _ => Err(DelightQLError::validation_error(
                "Expected text",
                format!("Got {:?}", value),
            )),
        }
    }
}

impl FromDbValue for Vec<u8> {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Blob(b) => Ok(b.clone()),
            _ => Err(DelightQLError::validation_error(
                "Expected blob",
                format!("Got {:?}", value),
            )),
        }
    }
}

impl FromDbValue for bool {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Integer(i) => Ok(*i != 0),
            _ => Err(DelightQLError::validation_error(
                "Expected boolean (integer)",
                format!("Got {:?}", value),
            )),
        }
    }
}

impl<T: FromDbValue> FromDbValue for Option<T> {
    fn from_db_value(value: &DbValue) -> Result<Self> {
        match value {
            DbValue::Null => Ok(None),
            other => Ok(Some(T::from_db_value(other)?)),
        }
    }
}
