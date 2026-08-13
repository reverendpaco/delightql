// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

    /// The bytes this value carries to a consumer that speaks bytes;
    /// `None` is SQL NULL.
    ///
    /// Absence is the ONLY spelling of NULL. No byte string stands in for
    /// it — a text value whose characters are `NULL` is `Some(b"NULL")`
    /// and stays distinguishable from `DbValue::Null` for the whole
    /// journey.
    pub fn into_wire_bytes(self) -> Option<Vec<u8>> {
        match self {
            DbValue::Null => None,
            DbValue::Integer(i) => Some(i.to_string().into_bytes()),
            DbValue::Real(f) => Some(f.to_string().into_bytes()),
            DbValue::Text(s) => Some(s.into_bytes()),
            DbValue::Blob(b) => Some(b),
        }
    }

    /// A whole number of any width the engines carry.
    ///
    /// `Integer` is 64 bits and some engines are wider (DuckDB's HUGEINT
    /// and UBIGINT, and every unsigned bigint). A value that fits becomes
    /// `Integer`; one that does not keeps its EXACT decimal spelling
    /// instead. Both reach the wire as the same digits, so nothing is lost
    /// either way — whereas `as i64` answers a different number and says
    /// nothing about having done so.
    pub fn whole(value: i128) -> Self {
        match i64::try_from(value) {
            Ok(fits) => DbValue::Integer(fits),
            Err(_) => DbValue::Text(value.to_string()),
        }
    }

    /// An engine's EXACT numeric (`DECIMAL`/`NUMERIC`), as that engine
    /// spells it.
    ///
    /// It stays text because `Real` is binary floating point and cannot
    /// hold every decimal: `1234567890.1234567891` through an `f64` comes
    /// back a different number, with no failure to notice. Rounding a
    /// value quietly is the same defect as wrapping one quietly, and an
    /// exact numeric already has a faithful representation — its own.
    pub fn exact_numeric(spelling: impl Into<String>) -> Self {
        DbValue::Text(spelling.into())
    }

    /// This value read as text, or `None` for SQL NULL. Numbers give their
    /// decimal spelling and a blob its lossy UTF-8 reading.
    ///
    /// For catalog and metadata reads, whose columns are text by
    /// construction. It has no text for NULL on purpose: a caller that
    /// needs one is at a display boundary and chooses it there.
    pub fn as_wire_text(&self) -> Option<String> {
        match self {
            DbValue::Null => None,
            DbValue::Integer(i) => Some(i.to_string()),
            DbValue::Real(f) => Some(f.to_string()),
            DbValue::Text(s) => Some(s.clone()),
            DbValue::Blob(b) => Some(String::from_utf8_lossy(b).into_owned()),
        }
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

    /// Query for all rows and return (column_names, rows) as typed values.
    ///
    /// The one way to read a whole result set from a connection. It answers
    /// in the engine's own value vocabulary, so NULL is `DbValue::Null` and
    /// never a text spelling: text whose characters are `NULL` is an
    /// ordinary `DbValue::Text` and stays distinct from it. A caller that
    /// wants strings converts at its own boundary, where the choice is
    /// visible.
    ///
    /// Default implementation returns an error; connections override.
    fn query_all_rows(
        &self,
        _sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<DbValue>>)> {
        Err(DelightQLError::validation_error(
            "query_all_rows not implemented for this connection type",
            "This connection does not support full result set queries",
        ))
    }

    /// Attach a read-only in-memory schema deserialized from a static SQLite
    /// image (`delightql-bytes://` mounts). The
    /// default refusal IS the design's SQLite-primary-only rule: only the
    /// native SQLite adapter overrides this; pipe, fatboy, DuckDB, and WASM
    /// connections refuse with an actionable error.
    fn attach_static_bytes(&self, _schema_alias: &str, _bytes: &'static [u8]) -> Result<()> {
        Err(DelightQLError::validation_error(
            "delightql-bytes:// mounts require a native SQLite primary connection",
            "This connection type cannot attach a deserialized in-memory schema",
        ))
    }

    /// The owned-buffer sibling of `attach_static_bytes`: the image is
    /// COPIED into SQLite-owned memory (for buffers built at runtime, e.g.
    /// the CLI's live surface database). Same attach-class semantics and
    /// default refusal — but NOT the same write protection: unlike the
    /// static variant, the copied schema is engine-level WRITABLE, because
    /// `BEGIN IMMEDIATE` locks every attached database and a READONLY
    /// member would fail all such transactions session-wide (imprint!).
    /// Not-for-writing is the host's convention until the delightql-level
    /// DML gate lands.
    fn attach_bytes_copied(&self, _schema_alias: &str, _bytes: &[u8]) -> Result<()> {
        Err(DelightQLError::validation_error(
            "delightql-bytes:// mounts require a native SQLite primary connection",
            "This connection type cannot attach a deserialized in-memory schema",
        ))
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

#[cfg(test)]
mod tests {
    use super::DbValue;

    /// A whole number wider than `Integer` keeps its exact digits rather
    /// than becoming a different number. `i64::MAX + 1` is the first value
    /// `as i64` would wrap — to `i64::MIN`.
    #[test]
    fn a_signed_whole_past_the_top_is_not_wrapped() {
        let past_the_top = i128::from(i64::MAX) + 1;
        assert_eq!(
            DbValue::whole(past_the_top).into_wire_bytes(),
            Some(b"9223372036854775808".to_vec())
        );
        assert_eq!(
            DbValue::whole(i128::from(i64::MIN) - 1).into_wire_bytes(),
            Some(b"-9223372036854775809".to_vec())
        );
        assert_eq!(
            DbValue::whole(i128::MAX).into_wire_bytes(),
            Some(i128::MAX.to_string().into_bytes())
        );
    }

    /// The unsigned case: every `u64` above `i64::MAX` reads as a NEGATIVE
    /// number through `as i64`, so this is the family where narrowing does
    /// not merely lose magnitude — it loses the sign.
    #[test]
    fn an_unsigned_whole_past_the_top_is_not_wrapped() {
        assert_eq!(
            DbValue::whole(i128::from(u64::MAX)).into_wire_bytes(),
            Some(b"18446744073709551615".to_vec())
        );
        // The wrap this replaces: u64::MAX `as i64` is -1.
        assert_ne!(
            DbValue::whole(i128::from(u64::MAX)).into_wire_bytes(),
            DbValue::Integer(-1).into_wire_bytes()
        );
    }

    /// A number that fits is still an `Integer`, and the two spellings put
    /// the SAME digits on the wire — which is why widening the road costs
    /// no reader anything.
    #[test]
    fn a_whole_that_fits_stays_an_integer() {
        assert!(matches!(DbValue::whole(7), DbValue::Integer(7)));
        assert!(matches!(
            DbValue::whole(i128::from(i64::MAX)),
            DbValue::Integer(i) if i == i64::MAX
        ));
        assert_eq!(
            DbValue::whole(7).into_wire_bytes(),
            DbValue::Text("7".to_string()).into_wire_bytes()
        );
    }

    /// An exact numeric keeps its own spelling, scale included. The
    /// comparison value is what an `f64` round trip does to it.
    #[test]
    fn an_exact_numeric_keeps_its_spelling() {
        let spelled = "1234567890.1234567891";
        assert_eq!(
            DbValue::exact_numeric(spelled).into_wire_bytes(),
            Some(spelled.as_bytes().to_vec())
        );
        let through_a_float = DbValue::Real(spelled.parse::<f64>().unwrap());
        assert_ne!(
            through_a_float.into_wire_bytes(),
            Some(spelled.as_bytes().to_vec()),
            "the f64 road loses digits here — that is why this one exists"
        );
        // Trailing scale is part of the value's own spelling.
        assert_eq!(
            DbValue::exact_numeric("1.10").into_wire_bytes(),
            Some(b"1.10".to_vec())
        );
    }
}
