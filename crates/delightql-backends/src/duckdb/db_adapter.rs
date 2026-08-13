// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// DuckDB adapter implementing DelightQL database traits
//
// This module adapts duckdb to work with DelightQL's DatabaseConnection trait.

use delightql_types::{DatabaseConnection, DbValue, Result as DelightQLResult, Row};
use duckdb::{Connection, Error as DuckDBError};
use std::sync::{Arc, Mutex};

/// Wrapper for duckdb Row that implements DelightQL's Row trait
struct DuckDBRow<'stmt> {
    row: &'stmt duckdb::Row<'stmt>,
}

impl<'stmt> Row for DuckDBRow<'stmt> {
    fn get_value(&self, idx: usize) -> DelightQLResult<DbValue> {
        let value = self.row.get_ref(idx).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to get column value", e.to_string())
        })?;

        Ok(duckdb_value_to_db_value(value))
    }

    fn get_value_by_name(&self, name: &str) -> DelightQLResult<DbValue> {
        let value = self.row.get_ref(name).map_err(|e| {
            delightql_types::DelightQLError::database_error(
                format!("Failed to get column '{}'", name),
                e.to_string(),
            )
        })?;

        Ok(duckdb_value_to_db_value(value))
    }

    fn column_count(&self) -> usize {
        self.row.as_ref().column_count()
    }

    fn column_name(&self, idx: usize) -> DelightQLResult<&str> {
        match self.row.as_ref().column_name(idx) {
            Ok(name) => Ok(name),
            Err(e) => Err(delightql_types::DelightQLError::database_error("Invalid column index", e.to_string()))
        }
    }
}

/// Convert duckdb ValueRef to DbValue.
///
/// Every number reaches `DbValue` without narrowing. DuckDB carries wider
/// integers than `DbValue::Integer` (HUGEINT is 128 bits, UBIGINT is
/// unsigned 64) and an exact DECIMAL that no `f64` can hold; `DbValue::whole`
/// and `DbValue::exact_numeric` say what becomes of each. There is no `as`
/// on a value here, and no numeric default anywhere: a conversion that
/// cannot be made faithfully is not made.
fn duckdb_value_to_db_value(value: duckdb::types::ValueRef<'_>) -> DbValue {
    use duckdb::types::ValueRef;

    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Boolean(b) => DbValue::Integer(if b { 1 } else { 0 }),
        ValueRef::TinyInt(i) => DbValue::whole(i.into()),
        ValueRef::SmallInt(i) => DbValue::whole(i.into()),
        ValueRef::Int(i) => DbValue::whole(i.into()),
        ValueRef::BigInt(i) => DbValue::whole(i.into()),
        ValueRef::HugeInt(i) => DbValue::whole(i),
        ValueRef::UTinyInt(i) => DbValue::whole(i.into()),
        ValueRef::USmallInt(i) => DbValue::whole(i.into()),
        ValueRef::UInt(i) => DbValue::whole(i.into()),
        ValueRef::UBigInt(i) => DbValue::whole(i.into()),
        // f32 -> f64 is exact: every f32 is an f64.
        ValueRef::Float(f) => DbValue::Real(f.into()),
        ValueRef::Double(f) => DbValue::Real(f),
        ValueRef::Decimal(d) => DbValue::exact_numeric(d.to_string()),
        ValueRef::Timestamp(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Text(s) => DbValue::Text(String::from_utf8_lossy(s).to_string()),
        ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
        ValueRef::Date32(_) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Time64(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Interval { .. } => DbValue::Text(format!("{:?}", value)),
        // An enum's ordinal is a `usize`; on a hypothetical platform where
        // that is wider than i128 the spelling still stands in for it.
        ValueRef::Enum(_, v) => match i128::try_from(v) {
            Ok(ordinal) => DbValue::whole(ordinal),
            Err(_) => DbValue::Text(v.to_string()),
        },
        ValueRef::List(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Struct(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Array(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Map(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Union(_, _) => DbValue::Text(format!("{:?}", value)),
    }
}

/// Convert DbValue to duckdb Value for parameter binding
fn db_value_to_duckdb(value: &DbValue) -> duckdb::types::Value {
    match value {
        DbValue::Null => duckdb::types::Value::Null,
        DbValue::Integer(i) => duckdb::types::Value::BigInt(*i),
        DbValue::Real(f) => duckdb::types::Value::Double(*f),
        DbValue::Text(s) => duckdb::types::Value::Text(s.clone()),
        DbValue::Blob(b) => duckdb::types::Value::Blob(b.clone()),
    }
}

/// DuckDB database connection adapter
pub struct DuckDBConnection {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDBConnection {
    /// Create a new DuckDB connection adapter
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        DuckDBConnection { conn }
    }

    /// Create from a database path
    pub fn open(path: &str) -> DelightQLResult<Self> {
        let conn = Connection::open(path).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to open database", e.to_string())
        })?;

        Ok(DuckDBConnection {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Get the underlying Arc<Mutex<Connection>>
    pub fn get_connection_arc(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

impl DatabaseConnection for DuckDBConnection {
    fn execute(&self, sql: &str, params: &[DbValue]) -> DelightQLResult<usize> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let duckdb_params: Vec<duckdb::types::Value> =
            params.iter().map(db_value_to_duckdb).collect();

        let params_refs: Vec<&dyn duckdb::ToSql> = duckdb_params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        conn.execute(sql, params_refs.as_slice())
            .map_err(|e| delightql_types::DelightQLError::database_error("Execute failed", e.to_string()))
    }

    fn last_insert_rowid(&self) -> DelightQLResult<i64> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        // DuckDB doesn't have a direct equivalent to last_insert_rowid
        // We need to query the last inserted rowid using a different method
        // For now, we'll return an error indicating this is not supported
        // In a real implementation, you'd need to track this differently
        conn.query_row("SELECT last_insert_id()", [], |row| {
            row.get::<_, i64>(0)
        }).map_err(|e| {
            delightql_types::DelightQLError::database_error(
                "DuckDB does not support last_insert_rowid in the same way as SQLite",
                e.to_string()
            )
        })
    }

    fn query_row_values(&self, sql: &str, params: &[DbValue]) -> DelightQLResult<Option<Vec<DbValue>>> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let duckdb_params: Vec<duckdb::types::Value> =
            params.iter().map(db_value_to_duckdb).collect();

        let params_refs: Vec<&dyn duckdb::ToSql> = duckdb_params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        match conn.query_row(sql, params_refs.as_slice(), |row| {
            let column_count = row.as_ref().column_count();
            let mut values = Vec::with_capacity(column_count);

            for i in 0..column_count {
                let value = row.get_ref(i).map_err(|e| {
                    DuckDBError::ToSqlConversionFailure(Box::new(
                        delightql_types::DelightQLError::database_error(
                            "Failed to get column value",
                            e.to_string()
                        )
                    ))
                })?;
                values.push(duckdb_value_to_db_value(value));
            }

            Ok(values)
        }) {
            Ok(values) => Ok(Some(values)),
            Err(DuckDBError::QueryReturnedNoRows) => Ok(None),
            Err(DuckDBError::ToSqlConversionFailure(boxed)) => {
                Err(delightql_types::DelightQLError::database_error(
                    "Query callback failed",
                    boxed.to_string(),
                ))
            }
            Err(e) => Err(delightql_types::DelightQLError::database_error(
                "Query failed",
                e.to_string(),
            )),
        }
    }

    fn query_all_rows(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> DelightQLResult<(Vec<String>, Vec<Vec<DbValue>>)> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let duckdb_params: Vec<duckdb::types::Value> =
            params.iter().map(db_value_to_duckdb).collect();
        let params_refs: Vec<&dyn duckdb::ToSql> = duckdb_params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        let mut stmt = conn.prepare(sql).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to prepare query", e.to_string())
        })?;

        // ROWS FIRST, HEADING SECOND. A duckdb statement has no schema
        // until it has run — asking a prepared-but-unexecuted statement for
        // its column names panics inside the driver — so the width comes
        // from each row and the names are read once the rows are drained.
        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let width = row.as_ref().column_count();
                let mut values = Vec::with_capacity(width);
                for idx in 0..width {
                    values.push(duckdb_value_to_db_value(row.get_ref(idx)?));
                }
                Ok(values)
            })
            .map_err(|e| {
                delightql_types::DelightQLError::database_error("Query execution failed", e.to_string())
            })?;

        let mut result_rows = Vec::new();
        for row_result in rows {
            result_rows.push(row_result.map_err(|e| {
                delightql_types::DelightQLError::database_error("Failed to fetch row", e.to_string())
            })?);
        }

        let column_names: Vec<String> = stmt.column_names();

        Ok((column_names, result_rows))
    }

}

// Note: DatabaseConnectionExt is automatically implemented for DuckDBConnection
// via the blanket implementation in delightql_types::db_traits.
// The blanket impl provides query_row() and query() methods using query_row_values().

#[cfg(test)]
mod tests {
    use super::DuckDBConnection;
    use crate::duckdb::connection::DuckDBConnectionManager;
    use delightql_types::{DatabaseConnection, DbValue};

    /// Every value below comes from the ENGINE, not from a hand-built
    /// `ValueRef`: HUGEINT, UBIGINT and DECIMAL are exactly the widths a
    /// Rust-side literal cannot stand in for.
    ///
    /// NOTE: this module needs `libduckdb` to link, so it runs only where
    /// the `duckdb` feature can be built (`cargo test -p delightql-backends
    /// --no-default-features --features duckdb`). It is not part of the
    /// default lane.
    fn one_cell(sql: &str) -> DbValue {
        let manager = DuckDBConnectionManager::new_memory().expect("in-memory duckdb");
        let connection = DuckDBConnection::new(manager.get_connection_arc());
        let (_columns, mut rows) = connection
            .query_all_rows(sql, &[])
            .expect("the engine answers");
        assert_eq!(rows.len(), 1, "one row expected from: {sql}");
        let mut row = rows.swap_remove(0);
        assert_eq!(row.len(), 1, "one column expected from: {sql}");
        row.swap_remove(0)
    }

    fn wire(sql: &str) -> Vec<u8> {
        one_cell(sql)
            .into_wire_bytes()
            .expect("a present value, not NULL")
    }

    /// A HUGEINT past `i64` keeps its digits. `as i64` would have answered
    /// `-9223372036854775808` for the first of these.
    #[test]
    fn a_hugeint_past_the_top_is_not_narrowed() {
        assert_eq!(
            wire("SELECT CAST('9223372036854775808' AS HUGEINT)"),
            b"9223372036854775808".to_vec()
        );
        assert_eq!(
            wire("SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT)"),
            b"170141183460469231731687303715884105727".to_vec()
        );
        // One that fits is still an Integer, unchanged by the widening.
        assert!(matches!(
            one_cell("SELECT CAST('42' AS HUGEINT)"),
            DbValue::Integer(42)
        ));
    }

    /// A UBIGINT past `i64::MAX` keeps its digits AND its sign. `as i64`
    /// would have answered `-1` for `u64::MAX`.
    #[test]
    fn a_ubigint_past_the_top_is_not_narrowed() {
        assert_eq!(
            wire("SELECT CAST('18446744073709551615' AS UBIGINT)"),
            b"18446744073709551615".to_vec()
        );
        assert!(matches!(
            one_cell("SELECT CAST('7' AS UBIGINT)"),
            DbValue::Integer(7)
        ));
    }

    /// A DECIMAL keeps its own exact spelling. Through an `f64` the first
    /// of these comes back `1234567890.1234567`, and the old code did that
    /// silently; a DECIMAL the conversion could not make at all became
    /// `0.0`, which is why no `f64` step remains.
    #[test]
    fn a_decimal_keeps_its_exact_spelling() {
        assert_eq!(
            wire("SELECT CAST('1234567890.1234567891' AS DECIMAL(20,10))"),
            b"1234567890.1234567891".to_vec()
        );
        // Scale is part of the spelling: a DECIMAL(4,2) two is "2.00".
        assert_eq!(
            wire("SELECT CAST('2' AS DECIMAL(4,2))"),
            b"2.00".to_vec()
        );
    }

    /// The kinds P.11's carrier pins already cover on the other engines,
    /// asserted here on DuckDB's own values so the roads agree.
    #[test]
    fn absence_blobs_and_text_survive_the_duckdb_road() {
        assert!(matches!(one_cell("SELECT NULL"), DbValue::Null));
        assert_eq!(one_cell("SELECT NULL").into_wire_bytes(), None);
        assert_eq!(wire("SELECT 'NULL'"), b"NULL".to_vec());
        assert_eq!(wire("SELECT CAST('NULL' AS BLOB)"), b"NULL".to_vec());
        assert_eq!(wire("SELECT '\\x00\\x01\\xFF'::BLOB"), vec![0x00, 0x01, 0xff]);
    }
}
