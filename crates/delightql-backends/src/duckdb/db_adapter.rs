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

/// Convert duckdb ValueRef to DbValue
fn duckdb_value_to_db_value(value: duckdb::types::ValueRef<'_>) -> DbValue {
    use duckdb::types::ValueRef;

    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Boolean(b) => DbValue::Integer(if b { 1 } else { 0 }),
        ValueRef::TinyInt(i) => DbValue::Integer(i as i64),
        ValueRef::SmallInt(i) => DbValue::Integer(i as i64),
        ValueRef::Int(i) => DbValue::Integer(i as i64),
        ValueRef::BigInt(i) => DbValue::Integer(i),
        ValueRef::HugeInt(i) => DbValue::Integer(i as i64),
        ValueRef::UTinyInt(i) => DbValue::Integer(i as i64),
        ValueRef::USmallInt(i) => DbValue::Integer(i as i64),
        ValueRef::UInt(i) => DbValue::Integer(i as i64),
        ValueRef::UBigInt(i) => DbValue::Integer(i as i64),
        ValueRef::Float(f) => DbValue::Real(f as f64),
        ValueRef::Double(f) => DbValue::Real(f),
        ValueRef::Decimal(d) => DbValue::Real(d.try_into().unwrap_or(0.0)),
        ValueRef::Timestamp(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Text(s) => DbValue::Text(String::from_utf8_lossy(s).to_string()),
        ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
        ValueRef::Date32(_) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Time64(_, _) => DbValue::Text(format!("{:?}", value)),
        ValueRef::Interval { .. } => DbValue::Text(format!("{:?}", value)),
        ValueRef::Enum(_, v) => DbValue::Integer(v as i64),
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

    fn query_all_string_rows(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> DelightQLResult<(Vec<String>, Vec<Vec<String>>)> {
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

        let column_names: Vec<String> = stmt.column_names();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut values = Vec::new();
                for idx in 0..column_names.len() {
                    let val = row.get_ref(idx)?;
                    let string_val = match val {
                        duckdb::types::ValueRef::Null => "NULL".to_string(),
                        duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                        duckdb::types::ValueRef::Blob(b) => format!("<blob {} bytes>", b.len()),
                        other => format!("{:?}", other),
                    };
                    values.push(string_val);
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

        Ok((column_names, result_rows))
    }
}

// Note: DatabaseConnectionExt is automatically implemented for DuckDBConnection
// via the blanket implementation in delightql_types::db_traits.
// The blanket impl provides query_row() and query() methods using query_row_values().
