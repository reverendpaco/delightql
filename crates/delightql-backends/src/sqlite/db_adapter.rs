// SQLite adapter implementing DelightQL database traits
//
// This module adapts rusqlite to work with DelightQL's DatabaseConnection trait.

use delightql_types::{DatabaseConnection, DbValue, Result as DelightQLResult};
use rusqlite::{Connection, Error as RusqliteError};
use std::sync::{Arc, Mutex};

/// Convert rusqlite ValueRef to DbValue
fn rusqlite_value_to_db_value(value: rusqlite::types::ValueRef<'_>) -> DbValue {
    use rusqlite::types::ValueRef;

    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(i) => DbValue::Integer(i),
        ValueRef::Real(f) => DbValue::Real(f),
        ValueRef::Text(s) => DbValue::Text(String::from_utf8_lossy(s).to_string()),
        ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
    }
}

/// Convert DbValue to rusqlite Value for parameter binding
fn db_value_to_rusqlite(value: &DbValue) -> rusqlite::types::Value {
    match value {
        DbValue::Null => rusqlite::types::Value::Null,
        DbValue::Integer(i) => rusqlite::types::Value::Integer(*i),
        DbValue::Real(f) => rusqlite::types::Value::Real(*f),
        DbValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
        DbValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
    }
}

/// SQLite database connection adapter
pub struct SqliteConnection {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteConnection {
    /// Create a new SQLite connection adapter
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        SqliteConnection { conn }
    }

    /// Create from a database path
    pub fn open(path: &str) -> DelightQLResult<Self> {
        let conn = Connection::open(path).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to open database", e.to_string())
        })?;

        Ok(SqliteConnection {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Get the underlying Arc<Mutex<Connection>>
    pub fn get_connection_arc(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

impl DatabaseConnection for SqliteConnection {
    fn execute(&self, sql: &str, params: &[DbValue]) -> DelightQLResult<usize> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(db_value_to_rusqlite).collect();

        let params_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
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

        Ok(conn.last_insert_rowid())
    }

    fn query_row_values(&self, sql: &str, params: &[DbValue]) -> DelightQLResult<Option<Vec<DbValue>>> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(db_value_to_rusqlite).collect();

        let params_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        match conn.query_row(sql, params_refs.as_slice(), |row| {
            let column_count = row.as_ref().column_count();
            let mut values = Vec::with_capacity(column_count);

            for i in 0..column_count {
                let value = row.get_ref(i).map_err(|e| {
                    RusqliteError::ToSqlConversionFailure(Box::new(
                        delightql_types::DelightQLError::database_error(
                            "Failed to get column value",
                            e.to_string()
                        )
                    ))
                })?;
                values.push(rusqlite_value_to_db_value(value));
            }

            Ok(values)
        }) {
            Ok(values) => Ok(Some(values)),
            Err(RusqliteError::QueryReturnedNoRows) => Ok(None),
            Err(RusqliteError::ToSqlConversionFailure(boxed)) => {
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

        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(db_value_to_rusqlite).collect();
        let params_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        let mut stmt = conn.prepare(sql).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to prepare query", e.to_string())
        })?;

        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut values = Vec::new();
                for idx in 0..column_names.len() {
                    let val: rusqlite::types::Value = row.get(idx)?;
                    let string_val = match val {
                        rusqlite::types::Value::Null => "NULL".to_string(),
                        rusqlite::types::Value::Integer(i) => i.to_string(),
                        rusqlite::types::Value::Real(f) => f.to_string(),
                        rusqlite::types::Value::Text(s) => s,
                        rusqlite::types::Value::Blob(b) => format!("<blob {} bytes>", b.len()),
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

    fn query_all_nullable_rows(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> DelightQLResult<(Vec<String>, Vec<Vec<Option<String>>>)> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error(
                "Connection mutex poisoned",
                e.to_string(),
            )
        })?;

        let rusqlite_params: Vec<rusqlite::types::Value> =
            params.iter().map(db_value_to_rusqlite).collect();
        let params_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params
            .iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        let mut stmt = conn.prepare(sql).map_err(|e| {
            delightql_types::DelightQLError::database_error("Failed to prepare query", e.to_string())
        })?;

        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut values = Vec::new();
                for idx in 0..column_names.len() {
                    let val: rusqlite::types::Value = row.get(idx)?;
                    let opt_val = match val {
                        rusqlite::types::Value::Null => None,
                        rusqlite::types::Value::Integer(i) => Some(i.to_string()),
                        rusqlite::types::Value::Real(f) => Some(f.to_string()),
                        rusqlite::types::Value::Text(s) => Some(s),
                        rusqlite::types::Value::Blob(b) => Some(format!("<blob {} bytes>", b.len())),
                    };
                    values.push(opt_val);
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

// Note: DatabaseConnectionExt is automatically implemented for SqliteConnection
// via the blanket implementation in delightql_types::db_traits.
// The blanket impl provides query_row() and query() methods using query_row_values().
