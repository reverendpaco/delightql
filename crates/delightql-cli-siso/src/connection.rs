use std::sync::Arc;

use delightql_types::db_traits::{DatabaseConnection, DbValue};
use delightql_types::error::{DelightQLError, Result};

use crate::coprocess::SharedCoprocess;

/// A database connection backed by a coprocess pipe.
///
/// All values are returned as `DbValue::Text` (or `DbValue::Null`).
/// Parameters are ignored — SQL must have values inlined.
pub struct PipeConnection {
    shared: Arc<SharedCoprocess>,
}

impl PipeConnection {
    pub fn new(shared: Arc<SharedCoprocess>) -> Self {
        Self { shared }
    }
}

impl DatabaseConnection for PipeConnection {
    fn execute(&self, sql: &str, _params: &[DbValue]) -> Result<usize> {
        let (_columns, rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe execution failed: {}", e),
                e.to_string(),
            )
        })?;
        Ok(rows.len())
    }

    fn last_insert_rowid(&self) -> Result<i64> {
        Ok(0)
    }

    fn query_row_values(&self, sql: &str, _params: &[DbValue]) -> Result<Option<Vec<DbValue>>> {
        let (_columns, rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe query failed: {}", e),
                e.to_string(),
            )
        })?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        let values: Vec<DbValue> = row
            .iter()
            .map(|v| {
                if v.is_empty() || v == "NULL" {
                    DbValue::Null
                } else {
                    DbValue::Text(v.clone())
                }
            })
            .collect();

        Ok(Some(values))
    }

    fn query_all_string_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe query failed: {}", e),
                e.to_string(),
            )
        })
    }

    fn query_all_nullable_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        let (cols, rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe query failed: {}", e),
                e.to_string(),
            )
        })?;
        let nullable_rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| {
                        if v.is_empty() {
                            None
                        } else {
                            Some(v)
                        }
                    })
                    .collect()
            })
            .collect();
        Ok((cols, nullable_rows))
    }
}

// Safety: PipeConnection holds Arc<SharedCoprocess> which is Send+Sync
unsafe impl Send for PipeConnection {}
unsafe impl Sync for PipeConnection {}
