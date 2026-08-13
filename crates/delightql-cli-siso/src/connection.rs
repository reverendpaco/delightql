// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

    /// One coprocess field read as a value.
    ///
    /// A coprocess speaks text, so its null is whatever spelling the
    /// profile told it to print (`.nullvalue`, `\pset null`). That
    /// spelling is the ONLY thing that may mean absence here, and text
    /// that happens to match it is indistinguishable from it — a
    /// property of the wire the profile chose, not a decision this
    /// connection is free to make differently in two places.
    fn field(&self, text: String) -> DbValue {
        if text == self.shared.profile().null_value {
            DbValue::Null
        } else {
            DbValue::Text(text)
        }
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
        let (_columns, mut rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe query failed: {}", e),
                e.to_string(),
            )
        })?;

        if rows.is_empty() {
            return Ok(None);
        }

        let values: Vec<DbValue> = rows
            .swap_remove(0)
            .into_iter()
            .map(|v| self.field(v))
            .collect();

        Ok(Some(values))
    }

    fn query_all_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<DbValue>>)> {
        let (cols, rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Pipe query failed: {}", e),
                e.to_string(),
            )
        })?;
        let typed_rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| self.field(v)).collect())
            .collect();
        Ok((cols, typed_rows))
    }
}

// Safety: PipeConnection holds Arc<SharedCoprocess> which is Send+Sync
unsafe impl Send for PipeConnection {}
unsafe impl Sync for PipeConnection {}
