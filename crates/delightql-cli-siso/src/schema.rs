// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use std::sync::Arc;

use delightql_types::schema::{ColumnInfo, DatabaseSchema};
use delightql_types::{DelightQLError, Result};

use crate::coprocess::SharedCoprocess;
use crate::profile::SchemaMode;

/// Schema provider that queries table metadata through the pipe.
///
/// Uses `PRAGMA table_info(table_name)` which works for both SQLite and osquery.
pub struct PipeSchema {
    shared: Arc<SharedCoprocess>,
}

impl PipeSchema {
    pub fn new(shared: Arc<SharedCoprocess>) -> Self {
        Self { shared }
    }
}

impl DatabaseSchema for PipeSchema {
    fn get_table_columns(
        &self,
        _schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<Vec<ColumnInfo>>> {
        let sql = match &self.shared.profile().schema_mode {
            SchemaMode::Pragma => format!("PRAGMA table_info({})", table_name),
            SchemaMode::Query(template) => template.replace("{table}", table_name),
        };
        let (columns, rows) = self.shared.execute_query_raw(&sql).map_err(|error| {
            DelightQLError::database_error("Pipe schema query failed", error.to_string())
        })?;

        if rows.is_empty() {
            return Ok(None);
        }

        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        // Find column indices by name
        let Some(name_idx) = columns.iter().position(|c| c.eq_ignore_ascii_case("name")) else {
            return Err(DelightQLError::database_error(
                "Pipe schema metadata is malformed",
                "missing required column 'name'",
            ));
        };
        let notnull_idx = columns.iter().position(|c| c.eq_ignore_ascii_case("notnull"));
        let cid_idx = columns.iter().position(|c| c.eq_ignore_ascii_case("cid"));

        let column_infos: Vec<ColumnInfo> = rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let name = row.get(name_idx).cloned().unwrap_or_default();
                let nullable = notnull_idx
                    .and_then(|idx| row.get(idx))
                    .map(|v| v == "0")
                    .unwrap_or(true);
                let position = cid_idx
                    .and_then(|idx| row.get(idx))
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(i);

                ColumnInfo {
                    name: name.into(),
                    nullable,
                    position,
                    declared_type: None,
                }
            })
            .collect();

        Ok(Some(column_infos))
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        Ok(self.get_table_columns(schema, table_name)?.is_some())
    }
}

// Safety: PipeSchema holds Arc<SharedCoprocess> which is Send+Sync
unsafe impl Send for PipeSchema {}
unsafe impl Sync for PipeSchema {}
