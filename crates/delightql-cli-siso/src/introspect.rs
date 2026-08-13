// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use std::sync::Arc;

use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};

use crate::coprocess::SharedCoprocess;
use crate::metadata::{
    decode_discovery, decode_relation_columns, decode_single_query, PipeMetadataSource,
};
use crate::profile::IntrospectionMode;

/// Introspector that discovers tables and columns through a pipe coprocess.
///
/// Dispatches on the profile's `IntrospectionMode`:
/// - `SingleQuery`: one SQL returns all tables + columns (e.g. sqlite3)
/// - `TwoPhase`: discovery query + per-table PRAGMA (e.g. osqueryi)
/// - `None`: returns empty
pub struct PipeIntrospector<S = SharedCoprocess> {
    source: Arc<S>,
    introspection: IntrospectionMode,
}

impl PipeIntrospector<SharedCoprocess> {
    pub fn new(shared: Arc<SharedCoprocess>) -> Self {
        Self {
            introspection: shared.profile().introspection.clone(),
            source: shared,
        }
    }
}

impl<S> PipeIntrospector<S> {
    #[cfg(test)]
    pub(crate) fn with_source(source: Arc<S>, introspection: IntrospectionMode) -> Self {
        Self {
            source,
            introspection,
        }
    }

    /// SingleQuery mode: one SQL returns (table_name, table_type, cid, col_name, col_type, notnull).
    fn introspect_single_query(&self, sql: &str) -> delightql_types::Result<Vec<DiscoveredEntity>>
    where
        S: PipeMetadataSource,
    {
        let raw = self.source.query_metadata(sql).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection query failed",
                e.to_string(),
            )
        })?;
        decode_single_query(&raw).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection metadata is malformed",
                e.to_string(),
            )
        })
    }

    /// TwoPhase mode: discovery query lists table names, then PRAGMA table_info per table.
    fn introspect_two_phase(
        &self,
        discovery_sql: &str,
        has_type_column: bool,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>>
    where
        S: PipeMetadataSource,
    {
        let raw = self.source.query_metadata(discovery_sql).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection discovery query failed",
                e.to_string(),
            )
        })?;
        let tables = decode_discovery(&raw, has_type_column).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection discovery metadata is malformed",
                e.to_string(),
            )
        })?;
        let mut entities = Vec::with_capacity(tables.len());
        for (table_name, entity_type_id) in tables {
            let pragma_sql = format!("PRAGMA table_info({})", table_name);
            let columns = self.source.query_metadata(&pragma_sql).map_err(|e| {
                delightql_types::error::DelightQLError::database_error(
                    format!("Pipe introspection table metadata query failed for '{table_name}'"),
                    e.to_string(),
                )
            })?;
            let attributes = decode_relation_columns(&columns, &table_name).map_err(|e| {
                delightql_types::error::DelightQLError::database_error(
                    format!("Pipe introspection table metadata is malformed for '{table_name}'"),
                    e.to_string(),
                )
            })?;
            entities.push(DiscoveredEntity {
                name: table_name.into(),
                entity_type_id,
                attributes,
            });
        }
        Ok(entities)
    }
}

impl<S> DatabaseIntrospector for PipeIntrospector<S>
where
    S: PipeMetadataSource,
{
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        match &self.introspection {
            IntrospectionMode::None => Ok(vec![]),
            IntrospectionMode::SingleQuery(sql) => self.introspect_single_query(sql),
            IntrospectionMode::TwoPhase {
                discovery_sql,
                has_type_column,
            } => self.introspect_two_phase(discovery_sql, *has_type_column),
        }
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        // Pipe connections don't support ATTACH / schemas
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::error::{PipeError, Result};
    use crate::metadata::RawPipeTable;

    struct CannedMetadataSource {
        answers: Mutex<Vec<(String, RawPipeTable)>>,
    }

    impl CannedMetadataSource {
        fn new(answers: Vec<(&str, RawPipeTable)>) -> Self {
            Self {
                answers: Mutex::new(
                    answers
                        .into_iter()
                        .map(|(sql, table)| (sql.to_string(), table))
                        .collect(),
                ),
            }
        }
    }

    impl PipeMetadataSource for CannedMetadataSource {
        fn query_metadata(&self, sql: &str) -> Result<RawPipeTable> {
            let mut answers = self.answers.lock().unwrap();
            let index = answers
                .iter()
                .position(|(expected, _)| expected == sql)
                .ok_or_else(|| PipeError::QueryFailed(format!("unexpected metadata SQL: {sql}")))?;
            Ok(answers.swap_remove(index).1)
        }
    }

    fn raw(columns: &[&str], rows: Vec<Vec<&str>>) -> RawPipeTable {
        RawPipeTable {
            columns: columns.iter().map(|value| (*value).to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(str::to_string).collect())
                .collect(),
        }
    }

    #[test]
    fn canned_source_exercises_two_phase_introspection_without_a_child_process() {
        let discovery_sql = "SELECT name FROM registry";
        let pragma_sql = "PRAGMA table_info(users)";
        let source = Arc::new(CannedMetadataSource::new(vec![
            (discovery_sql, raw(&["name"], vec![vec!["users"]])),
            (
                pragma_sql,
                raw(
                    &["cid", "name", "type", "notnull"],
                    vec![vec!["0", "id", "INTEGER", "1"]],
                ),
            ),
        ]));
        let introspector = PipeIntrospector::with_source(
            source,
            IntrospectionMode::TwoPhase {
                discovery_sql: discovery_sql.to_string(),
                has_type_column: false,
            },
        );

        let entities = introspector.introspect_entities().unwrap();
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].name.as_str(), "users");
        assert_eq!(entities[0].attributes[0].name.as_str(), "id");
        assert_eq!(entities[0].attributes[0].position, 0);
        assert!(!entities[0].attributes[0].is_nullable);
    }

    #[test]
    fn canned_source_exercises_single_query_introspection_without_a_child_process() {
        let sql = "SELECT metadata";
        let source = Arc::new(CannedMetadataSource::new(vec![(
            sql,
            raw(
                &[
                    "table_name",
                    "table_type",
                    "cid",
                    "col_name",
                    "col_type",
                    "notnull",
                ],
                vec![
                    vec!["users", "BASE TABLE", "0", "id", "INTEGER", "1"],
                    vec!["users", "BASE TABLE", "1", "name", "VARCHAR", "0"],
                ],
            ),
        )]));
        let introspector =
            PipeIntrospector::with_source(source, IntrospectionMode::SingleQuery(sql.to_string()));

        let entities = introspector.introspect_entities().unwrap();
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].entity_type_id, 10);
        assert_eq!(entities[0].attributes.len(), 2);
        assert_eq!(entities[0].attributes[1].name.as_str(), "name");
        assert!(entities[0].attributes[1].is_nullable);
    }
}
