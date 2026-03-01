use std::sync::Arc;

use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};

use crate::coprocess::SharedCoprocess;
use crate::profile::IntrospectionMode;

/// Introspector that discovers tables and columns through a pipe coprocess.
///
/// Dispatches on the profile's `IntrospectionMode`:
/// - `SingleQuery`: one SQL returns all tables + columns (e.g. sqlite3)
/// - `TwoPhase`: discovery query + per-table PRAGMA (e.g. osqueryi)
/// - `None`: returns empty
pub struct PipeIntrospector {
    shared: Arc<SharedCoprocess>,
}

impl PipeIntrospector {
    pub fn new(shared: Arc<SharedCoprocess>) -> Self {
        Self { shared }
    }

    /// SingleQuery mode: one SQL returns (table_name, table_type, cid, col_name, col_type, notnull).
    fn introspect_single_query(
        &self,
        sql: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        let (_columns, rows) = self.shared.execute_query_raw(sql).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection query failed",
                e.to_string(),
            )
        })?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Expected columns: table_name(0), table_type(1), cid(2), col_name(3), col_type(4), notnull(5)
        let mut entities: Vec<DiscoveredEntity> = Vec::new();
        let mut current_name: Option<String> = None;
        let mut current_attrs: Vec<DiscoveredAttribute> = Vec::new();
        let mut current_type_id: i32 = 10;

        for row in &rows {
            let table_name = row.get(0).cloned().unwrap_or_default();
            let table_type = row.get(1).cloned().unwrap_or_default();
            let cid: i32 = row
                .get(2)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let col_name = row.get(3).cloned().unwrap_or_default();
            let col_type = row.get(4).cloned().unwrap_or_default();
            let notnull: bool = row
                .get(5)
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

            let type_id = if table_type.eq_ignore_ascii_case("view") { 11 } else { 10 };

            if current_name.as_deref() != Some(&table_name) {
                // Flush previous entity
                if let Some(name) = current_name.take() {
                    entities.push(DiscoveredEntity {
                        name: name.into(),
                        entity_type_id: current_type_id,
                        attributes: std::mem::take(&mut current_attrs),
                    });
                }
                current_name = Some(table_name);
                current_type_id = type_id;
            }

            current_attrs.push(DiscoveredAttribute {
                name: col_name.into(),
                data_type: col_type,
                position: cid,
                is_nullable: !notnull,
            });
        }

        // Flush last entity
        if let Some(name) = current_name {
            entities.push(DiscoveredEntity {
                name: name.into(),
                entity_type_id: current_type_id,
                attributes: current_attrs,
            });
        }

        Ok(entities)
    }

    /// TwoPhase mode: discovery query lists table names, then PRAGMA table_info per table.
    fn introspect_two_phase(
        &self,
        discovery_sql: &str,
        has_type_column: bool,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        let (_columns, rows) = self.shared.execute_query_raw(discovery_sql).map_err(|e| {
            delightql_types::error::DelightQLError::database_error(
                "Pipe introspection discovery query failed",
                e.to_string(),
            )
        })?;

        if rows.is_empty() {
            return Ok(vec![]);
        }

        let mut entities = Vec::new();

        for row in &rows {
            let table_name = row.get(0).cloned().unwrap_or_default();
            let entity_type_id = if has_type_column {
                let table_type = row.get(1).cloned().unwrap_or_default();
                if table_type.eq_ignore_ascii_case("view") { 11 } else { 10 }
            } else {
                10 // default to table
            };

            // Query columns via PRAGMA
            let pragma_sql = format!("PRAGMA table_info({})", table_name);
            let (pragma_cols, pragma_rows) =
                self.shared.execute_query_raw(&pragma_sql).map_err(|e| {
                    delightql_types::error::DelightQLError::database_error(
                        format!(
                            "Pipe introspection PRAGMA table_info({}) failed",
                            table_name
                        ),
                        e.to_string(),
                    )
                })?;

            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            let name_idx = pragma_cols.iter().position(|c| c == "name").unwrap_or(1);
            let type_idx = pragma_cols.iter().position(|c| c == "type").unwrap_or(2);
            let notnull_idx = pragma_cols.iter().position(|c| c == "notnull").unwrap_or(3);
            let cid_idx = pragma_cols.iter().position(|c| c == "cid").unwrap_or(0);

            let attributes: Vec<DiscoveredAttribute> = pragma_rows
                .iter()
                .map(|prow| {
                    let col_name = prow.get(name_idx).cloned().unwrap_or_default();
                    let col_type = prow.get(type_idx).cloned().unwrap_or_default();
                    let notnull = prow
                        .get(notnull_idx)
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    let cid: i32 = prow
                        .get(cid_idx)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);

                    DiscoveredAttribute {
                        name: col_name.into(),
                        data_type: col_type,
                        position: cid,
                        is_nullable: !notnull,
                    }
                })
                .collect();

            entities.push(DiscoveredEntity {
                name: table_name.into(),
                entity_type_id,
                attributes,
            });
        }

        Ok(entities)
    }
}

impl DatabaseIntrospector for PipeIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        match &self.shared.profile().introspection {
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

// Safety: PipeIntrospector holds Arc<SharedCoprocess> which is Send+Sync
unsafe impl Send for PipeIntrospector {}
unsafe impl Sync for PipeIntrospector {}
