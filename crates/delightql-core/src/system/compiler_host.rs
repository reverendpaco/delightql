// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The native implementation of the compiler host contracts in `crate::host`.

use super::{DelightQLSystem, RealCreatedObjectCatalog};
use crate::diagnostic::{Runtime, Sqlite, SqliteNative};
use crate::error::{DelightQLError, Result};
use crate::external_effects::RegistrationOutcome;
use rusqlite::OptionalExtension;
use std::sync::Arc;

fn session_catalog_engine_error(operation: &str, error: rusqlite::Error) -> DelightQLError {
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            let text =
                delightql_types::teach_runtime_message(message.unwrap_or_else(|| code.to_string()));
            let message = format!("{operation}: {text}");
            match SqliteNative::new(code.extended_code, message.clone()) {
                Some(native) => Sqlite::Native(native).into(),
                None => Sqlite::Engine { message }.into(),
            }
        }
        other => Sqlite::Engine {
            message: format!("{operation}: {other}"),
        }
        .into(),
    }
}

fn session_catalog_value(value: rusqlite::types::Value) -> delightql_types::DbValue {
    use delightql_types::DbValue;
    match value {
        rusqlite::types::Value::Null => DbValue::Null,
        rusqlite::types::Value::Integer(value) => DbValue::Integer(value),
        rusqlite::types::Value::Real(value) => DbValue::Real(value),
        rusqlite::types::Value::Text(value) => DbValue::Text(value),
        rusqlite::types::Value::Blob(value) => DbValue::Blob(value),
    }
}

impl crate::host::CompilerHost for DelightQLSystem {
    fn capabilities(&self) -> crate::host::HostCapabilities {
        self.capabilities
    }

    fn stated_dialect(&self) -> Result<Option<crate::pipeline::generator::SqlDialect>> {
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock to read the dialect")?;
        crate::settings::stated_dialect(&conn)
    }

    fn connection_dialects(&self) -> Result<Vec<crate::pipeline::generator::SqlDialect>> {
        // The ids are read under the lock and released before each is
        // judged: the per-connection judgment takes the lock itself.
        let ids: Vec<i64> = {
            let conn =
                self.lock_bootstrap("Failed to acquire bootstrap lock to list connections")?;
            let mut statement = conn
                .prepare("SELECT id FROM connection ORDER BY id")
                .map_err(|e| Runtime::catalog("list connections", e.to_string()))?;
            let ids = statement
                .query_map([], |row| row.get(0))
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<i64>>>())
                .map_err(|e| Runtime::catalog("list connections", e.to_string()))?;
            ids
        };
        let mut dialects = vec![DelightQLSystem::dialect_for_connection(self, None)];
        for id in ids {
            let dialect = DelightQLSystem::dialect_for_connection(self, Some(id));
            if !dialects.contains(&dialect) {
                dialects.push(dialect);
            }
        }
        Ok(dialects)
    }

    fn aggregate_catalog(
        &self,
    ) -> Result<Arc<crate::pipeline::aggregate_catalog::AggregateCatalog>> {
        use crate::pipeline::aggregate_catalog::{select_rows, AggregateCatalog, AggregateRow};
        let rows = {
            let conn =
                self.lock_bootstrap("Failed to acquire bootstrap lock for the aggregate catalog")?;
            let mut statement = conn
                .prepare_cached(&select_rows("aggregates"))
                .map_err(|e| Runtime::catalog("read the aggregate catalog", e.to_string()))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(AggregateRow {
                        dialect: row.get(0)?,
                        functor_name: row.get(1)?,
                        arity: row.get(2)?,
                        can_be_globbed: row.get(3)?,
                        can_be_windowed: row.get(4)?,
                    })
                })
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(|e| Runtime::catalog("read the aggregate catalog", e.to_string()))?;
            rows
        };
        Ok(Arc::new(AggregateCatalog::from_rows(rows)?))
    }

    fn type_classes(&self) -> Result<Arc<crate::pipeline::type_classes::TypeClasses>> {
        use crate::pipeline::type_classes::{select_rows, TypeClassRow, TypeClasses};
        let rows = {
            let conn =
                self.lock_bootstrap("Failed to acquire bootstrap lock for the type classes")?;
            let mut statement = conn
                .prepare_cached(&select_rows("type_classes"))
                .map_err(|e| Runtime::catalog("read the type classes", e.to_string()))?;
            let rows = statement
                .query_map([], |row| {
                    Ok(TypeClassRow {
                        dialect: row.get(0)?,
                        reads: row.get(1)?,
                        type_name: row.get(2)?,
                        class: row.get(3)?,
                    })
                })
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(|e| Runtime::catalog("read the type classes", e.to_string()))?;
            rows
        };
        Ok(Arc::new(TypeClasses::from_rows(rows)?))
    }

    fn dialect_pack(&self) -> Result<Arc<crate::pipeline::dialect_pack::DialectPack>> {
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock for dialect pack")?;
        let pack = crate::pipeline::dialect_pack::DialectPack::load(&conn).map_err(|error| {
            Runtime::catalog(
                format!("Failed to load dialect pack: {error}"),
                error.to_string(),
            )
        })?;
        Ok(Arc::new(pack))
    }

    fn definition_catalog(
        &self,
        operation: &str,
    ) -> Result<Box<dyn crate::definition_catalog::DefinitionCatalog + '_>> {
        let guard = self.lock_bootstrap(operation)?;
        Ok(Box::new(
            crate::definition_catalog::LockedDefinitionCatalog::new(guard),
        ))
    }

    fn namespace_kind(&self, namespace: &str) -> Result<Option<crate::namespace::NamespaceKind>> {
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock")?;
        let kind: Option<Option<String>> = conn
            .query_row(
                "SELECT kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                Runtime::catalog("Failed to read namespace catalog", error.to_string())
            })?;
        kind.map(|kind| crate::namespace::NamespaceKind::decode(namespace, kind.as_deref()))
            .transpose()
    }

    fn query_session_catalog(&self, sql: &str) -> Result<crate::host::HostQueryResult> {
        let conn = self.lock_bootstrap("Bootstrap lock")?;
        let mut statement = conn
            .prepare(sql)
            .map_err(|error| session_catalog_engine_error("Bootstrap prepare", error))?;
        let column_count = statement.column_count();
        let columns = statement
            .column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();
        let declared = statement
            .columns()
            .iter()
            .map(|column| column.decl_type().map(str::to_string))
            .collect();
        let rows = statement
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for index in 0..column_count {
                    values.push(session_catalog_value(row.get(index)?));
                }
                Ok(values)
            })
            .map_err(|error| session_catalog_engine_error("Bootstrap query", error))?;
        let rows = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| session_catalog_engine_error("Bootstrap fetch", error))?;
        Ok(crate::host::HostQueryResult {
            columns,
            declared,
            rows,
        })
    }
}

impl crate::host::CompilerExecutionHost for DelightQLSystem {
    fn register_prompt_blocks(
        &mut self,
        blocks: Vec<crate::pipeline::ast_unresolved::InlineDdlSpec>,
    ) -> Result<()> {
        crate::pipeline::inline_ddl::register_prompt_blocks(blocks, self)
    }

    fn set_entity_docs_atomic(
        &mut self,
        entries: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        DelightQLSystem::set_entity_docs_atomic(self, entries)
    }

    fn observe_effect_plan(
        &mut self,
        plan: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()> {
        DelightQLSystem::materialize_effect_plan(self, plan)
    }

    fn reconcile_created_objects(
        &mut self,
        objects: &[crate::pipeline::compiled_query::PlanCreatedObject],
    ) -> Result<crate::host::CreatedObjectReconciliation> {
        let outcomes =
            self.register_run_created_objects_with(objects, &RealCreatedObjectCatalog)?;
        Ok(outcomes
            .into_iter()
            .find_map(|outcome| match outcome {
                RegistrationOutcome::Unsupported { reason } => Some(
                    crate::host::CreatedObjectReconciliation::Unsupported(reason),
                ),
                _ => None,
            })
            .unwrap_or(crate::host::CreatedObjectReconciliation::Complete))
    }
}
