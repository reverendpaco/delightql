//! WASM-compatible DelightQL System
//!
//! This module provides a minimal system implementation for WASM builds.
//! It doesn't use rusqlite and instead relies on the JavaScript bridge for database access.

use crate::bin_cartridge::registry::BinCartridgeRegistry;
use crate::error::{DelightQLError, Result};
use delightql_types::{ColumnInfo, DatabaseConnection, DatabaseSchema, NamespacePath};
use std::sync::{Arc, Mutex};

/// Result of a `consult_file` operation.
pub(crate) struct ConsultResult {
    /// Number of definitions loaded.
    pub definitions_loaded: usize,
    /// Entity names that were replaced (non-empty only for inline DDL drop-and-replace).
    pub replaced_entities: Vec<String>,
}

/// Connection-backed database schema for WASM.
///
/// Implements DatabaseSchema by routing PRAGMA and sqlite_master queries
/// through the stored connection (which calls bridge_sql on the JS side).
struct ConnectionBackedSchema {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
}

impl DatabaseSchema for ConnectionBackedSchema {
    fn get_table_columns(&self, schema: Option<&str>, table_name: &str) -> Option<Vec<ColumnInfo>> {
        let sql = match schema {
            Some(s) => format!("PRAGMA {}.table_info({})", s, table_name),
            None => format!("PRAGMA table_info({})", table_name),
        };
        let conn = self.connection.lock().ok()?;
        let (columns, rows) = conn.query_all_nullable_rows(&sql, &[]).ok()?;
        if rows.is_empty() {
            return None;
        }

        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        let name_idx = columns.iter().position(|c| c == "name")?;
        let notnull_idx = columns.iter().position(|c| c == "notnull")?;

        let cols: Vec<ColumnInfo> = rows
            .iter()
            .enumerate()
            .map(|(pos, row)| {
                let name = row
                    .get(name_idx)
                    .and_then(|v| v.as_deref())
                    .unwrap_or("")
                    .to_string();
                let notnull = row
                    .get(notnull_idx)
                    .and_then(|v| v.as_deref())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                ColumnInfo {
                    name: name.into(),
                    nullable: notnull == 0,
                    position: pos,
                }
            })
            .collect();

        Some(cols)
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> bool {
        let sql = match schema {
            Some(s) => format!(
                "SELECT 1 FROM {}.sqlite_master WHERE type='table' AND name='{}'",
                s, table_name
            ),
            None => format!(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}'",
                table_name
            ),
        };
        let conn = match self.connection.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        match conn.query_all_nullable_rows(&sql, &[]) {
            Ok((_, rows)) => !rows.is_empty(),
            Err(_) => false,
        }
    }
}

/// Minimal WASM-compatible system state
///
/// Unlike the full DelightQLSystem, this doesn't use rusqlite for internal metadata.
/// All database operations go through the JavaScript bridge.
pub(crate) struct DelightQLSystem {
    /// User database connection (JavaScript bridge)
    pub connection: Arc<Mutex<dyn DatabaseConnection>>,

    /// Database schema provider (injected)
    schema: Option<Box<dyn DatabaseSchema>>,

    /// Bin cartridge registry for built-in entities
    bin_registry: Arc<BinCartridgeRegistry>,

    /// Flag: is namespace resolver authoritative?
    pub namespace_authoritative: bool,
}

impl DelightQLSystem {
    /// Create a new WASM DelightQL system
    pub fn new(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        _introspector: Box<dyn delightql_types::DatabaseIntrospector>,
        _db_type: &str,
    ) -> Result<Self> {
        // Initialize bin cartridge registry
        let mut bin_registry = BinCartridgeRegistry::new();

        // Register the prelude cartridge
        bin_registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());

        // Register the predicates cartridge
        bin_registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());

        let schema = Box::new(ConnectionBackedSchema {
            connection: connection.clone(),
        });

        Ok(DelightQLSystem {
            connection,
            schema: Some(schema),
            bin_registry: Arc::new(bin_registry),
            namespace_authoritative: false,
        })
    }

    /// Create a new WASM system with injected schema
    pub fn new_with_schema(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn delightql_types::DatabaseIntrospector>,
        db_type: &str,
        schema: Box<dyn DatabaseSchema>,
    ) -> Result<Self> {
        let mut system = Self::new(connection, introspector, db_type)?;
        system.schema = Some(schema);
        Ok(system)
    }

    /// Get the database schema
    pub fn get_schema(&self) -> Result<&dyn DatabaseSchema> {
        self.schema
            .as_ref()
            .ok_or_else(|| {
                DelightQLError::validation_error(
                    "No database schema configured",
                    "Use DelightQLSystem::new_with_schema() to inject a schema",
                )
            })
            .map(|boxed| boxed.as_ref())
    }

    /// Set schema after construction
    pub fn set_schema(&mut self, schema: Box<dyn DatabaseSchema>) {
        self.schema = Some(schema);
    }

    /// Get the bin cartridge registry
    pub fn bin_registry(&self) -> Arc<BinCartridgeRegistry> {
        Arc::clone(&self.bin_registry)
    }

    /// Get connection for a given connection_id (WASM always returns user connection)
    pub fn get_connection(
        &self,
        _connection_id: i64,
    ) -> Result<Arc<Mutex<dyn DatabaseConnection>>> {
        Ok(Arc::clone(&self.connection))
    }

    /// Mount a database - not supported in WASM
    pub fn mount_database(&mut self, _db_path: &str, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "mount!() not supported in WASM",
            "Database mounting is only available in native builds",
        ))
    }

    /// Enlist namespace - not supported in WASM
    pub fn enlist_namespace(&mut self, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "enlist!() not supported in WASM",
            "Namespace enlistment is only available in native builds",
        ))
    }

    /// Delist namespace - not supported in WASM
    pub fn delist_namespace(&mut self, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "delist!() not supported in WASM",
            "Namespace delisting is only available in native builds",
        ))
    }

    /// Unmount database - not supported in WASM
    pub fn unmount_database(&mut self, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "unmount!() not supported in WASM",
            "Database unmounting is only available in native builds",
        ))
    }

    /// Unconsult namespace - not supported in WASM
    pub fn unconsult_namespace(&mut self, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "unconsult!() not supported in WASM",
            "Namespace unconsulting is only available in native builds",
        ))
    }

    /// Refresh namespace - not supported in WASM
    pub fn refresh_namespace(&mut self, _namespace: &str) -> Result<usize> {
        Err(DelightQLError::validation_error(
            "refresh!() not supported in WASM",
            "Namespace refresh is only available in native builds",
        ))
    }

    /// Reconsult namespace - not supported in WASM
    pub fn reconsult_namespace(
        &mut self,
        _namespace: &str,
        _new_file: Option<&str>,
    ) -> Result<usize> {
        Err(DelightQLError::validation_error(
            "reconsult!() not supported in WASM",
            "Namespace reconsulting is only available in native builds",
        ))
    }

    /// Register namespace alias - not supported in WASM
    pub fn register_namespace_alias(&mut self, _alias: &str, _namespace: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "Namespace aliases not supported in WASM",
            "Namespace alias registration is only available in native builds",
        ))
    }

    /// Consult file - not supported in WASM
    pub fn consult_file(
        &mut self,
        _path: &str,
        _namespace: &str,
        _ddl: crate::pipeline::parser::DDLFile,
    ) -> Result<ConsultResult> {
        Err(DelightQLError::validation_error(
            "consult!() not supported in WASM",
            "File consultation is only available in native builds",
        ))
    }

    /// Imprint namespace - not supported in WASM
    pub fn imprint_namespace(
        &mut self,
        _source_ns: &str,
        _target_ns: &str,
    ) -> Result<Vec<(String, String, String)>> {
        Err(DelightQLError::validation_error(
            "imprint!() not supported in WASM",
            "Companion table materialization is only available in native builds",
        ))
    }

    /// Resolve namespace path - WASM returns None (no namespace support)
    pub fn resolve_namespace_path(
        &self,
        _path: &NamespacePath,
    ) -> Result<Option<(Option<String>, i64)>> {
        // WASM doesn't have namespace resolution - return main schema
        Ok(Some((None, 2))) // connection_id=2 is user connection
    }

    /// Resolve unqualified entity - WASM returns None (no namespace support)
    pub fn resolve_unqualified_entity(
        &self,
        _entity_name: &str,
        _current_namespace: &str,
        _fallback_namespace: Option<&str>,
    ) -> Result<Option<(NamespacePath, delightql_types::SqlIdentifier)>> {
        // WASM doesn't support unqualified entity resolution from bootstrap
        Ok(None)
    }

    /// Reinit bootstrap - not supported in WASM (no bootstrap database)
    pub fn reinit_bootstrap(&mut self) -> Result<()> {
        Err(DelightQLError::validation_error(
            "reinit_bootstrap not supported in WASM",
            "Bootstrap database is only available in native builds",
        ))
    }

    /// Get canonical entity name - WASM returns None (no bootstrap)
    pub fn get_canonical_entity_name(
        &self,
        _namespace_fq: &str,
        _entity_name: &str,
    ) -> Result<Option<delightql_types::SqlIdentifier>> {
        Ok(None)
    }

    /// Save enlisted state - WASM no-op (no bootstrap)
    pub fn save_enlisted_state(&self) -> Result<Vec<(i32, i32)>> {
        Ok(Vec::new())
    }

    /// Save alias state - WASM no-op (no bootstrap)
    pub fn save_alias_state(&self) -> Result<Vec<(String, i32)>> {
        Ok(Vec::new())
    }

    /// Restore enlisted state - WASM no-op (no bootstrap)
    pub fn restore_enlisted_state(&mut self, _saved: &[(i32, i32)]) -> Result<()> {
        Ok(())
    }

    /// Restore alias state - WASM no-op (no bootstrap)
    pub fn restore_alias_state(&mut self, _saved: &[(String, i32)]) -> Result<()> {
        Ok(())
    }

    /// Record namespace-local enlists - WASM no-op
    pub fn record_namespace_local_enlists(
        &mut self,
        _namespace: &str,
        _new_enlists: &[(i32, i32)],
    ) -> Result<()> {
        Ok(())
    }

    /// Record namespace-local aliases - WASM no-op
    pub fn record_namespace_local_aliases(
        &mut self,
        _namespace: &str,
        _new_aliases: &[(String, i32)],
    ) -> Result<()> {
        Ok(())
    }

    /// Ground namespace - not supported in WASM
    pub fn ground_namespace(
        &mut self,
        _data_ns: &str,
        _lib_ns: &str,
        _new_ns_name: &str,
    ) -> Result<usize> {
        Err(DelightQLError::validation_error(
            "ground_namespace not supported in WASM",
            "Namespace grounding is only available in native builds",
        ))
    }

    /// Expose namespace - not supported in WASM
    pub fn expose_namespace(&mut self, _exposing_ns: &str, _exposed_ns: &str) -> Result<()> {
        Err(DelightQLError::validation_error(
            "expose_namespace not supported in WASM",
            "Namespace exposure is only available in native builds",
        ))
    }

    /// Get bootstrap connection - not available in WASM
    pub fn get_bootstrap_connection(&self) -> Arc<Mutex<dyn DatabaseConnection>> {
        // WASM has no bootstrap — return the user connection as fallback
        Arc::clone(&self.connection)
    }

    /// Get schema map - WASM returns empty map
    pub fn get_schema_map(&self) -> &std::collections::HashMap<i64, Box<dyn DatabaseSchema>> {
        // WASM has no schema map — use a static empty map
        static EMPTY: std::sync::LazyLock<std::collections::HashMap<i64, Box<dyn DatabaseSchema>>> =
            std::sync::LazyLock::new(std::collections::HashMap::new);
        &EMPTY
    }
}
