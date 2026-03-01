/// DatabaseSchema implementation backed by bootstrap metadata.
///
/// Instead of querying the live database connection (via PRAGMA table_xinfo),
/// this reads column information from the bootstrap's `entity` + `entity_attribute`
/// tables. This is the single source of truth after mount! introspects a database.
///
/// Advantages:
/// - No coupling to backend-specific types (no DynamicSqliteSchema dependency)
/// - Works identically for SQLite, DuckDB, and future backends
/// - Schema information arrives via mount!, not at open() time
use delightql_types::schema::{ColumnInfo, DatabaseSchema};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Schema provider that reads from bootstrap metadata tables.
///
/// Queries the `entity_attribute` table joined through `activated_entity`
/// and `namespace` to find columns for a given table name.
pub struct BootstrapBackedSchema {
    bootstrap_conn: Arc<Mutex<Connection>>,
}

impl BootstrapBackedSchema {
    pub fn new(bootstrap_conn: Arc<Mutex<Connection>>) -> Self {
        Self { bootstrap_conn }
    }
}

// Safety: Arc<Mutex<Connection>> is Send+Sync when Connection is Send.
// rusqlite::Connection is Send but not Sync; the Mutex provides Sync.
unsafe impl Sync for BootstrapBackedSchema {}

impl DatabaseSchema for BootstrapBackedSchema {
    fn get_table_columns(&self, schema: Option<&str>, table_name: &str) -> Option<Vec<ColumnInfo>> {
        let conn = self.bootstrap_conn.lock().ok()?;

        // The schema qualifier can be either:
        // 1. A namespace fq_name (e.g., "main", "zot") — used by direct user queries
        // 2. An internal ATTACH alias (e.g., "_imported_8") — used by the resolver
        //    after resolve_namespace_path returns cartridge.source_ns
        //
        // We try namespace fq_name first, then fall back to cartridge source_ns.
        let qualifier = schema.unwrap_or("main");

        // Primary path: look up by namespace fq_name
        let sql_by_namespace = r#"
            SELECT ea.attribute_name, ea.position, ea.is_nullable
            FROM entity_attribute ea
            JOIN entity e ON e.id = ea.entity_id
            JOIN activated_entity ae ON ae.entity_id = e.id
            JOIN namespace n ON n.id = ae.namespace_id
            WHERE n.fq_name = ?1
              AND e.name = ?2
              AND ea.attribute_type = 'output_column'
            ORDER BY ea.position
        "#;

        let columns = Self::query_columns(&conn, sql_by_namespace, qualifier, table_name);
        if let Some(cols) = columns {
            if !cols.is_empty() {
                return Some(cols);
            }
        }

        // Fallback: look up by cartridge source_ns (ATTACH alias)
        let sql_by_source_ns = r#"
            SELECT ea.attribute_name, ea.position, ea.is_nullable
            FROM entity_attribute ea
            JOIN entity e ON e.id = ea.entity_id
            JOIN activated_entity ae ON ae.entity_id = e.id
            JOIN cartridge c ON ae.cartridge_id = c.id
            WHERE c.source_ns = ?1
              AND e.name = ?2
              AND ea.attribute_type = 'output_column'
            ORDER BY ea.position
        "#;

        let columns = Self::query_columns(&conn, sql_by_source_ns, qualifier, table_name);
        if let Some(cols) = columns {
            if !cols.is_empty() {
                return Some(cols);
            }
        }

        None
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> bool {
        self.get_table_columns(schema, table_name).is_some()
    }
}

impl BootstrapBackedSchema {
    fn query_columns(
        conn: &Connection,
        sql: &str,
        qualifier: &str,
        table_name: &str,
    ) -> Option<Vec<ColumnInfo>> {
        let mut stmt = conn.prepare(sql).ok()?;
        let columns: Vec<ColumnInfo> = stmt
            .query_map(rusqlite::params![qualifier, table_name], |row| {
                let name: String = row.get(0)?;
                let position: i32 = row.get(1)?;
                let is_nullable: Option<i32> = row.get(2)?;

                Ok(ColumnInfo {
                    name: name.into(),
                    nullable: is_nullable.unwrap_or(1) != 0,
                    position: (position + 1) as usize, // 0-based to 1-based
                })
            })
            .ok()?
            .collect::<Result<Vec<_>, _>>()
            .ok()?;

        Some(columns)
    }
}
