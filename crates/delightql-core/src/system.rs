//! DelightQL System Management
//!
//! This module provides the `DelightQLSystem` struct which encapsulates
//! the user database connection and the internal _bootstrap metadata store.

use crate::bootstrap::SourceType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::parser::{
    setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
    setup_errors_table_on_bootstrap, DDLFile,
};
use delightql_types::{
    schema::DatabaseSchema, ConnectionComponents, ConnectionFactory, DatabaseConnection,
};
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Result of a `consult_file` operation.
pub(crate) struct ConsultResult {
    /// Number of definitions loaded.
    pub definitions_loaded: usize,
    /// Entity names that were replaced (non-empty only for inline DDL drop-and-replace).
    pub replaced_entities: Vec<String>,
}

/// DelightQL system state with user database and internal metadata store
///
/// This struct manages:
/// 1. User database connection (can be any backend: SQLite, Postgres, DuckDB)
/// 2. Internal _bootstrap SQLite database (always SQLite, engine implementation detail)
/// 3. System schema (sys) attached to user database
/// 4. Connection routing map for query execution
///
/// The _bootstrap database is NOT attached to the user database - it's a completely
/// separate SQLite connection used internally by the engine for metadata storage.
pub(crate) struct DelightQLSystem {
    /// User database connection (target backend)
    pub connection: Arc<Mutex<dyn DatabaseConnection>>,

    /// Internal _bootstrap metadata store (always SQLite)
    /// This is an engine implementation detail, not part of the user's database
    bootstrap_connection: Arc<Mutex<Connection>>,

    /// Database schema provider (injected by CLI)
    /// Stores trait object to avoid coupling to concrete backend implementations
    schema: Option<Box<dyn DatabaseSchema>>,

    /// Connection routing map: connection_id → DatabaseConnection
    /// This maps logical connection IDs to physical database connections for query execution.
    /// - connection_id=1 → Bootstrap connection (internal metadata)
    /// - connection_id=2 → User connection (target database)
    /// Additional connections can be added for attached databases, federation, etc.
    connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>>,

    /// Database introspector for discovering schema metadata
    introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,

    /// Bin cartridge registry for built-in entities (pseudo-predicates, functions, etc.)
    /// Wrapped in Arc so it can be shared with transformer without cloning
    bin_registry: Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,

    /// When true, the namespace resolver is authoritative: `Ok(None)` from
    /// `resolve_unqualified_entity` means the entity genuinely isn't enlisted.
    /// When false (pipe/SISO connections), namespace resolution is a stub and
    /// raw database lookup should be used as a fallback.
    pub namespace_authoritative: bool,

    /// Factory for creating connections from URIs (injected by CLI).
    /// Enables import! to handle pipe:// and other URI schemes.
    connection_factory: Option<Box<dyn ConnectionFactory>>,

    /// Schema map: connection_id → DatabaseSchema for imported connections.
    /// The primary connection schema is in `self.schema`; this holds schemas
    /// for connections created via import!/ConnectionFactory.
    schema_map: HashMap<i64, Box<dyn DatabaseSchema>>,

    /// Cartridge ID for catalog wrapper views in sys::meta.
    /// Lazily initialized on first access to catalog features.
    catalog_cartridge_id: Cell<Option<i32>>,

    /// Database type string ("sqlite", "duckdb", "postgres").
    /// Stored for reinit_bootstrap() to re-register the user connection.
    db_type: String,
}

/// Embedded DQL source for the sys::meta generator HO view.
/// This is the sole definition of the catalog functor join logic.
const SYS_META_SOURCE: &str = include_str!("../autoload/sys/meta.dql");

/// Register a thin catalog wrapper view for a namespace in sys::meta.
///
/// Creates an entity like `main::` with definition `sys::meta.generator("main")(*)`
/// so that `main::(*)` resolves through normal HO view expansion.
fn register_catalog_wrapper(
    conn: &Connection,
    ns_fq: &str,
    sys_meta_ns_id: i32,
    cartridge_id: i32,
) -> Result<()> {
    let entity_name = format!("{}::", ns_fq);
    let definition = format!(r#"_(*) :- sys::meta.generator("{}")(*)"#, ns_fq);

    conn.execute(
        "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![&entity_name, 4, cartridge_id], // type 4 = DqlTemporaryViewExpression
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to insert catalog wrapper entity '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;
    let entity_id = conn.last_insert_rowid() as i32;

    conn.execute(
        "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, ?2)",
        rusqlite::params![entity_id, &definition],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to insert catalog wrapper clause for '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;

    conn.execute(
        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![entity_id, sys_meta_ns_id, cartridge_id],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to activate catalog wrapper '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;

    debug!(
        "register_catalog_wrapper: Registered '{}' in sys::meta",
        entity_name
    );
    Ok(())
}

/// Register catalog views in sys::meta at bootstrap time.
///
/// 1. Loads the generator HO view from embedded sys/meta.dql
/// 2. Creates thin wrapper views for every existing namespace
/// 3. Auto-enlists sys::meta into main
///
/// Returns the cartridge_id used for catalog wrapper entities.
fn register_catalog_views(bootstrap_conn: &Connection) -> Result<i32> {
    // Parse and register the generator HO view via consult_file_inner
    let ddl = crate::pipeline::parser::parse_ddl_file(SYS_META_SOURCE).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to parse sys/meta.dql: {}", e),
            e.to_string(),
        )
    })?;
    let count = ddl.definitions.len();
    DelightQLSystem::consult_file_inner(
        bootstrap_conn,
        "embedded://sys::meta",
        "sys::meta",
        ddl,
        count,
        None,
    )?;

    // Create a separate cartridge for the catalog wrapper entities
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (1, ?1, 'catalog://sys::meta', 'sys::meta', 1, 1, 0)",
            rusqlite::params![SourceType::FileBin.as_i32()],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create catalog wrapper cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let catalog_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    // Get sys::meta namespace ID
    let sys_meta_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::meta namespace: {}", e),
                e.to_string(),
            )
        })?;

    // Register a catalog wrapper for every existing namespace
    let mut stmt = bootstrap_conn
        .prepare("SELECT fq_name FROM namespace ORDER BY id")
        .map_err(|e| {
            DelightQLError::database_error("Failed to prepare namespace query", e.to_string())
        })?;
    let ns_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| DelightQLError::database_error("Failed to query namespaces", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);

    for ns_fq in &ns_names {
        register_catalog_wrapper(bootstrap_conn, ns_fq, sys_meta_ns_id, catalog_cartridge_id)?;
    }

    // Auto-enlist sys::meta into main
    let main_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'main'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query main namespace for enlist: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
             VALUES (?1, ?2)",
            [sys_meta_ns_id, main_ns_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to enlist sys::meta into main: {}", e),
                e.to_string(),
            )
        })?;

    debug!(
        "register_catalog_views: Registered {} catalog wrappers, enlisted sys::meta into main",
        ns_names.len()
    );

    Ok(catalog_cartridge_id)
}

/// Lazily initialize catalog views. Uses Cell for interior mutability so
/// callers holding &self (e.g. ensure_stdlib_loaded) can trigger initialization.
fn ensure_catalog_initialized(
    catalog_cartridge_id: &Cell<Option<i32>>,
    bootstrap_conn: &Connection,
) -> Result<i32> {
    if let Some(id) = catalog_cartridge_id.get() {
        return Ok(id);
    }
    let id = register_catalog_views(bootstrap_conn)?;
    catalog_cartridge_id.set(Some(id));
    Ok(id)
}

/// Check that a namespace fq_name is not already registered in bootstrap.
/// Returns Ok(()) if available, Err if already taken.
fn ensure_namespace_available(conn: &rusqlite::Connection, fq_name: &str) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM namespace WHERE fq_name = ?1)",
            [fq_name],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to check namespace existence", e.to_string())
        })?;

    if exists {
        return Err(DelightQLError::database_error(
            format!(
                "Namespace '{}' already exists. Cannot register the same namespace twice.",
                fq_name
            ),
            "Duplicate namespace",
        ));
    }
    Ok(())
}

impl DelightQLSystem {
    /// Create a new DelightQL system from an injected connection
    ///
    /// Creates:
    /// 1. Session tables in user database (sys, _c, delightql_diagnostics)
    /// 2. Internal _bootstrap SQLite database (separate, not attached to user DB)
    /// 3. Initializes _bootstrap with meta-circular metadata system
    ///
    /// # Arguments
    /// * `connection` - User database connection trait object (for execution)
    /// * `introspector` - Backend-specific introspector for discovering schema
    /// * `db_type` - Database type string ("sqlite", "duckdb", "postgres")
    ///
    /// # Returns
    /// A DelightQLSystem ready for query execution
    pub fn new(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
    ) -> Result<Self> {
        // Create internal _bootstrap metadata store (ALWAYS SQLite)
        let bootstrap_conn = Connection::open_in_memory().map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to create _bootstrap metadata store",
                format!("SQLite error: {}", e),
                Box::new(e),
            )
        })?;

        // Initialize _bootstrap schema and seed data
        crate::bootstrap::initialize_bootstrap_db(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to initialize _bootstrap schema: {}", e),
                e.to_string(),
            )
        })?;

        // Create session tables on bootstrap (assertions, danger, errors)
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_errors_table_on_bootstrap(&bootstrap_conn)?;

        // Register bootstrap connection (id=1) BEFORE installing cartridge
        // (cartridge has FK to connection)
        let bootstrap_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "bootstrap://internal",
            5, // bootstrap connection type
            "Internal engine metadata store",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register bootstrap connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Sanity check: bootstrap connection should always be id=1
        if bootstrap_conn_id != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "Bootstrap connection has unexpected ID: expected id=1, got id={}",
                    bootstrap_conn_id
                ),
                "Internal consistency error".to_string(),
            ));
        }

        // Install bootstrap://sys cartridge and activate entities
        // Note: introspects the _bootstrap database itself (schema = None, it's main)
        let cartridge_id = crate::import::install_cartridge(
            &bootstrap_conn,
            "bootstrap://sys",
            crate::import::SourceType::Db,
            3,       // SQLite language ID
            None,    // _bootstrap tables are in main schema, not attached
            Some(1), // connection_id=1 (bootstrap connection)
            false,   // not universal
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to install bootstrap cartridge: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::create_bootstrap_namespaces(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create bootstrap namespaces: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::activate_bootstrap_entities(&bootstrap_conn, cartridge_id).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate bootstrap entities: {}", e),
                e.to_string(),
            )
        })?;

        // Initialize bin cartridge registry and sync to bootstrap
        let mut bin_registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();

        // Register the prelude cartridge (contains import!, enlist!, delist!)
        bin_registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());

        // Register the predicates cartridge (contains like(), etc.)
        bin_registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());

        // Sync all bin cartridges to bootstrap metadata
        let universal_namespaces =
            crate::bootstrap::sync_bin_cartridges_to_bootstrap(&bootstrap_conn, &bin_registry)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to sync bin cartridges to bootstrap: {}", e),
                        e.to_string(),
                    )
                })?;
        // Register user connection in bootstrap metadata
        // Determine connection type ID from database type string (case-insensitive)
        let db_type_lower = db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => {
                // TODO: Distinguish between file and memory SQLite
                // For now, default to file (type 1)
                1 // sqlite-file
            }
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            _ => {
                return Err(DelightQLError::validation_error(
                    "Unsupported database type",
                    format!("Database type '{}' is not supported", db_type),
                ));
            }
        };

        let user_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "user://main",
            connection_type,
            "User target database",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register user connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // "main" namespace is created empty by create_bootstrap_namespaces().
        // No user cartridge, no introspection — the CLI sends mount!("path", "main")
        // as its first query to populate the namespace.

        // Register session table metadata in bootstrap so they're queryable via DQL
        // Create a cartridge for the sys schema session tables (on user connection)
        bootstrap_conn
            .execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, 'sys://session', NULL, 1, ?3, 0)",
                rusqlite::params![
                    3, // SQLite language (bootstrap is always SQLite)
                    SourceType::Db.as_i32(),
                    bootstrap_conn_id,
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to create sys session cartridge: {}", e),
                    e.to_string(),
                )
            })?;
        let sys_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        // Insert assertions entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('assertions', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions entity: {}", e),
                    e.to_string(),
                )
            })?;
        let assertions_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        // Insert entity clause for assertions
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.assertions system table')",
                rusqlite::params![assertions_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        // Insert column attributes for assertions entity
        let assertion_columns = [
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, true),
            ("source_file", "TEXT", 3, true),
            ("source_line", "INTEGER", 4, true),
            ("body", "TEXT", 5, false),
            ("outcome", "TEXT", 6, false),
            ("detail", "TEXT", 7, true),
            ("run_id", "TEXT", 8, false),
        ];
        for (col_name, data_type, position, nullable) in &assertion_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![
                        assertions_entity_id,
                        col_name,
                        data_type,
                        position,
                        nullable,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to insert sys.assertions column '{}': {}",
                            col_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        // Insert danger entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('danger', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity: {}", e),
                    e.to_string(),
                )
            })?;
        let danger_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.danger system table')",
                rusqlite::params![danger_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        let danger_columns = [
            ("uri", "TEXT", 1, false),
            ("state", "TEXT", 2, false),
            ("cli_overridable", "INTEGER", 3, false),
            ("description", "TEXT", 4, true),
        ];
        for (col_name, data_type, position, nullable) in &danger_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![danger_entity_id, col_name, data_type, position, nullable,],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.danger column '{}': {}", col_name, e),
                        e.to_string(),
                    )
                })?;
        }

        // Insert errors entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('errors', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity: {}", e),
                    e.to_string(),
                )
            })?;
        let errors_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.errors system table')",
                rusqlite::params![errors_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        let errors_columns = [
            ("id", "INTEGER", 1, false),
            ("uri", "TEXT", 2, false),
            ("message", "TEXT", 3, false),
            ("query_text", "TEXT", 4, true),
            ("timestamp", "TEXT", 5, true),
        ];
        for (col_name, data_type, position, nullable) in &errors_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![errors_entity_id, col_name, data_type, position, nullable,],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.errors column '{}': {}", col_name, e),
                        e.to_string(),
                    )
                })?;
        }

        // Get sys namespace ID and activate sys entities there
        let sys_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to query sys namespace: {}", e),
                    e.to_string(),
                )
            })?;

        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            sys_cartridge_id,
            sys_ns_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys.assertions in sys namespace: {}", e),
                e.to_string(),
            )
        })?;

        // Initialize connection routing map
        let mut connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>> = HashMap::new();
        connection_map.insert(user_conn_id, Arc::clone(&connection)); // User connection

        let bootstrap_arc = Arc::new(Mutex::new(bootstrap_conn));
        let schema = Box::new(crate::bootstrap_schema::BootstrapBackedSchema::new(
            bootstrap_arc.clone(),
        ));

        let system = DelightQLSystem {
            connection,
            bootstrap_connection: bootstrap_arc,
            schema: Some(schema),
            connection_map,
            introspector,
            bin_registry: Arc::new(bin_registry),
            namespace_authoritative: true,
            connection_factory: None,
            schema_map: HashMap::new(),
            catalog_cartridge_id: Cell::new(None),
            db_type: db_type.to_string(),
        };

        // Eagerly load stdlib DQL overlays for universal (auto-enlisted) namespaces
        for ns in &universal_namespaces {
            system.ensure_stdlib_loaded(ns);
        }

        Ok(system)
    }

    /// Create a new DelightQL system with injected database schema (Phase 2)
    ///
    /// This is the preferred constructor after Phase 2 refactor. It accepts
    /// a database schema implementation via dependency injection, allowing
    /// core to remain database-agnostic.
    ///
    /// # Arguments
    /// * `connection` - Database connection (created by backend/CLI)
    /// * `introspector` - Backend-specific introspector for discovering schema
    /// * `db_type` - Database type string ("sqlite", "duckdb", "postgres")
    /// * `schema` - Database schema implementation (created by CLI)
    ///
    /// # Returns
    /// A DelightQLSystem ready for query execution with schema support
    ///
    /// # Example
    /// ```ignore
    /// Get the injected database schema (Phase 2)
    ///
    /// Returns a reference to the database schema provider that was injected
    /// during system construction. This allows the Pipeline to access schema
    /// information without knowing about concrete backend implementations.
    ///
    /// # Returns
    /// Reference to the DatabaseSchema trait object
    ///
    /// # Errors
    /// Returns error if no schema was injected (old code path)
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

    /// Get a reference to the bootstrap connection (for session tables: assertions, danger, errors).
    pub fn bootstrap_connection(&self) -> &Arc<Mutex<Connection>> {
        &self.bootstrap_connection
    }

    /// Get the schema map for imported connections
    pub fn get_schema_map(&self) -> &HashMap<i64, Box<dyn DatabaseSchema>> {
        &self.schema_map
    }

    /// Register an external connection: introspect, register in bootstrap, activate in namespace.
    /// Used by import! when a ConnectionFactory is available (for pipe://, file://, etc.).
    ///
    /// Returns (connection_id, entity_count) on success.
    pub fn register_external_connection(
        &mut self,
        components: ConnectionComponents,
        namespace: &str,
        connection_uri: &str,
    ) -> Result<(i64, usize)> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for external connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Idempotent mount: if namespace already exists with the SAME URI, return
        // existing connection info. If a different URI, that's an error.
        {
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN activated_entity ae ON ae.namespace_id = n.id
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE n.fq_name = ?1
                 LIMIT 1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    match bootstrap_conn.query_row(
                        "SELECT 1 FROM namespace WHERE fq_name = ?1",
                        [namespace],
                        |_| Ok(()),
                    ) {
                        Ok(()) => Some(String::new()),
                        Err(_) => None,
                    }
                }
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        "Failed to check namespace existence",
                        e.to_string(),
                    ));
                }
            };
            if let Some(existing_uri) = existing {
                if existing_uri == connection_uri || existing_uri.is_empty() {
                    // Same database — return existing connection info
                    let conn_id: i64 = bootstrap_conn
                        .query_row(
                            "SELECT id FROM connection WHERE connection_uri = ?1",
                            [connection_uri],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    let entity_count: usize = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) FROM namespace n JOIN activated_entity ae ON ae.namespace_id = n.id WHERE n.fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    drop(bootstrap_conn);
                    return Ok((conn_id, entity_count));
                } else {
                    return Err(DelightQLError::database_error(
                        format!(
                            "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                            namespace, existing_uri, connection_uri
                        ),
                        "Duplicate namespace with different source",
                    ));
                }
            }
        }

        // Determine connection type from db_type string
        let db_type_lower = components.db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => 1,
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            other => panic!(
                "catch-all hit in system.rs mount_database: unexpected db_type: {}",
                other
            ),
        };

        // Register the connection in bootstrap
        let connection_id = crate::import::register_connection(
            &bootstrap_conn,
            connection_uri,
            connection_type,
            &format!("Mounted database: {}", namespace),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Introspect the connection to discover entities
        let entities = components.introspector.introspect_entities().map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect imported database: {}", e),
                e.to_string(),
            )
        })?;

        // Install as a cartridge (no source_ns since it's a separate connection)
        let cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, NULL, 1, ?4, 0)",
                    rusqlite::params![
                        connection_type,
                        crate::bootstrap::SourceType::Db.as_i32(),
                        connection_uri,
                        connection_id,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert cartridge",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Insert discovered entities into bootstrap metadata
        let entity_count = entities.len();
        crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            )
        })?;

        // Create the namespace
        let namespace_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace (name, pid, fq_name, kind, provenance, source_path)
                     VALUES (?1, NULL, ?2, 'data', 'uri', ?3)",
                    rusqlite::params![namespace, namespace, connection_uri],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Activate all entities from the cartridge in the namespace
        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            namespace_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            )
        })?;

        // Register catalog wrapper for the new namespace (lazy-init catalog if needed)
        let catalog_id = ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
        let sys_meta_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to query sys::meta namespace for catalog wrapper",
                    e.to_string(),
                )
            })?;
        register_catalog_wrapper(&bootstrap_conn, namespace, sys_meta_ns_id, catalog_id)?;

        debug!(
            "register_external_connection: Registered {} entities in namespace '{}' (connection_id={})",
            entity_count, namespace, connection_id
        );

        // Drop bootstrap lock before mutating self's maps
        drop(bootstrap_conn);

        // Store connection and schema in routing maps
        self.connection_map
            .insert(connection_id, components.connection);
        self.schema_map.insert(connection_id, components.schema);

        Ok((connection_id, entity_count))
    }

    /// Get the internal _bootstrap metadata connection
    ///
    /// Returns a reference to the internal SQLite connection used for metadata storage.
    /// This connection is independent of the user's database and is always SQLite.
    ///
    /// Used by:
    /// - Resolver for namespace lookups (_bootstrap.namespace)
    /// - Import operations (.attach, .borrow, etc.)
    /// - Metadata queries (sys::* namespaces)
    pub fn get_bootstrap_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.bootstrap_connection)
    }

    /// Get the bin cartridge registry
    ///
    /// Returns a reference to the registry containing all registered bin cartridges
    /// and their entities. Used by the effect executor to look up pseudo-predicates
    /// for execution.
    pub fn bin_registry(&self) -> Arc<crate::bin_cartridge::registry::BinCartridgeRegistry> {
        Arc::clone(&self.bin_registry)
    }

    /// Reset the system to a clean state equivalent to `System::new()`.
    ///
    /// Drops and rebuilds the in-memory bootstrap database, clears session tables,
    /// re-introspects the user connection, and resets all ancillary state.
    /// Used by the server to cheaply reset between test queries (~5ms).
    pub fn reinit_bootstrap(&mut self) -> Result<()> {
        use crate::pipeline::parser::{
            setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
            setup_errors_table_on_bootstrap,
        };

        // 1. DETACH all imported schemas from user connection
        {
            let user_conn = self.connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire user connection lock for reinit",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            // Query PRAGMA database_list and detach everything except "main", "temp", and "sys".
            // "sys" is an in-memory ATTACH used for session tables — we keep it and clear its tables.
            let schemas: Vec<String> = {
                match user_conn.query_all_string_rows("PRAGMA database_list", &[]) {
                    Ok((_cols, rows)) => rows
                        .iter()
                        .filter_map(|row| row.get(1).cloned())
                        .filter(|s| s != "main" && s != "temp" && s != "sys")
                        .collect(),
                    Err(_) => Vec::new(),
                }
            };
            for schema in &schemas {
                let _ = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]);
            }
        }

        // 2. Create fresh in-memory bootstrap (session tables are created on bootstrap below)
        let bootstrap_conn = Connection::open_in_memory().map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to create _bootstrap metadata store during reinit",
                format!("SQLite error: {}", e),
                Box::new(e),
            )
        })?;

        crate::bootstrap::initialize_bootstrap_db(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to initialize _bootstrap schema during reinit: {}",
                    e
                ),
                e.to_string(),
            )
        })?;

        // 3. Create session tables on bootstrap
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_errors_table_on_bootstrap(&bootstrap_conn)?;

        // 4. Register connections (bootstrap=1, user=2)
        let bootstrap_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "bootstrap://internal",
            5,
            "Internal engine metadata store",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to register bootstrap connection during reinit: {}",
                    e
                ),
                e.to_string(),
            )
        })? as i64;

        if bootstrap_conn_id != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "Bootstrap connection has unexpected ID during reinit: expected id=1, got id={}",
                    bootstrap_conn_id
                ),
                "Internal consistency error".to_string(),
            ));
        }

        let db_type_lower = self.db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => 1,
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            _ => {
                return Err(DelightQLError::validation_error(
                    "Unsupported database type during reinit",
                    format!("Database type '{}' is not supported", self.db_type),
                ));
            }
        };

        let user_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "user://main",
            connection_type,
            "User target database",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register user connection during reinit: {}", e),
                e.to_string(),
            )
        })? as i64;

        // 5. Install bootstrap cartridge, namespaces, entities
        let cartridge_id = crate::import::install_cartridge(
            &bootstrap_conn,
            "bootstrap://sys",
            crate::import::SourceType::Db,
            3,
            None,
            Some(1),
            false,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to install bootstrap cartridge during reinit: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::create_bootstrap_namespaces(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create bootstrap namespaces during reinit: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::activate_bootstrap_entities(&bootstrap_conn, cartridge_id).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate bootstrap entities during reinit: {}", e),
                e.to_string(),
            )
        })?;

        // 6. Sync bin cartridges
        let universal_namespaces =
            crate::bootstrap::sync_bin_cartridges_to_bootstrap(&bootstrap_conn, &self.bin_registry)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to sync bin cartridges during reinit: {}", e),
                        e.to_string(),
                    )
                })?;

        // 7. Leave "main" namespace EMPTY — caller is expected to mount! the db they need.
        //    This allows pack-man to reset + mount a different db each time.
        //    The user connection still exists (connection_id=2) for SQL execution;
        //    mount! will register entities and ATTACH the target db.

        // 8. Register session table metadata in bootstrap (sys.assertions, sys.danger, sys.errors)
        bootstrap_conn
            .execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, 'sys://session', NULL, 1, ?3, 0)",
                rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to create sys session cartridge during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let sys_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        // Register assertions entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('assertions', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to insert sys.assertions entity during reinit: {}",
                        e
                    ),
                    e.to_string(),
                )
            })?;
        let assertions_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.assertions system table')",
                rusqlite::params![assertions_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, true),
            ("source_file", "TEXT", 3, true),
            ("source_line", "INTEGER", 4, true),
            ("body", "TEXT", 5, false),
            ("outcome", "TEXT", 6, false),
            ("detail", "TEXT", 7, true),
            ("run_id", "TEXT", 8, false),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![assertions_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.assertions column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Register danger entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('danger', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let danger_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.danger system table')",
                rusqlite::params![danger_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("uri", "TEXT", 1, false),
            ("state", "TEXT", 2, false),
            ("cli_overridable", "INTEGER", 3, false),
            ("description", "TEXT", 4, true),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![danger_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.danger column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Register errors entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('errors', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let errors_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.errors system table')",
                rusqlite::params![errors_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("id", "INTEGER", 1, false),
            ("uri", "TEXT", 2, false),
            ("message", "TEXT", 3, false),
            ("query_text", "TEXT", 4, true),
            ("timestamp", "TEXT", 5, true),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![errors_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.errors column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Activate sys entities
        let sys_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to query sys namespace during reinit: {}", e),
                    e.to_string(),
                )
            })?;

        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            sys_cartridge_id,
            sys_ns_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys entities during reinit: {}", e),
                e.to_string(),
            )
        })?;

        // 9. Swap bootstrap connection
        *self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for reinit swap",
                format!("Connection was poisoned: {}", e),
            )
        })? = bootstrap_conn;

        // 10. Reset ancillary state
        self.connection_map.clear();
        self.connection_map
            .insert(user_conn_id, Arc::clone(&self.connection));
        self.schema_map.clear();
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        )); // Empty until mount! runs again
        self.catalog_cartridge_id.set(None);

        // 11. Eagerly load stdlib DQL overlays for universal namespaces
        //     (mirrors the same step in DelightQLSystem::new)
        for ns in &universal_namespaces {
            self.ensure_stdlib_loaded(ns);
        }

        Ok(())
    }

    /// Ensure a stdlib module is consulted into the bootstrap DB, if it exists.
    ///
    /// Checks whether `namespace_fq` matches a stdlib module (e.g., "std::info").
    /// If the namespace doesn't yet exist in the bootstrap DB but a matching
    /// embedded module is available, consults it on the fly.
    ///
    /// Ensure catalog views (sys::meta) are initialized.
    /// Called lazily on first access to sys::meta entities.
    pub fn ensure_catalog_loaded(&self) {
        if self.catalog_cartridge_id.get().is_some() {
            return;
        }
        if let Ok(conn) = self.bootstrap_connection.lock() {
            let _ = ensure_catalog_initialized(&self.catalog_cartridge_id, &conn);
        }
    }

    /// Returns true if the module was just loaded (caller should retry lookup).
    pub fn ensure_stdlib_loaded(&self, namespace_fq: &str) -> bool {
        // Find matching embedded module (covers std::*, sys::*, etc.)
        let module = crate::stdlib_manifest::STDLIB_MODULES
            .iter()
            .find(|(ns, _)| *ns == namespace_fq);

        let Some((_namespace, source)) = module else {
            return false;
        };

        // Check if already loaded (namespace row exists in bootstrap DB)
        let bootstrap_conn = match self.bootstrap_connection.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let source_uri = format!("embedded://{}", namespace_fq);
        let already_loaded: bool = bootstrap_conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM cartridge WHERE source_uri = ?1 AND source_ns = ?2",
                rusqlite::params![&source_uri, namespace_fq],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if already_loaded {
            return false;
        }

        // Consult the module
        let ddl = match crate::pipeline::parser::parse_ddl_file(source) {
            Ok(d) => d,
            Err(e) => {
                log::warn!("Failed to parse stdlib '{}': {}", namespace_fq, e);
                return false;
            }
        };

        let count = ddl.definitions.len();
        let path = format!("embedded://{}", namespace_fq);

        bootstrap_conn.execute_batch("BEGIN").ok();

        match Self::consult_file_inner(&bootstrap_conn, &path, namespace_fq, ddl, count, None) {
            Ok(_) => {
                let _ = bootstrap_conn.execute_batch("COMMIT");
                // Register catalog wrapper for the newly-loaded stdlib namespace
                if let Ok(catalog_id) =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)
                {
                    if let Ok(sys_meta_ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let _ = register_catalog_wrapper(
                            &bootstrap_conn,
                            namespace_fq,
                            sys_meta_ns_id,
                            catalog_id,
                        );
                    }
                }
                true
            }
            Err(e) => {
                log::warn!("Failed to consult stdlib '{}': {}", namespace_fq, e);
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                false
            }
        }
    }

    /// Get the appropriate connection for executing a query based on connection_id
    ///
    /// Routes query execution to the correct physical connection:
    /// - connection_id=1 → Bootstrap connection (internal metadata)
    /// - connection_id=2 → User connection (target database)
    ///
    /// # Arguments
    /// * `connection_id` - The connection ID from cartridge metadata
    ///
    /// # Returns
    /// * `Ok(Arc<Mutex<dyn DatabaseConnection>>)` - Arc reference to the appropriate connection
    /// * `Err(...)` - If connection_id is invalid/unknown
    pub fn get_connection(&self, connection_id: i64) -> Result<Arc<Mutex<dyn DatabaseConnection>>> {
        self.connection_map
            .get(&connection_id)
            .cloned()
            .ok_or_else(|| {
                DelightQLError::validation_error(
                    "Unknown connection ID",
                    format!(
                        "Connection ID {} is not recognized. Valid IDs: 1 (bootstrap), 2 (user)",
                        connection_id
                    ),
                )
            })
    }

    /// Mount a database and register it with a namespace
    ///
    /// This is called by the `mount!()` pseudo-predicate to:
    /// 1. Open a database connection at the specified path or URI
    /// 2. Register it in the bootstrap connection table
    /// 3. Introspect its schema and install as a cartridge
    /// 4. Activate all entities into the specified namespace
    /// 5. Add the connection to the routing map
    ///
    /// # Arguments
    /// * `db_path` - Path to the database file or URI (e.g., "pipe://snowflake")
    /// * `namespace` - Namespace name to register (e.g., "mfg", "sales")
    ///
    /// # Returns
    /// * `Ok(())` - Database successfully mounted and namespace registered
    /// * `Err(...)` - If database cannot be opened, introspected, or registered
    ///
    /// # Example
    /// ```ignore
    /// system.mount_database("./data.db", "mydata")?;
    /// // Now can query: mydata::users(*)
    /// ```
    pub fn mount_database(&mut self, db_path: &str, namespace: &str) -> Result<()> {
        // If a ConnectionFactory is available and the path looks like a URI scheme,
        // use the factory path (supports pipe://, duckdb://, etc.)
        let has_uri_scheme = db_path.contains("://");
        if has_uri_scheme {
            if let Some(factory) = self.connection_factory.as_ref() {
                let components = factory.create(db_path).map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to create connection for '{}': {}", db_path, e),
                        e.to_string(),
                    )
                })?;
                self.register_external_connection(components, namespace, db_path)?;
                return Ok(());
            } else {
                return Err(DelightQLError::validation_error(
                    format!(
                        "Cannot mount '{}': URI schemes require a connection factory (not available in this context)",
                        db_path
                    ),
                    "No connection factory configured",
                ));
            }
        }

        // Plain file path: use the existing ATTACH DATABASE path (SQLite-to-SQLite optimization)

        // Resolve relative path against session CWD (for test isolation).
        let resolved_path = crate::session_cwd::resolve_path(db_path);
        let db_path = resolved_path.display().to_string();
        let db_path = db_path.as_str();

        // Guard: file must exist and be a valid SQLite database
        let path = std::path::Path::new(db_path);
        if !path.exists() {
            return Err(DelightQLError::database_error(
                format!(
                    "mount!() failed: file '{}' does not exist. \
                     Use create!() to make a new database.",
                    db_path
                ),
                "File not found",
            ));
        }
        {
            use std::io::Read;
            let mut file = std::fs::File::open(path).map_err(|e| {
                DelightQLError::database_error(
                    format!("mount!() failed: cannot open '{}': {}", db_path, e),
                    "File open failed",
                )
            })?;
            let mut header = [0u8; 16];
            let bytes_read = file.read(&mut header).map_err(|e| {
                DelightQLError::database_error(
                    format!("mount!() failed: cannot read '{}': {}", db_path, e),
                    "File read failed",
                )
            })?;
            // Allow empty files (0 bytes) — SQLite creates the database on ATTACH.
            // Only reject non-empty files that don't have a valid SQLite header.
            if bytes_read > 0 && (bytes_read < 16 || &header != b"SQLite format 3\0") {
                return Err(DelightQLError::database_error(
                    format!(
                        "mount!() failed: '{}' is not a valid SQLite database",
                        db_path
                    ),
                    "Invalid database file",
                ));
            }
        }

        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for mount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Idempotent mount: if namespace already exists with the SAME database file,
        // this mount is a no-op. If a different file, that's an error.
        // If the namespace exists but is empty (e.g. "main" with :memory:), fall through
        // and reuse the existing namespace_id.
        let existing_namespace_id: Option<i32>;
        {
            let connection_uri = format!("file://{}", db_path);
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN activated_entity ae ON ae.namespace_id = n.id
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE n.fq_name = ?1
                 LIMIT 1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // Namespace might exist but have no entities — check namespace directly
                    match bootstrap_conn.query_row(
                        "SELECT 1 FROM namespace WHERE fq_name = ?1",
                        [namespace],
                        |_| Ok(()),
                    ) {
                        Ok(()) => Some(String::new()), // exists but empty
                        Err(_) => None,                // doesn't exist
                    }
                }
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        "Failed to check namespace existence",
                        e.to_string(),
                    ));
                }
            };
            if let Some(existing_uri) = existing {
                if existing_uri == connection_uri {
                    // Same database — true idempotent, skip
                    drop(bootstrap_conn);
                    return Ok(());
                } else if existing_uri.is_empty() {
                    // Namespace exists but has no file-backed entities (e.g. "main" with :memory:).
                    // Fall through to mount, reusing the existing namespace row.
                    let ns_id: i32 = bootstrap_conn
                        .query_row(
                            "SELECT id FROM namespace WHERE fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query existing namespace id",
                                e.to_string(),
                            )
                        })?;
                    existing_namespace_id = Some(ns_id);
                } else {
                    return Err(DelightQLError::database_error(
                        format!(
                            "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                            namespace, existing_uri, connection_uri
                        ),
                        "Duplicate namespace with different source",
                    ));
                }
            } else {
                existing_namespace_id = None;
            }
        }

        // Auto-generate unique SQLite schema alias
        let next_id: i32 = bootstrap_conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM cartridge",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to query next cartridge ID",
                    e.to_string(),
                    Box::new(e),
                )
            })?;
        let schema_alias = format!("_imported_{}", next_id);
        debug!("mount_database: Generated schema alias: {}", schema_alias);

        // ATTACH the database to the user connection
        let user_conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire user connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let attach_sql = format!("ATTACH DATABASE '{}' AS '{}'", db_path, schema_alias);
        debug!("mount_database: Executing ATTACH: {}", attach_sql);
        user_conn.execute(&attach_sql, &[]).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to attach database: {}", e),
                e.to_string(),
            )
        })?;

        // Register the connection in bootstrap
        let connection_uri = format!("file://{}", db_path);
        let _connection_id = crate::import::register_connection(
            &bootstrap_conn,
            &connection_uri,
            1, // sqlite-file
            &format!("Mounted database: {}", namespace),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register connection: {}", e),
                e.to_string(),
            )
        })?;

        // Introspect the attached database using the schema-specific method
        let entities = self
            .introspector
            .introspect_entities_in_schema(&schema_alias)
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to introspect attached database schema '{}': {}",
                        schema_alias, e
                    ),
                    e.to_string(),
                )
            })?;
        debug!(
            "mount_database: Discovered {} entities in schema '{}'",
            entities.len(),
            schema_alias
        );

        // Install as a cartridge
        // When mounting into "main", set source_ns = NULL so unqualified table references
        // resolve via SQLite's cross-schema search (matches the --db path behavior).
        let effective_source_ns: Option<&str> = if namespace == "main" {
            None
        } else {
            Some(&schema_alias)
        };
        let cartridge_id = {
            let sql = r#"
                INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)
            "#;
            bootstrap_conn
                .execute(
                    sql,
                    rusqlite::params![
                        3, // SQLite language ID
                        crate::bootstrap::SourceType::Db.as_i32(),
                        &format!("file://{}", db_path),
                        effective_source_ns,
                        2, // connection_id=2 (user connection where database is attached)
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert cartridge",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Insert discovered entities into bootstrap
        crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            )
        })?;

        // Create or reuse the namespace
        let namespace_id = if let Some(ns_id) = existing_namespace_id {
            debug!(
                "mount_database: Reusing existing namespace_id={} for '{}'",
                ns_id, namespace
            );
            ns_id
        } else {
            let sql = r#"
                INSERT INTO namespace (name, pid, fq_name, kind, provenance, source_path)
                VALUES (?1, NULL, ?2, 'data', 'file', ?3)
            "#;
            debug!(
                "mount_database: Creating namespace name='{}', fq_name='{}'",
                namespace, namespace
            );
            bootstrap_conn
                .execute(sql, rusqlite::params![namespace, namespace, db_path])
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let id = bootstrap_conn.last_insert_rowid() as i32;
            debug!("mount_database: Created namespace_id={}", id);
            id
        };

        // Activate all entities from the cartridge in the namespace
        let activated_count = crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            namespace_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            )
        })?;
        debug!(
            "mount_database: Activated {} entities in namespace '{}'",
            activated_count, namespace
        );

        // Note: The ATTACH path shares the user connection (connection_id=2),
        // so no additional entry in connection_map is needed. The attached schema
        // is accessed through the existing connection via the schema alias prefix.

        // Register catalog wrapper for the new namespace (lazy-init catalog if needed)
        let catalog_id = ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
        let sys_meta_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to query sys::meta namespace for catalog wrapper",
                    e.to_string(),
                )
            })?;
        register_catalog_wrapper(&bootstrap_conn, namespace, sys_meta_ns_id, catalog_id)?;

        // Explicitly drop the bootstrap connection lock to ensure all writes are committed
        // This is necessary for sequential query execution to see the mounted namespace
        drop(bootstrap_conn);

        // Set the schema provider to read from bootstrap metadata.
        // This replaces the old DynamicSqliteSchema that queried the live connection.
        // Now column information comes from bootstrap's entity_attribute table,
        // which was populated by the introspection above.
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        ));

        Ok(())
    }

    /// Consult a DQL file containing definitions (functions and views)
    ///
    /// Load definitions from a parsed DDL file into the bootstrap metadata system.
    ///
    /// For each definition: creates an entity row, activates it in the namespace.
    /// The bootstrap DB is the single source of truth — no in-memory cache.
    ///
    /// # Arguments
    /// * `path` - Path to the DQL file (for cartridge source_uri)
    /// * `namespace` - Namespace to register under (e.g., "lib::math")
    /// * `ddl` - Pre-parsed DDL file (consumed)
    ///
    /// # Returns
    /// ConsultResult with definitions loaded count and any replaced entity names
    pub fn consult_file(
        &mut self,
        path: &str,
        namespace: &str,
        ddl: DDLFile,
    ) -> Result<ConsultResult> {
        let count = ddl.definitions.len();
        debug!(
            "consult_file: Loading {} definitions from '{}' into namespace '{}'",
            count, path, namespace
        );

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for consult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error("Failed to begin consult transaction", e.to_string())
        })?;

        // Determine ambient DataNs for scratch namespaces.
        // Inline DDL views should be able to reference base tables from the
        // primary data namespace (typically "main") without explicit grounding.
        let ambient_data_ns = if path == "(inline)" {
            bootstrap_conn
                .query_row(
                    "SELECT fq_name FROM namespace WHERE kind = 'data' AND fq_name = 'main'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .ok()
        } else {
            None
        };

        let result = Self::consult_file_inner(
            &bootstrap_conn,
            path,
            namespace,
            ddl,
            count,
            ambient_data_ns.as_deref(),
        );

        if result.is_ok() {
            bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to commit consult transaction",
                    e.to_string(),
                )
            })?;

            // If consult created a new namespace, register a catalog wrapper for it.
            // Check by looking for an existing wrapper entity named "namespace::" in sys::meta.
            let wrapper_name = format!("{}::", namespace);
            let already_has_wrapper: bool = bootstrap_conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON ae.namespace_id = n.id
                     WHERE e.name = ?1 AND n.fq_name = 'sys::meta')",
                    rusqlite::params![&wrapper_name],
                    |row| row.get(0),
                )
                .unwrap_or(true);

            if !already_has_wrapper {
                if let Ok(catalog_id) =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)
                {
                    if let Ok(sys_meta_ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let _ = register_catalog_wrapper(
                            &bootstrap_conn,
                            namespace,
                            sys_meta_ns_id,
                            catalog_id,
                        );
                    }
                }
            }
        } else {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
        }

        drop(bootstrap_conn);

        result
    }

    fn consult_file_inner(
        bootstrap_conn: &Connection,
        path: &str,
        namespace: &str,
        ddl: DDLFile,
        count: usize,
        default_data_ns: Option<&str>,
    ) -> Result<ConsultResult> {
        // Embedded stdlib modules use their path directly as the URI;
        // filesystem consults get a file:// prefix.
        let (source_uri, source_type) = if path.starts_with("embedded://") {
            (path.to_string(), SourceType::FileBin)
        } else {
            (format!("file://{}", path), SourceType::File)
        };

        // Get or create namespace.
        // Allows appending definitions from different files to an existing namespace
        // (needed when DDL files contain embedded consult!() directives targeting
        // the same namespace). Errors if the exact same source file has already
        // been consulted into this namespace (duplicate consult detection).
        let namespace_id = {
            let existing_id: Option<i32> = bootstrap_conn
                .query_row(
                    "SELECT id FROM namespace WHERE fq_name = ?1",
                    rusqlite::params![namespace],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to check namespace existence",
                        e.to_string(),
                    )
                })?;

            match existing_id {
                Some(id) => {
                    // Write protection: only scratch namespaces accept inline DDL
                    if path == "(inline)" {
                        let writable: bool = bootstrap_conn
                            .query_row(
                                "SELECT writable FROM namespace WHERE id = ?1",
                                [id],
                                |row| row.get::<_, i32>(0).map(|v| v != 0),
                            )
                            .unwrap_or(false);
                        if !writable {
                            let (ns_kind, ns_source): (String, Option<String>) = bootstrap_conn
                                .query_row(
                                    "SELECT kind, source_path FROM namespace WHERE id = ?1",
                                    [id],
                                    |row| Ok((row.get(0)?, row.get(1)?)),
                                )
                                .unwrap_or(("unknown".into(), None));
                            let source_info = ns_source
                                .map(|s| format!(" (from {})", s))
                                .unwrap_or_default();
                            return Err(DelightQLError::database_error_categorized(
                                "runtime",
                                format!(
                                    "Cannot write definitions to namespace '{}' — \
                                     it is a {} namespace{} and is not writable. \
                                     Use (~~ddl:\"name\" ~~) to create a scratch namespace instead.",
                                    namespace, ns_kind, source_info
                                ),
                                "Write protection",
                            ));
                        }
                    }

                    // Namespace exists — check for duplicate source URI
                    // Skip for inline DDL: multiple (~~ddl:"name" ~~) blocks can append
                    // to the same scratch namespace (write protection above guards safety).
                    if path != "(inline)" {
                        let duplicate: bool = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) > 0 FROM cartridge WHERE source_uri = ?1 AND source_ns = ?2",
                            rusqlite::params![&source_uri, namespace],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                        if duplicate {
                            return Err(DelightQLError::database_error_categorized(
                                "runtime",
                                format!(
                                    "File '{}' has already been consulted into namespace '{}'",
                                    path, namespace
                                ),
                                "Duplicate consult",
                            ));
                        }
                    }
                    id
                }
                None => {
                    let (ns_kind, ns_provenance, ns_source, ns_writable) = if path == "(inline)" {
                        ("scratch", "scratch", None, 1i32)
                    } else if path.starts_with("embedded://") {
                        ("system", "bootstrap", Some(path), 0i32)
                    } else {
                        ("lib", "file", Some(path), 0i32)
                    };
                    let sql = r#"
                        INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance, source_path, writable)
                        VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7)
                    "#;
                    let name = namespace.split("::").last().unwrap_or(namespace);
                    bootstrap_conn
                        .execute(
                            sql,
                            rusqlite::params![
                                name,
                                namespace,
                                default_data_ns,
                                ns_kind,
                                ns_provenance,
                                ns_source,
                                ns_writable
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to create consult namespace",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    bootstrap_conn.last_insert_rowid() as i32
                }
            }
        };

        // For inline DDL: drop-and-replace conflicting entities by name.
        // Only entities whose names match a definition in the new DDL block are
        // removed; other entities from earlier inline blocks are preserved.
        let replaced_entities: Vec<String> = if path == "(inline)" {
            // Collect entity names from the incoming DDL
            let new_names: Vec<&str> = ddl.definitions.iter().map(|d| d.name.as_str()).collect();
            let new_names_deduped: std::collections::HashSet<&str> =
                new_names.iter().copied().collect();

            let mut replaced_names: Vec<String> = Vec::new();

            for name in &new_names_deduped {
                // Find existing inline entities with this name in this namespace
                let conflicting: Vec<(i64, i64)> = {
                    let mut stmt = bootstrap_conn
                        .prepare(
                            "SELECT e.id, e.cartridge_id FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             JOIN cartridge c ON e.cartridge_id = c.id
                             WHERE e.name = ?1 AND ae.namespace_id = ?2
                               AND c.source_uri LIKE '%inline%'",
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query conflicting inline entities",
                                e.to_string(),
                            )
                        })?;
                    let rows = stmt
                        .query_map(rusqlite::params![name, namespace_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query conflicting inline entities",
                                e.to_string(),
                            )
                        })?;
                    rows.flatten().collect()
                };

                if !conflicting.is_empty() {
                    replaced_names.push(name.to_string());
                }

                for (entity_id, cartridge_id) in &conflicting {
                    Self::clear_single_entity(bootstrap_conn, *entity_id)?;

                    // Clean up cartridge if it has no remaining entities
                    let remaining: i64 = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) FROM entity WHERE cartridge_id = ?1",
                            [cartridge_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(1);
                    if remaining == 0 {
                        bootstrap_conn
                            .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
                            .map_err(|e| {
                                DelightQLError::database_error(
                                    "Failed to delete empty cartridge",
                                    e.to_string(),
                                )
                            })?;
                    }
                }
            }

            if !replaced_names.is_empty() {
                log::warn!(
                    "Inline DDL: replacing {} entit{} in namespace '{}': {}",
                    replaced_names.len(),
                    if replaced_names.len() == 1 {
                        "y"
                    } else {
                        "ies"
                    },
                    namespace,
                    replaced_names.join(", ")
                );
            }

            replaced_names
        } else {
            Vec::new()
        };

        // Create cartridge for the consulted file
        let cartridge_id = {
            let sql = r#"
                INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)
            "#;
            bootstrap_conn
                .execute(
                    sql,
                    rusqlite::params![
                        1, // DqlStandard language ID
                        source_type.as_i32(),
                        &source_uri,
                        Some(namespace),
                        1, // bootstrap connection
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert consult cartridge",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Group definitions by name to support disjunctive clauses.
        // Multiple definitions with the same name (e.g., multi-clause sigma predicates
        // or guarded functions) are stored as a single entity with concatenated source.
        let mut groups: indexmap::IndexMap<String, Vec<&crate::pipeline::parser::Definition>> =
            indexmap::IndexMap::new();
        for def in &ddl.definitions {
            groups.entry(def.name.clone()).or_default().push(def);
        }

        for (_name, defs) in &groups {
            // For multi-clause groups, concatenate source texts
            let (source_to_store, first_def) = if defs.len() == 1 {
                (defs[0].full_source.clone(), defs[0])
            } else {
                let concatenated = defs
                    .iter()
                    .map(|d| d.full_source.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                debug!(
                    "consult_file: Grouping {} clauses for '{}' into single entity",
                    defs.len(),
                    defs[0].name
                );
                (concatenated, defs[0])
            };

            // Build typed DDL AST(s) from source text — eager validation.
            // If the body can't be parsed, error now rather than at query time.
            //
            // Skip eager validation for HO views: their bodies contain
            // unsubstituted HO parameter references (T(*), V(v), etc.) that
            // may create syntax patterns the body re-parser can't handle
            // until substitution occurs at call time.
            let is_ho_view = first_def.def_type == crate::pipeline::parser::DefinitionType::HoView;

            let ddl_defs = match crate::ddl::ddl_builder::build_ddl_file(&source_to_store) {
                Ok(d) if !d.is_empty() => d,
                Ok(_) if is_ho_view => {
                    // HO view with empty result — skip validation, proceed
                    // with registration using parser-level metadata only.
                    debug!(
                        "consult_file: Skipping eager validation for HO view '{}'",
                        first_def.name
                    );
                    Vec::new()
                }
                Ok(_) => {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "DDL definition '{}' could not be compiled (no definitions produced)",
                            first_def.name
                        ),
                        "DDL body validation failed",
                    ));
                }
                Err(ref e)
                    if is_ho_view
                        && !matches!(
                            e,
                            DelightQLError::TransformationError { .. }
                                | DelightQLError::ValidationError {
                                    subcategory: Some(_),
                                    ..
                                }
                        ) =>
                {
                    // HO view body failure that's NOT a semantic constraint
                    // error. Parse failures are expected when the body has
                    // complex HO parameter syntax (V(, ...) etc.) that the
                    // DQL parser can't handle before substitution. Defer
                    // validation to call time.
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) are still propagated eagerly.
                    debug!(
                            "consult_file: Deferring validation for HO view '{}' (body needs HO substitution)",
                            first_def.name
                        );
                    Vec::new()
                }
                Err(e) => {
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) propagate directly to
                    // preserve their specific URI subcategory
                    // (e.g., dql/semantic/constraint/column_ordinal).
                    if matches!(
                        &e,
                        DelightQLError::TransformationError { .. }
                            | DelightQLError::ValidationError {
                                subcategory: Some(_),
                                ..
                            }
                    ) {
                        return Err(e);
                    }
                    // Other errors (DatabaseOperationError from body_parser,
                    // etc.) get wrapped in ValidationError so the URI is
                    // dql/semantic rather than dql/runtime.
                    return Err(DelightQLError::validation_error(
                        format!(
                            "DDL definition '{}' has an invalid body: {}",
                            first_def.name, e
                        ),
                        "DDL body validation failed",
                    ));
                }
            };

            // When ddl_defs is empty (HO view body deferred), derive
            // entity metadata from the parser-level Definition instead.
            let entity_type: i32;
            let param_names: Vec<&str>;

            if ddl_defs.is_empty() {
                // HO view with deferred body validation. Parse just the head
                // to extract HO param metadata (kind, columns) needed for
                // parameter binding at call time.
                let (_head_name, head) = crate::ddl::ddl_builder::build_ddl_head(&source_to_store)?;
                entity_type = head.entity_type_id();
                param_names = head.param_names();

                debug!(
                    "consult_file: Registering {:?} '{}' (deferred body, type={})",
                    head, first_def.name, entity_type
                );

                // Proffer-parse each clause for early validation + reference extraction.
                // Creates synthetic bindings so the body can be parsed without real call-site args.
                let proffer_table_names: std::collections::HashSet<String> = head
                    .ho_param_names()
                    .iter()
                    .map(|n| format!("__proffer__{}", n))
                    .collect();
                let mut proffer_refs: Vec<crate::ddl::analyzer::ExtractedReference> = Vec::new();

                for def in defs.iter() {
                    let bindings =
                        crate::pipeline::resolver::grounding::create_proffer_bindings(&head);
                    match crate::ddl::body_parser::parse_view_body_with_bindings(
                        &def.full_source,
                        bindings,
                    ) {
                        Ok(query) => {
                            let clause_refs =
                                crate::ddl::analyzer::extract_references_from_query(&query);
                            proffer_refs.extend(
                                clause_refs
                                    .into_iter()
                                    .filter(|r| !proffer_table_names.contains(&r.name)),
                            );
                        }
                        Err(e) => {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "HO view '{}' body has a syntax error: {}",
                                    first_def.name, e
                                ),
                                "DDL body validation failed",
                            ));
                        }
                    }
                }

                // Insert entity
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, NULL)",
                        rusqlite::params![&first_def.name, entity_type, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert consult entity",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
                let entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Insert each clause into entity_clause
                for (ordinal, def) in defs.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, ?2, ?3)",
                            rusqlite::params![entity_id, (ordinal + 1) as i32, &def.full_source],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert entity clause",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                // Record input parameters
                for (position, param_name) in param_names.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, 'input_param', ?3)",
                            rusqlite::params![entity_id, param_name, position as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert entity attribute",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                // Write HO param metadata with cross-clause position analysis
                if let crate::pipeline::asts::ddl::DdlHead::HoView { .. } = head {
                    // Parse each clause's head to get per-clause HO params
                    let mut clause_heads: Vec<crate::pipeline::asts::ddl::DdlHead> = Vec::new();
                    for def in defs.iter() {
                        match crate::ddl::ddl_builder::build_ddl_head(&def.full_source) {
                            Ok((_name, clause_head)) => clause_heads.push(clause_head),
                            Err(_) => {
                                // If head parsing fails, use the primary head for this clause
                                clause_heads.push(head.clone());
                            }
                        }
                    }
                    if clause_heads.is_empty() {
                        clause_heads.push(head.clone());
                    }

                    // Extract HoParam vecs from heads
                    let param_vecs: Vec<Vec<crate::pipeline::asts::ddl::HoParam>> = clause_heads
                        .iter()
                        .filter_map(|h| match h {
                            crate::pipeline::asts::ddl::DdlHead::HoView { params, .. } => {
                                Some(params.clone())
                            }
                            _ => None,
                        })
                        .collect();
                    let head_refs: Vec<&Vec<crate::pipeline::asts::ddl::HoParam>> =
                        param_vecs.iter().collect();

                    let positions =
                        crate::pipeline::resolver::grounding::build_ho_position_analysis_from_heads(
                            &head_refs,
                        );

                    Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
                }

                // Store proffer-extracted references
                for ext_ref in &proffer_refs {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                &ext_ref.name,
                                &ext_ref.namespace,
                                ext_ref.apparent_type,
                                entity_id,
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert referenced entity",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                // Activate in namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![entity_id, namespace_id, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to activate consult entity",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;

                continue; // Skip typed DDL processing below
            }

            // Use first clause for entity metadata (type, params)
            let first_ddl = &ddl_defs[0];

            // Validate multi-clause (disjunctive) definitions
            if ddl_defs.len() > 1 {
                use crate::pipeline::asts::ddl::DdlHead;

                fn head_kind_name(head: &DdlHead) -> &'static str {
                    match head {
                        DdlHead::Function { .. } => "function",
                        DdlHead::View => "view",
                        DdlHead::ArgumentativeView { .. } => "view",
                        DdlHead::HoView { .. } => "higher-order view",
                        DdlHead::SigmaPredicate { .. } => "sigma predicate",
                        DdlHead::Fact => "fact",
                        DdlHead::ErRule { .. } => "er-context rule",
                    }
                }

                let first_type_id = first_ddl.head.entity_type_id();
                let first_arity = first_ddl.head.param_count();

                for (i, clause) in ddl_defs.iter().enumerate().skip(1) {
                    // Rule 1: All clauses must have the same entity type
                    if clause.head.entity_type_id() != first_type_id {
                        return Err(DelightQLError::parse_error(format!(
                            "Disjunctive definition '{}': clause {} is a {} but clause 1 is a {}. \
                             All clauses must be the same kind.",
                            first_ddl.name,
                            i + 1,
                            head_kind_name(&clause.head),
                            head_kind_name(&first_ddl.head)
                        )));
                    }

                    // Rule 2: All clauses must have the same arity (counting all positions,
                    // including GroundScalar). Different clauses may have ground at different
                    // positions (e.g., clause 1: GroundScalar + Glob, clause 2: Scalar + Glob)
                    // but they must have the same total number of positions.
                    let clause_arity = clause.head.param_count();
                    if clause_arity != first_arity {
                        return Err(DelightQLError::parse_error(format!(
                            "Disjunctive definition '{}': clause {} has {} parameter(s) but clause 1 has {}. \
                             All clauses must have the same arity.",
                            first_ddl.name, i + 1, clause_arity, first_arity
                        )));
                    }
                }

                // Note: argumentative head contract validation (mixed forms, arity, name conflict)
                // is done at expansion time in grounding::desugar_argumentative_defs so that
                // error assertions on query lines can catch the errors.

                // Rules 3 & 4: For functions with guards, at most one unguarded clause (must be last)
                if let DdlHead::Function { .. } = &first_ddl.head {
                    let has_any_guard = ddl_defs.iter().any(|d| {
                        if let DdlHead::Function { params, .. } = &d.head {
                            params.iter().any(|p| p.guard.is_some())
                        } else {
                            false
                        }
                    });

                    if has_any_guard {
                        let unguarded_indices: Vec<usize> = ddl_defs
                            .iter()
                            .enumerate()
                            .filter(|(_, d)| {
                                if let DdlHead::Function { params, .. } = &d.head {
                                    params.iter().all(|p| p.guard.is_none())
                                } else {
                                    true
                                }
                            })
                            .map(|(i, _)| i)
                            .collect();

                        // Rule 3: At most one unguarded clause
                        if unguarded_indices.len() > 1 {
                            return Err(DelightQLError::parse_error(format!(
                                "Disjunctive definition '{}': found {} unguarded clauses. \
                                 At most one clause may omit a guard (it becomes the ELSE/default).",
                                first_ddl.name, unguarded_indices.len()
                            )));
                        }

                        // Rule 4: Unguarded clause must be last
                        if let Some(&idx) = unguarded_indices.first() {
                            if idx != ddl_defs.len() - 1 {
                                return Err(DelightQLError::parse_error(format!(
                                    "Disjunctive definition '{}': unguarded clause is at position {} \
                                     but must be the last clause (position {}). \
                                     Move the default clause to the end.",
                                    first_ddl.name, idx + 1, ddl_defs.len()
                                )));
                            }
                        }
                    }
                }
            }

            debug!(
                "consult_file: Registering {:?} '{}' ({} clause{})",
                first_ddl.head,
                first_ddl.name,
                ddl_defs.len(),
                if ddl_defs.len() > 1 { "s" } else { "" }
            );

            entity_type = first_ddl.head.entity_type_id();
            param_names = first_ddl.head.param_names();

            // Insert entity (without definition — clauses go into entity_clause)
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![&first_ddl.name, entity_type, cartridge_id, &first_ddl.doc],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert consult entity",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let entity_id = bootstrap_conn.last_insert_rowid() as i32;

            // Insert each clause into entity_clause
            for (ordinal, def) in defs.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, ?2, ?3)",
                        rusqlite::params![entity_id, (ordinal + 1) as i32, &def.full_source],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert entity clause",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }

            // Record input parameters as entity attributes (from first clause)
            for (position, param_name) in param_names.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, 'input_param', ?3)",
                        rusqlite::params![entity_id, param_name, position as i32],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert entity attribute",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }

            // For HO views, write structured param metadata with cross-clause position analysis
            if matches!(
                first_ddl.head,
                crate::pipeline::asts::ddl::DdlHead::HoView { .. }
            ) {
                let positions =
                    crate::pipeline::resolver::grounding::build_ho_position_analysis(&ddl_defs);
                Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
            }

            // For ER-rules, write metadata to er_rule table
            // Each clause may have a different context, so iterate all clauses.
            // Use enumerate() so clause_ordinal (1-indexed) matches entity_clause.ordinal.
            for (idx, ddl_def) in ddl_defs.iter().enumerate() {
                if let crate::pipeline::asts::ddl::DdlHead::ErRule {
                    ref left_table,
                    ref right_table,
                    ref context,
                } = ddl_def.head
                {
                    // Canonical ordering: alphabetical pair
                    let (left, right) = if left_table <= right_table {
                        (left_table.as_str(), right_table.as_str())
                    } else {
                        (right_table.as_str(), left_table.as_str())
                    };
                    bootstrap_conn
                        .execute(
                            "INSERT INTO er_rule (entity_id, left_table, right_table, context_name, clause_ordinal) VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![entity_id, left, right, context, (idx + 1) as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert er_rule",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }
            }

            // Extract references from ALL clauses (union of references)
            {
                use crate::pipeline::asts::ddl::DdlBody;
                let mut all_refs = Vec::new();
                for ddl_def in &ddl_defs {
                    let clause_refs = match &ddl_def.body {
                        DdlBody::Scalar(expr) => {
                            crate::ddl::analyzer::extract_references_from_domain(expr)
                        }
                        DdlBody::Relational(query) => {
                            crate::ddl::analyzer::extract_references_from_query(query)
                        }
                    };
                    all_refs.extend(clause_refs);
                }

                // Filter out bound parameters from free variable references.
                // HO view params like T in active_only(T)(*) are bound, not free.
                let refs: Vec<_> = all_refs
                    .into_iter()
                    .filter(|r| !param_names.contains(&r.name.as_str()))
                    .collect();

                for ext_ref in &refs {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                &ext_ref.name,
                                &ext_ref.namespace,
                                ext_ref.apparent_type,
                                entity_id,
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert referenced entity",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                debug!(
                    "consult_file: Extracted {} references from '{}' ({} clause{})",
                    refs.len(),
                    first_ddl.name,
                    ddl_defs.len(),
                    if ddl_defs.len() > 1 { "s" } else { "" }
                );
            }

            // Register interior schemas for tree group columns
            {
                use crate::pipeline::asts::ddl::DdlBody;
                for ddl_def in &ddl_defs {
                    if let DdlBody::Relational(query) = &ddl_def.body {
                        register_interior_schemas_from_query(bootstrap_conn, entity_id, query)?;
                    }
                }
            }

            // Activate in namespace
            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, namespace_id, cartridge_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to activate consult entity",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
        }

        debug!(
            "consult_file: Successfully loaded {} definitions into '{}'",
            count, namespace
        );
        Ok(ConsultResult {
            definitions_loaded: count,
            replaced_entities,
        })
    }

    /// Engage a namespace (enables unqualified entity resolution)
    ///
    /// Creates an enlisted_namespace record in bootstrap, allowing entities from
    /// the specified namespace to be resolved without qualification.
    ///
    /// # Arguments
    /// * `namespace` - The namespace path to enlist (e.g., "mfg", "std::string")
    ///
    /// # Returns
    /// * `Ok(())` - Namespace enlisted successfully
    /// * `Err(...)` - Namespace not found or enlist failed
    pub fn enlist_namespace(&mut self, namespace: &str) -> Result<()> {
        // Lazy-load stdlib module if needed (e.g., "std::reshape")
        self.ensure_stdlib_loaded(namespace);

        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for enlist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID
        let from_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found. Make sure to mount!() it first.",
                        namespace
                    ),
                    e.to_string(),
                )
            })?;

        // Get the "main" namespace ID (to_namespace = "main")
        // This is the default namespace where entities are enlisted when no target is specified
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Default namespace 'main' not found in bootstrap (database corruption)",
                    e.to_string(),
                )
            })?;

        // Check for ER-context name collisions with already-enlisted namespaces.
        // Two enlisted namespaces with the same context name create ambiguity for
        // `under ctx:` lookups that search enlisted namespaces.
        {
            let mut collision_stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT new_er.context_name, existing_ns.fq_name
                     FROM er_rule new_er
                     JOIN entity new_e ON new_e.id = new_er.entity_id
                     JOIN activated_entity new_ae ON new_ae.entity_id = new_e.id
                        AND new_ae.namespace_id = ?1
                     JOIN er_rule existing_er ON existing_er.context_name = new_er.context_name
                     JOIN entity existing_e ON existing_e.id = existing_er.entity_id
                     JOIN activated_entity existing_ae ON existing_ae.entity_id = existing_e.id
                     JOIN namespace existing_ns ON existing_ns.id = existing_ae.namespace_id
                     JOIN enlisted_namespace en ON en.from_namespace_id = existing_ns.id
                        AND en.to_namespace_id = ?2
                     WHERE existing_ns.id != ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to prepare ER-context collision check",
                        e.to_string(),
                    )
                })?;

            let collisions: Vec<(String, String)> = collision_stmt
                .query_map(
                    rusqlite::params![from_namespace_id, to_namespace_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to check ER-context collisions",
                        e.to_string(),
                    )
                })?
                .filter_map(|r| r.ok())
                .collect();

            if !collisions.is_empty() {
                let details: Vec<String> = collisions
                    .iter()
                    .map(|(ctx, ns)| format!("context '{}' (already enlisted from '{}')", ctx, ns))
                    .collect();
                return Err(DelightQLError::validation_error(
                    format!(
                        "Cannot enlist namespace '{}': ER-context name collision — {}. \
                         Use qualified access (ns.view(*)) instead of enlist to avoid ambiguity.",
                        namespace,
                        details.join(", "),
                    ),
                    "ER-context collision on enlist",
                ));
            }
        }

        // Insert enlisted_namespace record (or ignore if already enlisted)
        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                [from_namespace_id, to_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to enlist namespace '{}': {}", namespace, e),
                    e.to_string(),
                )
            })?;

        debug!(
            "enlist_namespace: Enlisted '{}' into default namespace",
            namespace
        );

        // Explicitly drop the bootstrap connection lock
        drop(bootstrap_conn);

        Ok(())
    }

    /// Record that `exposing_ns` re-exports `exposed_ns` through its facade.
    /// When someone enlists `exposing_ns`, entities from `exposed_ns` become
    /// visible via a recursive CTE at resolution time.
    pub fn expose_namespace(&mut self, exposing_ns: &str, exposed_ns: &str) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for expose",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let exposing_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [exposing_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found for expose", exposing_ns),
                    "Namespace not found",
                )
            })?;

        let exposed_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [exposed_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found for expose", exposed_ns),
                    "Namespace not found",
                )
            })?;

        // Validate: exposed must be a child of exposing
        if !exposed_ns.starts_with(&format!("{}::", exposing_ns)) {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot expose '{}' through '{}': not a child namespace",
                    exposed_ns, exposing_ns
                ),
                "Invalid expose target",
            ));
        }

        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO exposed_namespace
                 (exposing_namespace_id, exposed_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![exposing_id, exposed_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to expose namespace '{}': {}", exposed_ns, e),
                    e.to_string(),
                )
            })?;

        debug!(
            "expose_namespace: '{}' now re-exports '{}'",
            exposing_ns, exposed_ns
        );

        Ok(())
    }

    /// Register a namespace alias (e.g., "l" → "lib::math")
    ///
    /// Creates a namespace_alias record in bootstrap, allowing a short alias
    /// to be used in place of a fully-qualified namespace path.
    pub fn register_namespace_alias(&mut self, alias: &str, namespace: &str) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for namespace alias",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found. Cannot create alias '{}'.",
                        namespace, alias
                    ),
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "INSERT OR REPLACE INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![alias, ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to register namespace alias '{}' → '{}': {}",
                        alias, namespace, e
                    ),
                    e.to_string(),
                )
            })?;

        debug!("register_namespace_alias: '{}' → '{}'", alias, namespace);

        drop(bootstrap_conn);
        Ok(())
    }

    /// Delist a namespace (disables unqualified entity resolution)
    ///
    /// Removes the enlisted_namespace record from bootstrap, preventing entities
    /// from the specified namespace from being resolved without qualification.
    /// Qualified access (e.g., `mfg.suppliers(*)`) still works after delist.
    ///
    /// # Arguments
    /// * `namespace` - The namespace path to delist (e.g., "mfg", "std::string")
    ///
    /// # Returns
    /// * `Ok(())` - Namespace delisted successfully
    /// * `Err(...)` - Namespace not found or delist failed
    pub fn delist_namespace(&mut self, namespace: &str) -> Result<()> {
        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for delist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID
        let from_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    e.to_string(),
                )
            })?;

        // Get the "main" namespace ID (to_namespace = "main")
        // This is the default namespace where entities are enlisted when no target is specified
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Default namespace 'main' not found in bootstrap (database corruption)",
                    e.to_string(),
                )
            })?;

        // Delete enlisted_namespace record
        let rows_affected = bootstrap_conn
            .execute(
                "DELETE FROM enlisted_namespace
                 WHERE from_namespace_id = ?1 AND to_namespace_id = ?2",
                [from_namespace_id, to_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to delist namespace '{}': {}", namespace, e),
                    e.to_string(),
                )
            })?;

        if rows_affected == 0 {
            return Err(DelightQLError::database_error_categorized(
                "useafterfree",
                format!("Namespace '{}' is not currently enlisted", namespace),
                "delist!() requires a prior enlist!() on the same namespace",
            ));
        } else {
            debug!("delist_namespace: Delisted namespace '{}'", namespace);
        }

        // Also clean up any namespace aliases pointing to this namespace
        bootstrap_conn
            .execute(
                "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
                [from_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to clean up aliases for namespace '{}': {}",
                        namespace, e
                    ),
                    e.to_string(),
                )
            })?;

        // Explicitly drop the bootstrap connection lock
        drop(bootstrap_conn);

        Ok(())
    }

    /// Snapshot the current enlisted_namespace state.
    /// Returns all (from_namespace_id, to_namespace_id) rows for later restoration.
    pub fn save_enlisted_state(&self) -> Result<Vec<(i32, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for save_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT from_namespace_id, to_namespace_id FROM enlisted_namespace")
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted_namespace snapshot",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to snapshot enlisted_namespace",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore the enlisted_namespace state from a previous snapshot.
    /// Deletes all current rows and re-inserts the saved ones.
    pub fn restore_enlisted_state(&mut self, saved: &[(i32, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for restore_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM enlisted_namespace", [])
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to clear enlisted_namespace for restore",
                    e.to_string(),
                )
            })?;

        for (from_id, to_id) in saved {
            bootstrap_conn
                .execute(
                    "INSERT INTO enlisted_namespace (from_namespace_id, to_namespace_id) VALUES (?1, ?2)",
                    [from_id, to_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to restore enlisted_namespace row",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Record which namespaces were enlisted inside a DDL as namespace-local dependencies.
    /// The enlisted_namespace rows (from_namespace_id, to_namespace_id) represent the delta
    /// of enlists that happened during the DDL. We store them as local dependencies of the
    /// DDL's namespace so the resolver can activate them during view body resolution.
    pub fn record_namespace_local_enlists(
        &mut self,
        namespace: &str,
        new_enlists: &[(i32, i32)],
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for record_namespace_local_enlists",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID
        let namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found for local enlist recording",
                        namespace
                    ),
                    e.to_string(),
                )
            })?;

        for (from_ns_id, _to_ns_id) in new_enlists {
            // The enlist was (from=enlisted_ns, to=main).
            // We record it as: namespace_id depends on from_ns_id.
            bootstrap_conn
                .execute(
                    "INSERT OR IGNORE INTO namespace_local_enlist (namespace_id, enlisted_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![namespace_id, from_ns_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to record namespace local enlist",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Snapshot the current namespace_alias state.
    /// Returns all (alias, target_namespace_id) rows for later restoration.
    pub fn save_alias_state(&self) -> Result<Vec<(String, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for save_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT alias, target_namespace_id FROM namespace_alias")
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare namespace_alias snapshot",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(String, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| {
                DelightQLError::database_error("Failed to snapshot namespace_alias", e.to_string())
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore namespace_alias to a previously saved state.
    pub fn restore_alias_state(&mut self, saved: &[(String, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for restore_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM namespace_alias", [])
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to clear namespace_alias for restore",
                    e.to_string(),
                )
            })?;

        for (alias, target_id) in saved {
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![alias, target_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to restore namespace_alias row",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Record which namespace aliases were created inside a DDL file.
    /// These are scoped to the DDL's namespace.
    pub fn record_namespace_local_aliases(
        &mut self,
        namespace: &str,
        new_aliases: &[(String, i32)],
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for record_namespace_local_aliases",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found for local alias recording",
                        namespace
                    ),
                    e.to_string(),
                )
            })?;

        for (alias, target_ns_id) in new_aliases {
            bootstrap_conn
                .execute(
                    "INSERT OR IGNORE INTO namespace_local_alias (namespace_id, alias, target_namespace_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![namespace_id, alias, target_ns_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to record namespace local alias",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Destroy a namespace and cascade-delete all its bootstrap metadata.
    ///
    /// Returns `(connection_id, source_ns)` from the cartridge so the caller
    /// can handle physical cleanup (DETACH, connection_map removal).
    fn destroy_namespace(&mut self, namespace_fq: &str) -> Result<(Option<i64>, Option<String>)> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for destroy_namespace",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up namespace
        let namespace_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace_fq],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace_fq),
                    "Namespace not found",
                )
            })?;

        // Find ALL cartridge(s) and their connection info
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                     FROM cartridge c
                     JOIN entity e ON e.cartridge_id = c.id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     WHERE ae.namespace_id = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query cartridges", e.to_string())
                })?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query cartridges", e.to_string())
                })?;
            rows.flatten().collect()
        };

        let (connection_id, source_ns) = cartridge_infos
            .first()
            .map(|(_, conn_id, src_ns)| (*conn_id, src_ns.clone()))
            .unwrap_or((None, None));

        // Cascade delete — order matters for FK constraints
        // 1. Namespace linking tables
        bootstrap_conn.execute(
            "DELETE FROM namespace_local_alias WHERE namespace_id = ?1 OR target_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete namespace_local_alias", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM namespace_local_enlist WHERE namespace_id = ?1 OR enlisted_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete namespace_local_enlist", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM enlisted_entity WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete enlisted_entity", e.to_string())
            })?;

        bootstrap_conn.execute(
            "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete enlisted_namespace", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1 OR exposed_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete exposed_namespace", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete namespace_alias", e.to_string())
            })?;

        // 2. Grounding table
        bootstrap_conn
            .execute(
                "DELETE FROM grounding WHERE grounded_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete grounding", e.to_string())
            })?;

        // 3. Entity-level tables (via cartridge)
        if !cartridge_infos.is_empty() {
            for (cartridge_id, _, _) in &cartridge_infos {
                // interior_entity_attribute (FK to interior_entity)
                bootstrap_conn.execute(
                    "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
                        SELECT ie.id FROM interior_entity ie JOIN entity e ON ie.parent_entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity_attribute", e.to_string()))?;

                // interior_entity
                bootstrap_conn.execute(
                    "DELETE FROM interior_entity WHERE parent_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity", e.to_string()))?;

                // ho_param_ground_value (FK to ho_param)
                bootstrap_conn
                    .execute(
                        "DELETE FROM ho_param_ground_value WHERE ho_param_id IN (
                        SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                        [cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to delete ho_param_ground_value",
                            e.to_string(),
                        )
                    })?;

                // ho_param_column (FK to ho_param)
                bootstrap_conn
                    .execute(
                        "DELETE FROM ho_param_column WHERE ho_param_id IN (
                        SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                        [cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to delete ho_param_column",
                            e.to_string(),
                        )
                    })?;

                // entity_resolution
                bootstrap_conn.execute(
                    "DELETE FROM entity_resolution WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_resolution", e.to_string()))?;

                // ho_param
                bootstrap_conn.execute(
                    "DELETE FROM ho_param WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete ho_param", e.to_string()))?;

                // er_rule
                bootstrap_conn.execute(
                    "DELETE FROM er_rule WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete er_rule", e.to_string()))?;

                // referenced_entity
                bootstrap_conn.execute(
                    "DELETE FROM referenced_entity WHERE containing_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete referenced_entity", e.to_string()))?;

                // entity_attribute
                bootstrap_conn.execute(
                    "DELETE FROM entity_attribute WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_attribute", e.to_string()))?;

                // entity_clause
                bootstrap_conn.execute(
                    "DELETE FROM entity_clause WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_clause", e.to_string()))?;

                // activated_entity
                bootstrap_conn.execute(
                    "DELETE FROM activated_entity WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete activated_entity", e.to_string()))?;

                // entity
                bootstrap_conn
                    .execute("DELETE FROM entity WHERE cartridge_id = ?1", [cartridge_id])
                    .map_err(|e| {
                        DelightQLError::database_error("Failed to delete entity", e.to_string())
                    })?;

                // cartridge
                bootstrap_conn
                    .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
                    .map_err(|e| {
                        DelightQLError::database_error("Failed to delete cartridge", e.to_string())
                    })?;
            }
        } else {
            // No cartridge — still clean up activated_entity rows referencing this namespace
            bootstrap_conn
                .execute(
                    "DELETE FROM activated_entity WHERE namespace_id = ?1",
                    [namespace_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to delete activated_entity",
                        e.to_string(),
                    )
                })?;
        }

        // 4. Delete namespace itself
        bootstrap_conn
            .execute("DELETE FROM namespace WHERE id = ?1", [namespace_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete namespace", e.to_string())
            })?;

        drop(bootstrap_conn);

        Ok((connection_id, source_ns))
    }

    /// Unmount a data namespace, releasing its database connection.
    ///
    /// Validates the namespace is of kind 'data' and is not borrowed by any
    /// grounded namespace. If clear, cascade-deletes all bootstrap metadata
    /// and performs physical cleanup (DETACH or connection_map removal).
    pub fn unmount_database(&mut self, namespace: &str) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for unmount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and is 'data' kind
        let (_ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        if kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot unmount '{}' — it is a {} namespace. Use unconsult!() for lib/grounded namespaces.",
                    namespace, kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Discover all descendant namespaces (for cascade)
        let pattern = format!("{}::%", namespace);
        let descendants: Vec<(String, String)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT fq_name, kind FROM namespace
                     WHERE fq_name LIKE ?1
                     ORDER BY length(fq_name) DESC",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([&pattern], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // 3. Borrow check: parent + descendants against external borrowers
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
                     JOIN namespace n_source ON n_source.id = g.data_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = borrower_info {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unmount '{}' — {} is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }

            // Also check lib borrows from descendants
            let lib_borrower: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
                     JOIN namespace n_source ON n_source.id = g.lib_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = lib_borrower {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unmount '{}' — {} is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }
        }

        drop(bootstrap_conn);

        // 4. Cascade delete: descendants first (deepest first), then parent
        let mut schemas_to_detach: Vec<String> = Vec::new();
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, source_ns) = self.destroy_namespace(desc_fq)?;
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        self.connection_map.remove(&conn_id);
                        self.schema_map.remove(&conn_id);
                    }
                }
                if let Some(schema) = source_ns {
                    schemas_to_detach.push(schema);
                }
            }
        }
        let (connection_id, source_ns) = self.destroy_namespace(namespace)?;

        // 5. Physical cleanup for parent
        if let Some(conn_id) = connection_id {
            if conn_id > 2 {
                self.connection_map.remove(&conn_id);
                self.schema_map.remove(&conn_id);
            }
        }
        if let Some(schema) = source_ns {
            schemas_to_detach.push(schema);
        }

        // 6. DETACH all released schemas from the user connection
        if !schemas_to_detach.is_empty() {
            let user_conn = self.connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire user connection lock for unmount detach",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            for schema in &schemas_to_detach {
                let _ = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]);
            }
        }

        debug!(
            "unmount_database: Unmounted namespace '{}' (cascade-deleted {} descendants)",
            namespace,
            descendants.len()
        );
        Ok(())
    }

    /// Unconsult a lib/grounded/scratch namespace, removing all its definitions.
    ///
    /// Validates the namespace is not of kind 'data' or 'system'. For lib namespaces,
    /// checks that no grounded namespace borrows from it. Then cascade-deletes all
    /// bootstrap metadata.
    pub fn unconsult_namespace(&mut self, namespace: &str) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for unconsult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and check kind
        let (_ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        match kind.as_str() {
            "data" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — it is a data namespace. Use unmount!() instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "system" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — system namespaces cannot be removed.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "lib" | "grounded" | "scratch" | "unknown" => {
                // These are all acceptable for unconsult
            }
            other => panic!(
                "catch-all hit in system.rs unconsult_namespace: unexpected namespace kind: {}",
                other
            ),
        }

        // 2. Discover all descendant namespaces (deepest first for bottom-up deletion)
        let pattern = format!("{}::%", namespace);
        let descendants: Vec<(String, String)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT fq_name, kind FROM namespace
                     WHERE fq_name LIKE ?1
                     ORDER BY length(fq_name) DESC",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([&pattern], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // 3. Borrow check: find external borrowers of any namespace in the tree
        //    (lib borrowed as lib_namespace_id, data borrowed as data_namespace_id,
        //     but only if the borrower is OUTSIDE the tree)
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
                     JOIN namespace n_source ON n_source.id = g.lib_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = borrower_info {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — descendant '{}' is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }

            // Also check data namespace borrows
            let data_borrower: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
                     JOIN namespace n_source ON n_source.id = g.data_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = data_borrower {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — descendant '{}' is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }
        }

        drop(bootstrap_conn);

        // 4. Cascade delete: descendants first (deepest first), then parent
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, _source_ns) = self.destroy_namespace(desc_fq)?;
            // Physical cleanup for data descendants
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        self.connection_map.remove(&conn_id);
                        self.schema_map.remove(&conn_id);
                    }
                }
            }
        }
        let _result = self.destroy_namespace(namespace)?;

        debug!(
            "unconsult_namespace: Unconsulted namespace '{}' (cascade-deleted {} descendants)",
            namespace,
            descendants.len()
        );
        Ok(())
    }

    /// Write HO parameter metadata to bootstrap from cross-clause position analysis.
    ///
    /// Inserts rows into ho_param, ho_param_column, and ho_param_ground_value
    /// based on the unified HoPositionInfo computed by `build_ho_position_analysis`.
    fn write_ho_params_to_bootstrap(
        bootstrap_conn: &Connection,
        entity_id: i32,
        positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    ) -> Result<()> {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};

        for pos_info in positions {
            let kind_str = match &pos_info.column_kind {
                HoColumnKind::TableGlob => "glob",
                HoColumnKind::TableArgumentative(_) => "argumentative",
                HoColumnKind::Scalar => match &pos_info.ground_mode {
                    HoGroundMode::PureGround => "ground_scalar",
                    HoGroundMode::MixedGround => "scalar",
                    _ => "scalar",
                },
            };

            let ground_mode_str = match &pos_info.ground_mode {
                HoGroundMode::PureGround => Some("pure_ground"),
                HoGroundMode::MixedGround => Some("mixed_ground"),
                HoGroundMode::PureUnbound => Some("pure_unbound"),
                HoGroundMode::InputOnly => Some("input_only"),
            };

            // Use column_name for param_name when available, fall back to position-based name
            let param_name_owned;
            let param_name = match &pos_info.column_name {
                Some(name) => name.as_str(),
                None => {
                    param_name_owned = format!("_pos{}", pos_info.position);
                    &param_name_owned
                }
            };

            bootstrap_conn
                .execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, ground_mode, column_name) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![entity_id, param_name, pos_info.position as i32, kind_str, ground_mode_str, &pos_info.column_name],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert ho_param",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let ho_param_id = bootstrap_conn.last_insert_rowid() as i32;

            // Write argumentative columns
            if let HoColumnKind::TableArgumentative(ref columns) = pos_info.column_kind {
                for (col_pos, col_name) in columns.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO ho_param_column (ho_param_id, column_name, column_position) VALUES (?1, ?2, ?3)",
                            rusqlite::params![ho_param_id, col_name, col_pos as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert ho_param_column",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }
            }

            // Write per-clause ground values
            for (clause_ordinal, ground_value) in &pos_info.ground_values {
                bootstrap_conn
                    .execute(
                        "INSERT INTO ho_param_ground_value (ho_param_id, clause_ordinal, ground_value) VALUES (?1, ?2, ?3)",
                        rusqlite::params![ho_param_id, *clause_ordinal as i32, ground_value],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert ho_param_ground_value",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }
        }

        Ok(())
    }

    /// Deep-copy all sub-tables for an entity (clause, attribute, referenced,
    /// ho_param+columns, er_rule, interior_entity+attributes).
    fn copy_entity_subtables(
        conn: &Connection,
        old_entity_id: i32,
        new_entity_id: i32,
    ) -> Result<()> {
        // entity_clause
        conn.execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition, location)
             SELECT ?1, ordinal, definition, location
             FROM entity_clause WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to copy entity_clause", e.to_string())
        })?;

        // entity_attribute
        conn.execute(
            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
             SELECT ?1, attribute_name, attribute_type, data_type, position, is_nullable, default_value
             FROM entity_attribute WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        ).map_err(|e| DelightQLError::database_error("Failed to copy entity_attribute", e.to_string()))?;

        // referenced_entity
        conn.execute(
            "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id, location)
             SELECT name, namespace, apparent_type, ?1, location
             FROM referenced_entity WHERE containing_entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        ).map_err(|e| DelightQLError::database_error("Failed to copy referenced_entity", e.to_string()))?;

        // ho_param + ho_param_column + ho_param_ground_value (FK chain: entity → ho_param → children)
        {
            let mut stmt = conn
                .prepare("SELECT id, param_name, position, kind, ground_mode, column_name FROM ho_param WHERE entity_id = ?1")
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?;
            let old_params: Vec<(i32, String, i32, String, Option<String>, Option<String>)> = stmt
                .query_map([old_entity_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                })
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?
                .flatten()
                .collect();

            for (old_hp_id, param_name, position, kind, ground_mode, column_name) in &old_params {
                conn.execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, ground_mode, column_name) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![new_entity_id, param_name, position, kind, ground_mode, column_name],
                ).map_err(|e| DelightQLError::database_error("Failed to copy ho_param", e.to_string()))?;
                let new_hp_id = conn.last_insert_rowid() as i32;

                conn.execute(
                    "INSERT INTO ho_param_column (ho_param_id, column_name, column_position)
                     SELECT ?1, column_name, column_position
                     FROM ho_param_column WHERE ho_param_id = ?2",
                    rusqlite::params![new_hp_id, old_hp_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to copy ho_param_column", e.to_string())
                })?;

                conn.execute(
                    "INSERT INTO ho_param_ground_value (ho_param_id, clause_ordinal, ground_value)
                     SELECT ?1, clause_ordinal, ground_value
                     FROM ho_param_ground_value WHERE ho_param_id = ?2",
                    rusqlite::params![new_hp_id, old_hp_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to copy ho_param_ground_value",
                        e.to_string(),
                    )
                })?;
            }
        }

        // er_rule
        conn.execute(
            "INSERT INTO er_rule (entity_id, left_table, right_table, context_name, clause_ordinal)
             SELECT ?1, left_table, right_table, context_name, clause_ordinal
             FROM er_rule WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| DelightQLError::database_error("Failed to copy er_rule", e.to_string()))?;

        // interior_entity + interior_entity_attribute (FK chain)
        {
            let mut stmt = conn
                .prepare("SELECT id, column_name FROM interior_entity WHERE parent_entity_id = ?1")
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query interior_entity", e.to_string())
                })?;
            let old_ies: Vec<(i32, String)> = stmt
                .query_map([old_entity_id], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query interior_entity", e.to_string())
                })?
                .flatten()
                .collect();

            for (old_ie_id, column_name) in &old_ies {
                conn.execute(
                    "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
                    rusqlite::params![new_entity_id, column_name],
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to copy interior_entity", e.to_string())
                })?;
                let new_ie_id = conn.last_insert_rowid() as i32;

                conn.execute(
                    "INSERT INTO interior_entity_attribute (interior_entity_id, attribute_name, position, child_interior_entity_id)
                     SELECT ?1, attribute_name, position, child_interior_entity_id
                     FROM interior_entity_attribute WHERE interior_entity_id = ?2",
                    rusqlite::params![new_ie_id, old_ie_id],
                ).map_err(|e| DelightQLError::database_error("Failed to copy interior_entity_attribute", e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Delete all entity sub-tables and the cartridge row for a single cartridge.
    /// FK-safe deletion order: interior_entity_attribute, interior_entity,
    /// ho_param_ground_value, ho_param_column, entity_resolution, ho_param, er_rule,
    /// referenced_entity, entity_attribute, entity_clause, activated_entity, entity, cartridge.
    fn clear_cartridge_entities(bootstrap_conn: &Connection, cartridge_id: i64) -> Result<()> {
        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
                SELECT ie.id FROM interior_entity ie JOIN entity e ON ie.parent_entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete interior_entity_attribute",
                    e.to_string(),
                )
            })?;

        bootstrap_conn.execute(
            "DELETE FROM interior_entity WHERE parent_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM ho_param_ground_value WHERE ho_param_id IN (
                SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete ho_param_ground_value",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM ho_param_column WHERE ho_param_id IN (
                SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param_column", e.to_string())
            })?;

        bootstrap_conn.execute(
            "DELETE FROM entity_resolution WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_resolution", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM ho_param WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete ho_param", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM er_rule WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete er_rule", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM referenced_entity WHERE containing_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete referenced_entity", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM entity_attribute WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_attribute", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM entity_clause WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_clause", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM activated_entity WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete activated_entity", e.to_string()))?;

        bootstrap_conn
            .execute("DELETE FROM entity WHERE cartridge_id = ?1", [cartridge_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete cartridge", e.to_string())
            })?;

        Ok(())
    }

    /// Delete a single entity and all its sub-table rows.
    /// FK-safe deletion order matching clear_cartridge_entities.
    /// Does NOT delete the parent cartridge (caller may have other entities in it).
    fn clear_single_entity(bootstrap_conn: &Connection, entity_id: i64) -> Result<()> {
        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
            SELECT ie.id FROM interior_entity ie WHERE ie.parent_entity_id = ?1)",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete interior_entity_attribute",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity WHERE parent_entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete interior_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM ho_param_column WHERE ho_param_id IN (
            SELECT hp.id FROM ho_param hp WHERE hp.entity_id = ?1)",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param_column", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_resolution WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_resolution", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM ho_param WHERE entity_id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM er_rule WHERE entity_id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete er_rule", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM referenced_entity WHERE containing_entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete referenced_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_attribute WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_attribute", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_clause WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_clause", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM activated_entity WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete activated_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM entity WHERE id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity", e.to_string())
            })?;

        Ok(())
    }

    /// Clear all content tables for a namespace, preserving identity/shell.
    ///
    /// Deletes: cartridge(s), entity(+all sub-tables), activated_entity,
    /// namespace_local_enlist, namespace_local_alias.
    /// Preserves: namespace row, enlisted_namespace, enlisted_entity,
    /// namespace_alias, grounding.
    ///
    /// Returns deleted cartridge metadata for physical cleanup by caller.
    fn clear_namespace_contents(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<Vec<(i64, Option<i64>, Option<String>)>> {
        // Collect ALL cartridge IDs for this namespace
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query cartridges for clear",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query cartridges for clear",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // Delete entity sub-tables for each cartridge in FK-safe order
        for (cartridge_id, _, _) in &cartridge_infos {
            Self::clear_cartridge_entities(bootstrap_conn, *cartridge_id)?;
        }

        // Clean up namespace-local tables
        bootstrap_conn
            .execute(
                "DELETE FROM namespace_local_enlist WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete namespace_local_enlist",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM namespace_local_alias WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete namespace_local_alias",
                    e.to_string(),
                )
            })?;

        // Safety: catch orphan activated_entity rows
        bootstrap_conn
            .execute(
                "DELETE FROM activated_entity WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete activated_entity orphans",
                    e.to_string(),
                )
            })?;

        Ok(cartridge_infos)
    }

    /// Check that all unqualified references in a lib namespace resolve against
    /// a data namespace. Returns Ok(()) if contract holds, or Err with details.
    fn validate_grounding_contract(
        bootstrap_conn: &Connection,
        lib_ns_id: i64,
        lib_ns_fq: &str,
        data_ns_id: i64,
        data_ns_fq: &str,
    ) -> Result<()> {
        let mut stmt = bootstrap_conn
            .prepare(
                "SELECT DISTINCT re.name, e.name
             FROM referenced_entity re
             JOIN entity e ON re.containing_entity_id = e.id
             JOIN activated_entity ae ON ae.entity_id = e.id
             WHERE ae.namespace_id = ?1
               AND re.namespace IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM entity e2
                   JOIN activated_entity ae2 ON ae2.entity_id = e2.id
                   WHERE ae2.namespace_id = ?2
                     AND e2.name = re.name COLLATE NOCASE
               )",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare grounding contract query",
                    e.to_string(),
                )
            })?;

        let broken: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![lib_ns_id, data_ns_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to query grounding contract", e.to_string())
            })?
            .flatten()
            .collect();

        if !broken.is_empty() {
            let details: Vec<String> = broken
                .iter()
                .map(|(ref_name, entity_name)| {
                    format!(
                        "'{}' references '{}' (not in '{}')",
                        entity_name, ref_name, data_ns_fq
                    )
                })
                .collect();
            return Err(DelightQLError::database_error(
                format!(
                    "Grounding contract violation: lib '{}' → data '{}'. Broken references: {}",
                    lib_ns_fq,
                    data_ns_fq,
                    details.join("; ")
                ),
                "Grounding contract violated",
            ));
        }
        Ok(())
    }

    /// Rebuild a grounded namespace's entity copies from its source lib namespace.
    fn rebuild_grounded_namespace(
        bootstrap_conn: &Connection,
        grounded_ns_id: i64,
        lib_ns_fq: &str,
        data_ns_fq: &str,
    ) -> Result<usize> {
        // Clear old contents
        Self::clear_namespace_contents(bootstrap_conn, grounded_ns_id)?;

        // Retrieve lib entities
        let entities: Vec<(i32, String, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.type, e.doc
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE n.fq_name = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib entities for rebuild",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([lib_ns_fq], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib entities for rebuild",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // Create new cartridge
        bootstrap_conn.execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
            rusqlite::params![
                1, // DqlStandard
                SourceType::File.as_i32(),
                &format!("ground://{}<-{}", lib_ns_fq, data_ns_fq),
                rusqlite::types::Null,
                1, // bootstrap connection
            ],
        ).map_err(|e| DelightQLError::database_error("Failed to create rebuild cartridge", e.to_string()))?;
        let cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        let count = entities.len();
        for (old_entity_id, entity_name, entity_type, entity_doc) in &entities {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![entity_name, entity_type, cartridge_id, entity_doc],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to copy entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
            let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

            Self::copy_entity_subtables(bootstrap_conn, *old_entity_id, new_entity_id)?;

            bootstrap_conn.execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![new_entity_id, grounded_ns_id, cartridge_id],
            ).map_err(|e| DelightQLError::database_error(format!("Failed to activate entity '{}'", entity_name), e.to_string()))?;
        }

        Ok(count)
    }

    /// Ground a lib namespace into a new namespace, binding it to a data namespace
    ///
    /// Validates all entities in `lib_ns` resolve against `data_ns`, then creates
    /// a new namespace with copies of those entities pre-bound to the data namespace.
    /// The new namespace has `default_data_ns` set so the resolver auto-applies grounding.
    ///
    /// # Arguments
    /// * `data_ns` - Data namespace (e.g., "data::production")
    /// * `lib_ns` - Library namespace containing definitions (e.g., "lib::analytics")
    /// * `new_ns_name` - Name for the new grounded namespace (e.g., "lib::analytics_prod")
    ///
    /// # Returns
    /// Number of entities grounded
    pub fn ground_namespace(
        &mut self,
        data_ns: &str,
        lib_ns: &str,
        new_ns_name: &str,
    ) -> Result<usize> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for ground",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate data_ns exists
        let data_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [data_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Data namespace '{}' not found. Mount it first with mount!().",
                        data_ns
                    ),
                    "Namespace not found",
                )
            })?;

        // 2. Validate lib_ns exists
        let lib_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [lib_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Library namespace '{}' not found. Consult it first with consult!().",
                        lib_ns
                    ),
                    "Namespace not found",
                )
            })?;

        // 3. Validate new_ns_name does NOT exist
        ensure_namespace_available(&bootstrap_conn, new_ns_name)?;

        // 4. Retrieve all entities from lib_ns
        let entities: Vec<(i32, String, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.type, e.doc
                     FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE n.fq_name = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib namespace entities",
                        e.to_string(),
                    )
                })?;

            let rows = match stmt.query_map([lib_ns], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            }) {
                Ok(r) => r,
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        "Failed to query lib namespace entities",
                        e.to_string(),
                    ));
                }
            };
            rows.flatten().collect()
        };

        // 4b. Discover manifest-only entities from _internal (if lib_ns has none of its own)
        use crate::ddl::manifest;
        let internal_ns_id = manifest::find_internal_ns(&bootstrap_conn, lib_ns)?;

        let manifest_entity_names: Vec<String> = if entities.is_empty() {
            if let Some(int_ns_id) = internal_ns_id {
                manifest::discover_schema_entities(&bootstrap_conn, int_ns_id)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if entities.is_empty() && manifest_entity_names.is_empty() {
            return Err(DelightQLError::database_error(
                format!("Library namespace '{}' has no entities to ground", lib_ns),
                "Empty namespace",
            ));
        }

        // 5. STRICT VALIDATION: For each entity with references, check that all
        //    referenced entities (unqualified free variables) exist in the data namespace
        for (entity_id, entity_name, _entity_type, _doc) in &entities {
            let refs: Vec<String> = {
                let mut ref_stmt = bootstrap_conn
                    .prepare(
                        "SELECT re.name FROM referenced_entity re
                         WHERE re.containing_entity_id = ?1
                           AND re.namespace IS NULL",
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to query entity references",
                            e.to_string(),
                        )
                    })?;

                let rows = match ref_stmt.query_map([entity_id], |row| row.get::<_, String>(0)) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(DelightQLError::database_error(
                            "Failed to query entity references",
                            e.to_string(),
                        ));
                    }
                };
                rows.flatten().collect()
            };

            for ref_name in &refs {
                // Check if ref_name exists as an activated entity in data_ns
                let exists: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM entity e
                            JOIN activated_entity ae ON ae.entity_id = e.id
                            WHERE ae.namespace_id = ?1 AND e.name = ?2 COLLATE NOCASE
                        )",
                        rusqlite::params![data_ns_id, ref_name],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if !exists {
                    return Err(DelightQLError::database_error(
                        format!(
                            "ground!() validation failed: entity '{}' references '{}' \
                             which does not exist in data namespace '{}'",
                            entity_name, ref_name, data_ns
                        ),
                        "Unresolved reference",
                    ));
                }
            }
        }

        // 6. Create new namespace with default_data_ns
        let new_ns_id = {
            let name = new_ns_name.split("::").last().unwrap_or(new_ns_name);
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance)
                     VALUES (?1, NULL, ?2, ?3, 'grounded', 'ground')",
                    rusqlite::params![name, new_ns_name, data_ns],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create grounded namespace",
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // 6b. Record grounding dependency for ownership enforcement
        bootstrap_conn
            .execute(
                "INSERT INTO grounding (grounded_namespace_id, data_namespace_id, lib_namespace_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![new_ns_id, data_ns_id, lib_ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to record grounding dependency",
                    e.to_string(),
                )
            })?;

        // 7. Create cartridge for ground
        let cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        1, // DqlStandard language ID
                        SourceType::File.as_i32(),
                        &format!("ground://{}<-{}", lib_ns, data_ns),
                        None::<String>, // No SQL schema qualifier for bootstrap-local temp tables
                        1, // bootstrap connection
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create ground cartridge",
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // 8. Copy entities from lib_ns into new namespace
        let mut count = entities.len();
        for (old_entity_id, entity_name, entity_type, entity_doc) in &entities {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![entity_name, entity_type, cartridge_id, entity_doc],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to copy entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
            let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

            Self::copy_entity_subtables(&bootstrap_conn, *old_entity_id, new_entity_id)?;

            // If entity has manifest data in _internal, create TEMP table from it
            if let Some(int_ns_id) = internal_ns_id {
                if let Some(result) = crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
                )? {
                    bootstrap_conn
                        .execute_batch(&result.create_sql)
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!(
                                    "Failed to CREATE TEMP TABLE for '{}': {}",
                                    entity_name, result.create_sql
                                ),
                                e.to_string(),
                            )
                        })?;
                }
            }

            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![new_entity_id, new_ns_id, cartridge_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to activate entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
        }

        // 8b. Create manifest-only entities (discovered from _internal, no entity in lib_ns)
        if let Some(int_ns_id) = internal_ns_id {
            for entity_name in &manifest_entity_names {
                let result = match crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
                )? {
                    Some(r) => r,
                    None => continue,
                };
                let crate::ddl_pipeline::ManifestCreateResult {
                    create_sql,
                    schema_rows,
                } = result;
                bootstrap_conn.execute_batch(&create_sql).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to CREATE TEMP TABLE for '{}': {}",
                            entity_name, create_sql
                        ),
                        e.to_string(),
                    )
                })?;

                // Register entity in bootstrap
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            entity_name,
                            1, // Table entity type
                            cartridge_id,
                            format!("Grounded from {} manifest", lib_ns),
                        ],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to create grounded entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;
                let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Register entity attributes from manifest schema rows
                for (position, sr) in schema_rows.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                             VALUES (?1, ?2, 'output_column', ?3, ?4, 1)",
                            rusqlite::params![new_entity_id, &sr.name, &sr.col_type, position as i32 + 1],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!("Failed to register attribute '{}' for '{}'", sr.name, entity_name),
                                e.to_string(),
                            )
                        })?;
                }

                // Activate entity in grounded namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![new_entity_id, new_ns_id, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to activate grounded entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;

                count += 1;
            }
        }

        drop(bootstrap_conn);

        debug!(
            "ground_namespace: Grounded {} entities from '{}' into '{}' (data: '{}')",
            count, lib_ns, new_ns_name, data_ns
        );

        Ok(count)
    }

    /// Imprint definitions from a library namespace into a data namespace.
    ///
    /// Reads manifest data from the `_internal` child namespace (schema, constraints,
    /// defaults, imprinting HO entities), assembles CREATE TABLE DDL, and executes
    /// on the target database. For CTAS entities, populates via INSERT INTO ... SELECT.
    ///
    /// Returns a list of (entity_name, status, sql) tuples for reporting.
    pub fn imprint_namespace(
        &mut self,
        source_ns: &str,
        target_ns: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for imprint",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate source namespace exists and is a lib namespace
        let (source_ns_id, source_kind): (i32, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [source_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Source namespace '{}' not found. Consult it first with consult!().",
                        source_ns
                    ),
                    "Namespace not found",
                )
            })?;

        if source_kind == "data" || source_kind == "system" {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() source '{}' is a {} namespace. Source must be a lib namespace.",
                    source_ns, source_kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Check borrow: source must not be borrowed by any active grounding
        let borrowed: bool = bootstrap_conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grounding WHERE lib_namespace_id = ?1)",
                [source_ns_id],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if borrowed {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() cannot consume '{}' — it is borrowed by an active grounding. \
                     Unconsult the grounded namespace first.",
                    source_ns
                ),
                "Source namespace borrowed",
            ));
        }

        // 3. Validate target namespace exists and is a data namespace
        let (target_ns_id, target_kind): (i32, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [target_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Target namespace '{}' not found. Mount it first with mount!().",
                        target_ns
                    ),
                    "Namespace not found",
                )
            })?;

        if target_kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() target '{}' is a {} namespace. Target must be a data namespace.",
                    target_ns, target_kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 4. Get target connection info: schema alias + connection_id
        let (target_schema_alias, connection_id): (Option<String>, i64) = bootstrap_conn
            .query_row(
                "SELECT c.source_ns, c.connection_id
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON ae.namespace_id = n.id
                 WHERE n.fq_name = ?1
                 LIMIT 1",
                [target_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or_else(|_| (None, 2)); // default: user connection, no schema

        let target_conn = self
            .connection_map
            .get(&connection_id)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.connection));

        // 5. Find _internal child namespace for manifest data
        use crate::ddl::manifest;

        let internal_ns_id =
            manifest::find_internal_ns(&bootstrap_conn, source_ns)?.ok_or_else(|| {
                DelightQLError::database_error(
                    format!(
                        "imprint!() source '{}' has no _internal namespace \
                         (no schema/constraints/defaults definitions)",
                        source_ns
                    ),
                    "No _internal namespace",
                )
            })?;

        // Discover entities: prefer imprinting() manifest, fall back to schema() ground values
        let imprinting_rows = manifest::read_imprinting(&bootstrap_conn, internal_ns_id)?;

        struct EntityTodo {
            name: String,
            materialization: String,
            extent: String,
        }

        let entity_todos: Vec<EntityTodo> = if !imprinting_rows.is_empty() {
            imprinting_rows
                .into_iter()
                .map(|row| EntityTodo {
                    name: row.entity,
                    materialization: row.materialization,
                    extent: row.extent,
                })
                .collect()
        } else {
            // No imprinting() — discover from schema() ground values
            let schema_entities =
                manifest::discover_schema_entities(&bootstrap_conn, internal_ns_id)?;
            schema_entities
                .into_iter()
                .map(|name| EntityTodo {
                    name,
                    materialization: "table".to_string(),
                    extent: "permanent".to_string(),
                })
                .collect()
        };

        if entity_todos.is_empty() {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() source '{}' has no manifest entities \
                     (no schema() or imprinting() definitions in _internal)",
                    source_ns
                ),
                "No manifest entities",
            ));
        }

        // --- Phase 0: Read ALL manifest data from bootstrap, then drop the lock ---
        // self.schema is BootstrapBackedSchema which locks self.bootstrap_connection
        // internally. compile_source_to_sql -> resolver -> schema.get_table_columns()
        // -> BootstrapBackedSchema::get_table_columns() -> self.bootstrap_conn.lock().
        // If we still hold bootstrap_conn here, that's a deadlock. So we read
        // everything we need, drop the lock, then compile in Phase 1.

        struct ManifestData {
            name: String,
            materialization: String,
            extent: String,
            schema_rows: Vec<manifest::SchemaRow>,
            constraint_rows: Vec<manifest::ConstraintRow>,
            default_rows: Vec<manifest::DefaultRow>,
            ctas_body: Option<String>,
        }

        let mut manifest_items: Vec<ManifestData> = Vec::new();

        for todo in &entity_todos {
            let entity_name = &todo.name;

            let schema_rows = manifest::read_schema(&bootstrap_conn, internal_ns_id, entity_name)?;
            let constraint_rows =
                manifest::read_constraints(&bootstrap_conn, internal_ns_id, entity_name)?;
            let default_rows =
                manifest::read_defaults(&bootstrap_conn, internal_ns_id, entity_name)?;

            // Check for CTAS body: entity with :- or := view body in source namespace
            let ctas_body: Option<String> = {
                let stmt = bootstrap_conn
                    .prepare(
                        "SELECT ec.definition FROM entity_clause ec
                         JOIN entity e ON ec.entity_id = e.id
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE ae.namespace_id = ?1 AND e.name = ?2
                         ORDER BY ec.ordinal LIMIT 1",
                    )
                    .ok();
                stmt.and_then(|mut s| {
                    s.query_row(rusqlite::params![source_ns_id, entity_name], |row| {
                        row.get::<_, String>(0)
                    })
                    .ok()
                })
                .and_then(|def| {
                    if let Some(pos) = def.find(":-") {
                        let body = def[pos + 2..].trim();
                        if !body.is_empty() {
                            Some(body.to_string())
                        } else {
                            None
                        }
                    } else if let Some(pos) = def.find(":=") {
                        let body = def[pos + 2..].trim();
                        if !body.is_empty() {
                            Some(body.to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            };

            manifest_items.push(ManifestData {
                name: entity_name.clone(),
                materialization: todo.materialization.clone(),
                extent: todo.extent.clone(),
                schema_rows,
                constraint_rows,
                default_rows,
                ctas_body,
            });
        }

        // Drop bootstrap lock -- Phase 1 needs schema access which locks bootstrap internally
        drop(bootstrap_conn);

        // --- Phase 1: Compile (no bootstrap lock held) ---
        // compile_source_to_sql -> resolver -> schema.get_table_columns()
        // -> BootstrapBackedSchema -> self.bootstrap_connection.lock(). Safe now.
        let empty_schema = crate::ddl::manifest::EmptySchema;
        let schema: &dyn DatabaseSchema = if connection_id == 2 {
            self.schema
                .as_ref()
                .map(|s| s.as_ref())
                .unwrap_or(&empty_schema)
        } else {
            self.schema_map
                .get(&connection_id)
                .map(|s| s.as_ref())
                .unwrap_or(&empty_schema)
        };

        struct PreparedEntity {
            name: String,
            materialization: String,
            temp: bool,
            qualified_create: String,
            ctas_insert_sql: Option<String>,
            effective_schema: Vec<manifest::SchemaRow>,
        }

        let mut prepared: Vec<PreparedEntity> = Vec::new();

        for item in &manifest_items {
            let entity_name = &item.name;

            if item.materialization == "view" {
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint!() entity '{}' has materialization 'view' which is not yet supported",
                        entity_name
                    ),
                    "View materialization in imprint is deferred",
                ));
            }

            let temp = item.extent == "temporary";

            // Compile CTAS body (schema access locks bootstrap internally -- safe now)
            let ctas_select_sql = if let Some(body) = &item.ctas_body {
                Some(crate::pipeline::compile_source_to_sql(body, schema)?)
            } else {
                None
            };

            // For CTAS without explicit schema, infer from LIMIT 0 query on target
            let effective_schema = if item.schema_rows.is_empty() && ctas_select_sql.is_some() {
                let select_sql = ctas_select_sql.as_ref().unwrap();
                let limit_sql = format!("SELECT * FROM ({}) LIMIT 0", select_sql);
                let target_conn_tmp = target_conn.lock().map_err(|e| {
                    DelightQLError::connection_poison_error(
                        "Failed to acquire target connection for schema inference",
                        format!("Connection was poisoned: {}", e),
                    )
                })?;
                let (col_names, _rows) = target_conn_tmp
                    .query_all_string_rows(&limit_sql, &[])
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Failed to infer schema for CTAS entity '{}': {}",
                                entity_name, limit_sql
                            ),
                            e.to_string(),
                        )
                    })?;
                drop(target_conn_tmp);
                col_names
                    .into_iter()
                    .map(|name| manifest::SchemaRow {
                        name,
                        col_type: "TEXT".to_string(),
                    })
                    .collect()
            } else {
                item.schema_rows.clone()
            };

            // Assemble CREATE TABLE from manifest via DDL pipeline
            let unresolved = crate::ddl_pipeline::assemble_manifest::assemble_from_manifest(
                entity_name,
                temp,
                &effective_schema,
                &item.constraint_rows,
                &item.default_rows,
            )?;
            let resolved = crate::ddl_pipeline::resolver::resolve(unresolved)?;
            let sql_ast = crate::ddl_pipeline::transformer::transform(resolved)?;
            let create_sql = crate::ddl_pipeline::generator::generate(&sql_ast);

            // Schema-qualify for ATTACHed databases
            let qualified_create = if let Some(schema_name) = target_schema_alias.as_deref() {
                create_sql.replacen(
                    &format!("CREATE TABLE \"{}\"", entity_name),
                    &format!("CREATE TABLE \"{}\".\"{}\"", schema_name, entity_name),
                    1,
                )
            } else {
                create_sql
            };

            // Build CTAS insert statement
            let ctas_insert_sql = ctas_select_sql.map(|select_sql| {
                let qualified_table = if let Some(schema_name) = target_schema_alias.as_deref() {
                    format!("\"{}\".\"{}\"", schema_name, entity_name)
                } else {
                    format!("\"{}\"", entity_name)
                };
                format!("INSERT INTO {} {}", qualified_table, select_sql)
            });

            prepared.push(PreparedEntity {
                name: entity_name.clone(),
                materialization: item.materialization.clone(),
                temp,
                qualified_create,
                ctas_insert_sql,
                effective_schema,
            });
        }

        // --- Phase 2: Execute (re-acquire bootstrap + target locks) ---
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to re-acquire bootstrap lock for imprint execution",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let target_conn_guard = target_conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire target connection lock for imprint",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Enable FK enforcement on target
        let _ = target_conn_guard.execute("PRAGMA foreign_keys = ON", &[]);

        // Create a cartridge for the imprinted entities
        let imprint_cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        3, // SQLite language ID
                        SourceType::Db.as_i32(),
                        &format!("imprint://{}->{}", source_ns, target_ns),
                        target_schema_alias,
                        connection_id,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create imprint cartridge",
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        let mut results = Vec::new();

        for entity in &prepared {
            let entity_name = &entity.name;

            target_conn_guard
                .execute(&entity.qualified_create, &[])
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to execute CREATE TABLE for '{}': {}",
                            entity_name, entity.qualified_create,
                        ),
                        e.to_string(),
                    )
                })?;

            // Execute CTAS INSERT if present
            if let Some(insert) = &entity.ctas_insert_sql {
                target_conn_guard.execute(insert, &[]).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to execute CTAS INSERT for '{}': {}",
                            entity_name, insert,
                        ),
                        e.to_string(),
                    )
                })?;
            }

            // Register the new entity in the target namespace
            // Entity type: 1 = table, 2 = view
            let entity_type = if entity.materialization == "view" {
                2
            } else {
                1
            };
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        entity_name,
                        entity_type,
                        imprint_cartridge_id,
                        format!("Imprinted from {}", source_ns),
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to register imprinted entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
            let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

            // Register entity attributes from manifest schema rows
            for (position, sr) in entity.effective_schema.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
                         VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                        rusqlite::params![new_entity_id, &sr.name, &sr.col_type, position as i32 + 1],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to register attribute '{}' for '{}'", sr.name, entity_name),
                            e.to_string(),
                        )
                    })?;
            }

            // Activate entity in target namespace
            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![new_entity_id, target_ns_id, imprint_cartridge_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to activate imprinted entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;

            let status = if entity.ctas_insert_sql.is_some() {
                "created+populated"
            } else {
                "created"
            };

            results.push((
                entity_name.clone(),
                status.to_string(),
                entity.qualified_create.clone(),
            ));
        }

        drop(target_conn_guard);
        drop(bootstrap_conn);

        debug!(
            "imprint_namespace: Materialized {} entities from '{}' into '{}'",
            results.len(),
            source_ns,
            target_ns
        );

        Ok(results)
    }

    /// Resolve a namespace path to its backend schema name and connection ID
    ///
    /// This is an engine implementation detail that queries the internal _bootstrap
    /// metadata to map namespace paths to backend schema names and connection routing info.
    /// This method encapsulates all bootstrap access, keeping it internal to the engine.
    ///
    /// # Arguments
    /// * `path` - The namespace path to resolve
    ///
    /// # Returns
    /// * `Ok(Some((schema_name, connection_id)))` - Namespace resolved to backend schema and connection
    /// * `Ok(None)` - Namespace not found or has no activated entities
    /// * `Err(...)` - Database error during resolution
    pub fn resolve_namespace_path(
        &self,
        path: &delightql_types::namespace::NamespacePath,
    ) -> Result<Option<(Option<String>, i64)>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for namespace resolution",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // META-CIRCULAR IMPLEMENTATION: Use bootstrap.namespace for namespace resolution
        // Build the fully-qualified namespace path (e.g., "main" or "sys::cartridges")
        // DEFAULT: Empty namespace path → "main" namespace
        let fq_name = if path.is_empty() {
            "main".to_string()
        } else {
            let path_parts: Vec<String> = path
                .iter()
                .map(|segment| segment.name.to_string())
                .collect();
            path_parts.join("::")
        };

        // Step 1: Look up namespace in bootstrap.namespace by fq_name
        // NOTE: _bootstrap is a separate connection, NOT attached, so no schema prefix needed
        debug!("resolve_namespace_path: Looking up fq_name={}", fq_name);
        let namespace_id = match conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [&fq_name],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(id) => {
                debug!("resolve_namespace_path: Found namespace_id={}", id);
                id
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Namespace not found
                debug!("resolve_namespace_path: Namespace '{}' not found", fq_name);
                return Ok(None);
            }
            Err(e) => {
                if e.to_string().contains("no such table") {
                    // Bootstrap table doesn't exist - system not initialized
                    return Ok(None);
                }
                return Err(DelightQLError::database_error_with_source(
                    "Failed to query bootstrap.namespace",
                    e.to_string(),
                    Box::new(e),
                ));
            }
        };

        // Step 2: Get the backend schema (source_ns) and connection_id for this namespace
        // First try to find cartridges with activated entities
        let result = conn.query_row(
            "SELECT DISTINCT c.source_ns, c.connection_id
             FROM activated_entity ae
             JOIN cartridge c ON ae.cartridge_id = c.id
             WHERE ae.namespace_id = ?1
             LIMIT 1",
            [namespace_id],
            |row| {
                let source_ns = row.get::<_, Option<String>>(0)?;
                let connection_id = row.get::<_, i64>(1)?;
                Ok((source_ns, connection_id))
            },
        );

        match result {
            Ok((source_ns, connection_id)) => Ok(Some((source_ns, connection_id))),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Namespace exists but has no activated entities
                Ok(None)
            }
            Err(e) => Err(DelightQLError::database_error_with_source(
                "Failed to resolve backend schema and connection from bootstrap",
                e.to_string(),
                Box::new(e),
            )),
        }
    }

    /// Resolve an unqualified entity name to its namespace path
    ///
    /// Queries the bootstrap metadata to find where an entity is activated
    /// and whether it's accessible from the current namespace.
    ///
    /// # Algorithm
    /// 1. Look up namespace_id for current_namespace (e.g., "main")
    /// 2. Search activated_entity for entity_name in:
    ///    - Current namespace
    ///    - Engaged namespaces (via enlisted_namespace table)
    /// 3. If found, return the namespace path
    /// 4. If not found, return None
    ///
    /// # Arguments
    /// * `entity_name` - Unqualified entity name (e.g., "team")
    /// * `current_namespace` - Current namespace (typically "main")
    ///
    /// # Returns
    /// * `Ok(Some(namespace_path))` - Entity found in accessible namespace
    /// * `Ok(None)` - Entity not found or not accessible
    /// * `Err(...)` - Database error during resolution
    /// Resolve an unqualified entity name within a namespace scope.
    ///
    /// Searches `current_namespace` and its enlisted namespaces. When
    /// `fallback_namespace` is provided and the primary search yields no
    /// results, the fallback scope is searched too. This supports DDL view
    /// body resolution: the DDL namespace is primary (with its own enlists),
    /// and "main" is the fallback for database tables not in any enlist.
    pub fn resolve_unqualified_entity(
        &self,
        entity_name: &str,
        current_namespace: &str,
        fallback_namespace: Option<&str>,
    ) -> Result<
        Option<(
            delightql_types::namespace::NamespacePath,
            delightql_types::SqlIdentifier,
        )>,
    > {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Step 1: Get namespace_id for current namespace
        let current_ns_id: i64 = match conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [current_namespace],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Current namespace doesn't exist
                return Ok(None);
            }
            Err(e) => {
                return Err(DelightQLError::database_error_with_source(
                    "Failed to query current namespace",
                    e.to_string(),
                    Box::new(e),
                ));
            }
        };

        // Step 2 & 3: Find entity in current namespace OR enlisted namespaces.
        // Collect ALL matches across namespaces to detect ambiguity.
        let query = "
            WITH RECURSIVE
            direct(ns_id) AS (
                SELECT ?2 AS ns_id
                UNION
                SELECT en.from_namespace_id
                FROM enlisted_namespace en
                WHERE en.to_namespace_id = ?2
            ),
            reachable(ns_id) AS (
                SELECT ns_id FROM direct
                UNION
                SELECT exp.exposed_namespace_id
                FROM exposed_namespace exp
                JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
            )
            SELECT DISTINCT n.fq_name, e.name
            FROM activated_entity ae
            JOIN entity e ON ae.entity_id = e.id
            JOIN namespace n ON ae.namespace_id = n.id
            JOIN reachable r ON r.ns_id = ae.namespace_id
            WHERE e.name = ?1 COLLATE NOCASE
        ";

        let mut stmt = conn.prepare(query).map_err(|e| {
            DelightQLError::database_error(
                "Failed to prepare unqualified entity resolution",
                e.to_string(),
            )
        })?;

        let matches: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![entity_name, current_ns_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to resolve unqualified entity",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        // When a fallback namespace is provided (DDL view body resolution),
        // also search the fallback scope and merge results. Ambiguity across
        // both scopes is still an error (e.g., DDL-enlisted `items` overlapping
        // with main's `items`).
        let mut all_matches = matches;
        if let Some(fallback_ns) = fallback_namespace {
            let fallback_ns_id: i64 = match conn.query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [fallback_ns],
                |row| row.get(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    return Self::resolve_matches(all_matches, entity_name, current_namespace)
                }
            };

            let mut fallback_stmt = conn
                .prepare(
                    "WITH RECURSIVE
                     direct(ns_id) AS (
                         SELECT ?2 AS ns_id
                         UNION
                         SELECT en.from_namespace_id
                         FROM enlisted_namespace en
                         WHERE en.to_namespace_id = ?2
                     ),
                     reachable(ns_id) AS (
                         SELECT ns_id FROM direct
                         UNION
                         SELECT exp.exposed_namespace_id
                         FROM exposed_namespace exp
                         JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                     )
                     SELECT DISTINCT n.fq_name, e.name
                     FROM activated_entity ae
                     JOIN entity e ON ae.entity_id = e.id
                     JOIN namespace n ON ae.namespace_id = n.id
                     JOIN reachable r ON r.ns_id = ae.namespace_id
                     WHERE e.name = ?1 COLLATE NOCASE",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to prepare fallback entity resolution",
                        e.to_string(),
                    )
                })?;

            let fallback_matches: Vec<(String, String)> = fallback_stmt
                .query_map(rusqlite::params![entity_name, fallback_ns_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to resolve entity in fallback namespace",
                        e.to_string(),
                    )
                })?
                .filter_map(|r| r.ok())
                .collect();

            // Merge, dedup by fq_name
            for m in fallback_matches {
                if !all_matches.iter().any(|(ns, _)| *ns == m.0) {
                    all_matches.push(m);
                }
            }
        }

        Self::resolve_matches(all_matches, entity_name, current_namespace)
    }

    /// Helper: interpret a set of entity matches — 0 = not found, 1 = found, 2+ = ambiguous.
    fn resolve_matches(
        matches: Vec<(String, String)>,
        entity_name: &str,
        scope_namespace: &str,
    ) -> Result<
        Option<(
            delightql_types::namespace::NamespacePath,
            delightql_types::SqlIdentifier,
        )>,
    > {
        match matches.len() {
            0 => Ok(None),
            1 => {
                let (fq_name, canonical_name) = &matches[0];
                let parts: Vec<String> = fq_name.split("::").map(|s| s.to_string()).collect();
                let namespace_path = delightql_types::namespace::NamespacePath::from_parts(parts);
                Ok(Some((
                    namespace_path,
                    delightql_types::SqlIdentifier::new(canonical_name),
                )))
            }
            _ => {
                // Multiple matches from different namespaces — ambiguous.
                let namespaces: Vec<&str> = matches.iter().map(|(ns, _)| ns.as_str()).collect();
                let enlisted_ns = namespaces
                    .iter()
                    .find(|ns| **ns != scope_namespace)
                    .unwrap_or(namespaces.last().unwrap_or(&"ns"));
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous entity '{}': found in namespaces {}. \
                         enlist!() brought overlapping names into scope. \
                         Fix: use qualified access ({}.{}(*)), \
                         or delist!(\"{}\") to remove the namespace.",
                        entity_name,
                        namespaces.join(", "),
                        enlisted_ns,
                        entity_name,
                        enlisted_ns,
                    ),
                    "Ambiguous unqualified entity resolution",
                ))
            }
        }
    }

    /// Refresh a data namespace by re-introspecting its source database.
    ///
    /// Clears all entity metadata and re-discovers entities from the same
    /// database source. Preserves namespace identity, enlistments, aliases,
    /// and groundings. Validates grounding contracts after refresh.
    pub fn refresh_namespace(&mut self, namespace: &str) -> Result<usize> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for refresh",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and is 'data' kind
        let (ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        if kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot refresh '{}' — it is a {} namespace. refresh!() only works on data namespaces. \
                     Use reconsult!() for lib namespaces.",
                    namespace, kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Retrieve cartridge metadata for re-introspection
        let cartridge_meta: Option<(i64, Option<i64>, Option<String>, Option<String>)> =
            bootstrap_conn
                .query_row(
                    "SELECT c.id, c.connection_id, c.source_ns, c.source_uri
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1
                 LIMIT 1",
                    [ns_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .ok();

        let (connection_id, schema_alias, source_uri) = match &cartridge_meta {
            Some((_, conn_id, src_ns, src_uri)) => (
                conn_id.unwrap_or(2),
                src_ns.clone(),
                src_uri.clone().unwrap_or_default(),
            ),
            None => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Namespace '{}' has no cartridge — cannot refresh",
                        namespace
                    ),
                    "No cartridge found",
                ));
            }
        };

        // 3. Begin transaction
        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error("Failed to begin refresh transaction", e.to_string())
        })?;

        // 4. Clear contents
        let clear_result = Self::clear_namespace_contents(&bootstrap_conn, ns_id);
        if let Err(e) = clear_result {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        // 5. Re-introspect
        let entities = if connection_id == 2 {
            // ATTACH path: use schema alias
            let alias = schema_alias.as_deref().unwrap_or(namespace);
            match self.introspector.introspect_entities_in_schema(alias) {
                Ok(e) => e,
                Err(e) => {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(DelightQLError::database_error(
                        format!("Failed to re-introspect schema '{}': {}", alias, e),
                        e.to_string(),
                    ));
                }
            }
        } else {
            // Factory path: use connection_factory
            match &self.connection_factory {
                Some(factory) => {
                    let components = match factory.create(&source_uri) {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = bootstrap_conn.execute_batch("ROLLBACK");
                            return Err(DelightQLError::database_error(
                                format!("Failed to create connection for refresh: {}", e),
                                e.to_string(),
                            ));
                        }
                    };
                    match components.introspector.introspect_entities() {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = bootstrap_conn.execute_batch("ROLLBACK");
                            return Err(DelightQLError::database_error(
                                format!("Failed to re-introspect '{}': {}", source_uri, e),
                                e.to_string(),
                            ));
                        }
                    }
                }
                None => {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(DelightQLError::database_error(
                        "Cannot refresh factory-mounted namespace without connection factory",
                        "No connection factory",
                    ));
                }
            }
        };

        // 6. Re-register: new cartridge + entities
        let cartridge_id = {
            let language = if connection_id == 2 {
                3
            } else {
                // Determine from source_uri
                if source_uri.starts_with("duckdb://") {
                    4
                } else if source_uri.starts_with("postgres://")
                    || source_uri.starts_with("postgresql://")
                {
                    3
                } else {
                    3
                }
            };
            bootstrap_conn.execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                rusqlite::params![
                    language,
                    crate::bootstrap::SourceType::Db.as_i32(),
                    &source_uri,
                    schema_alias.as_deref(),
                    connection_id,
                ],
            ).map_err(|e| {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                DelightQLError::database_error("Failed to create refresh cartridge", e.to_string())
            })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        let entity_count = entities.len();
        if let Err(e) = crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        ) {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(DelightQLError::database_error(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            ));
        }

        if let Err(e) = crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            ns_id as i32,
        ) {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            ));
        }

        // 7. Validate groundings: check all grounded namespaces borrowing this data ns
        {
            let mut gnd_stmt = bootstrap_conn
                .prepare(
                    "SELECT g.grounded_namespace_id, g.lib_namespace_id, gn.fq_name, ln.fq_name
                 FROM grounding g
                 JOIN namespace gn ON gn.id = g.grounded_namespace_id
                 JOIN namespace ln ON ln.id = g.lib_namespace_id
                 WHERE g.data_namespace_id = ?1",
                )
                .map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings", e.to_string())
                })?;
            let groundings: Vec<(i64, i64, String, String)> = gnd_stmt
                .query_map([ns_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings", e.to_string())
                })?
                .flatten()
                .collect();

            for (_grounded_id, lib_id, _grounded_fq, lib_fq) in &groundings {
                if let Err(e) = Self::validate_grounding_contract(
                    &bootstrap_conn,
                    *lib_id,
                    lib_fq,
                    ns_id,
                    namespace,
                ) {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(e);
                }
            }
        }

        // 8. Commit
        bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
            DelightQLError::database_error("Failed to commit refresh transaction", e.to_string())
        })?;

        drop(bootstrap_conn);

        debug!(
            "refresh_namespace: Refreshed namespace '{}' with {} entities",
            namespace, entity_count
        );

        Ok(entity_count)
    }

    /// Reconsult a lib/scratch namespace by re-reading and re-parsing its source file.
    ///
    /// Clears all entity definitions and re-loads from the same (or new) source file.
    /// Preserves namespace identity, enlistments, aliases. If grounded namespaces
    /// borrow from this lib, validates the grounding contract and auto-rebuilds.
    pub fn reconsult_namespace(
        &mut self,
        namespace: &str,
        new_file_path: Option<&str>,
    ) -> Result<usize> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for reconsult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and check kind
        let (ns_id, kind, source_path): (i64, String, Option<String>) = bootstrap_conn
            .query_row(
                "SELECT id, kind, source_path FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        match kind.as_str() {
            "data" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — it is a data namespace. Use refresh!() instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "system" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — system namespaces cannot be modified.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "grounded" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — it is a grounded namespace. Reconsult the source lib namespace instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "lib" | "scratch" | "unknown" => { /* acceptable */ }
            other => panic!(
                "catch-all hit in system.rs reconsult_namespace: unexpected namespace kind: {}",
                other
            ),
        }

        // 2. Determine source file
        let file_path = match new_file_path {
            Some(p) => p.to_string(),
            None => {
                if let Some(ref sp) = source_path {
                    sp.clone()
                } else {
                    // Try to find from cartridge source_uri
                    let uri: Option<String> = bootstrap_conn
                        .query_row(
                            "SELECT c.source_uri
                             FROM cartridge c
                             JOIN entity e ON e.cartridge_id = c.id
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE ae.namespace_id = ?1
                             LIMIT 1",
                            [ns_id],
                            |row| row.get(0),
                        )
                        .ok();
                    match uri {
                        Some(u) if u.starts_with("file://") => u[7..].to_string(),
                        _ => {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "Cannot determine source file for namespace '{}'. \
                                     Provide a file path: reconsult!(\"ns\", \"path/to/file.dql\")",
                                    namespace
                                ),
                                "No source file",
                            ));
                        }
                    }
                }
            }
        };

        // 3. Read + parse new file
        drop(bootstrap_conn);

        // Resolve relative path against session CWD (for test isolation).
        let resolved_path = crate::session_cwd::resolve_path(&file_path);
        let file_path = resolved_path.display().to_string();

        let source = std::fs::read_to_string(&file_path).map_err(|e| {
            DelightQLError::database_error(
                format!("reconsult!() failed to read file '{}': {}", file_path, e),
                "File read error",
            )
        })?;

        let (cleaned_source, directives) =
            crate::bin_cartridge::prelude::consult::extract_embedded_directives(&source)?;

        // Save enlist/alias state before processing embedded directives
        let saved_enlisted = self.save_enlisted_state()?;
        let saved_aliases = self.save_alias_state()?;

        // Execute embedded directives (resolve .:: and :: prefixes relative to namespace)
        for directive in &directives {
            match directive.name.as_str() {
                "consult" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[1], namespace)?;
                        crate::bin_cartridge::prelude::consult::execute_consult(
                            self,
                            &directive.args[0],
                            &resolved_ns,
                            Some(namespace),
                        )?;
                    }
                }
                "mount" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[1], namespace)?;
                        self.mount_database(&directive.args[0], &resolved_ns)?;
                    }
                }
                "enlist" => {
                    if directive.args.len() == 1 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.enlist_namespace(&resolved_ns)?;
                    }
                }
                "delist" => {
                    if directive.args.len() == 1 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.delist_namespace(&resolved_ns)?;
                    }
                }
                "alias" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.register_namespace_alias(&directive.args[1], &resolved_ns)?;
                    }
                }
                other => panic!("catch-all hit in system.rs reconsult_namespace directive processing: unexpected directive name: {}", other),
            }
        }

        // Parse DDL
        let ddl = crate::pipeline::parser::parse_ddl_file(&cleaned_source).map_err(|e| {
            DelightQLError::database_error(
                format!("reconsult!() failed to parse '{}': {}", file_path, e),
                "Parse error",
            )
        })?;

        if ddl.definitions.is_empty() {
            // Restore state and return error
            let _ = self.restore_enlisted_state(&saved_enlisted);
            let _ = self.restore_alias_state(&saved_aliases);
            return Err(DelightQLError::database_error(
                format!(
                    "reconsult!() failed: '{}' contains no DDL definitions.",
                    file_path
                ),
                "Not a DDL file",
            ));
        }

        // 4. Transaction: clear old contents, insert new, validate groundings
        let entity_count = {
            let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap database lock for reconsult",
                    format!("Connection was poisoned: {}", e),
                )
            })?;

            bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to begin reconsult transaction",
                    e.to_string(),
                )
            })?;

            // 5. Clear old contents
            if let Err(e) = Self::clear_namespace_contents(&bootstrap_conn, ns_id) {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(e);
            }

            // 6. Insert new entities (via consult_file_inner — namespace row already exists)
            let count = ddl.definitions.len();
            let result =
                Self::consult_file_inner(&bootstrap_conn, &file_path, namespace, ddl, count, None);
            if let Err(e) = result {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(e);
            }
            let entity_count = result.unwrap().definitions_loaded;

            // 7. Validate + rebuild groundings
            {
                let mut gnd_stmt = bootstrap_conn.prepare(
                    "SELECT g.grounded_namespace_id, g.data_namespace_id, gn.fq_name, dn.fq_name
                     FROM grounding g
                     JOIN namespace gn ON gn.id = g.grounded_namespace_id
                     JOIN namespace dn ON dn.id = g.data_namespace_id
                     WHERE g.lib_namespace_id = ?1",
                ).map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings for reconsult", e.to_string())
                })?;
                let groundings: Vec<(i64, i64, String, String)> = gnd_stmt
                    .query_map([ns_id], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .map_err(|e| {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        DelightQLError::database_error(
                            "Failed to query groundings for reconsult",
                            e.to_string(),
                        )
                    })?
                    .flatten()
                    .collect();

                for (grounded_id, data_id, _grounded_fq, data_fq) in &groundings {
                    if let Err(e) = Self::validate_grounding_contract(
                        &bootstrap_conn,
                        ns_id,
                        namespace,
                        *data_id,
                        data_fq,
                    ) {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        return Err(e);
                    }

                    if let Err(e) = Self::rebuild_grounded_namespace(
                        &bootstrap_conn,
                        *grounded_id,
                        namespace,
                        data_fq,
                    ) {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        return Err(e);
                    }
                }
            }

            // 8. Update source_path if new file was provided
            if new_file_path.is_some() {
                bootstrap_conn
                    .execute(
                        "UPDATE namespace SET source_path = ?1 WHERE id = ?2",
                        rusqlite::params![&file_path, ns_id],
                    )
                    .map_err(|e| {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        DelightQLError::database_error(
                            "Failed to update source_path",
                            e.to_string(),
                        )
                    })?;
            }

            // 9. Commit
            bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to commit reconsult transaction",
                    e.to_string(),
                )
            })?;

            entity_count
        }; // bootstrap_conn dropped here

        // 10. Record namespace-local enlists/aliases, restore caller state
        let current_enlisted = self.save_enlisted_state()?;
        let current_aliases = self.save_alias_state()?;
        let new_enlists: Vec<(i32, i32)> = current_enlisted
            .iter()
            .filter(|row| !saved_enlisted.contains(row))
            .cloned()
            .collect();
        let new_aliases: Vec<(String, i32)> = current_aliases
            .iter()
            .filter(|row| !saved_aliases.contains(row))
            .cloned()
            .collect();

        if !new_enlists.is_empty() {
            self.record_namespace_local_enlists(namespace, &new_enlists)?;
        }
        if !new_aliases.is_empty() {
            self.record_namespace_local_aliases(namespace, &new_aliases)?;
        }
        self.restore_enlisted_state(&saved_enlisted)?;
        self.restore_alias_state(&saved_aliases)?;

        debug!(
            "reconsult_namespace: Reconsulted namespace '{}' from '{}' with {} entities",
            namespace, file_path, entity_count
        );

        Ok(entity_count)
    }

    /// Get the canonical (bootstrap-stored) name for an entity in a specific namespace.
    /// Used for namespace-qualified and grounded lookups where resolve_unqualified_entity
    /// is not used.
    pub fn get_canonical_entity_name(
        &self,
        namespace_fq: &str,
        entity_name: &str,
    ) -> Result<Option<delightql_types::SqlIdentifier>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let query = "
            SELECT e.name
            FROM activated_entity ae
            JOIN entity e ON ae.entity_id = e.id
            JOIN namespace n ON ae.namespace_id = n.id
            WHERE e.name = ?1 COLLATE NOCASE
              AND n.fq_name = ?2
            LIMIT 1
        ";

        match conn.query_row(query, rusqlite::params![entity_name, namespace_fq], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(canonical) => Ok(Some(delightql_types::SqlIdentifier::new(canonical))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DelightQLError::database_error_with_source(
                "Failed to get canonical entity name",
                e.to_string(),
                Box::new(e),
            )),
        }
    }
}

// =============================================================================
// Interior Schema Registration (for drill-down support)
// =============================================================================

/// Walk an unresolved query AST and register any tree group interior schemas
/// into the `interior_entity` / `interior_entity_attribute` sys tables.
fn register_interior_schemas_from_query(
    conn: &Connection,
    entity_id: i32,
    query: &crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Unresolved>,
) -> Result<()> {
    use crate::pipeline::asts::core::Query;

    match query {
        Query::Relational(rel_expr) => {
            walk_relational_for_tree_groups(conn, entity_id, rel_expr)?;
        }
        Query::WithCtes {
            ctes,
            query: main_expr,
        } => {
            walk_relational_for_tree_groups(conn, entity_id, main_expr)?;
            for cte in ctes {
                walk_relational_for_tree_groups(conn, entity_id, &cte.expression)?;
            }
        }
        // WithCfes: recurse into inner query
        Query::WithCfes { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // WithPrecompiledCfes: recurse into inner query
        Query::WithPrecompiledCfes { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // ReplTempTable/ReplTempView: recurse into inner query
        Query::ReplTempTable { query: inner, .. } | Query::ReplTempView { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // WithErContext: consumed before registration — shouldn't appear
        Query::WithErContext { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
    }

    Ok(())
}

#[stacksafe::stacksafe]
fn walk_relational_for_tree_groups(
    conn: &Connection,
    entity_id: i32,
    expr: &crate::pipeline::asts::core::RelationalExpression<
        crate::pipeline::asts::core::Unresolved,
    >,
) -> Result<()> {
    use crate::pipeline::asts::core::specs::ModuloSpec;
    use crate::pipeline::asts::core::{RelationalExpression, UnaryRelationalOperator};

    match expr {
        RelationalExpression::Pipe(pipe) => {
            // Walk source
            walk_relational_for_tree_groups(conn, entity_id, &pipe.source)?;
            // Check operator for tree groups
            match &pipe.operator {
                UnaryRelationalOperator::Modulo { spec, .. } => {
                    if let ModuloSpec::GroupBy { reducing_on, .. } = spec {
                        for domain_expr in reducing_on {
                            register_tree_group_from_domain_expr(conn, entity_id, domain_expr)?;
                        }
                    }
                }
                // All non-Modulo operators: no tree groups to register
                // (General, ProjectOut, Embed, MapCover, RenameCover, Transform, etc.)
                _ => {}
            }
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                walk_relational_for_tree_groups(conn, entity_id, operand)?;
            }
        }
        // Relation: leaf — no tree groups
        RelationalExpression::Relation(_) => {}
        // Filter: recurse into source
        RelationalExpression::Filter { source, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, source)?;
        }
        // Join: recurse both sides
        RelationalExpression::Join { left, right, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, left)?;
            walk_relational_for_tree_groups(conn, entity_id, right)?;
        }
        // ER chains: walk the contained relations for tree groups
        RelationalExpression::ErJoinChain { relations, .. } => {
            for rel in relations {
                walk_relational_for_tree_groups(
                    conn,
                    entity_id,
                    &RelationalExpression::Relation(rel.clone()),
                )?;
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, left)?;
            walk_relational_for_tree_groups(conn, entity_id, right)?;
        }
    }

    Ok(())
}

/// If a domain expression is a Curly (tree group) with an alias, register it
/// as an interior_entity with its members as interior_entity_attribute rows.
fn register_tree_group_from_domain_expr(
    conn: &Connection,
    entity_id: i32,
    expr: &crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>,
) -> Result<()> {
    use crate::pipeline::asts::core::{DomainExpression, FunctionExpression};

    if let DomainExpression::Function(FunctionExpression::Curly {
        members,
        alias: Some(alias),
        ..
    }) = expr
    {
        let alias_str = alias.as_str();
        // Insert interior_entity
        conn.execute(
            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
            rusqlite::params![entity_id, alias_str],
        )
        .map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to insert interior_entity",
                e.to_string(),
                Box::new(e),
            )
        })?;
        let interior_entity_id = conn.last_insert_rowid() as i32;

        // Insert members as interior_entity_attribute rows
        register_curly_members(conn, interior_entity_id, entity_id, members)?;
    }

    Ok(())
}

/// Register curly members as interior_entity_attribute rows.
/// Handles nesting: if a member is a nested tree group, recurse.
fn register_curly_members(
    conn: &Connection,
    interior_entity_id: i32,
    parent_entity_id: i32,
    members: &[crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Unresolved>],
) -> Result<()> {
    use crate::pipeline::asts::core::{CurlyMember, DomainExpression, FunctionExpression};

    for (position, member) in members.iter().enumerate() {
        match member {
            CurlyMember::Shorthand { column, .. } => {
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, column.as_str(), position as i32],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert interior_entity_attribute",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            }
            CurlyMember::KeyValue {
                key,
                value,
                nested_reduction,
                ..
            } => {
                if *nested_reduction {
                    // Nested tree group: create a child interior_entity
                    if let DomainExpression::Function(FunctionExpression::Curly {
                        members: child_members,
                        ..
                    }) = value.as_ref()
                    {
                        // Insert child interior_entity (no alias needed for nested)
                        conn.execute(
                            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
                            rusqlite::params![parent_entity_id, key.as_str()],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert child interior_entity",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                        let child_interior_entity_id = conn.last_insert_rowid() as i32;

                        // Register child members recursively
                        register_curly_members(
                            conn,
                            child_interior_entity_id,
                            parent_entity_id,
                            child_members,
                        )?;

                        // Insert attribute pointing to child
                        conn.execute(
                            "INSERT INTO interior_entity_attribute \
                             (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                             VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                interior_entity_id,
                                key.as_str(),
                                position as i32,
                                child_interior_entity_id
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert interior_entity_attribute (nested)",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    }
                } else {
                    conn.execute(
                        "INSERT INTO interior_entity_attribute \
                         (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                         VALUES (?1, ?2, ?3, NULL)",
                        rusqlite::params![interior_entity_id, key.as_str(), position as i32],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert interior_entity_attribute",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
                }
            }
            other => panic!("catch-all hit in system.rs register_curly_members: unexpected CurlyMember variant: {:?}", other),
        }
    }

    Ok(())
}
