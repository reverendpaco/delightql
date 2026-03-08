//! Registry implementations for tracking entities from various sources

use crate::enums::EntityType;
use crate::error::DelightQLError;
use crate::pipeline::ast_resolved::{
    ColumnMetadata, ColumnProvenance, CprSchema, FqTable, NamespacePath, TableName,
};
use crate::pipeline::resolver::DatabaseSchema;
use log::debug;
use std::collections::{HashMap, HashSet};

/// Unified registry for all entity sources
pub struct EntityRegistry<'a> {
    pub database: DatabaseRegistry<'a>,
    pub query_local: QueryLocalRegistry,
    pub built_in: BuiltInRegistry,
    pub consult: ConsultRegistry,
    /// Connection IDs encountered during resolution.
    /// Used to route query execution and validate against cross-connection joins.
    connection_ids: HashSet<i64>,
}

impl<'a> EntityRegistry<'a> {
    /// Create a new registry without namespace resolution (for tests/simple cases)
    pub fn new(schema: &'a dyn DatabaseSchema) -> Self {
        Self {
            database: DatabaseRegistry::new(schema),
            query_local: QueryLocalRegistry::new(),
            built_in: BuiltInRegistry::new(),
            consult: ConsultRegistry::new(),
            connection_ids: HashSet::new(),
        }
    }

    /// Create a new registry with namespace resolution support (via system reference)
    pub fn new_with_system(
        schema: &'a dyn DatabaseSchema,
        system: &'a crate::system::DelightQLSystem,
    ) -> Self {
        Self {
            database: DatabaseRegistry::new_with_system(schema, system),
            query_local: QueryLocalRegistry::new(),
            built_in: BuiltInRegistry::new(),
            consult: ConsultRegistry::new_with_system(system),
            connection_ids: HashSet::new(),
        }
    }

    /// Track a connection_id encountered during resolution.
    /// Called when a table is resolved to record which connection it belongs to.
    pub fn track_connection_id(&mut self, connection_id: i64) {
        self.connection_ids.insert(connection_id);
    }

    /// Validate that all resolved tables belong to the same connection.
    /// Returns Ok(connection_id) if all tables are on the same connection,
    /// or Err with a descriptive error if tables span multiple connections.
    pub fn validate_single_connection(&self) -> crate::error::Result<Option<i64>> {
        match self.connection_ids.len() {
            0 => Ok(None), // No tables resolved (e.g., pure literal query)
            1 => Ok(self.connection_ids.iter().next().copied()),
            _ => {
                let ids: Vec<_> = self.connection_ids.iter().collect();
                Err(DelightQLError::validation_error_categorized(
                    "operational/federation-prohibited",
                    format!(
                        "Query references tables from multiple database connections ({:?}). \
                         Cross-connection joins are not supported.",
                        ids
                    ),
                    "Cross-connection join detected",
                ))
            }
        }
    }
}

/// Registry for entities from database catalog
pub struct DatabaseRegistry<'a> {
    schema: &'a dyn DatabaseSchema,
    /// Optional system reference for namespace resolution
    pub(crate) system: Option<&'a crate::system::DelightQLSystem>,
}

impl<'a> DatabaseRegistry<'a> {
    /// Create without namespace resolution support (for tests/simple cases)
    pub fn new(schema: &'a dyn DatabaseSchema) -> Self {
        Self {
            schema,
            system: None,
        }
    }

    /// Create with namespace resolution support (via system)
    pub fn new_with_system(
        schema: &'a dyn DatabaseSchema,
        system: &'a crate::system::DelightQLSystem,
    ) -> Self {
        Self {
            schema,
            system: Some(system),
        }
    }

    /// Lookup a table in the database
    pub fn lookup_table(&self, name: &str) -> Option<CprSchema> {
        // Parse the name to check if it has a schema qualifier
        let (schema, table_name) = if let Some(dot_pos) = name.find('.') {
            let schema_part = &name[..dot_pos];
            let table_part = &name[dot_pos + 1..];
            (Some(schema_part), table_part)
        } else {
            (None, name)
        };

        self.schema
            .get_table_columns(schema, table_name)
            .map(|columns| {
                let column_metadata = columns
                    .into_iter()
                    .enumerate()
                    .map(|(idx, col)| {
                        use crate::pipeline::asts::resolved::PhaseBox;

                        ColumnMetadata::new(
                            ColumnProvenance::from_column(col.name.clone()),
                            FqTable {
                                parents_path: schema
                                    .map(|s| NamespacePath::single(s))
                                    .unwrap_or_else(|| NamespacePath::empty()),
                                name: TableName::Named(table_name.to_string().into()),
                                // For old-style lookup, backend schema is same as the parsed schema
                                backend_schema: PhaseBox::from_optional_schema(
                                    schema.map(|s| s.to_string()),
                                ),
                            },
                            Some(idx + 1), // 1-based position
                        )
                    })
                    .collect();

                CprSchema::Resolved(column_metadata)
            })
    }

    /// Resolve a namespace path to its backend schema name and connection ID.
    /// Returns `(Option<schema_name>, connection_id)` if found, `None` if not found.
    /// When `schema_name` is None, tables live in `main` schema of that connection.
    pub fn resolve_namespace(
        &self,
        namespace_path: &NamespacePath,
    ) -> crate::error::Result<Option<(Option<String>, i64)>> {
        let types_namespace_path = namespace_path.to_types_namespace_path();
        if let Some(system) = self.system {
            system.resolve_namespace_path(&types_namespace_path)
        } else {
            Ok(None)
        }
    }

    pub fn lookup_table_with_namespace(
        &self,
        namespace_path: &NamespacePath,
        table_name: &str,
    ) -> crate::error::Result<Option<(CprSchema, i64, delightql_types::SqlIdentifier)>> {
        debug!(
            "lookup_table_with_namespace called: namespace={:?}, table={}",
            namespace_path, table_name
        );
        // Resolve namespace to get the backend schema and connection ID
        // e.g., namespace=["c"] → lookup "_::c" → backend_schema="_c", connection_id=2
        // Convert core's rich NamespacePath to types version for system call
        let types_namespace_path = namespace_path.to_types_namespace_path();
        let (backend_schema_opt, connection_id) = if let Some(system) = self.system {
            match system.resolve_namespace_path(&types_namespace_path)? {
                Some((schema, conn_id)) => (schema, Some(conn_id)),
                None => {
                    // Namespace not found
                    return Ok(None);
                }
            }
        } else {
            // No system - return None to signal namespace not found
            // This happens in tests or when namespace resolution isn't set up
            return Ok(None);
        };

        debug!(
            "REGISTRY: connection_id={:?}, table={}",
            connection_id, table_name
        );

        // Get canonical entity name from bootstrap (for case-sensitive backends)
        let canonical_name: delightql_types::SqlIdentifier = if let Some(system) = self.system {
            let fq: String = namespace_path
                .items()
                .iter()
                .map(|i| i.name.as_str())
                .collect::<Vec<_>>()
                .join("::");
            system
                .get_canonical_entity_name(&fq, table_name)?
                .unwrap_or_else(|| delightql_types::SqlIdentifier::new(table_name))
        } else {
            delightql_types::SqlIdentifier::new(table_name)
        };

        // For connection_id=1 (bootstrap), introspect the bootstrap connection directly
        // For connection_id=2 (user), use the injected schema (existing behavior)
        debug!(
            "lookup_table_with_namespace: connection_id={:?}, backend_schema={:?}, table={}",
            connection_id, backend_schema_opt, table_name
        );
        let columns = if let Some(conn_id) = connection_id {
            if conn_id == 1 {
                // Bootstrap connection introspection
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // Native: introspect directly using rusqlite
                    debug!(
                        "Introspecting bootstrap connection for table: {}",
                        table_name
                    );
                    let system = self.system.ok_or_else(|| {
                        crate::error::DelightQLError::validation_error(
                            "No system available",
                            "Cannot introspect bootstrap connection without system reference",
                        )
                    })?;

                    let bootstrap_conn = system.get_bootstrap_connection();
                    let conn = bootstrap_conn.lock().map_err(|e| {
                        crate::error::DelightQLError::connection_poison_error(
                            "Failed to acquire bootstrap connection lock",
                            format!("Connection was poisoned: {}", e),
                        )
                    })?;

                    // Use PRAGMA table_xinfo to get column information (includes generated columns)
                    let query = format!("PRAGMA table_xinfo('{}')", table_name);
                    let mut stmt = conn.prepare(&query).map_err(|e| {
                        crate::error::DelightQLError::database_error(
                            format!("Failed to prepare PRAGMA query: {}", e),
                            e.to_string(),
                        )
                    })?;

                    let cols: Result<Vec<_>, _> = stmt
                        .query_map([], |row| {
                            let name: String = row.get(1)?;
                            let notnull: i32 = row.get(3)?;
                            let cid: i32 = row.get(0)?;

                            Ok(delightql_types::ColumnInfo {
                                name: name.into(),
                                nullable: notnull == 0,
                                position: (cid + 1) as usize,
                            })
                        })
                        .map_err(|e| {
                            crate::error::DelightQLError::database_error(
                                format!("Failed to query table_info: {}", e),
                                e.to_string(),
                            )
                        })?
                        .collect();

                    let cols = cols.map_err(|e| {
                        crate::error::DelightQLError::database_error(
                            format!("Failed to fetch column info: {}", e),
                            e.to_string(),
                        )
                    })?;

                    if cols.is_empty() {
                        return Ok(None);
                    }

                    Some(cols)
                }
                #[cfg(target_arch = "wasm32")]
                {
                    // WASM: Bootstrap connection not supported, return None
                    return Ok(None);
                }
            } else {
                // User connection or imported — pick the right schema provider.
                // Primary connection (id=2) uses self.schema; imported connections
                // use the per-connection schema from system.schema_map.
                let backend_schema = backend_schema_opt.as_deref();
                debug!(
                    "lookup_table_with_namespace: Using schema provider for table '{}' (canonical: '{}') in schema {:?}, connection_id={}",
                    table_name, canonical_name, backend_schema, conn_id
                );
                let effective_schema: &dyn DatabaseSchema = if conn_id != 2 {
                    if let Some(sys) = self.system {
                        if let Some(s) = sys.get_schema_map().get(&conn_id) {
                            s.as_ref()
                        } else {
                            self.schema
                        }
                    } else {
                        self.schema
                    }
                } else {
                    self.schema
                };
                effective_schema.get_table_columns(backend_schema, &canonical_name)
            }
        } else {
            // No connection_id - use existing schema lookup
            let backend_schema = backend_schema_opt.as_deref();
            self.schema
                .get_table_columns(backend_schema, &canonical_name)
        };

        let conn_id = connection_id.unwrap_or(2);

        Ok(columns.map(|columns| {
            let column_metadata = columns
                .into_iter()
                .enumerate()
                .map(|(idx, col)| {
                    use crate::pipeline::asts::resolved::PhaseBox;

                    ColumnMetadata::new(
                        ColumnProvenance::from_column(col.name.clone()),
                        FqTable {
                            parents_path: namespace_path.clone(),
                            name: TableName::Named(canonical_name.clone()),
                            backend_schema: PhaseBox::from_optional_schema(
                                backend_schema_opt.clone(),
                            ),
                        },
                        Some(idx + 1), // 1-based position
                    )
                })
                .collect();

            (
                CprSchema::Resolved(column_metadata),
                conn_id,
                canonical_name.clone(),
            )
        }))
    }

    /// Get the underlying schema for direct access when needed
    pub fn schema(&self) -> &'a dyn DatabaseSchema {
        self.schema
    }
}

/// Registry for entities defined in the current query
#[derive(Clone)]
pub struct QueryLocalRegistry {
    pub ctes: HashMap<String, CprSchema>,
    pub aliases: HashMap<String, String>,
    pub cfes: HashMap<String, crate::pipeline::ast_unresolved::PrecompiledCfeDefinition>,
}

impl Default for QueryLocalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryLocalRegistry {
    pub fn new() -> Self {
        Self {
            ctes: HashMap::new(),
            aliases: HashMap::new(),
            cfes: HashMap::new(),
        }
    }

    pub fn register_cte(&mut self, name: String, schema: CprSchema) {
        self.ctes.insert(name, schema);
    }

    pub fn register_alias(&mut self, alias: String, target: String) {
        self.aliases.insert(alias, target);
    }

    pub fn register_cfe(&mut self, cfe: crate::pipeline::ast_unresolved::PrecompiledCfeDefinition) {
        self.cfes.insert(cfe.name.clone(), cfe);
    }

    pub fn lookup_cte(&self, name: &str) -> Option<&CprSchema> {
        self.ctes.get(name)
    }

    pub fn resolve_alias(&self, alias: &str) -> Option<&str> {
        self.aliases.get(alias).map(|s| s.as_str())
    }
}

/// Registry for language built-in functions
#[derive(Clone)]
pub struct BuiltInRegistry {
    pub functions: HashSet<String>,
    pub aggregates: HashSet<String>,
}

impl Default for BuiltInRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltInRegistry {
    pub fn new() -> Self {
        let mut functions = HashSet::new();
        let mut aggregates = HashSet::new();

        functions.insert("upper".to_string());
        functions.insert("lower".to_string());
        functions.insert("trim".to_string());
        functions.insert("length".to_string());
        functions.insert("substr".to_string());
        functions.insert("replace".to_string());
        functions.insert("coalesce".to_string());
        functions.insert("greatest".to_string());
        functions.insert("least".to_string());
        functions.insert("abs".to_string());
        functions.insert("round".to_string());

        aggregates.insert("sum".to_string());
        aggregates.insert("count".to_string());
        aggregates.insert("avg".to_string());
        aggregates.insert("min".to_string());
        aggregates.insert("max".to_string());
        aggregates.insert("group_concat".to_string());

        Self {
            functions,
            aggregates,
        }
    }

    /// Check if a function is known
    pub fn is_known_function(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        self.functions.contains(&lower) || self.aggregates.contains(&lower)
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.aggregates.contains(&name.to_lowercase())
    }
}

/// HO parameter kind — mirrors `ddl::HoParamKind` but lives in the registry layer.
#[derive(Debug, Clone, PartialEq)]
pub enum HoParamKind {
    /// `T(*)` — structural/duck-typed table parameter
    Glob,
    /// `T(x, y)` — positionally-typed table parameter
    Argumentative(Vec<String>),
    /// `n` — scalar value, or legacy bare table name
    Scalar,
    /// `"value"` or `42` — ground scalar literal (constant in this clause)
    GroundScalar(String),
}

/// A parameter of a consulted entity (function or HO view).
#[derive(Debug, Clone)]
pub struct HoParamInfo {
    pub name: String,
    pub kind: HoParamKind,
}

impl HoParamInfo {
    /// Create a scalar/legacy param (used for functions and old-style HO views).
    pub fn scalar(name: String) -> Self {
        Self {
            name,
            kind: HoParamKind::Scalar,
        }
    }
}

/// A consulted entity retrieved from the bootstrap database
#[derive(Debug, Clone)]
pub struct ConsultedEntity {
    /// Entity name
    pub name: delightql_types::SqlIdentifier,
    /// Entity type (1=Function, 4=View)
    pub entity_type: i32,
    /// Full definition source text (head + neck + body, e.g. "double:(x) :- x * 2").
    /// body_parser extracts the body portion automatically.
    pub definition: String,
    /// Parameters with kind metadata
    pub params: Vec<HoParamInfo>,
    /// Cross-clause unified position analysis (populated for HO views with new schema).
    /// Empty for non-HO entities or when sys tables lack the new columns (backward compat).
    pub positions: Vec<crate::pipeline::asts::ddl::HoPositionInfo>,
    /// Namespace where entity is activated
    pub namespace: String,
}

/// Registry for entities from consult files
///
/// Queries the bootstrap database to look up consulted definitions.
/// Used by the resolver during grounded resolution to find functions and views
/// activated in specific namespaces.
pub struct ConsultRegistry {
    /// Optional system reference for bootstrap queries
    system: Option<*const crate::system::DelightQLSystem>,
}

// SAFETY: The ConsultRegistry only holds a raw pointer to the system, which
// is owned by the resolver's caller and guaranteed to outlive the registry.
// The raw pointer is used to break the circular reference (system contains
// bootstrap, registry queries bootstrap).
unsafe impl Send for ConsultRegistry {}
unsafe impl Sync for ConsultRegistry {}

impl Default for ConsultRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Namespace scope for ER-rule queries
#[cfg(not(target_arch = "wasm32"))]
enum ErRuleScope<'a> {
    /// Only rules from namespaces enlisted into 'main'
    Enlisted,
    /// Only rules from a specific namespace (by fq_name)
    Namespace(&'a str),
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> ErRuleScope<'a> {
    /// SQL fragments for single-pair queries (namespace JOIN condition + extra JOINs).
    /// Returns (ns_join_condition, extra_joins) to splice into the query.
    /// Namespace param is always ?5 in single queries (?1=ctx, ?2=left, ?3=right, ?4=type).
    fn sql_fragments_single(&self) -> (&'static str, &'static str) {
        match self {
            Self::Enlisted => (
                "",
                "JOIN enlisted_namespace bn ON bn.from_namespace_id = n.id \
                 JOIN namespace main_ns ON main_ns.id = bn.to_namespace_id \
                    AND main_ns.fq_name = 'main'",
            ),
            Self::Namespace(_) => (" AND n.fq_name = ?5", ""),
        }
    }

    /// SQL fragments for multi queries (all rules in context).
    /// Namespace param is ?3 in multi queries (?1=ctx, ?2=type).
    fn sql_fragments_multi(&self) -> (&'static str, &'static str) {
        match self {
            Self::Enlisted => (
                "",
                "JOIN enlisted_namespace bn ON bn.from_namespace_id = n.id \
                 JOIN namespace main_ns ON main_ns.id = bn.to_namespace_id \
                    AND main_ns.fq_name = 'main'",
            ),
            Self::Namespace(_) => (" AND n.fq_name = ?3", ""),
        }
    }
}

impl ConsultRegistry {
    pub fn new() -> Self {
        Self { system: None }
    }

    /// Create with a system reference for bootstrap queries
    pub fn new_with_system(system: &crate::system::DelightQLSystem) -> Self {
        Self {
            system: Some(system as *const _),
        }
    }

    /// Query parameter info for an entity.
    ///
    /// For HO views (entity_type=8), reads from `ho_param` + `ho_param_column`
    /// to get kind metadata. For all other types, reads from `entity_attribute`
    /// and wraps as Scalar.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_params(
        conn: &rusqlite::Connection,
        entity_id: i32,
        entity_type: i32,
    ) -> Vec<HoParamInfo> {
        use crate::enums::EntityType as BootstrapEntityType;

        // Try ho_param table first for HO views
        if entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression.as_i32() {
            if let Ok(params) = Self::query_ho_params(conn, entity_id) {
                if !params.is_empty() {
                    return params;
                }
            }
            // Fall through to entity_attribute for legacy HO views without ho_param rows
        }

        // Default: read from entity_attribute, wrap as Scalar
        let mut stmt = match conn.prepare(
            "SELECT attribute_name FROM entity_attribute
             WHERE entity_id = ?1 AND attribute_type = 'input_param'
             ORDER BY position",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(rusqlite::params![entity_id], |row| row.get::<_, String>(0))
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .map(HoParamInfo::scalar)
            .collect()
    }

    /// Read structured HO param metadata from ho_param + ho_param_column tables.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_ho_params(
        conn: &rusqlite::Connection,
        entity_id: i32,
    ) -> rusqlite::Result<Vec<HoParamInfo>> {
        let mut stmt = conn.prepare(
            "SELECT id, param_name, kind FROM ho_param
             WHERE entity_id = ?1
             ORDER BY position",
        )?;
        let rows: Vec<(i32, String, String)> = stmt
            .query_map(rusqlite::params![entity_id], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut params = Vec::new();
        for (hp_id, name, kind_str) in rows {
            let kind = match kind_str.as_str() {
                "glob" => HoParamKind::Glob,
                "argumentative" => {
                    // Read column names for this argumentative param
                    let mut col_stmt = conn.prepare(
                        "SELECT column_name FROM ho_param_column
                         WHERE ho_param_id = ?1
                         ORDER BY column_position",
                    )?;
                    let columns: Vec<String> = col_stmt
                        .query_map(rusqlite::params![hp_id], |row| row.get::<_, String>(0))?
                        .filter_map(|r| r.ok())
                        .collect();
                    HoParamKind::Argumentative(columns)
                }
                "ground_scalar" => HoParamKind::GroundScalar(name.clone()),
                _ => HoParamKind::Scalar,
            };
            params.push(HoParamInfo { name, kind });
        }
        Ok(params)
    }

    /// Read cross-clause position analysis from ho_param + ho_param_ground_value tables.
    ///
    /// Returns empty Vec if the new columns (ground_mode, column_name) are not present
    /// (backward compatibility with older bootstrap schemas).
    #[cfg(not(target_arch = "wasm32"))]
    fn query_ho_positions(
        conn: &rusqlite::Connection,
        entity_id: i32,
    ) -> Vec<crate::pipeline::asts::ddl::HoPositionInfo> {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode, HoPositionInfo};

        let mut stmt = match conn.prepare(
            "SELECT id, param_name, position, kind, ground_mode, column_name
             FROM ho_param WHERE entity_id = ?1 ORDER BY position",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(), // Schema doesn't have new columns
        };

        let rows: Vec<(i32, String, i32, String, Option<String>, Option<String>)> = match stmt
            .query_map(rusqlite::params![entity_id], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            }) {
            Ok(r) => r.filter_map(|r| r.ok()).collect(),
            Err(_) => return Vec::new(),
        };

        let mut positions = Vec::new();
        for (hp_id, _name, position, kind_str, ground_mode_str, column_name) in rows {
            // Skip rows without ground_mode (old schema)
            let ground_mode_str = match ground_mode_str {
                Some(s) => s,
                None => continue,
            };

            let column_kind = match kind_str.as_str() {
                "glob" => HoColumnKind::TableGlob,
                "argumentative" => {
                    let columns = Self::query_argumentative_columns(conn, hp_id);
                    HoColumnKind::TableArgumentative(columns)
                }
                _ => HoColumnKind::Scalar,
            };

            let ground_mode = match ground_mode_str.as_str() {
                "pure_ground" => HoGroundMode::PureGround,
                "mixed_ground" => HoGroundMode::MixedGround,
                "pure_unbound" => HoGroundMode::PureUnbound,
                "input_only" => HoGroundMode::InputOnly,
                _ => HoGroundMode::PureUnbound,
            };

            // Read ground values
            let ground_values = Self::query_ground_values(conn, hp_id);

            positions.push(HoPositionInfo {
                position: position as usize,
                column_kind,
                ground_mode,
                ground_values,
                column_name,
            });
        }

        positions
    }

    /// Read argumentative column names for an ho_param_id.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_argumentative_columns(conn: &rusqlite::Connection, hp_id: i32) -> Vec<String> {
        let mut stmt = match conn.prepare(
            "SELECT column_name FROM ho_param_column
             WHERE ho_param_id = ?1 ORDER BY column_position",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map(rusqlite::params![hp_id], |row| row.get::<_, String>(0))
            .map(|rows| rows.filter_map(|r| r.ok()).collect())
            .unwrap_or_default()
    }

    /// Read per-clause ground values for an ho_param_id.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_ground_values(conn: &rusqlite::Connection, hp_id: i32) -> Vec<(usize, String)> {
        let mut stmt = match conn.prepare(
            "SELECT clause_ordinal, ground_value
             FROM ho_param_ground_value WHERE ho_param_id = ?1
             ORDER BY clause_ordinal",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map(rusqlite::params![hp_id], |row| {
            Ok((row.get::<_, i32>(0)? as usize, row.get::<_, String>(1)?))
        })
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    /// Look up a consulted entity by name and namespace
    ///
    /// Queries bootstrap: entity JOIN activated_entity JOIN namespace
    /// where entity.name = name AND namespace.fq_name = namespace_fq
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_entity(&self, name: &str, namespace_fq: &str) -> Option<ConsultedEntity> {
        let system = self.system?;
        // SAFETY: System pointer is valid for the lifetime of the resolver
        let system_ref = unsafe { &*system };

        // Lazy-load stdlib module if needed (no-op for non-std:: namespaces)
        system_ref.ensure_stdlib_loaded(namespace_fq);

        // Catalog functor: name like "std::string::" lives in sys::meta but
        // refers to namespace "std::string". Lazy-load that namespace first
        // so its catalog wrapper gets registered before we look it up.
        if namespace_fq == "sys::meta" {
            if let Some(ns) = name.strip_suffix("::") {
                system_ref.ensure_stdlib_loaded(ns);
            }
            system_ref.ensure_catalog_loaded();
        }

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().ok()?;

        let mut stmt = conn
            .prepare(
                "SELECT e.id, e.name, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE e.name = ?1 COLLATE NOCASE
                   AND (n.fq_name = ?2
                        OR n.id IN (SELECT target_namespace_id FROM namespace_alias WHERE alias = ?2))",
            )
            .ok()?;

        let result = stmt
            .query_row(rusqlite::params![name, namespace_fq], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .ok()?;

        let (entity_id, entity_name, entity_type, definition, namespace) = result;
        let definition = definition.unwrap_or_default();

        // Look up parameters for functions (type 1, 3) and HO views (type 8)
        let is_ho = entity_type == EntityType::DqlHoTemporaryViewExpression.as_i32();
        let params = if EntityType::from_i32(entity_type).map_or(false, |t| t.is_fn()) || is_ho {
            Self::query_params(&conn, entity_id, entity_type)
        } else {
            Vec::new()
        };
        let positions = if is_ho {
            Self::query_ho_positions(&conn, entity_id)
        } else {
            Vec::new()
        };

        Some(ConsultedEntity {
            name: entity_name.into(),
            entity_type,
            definition,
            params,
            positions,
            namespace,
        })
    }

    /// WASM stub: consult lookups not supported
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_entity(&self, _name: &str, _namespace_fq: &str) -> Option<ConsultedEntity> {
        None
    }

    /// Look up a consulted function by name across all namespaces enlisted into "main".
    ///
    /// Used for function inlining via enlist (as opposed to grounding).
    /// Only returns functions (entity_type = 1) from consulted namespaces.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_function(
        &self,
        name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for enlisted function lookup",
                format!("{}", e),
            )
        })?;

        let mut stmt = conn
            .prepare(
                "WITH RECURSIVE reachable(ns_id) AS (
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace main_ns ON main_ns.id = en.to_namespace_id
                       AND main_ns.fq_name = 'main'
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted function lookup",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![name, EntityType::DqlFunctionExpression.as_i32()],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to query enlisted functions", e.to_string())
            })?
            .filter_map(|r| r.ok())
            .collect();

        match rows.len() {
            0 => Ok(None),
            1 => {
                let (entity_id, entity_name, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: entity_name.into(),
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, ns)| ns.clone()).collect();
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous unqualified function '{}': found in multiple enlisted namespaces [{}]. \
                         Use qualified syntax (e.g., {}.{}:(args)) to disambiguate.",
                        name,
                        namespaces.join(", "),
                        namespaces[0],
                        name,
                    ),
                    "Ambiguous enlisted function",
                ))
            }
        }
    }

    /// WASM stub: consult lookups not supported
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_enlisted_function(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up a consulted context-aware function (entity_type = 3) by unqualified name
    /// across all namespaces enlisted into "main".
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_context_aware_function(
        &self,
        name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for enlisted context-aware function lookup",
                format!("{}", e),
            )
        })?;

        let mut stmt = conn
            .prepare(
                "WITH RECURSIVE reachable(ns_id) AS (
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace main_ns ON main_ns.id = en.to_namespace_id
                       AND main_ns.fq_name = 'main'
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted context-aware function lookup",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![name, EntityType::DqlContextAwareFunctionExpression.as_i32()],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to query enlisted context-aware functions",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();

        match rows.len() {
            0 => Ok(None),
            1 => {
                let (entity_id, entity_name, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: entity_name.into(),
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, ns)| ns.clone()).collect();
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous unqualified context-aware function '{}': found in multiple enlisted namespaces [{}].",
                        name,
                        namespaces.join(", "),
                    ),
                    "Ambiguous enlisted context-aware function",
                ))
            }
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_enlisted_context_aware_function(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up a consulted sigma predicate (entity_type = 9) by unqualified name
    /// across all namespaces enlisted into "main".
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_sigma(
        &self,
        name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for enlisted sigma lookup",
                format!("{}", e),
            )
        })?;

        let mut stmt = conn
            .prepare(
                "WITH RECURSIVE reachable(ns_id) AS (
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace main_ns ON main_ns.id = en.to_namespace_id
                       AND main_ns.fq_name = 'main'
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted sigma lookup",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![name, EntityType::DqlTemporarySigmaRule.as_i32()],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to query enlisted sigma predicates",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();

        match rows.len() {
            0 => Ok(None),
            1 => {
                let (entity_id, entity_name, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: entity_name.into(),
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, ns)| ns.clone()).collect();
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous unqualified sigma predicate '{}': found in multiple enlisted namespaces [{}]. \
                         Use qualified syntax to disambiguate.",
                        name,
                        namespaces.join(", "),
                    ),
                    "Ambiguous enlisted sigma predicate",
                ))
            }
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_enlisted_sigma(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up an enlisted HO view (entity_type = 8) by unqualified name
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_ho_view(
        &self,
        name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for enlisted HO view lookup",
                format!("{}", e),
            )
        })?;

        let mut stmt = conn
            .prepare(
                "WITH RECURSIVE reachable(ns_id) AS (
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace main_ns ON main_ns.id = en.to_namespace_id
                       AND main_ns.fq_name = 'main'
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted HO view lookup",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![name, EntityType::DqlHoTemporaryViewExpression.as_i32()],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to query enlisted HO views", e.to_string())
            })?
            .filter_map(|r| r.ok())
            .collect();

        match rows.len() {
            0 => Ok(None),
            1 => {
                let (entity_id, entity_name, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let params = Self::query_params(&conn, entity_id, entity_type);
                let positions = Self::query_ho_positions(&conn, entity_id);
                Ok(Some(ConsultedEntity {
                    name: entity_name.into(),
                    entity_type,
                    definition,
                    params,
                    positions,
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, ns)| ns.clone()).collect();
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous unqualified HO view '{}': found in multiple enlisted namespaces [{}].",
                        name,
                        namespaces.join(", "),
                    ),
                    "Ambiguous enlisted HO view",
                ))
            }
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_enlisted_ho_view(
        &self,
        _name: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    // --- ER-Rule lookup methods ---
    //
    // Six public methods organized along two dimensions:
    //   Scope: Enlisted | Namespace(fq) | AllConsulted
    //   Cardinality: Single (by table pair) | Multi (all in context)
    //
    // Shared logic is factored into private helpers below.

    /// Look up an ER-rule by (context, table_a, table_b) across enlisted namespaces.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_er_rule(
        &self,
        context: &str,
        table_a: &str,
        table_b: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        self.query_er_rule_single(context, table_a, table_b, ErRuleScope::Enlisted)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn lookup_er_rule(
        &self,
        _context: &str,
        _table_a: &str,
        _table_b: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up ALL ER-rules in a context across enlisted namespaces.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_er_rules_in_context(
        &self,
        context: &str,
    ) -> std::result::Result<Vec<(String, String, ConsultedEntity)>, DelightQLError> {
        self.query_er_rules_multi(context, ErRuleScope::Enlisted)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn lookup_er_rules_in_context(
        &self,
        _context: &str,
    ) -> std::result::Result<Vec<(String, String, ConsultedEntity)>, DelightQLError> {
        Ok(Vec::new())
    }

    /// Look up a specific ER-rule scoped to a namespace (for qualified view body resolution).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_er_rule_for_namespace(
        &self,
        context: &str,
        table_a: &str,
        table_b: &str,
        namespace_fq: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        self.query_er_rule_single(
            context,
            table_a,
            table_b,
            ErRuleScope::Namespace(namespace_fq),
        )
    }

    #[cfg(target_arch = "wasm32")]
    pub fn lookup_er_rule_for_namespace(
        &self,
        _context: &str,
        _table_a: &str,
        _table_b: &str,
        _namespace_fq: &str,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up ALL ER-rules in a context scoped to a namespace (for qualified view body resolution).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_er_rules_in_context_for_namespace(
        &self,
        context: &str,
        namespace_fq: &str,
    ) -> std::result::Result<Vec<(String, String, ConsultedEntity)>, DelightQLError> {
        self.query_er_rules_multi(context, ErRuleScope::Namespace(namespace_fq))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn lookup_er_rules_in_context_for_namespace(
        &self,
        _context: &str,
        _namespace_fq: &str,
    ) -> std::result::Result<Vec<(String, String, ConsultedEntity)>, DelightQLError> {
        Ok(Vec::new())
    }

    // --- Private ER-rule query implementation ---

    /// Query a single ER-rule by (context, table_a, table_b) with scope filtering.
    /// Returns at most one rule; errors on cross-namespace ambiguity.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_er_rule_single(
        &self,
        context: &str,
        table_a: &str,
        table_b: &str,
        scope: ErRuleScope,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error("Failed to acquire bootstrap lock", format!("{}", e))
        })?;

        // Canonical ordering: alphabetical
        let (left, right) = if table_a <= table_b {
            (table_a, table_b)
        } else {
            (table_b, table_a)
        };

        let (ns_join_cond, extra_joins) = scope.sql_fragments_single();

        let sql = format!(
            "SELECT e.name, e.type, ec.definition, n.fq_name
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id{ns_join_cond}
             {extra_joins}
             JOIN er_rule er ON er.entity_id = e.id
             JOIN entity_clause ec ON ec.entity_id = e.id AND ec.ordinal = er.clause_ordinal
             WHERE er.context_name = ?1
               AND er.left_table = ?2 AND er.right_table = ?3
               AND e.type = ?4"
        );

        let entity_type = EntityType::DqlErContextRule.as_i32();
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error("Failed to prepare ER-rule lookup", e.to_string())
        })?;

        let row_mapper = |row: &rusqlite::Row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
            ))
        };

        let rows: Vec<(String, i32, Option<String>, String)> = match scope {
            ErRuleScope::Namespace(ns) => stmt.query_map(
                rusqlite::params![context, left, right, entity_type, ns],
                row_mapper,
            ),
            _ => stmt.query_map(
                rusqlite::params![context, left, right, entity_type],
                row_mapper,
            ),
        }
        .map_err(|e| DelightQLError::database_error("Failed to query ER-rules", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

        // Check for cross-namespace ambiguity
        let namespaces: std::collections::HashSet<&str> =
            rows.iter().map(|(_, _, _, ns)| ns.as_str()).collect();
        if namespaces.len() > 1 {
            let ns_list: Vec<&str> = namespaces.into_iter().collect();
            return Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous ER-rule for ({}, {}) in context '{}': found in namespaces [{}].{}",
                    table_a,
                    table_b,
                    context,
                    ns_list.join(", "),
                    "",
                ),
                "Ambiguous ER-rule",
            ));
        }

        match rows.into_iter().next() {
            None => Ok(None),
            Some((entity_name, entity_type, definition, namespace)) => Ok(Some(ConsultedEntity {
                name: entity_name.into(),
                entity_type,
                definition: definition.unwrap_or_default(),
                params: Vec::new(),
                positions: Vec::new(),
                namespace,
            })),
        }
    }

    /// Query all ER-rules in a context with scope filtering.
    /// Returns (left_table, right_table, entity) tuples.
    #[cfg(not(target_arch = "wasm32"))]
    fn query_er_rules_multi(
        &self,
        context: &str,
        scope: ErRuleScope,
    ) -> std::result::Result<Vec<(String, String, ConsultedEntity)>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(Vec::new());
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error("Failed to acquire bootstrap lock", format!("{}", e))
        })?;

        let (ns_join_cond, extra_joins) = scope.sql_fragments_multi();

        let sql = format!(
            "SELECT e.name, e.type, ec.definition, n.fq_name,
                    er.left_table, er.right_table
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id{ns_join_cond}
             {extra_joins}
             JOIN er_rule er ON er.entity_id = e.id
             JOIN entity_clause ec ON ec.entity_id = e.id AND ec.ordinal = er.clause_ordinal
             WHERE er.context_name = ?1
               AND e.type = ?2"
        );

        let entity_type = EntityType::DqlErContextRule.as_i32();
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error("Failed to prepare ER-rules lookup", e.to_string())
        })?;

        let row_mapper = |row: &rusqlite::Row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i32>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        };

        let rows: Vec<(String, i32, Option<String>, String, String, String)> = match scope {
            ErRuleScope::Namespace(ns) => {
                stmt.query_map(rusqlite::params![context, entity_type, ns], row_mapper)
            }
            _ => stmt.query_map(rusqlite::params![context, entity_type], row_mapper),
        }
        .map_err(|e| DelightQLError::database_error("Failed to query ER-rules", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

        Ok(rows
            .into_iter()
            .map(
                |(entity_name, entity_type, definition, namespace, left, right)| {
                    (
                        left,
                        right,
                        ConsultedEntity {
                            name: entity_name.into(),
                            entity_type,
                            definition: definition.unwrap_or_default(),
                            params: Vec::new(),
                            positions: Vec::new(),
                            namespace,
                        },
                    )
                },
            )
            .collect())
    }

    /// Query the default_data_ns for a namespace (set by ground!).
    ///
    /// Returns Some(data_ns_fq_name) if the namespace was created via ground!
    /// and has a pre-bound data namespace, None otherwise.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_namespace_default_data_ns(&self, namespace_fq: &str) -> Option<String> {
        let system = self.system?;
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().ok()?;

        conn.query_row(
            "SELECT default_data_ns FROM namespace WHERE fq_name = ?1 AND default_data_ns IS NOT NULL",
            [namespace_fq],
            |row| row.get::<_, String>(0),
        )
        .ok()
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn get_namespace_default_data_ns(&self, _namespace_fq: &str) -> Option<String> {
        None
    }

    /// Temporarily activate namespace-local enlists into the DDL's own namespace scope.
    /// Returns the list of (from_namespace_id, to_namespace_id) rows inserted,
    /// for later deactivation.
    ///
    /// Engages are scoped to the DDL namespace (not main) so that
    /// `resolve_unqualified_entity(name, ddl_ns)` sees only the DDL's enlisted
    /// entities — avoiding ambiguity with main's entities when names overlap.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn activate_namespace_local_enlists(&self, namespace_fq: &str) -> Vec<(i32, i32)> {
        self.activate_namespace_local_enlists_into(namespace_fq, namespace_fq)
    }

    /// Activate namespace-local enlists into "main" so that `lookup_enlisted_function`
    /// (which searches enlisted_namespace with to_namespace = main) can find them.
    /// Used by BorrowedInliner when inlining functions from nested namespaces.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn activate_namespace_local_enlists_into_main(
        &self,
        namespace_fq: &str,
    ) -> Vec<(i32, i32)> {
        self.activate_namespace_local_enlists_into(namespace_fq, "main")
    }

    /// Core: activate namespace-local enlists, inserting enlisted_namespace rows
    /// pointing to `target_ns_fq`.
    #[cfg(not(target_arch = "wasm32"))]
    fn activate_namespace_local_enlists_into(
        &self,
        namespace_fq: &str,
        target_ns_fq: &str,
    ) -> Vec<(i32, i32)> {
        let Some(system) = self.system else {
            return Vec::new();
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = match bootstrap.lock() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        // Get the target namespace ID
        let target_ns_id: i32 = match conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [target_ns_fq],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => return Vec::new(),
        };

        // Get namespace-local enlist IDs
        let mut stmt = match conn.prepare(
            "SELECT nle.enlisted_namespace_id
             FROM namespace_local_enlist nle
             JOIN namespace ns ON ns.id = nle.namespace_id AND ns.fq_name = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let enlisted_ids: Vec<i32> =
            match stmt.query_map([namespace_fq], |row| row.get::<_, i32>(0)) {
                Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                Err(_) => return Vec::new(),
            };
        drop(stmt);

        // Insert each as an enlisted_namespace (from=enlisted_ns, to=target_ns)
        let mut inserted = Vec::new();
        for enlisted_id in enlisted_ids {
            // Only insert if not already enlisted
            let already = conn.query_row(
                "SELECT COUNT(*) FROM enlisted_namespace WHERE from_namespace_id = ?1 AND to_namespace_id = ?2",
                rusqlite::params![enlisted_id, target_ns_id],
                |row| row.get::<_, i32>(0),
            ).unwrap_or(0);

            if already == 0 {
                if conn.execute(
                    "INSERT INTO enlisted_namespace (from_namespace_id, to_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![enlisted_id, target_ns_id],
                ).is_ok() {
                    inserted.push((enlisted_id, target_ns_id));
                }
            }
        }

        inserted
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn activate_namespace_local_enlists(&self, _namespace_fq: &str) -> Vec<(i32, i32)> {
        Vec::new()
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn activate_namespace_local_enlists_into_main(
        &self,
        _namespace_fq: &str,
    ) -> Vec<(i32, i32)> {
        Vec::new()
    }

    /// Deactivate previously activated namespace-local enlists.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn deactivate_namespace_local_enlists(&self, activated: &[(i32, i32)]) {
        let Some(system) = self.system else {
            return;
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = match bootstrap.lock() {
            Ok(c) => c,
            Err(_) => return,
        };

        for (from_id, to_id) in activated {
            let _ = conn.execute(
                "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 AND to_namespace_id = ?2",
                rusqlite::params![from_id, to_id],
            );
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn deactivate_namespace_local_enlists(&self, _activated: &[(i32, i32)]) {}

    /// Activate namespace-local aliases for a DDL namespace during view resolution.
    /// Returns the list of aliases inserted (for cleanup).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn activate_namespace_local_aliases(&self, namespace_fq: &str) -> Vec<(String, i32)> {
        let Some(system) = self.system else {
            return Vec::new();
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = match bootstrap.lock() {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        // Query namespace_local_alias for aliases defined in this DDL namespace
        let mut stmt = match conn.prepare(
            "SELECT nla.alias, nla.target_namespace_id
             FROM namespace_local_alias nla
             JOIN namespace ns ON ns.id = nla.namespace_id AND ns.fq_name = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let alias_rows: Vec<(String, i32)> = match stmt.query_map([namespace_fq], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?))
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(_) => return Vec::new(),
        };
        drop(stmt);

        // Insert each into namespace_alias (only if not already present)
        let mut inserted = Vec::new();
        for (alias, target_id) in alias_rows {
            let already = conn
                .query_row(
                    "SELECT COUNT(*) FROM namespace_alias WHERE alias = ?1",
                    rusqlite::params![alias],
                    |row| row.get::<_, i32>(0),
                )
                .unwrap_or(0);

            if already == 0 {
                if conn
                    .execute(
                        "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                        rusqlite::params![alias, target_id],
                    )
                    .is_ok()
                {
                    inserted.push((alias, target_id));
                }
            }
        }

        inserted
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn activate_namespace_local_aliases(&self, _namespace_fq: &str) -> Vec<(String, i32)> {
        Vec::new()
    }

    /// Deactivate previously activated namespace-local aliases.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn deactivate_namespace_local_aliases(&self, activated: &[(String, i32)]) {
        let Some(system) = self.system else {
            return;
        };
        let system_ref = unsafe { &*system };

        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = match bootstrap.lock() {
            Ok(c) => c,
            Err(_) => return,
        };

        for (alias, _target_id) in activated {
            let _ = conn.execute(
                "DELETE FROM namespace_alias WHERE alias = ?1",
                rusqlite::params![alias],
            );
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn deactivate_namespace_local_aliases(&self, _activated: &[(String, i32)]) {}
}
