// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Registry implementations for tracking entities from various sources

use crate::enums::EntityType;
use crate::error::DelightQLError;
use crate::names::{Addressing, ColumnOrigin, Hint, Registry, ScopeId, ScopeOrigin, ValueFacts};
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::resolver::DatabaseSchema;
use crate::system::PRIMARY_CONNECTION_ID;
use log::debug;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Unified registry for all entity sources
pub struct EntityRegistry<'a> {
    pub identities: Rc<Registry>,
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
    pub fn new(schema: &'a dyn DatabaseSchema, identities: Rc<Registry>) -> Self {
        Self {
            database: DatabaseRegistry::new(schema, Rc::clone(&identities)),
            identities,
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
        identities: Rc<Registry>,
    ) -> Self {
        Self {
            database: DatabaseRegistry::new_with_system(schema, system, Rc::clone(&identities)),
            identities,
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

    /// Run `body` inside a LEXICAL BINDING EXTENT: the query-scoped
    /// bindings it introduces — CTE registrations and CFE definitions —
    /// end when it returns, resolved and refused alike, so a consulted
    /// body's own bindings can never replace or outlive the caller's.
    ///
    /// One door for both maps, so a binding kind cannot leak by being
    /// forgotten in a hand-paired save/restore. Materialized relations
    /// and session aliases stay out: they are plan and session state, not
    /// lexical bindings, and an extent that swallowed them would undo
    /// registrations the enclosing plan still owns.
    pub fn with_binding_extent<T>(&mut self, body: impl FnOnce(&mut Self) -> T) -> T {
        let saved_ctes = self.query_local.ctes.clone();
        let saved_cfes = self.query_local.scoped_cfes.clone();
        let result = body(self);
        self.query_local.ctes = saved_ctes;
        self.query_local.scoped_cfes = saved_cfes;
        result
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
    identities: Rc<Registry>,
    schema: &'a dyn DatabaseSchema,
    /// Optional system reference for namespace resolution
    pub(crate) system: Option<&'a crate::system::DelightQLSystem>,
}

impl<'a> DatabaseRegistry<'a> {
    fn catalog_heading(
        &self,
        table_name: &str,
        columns: Vec<delightql_types::schema::ColumnInfo>,
    ) -> crate::names::ScopeId {
        let table_spelling = self.identities.intern(table_name, false);
        let entity = self.identities.mint_entity(table_spelling);
        let scope = self.identities.mint_scope(
            ScopeOrigin::BaseTable { entity },
            Hint::User(table_spelling),
            None,
        );

        for (idx, col) in columns.into_iter().enumerate() {
            let published = self
                .identities
                .intern(col.name.as_str(), col.name.is_stropped());
            let declared_type = col.declared_type.clone();
            self.identities.mint_column(
                scope,
                ColumnOrigin::CatalogColumn {
                    entity,
                    position: idx as u32,
                },
                Some(published),
                Addressing::Published,
                ValueFacts {
                    declared_type,
                    ..Default::default()
                },
            );
        }
        scope
    }

    /// Create without namespace resolution support (for tests/simple cases)
    pub fn new(schema: &'a dyn DatabaseSchema, identities: Rc<Registry>) -> Self {
        Self {
            identities,
            schema,
            system: None,
        }
    }

    /// Create with namespace resolution support (via system)
    pub fn new_with_system(
        schema: &'a dyn DatabaseSchema,
        system: &'a crate::system::DelightQLSystem,
        identities: Rc<Registry>,
    ) -> Self {
        Self {
            identities,
            schema,
            system: Some(system),
        }
    }

    /// Lookup a table in the database
    pub fn lookup_table(&self, name: &str) -> crate::error::Result<Option<ScopeId>> {
        // Parse the name to check if it has a schema qualifier
        let (schema, table_name) = if let Some(dot_pos) = name.find('.') {
            let schema_part = &name[..dot_pos];
            let table_part = &name[dot_pos + 1..];
            (Some(schema_part), table_part)
        } else {
            (None, name)
        };

        Ok(self
            .schema
            .get_table_columns(schema, table_name)?
            .map(|columns| self.catalog_heading(table_name, columns)))
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

    /// Namespace lookup for an UNQUALIFIED reference (the resolver reached
    /// this namespace by searching, not because the user spelled it). When
    /// a session-materialized temp shadows a same-name physical entity,
    /// this path answers the TEMP (bare spelling; the engine's temp-first
    /// resolution and `BootstrapBackedSchema`'s session-first preference
    /// agree) — materialize-pipe §6's ruled shadowing.
    pub fn lookup_table_with_namespace(
        &self,
        namespace_path: &NamespacePath,
        table_name: &str,
    ) -> crate::error::Result<Option<(ScopeId, i64, delightql_types::SqlIdentifier, Option<String>)>>
    {
        self.lookup_table_with_namespace_impl(namespace_path, table_name, false)
    }

    /// Namespace lookup for a QUALIFIED reference (`main.staged(*)`) — the
    /// user spelled the namespace, so a session-materialized shadow must
    /// NOT win: materialize-pipe §6 scopes the temp shadow to unqualified
    /// names only. On a shadow collision this returns the competitor
    /// (physical) entity's columns with an explicit backend schema, so the
    /// generated SQL spells `main.<table>` and bypasses the engine's own
    /// temp-first resolution. Pinned by session_shadow_tests::
    /// qualified_read_reaches_physical_after_same_name_temp.
    pub fn lookup_table_with_namespace_qualified(
        &self,
        namespace_path: &NamespacePath,
        table_name: &str,
    ) -> crate::error::Result<Option<(ScopeId, i64, delightql_types::SqlIdentifier, Option<String>)>>
    {
        self.lookup_table_with_namespace_impl(namespace_path, table_name, true)
    }

    /// Resolve an explicitly passthrough relation.
    ///
    /// Catalog lookup remains first: it carries activation, shadowing, and
    /// qualification policy. A miss may still be a backend-owned relation
    /// that the catalog deliberately does not enumerate. For the primary
    /// target, ask its live introspector for that one name and mint the same
    /// identity-backed heading ordinary catalog resolution would have made.
    pub fn lookup_passthrough_table_with_namespace(
        &self,
        namespace_path: &NamespacePath,
        table_name: &str,
    ) -> crate::error::Result<Option<(ScopeId, i64, delightql_types::SqlIdentifier, Option<String>)>>
    {
        if let Some(found) =
            self.lookup_table_with_namespace_qualified(namespace_path, table_name)?
        {
            return Ok(Some(found));
        }

        let Some(system) = self.system else {
            return Ok(None);
        };
        let Some((backend_schema, connection_id)) = self.resolve_namespace(namespace_path)? else {
            return Ok(None);
        };
        if connection_id != PRIMARY_CONNECTION_ID {
            return Ok(None);
        }

        let namespace_fq = namespace_path
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>()
            .join("::");
        let introspection_schema = system
            .physical_schema_alias_for_namespace(&namespace_fq, connection_id)?
            .or_else(|| backend_schema.clone());
        let Some(discovered) =
            system.introspect_passthrough_relation(introspection_schema.as_deref(), table_name)?
        else {
            return Ok(None);
        };

        let entity = discovered.entity;
        let canonical_name = entity.name;
        let columns = entity
            .attributes
            .into_iter()
            .map(|attribute| delightql_types::schema::ColumnInfo {
                name: attribute.name,
                nullable: attribute.is_nullable,
                position: (attribute.position + 1) as usize,
                declared_type: (!attribute.data_type.is_empty()).then_some(attribute.data_type),
            })
            .collect();
        let scope = self.catalog_heading(canonical_name.as_str(), columns);

        Ok(Some((
            scope,
            connection_id,
            canonical_name,
            // The backend reports the schema that answers execution. It may
            // differ from the requested introspection schema for a
            // connection-local catalog.
            discovered.backend_schema,
        )))
    }

    fn lookup_table_with_namespace_impl(
        &self,
        namespace_path: &NamespacePath,
        table_name: &str,
        qualified: bool,
    ) -> crate::error::Result<Option<(ScopeId, i64, delightql_types::SqlIdentifier, Option<String>)>>
    {
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
            let activated_name = system.get_canonical_entity_name(&fq, table_name)?;
            // Bootstrap routing is not exposure. Several sys::* namespaces
            // share connection 1, but a physical bootstrap table is public in
            // a namespace only when an entity was activated THERE. Without
            // this gate the raw PRAGMA fallback below made every bootstrap
            // table reachable through every sys::* qualifier.
            if connection_id == Some(1) && activated_name.is_none() {
                return Ok(None);
            }
            activated_name.unwrap_or_else(|| delightql_types::SqlIdentifier::new(table_name))
        } else {
            delightql_types::SqlIdentifier::new(table_name)
        };

        // For connection_id=1 (bootstrap), introspect the bootstrap connection directly
        // For connection_id=2 (user), use the injected schema (existing behavior)
        debug!(
            "lookup_table_with_namespace: connection_id={:?}, backend_schema={:?}, table={}",
            connection_id, backend_schema_opt, table_name
        );

        // Shadow punch-through, QUALIFIED references only (temp shadows
        // main for unqualified names; a qualified read must reach the
        // physical entity). When a
        // session-materialized entity and a competitor share this name in the
        // namespace, answer the COMPETITOR's registered columns with an
        // explicit backend schema — the ATTACH alias where the namespace's
        // physical tables live (PRAGMA database_list recovery, the
        // imprint_namespace precedent) — so the generated SQL spells
        // `<alias>.<table>`, bypassing the engine's temp-first resolution.
        // `session_shadow_split` is gated on the session having materialized
        // anything, so plain sessions never reach bootstrap here. Pinned by
        // session_shadow_tests::qualified_read_reaches_physical_after_same_name_temp;
        // the bare counterpart (no punch-through) by
        // session_shadow_tests::bare_read_prefers_session_materialized_temp.
        if qualified && backend_schema_opt.is_none() && connection_id != Some(1) {
            if let Some(system) = self.system {
                let fq: String = namespace_path
                    .items()
                    .iter()
                    .map(|i| i.name.as_str())
                    .collect::<Vec<_>>()
                    .join("::");
                if let Some((_session_id, competitor_id)) =
                    system.session_shadow_split(&fq, table_name)?
                {
                    let physical_alias = system
                        .physical_schema_alias_for_namespace(&fq, connection_id.unwrap_or(2))?;
                    if let Some(alias) = physical_alias {
                        let cols = system.output_columns_for_entity(competitor_id)?;
                        if !cols.is_empty() {
                            let scope = self.catalog_heading(&canonical_name, cols);
                            return Ok(Some((
                                scope,
                                connection_id.unwrap_or(2),
                                canonical_name.clone(),
                                Some(alias),
                            )));
                        }
                    }
                }
            }
        }

        // Curated safe-subset guard: for bootstrap system tables (connection_id
        // == 1), star-expansion below reads the raw physical schema via PRAGMA.
        // When the entity is activated in this namespace with an explicit
        // registered column set, honor that instead. Bootstrap tables normally
        // register every physical column (so this is behavior-preserving),
        // except where a curated entity deliberately omits secret columns —
        // sys::connections.connection never registers resource_uri/identity, so
        // they stay structurally unprojectable.
        if connection_id == Some(1) {
            let fq_name: String = namespace_path
                .items()
                .iter()
                .map(|i| i.name.as_str())
                .collect::<Vec<_>>()
                .join("::");
            if let Some(cols) = self
                .schema
                .get_table_columns(Some(&fq_name), &canonical_name)?
            {
                if !cols.is_empty() {
                    let scope = self.catalog_heading(&canonical_name, cols);
                    return Ok(Some((
                        scope,
                        1,
                        canonical_name.clone(),
                        backend_schema_opt.clone(),
                    )));
                }
            }
        }

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
                            let decltype: String = row.get(2)?;
                            let notnull: i32 = row.get(3)?;
                            let cid: i32 = row.get(0)?;

                            Ok(delightql_types::ColumnInfo {
                                name: name.into(),
                                nullable: notnull == 0,
                                position: (cid + 1) as usize,
                                declared_type: (!decltype.is_empty()).then_some(decltype),
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
                effective_schema.get_table_columns(backend_schema, &canonical_name)?
            }
        } else {
            // No connection_id - use existing schema lookup
            let backend_schema = backend_schema_opt.as_deref();
            self.schema
                .get_table_columns(backend_schema, &canonical_name)?
        };

        let conn_id = connection_id.unwrap_or(2);

        Ok(columns.map(|columns| {
            let scope = self.catalog_heading(&canonical_name, columns);

            (
                scope,
                conn_id,
                canonical_name.clone(),
                backend_schema_opt.clone(),
            )
        }))
    }

    /// Get the underlying schema for direct access when needed
    pub fn schema(&self) -> &'a dyn DatabaseSchema {
        self.schema
    }
}

/// Registry for entities defined in the current query
///
/// Every map here is keyed by the authored spelling and compares by the
/// identifier law — `SqlIdentifier`'s equality folds an unstropped spelling
/// and keeps a stropped one verbatim — so a folded reference reaches its
/// binding and a stropped case survivor stays a different name.
#[derive(Clone)]
pub struct QueryLocalRegistry {
    pub ctes: HashMap<delightql_types::SqlIdentifier, ScopeId>,
    materialized_relations: HashMap<delightql_types::SqlIdentifier, ScopeId>,
    pub aliases: HashMap<delightql_types::SqlIdentifier, delightql_types::SqlIdentifier>,
    /// Query-scoped value definitions, held AS AUTHORED and keyed by the
    /// name's canonical identity. Each is spent WHOLE at its call sites
    /// during resolution — the definition is macro-like, so nothing of it
    /// survives to compare later and no callable identity is minted for
    /// it: an identity nothing can consume would be ceremony.
    pub scoped_cfes:
        HashMap<delightql_types::SqlIdentifier, crate::pipeline::asts::core::CfeDefinition>,
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
            materialized_relations: HashMap::new(),
            aliases: HashMap::new(),
            scoped_cfes: HashMap::new(),
        }
    }

    pub fn register_cte(&mut self, name: delightql_types::SqlIdentifier, schema: ScopeId) {
        self.ctes.insert(name, schema);
    }

    /// Register a relation that earlier statements in the same plan create.
    ///
    /// Its heading is query-local knowledge, but the relation is a physical
    /// DML target rather than a SQL CTE. Keeping it out of `ctes` preserves
    /// that distinction while a same-name CTE can still shadow it.
    pub fn register_materialized_relation(
        &mut self,
        name: delightql_types::SqlIdentifier,
        schema: ScopeId,
    ) {
        self.materialized_relations.insert(name, schema);
    }

    pub fn register_alias(
        &mut self,
        alias: delightql_types::SqlIdentifier,
        target: delightql_types::SqlIdentifier,
    ) {
        self.aliases.insert(alias, target);
    }

    /// Register a query-scoped value definition. A later same-named
    /// definition shadows an earlier one — nearest wins, under the
    /// identifier law's agreement.
    pub fn register_scoped_cfe(&mut self, cfe: crate::pipeline::asts::core::CfeDefinition) {
        self.scoped_cfes.insert(cfe.name.clone(), cfe);
    }

    pub fn lookup_cte(&self, name: &delightql_types::SqlIdentifier) -> Option<&ScopeId> {
        self.ctes.get(name)
    }

    pub fn lookup_materialized_relation(
        &self,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<&ScopeId> {
        self.materialized_relations.get(name)
    }

    pub fn resolve_alias(
        &self,
        alias: &delightql_types::SqlIdentifier,
    ) -> Option<&delightql_types::SqlIdentifier> {
        self.aliases.get(alias)
    }
}

/// Registry for language built-in functions
#[derive(Clone)]
pub struct BuiltInRegistry {
    pub functions: HashSet<String>,
    pub aggregates: HashSet<String>,
    /// The engine window builtins and their argument bounds (min, max).
    /// The one compile-time signature authority for these names — a
    /// rebuilt invocation is judged here, never by the engine's error.
    pub window_signatures: HashMap<&'static str, (u8, u8)>,
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

        let mut window_signatures = HashMap::new();
        window_signatures.insert("row_number", (0, 0));
        window_signatures.insert("rank", (0, 0));
        window_signatures.insert("dense_rank", (0, 0));
        window_signatures.insert("percent_rank", (0, 0));
        window_signatures.insert("cume_dist", (0, 0));
        window_signatures.insert("ntile", (1, 1));
        window_signatures.insert("lag", (1, 3));
        window_signatures.insert("lead", (1, 3));
        window_signatures.insert("first_value", (1, 1));
        window_signatures.insert("last_value", (1, 1));
        window_signatures.insert("nth_value", (2, 2));

        aggregates.insert("sum".to_string());
        aggregates.insert("count".to_string());
        aggregates.insert("avg".to_string());
        aggregates.insert("min".to_string());
        aggregates.insert("max".to_string());
        aggregates.insert("group_concat".to_string());

        Self {
            functions,
            aggregates,
            window_signatures,
        }
    }

    /// The argument bounds of an engine window builtin, if the name is one.
    pub fn window_signature(&self, name: &str) -> Option<(u8, u8)> {
        self.window_signatures
            .get(name.to_lowercase().as_str())
            .copied()
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

/// A parameter of a consulted entity, read back from the catalog.
///
/// It is the SAME `HoParam` the head produced. A stored row reconstructs
/// the parameter the signature declared; a mirror type here would be a
/// second answer to what a parameter is, differing from the first exactly
/// where nobody looked.
pub type HoParamInfo = crate::pipeline::asts::core::definitions::HoParam;

/// A parameter carrying only its name. `entity_attribute` records the name
/// and nothing else, so a parameter reconstructed from it is Scalar by the
/// limit of what was stored, not by a declaration.
pub fn scalar_param(name: String) -> HoParamInfo {
    HoParamInfo::Scalar {
        name: delightql_types::SqlIdentifier::new(name),
        guard: None,
        callable: false,
    }
}

/// A consulted entity retrieved from the bootstrap database
#[derive(Debug, Clone)]
pub struct ConsultedEntity {
    /// Entity name
    pub name: delightql_types::SqlIdentifier,
    /// Entity kind: the enum, not its i32
    /// encoding — the catalog stores i32; conversion happens at load.
    pub entity_type: crate::enums::EntityType,
    /// Full definition source text (head + neck + body, e.g. "double:(x) :- x * 2").
    /// body_parser extracts the body portion automatically.
    pub definition: String,
    /// Parameters with kind metadata
    pub params: Vec<HoParamInfo>,
    /// Cross-clause unified position analysis. Empty for anything that is
    /// not an HO view, and for an HO view whose head declares no parameters.
    pub positions: Vec<crate::pipeline::asts::ddl::HoPositionInfo>,
    /// Namespace where entity is activated
    pub namespace: String,
}

/// THE DECLARED MODE, as the catalog holds it.
///
/// The ordered input and output attributes of one entity's functional
/// dependency, with the authored identifiers' stropping preserved. This is
/// the DECLARATION — not the arms, which live in the clause source like every
/// other body. Nothing re-derives it from source text, argument count, an
/// `entity_attribute` string, or a callable category.
#[derive(Debug, Clone, PartialEq)]
pub struct DeclaredMode {
    pub inputs: Vec<delightql_types::SqlIdentifier>,
    pub outputs: Vec<delightql_types::SqlIdentifier>,
}

impl DeclaredMode {
    /// The position the named output occupies, by exact identifier
    /// agreement — a stropped name compares verbatim, an unstropped one
    /// folds.
    pub fn output_position(&self, name: &delightql_types::SqlIdentifier) -> Option<usize> {
        self.outputs.iter().position(|declared| declared == name)
    }

    /// Whether this declaration and one read from a stored definition are
    /// the SAME declaration: same roles, same order, same names, same
    /// stropping. The catalog chooses the selected POSITION and the source
    /// supplies the expression AT that position, so agreement by width alone
    /// would let an equal-width disagreement select the wrong output.
    pub fn agrees_with(
        &self,
        inputs: &[delightql_types::SqlIdentifier],
        outputs: &[delightql_types::SqlIdentifier],
    ) -> bool {
        fn same(
            a: &[delightql_types::SqlIdentifier],
            b: &[delightql_types::SqlIdentifier],
        ) -> bool {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(left, right)| left == right && left.is_stropped() == right.is_stropped())
        }
        same(&self.inputs, inputs) && same(&self.outputs, outputs)
    }

    /// The declared outputs, for teaching a pick that named none of them.
    pub fn output_spellings(&self) -> String {
        self.outputs
            .iter()
            .map(|name| format!(".{name}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Registry for entities from consult files
///
/// Queries the bootstrap database to look up consulted definitions.
/// Used by the resolver during grounded resolution to find functions and views
/// activated in specific namespaces.
pub struct ConsultRegistry {
    /// Optional system reference for bootstrap queries
    system: Option<*const crate::system::DelightQLSystem>,
    /// Whether ANY entity in the catalog declares a functional dependency.
    ///
    /// A call in value position asks the mode authority before the ordinary
    /// road, because a declared mode is what makes an entity callable at
    /// all. Where nothing declares one there is nothing to ask about, and
    /// this answers that once per compilation instead of once per call.
    any_mode: std::cell::Cell<Option<bool>>,
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
    /// Only rules from namespaces enlisted into the session scope `home`
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
                // Admit namespaces enlisted into `home` AND
                // `home` itself (in-session definitions live in the scope).
                "JOIN namespace scope_ns ON scope_ns.fq_name = 'home' \
                    AND (n.id = scope_ns.id OR EXISTS (SELECT 1 \
                         FROM enlisted_namespace bn \
                         WHERE bn.from_namespace_id = n.id \
                           AND bn.to_namespace_id = scope_ns.id))",
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
                // Admit namespaces enlisted into `home` AND
                // `home` itself (in-session definitions live in the scope).
                "JOIN namespace scope_ns ON scope_ns.fq_name = 'home' \
                    AND (n.id = scope_ns.id OR EXISTS (SELECT 1 \
                         FROM enlisted_namespace bn \
                         WHERE bn.from_namespace_id = n.id \
                           AND bn.to_namespace_id = scope_ns.id))",
            ),
            Self::Namespace(_) => (" AND n.fq_name = ?3", ""),
        }
    }
}

impl ConsultRegistry {
    pub fn new() -> Self {
        Self {
            system: None,
            any_mode: std::cell::Cell::new(None),
        }
    }

    /// Create with a system reference for bootstrap queries
    pub fn new_with_system(system: &crate::system::DelightQLSystem) -> Self {
        Self {
            system: Some(system as *const _),
            any_mode: std::cell::Cell::new(None),
        }
    }

    /// Whether the catalog holds any declared mode at all.
    ///
    /// An OPTIMIZATION, and it refuses rather than guesses. Answering `false`
    /// because the catalog could not be read would send a declared-mode call
    /// down the ordinary-call road, where the name is handed to the target —
    /// a wrong answer produced by a failure to look.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn any_declared_mode(&self) -> std::result::Result<bool, DelightQLError> {
        if let Some(known) = self.any_mode.get() {
            return Ok(known);
        }
        let Some(system) = self.system else {
            return Ok(false);
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for declared mode probe",
                format!("{e}"),
            )
        })?;
        let exists: i64 = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM functional_dependency)",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to probe for declared modes", e.to_string())
            })?;
        let answer = exists != 0;
        self.any_mode.set(Some(answer));
        Ok(answer)
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn any_declared_mode(&self) -> std::result::Result<bool, DelightQLError> {
        Ok(false)
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
        entity_type: crate::enums::EntityType,
    ) -> Vec<HoParamInfo> {
        use crate::enums::EntityType as BootstrapEntityType;

        // Try ho_param table first for HO views
        if entity_type == BootstrapEntityType::DqlHoTemporaryViewExpression {
            if let Ok(params) = Self::query_ho_params(conn, entity_id) {
                if !params.is_empty() {
                    return params;
                }
            }
            // A view whose position analysis produced nothing — a head with no
            // parameters — writes no ho_param rows, and entity_attribute is
            // then the only record of what the head declared.
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
        rows.filter_map(|r| r.ok()).map(scalar_param).collect()
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

        use crate::pipeline::asts::core::definitions::{HeadItem, HeadItems};
        let mut params = Vec::new();
        for (hp_id, name, kind_str) in rows {
            let identifier = delightql_types::SqlIdentifier::new(name.clone());
            params.push(match kind_str.as_str() {
                "glob" => HoParamInfo::Relation {
                    name: identifier,
                    cols: HeadItems::Glob,
                },
                "argumentative" => {
                    // Read column names for this argumentative param
                    let mut col_stmt = conn.prepare(
                        "SELECT column_name FROM ho_param_column
                         WHERE ho_param_id = ?1
                         ORDER BY column_position",
                    )?;
                    let columns: Vec<HeadItem> = col_stmt
                        .query_map(rusqlite::params![hp_id], |row| row.get::<_, String>(0))?
                        .filter_map(|r| r.ok())
                        .map(HeadItem::plumb)
                        .collect();
                    HoParamInfo::Relation {
                        name: identifier,
                        cols: HeadItems::Listed(columns),
                    }
                }
                // The stored name of a ground position IS its value.
                "ground_scalar" => HoParamInfo::Ground {
                    name: identifier,
                    text: name,
                },
                _ => HoParamInfo::Scalar {
                    name: identifier,
                    guard: None,
                    callable: false,
                },
            });
        }
        Ok(params)
    }

    /// Read cross-clause position analysis from ho_param + ho_param_ground_value tables.
    ///
    /// A read that cannot be prepared or stepped yields no positions rather
    /// than an error: absent analysis is a legible state here, and the
    /// parameter names remain available from `entity_attribute`.
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
            Err(_) => return Vec::new(),
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
    pub fn lookup_entity(
        &self,
        name: &str,
        name_stropped: bool,
        namespace_fq: &str,
        scope: Option<&str>,
    ) -> Option<ConsultedEntity> {
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
                "SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND (n.fq_name = ?2
                        OR (?3 IS NULL AND n.id IN (
                              SELECT target_namespace_id FROM namespace_alias
                              WHERE alias = ?2))
                        OR (?3 IS NOT NULL AND n.id IN (
                              SELECT nla.target_namespace_id
                              FROM namespace_local_alias nla
                              JOIN namespace owner ON owner.id = nla.namespace_id
                                 AND owner.fq_name = ?3
                              WHERE nla.alias = ?2)))",
            )
            .ok()?;

        let map_row = |row: &rusqlite::Row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, i32>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
            ))
        };

        // The identifier law's agreement: an unstropped spelling folds, a
        // stropped one keeps its authored bytes.
        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let result =
            match stmt.query_row(rusqlite::params![canonical, namespace_fq, scope], map_row) {
                Ok(r) => r,
                Err(_) => {
                    // §IV MIDDLE ACCESS RUNG (plain qualifier): the exact
                    // (name, namespace_fq) pair missed. Consult the enlist set for
                    // an enlisted-parent namespace whose DIRECT child bears this
                    // plain qualifier (home first), then retry ONCE with the
                    // expanded fq. Fires only on this miss, so no lookup that
                    // resolves today is affected (§IV precedence rule 1). The
                    // returned entity carries the RESOLVED (expanded) namespace, so
                    // the blueprint safety net below and every downstream body
                    // resolution see the real fq. A plain-qualifier AMBIGUITY
                    // (multiple non-home parents) is loud on the relation door
                    // (`resolve_namespace_path`); here — a bare Option return — it
                    // degrades to a miss, and the caller surfaces "not found".
                    let expanded = crate::system::expand_plain_namespace(&conn, namespace_fq)
                        .ok()
                        .flatten()?;
                    stmt.query_row(rusqlite::params![canonical, expanded, scope], map_row)
                        .ok()?
                }
            };

        let (entity_id, entity_name, entity_stropped, entity_type, definition, namespace) = result;

        // §IV plain-qualifier SHADOW: if this is an EXACT hit on a
        // top-level namespace (`namespace == namespace_fq`, so no
        // expansion happened) and an enlisted `home::{namespace_fq}` child sits
        // shadowed behind it, warn that the full path is needed to reach it.
        // The `== namespace_fq` guard is load-bearing: it excludes the normal
        // expanded case (where `namespace` is the `home::…` fq), which must NOT
        // warn.
        if namespace == namespace_fq && crate::system::home_child_shadows(&conn, namespace_fq) {
            log::warn!(
                "plain qualifier '{n}' resolved to the top-level namespace '{n}'; an \
                 enlisted scratch child 'home::{n}' is shadowed behind it — spell \
                 'home::{n}' to reach it",
                n = namespace_fq
            );
        }

        // Blueprint inertness SAFETY NET (companion_linear--70/--74): if
        // the RESOLVED namespace is an archived blueprint (or nested under
        // one), treat the lookup as a miss. This is the quiet deep layer —
        // every consulted-lookup route (relations, function inlining, CFE
        // instantiation, HO/curried) funnels through lookup_entity, so no
        // present-or-future route can silently execute archived rules. The
        // LOUD badged refusals live at the front doors (resolve_namespace_path,
        // refuse_if_blueprint_fq below, enlist!, ground!). A failed scan
        // degrades to a miss too — the loud doors re-scan and surface it.
        if crate::system::blueprint_shadowing(&conn, &namespace)
            .ok()
            .flatten()
            .is_some()
        {
            return None;
        }

        let definition = definition.unwrap_or_default();
        // Unknown entity_type in the catalog = treat as lookup miss (the
        // catalog is compiler-owned; this is unreachable short of corruption).
        let entity_type = EntityType::from_i32(entity_type).ok()?;

        // Look up parameters for functions (type 1, 3) and HO views (type 8)
        let is_ho = entity_type == EntityType::DqlHoTemporaryViewExpression;
        let params = if entity_type.is_fn() || is_ho {
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
            name: if entity_stropped {
                delightql_types::SqlIdentifier::stropped(entity_name)
            } else {
                delightql_types::SqlIdentifier::new(entity_name)
            },
            entity_type,
            definition,
            params,
            positions,
            namespace,
        })
    }

    /// WASM stub: consult lookups not supported
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_entity(
        &self,
        _name: &str,
        _name_stropped: bool,
        _namespace_fq: &str,
        _scope: Option<&str>,
    ) -> Option<ConsultedEntity> {
        None
    }

    /// Loud front door for the FUNCTION-inlining route (companion_linear--74):
    /// refuse a namespace-qualified consulted lookup whose path is an archived
    /// blueprint, with the badged `imprint/blueprint/inert` error. The relation
    /// route gets the same refusal from `resolve_namespace_path`; this covers
    /// `grounding.rs`'s qualified colon-functor entries, which do not pass
    /// through it. (The quiet safety net in `lookup_entity` backstops every
    /// other route with a plain miss.)
    #[cfg(not(target_arch = "wasm32"))]
    pub fn refuse_if_blueprint_fq(&self, fq: &str) -> crate::error::Result<()> {
        let Some(system) = self.system else {
            return Ok(());
        };
        // SAFETY: System pointer is valid for the lifetime of the resolver
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let Ok(conn) = bootstrap.lock() else {
            return Ok(());
        };
        crate::system::refuse_if_blueprint(&conn, fq)
    }

    /// WASM stub: no consults, nothing to refuse.
    #[cfg(target_arch = "wasm32")]
    pub fn refuse_if_blueprint_fq(&self, _fq: &str) -> crate::error::Result<()> {
        Ok(())
    }

    /// Look up a consulted function by name across all namespaces enlisted into the `home` session scope.
    ///
    /// Used for function inlining via enlist (as opposed to grounding).
    /// Only returns functions (entity_type = 1) from consulted namespaces.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_function(
        &self,
        name: &str,
        name_stropped: bool,
        scope: Option<&str>,
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
                    SELECT id FROM namespace WHERE fq_name = ?3
                    UNION
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                       AND scope_ns.fq_name = ?3
                    UNION
                    SELECT nle.enlisted_namespace_id
                    FROM namespace_local_enlist nle
                    JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                       AND scope_ns2.fq_name = ?3
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted function lookup",
                    e.to_string(),
                )
            })?;

        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let rows: Vec<(i32, String, bool, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlFunctionExpression.as_i32(),
                    scope.unwrap_or("home")
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, i32>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
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
                let (entity_id, entity_name, entity_stropped, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let entity_type = EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?;
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: if entity_stropped {
                        delightql_types::SqlIdentifier::stropped(entity_name)
                    } else {
                        delightql_types::SqlIdentifier::new(entity_name)
                    },
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, _, ns)| ns.clone()).collect();
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
        _name_stropped: bool,
        _scope: Option<&str>,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up a consulted context-aware function (entity_type = 3) by unqualified name
    /// across all namespaces enlisted into the `home` session scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_context_aware_function(
        &self,
        name: &str,
        name_stropped: bool,
        scope: Option<&str>,
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
                    SELECT id FROM namespace WHERE fq_name = ?3
                    UNION
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                       AND scope_ns.fq_name = ?3
                    UNION
                    SELECT nle.enlisted_namespace_id
                    FROM namespace_local_enlist nle
                    JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                       AND scope_ns2.fq_name = ?3
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted context-aware function lookup",
                    e.to_string(),
                )
            })?;

        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let rows: Vec<(i32, String, bool, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlContextAwareFunctionExpression.as_i32(),
                    scope.unwrap_or("home")
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, i32>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
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
                let (entity_id, entity_name, entity_stropped, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let entity_type = EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?;
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: if entity_stropped {
                        delightql_types::SqlIdentifier::stropped(entity_name)
                    } else {
                        delightql_types::SqlIdentifier::new(entity_name)
                    },
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, _, ns)| ns.clone()).collect();
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
        _name_stropped: bool,
        _scope: Option<&str>,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// Look up a consulted sigma predicate (entity_type = 9) by unqualified name
    /// across all namespaces enlisted into the `home` session scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_sigma(
        &self,
        name: &str,
        name_stropped: bool,
        scope: Option<&str>,
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
                    SELECT id FROM namespace WHERE fq_name = ?3
                    UNION
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                       AND scope_ns.fq_name = ?3
                    UNION
                    SELECT nle.enlisted_namespace_id
                    FROM namespace_local_enlist nle
                    JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                       AND scope_ns2.fq_name = ?3
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted sigma lookup",
                    e.to_string(),
                )
            })?;

        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let rows: Vec<(i32, String, bool, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlTemporarySigmaRule.as_i32(),
                    scope.unwrap_or("home")
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, i32>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
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
                let (entity_id, entity_name, entity_stropped, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let entity_type = EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?;
                let params = Self::query_params(&conn, entity_id, entity_type);
                Ok(Some(ConsultedEntity {
                    name: if entity_stropped {
                        delightql_types::SqlIdentifier::stropped(entity_name)
                    } else {
                        delightql_types::SqlIdentifier::new(entity_name)
                    },
                    entity_type,
                    definition,
                    params,
                    positions: Vec::new(),
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, _, ns)| ns.clone()).collect();
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
        _scope: Option<&str>,
    ) -> std::result::Result<Option<ConsultedEntity>, DelightQLError> {
        Ok(None)
    }

    /// THE DECLARED MODE of a named entity, and the clause source its arms
    /// live in.
    ///
    /// One reading answers both of the questions the pick asks: whether the
    /// callee declares a functional dependency at all, and which output the
    /// name reaches. A qualified call reads the named namespace; an
    /// unqualified one reads the enlisted reach, exactly as every other
    /// name does.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_declared_mode(
        &self,
        name: &str,
        namespace: Option<&str>,
        scope: Option<&str>,
    ) -> std::result::Result<Option<(ConsultedEntity, DeclaredMode)>, DelightQLError> {
        use crate::bootstrap::enums::EntityType;

        let Some(system) = self.system else {
            return Ok(None);
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for declared mode lookup",
                format!("{}", e),
            )
        })?;

        let reach = if namespace.is_some() {
            "SELECT id FROM namespace WHERE fq_name = ?3"
        } else {
            "SELECT id FROM namespace WHERE fq_name = ?3
             UNION
             SELECT en.from_namespace_id
             FROM enlisted_namespace en
             JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                AND scope_ns.fq_name = ?3
             UNION
             SELECT nle.enlisted_namespace_id
             FROM namespace_local_enlist nle
             JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                AND scope_ns2.fq_name = ?3
             UNION
             SELECT exp.exposed_namespace_id
             FROM exposed_namespace exp
             JOIN reachable r ON r.ns_id = exp.exposing_namespace_id"
        };
        let sql = format!(
            "WITH RECURSIVE reachable(ns_id) AS ({reach})
             SELECT e.id, e.name,
                    (SELECT GROUP_CONCAT(ec.definition, char(10))
                     FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                    ) as definition,
                    n.fq_name
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             JOIN reachable r ON r.ns_id = n.id
             WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2
               AND EXISTS (SELECT 1 FROM functional_dependency fd WHERE fd.entity_id = e.id)"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error("Failed to prepare declared mode lookup", e.to_string())
        })?;
        // EVERY CANDIDATE, OR NONE. A row that will not decode is not
        // evidence of absence: dropping it here would turn an ambiguous
        // lookup into a unique winner, and a corrupt catalog would read as a
        // decision. The cardinality judgment below is only sound over the
        // complete candidate set.
        let rows: Vec<(i32, String, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    name,
                    EntityType::DqlFactExpression.as_i32(),
                    namespace.unwrap_or(scope.unwrap_or("home"))
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to query declared modes", e.to_string())
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| {
                DelightQLError::database_error(
                    "corrupt catalog: a declared-mode candidate row could not be read",
                    e.to_string(),
                )
            })?;

        match rows.len() {
            0 => Ok(None),
            1 => {
                let (entity_id, entity_name, definition, ns) = rows.into_iter().next().unwrap();
                // THE ENTITY HAS ALREADY ADVERTISED THE CAPABILITY — it was
                // selected BY having declaration rows — so a declaration that
                // will not read whole is corruption, not absence. Answering
                // `None` here would send the call down the ordinary-call road,
                // which is the "failed to read means absent" outcome this
                // reader exists to remove.
                let mode = Self::query_declared_mode(&conn, entity_id)?;
                Ok(Some((
                    ConsultedEntity {
                        name: entity_name.into(),
                        entity_type: EntityType::DqlFactExpression,
                        definition: definition.unwrap_or_default(),
                        params: Vec::new(),
                        positions: Vec::new(),
                        namespace: ns,
                    },
                    mode,
                )))
            }
            _ => Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous unqualified fact function '{name}': it declares a mode in \
                     several enlisted namespaces. Qualify the call to say which."
                ),
                "Ambiguous declared mode",
            )),
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_declared_mode(
        &self,
        _name: &str,
        _namespace: Option<&str>,
        _scope: Option<&str>,
    ) -> std::result::Result<Option<(ConsultedEntity, DeclaredMode)>, DelightQLError> {
        Ok(None)
    }

    /// The typed declaration rows, in declared order. A stropped name is
    /// rebuilt stropped, so the pick's comparison honours the spelling the
    /// author wrote.
    ///
    /// WHOLE OR NOT AT ALL. A declaration with no inputs, no outputs, a role
    /// outside the vocabulary, a stropping bit outside the vocabulary, or
    /// positions that are not `0..n` is malformed evidence, and malformed
    /// evidence is reported — never accepted as another lawful spelling and
    /// never rounded down to absence.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn query_declared_mode(
        conn: &rusqlite::Connection,
        entity_id: i32,
    ) -> std::result::Result<DeclaredMode, DelightQLError> {
        let mut stmt = conn
            .prepare(
                "SELECT role, position, attribute_name, stropped FROM functional_dependency
                 WHERE entity_id = ?1 ORDER BY role, position",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare functional dependency read",
                    e.to_string(),
                )
            })?;
        let rows: Vec<(String, i64, String, i64)> = stmt
            .query_map(rusqlite::params![entity_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to read functional dependency",
                    e.to_string(),
                )
            })?
            // A declaration is read whole or not at all. An unreadable row
            // silently omitted would narrow a mode nobody narrowed.
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| {
                DelightQLError::database_error(
                    "corrupt catalog: a functional dependency row could not be read",
                    e.to_string(),
                )
            })?;
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        for (role, position, name, stropped) in rows {
            let identifier = match stropped {
                0 => delightql_types::SqlIdentifier::new(name),
                1 => delightql_types::SqlIdentifier::stropped(name),
                other => {
                    return Err(DelightQLError::database_error(
                        "corrupt catalog: a functional dependency's stropping is neither \
                         stropped nor unstropped",
                        other.to_string(),
                    ))
                }
            };
            let side =
                match role.as_str() {
                    "input" => &mut inputs,
                    "output" => &mut outputs,
                    other => return Err(DelightQLError::database_error(
                        "corrupt catalog: a functional dependency role is neither input nor output",
                        other.to_string(),
                    )),
                };
            // The read is ordered by position, so each row's position must be
            // the next one. A gap or a repeat means the stored order is not
            // the declared order, and the selected POSITION is chosen by it.
            if position != side.len() as i64 {
                return Err(DelightQLError::database_error(
                    "corrupt catalog: a functional dependency's positions are not the \
                     declared order",
                    format!("{role} at position {position}"),
                ));
            }
            side.push(identifier);
        }
        if inputs.is_empty() || outputs.is_empty() {
            return Err(DelightQLError::database_error(
                "corrupt catalog: a declared mode has no inputs or no outputs",
                format!("{} input(s), {} output(s)", inputs.len(), outputs.len()),
            ));
        }
        Ok(DeclaredMode { inputs, outputs })
    }

    /// Check if an enlisted table expression (entity_type = 6) exists by name.
    /// Used to detect DDL-defined facts that can be used as sigma predicates.
    #[cfg(not(target_arch = "wasm32"))]
    /// A consulted single-definition VIEW whose body references a
    /// runtime-served bin relation, reachable from `scope` unqualified or
    /// standing in `namespace_fq` when one was written.
    ///
    /// The executable boundary asks this so a definition WRAPPING
    /// `sys::execution.compile`/`explain_run` reaches the same execution
    /// road the top-level spelling does. Only a DIRECT reference answers:
    /// a view reaching the served relation through another view keeps the
    /// resolver's fail-closed fence.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_runtime_served_view(
        &self,
        name: &delightql_types::SqlIdentifier,
        namespace_fq: Option<&str>,
        scope: Option<&str>,
    ) -> crate::error::Result<Option<(String, String)>> {
        use crate::bootstrap::enums::EntityType;
        use crate::error::DelightQLError;
        let canonical = if name.is_stropped() {
            name.as_str().to_string()
        } else {
            name.as_str().to_ascii_lowercase()
        };
        let Some(system) = self.system else {
            return Ok(None);
        };
        // SAFETY: System pointer is valid for the lifetime of the resolver
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|_| {
            DelightQLError::database_error(
                "bootstrap connection lock poisoned during runtime-served lookup".to_string(),
                "runtime_served_lookup".to_string(),
            )
        })?;
        let (ns_filter, ns_param) = match namespace_fq {
            Some(fq) => ("n.fq_name = ?4", fq.to_string()),
            None => (
                "n.id IN (
                     WITH RECURSIVE reachable(ns_id) AS (
                         SELECT id FROM namespace WHERE fq_name = ?4
                         UNION
                         SELECT en.from_namespace_id
                         FROM enlisted_namespace en
                         JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                            AND scope_ns.fq_name = ?4
                         UNION
                         SELECT nle.enlisted_namespace_id
                         FROM namespace_local_enlist nle
                         JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                            AND scope_ns2.fq_name = ?4
                         UNION
                         SELECT exp.exposed_namespace_id
                         FROM exposed_namespace exp
                         JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                     )
                     SELECT ns_id FROM reachable
                 )",
                scope.unwrap_or("home").to_string(),
            ),
        };
        let sql = format!(
            "SELECT (SELECT GROUP_CONCAT(ec.definition, char(10))
                     FROM (SELECT definition FROM entity_clause
                           WHERE entity_id = e.id ORDER BY ordinal) ec),
                    n.fq_name
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
               AND e.type IN (?2, ?3)
               AND {ns_filter}
               AND EXISTS (
                   SELECT 1 FROM referenced_entity r
                   JOIN entity b ON b.name = r.name COLLATE NOCASE
                      AND b.type = {bin}
                   JOIN activated_entity bae ON bae.entity_id = b.id
                   JOIN namespace bn ON bn.id = bae.namespace_id
                      AND bn.fq_name = r.namespace
                   WHERE r.containing_entity_id = e.id)",
            ns_filter = ns_filter,
            bin = EntityType::BinRelation.as_i32(),
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error(
                format!("runtime-served lookup prepare failed: {e}"),
                e.to_string(),
            )
        })?;
        // The COMPLETE candidate set is read and judged. `query_row` would
        // take the first row, making execution depend on consultation
        // order; several candidates take the ordinary ambiguity road.
        let candidates: Vec<(Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlTemporaryViewExpression.as_i32(),
                    EntityType::DqlPermanentViewExpression.as_i32(),
                    ns_param,
                ],
                |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("runtime-served lookup query failed: {e}"),
                    e.to_string(),
                )
            })?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("runtime-served lookup row decode failed: {e}"),
                    e.to_string(),
                )
            })?;
        // A reachable view with NO clauses is catalog corruption, not
        // absence: it must not be pruned before the cardinality judgment.
        let mut found: Vec<(String, String)> = Vec::with_capacity(candidates.len());
        for (definition, fq) in candidates {
            match definition {
                Some(definition) => found.push((definition, fq)),
                None => {
                    return Err(DelightQLError::database_error(
                        format!(
                            "corrupt catalog: view '{}' in namespace '{fq}' has no \
                             entity_clause rows",
                            name.as_str()
                        ),
                        "runtime_served_lookup".to_string(),
                    ))
                }
            }
        }
        match found.len() {
            0 => Ok(None),
            1 => Ok(Some(found.remove(0))),
            _ => {
                let mut namespaces: Vec<&str> = found.iter().map(|(_, fq)| fq.as_str()).collect();
                namespaces.sort_unstable();
                Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    format!(
                        "Ambiguous entity '{}': found in namespaces {}. enlist!() brought overlapping names into scope.",
                        name.as_str(),
                        namespaces.join(", ")
                    ),
                    format!(
                        "use qualified access ({}.{}(*))",
                        namespaces.first().expect("several candidates"),
                        name.as_str()
                    ),
                ))
            }
        }
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_runtime_served_view(
        &self,
        _name: &delightql_types::SqlIdentifier,
        _namespace_fq: Option<&str>,
        _scope: Option<&str>,
    ) -> crate::error::Result<Option<(String, String)>> {
        Ok(None)
    }

    pub fn lookup_enlisted_table(
        &self,
        name: &str,
        scope: Option<&str>,
    ) -> std::result::Result<bool, DelightQLError> {
        use crate::bootstrap::enums::EntityType;

        let Some(system) = self.system else {
            return Ok(false);
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error(
                "Failed to acquire bootstrap lock for enlisted table lookup",
                format!("{}", e),
            )
        })?;

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE e.name = ?1 COLLATE NOCASE AND e.type = ?2
                 AND n.id IN (
                     WITH RECURSIVE reachable(ns_id) AS (
                         SELECT id FROM namespace WHERE fq_name = ?3
                         UNION
                         SELECT en.from_namespace_id
                         FROM enlisted_namespace en
                         JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                            AND scope_ns.fq_name = ?3
                         UNION
                         SELECT nle.enlisted_namespace_id
                         FROM namespace_local_enlist nle
                         JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                            AND scope_ns2.fq_name = ?3
                         UNION
                         SELECT exp.exposed_namespace_id
                         FROM exposed_namespace exp
                         JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                     )
                     SELECT ns_id FROM reachable
                 )",
                rusqlite::params![
                    name,
                    EntityType::DqlFactExpression.as_i32(),
                    scope.unwrap_or("home")
                ],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count > 0)
    }

    /// WASM stub
    #[cfg(target_arch = "wasm32")]
    pub fn lookup_enlisted_table(
        &self,
        _name: &str,
        _scope: Option<&str>,
    ) -> std::result::Result<bool, DelightQLError> {
        Ok(false)
    }

    /// Look up an enlisted HO view (entity_type = 8) by unqualified name
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lookup_enlisted_ho_view(
        &self,
        name: &str,
        name_stropped: bool,
        scope: Option<&str>,
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
                    SELECT id FROM namespace WHERE fq_name = ?3
                    UNION
                    SELECT en.from_namespace_id
                    FROM enlisted_namespace en
                    JOIN namespace scope_ns ON scope_ns.id = en.to_namespace_id
                       AND scope_ns.fq_name = ?3
                    UNION
                    SELECT nle.enlisted_namespace_id
                    FROM namespace_local_enlist nle
                    JOIN namespace scope_ns2 ON scope_ns2.id = nle.namespace_id
                       AND scope_ns2.fq_name = ?3
                    UNION
                    SELECT exp.exposed_namespace_id
                    FROM exposed_namespace exp
                    JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                 )
                 SELECT e.id, e.name, e.name_stropped, e.type,
                        (SELECT GROUP_CONCAT(ec.definition, char(10))
                         FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                        ) as definition,
                        n.fq_name
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN reachable r ON r.ns_id = n.id
                 WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
                   AND e.type = ?2",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted HO view lookup",
                    e.to_string(),
                )
            })?;

        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let rows: Vec<(i32, String, bool, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlHoTemporaryViewExpression.as_i32(),
                    scope.unwrap_or("home")
                ],
                |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, i32>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
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
                let (entity_id, entity_name, entity_stropped, entity_type, definition, namespace) =
                    rows.into_iter().next().unwrap();
                let definition = definition.unwrap_or_default();
                let entity_type = EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?;
                let params = Self::query_params(&conn, entity_id, entity_type);
                let positions = Self::query_ho_positions(&conn, entity_id);
                Ok(Some(ConsultedEntity {
                    name: if entity_stropped {
                        delightql_types::SqlIdentifier::stropped(entity_name)
                    } else {
                        delightql_types::SqlIdentifier::new(entity_name)
                    },
                    entity_type,
                    definition,
                    params,
                    positions,
                    namespace,
                }))
            }
            _ => {
                let namespaces: Vec<String> =
                    rows.iter().map(|(_, _, _, _, _, ns)| ns.clone()).collect();
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
        _scope: Option<&str>,
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
             JOIN join_edge er ON er.entity_id = e.id
             JOIN entity_clause ec ON ec.entity_id = e.id AND ec.ordinal = er.clause_ordinal
             WHERE er.context_name = ?1
               AND er.left_spelling = ?2 AND er.right_spelling = ?3
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

        // Ambiguity is a property of the ROW COUNT, not of how the rows
        // spread across namespaces: two rules covering the same pair in
        // ONE namespace (a&b and b&a with different bodies) are exactly
        // as ambiguous as two namespaces each holding one — and the
        // query has no ORDER BY, so a first-row pick is scan-order
        // arbitrary, silently choosing a join condition.
        if rows.len() > 1 {
            let mut sources: Vec<String> = rows
                .iter()
                .map(|(name, _, _, ns)| format!("{}::{}", ns, name))
                .collect();
            sources.sort();
            return Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous ER-rule for ({}, {}) in context '{}': {} rules cover this pair [{}].",
                    table_a,
                    table_b,
                    context,
                    sources.len(),
                    sources.join(", "),
                ),
                "Ambiguous ER-rule",
            ));
        }

        match rows.into_iter().next() {
            None => Ok(None),
            Some((entity_name, entity_type, definition, namespace)) => Ok(Some(ConsultedEntity {
                name: entity_name.into(),
                entity_type: EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?,
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
                    er.left_spelling, er.right_spelling
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id{ns_join_cond}
             {extra_joins}
             JOIN join_edge er ON er.entity_id = e.id
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
            // Unknown entity_type = corrupt catalog; skip the row (this
            // listing path has no error channel per row).
            .filter_map(
                |(entity_name, entity_type, definition, namespace, left, right)| {
                    let entity_type = EntityType::from_i32(entity_type).ok()?;
                    Some((
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
                    ))
                },
            )
            .collect())
    }

    /// Is any edge declared in this context (enlisted scope)? The edge
    /// set per context is finite and declared, so an unknown context is
    /// a hard error at first use, never an empty result.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn er_context_known(&self, context: &str) -> std::result::Result<bool, DelightQLError> {
        Ok(!self
            .query_er_rules_multi(context, ErRuleScope::Enlisted)?
            .is_empty())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn er_context_known(&self, _context: &str) -> std::result::Result<bool, DelightQLError> {
        Ok(false)
    }

    /// All contexts with at least one declared edge (enlisted scope) —
    /// the unknown-context teaching enumerates these.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_er_contexts(&self) -> std::result::Result<Vec<String>, DelightQLError> {
        let Some(system) = self.system else {
            return Ok(Vec::new());
        };
        let system_ref = unsafe { &*system };
        let bootstrap = system_ref.get_bootstrap_connection();
        let conn = bootstrap.lock().map_err(|e| {
            DelightQLError::database_error("Failed to acquire bootstrap lock", format!("{}", e))
        })?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT er.context_name
                 FROM join_edge er
                 JOIN activated_entity ae ON ae.entity_id = er.entity_id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN enlisted_namespace en ON en.from_namespace_id = n.id
                 ORDER BY er.context_name",
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to prepare context listing", e.to_string())
            })?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| DelightQLError::database_error("Failed to list contexts", e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn list_er_contexts(&self) -> std::result::Result<Vec<String>, DelightQLError> {
        Ok(Vec::new())
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
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod mode_tests {
    use super::{ConsultRegistry, DeclaredMode};
    use delightql_types::SqlIdentifier;

    fn catalog(rows: &[(&str, i64, &str, i64)]) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("an in-memory catalog");
        conn.execute(
            "CREATE TABLE functional_dependency (
                 id INTEGER PRIMARY KEY,
                 entity_id INTEGER NOT NULL,
                 role TEXT NOT NULL,
                 position INTEGER NOT NULL,
                 attribute_name TEXT NOT NULL,
                 stropped INTEGER NOT NULL DEFAULT 0)",
            [],
        )
        .expect("the declaration table");
        for (role, position, name, stropped) in rows {
            conn.execute(
                "INSERT INTO functional_dependency \
                 (entity_id, role, position, attribute_name, stropped) \
                 VALUES (1, ?1, ?2, ?3, ?4)",
                rusqlite::params![role, position, name, stropped],
            )
            .expect("a declaration row");
        }
        conn
    }

    /// A declaration is read WHOLE. A row the reader cannot decode is not
    /// evidence that the mode is narrower than it is — silently omitting one
    /// would drop a declared output and let a pick at a later position
    /// select the wrong one, or vanish.
    #[test]
    fn an_unreadable_declaration_row_refuses_rather_than_disappears() {
        let conn = catalog(&[("input", 0, "a", 0), ("output", 0, "b", 0)]);
        // The stropping bit is an integer; a row carrying text there cannot
        // be decoded, and the reader must say so.
        conn.execute(
            "INSERT INTO functional_dependency \
             (entity_id, role, position, attribute_name, stropped) \
             VALUES (1, 'output', 1, 'c', 'not an integer')",
            [],
        )
        .expect("a corrupt row");
        let read = ConsultRegistry::query_declared_mode(&conn, 1);
        assert!(
            read.is_err(),
            "a corrupt row must refuse, not narrow the declaration: {read:?}"
        );
    }

    /// A HALF DECLARATION IS CORRUPTION, NOT ABSENCE. The entity was selected
    /// BY advertising the capability, so a missing side cannot answer "no
    /// mode here" and send the call down the ordinary-call road.
    #[test]
    fn a_half_declaration_refuses_rather_than_reads_as_absent() {
        let inputs_only = catalog(&[("input", 0, "a", 0)]);
        assert!(ConsultRegistry::query_declared_mode(&inputs_only, 1).is_err());
        let outputs_only = catalog(&[("output", 0, "b", 0)]);
        assert!(ConsultRegistry::query_declared_mode(&outputs_only, 1).is_err());
        let neither = catalog(&[]);
        assert!(ConsultRegistry::query_declared_mode(&neither, 1).is_err());
    }

    /// The stored vocabularies are checked, not trusted: a stropping bit
    /// outside {0,1} and a position that is not the next one are malformed
    /// evidence, and the selected POSITION is chosen by that order.
    #[test]
    fn the_stored_vocabularies_are_validated() {
        let bad_strop = catalog(&[("input", 0, "a", 0), ("output", 0, "b", 7)]);
        assert!(ConsultRegistry::query_declared_mode(&bad_strop, 1).is_err());

        let gap = catalog(&[
            ("input", 0, "a", 0),
            ("output", 0, "b", 0),
            ("output", 2, "c", 0),
        ]);
        assert!(ConsultRegistry::query_declared_mode(&gap, 1).is_err());

        let repeated = catalog(&[
            ("input", 0, "a", 0),
            ("output", 0, "b", 0),
            ("output", 0, "c", 0),
        ]);
        assert!(ConsultRegistry::query_declared_mode(&repeated, 1).is_err());
    }

    /// And a role the vocabulary does not contain is the same kind of
    /// corruption — reported, never rounded to one of the two.
    #[test]
    fn an_unknown_declaration_role_refuses() {
        let conn = catalog(&[
            ("input", 0, "a", 0),
            ("output", 0, "b", 0),
            ("sideways", 0, "c", 0),
        ]);
        assert!(ConsultRegistry::query_declared_mode(&conn, 1).is_err());
    }

    /// The ordinary read: roles split, order preserved, stropping restored.
    #[test]
    fn a_whole_declaration_reads_back_as_it_was_written() {
        let conn = catalog(&[
            ("input", 0, "zone", 0),
            ("input", 1, "weight", 0),
            ("output", 0, "carrier", 0),
            ("output", 1, "Days In Transit", 1),
        ]);
        let mode = ConsultRegistry::query_declared_mode(&conn, 1).expect("readable");
        assert_eq!(mode.inputs.len(), 2);
        assert_eq!(mode.outputs.len(), 2);
        assert_eq!(mode.inputs[0].as_str(), "zone");
        assert_eq!(mode.outputs[1].as_str(), "Days In Transit");
        assert!(mode.outputs[1].is_stropped());
        assert!(!mode.outputs[0].is_stropped());
    }

    /// EQUAL WIDTHS ARE NOT AGREEMENT. The catalog chooses the selected
    /// position and the stored source supplies the expression at it, so a
    /// disagreement about names, order or stropping would select the wrong
    /// output while every count matched.
    #[test]
    fn agreement_is_by_name_order_and_stropping_not_width() {
        let declaration = DeclaredMode {
            inputs: vec![SqlIdentifier::new("a")],
            outputs: vec![
                SqlIdentifier::new("carrier"),
                SqlIdentifier::stropped("Days"),
            ],
        };
        let same = [
            SqlIdentifier::new("carrier"),
            SqlIdentifier::stropped("Days"),
        ];
        assert!(declaration.agrees_with(&[SqlIdentifier::new("a")], &same));

        let reordered = [
            SqlIdentifier::stropped("Days"),
            SqlIdentifier::new("carrier"),
        ];
        assert!(!declaration.agrees_with(&[SqlIdentifier::new("a")], &reordered));

        let renamed = [
            SqlIdentifier::new("courier"),
            SqlIdentifier::stropped("Days"),
        ];
        assert!(!declaration.agrees_with(&[SqlIdentifier::new("a")], &renamed));

        // Same bytes, different spelling law: `Days` unstropped folds and
        // `Days` stropped does not, so they are two names.
        let unstropped = [SqlIdentifier::new("carrier"), SqlIdentifier::new("Days")];
        assert!(!declaration.agrees_with(&[SqlIdentifier::new("a")], &unstropped));

        // And the input side is judged too.
        assert!(!declaration.agrees_with(&[SqlIdentifier::new("b")], &same));
    }
}

#[cfg(test)]
mod scoped_cfe_registration_tests {
    use super::QueryLocalRegistry;
    use crate::pipeline::asts::core::{CfeDefinition, CfeFormals, ContextMode, OutValue};
    use delightql_types::SqlIdentifier;

    fn definition(name: SqlIdentifier) -> CfeDefinition {
        CfeDefinition {
            name,
            formals: CfeFormals::from_role_groups([], [SqlIdentifier::new("x")]),
            context_mode: ContextMode::None,
            body: OutValue::Domain(crate::pipeline::asts::core::DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Null,
                ),
            )),
            source_namespace: None,
        }
    }

    /// The map's key is the identifier law's agreement: a folded spelling
    /// reaches the entry, and a later same-named definition shadows the
    /// earlier one — nearest wins, one live entry per name.
    #[test]
    fn registration_keys_by_the_identifier_law_and_nearest_wins() {
        let mut local = QueryLocalRegistry::new();

        local.register_scoped_cfe(definition(SqlIdentifier::new("f")));
        assert!(local.scoped_cfes.contains_key(&SqlIdentifier::new("F")));

        local.register_scoped_cfe(definition(SqlIdentifier::new("F")));
        assert_eq!(local.scoped_cfes.len(), 1, "one name, one live entry");
        assert_eq!(
            local.scoped_cfes[&SqlIdentifier::new("f")].name.as_str(),
            "F",
            "the nearest definition answers"
        );
    }

    /// A binding extent ends with its body: registrations made inside —
    /// CTEs and CFE definitions alike — vanish when it returns, an inner
    /// same-named definition does not replace the caller's, and both maps
    /// restore on the refusal road exactly as on the resolved one.
    #[test]
    fn a_binding_extent_ends_with_its_body() {
        let schema = crate::ddl::manifest::EmptySchema;
        let identities = std::rc::Rc::new(crate::names::Registry::new(&[]));
        let mut registry = super::EntityRegistry::new(&schema, identities);

        registry
            .query_local
            .register_scoped_cfe(definition(SqlIdentifier::new("f")));
        let caller_scope = registry.identities.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::Prefix("cte"),
            None,
        );
        registry
            .query_local
            .register_cte(SqlIdentifier::new("outer_cte"), caller_scope);

        registry.with_binding_extent(|registry| {
            // The caller's bindings are visible inside the extent…
            assert!(registry
                .query_local
                .scoped_cfes
                .contains_key(&SqlIdentifier::new("f")));
            // …and the body's own registrations land on top of them.
            let mut shadow = definition(SqlIdentifier::new("f"));
            shadow.source_namespace = Some("lib::v".to_string());
            registry.query_local.register_scoped_cfe(shadow);
            registry
                .query_local
                .register_cte(SqlIdentifier::new("inner_cte"), caller_scope);
        });

        assert_eq!(
            registry.query_local.scoped_cfes[&SqlIdentifier::new("f")].source_namespace, None,
            "the caller's definition answers again after the extent"
        );
        assert!(
            registry
                .query_local
                .lookup_cte(&SqlIdentifier::new("inner_cte"))
                .is_none(),
            "a body-internal CTE registration ends with the body"
        );
        assert!(registry
            .query_local
            .lookup_cte(&SqlIdentifier::new("outer_cte"))
            .is_some());

        // The refusal road restores exactly the same way.
        let refused: Result<(), ()> = registry.with_binding_extent(|registry| {
            registry
                .query_local
                .register_scoped_cfe(definition(SqlIdentifier::new("g")));
            Err(())
        });
        assert!(refused.is_err());
        assert!(!registry
            .query_local
            .scoped_cfes
            .contains_key(&SqlIdentifier::new("g")));
    }
}
