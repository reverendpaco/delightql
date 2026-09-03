// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The durable resolver core and its catalog readers.

use crate::error::DelightQLError;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::resolver::DatabaseSchema;
use crate::system::PRIMARY_CONNECTION_ID;
use log::debug;
use std::collections::{HashMap, HashSet};

/// THE DURABLE RESOLVER CORE: the database catalog, the built-in function
/// vocabulary, the definition catalog reader, the relation authority's
/// planning capability, and the connections a resolution touched. It holds
/// NO lexical binding map: which CTEs, CFEs, aliases, carriers, and
/// plan-created relations a name can reach is a question for the one
/// lexical world the resolver is standing in (`defuse::environment`), and
/// that world is a separate owned value.
pub struct ResolverCore<'a> {
    /// THE OPEN SEMANTIC EPOCH. Resolution constructs relations, so what it
    /// threads is the capability, not the bare naming handle a lowering
    /// context is given.
    pub identities: &'a crate::relation::Planning,
    pub database: DatabaseRegistry<'a>,
    pub built_in: BuiltInRegistry,
    /// The statement's catalog read rides here for the compilation's
    /// whole extent.
    pub consult: ConsultRegistry<'a>,
    /// Closed residual values constructed during this statement. The store
    /// is owned by the definition-use subsystem and contains no lookup by
    /// spelling; callers cross scopes with opaque identities only.
    pub(crate) residuals: std::rc::Rc<crate::defuse::ho::ResidualStore>,
    /// Connection IDs encountered during resolution.
    /// Used to route query execution and validate against cross-connection joins.
    connection_ids: HashSet<i64>,
}

impl<'a> ResolverCore<'a> {
    /// Create a new core without namespace resolution (for tests/simple cases)
    pub fn new(schema: &'a dyn DatabaseSchema, identities: &'a crate::relation::Planning) -> Self {
        Self {
            database: DatabaseRegistry::new(schema, identities),
            identities,
            built_in: BuiltInRegistry::new(),
            consult: ConsultRegistry::new(),
            residuals: std::rc::Rc::new(crate::defuse::ho::ResidualStore::default()),
            connection_ids: HashSet::new(),
        }
    }

    /// Create a new core with namespace resolution support (via system reference)
    pub fn new_with_system(
        schema: &'a dyn DatabaseSchema,
        system: &'a crate::system::DelightQLSystem,
        identities: &'a crate::relation::Planning,
    ) -> Self {
        Self {
            database: DatabaseRegistry::new_with_system(schema, system, identities),
            identities,
            built_in: BuiltInRegistry::new(),
            consult: ConsultRegistry::new_with_system(system),
            residuals: std::rc::Rc::new(crate::defuse::ho::ResidualStore::default()),
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
    identities: &'a crate::relation::Planning,
    schema: &'a dyn DatabaseSchema,
    /// Optional system reference for namespace resolution
    pub(crate) system: Option<&'a crate::system::DelightQLSystem>,
}

impl<'a> DatabaseRegistry<'a> {
    /// THE CATALOG SOURCE. One entrance: the authority receives the
    /// operation — a catalog read of this entity, publishing these
    /// dimensions in catalog order — and answers with the relation and its
    /// interface together.
    fn catalog_heading(
        &self,
        table_name: &str,
        columns: Vec<delightql_types::schema::ColumnInfo>,
        backend_schema: Option<&str>,
    ) -> crate::error::Result<crate::relation::SemanticRelation> {
        use crate::relation::form::{SourceOrigin, SourceSlot, SourceSpec};
        let table_spelling = self.identities.intern(table_name, false);
        let entity = self.identities.mint_entity(table_spelling);
        self.identities.bind_entity_physical(
            entity,
            Some(table_spelling),
            backend_schema.map(|schema| self.identities.intern(schema, false)),
        );
        let slots: Vec<SourceSlot> = columns
            .into_iter()
            .enumerate()
            .map(|(idx, col)| SourceSlot {
                position: idx as u32,
                named: Some(
                    self.identities
                        .intern(col.name.as_str(), col.name.is_stropped()),
                ),
                declared_type: col.declared_type.clone(),
            })
            .collect();
        self.identities
            .authority()
            .derive(crate::relation::RelForm::Source(SourceSpec {
                origin: SourceOrigin::Catalog { entity },
                slots: &slots,
                answers_to: Some(table_spelling),
            }))
    }

    /// Create without namespace resolution support (for tests/simple cases)
    pub fn new(schema: &'a dyn DatabaseSchema, identities: &'a crate::relation::Planning) -> Self {
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
        identities: &'a crate::relation::Planning,
    ) -> Self {
        Self {
            identities,
            schema,
            system: Some(system),
        }
    }

    /// Lookup a table in the database
    pub fn lookup_table(
        &self,
        name: &str,
    ) -> crate::error::Result<Option<crate::relation::SemanticRelation>> {
        // Parse the name to check if it has a schema qualifier
        let (schema, table_name) = if let Some(dot_pos) = name.find('.') {
            let schema_part = &name[..dot_pos];
            let table_part = &name[dot_pos + 1..];
            (Some(schema_part), table_part)
        } else {
            (None, name)
        };

        match self.schema.get_table_columns(schema, table_name)? {
            Some(columns) => Ok(Some(self.catalog_heading(table_name, columns, schema)?)),
            None => Ok(None),
        }
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
    ) -> crate::error::Result<
        Option<(
            crate::relation::SemanticRelation,
            i64,
            delightql_types::SqlIdentifier,
            Option<String>,
        )>,
    > {
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
    ) -> crate::error::Result<
        Option<(
            crate::relation::SemanticRelation,
            i64,
            delightql_types::SqlIdentifier,
            Option<String>,
        )>,
    > {
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
    ) -> crate::error::Result<
        Option<(
            crate::relation::SemanticRelation,
            i64,
            delightql_types::SqlIdentifier,
            Option<String>,
        )>,
    > {
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
            .or_else(|| backend_schema);
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
        let scope = self.catalog_heading(
            canonical_name.as_str(),
            columns,
            discovered.backend_schema.as_deref(),
        )?;

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
    ) -> crate::error::Result<
        Option<(
            crate::relation::SemanticRelation,
            i64,
            delightql_types::SqlIdentifier,
            Option<String>,
        )>,
    > {
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
                            let scope =
                                self.catalog_heading(&canonical_name, cols, Some(&alias))?;
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
                    let scope =
                        self.catalog_heading(&canonical_name, cols, backend_schema_opt.as_deref())?;
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

        match columns {
            Some(columns) => Ok(Some((
                self.catalog_heading(&canonical_name, columns, backend_schema_opt.as_deref())?,
                conn_id,
                canonical_name.clone(),
                backend_schema_opt.clone(),
            ))),
            None => Ok(None),
        }
    }

    /// Get the underlying schema for direct access when needed
    pub fn schema(&self) -> &'a dyn DatabaseSchema {
        self.schema
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
/// Holds the statement's catalog read: the definition-use authority reaches
/// consulted definitions only through it, for exactly the extent of the
/// compilation that borrowed the system.
pub struct ConsultRegistry<'s> {
    /// The statement's catalog read, absent for a registry built without a
    /// system (unit tests, the DDL manifest road, WASM).
    catalog: Option<crate::defuse::CatalogRead<'s>>,
    /// Whether ANY entity in the catalog declares a functional dependency.
    ///
    /// A call in value position asks the mode authority before the ordinary
    /// road, because a declared mode is what makes an entity callable at
    /// all. Where nothing declares one there is nothing to ask about, and
    /// this answers that once per compilation instead of once per call.
    any_mode: std::cell::Cell<Option<bool>>,
}

impl Default for ConsultRegistry<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'s> ConsultRegistry<'s> {
    pub fn new() -> Self {
        Self {
            catalog: None,
            any_mode: std::cell::Cell::new(None),
        }
    }

    /// The statement's catalog read, FOR THE DEFINITION-USE AUTHORITY
    /// ONLY: every selection road lives in `crate::defuse`, and this is
    /// the one capability those roads reach the catalog through.
    pub(crate) fn catalog(&self) -> Option<crate::defuse::CatalogRead<'s>> {
        self.catalog
    }

    pub fn new_with_system(system: &'s crate::system::DelightQLSystem) -> Self {
        Self {
            catalog: Some(crate::defuse::CatalogRead::of(system)),
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
        let Some(catalog) = self.catalog else {
            return Ok(false);
        };
        let conn =
            catalog.connection("Failed to acquire bootstrap lock for declared mode probe")?;
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
    pub(crate) fn query_params(
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

        // Default: read from entity_attribute, wrap as Scalar. The stored
        // attribute_type carries the parameter's ROLE ('code_param' for a
        // `f:()`-spelled code formal), a fact the call site partitions its
        // members by before the body is admitted.
        let mut stmt = match conn.prepare(
            "SELECT attribute_name, attribute_type FROM entity_attribute
             WHERE entity_id = ?1 AND attribute_type IN ('input_param', 'code_param')
             ORDER BY position",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(rusqlite::params![entity_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .map(|(name, attribute_type)| HoParamInfo::Scalar {
                name: delightql_types::SqlIdentifier::new(name),
                guard: None,
                callable: attribute_type == "code_param",
            })
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
                // Exact rule signatures are reconstructed from the selected
                // family's stored source by the definition-use authority.
                // This legacy metadata reader is not an admission road.
                "rule" => continue,
                // A fully-ground position remains an inbound scalar role.
                // Its synthetic name is only the internal formal key; clause
                // values are reconstructed from the family's stored source.
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

    // --- Private ER-rule query implementation ---
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
