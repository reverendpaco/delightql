// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The mount relation from attach to detach: each mounted namespace's one
//! binding, the shared attach spine and its file, bytes, external, tree and
//! create-first entrances, refresh, unmount, and emptying `main` back to its
//! fixture. A binding that borrowed an already-open schema never detaches it.

use super::created::retire_session_pool;
use super::entity_rows;
use super::population::{ensure_catalog_initialized, register_catalog_wrapper};
use super::removal::{outside_borrow, refuse_while_children_remain, refuse_while_dependents_remain, BorrowedAs};
use super::{
    ensure_namespace_available, validate_producer_target, validate_user_namespace_target, BootstrapTxn,
    CatalogSavepoint,
    DelightQLSystem, PRIMARY_CONNECTION_ID,
};
use crate::ddl::lifecycle::{admit_kind, Verb};
use crate::diagnostic::{Constraint, Runtime};
use crate::error::{DelightQLError, Result};
use crate::external_effects::CreatedFilePriorState;
use delightql_types::{ConnectionComponents, ConnectionFactory, DatabaseConnection};
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MountBinding {
    namespace_id: i64,
    cartridge_id: i64,
    pub(super) connection_id: i64,
    attach_alias: Option<String>,
    /// Who opened the schema this binding names — `Some("owned")` when this
    /// mount attached it, `Some("borrowed")` when it was already open.
    /// `None` for an external mount, which holds no attachment handle.
    attachment: Option<String>,
    qualification: String,
    engine_schema: Option<String>,
    class: String,
}

/// Who opened the physical schema a mount names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attachment {
    /// This mount attached it, and teardown may detach it.
    Owned,
    /// It was already open. This mount named it and nothing more; closing it
    /// was never this binding's to do.
    Borrowed,
}

impl Attachment {
    fn as_str(self) -> &'static str {
        match self {
            Attachment::Owned => "owned",
            Attachment::Borrowed => "borrowed",
        }
    }
}

impl MountBinding {
    fn attach(
        namespace_id: i64,
        cartridge_id: i64,
        connection_id: i64,
        alias: impl Into<String>,
        attachment: Attachment,
        qualification: impl Into<String>,
    ) -> Self {
        Self {
            namespace_id,
            cartridge_id,
            connection_id,
            attach_alias: Some(alias.into()),
            attachment: Some(attachment.as_str().to_string()),
            qualification: qualification.into(),
            engine_schema: None,
            class: "attach".to_string(),
        }
    }

    /// The schema this binding's teardown may DETACH.
    ///
    /// Not the schema it NAMES: a binding that BORROWED an already-open
    /// schema never acquired the right to close it — the schema may be
    /// SQLite's own `main`, which cannot be detached at all, or another
    /// owner's attachment that is still being read.
    ///
    /// One answer, here, because there is more than one teardown road: the
    /// ordinary destroy and `main`'s empty-back-to-fixture. A road that
    /// decided this for itself would be a second authority, and the second
    /// one is the one that gets it wrong.
    pub(super) fn detachable_alias(&self) -> Option<String> {
        match self.attachment.as_deref() {
            Some("borrowed") => None,
            _ => self.attach_alias.clone(),
        }
    }

    fn external(
        namespace_id: i64,
        cartridge_id: i64,
        connection_id: i64,
        engine_schema: Option<String>,
    ) -> Self {
        Self {
            namespace_id,
            cartridge_id,
            connection_id,
            attach_alias: None,
            attachment: None,
            qualification: if engine_schema.is_some() {
                "engine_schema".to_string()
            } else {
                "unqualified".to_string()
            },
            engine_schema,
            class: "external".to_string(),
        }
    }
}

/// Parameters for one attach-class mount, consumed by the shared spine
/// (`mount_attach_class`). The path-specific halves —
/// how to attach, and what counts as the same source — travel as closures.
struct AttachClassMount<'a> {
    /// Target namespace fq_name.
    namespace: &'a str,
    /// `cartridge.source_uri` (also the conflict-message spelling).
    source_uri: String,
    /// `namespace.source_path` value (file path or locator).
    source_path: String,
    /// `namespace.provenance` ("file" | "bytes").
    provenance: &'static str,
    /// sys::connections registration: resource / mechanism / identity.
    conn_resource: &'a str,
    conn_mechanism: &'static str,
    conn_identity: Option<String>,
    conn_description: String,
    /// The engine schema this resource is ALREADY open under on the session
    /// connection, when it is.
    ///
    /// A file may be the connection's own `main`, or already attached for
    /// another namespace. Attaching it a second time gives one connection two
    /// independent handles on one file, and SQLite refuses to let one of them
    /// write while the other is reading it — "database is locked", from a
    /// statement with no second party anywhere in sight. One resource, one
    /// schema: the namespace is the naming, and naming a file twice must not
    /// open it twice.
    existing_schema: Option<String>,
}

/// Guard for a freshly attached mount alias. Ordinary mount exits must call
/// `rollback` or `commit` explicitly; `Drop` is only an emergency backstop
/// for panic/unwinding and cannot be the required cleanup path.
struct DetachOnDrop<'a> {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
    alias: &'a str,
    armed: bool,
}

impl DetachOnDrop<'_> {
    fn commit(&mut self) {
        self.armed = false;
    }

    fn rollback(&mut self) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        let result = match self.connection.lock() {
            Ok(conn) => conn
                .execute(&format!("DETACH DATABASE '{}'", self.alias), &[])
                .map(|_| ())
                .map_err(|error| {
                    DelightQLError::from(Runtime::General {
                        message: format!("Failed to detach mounted alias '{}'", self.alias),
                        details: error.to_string(),
                    })
                }),
            Err(error) => Err(Runtime::poisoned(
                format!(
                    "Failed to acquire connection lock to detach '{}'",
                    self.alias
                ),
                format!("Connection was poisoned: {error}"),
            )),
        };
        // A failed explicit inverse is transferred to session health by the
        // caller. Disarm here so Drop never retries it; reset owns recovery.
        self.armed = false;
        result
    }
}

impl Drop for DetachOnDrop<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match self.connection.lock() {
            Ok(conn) => {
                if let Err(e) = conn.execute(&format!("DETACH DATABASE '{}'", self.alias), &[]) {
                    debug!("mount cleanup: DETACH '{}' failed: {}", self.alias, e);
                }
            }
            Err(_) => debug!(
                "mount cleanup: connection lock poisoned; alias '{}' may leak",
                self.alias
            ),
        }
    }
}

/// A host-bound database image: static rodata
/// (`include_bytes!`, referenced zero-copy at attach) or an owned
/// runtime-built buffer (copied into SQLite memory at attach). Both are
/// validated once, at bind time, in a scratch connection.
#[derive(Clone)]
pub(super) enum ByteBinding {
    Static(&'static [u8]),
    Owned(std::sync::Arc<[u8]>),
}

/// Byte-binding names are lowercase capability labels: `[a-z][a-z0-9._-]*`.
fn valid_byte_binding_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
}

/// Validate a bound image ONCE, at bind time, in a scratch connection —
/// never on the session connection. sqlite3_deserialize installs a buffer
/// without validating it (validation is lazy, on first access), and a
/// garbage image POISONS the hosting connection: every later statement,
/// including the DETACH that would remove it, fails with "file is not a
/// database". Bindings are immutable, so one bind-time proof covers every
/// future mount.
fn validate_sqlite_image(name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.len() < 100 || !bytes.starts_with(b"SQLite format 3\0") {
        return Err(DelightQLError::from(Runtime::General {
            message: format!(
                "byte binding '{}' is not a valid SQLite database image",
                name
            ),
            details: "delightql-bytes:// bindings must be complete SQLite images".to_string(),
        }));
    }
    let mut scratch = Connection::open_in_memory().map_err(|e| {
        Runtime::catalog(
            "Failed to open scratch connection for image validation",
            e.to_string(),
        )
    })?;
    scratch
        .deserialize_read_exact("main", bytes, bytes.len(), true)
        .and_then(|_| {
            scratch.query_row("SELECT count(*) FROM sqlite_master", [], |row| {
                row.get::<_, i64>(0)
            })
        })
        .map_err(|e| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "byte binding '{}' is not a valid SQLite database image: {}",
                    name, e
                ),
                details: "delightql-bytes:// bindings must be complete SQLite images".to_string(),
            })
        })?;
    Ok(())
}

/// Materialize the namespace tree required by a mounted data leaf.
///
/// A qualified mount such as `cli::surface` is two catalog facts, not one flat
/// row whose `name` happens to contain `::`: `cli` is a structural container
/// parented to `_`, and `surface` is the data namespace parented to `cli`.
/// Existing ancestors are reused without changing their ownership kind (a data
/// mount may legitimately live below a consulted library namespace).
fn create_mounted_namespace_path(
    conn: &Connection,
    namespace: &str,
    provenance: &str,
    source_path: &str,
) -> Result<(i32, Vec<String>)> {
    let mut specs =
        crate::import::namespace::parse_namespace_path(conn, namespace).map_err(|e| {
            Runtime::catalog(
                format!("Failed to construct namespace path '{}': {}", namespace, e),
                e.to_string(),
            )
        })?;

    let created_names: Vec<String> = specs.iter().map(|spec| spec.fq_name.clone()).collect();
    // Every segment this mount will CREATE must honor the
    // alias/namespace exclusivity invariant and its parent's name pool.
    for name in &created_names {
        ensure_namespace_available(conn, name)?;
        super::refuse_child_named_like_entity(conn, name)?;
    }
    for spec in &mut specs {
        if spec.fq_name == namespace {
            spec.kind = crate::namespace::NamespaceKind::Data;
            spec.provenance = Some(provenance.into());
            spec.source_path = Some(source_path.into());
        } else {
            spec.kind = crate::namespace::NamespaceKind::Container;
            spec.provenance = Some("mount".into());
            spec.source_path = None;
        }
    }

    crate::import::namespace::create_namespace_hierarchy(conn, &specs).map_err(|e| {
        Runtime::catalog(
            format!("Failed to create namespace path '{}': {}", namespace, e),
            e.to_string(),
        )
    })?;

    let leaf_id = if let Some(leaf) = specs.last() {
        leaf.id
    } else {
        conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [namespace],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to find namespace '{}': {}", namespace, e),
                e.to_string(),
            )
        })?
    };

    Ok((leaf_id, created_names))
}

fn register_mounted_catalog_wrappers(
    conn: &Connection,
    namespace: &str,
    created_names: &[String],
    sys_meta_ns_id: i32,
    catalog_id: i32,
) -> Result<()> {
    for fq_name in created_names {
        register_catalog_wrapper(conn, fq_name, sys_meta_ns_id, catalog_id)?;
    }
    // Reused empty leaves (notably `main`, and a former structural parent)
    // are absent from `created_names`; the idempotent registration covers both.
    register_catalog_wrapper(conn, namespace, sys_meta_ns_id, catalog_id)
}

impl DelightQLSystem {
    /// Read the authoritative mount binding for a namespace.  The connection
    /// is intentionally derived through the cartridge: `mount` owns the
    /// namespace↔cartridge binding, and `cartridge.connection_id` is the
    /// single connection authority.
    pub(super) fn mount_binding(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<Option<MountBinding>> {
        bootstrap_conn
            .query_row(
                "SELECT m.namespace_id, m.cartridge_id, c.connection_id,
                        m.attach_alias, m.attachment, m.qualification,
                        m.engine_schema, m.class
                 FROM mount m
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE m.namespace_id = ?1",
                [namespace_id],
                |row| {
                    Ok(MountBinding {
                        namespace_id: row.get(0)?,
                        cartridge_id: row.get(1)?,
                        connection_id: row.get(2)?,
                        attach_alias: row.get(3)?,
                        attachment: row.get(4)?,
                        qualification: row.get(5)?,
                        engine_schema: row.get(6)?,
                        class: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(|e| Runtime::catalog("Failed to read namespace mount binding", e.to_string()))
    }

    /// Insert the single binding for a mounted namespace. The UNIQUE/FK
    /// constraints in `mount` make orphaned bindings and a second namespace
    /// on one cartridge loud. A shared attach ALIAS is not among them: one
    /// file may be named by several namespaces, and the alternative — opening
    /// it once per name — is a self-deadlock. Teardown refcounts instead.
    /// Refresh explicitly clears the old row before inserting its
    /// replacement; an unexpected second writer therefore refuses rather
    /// than silently re-pointing a live namespace. Callers must invoke this
    /// inside their catalog transaction.
    fn record_mount_binding(bootstrap_conn: &Connection, binding: &MountBinding) -> Result<()> {
        bootstrap_conn
            .execute(
                "INSERT INTO mount
                 (namespace_id, cartridge_id, attach_alias, attachment,
                  qualification, engine_schema, class)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    binding.namespace_id,
                    binding.cartridge_id,
                    binding.attach_alias,
                    binding.attachment,
                    binding.qualification,
                    binding.engine_schema,
                    binding.class,
                ],
            )
            .map_err(|e| Runtime::catalog("Failed to record mount binding", e.to_string()))?;
        Ok(())
    }

    /// Remove a binding before its cartridge/namespace is deleted.  Callers
    /// must invoke this inside the same catalog transaction as the cascade.
    pub(super) fn clear_mount_binding(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<()> {
        bootstrap_conn
            .execute("DELETE FROM mount WHERE namespace_id = ?1", [namespace_id])
            .map_err(|e| Runtime::catalog("Failed to clear mount binding", e.to_string()))?;
        Ok(())
    }

    /// Register an external connection: introspect, register in bootstrap, activate in namespace.
    /// Used by import! when a ConnectionFactory is available (for delightql-siso://, file://, etc.).
    ///
    /// Returns (connection_id, entity_count) on success.
    pub fn register_external_connection(
        &mut self,
        components: ConnectionComponents,
        namespace: &str,
        connection_uri: &str,
    ) -> Result<(i64, usize)> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for external connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Idempotent mount: if namespace already exists with the SAME URI, return
        // existing connection info. If a different URI, that's an error.
        // A namespace that exists but has NO activated entities (e.g. the
        // empty "main" pre-created by open()) is NOT "already mounted" — it
        // is reused and populated below.
        let mut empty_namespace_id: Option<i32> = None;
        {
            // Mount identity via the STORED link — never entity joins.
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN mount m ON m.namespace_id = n.id
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE n.fq_name = ?1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // No mount link. The namespace may still EXIST — and if
                    // it holds activated entities it is someone else's
                    // (a lib/consult namespace deliberately has a NULL
                    // link): reusing it would flip it to 'data' and mix the
                    // new cartridge into its definitions. Mirror the attach
                    // spine's occupancy check: only
                    // a genuinely EMPTY namespace is reusable.
                    if let Ok(ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = ?1",
                        [namespace],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let occupied: bool = bootstrap_conn
                            .query_row(
                                "SELECT EXISTS(SELECT 1 FROM activated_entity WHERE namespace_id = ?1)",
                                [ns_id],
                                |row| row.get(0),
                            )
                            .map_err(|e| {
                                Runtime::catalog("Failed to check namespace occupancy", e.to_string())
                            })?;
                        if occupied {
                            return Err(DelightQLError::from(Runtime::General {
    message: format!(
                                    "Namespace '{}' already exists and is in use, cannot mount '{}' over it",
                                    namespace, connection_uri
                                ),
    details: "Namespace occupied".to_string(),
}));
                        }
                        empty_namespace_id = Some(ns_id);
                    }
                    None
                }
                Err(e) => {
                    return Err(Runtime::catalog(
                        "Failed to check namespace existence",
                        e.to_string(),
                    ));
                }
            };
            if let Some(existing_uri) = existing {
                if existing_uri == connection_uri {
                    // Same database — return existing connection info
                    let conn_id: i64 = bootstrap_conn
                        .query_row(
                            "SELECT id FROM connection WHERE resource_uri = ?1",
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
                    // Different SPELLING may still be the same RESOURCE
                    // (postgres:///db vs postgres://localhost:5433/db):
                    // compare resource-asserted identity before declaring
                    // a conflict (connect-before-dedupe —
                    // the new connection is already live, so its identity
                    // is in hand).
                    let existing_identity: Option<String> = bootstrap_conn
                        .query_row(
                            "SELECT co.identity FROM namespace n
                             JOIN mount m ON m.namespace_id = n.id
                             JOIN cartridge c ON c.id = m.cartridge_id
                             JOIN connection co ON co.id = c.connection_id
                             WHERE n.fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .ok()
                        .flatten();
                    if let (Some(new_id), Some(old_id)) =
                        (components.identity.as_deref(), existing_identity.as_deref())
                    {
                        if new_id == old_id {
                            // Same resource, different spelling — idempotent.
                            let conn_id: i64 = bootstrap_conn
                                .query_row(
                                    "SELECT co.id FROM connection co WHERE co.identity = ?1",
                                    [new_id],
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
                        }
                    }
                    return Err(DelightQLError::from(Runtime::General {
    message: format!(
                            "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                            namespace, existing_uri, connection_uri
                        ),
    details: "Duplicate namespace with different source".to_string(),
}));
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

        // Atomic registration: every bootstrap write
        // from here — connection, cartridge, entities, namespace, link,
        // wrappers — rolls back together on any failure, so a partially
        // registered external mount (e.g. a linked namespace whose
        // connection never reached connection_map) cannot exist.
        let txn = BootstrapTxn::begin(&bootstrap_conn, "external_mount")?;

        // Register the connection in bootstrap
        let connection_id = crate::import::register_connection(
            &bootstrap_conn,
            connection_uri,
            &components.mechanism,
            components.identity.as_deref(),
            connection_type,
            &format!("Mounted database: {}", namespace),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to register connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Introspect the connection to discover entities
        let entities = components.introspector.introspect_entities().map_err(|e| {
            Runtime::catalog(
                format!("Failed to introspect imported database: {}", e),
                e.to_string(),
            )
        })?;

        // Install the source cartridge. `source_ns` remains source metadata;
        // the binding written below owns qualification policy. A specific
        // schema (`#schema` / `mount_tree!`) is retained here
        // for non-mount source consumers too, but mounted reads do not infer
        // policy from this nullable field.
        let cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        connection_type,
                        crate::bootstrap::SourceType::Db.as_i32(),
                        connection_uri,
                        components.mounted_schema.as_deref(),
                        connection_id,
                    ],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to insert cartridge", e.to_string())
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
            Runtime::catalog(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            )
        })?;

        // Create the namespace — or reuse a pre-existing EMPTY one (the
        // "main" namespace open() pre-creates), recording its new source.
        let (namespace_id, created_names) = if let Some(id) = empty_namespace_id {
            bootstrap_conn
                .execute(
                    "UPDATE namespace
                     SET kind = 'data', provenance = 'uri', source_path = ?2
                     WHERE id = ?1",
                    rusqlite::params![id, connection_uri],
                )
                .map_err(|e| Runtime::catalog("Failed to update empty namespace", e.to_string()))?;
            (id, Vec::new())
        } else {
            create_mounted_namespace_path(&bootstrap_conn, namespace, "uri", connection_uri)?
        };

        // Activate all entities from the cartridge in the namespace
        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            namespace_id,
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to activate entities: {}", e), e.to_string())
        })?;

        // External/factory mounts have no ATTACH alias.  Their qualification
        // policy is the mounted engine schema (or unqualified engine default)
        // and their live connection is retained in the registry-owned maps.
        Self::record_mount_binding(
            &bootstrap_conn,
            &MountBinding::external(
                namespace_id as i64,
                cartridge_id as i64,
                connection_id,
                components.mounted_schema.clone(),
            ),
        )?;

        // Register catalog wrapper for the new namespace (lazy-init catalog if needed)
        let catalog_id = ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
        let sys_meta_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                Runtime::catalog(
                    "Failed to query sys::meta namespace for catalog wrapper",
                    e.to_string(),
                )
            })?;
        register_mounted_catalog_wrappers(
            &bootstrap_conn,
            namespace,
            &created_names,
            sys_meta_ns_id,
            catalog_id,
        )?;

        debug!(
            "register_external_connection: Registered {} entities in namespace '{}' (connection_id={})",
            entity_count, namespace, connection_id
        );

        // All bootstrap writes landed — commit before touching the
        // in-memory maps, so catalog and maps can never disagree.
        txn.commit()?;

        // Drop bootstrap lock before mutating self's maps
        drop(bootstrap_conn);

        // Store connection and schema in routing maps
        self.connection_map
            .insert(connection_id, components.connection);
        self.schema_map.insert(connection_id, components.schema);
        self.introspector_map
            .insert(connection_id, components.introspector);
        self.journal_external_connection(connection_id);

        Ok((connection_id, entity_count))
    }

    /// Install the connection factory used to mount URI-scheme databases
    /// (`delightql-siso://`, etc.). Without it, `mount_database` on a URI errors with
    /// "connection factory not available in this context". Installed by
    /// `open()` when the embedding provides a types-level factory.
    pub fn set_connection_factory(&mut self, factory: Box<dyn ConnectionFactory>) {
        self.connection_factory = Some(factory);
    }

    /// `mount_tree!()`'s system half:
    /// enumerate the target's PERSISTENT schemas and bind one sub-namespace
    /// per schema (`namespace::<schema>`), ALL on ONE connection.
    ///
    /// The factory's `create_tree` opens a single connection (one fatboy
    /// child) and hands back one `ConnectionComponents` per schema, every
    /// one carrying the SAME resource identity. `register_external_connection`
    /// deduplicates the bootstrap `connection` row by identity, so every
    /// sub-namespace lands on ONE `connection_id` (a cross-schema
    /// `run!` is a single-connection, one-bracket plan). Returns the created
    /// sub-namespaces in enumeration order (for the receipt's JSON array).
    /// SQLite/siso targets refuse inside `create_tree`.
    pub fn mount_database_tree(&mut self, uri: &str, namespace: &str) -> Result<Vec<String>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: the USER-TYPED root may not take over a reserved
        // system name (the sub-namespaces derive from it).
        validate_user_namespace_target(namespace)?;
        validate_producer_target(namespace, super::Producer::Data)?;

        // Enumerate + build per-schema components (all sharing one child).
        // The factory borrow ends here (NLL), freeing `&mut self` for the
        // registration loop below (mount_database's own pattern).
        let per_schema = {
            let factory = self.connection_factory.as_ref().ok_or_else(|| {
                DelightQLError::from(Runtime::General {
                    message: format!(
                        "Cannot mount_tree! '{}': URI schemes require a connection factory \
                         (not available in this context)",
                        uri
                    ),
                    details: "No connection factory configured".to_string(),
                })
            })?;
            factory.create_tree(uri)?
        };

        if per_schema.is_empty() {
            return Err(DelightQLError::from(Runtime::General {
                message: format!("mount_tree!() found no persistent schemas on '{}'", uri),
                details: "Empty schema tree".to_string(),
            }));
        }

        let mut created = Vec::with_capacity(per_schema.len());
        for (schema, components) in per_schema {
            let sub_ns = format!("{}::{}", namespace, schema);
            self.register_external_connection(components, &sub_ns, uri)?;
            created.push(sub_ns);
        }
        Ok(created)
    }

    /// Bind a static database image under a host-chosen name, resolvable by
    /// `mount!("delightql-bytes://<name>", ...)(*)`.
    /// Names are lowercase capability labels (`[a-z][a-z0-9._-]*`) and are
    /// immutable for the life of the handle: rebinding refuses, even to the
    /// same bytes, so a locator's referent can never change underneath a
    /// mounted namespace.
    pub fn bind_static_bytes(&mut self, name: &str, bytes: &'static [u8]) -> Result<()> {
        if !valid_byte_binding_name(name) {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    name
                ),
            }));
        }
        if self.byte_bindings.contains_key(name) {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "byte binding '{}' already exists — bindings are immutable",
                    name
                ),
                details: "Rebinding refuses so a locator's referent cannot change".to_string(),
            }));
        }
        validate_sqlite_image(name, bytes)?;
        self.byte_bindings
            .insert(name.to_string(), ByteBinding::Static(bytes));
        Ok(())
    }

    /// Owned-buffer sibling of `bind_static_bytes` (same grammar,
    /// immutability, and bind-time validation): for images built at
    /// runtime, e.g. the CLI's live surface database. The buffer is copied
    /// into SQLite-owned memory at attach.
    pub fn bind_owned_bytes(&mut self, name: &str, bytes: Vec<u8>) -> Result<()> {
        if !valid_byte_binding_name(name) {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    name
                ),
            }));
        }
        if self.byte_bindings.contains_key(name) {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "byte binding '{}' already exists — bindings are immutable",
                    name
                ),
                details: "Rebinding refuses so a locator's referent cannot change".to_string(),
            }));
        }
        validate_sqlite_image(name, &bytes)?;
        self.byte_bindings.insert(
            name.to_string(),
            ByteBinding::Owned(std::sync::Arc::from(bytes)),
        );
        Ok(())
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
    /// * `db_path` - Path to the database file or URI (e.g., "delightql-siso://snowflake")
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
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: a USER-TYPED mount target
        // may not take over or nest under a reserved system name. mount_database
        // is only reached from the user-facing mount! verb (surface + embedded
        // directive), never from system-minted machinery.
        validate_user_namespace_target(namespace)?;
        validate_producer_target(namespace, super::Producer::Data)?;

        // delightql-bytes:// resolves BEFORE the generic URI→factory routing:
        // it is attach-class — the image joins the
        // session connection's schema space so the mounted namespace is
        // joinable — never a separate factory-created backend.
        if let Some(binding_name) = db_path.strip_prefix("delightql-bytes://") {
            return self.mount_database_from_static_bytes(binding_name, namespace);
        }

        // If a ConnectionFactory is available and the path looks like a URI scheme,
        // use the factory path (supports delightql-siso://, postgres://, etc.)
        let has_uri_scheme = db_path.contains("://");
        if has_uri_scheme {
            if let Some(factory) = self.connection_factory.as_ref() {
                let components = factory.create(db_path)?;
                self.register_external_connection(components, namespace, db_path)?;
                return Ok(());
            } else {
                return Err(DelightQLError::from(Runtime::General {
    message: format!(
                        "Cannot mount '{}': URI schemes require a connection factory (not available in this context)",
                        db_path
                    ),
    details: "No connection factory configured".to_string(),
}));
            }
        }

        // Plain file path: use the existing ATTACH DATABASE path (SQLite-to-SQLite optimization)

        // A relative path resolves against the base directory in force; there is
        // no fallback to the process directory.
        let resolved_path = self.resolve_path(db_path)?;
        let db_path = resolved_path.display().to_string();
        let db_path = db_path.as_str();

        // Guard: file must exist and be a valid SQLite database
        let path = std::path::Path::new(db_path);
        if !path.exists() {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "mount!() failed: file '{}' does not exist. \
                     Use create!() to make a new database.",
                    db_path
                ),
                details: "File not found".to_string(),
            }));
        }
        {
            use std::io::Read;
            let mut file = std::fs::File::open(path).map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: format!("mount!() failed: cannot open '{}': {}", db_path, e),
                    details: "File open failed".to_string(),
                })
            })?;
            let mut header = [0u8; 16];
            let bytes_read = file.read(&mut header).map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: format!("mount!() failed: cannot read '{}': {}", db_path, e),
                    details: "File read failed".to_string(),
                })
            })?;
            // DuckDB file (magic "DUCK" at offset 8): route through the
            // connection factory like any external resource — the factory
            // classifies the path and picks the duckdb adapter
            // (resource-first surface).
            if bytes_read >= 12 && &header[8..12] == b"DUCK" {
                if let Some(factory) = self.connection_factory.as_ref() {
                    let components = factory.create(db_path)?;
                    self.register_external_connection(components, namespace, db_path)?;
                    return Ok(());
                }
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "mount!() failed: '{}' is a DuckDB database but no \
                         connection factory is available",
                        db_path
                    ),
                    details: "No connection factory".to_string(),
                }));
            }
            // mount! is attach-only. An empty (0-byte, e.g. /dev/null) or
            // short file is not a
            // valid SQLite database and is rejected here — create intent
            // belongs to mount_new!, which materializes a valid header first.
            // Pinned by new_test_suite/balls/ddl_bugs/bug_nullmount--02 and
            // crates/delightql-cli/tests/mount_validation.rs.
            if bytes_read < 16 || &header != b"SQLite format 3\0" {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "mount!() failed: '{}' is not a valid SQLite database",
                        db_path
                    ),
                    details: "Invalid database file".to_string(),
                }));
            }
        }

        // ── Everything past validation is the shared attach-class spine:
        // authoritative idempotency, atomic
        // registration, one unforgettable tail. Only the file-mount
        // specifics live here, as closures.
        let attach_identity = std::fs::canonicalize(db_path)
            .ok()
            .map(|abs| format!("realpath:{}", abs.display()));
        let same_path = db_path.to_string();
        let same_source = move |existing: &str| -> bool {
            // Different SPELLING may still be the same FILE (the symlink
            // trap): compare filesystem identity.
            if existing == same_path {
                return true;
            }
            match (
                std::fs::canonicalize(existing),
                std::fs::canonicalize(&same_path),
            ) {
                (Ok(a), Ok(b)) => a == b,
                _ => false,
            }
        };
        let existing_schema = self.open_schema_for_file(db_path);
        let attach_path = db_path.to_string();
        let attach = move |conn: &dyn DatabaseConnection, alias: &str| -> Result<()> {
            conn.execute(
                &format!("ATTACH DATABASE '{}' AS '{}'", attach_path, alias),
                &[],
            )
            .map(|_| ())
            .map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: format!("Failed to attach database: {}", e),
                    details: e.to_string(),
                })
            })
        };
        self.mount_attach_class(
            AttachClassMount {
                namespace,
                source_uri: format!("file://{}", db_path),
                source_path: db_path.to_string(),
                provenance: "file",
                conn_resource: db_path,
                conn_mechanism: "attach",
                conn_identity: attach_identity,
                conn_description: format!("Mounted database: {}", namespace),
                existing_schema,
            },
            &same_source,
            &attach,
        )
    }

    /// The engine schema the session connection ALREADY holds this file
    /// under, if any.
    ///
    /// The connection answers, not the catalog: `PRAGMA database_list` is
    /// where the session's own `main` shows up, and the session's main is the
    /// case the catalog cannot see — nothing mounted it, so no namespace
    /// records it. Filesystem identity, not spelling, because `main.sqlite`
    /// and `./main.sqlite` and a symlink to either are one file.
    ///
    /// `None` for anything unresolvable: a connection that cannot answer, a
    /// path that cannot be canonicalized, an in-memory or temp schema (empty
    /// file). Not knowing means attaching, which is what happened before.
    fn open_schema_for_file(&self, db_path: &str) -> Option<String> {
        let want = std::fs::canonicalize(db_path).ok()?;
        let guard = self.connection.lock().ok()?;
        let (_cols, rows) = guard.query_all_rows("PRAGMA database_list", &[]).ok()?;
        rows.iter().find_map(|row| {
            let alias = row.get(1)?.as_wire_text()?;
            let file = row.get(2)?.as_wire_text()?;
            if file.is_empty() {
                return None;
            }
            (std::fs::canonicalize(file).ok()? == want).then_some(alias)
        })
    }

    /// The shared attach-class mount spine. Every
    /// attach-class mount path (file, `delightql-bytes://`; `mount_new!` by
    /// delegation) runs through here, so the recipe is correct exactly once:
    ///
    /// - IDENTITY IS AUTHORITATIVE: idempotency consults
    ///   `namespace.source_path` — never `activated_entity` joins — so a
    ///   valid-but-empty database has an identity too.
    /// - FAILURE IS ATOMIC: all bootstrap writes run inside one transaction,
    ///   and any failure after attachment rolls the metadata back AND
    ///   detaches the alias — a failed mount leaves nothing behind.
    /// - THE TAIL IS UNFORGETTABLE: connection → cartridge → entities →
    ///   namespace → activation → catalog wrappers → schema refresh happen
    ///   here, so a new mount path cannot partially transcribe the recipe
    ///   (the `m::(*)` bug class).
    fn mount_attach_class(
        &mut self,
        m: AttachClassMount<'_>,
        same_source: &dyn Fn(&str) -> bool,
        attach: &dyn Fn(&dyn DatabaseConnection, &str) -> Result<()>,
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for mount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // ── Authoritative idempotency (namespace.source_path) ──
        let existing: Option<(i32, Option<String>)> = match bootstrap_conn.query_row(
            "SELECT id, source_path FROM namespace WHERE fq_name = ?1",
            [m.namespace],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ) {
            Ok(pair) => Some(pair),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => {
                return Err(Runtime::catalog(
                    "Failed to check namespace existence",
                    e.to_string(),
                ));
            }
        };
        let existing_namespace_id: Option<i32> = match existing {
            None => None,
            Some((_, Some(ref existing_source))) if !existing_source.is_empty() => {
                if same_source(existing_source) {
                    // Same resource (any spelling) — idempotent, skip.
                    drop(bootstrap_conn);
                    return Ok(());
                }
                return Err(DelightQLError::from(Runtime::General {
    message: format!(
                        "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                        m.namespace, existing_source, m.source_uri
                    ),
    details: "Duplicate namespace with different source".to_string(),
}));
            }
            Some((ns_id, _)) => {
                // Namespace exists with no recorded source (e.g. a
                // pre-created empty "main"). Reusable only while nothing is
                // activated in it — an occupied namespace is someone else's.
                let occupied: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM activated_entity WHERE namespace_id = ?1)",
                        [ns_id],
                        |row| row.get(0),
                    )
                    .map_err(|e| {
                        Runtime::catalog("Failed to check namespace occupancy", e.to_string())
                    })?;
                if occupied {
                    return Err(DelightQLError::from(Runtime::General {
    message: format!(
                            "Namespace '{}' already exists and is in use, cannot mount '{}' over it",
                            m.namespace, m.source_uri
                        ),
    details: "Namespace occupied".to_string(),
}));
                }
                Some(ns_id)
            }
        };

        // Auto-generate unique SQLite schema alias
        let next_id: i32 = bootstrap_conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM cartridge",
                [],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("Failed to query next cartridge ID", e.to_string()))?;
        let schema_alias = match &m.existing_schema {
            Some(alias) => alias.clone(),
            None => format!("_imported_{}", next_id),
        };
        debug!(
            "mount_attach_class: alias {} for {} -> {}",
            schema_alias, m.source_uri, m.namespace
        );

        // The identity check and alias allocation are complete. Release the
        // catalog lock before touching the target and before any explicit
        // inverse may need to update session health.
        drop(bootstrap_conn);

        // ── Attach, unless the connection already holds this resource. No
        // bootstrap writes have happened yet, so an attach failure needs no
        // rollback. ──
        if m.existing_schema.is_none() {
            {
                let user_conn = self.connection.lock().map_err(|e| {
                    Runtime::poisoned(
                        "Failed to acquire user connection lock",
                        format!("Connection was poisoned: {}", e),
                    )
                })?;
                attach(&*user_conn, &schema_alias)?;
            }
            // A liminal program abort must detach what it attached; the
            // journal rollback is best-effort, so a mount failure that
            // already detached the alias makes the abort's DETACH a harmless
            // no-op.
            self.journal_attached_sqlite(schema_alias.clone());
        }

        // ── Registration, atomically: one bootstrap transaction, with
        // the alias guard armed from here — BEGIN failure, registration
        // failure, and COMMIT failure all detach on every exit path.
        //
        // Armed only for a schema THIS mount attached. Detaching one it found
        // already open would close a database somebody else is standing on —
        // and for the session's own `main` there is nothing to detach at all.
        let mut alias_guard = DetachOnDrop {
            connection: Arc::clone(&self.connection),
            alias: &schema_alias,
            armed: m.existing_schema.is_none(),
        };
        let registration = (|| -> Result<()> {
            let bootstrap_conn = self.bootstrap_connection.lock().map_err(|error| {
                Runtime::poisoned(
                    "Failed to acquire bootstrap database lock for mount registration",
                    format!("Connection was poisoned: {error}"),
                )
            })?;
            // A nestable savepoint, not BEGIN: mount! is liminal-eligible, so
            // this registration may run INSIDE the liminal-program savepoint
            // (where a raw BEGIN is "cannot start a transaction within a
            // transaction" — the semantic merge conflict between the mount
            // reification and the program spine).
            if let Err(error) = bootstrap_conn.execute_batch("SAVEPOINT dql_mount_attach") {
                return Err(Runtime::catalog(
                    "Failed to begin mount transaction",
                    error.to_string(),
                ));
            }
            let transaction = CatalogSavepoint {
                conn: &bootstrap_conn,
                name: "dql_mount_attach",
                active: true,
            };
            let registered = (|| -> Result<()> {
                crate::import::register_connection(
                    &bootstrap_conn,
                    m.conn_resource,
                    m.conn_mechanism,
                    m.conn_identity.as_deref(),
                    1, // sqlite-format data
                    &m.conn_description,
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Failed to register connection: {}", e),
                        e.to_string(),
                    )
                })?;

                let entities = self
                    .introspector
                    .introspect_entities_in_schema(&schema_alias)
                    .map_err(|e| {
                        Runtime::catalog(
                            format!(
                                "Failed to introspect attached database schema '{}': {}",
                                schema_alias, e
                            ),
                            e.to_string(),
                        )
                    })?;

                // source_ns records the physical source namespace uniformly,
                // main included. Whether generated SQL spells it is a separate
                // fact, carried by mount.qualification — encoding that decision
                // as NULL-ness here would conflate "which namespace" with
                // "whether to write it".
                let effective_source_ns = Some(schema_alias.as_str());
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
                                &m.source_uri,
                                effective_source_ns,
                                2, // user connection (the schema is attached there)
                            ],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to insert cartridge", e.to_string())
                        })?;
                    bootstrap_conn.last_insert_rowid() as i32
                };

                crate::bootstrap::introspect::insert_discovered_entities(
                    &bootstrap_conn,
                    cartridge_id,
                    &entities,
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Failed to insert discovered entities: {}", e),
                        e.to_string(),
                    )
                })?;

                let (namespace_id, created_names) = if let Some(ns_id) = existing_namespace_id {
                    bootstrap_conn
                        .execute(
                            "UPDATE namespace
                         SET kind = 'data', provenance = ?2, source_path = ?3
                         WHERE id = ?1",
                            rusqlite::params![ns_id, m.provenance, &m.source_path],
                        )
                        .map_err(|e| {
                            Runtime::catalog(
                                "Failed to record mount source on reused namespace",
                                e.to_string(),
                            )
                        })?;
                    (ns_id, Vec::new())
                } else {
                    create_mounted_namespace_path(
                        &bootstrap_conn,
                        m.namespace,
                        m.provenance,
                        &m.source_path,
                    )?
                };

                let activated_count = crate::import::activate_entities_from_cartridge(
                    &bootstrap_conn,
                    cartridge_id,
                    namespace_id,
                )
                .map_err(|e| {
                    Runtime::catalog(format!("Failed to activate entities: {}", e), e.to_string())
                })?;
                debug!(
                    "mount_attach_class: activated {} entities in '{}'",
                    activated_count, m.namespace
                );

                // Authoritative mount relation: `main` keeps unqualified
                // resolution policy while the relation retains the physical
                // attachment alias separately.
                Self::record_mount_binding(
                    &bootstrap_conn,
                    &MountBinding::attach(
                        namespace_id as i64,
                        cartridge_id as i64,
                        2,
                        &schema_alias,
                        // Whoever attached it may detach it, and this mount
                        // did so only when it found nothing already open.
                        match m.existing_schema {
                            Some(_) => Attachment::Borrowed,
                            None => Attachment::Owned,
                        },
                        if m.namespace == "main" {
                            "unqualified"
                        } else {
                            "aliased"
                        },
                    ),
                )?;

                // Catalog wrapper: what makes `ns::(*)` resolve for the mount.
                let catalog_id =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
                let sys_meta_ns_id: i32 = bootstrap_conn
                    .query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| {
                        Runtime::catalog(
                            "Failed to query sys::meta namespace for catalog wrapper",
                            e.to_string(),
                        )
                    })?;
                register_mounted_catalog_wrappers(
                    &bootstrap_conn,
                    m.namespace,
                    &created_names,
                    sys_meta_ns_id,
                    catalog_id,
                )?;
                Ok(())
            })();
            if let Err(e) = registered {
                drop(transaction); // rolls back the savepoint
                return Err(e);
            }
            // A failed RELEASE rolls back via the savepoint's Drop. The
            // explicit alias inverse runs after this closure releases the
            // bootstrap lock, so cleanup failures can update session health.
            transaction.commit("Failed to commit mount transaction")?;
            drop(bootstrap_conn);
            Ok(())
        })();
        if let Err(error) = registration {
            // The alias is already attached and journaled. Make the inverse
            // explicit even when registration failed before a catalog row was
            // written; a failed inverse is retained by session health instead
            // of disappearing into Drop.
            let cleanup = alias_guard.rollback();
            return Err(self.mount_error_after_alias_rollback(&schema_alias, cleanup, error));
        }
        alias_guard.commit();

        // Drop the bootstrap lock so sequential execution sees the mount,
        // then point the schema provider at the refreshed metadata.
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        ));

        Ok(())
    }

    /// Mount a host-bound static database image (`delightql-bytes://<name>`).
    /// Attach-class via the shared mount spine: the
    /// image is deserialized into a fresh in-memory schema ATTACHed to the
    /// session connection (joinable with `main`), read-only by rule. All
    /// bytes-specific refusals (bad name, unbound name, non-SQLite primary
    /// via the trait default) fire before any attachment.
    fn mount_database_from_static_bytes(
        &mut self,
        binding_name: &str,
        namespace: &str,
    ) -> Result<()> {
        let locator = format!("delightql-bytes://{}", binding_name);

        if !valid_byte_binding_name(binding_name) {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "mount!() failed: invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    binding_name
                ),
            }));
        }
        let Some(binding) = self.byte_bindings.get(binding_name).cloned() else {
            // Bound names are an intentionally enumerable, non-secret host
            // surface — the miss teaches, like dql man.
            let mut known: Vec<&str> = self.byte_bindings.keys().map(|s| s.as_str()).collect();
            known.sort_unstable();
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "mount!() failed: no byte binding named '{}' (bound: {})",
                    binding_name,
                    if known.is_empty() {
                        "none".to_string()
                    } else {
                        known.join(", ")
                    }
                ),
                details: "delightql-bytes:// resolves only names the host has bound".to_string(),
            }));
        };

        let expected = locator.clone();
        let same_source = move |existing: &str| existing == expected;
        let attach = move |conn: &dyn DatabaseConnection, alias: &str| -> Result<()> {
            match &binding {
                // Static rodata: referenced in place, zero-copy.
                ByteBinding::Static(b) => conn.attach_static_bytes(alias, b),
                // Runtime-built buffer: copied into SQLite-owned memory.
                ByteBinding::Owned(a) => conn.attach_bytes_copied(alias, a),
            }
        };
        self.mount_attach_class(
            AttachClassMount {
                namespace,
                source_uri: locator.clone(),
                source_path: locator.clone(),
                provenance: "bytes",
                conn_resource: &locator,
                conn_mechanism: "deserialize",
                conn_identity: Some(format!("host-binding:{}", binding_name)),
                conn_description: format!("Mounted embedded database: {}", namespace),
                // A bytes image is deserialized into a FRESH in-memory schema
                // every time; there is no file for the connection to already
                // hold, and two locators are two images.
                existing_schema: None,
            },
            &same_source,
            &attach,
        )
    }

    /// Provision a fresh, valid, empty SQLite database at `db_path` and bind it
    /// as namespace `namespace`. The create-intent counterpart of `mount_database`: where
    /// `mount!` ATTACHES an existing database and rejects a missing/empty/
    /// invalid path, `mount_new!` MATERIALIZES the database first, then binds
    /// it exactly as `mount!` would.
    ///
    /// CLOBBER POLICY (the `table!`/`table_replace!` refuse-over-clobber
    /// posture): refuse when the path already holds content — a real database
    /// OR any other non-empty bytes; only a MISSING or 0-byte path is
    /// materialized. On refusal the existing file is left untouched.
    ///
    /// v1 SCOPE: SQLite files only. A URI scheme (`postgres://`, …) or
    /// a DuckDB target refuses cleanly — extending to other engines is a
    /// future increment.
    ///
    /// MATERIALIZE: `rusqlite::Connection::open(path)` + `PRAGMA user_version =
    /// 0` forces the SQLite header page out (a valid 4096-byte empty db;
    /// a 0-byte file or a bare
    /// read-only open does NOT). Then delegate to `mount_database`, so the
    /// resulting mount — reserved-name refusal, namespace registration,
    /// catalog wrapper — is identical to `mount!` of a valid empty database.
    ///
    /// Pinned by `mount_new_database_tests` (below) and the CLI
    /// `mount_new_roundtrip` integration test (mount_new! then mount!).
    pub fn mount_new_database(&mut self, db_path: &str, namespace: &str) -> Result<()> {
        // Reserved-name refusal is inherited from mount_database; run it up
        // front so we never materialize a file for a target we will refuse
        // anyway. (mount_database re-runs it harmlessly on delegation.)
        validate_user_namespace_target(namespace)?;
        validate_producer_target(namespace, super::Producer::Data)?;

        // v1 SCOPE: SQLite files only. A URI scheme refuses cleanly — the same
        // `://` classification mount_database itself uses to route URIs.
        if db_path.contains("://") {
            let engine = db_path.split("://").next().unwrap_or("that engine");
            return Err(Runtime::catalog(
                format!(
                    "mount_new!() creates a new SQLite database; to create on {}, \
                     use its native tooling then mount!()",
                    engine
                ),
                "Unsupported create target",
            ));
        }

        // Resolve exactly as mount_database will, so the
        // file we materialize is the file it attaches.
        let resolved = self.resolve_path(db_path)?;
        let path = resolved.as_path();

        // CLOBBER POLICY: refuse a path already holding content. Only a MISSING
        // or 0-byte path is ours to create. (A non-empty DuckDB file lands here
        // too — refused as a clobber; attach it with mount!.)
        let prior_state = if path.exists() {
            let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if len > 0 {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "mount_new!() failed: '{}' already exists; use mount!() to attach it",
                        resolved.display()
                    ),
                    details: "Refuse to clobber".to_string(),
                }));
            }
            CreatedFilePriorState::Empty
        } else {
            CreatedFilePriorState::Absent
        };

        // MATERIALIZE a valid empty SQLite database (a header-bearing 4096-byte
        // file). `PRAGMA user_version = 0` forces the header page out — a bare
        // open would leave a 0-byte file that mount!'s attach-only guard
        // rejects.
        {
            let conn = rusqlite::Connection::open(path).map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: format!(
                        "mount_new!() failed: cannot create database at '{}': {}",
                        resolved.display(),
                        e
                    ),
                    details: e.to_string(),
                })
            })?;
            conn.execute_batch("PRAGMA user_version = 0;")
                .map_err(|e| {
                    DelightQLError::from(Runtime::General {
                        message: format!(
                            "mount_new!() failed: cannot initialize database at '{}': {}",
                            resolved.display(),
                            e
                        ),
                        details: e.to_string(),
                    })
                })?;
        }

        // The file exists independently of the bootstrap catalog. If an
        // enclosing consultation later aborts, catalog rollback/unmount is not
        // enough: restore the exact pre-program filesystem state as well.
        self.journal_created_file(path.to_path_buf(), prior_state);

        // Delegate to the ordinary mount path: the resulting mount is identical
        // to mount!() of a valid empty database.
        self.mount_database(db_path, namespace)
    }

    /// Empty `main` back to its pre-created bootstrap state (the unmount
    /// counterpart of open()'s pre-creation): contents and the mount
    /// cartridge go, the ROW and its wiring (home enlistment, routing)
    /// stay, and the mount facts are cleared so the next mount reuses it.
    /// Returns the physical cleanup identity like destroy_namespace.
    fn empty_main_namespace(bootstrap_conn: &Connection) -> Result<(Option<i64>, Option<String>)> {
        let ns_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("main namespace missing", e.to_string()))?;

        // Physical cleanup identity, read from the authoritative relation
        // BEFORE clearing it.
        let link_identity = Self::mount_binding(&bootstrap_conn, ns_id)?.map(|binding| {
            (
                binding.cartridge_id,
                Some(binding.connection_id),
                binding.detachable_alias(),
            )
        });

        let txn = BootstrapTxn::begin(&bootstrap_conn, "empty_main")?;
        // FK choreography: un-point before the cartridge dies; restore the
        // row's pre-created face.
        Self::clear_mount_binding(&bootstrap_conn, ns_id)?;
        bootstrap_conn
            .execute(
                "UPDATE namespace
                 SET source_path = NULL, provenance = 'bootstrap'
                 WHERE id = ?1",
                [ns_id],
            )
            .map_err(|e| Runtime::catalog("Failed to reset main namespace", e.to_string()))?;
        Self::clear_namespace_contents(&bootstrap_conn, ns_id)?;
        if let Some((cart_id, _, _)) = link_identity {
            entity_rows::retire_load(&bootstrap_conn, cart_id)?;
        }
        txn.commit()?;

        Ok(link_identity
            .map(|(_, conn_id, alias)| (conn_id, alias))
            .unwrap_or((None, None)))
    }

    /// Unmount a data namespace, releasing its database connection.
    ///
    /// Validates the namespace is of kind 'data', is not borrowed by any
    /// grounded namespace, and has no namespace beneath it. If clear, deletes
    /// its bootstrap metadata and performs physical cleanup (DETACH or
    /// connection_map removal).
    pub fn unmount_database(&mut self, namespace: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // A consulted file may undo a mount it created itself, but may not
        // rearrange a mount owned by the caller's pre-program session. The
        // savepoint makes the catalog mutation reversible; this remains a
        // language/session policy rather than a rollback limitation.
        self.refuse_preexisting_namespace_mutation_in_program(
            namespace,
            "unmounting",
            |message| crate::diagnostic::Directive::UnmountUncompensable { message }.into(),
        )?;
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for unmount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. The namespace and every namespace under it: the scope the borrow
        //    and child judgments read.
        let tree = crate::namespace::Subtree::of(&bootstrap_conn, namespace)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!("Namespace '{}' not found", namespace),
                details: "Namespace not found".to_string(),
            })
        })?;
        admit_kind(Verb::Unmount, namespace, tree.root().kind())?;

        // 2. Borrow check: a grounding rooted outside the subtree that
        //    borrows a namespace in it refuses it (the borrower named is the derived
        //    world's ROOT).
        if let Some(borrow) = outside_borrow(&bootstrap_conn, &tree, BorrowedAs::Data)? {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "Cannot unmount '{}' — {} is borrowed by grounded namespace '{}'. \
                     Unconsult the grounded namespace first.",
                    namespace, borrow.member, borrow.borrower
                ),
                details: "Namespace borrowed".to_string(),
            }));
        }
        // 3. One namespace: refuse while anything stands beneath it.
        refuse_while_children_remain("unmount", namespace, &tree)?;
        refuse_while_dependents_remain(&bootstrap_conn, "unmount", namespace)?;

        // 4–6 run under ONE transaction window: the
        // catalog tree AND the physical DETACH commit or fail
        // together. A failed DETACH rolls the catalog back, so the mount
        // identity is retained and the operation reports failure — never
        // "unmounted" in the catalog with the database still attached.
        // (In-memory map removals are collected and applied only after
        // COMMIT, so memory follows the catalog.)
        let unmount_txn = BootstrapTxn::begin(&bootstrap_conn, "unmount_window")?;

        let mut schemas_to_detach: Vec<String> = Vec::new();
        let mut connections_to_remove: Vec<i64> = Vec::new();

        // `main` is a bootstrap FIXTURE:
        // unmount EMPTIES it back to its pre-created state instead of
        // destroying the row — destroying loses the wiring open() gave it
        // (home enlistment, unqualified-read routing), which a later
        // remount cannot recreate. The
        // next mount then takes the ordinary reuse-empty branch.
        let (connection_id, source_ns) = if namespace == "main" {
            Self::empty_main_namespace(&bootstrap_conn)?
        } else {
            Self::destroy_namespace(&bootstrap_conn, tree.root())?
        };
        if let Some(conn_id) = connection_id {
            if conn_id > 2 {
                connections_to_remove.push(conn_id);
            }
        }
        if let Some(schema) = source_ns {
            schemas_to_detach.push(schema);
        }

        // Refcount, and the handover that makes it complete. One file may be
        // bound by more than one namespace — mounting it a second time reuses
        // the schema rather than opening the file twice — so unmounting one
        // binding must not pull the database out from under the others.
        //
        // Every alias reaching here was OWNED by the binding this tree
        // destroyed; a borrowed one contributed none. If a binding survives
        // naming it, that binding inherits the ownership the destroyed one
        // held and the schema stays attached: someone must still be able to
        // close it, and without the handover the last one out would find
        // only borrowed rows and leak the attachment for the session.
        let mut retained = Vec::with_capacity(schemas_to_detach.len());
        for schema in schemas_to_detach {
            let heir: Option<i64> = bootstrap_conn
                .query_row(
                    "SELECT namespace_id FROM mount WHERE attach_alias = ?1 LIMIT 1",
                    [&schema],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    Runtime::catalog("Failed to check surviving mount bindings", e.to_string())
                })?;
            match heir {
                Some(namespace_id) => {
                    bootstrap_conn
                        .execute(
                            "UPDATE mount SET attachment = 'owned' WHERE namespace_id = ?1",
                            [namespace_id],
                        )
                        .map_err(|e| {
                            Runtime::catalog(
                                "Failed to hand the attachment to a surviving binding",
                                e.to_string(),
                            )
                        })?;
                }
                None => retained.push(schema),
            }
        }
        let schemas_to_detach = retained;

        // Physical DETACH, inside the window: any failure returns Err and
        // the guard rolls the catalog tree back. (`unmount_txn` is
        // on the BOOTSTRAP connection; the DETACH runs on the USER
        // connection, so the two cannot deadlock.)
        if !schemas_to_detach.is_empty() {
            let user_conn = self.connection.lock().map_err(|e| {
                Runtime::poisoned(
                    "Failed to acquire user connection lock for unmount detach",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            for schema in &schemas_to_detach {
                if let Err(e) = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]) {
                    return Err(Runtime::catalog(
                        format!(
                            "unmount!() failed: could not DETACH '{}' — the mount is \
                             retained (catalog rolled back): {}",
                            schema, e
                        ),
                        e.to_string(),
                    )); // unmount_txn rolls back on unwind
                }
            }
        }

        // A mount_tree! creates several binding rows over one connection.
        // Do not retire a live routing resource while a sibling binding still
        // references it; the relation is now the refcount authority.
        connections_to_remove.sort_unstable();
        connections_to_remove.dedup();
        let connections_to_remove: Vec<i64> = connections_to_remove
            .into_iter()
            .filter(|connection_id| {
                !bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM mount m
                             JOIN cartridge c ON c.id = m.cartridge_id
                             WHERE c.connection_id = ?1
                         )",
                        [connection_id],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap_or(true)
            })
            .collect();
        for connection_id in &connections_to_remove {
            retire_session_pool(&bootstrap_conn, *connection_id)?;
        }

        unmount_txn.commit()?;
        drop(bootstrap_conn);

        // Memory follows the committed catalog.
        for conn_id in connections_to_remove {
            self.connection_map.remove(&conn_id);
            self.schema_map.remove(&conn_id);
            self.introspector_map.remove(&conn_id);
        }

        debug!("unmount_database: Unmounted namespace '{}'", namespace);
        Ok(())
    }

    /// Refresh a data namespace by re-introspecting its source database.
    ///
    /// Clears all entity metadata and re-discovers entities from the same
    /// database source. Preserves namespace identity, enlistments, aliases,
    /// and groundings. Validates grounding contracts after refresh.
    pub fn refresh_namespace(&mut self, namespace: &str) -> Result<usize> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for refresh",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and is 'data' kind
        let (ns_id, kind): (i64, Option<String>) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::from(Runtime::General {
                    message: format!("Namespace '{}' not found", namespace),
                    details: "Namespace not found".to_string(),
                })
            })?;
        admit_kind(
            Verb::Refresh,
            namespace,
            crate::namespace::NamespaceKind::decode(namespace, kind.as_deref())?,
        )?;

        // 2. Retrieve cartridge metadata for re-introspection
        // The mount's cartridge is the STORED link — authoritative even for a
        // valid-but-EMPTY image (a file image re-introspects to zero
        // entities; a bytes image reaches the immutable refusal below). The
        // old entity-join lookup and its source-match fallback are REPEALED.
        let cartridge_meta: Option<(
            i64,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        )> = bootstrap_conn
            .query_row(
                "SELECT m.cartridge_id, c.connection_id, c.source_ns, c.source_uri,
                            m.attach_alias, m.class, m.engine_schema, m.attachment
                     FROM mount m
                     JOIN cartridge c ON c.id = m.cartridge_id
                     WHERE m.namespace_id = ?1",
                [ns_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                Runtime::catalog("Failed to read mount binding for refresh", e.to_string())
            })?;

        let (
            old_cartridge_id,
            connection_id,
            source_ns,
            source_uri,
            attach_alias,
            mount_class,
            engine_schema,
            mount_attachment,
        ) = match &cartridge_meta {
            Some((cart_id, conn_id, src_ns, src_uri, alias, class, engine_schema, attachment)) => (
                *cart_id,
                conn_id.unwrap_or(2),
                src_ns.clone(),
                src_uri.clone().unwrap_or_default(),
                alias.clone(),
                class.clone(),
                engine_schema.clone(),
                match attachment.as_deref() {
                    Some("borrowed") => Attachment::Borrowed,
                    _ => Attachment::Owned,
                },
            ),
            None => {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "Namespace '{}' has no cartridge — cannot refresh",
                        namespace
                    ),
                    details: "No cartridge found".to_string(),
                }));
            }
        };

        // delightql-bytes:// mounts refuse refresh: an immutable embedded
        // image cannot have changed, so a refresh has
        // nothing to observe. unmount!/mount! if a re-mount is really wanted.
        if source_uri.starts_with("delightql-bytes://") {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "Cannot refresh '{}': it is mounted from an immutable embedded image ({}). \
                     Use unmount!() and mount!() to re-mount.",
                    namespace, source_uri
                ),
                details: "Embedded images are immutable".to_string(),
            }));
        }

        // 3. Begin a nestable transaction: refresh! is liminal-eligible and
        // may therefore run under the program-level savepoint.
        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_refresh_namespace",
            "Failed to begin refresh transaction",
        )?;

        // 4. Clear contents — FK choreography first: un-point the link BEFORE
        // the old cartridge row is deleted, inside this transaction, so a
        // rollback restores link and cartridge TOGETHER. Then clear the
        // entity-discovered cartridges plus the authoritative one from the
        // link (an ENTITYLESS cartridge is invisible to the entity scan;
        // the explicit clear is a no-op when the scan
        // already removed it).
        let clear_result = Self::clear_mount_binding(&bootstrap_conn, ns_id)
            .and_then(|_| Self::clear_namespace_contents(&bootstrap_conn, ns_id))
            .and_then(|_| entity_rows::retire_load(&bootstrap_conn, old_cartridge_id));
        if let Err(e) = clear_result {
            return Err(e);
        }

        // 5. Re-introspect
        let entities =
            if connection_id == PRIMARY_CONNECTION_ID {
                // ATTACH path: the PHYSICAL alias comes from the mount binding.
                // Substituting the namespace NAME would introspect SQLite's hub
                // instead of the attached database.
                // The read is LOUD: for an attach-class
                // mount the alias is REQUIRED identity — a catalog error or a
                // NULL is an internal consistency failure, never a silent
                // fallback that would recreate the hub-introspection bug.
                let Some(alias) = attach_alias.as_deref() else {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(DelightQLError::from(Runtime::General {
                        message: format!(
                            "Cannot refresh '{}': it has a mount cartridge but no recorded \
                         attachment alias — internal catalog inconsistency",
                            namespace
                        ),
                        details: "Missing attach_alias for attach-class mount".to_string(),
                    }));
                };
                match self.introspector.introspect_entities_in_schema(alias) {
                    Ok(e) => e,
                    Err(e) => {
                        return Err(Runtime::catalog(
                            format!("Failed to re-introspect schema '{}': {}", alias, e),
                            e.to_string(),
                        ));
                    }
                }
            } else {
                // Factory path: use connection_factory
                match &self.connection_factory {
                    Some(factory) => {
                        let components = factory.create(&source_uri)?;
                        match components.introspector.introspect_entities() {
                            Ok(e) => e,
                            Err(e) => {
                                return Err(Runtime::catalog(
                                    format!("Failed to re-introspect '{}': {}", source_uri, e),
                                    e.to_string(),
                                ));
                            }
                        }
                    }
                    None => {
                        return Err(DelightQLError::from(Runtime::General {
    message: "Cannot refresh factory-mounted namespace without connection factory".to_string(),
    details: "No connection factory".to_string(),
}));
                    }
                }
            };

        // 6. Re-register: new cartridge + entities
        let cartridge_id = {
            let language = if connection_id == PRIMARY_CONNECTION_ID {
                3
            } else {
                // Determine from source_uri
                if source_uri.starts_with("postgres://") || source_uri.starts_with("postgresql://")
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
                    source_ns.as_deref(),
                    connection_id,
                ],
            ).map_err(|e| {
                Runtime::catalog("Failed to create refresh cartridge", e.to_string())
            })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        let replacement = if mount_class == "attach" {
            MountBinding::attach(
                ns_id,
                cartridge_id as i64,
                connection_id,
                attach_alias.clone().ok_or_else(|| {
                    DelightQLError::from(Runtime::General {
                        message: "Cannot refresh attach mount without an attachment alias"
                            .to_string(),
                        details: "Missing mount alias".to_string(),
                    })
                })?,
                // Refresh re-reads a schema that is already open; it opens
                // nothing, so it cannot turn a borrowed handle into an owned
                // one. The fact travels from the row being replaced.
                mount_attachment,
                if namespace == "main" {
                    "unqualified"
                } else {
                    "aliased"
                },
            )
        } else {
            MountBinding::external(ns_id, cartridge_id as i64, connection_id, engine_schema)
        };
        if let Err(e) = Self::record_mount_binding(&bootstrap_conn, &replacement) {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        let entity_count = entities.len();
        if let Err(e) = crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        ) {
            return Err(Runtime::catalog(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            ));
        }

        if let Err(e) = crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            ns_id as i32,
        ) {
            return Err(Runtime::catalog(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            ));
        }

        // 7. The refreshed data world must still answer every derived
        // world's data holes: re-admit each world bound to it, whole, by
        // the same admission that published it.
        for root_id in crate::defuse::grounded_world::roots_bound_to(&bootstrap_conn, ns_id)? {
            crate::defuse::grounded_world::DerivedWorld::current(&bootstrap_conn, root_id)?
                .admit(&bootstrap_conn)
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Grounding contract violation: data '{namespace}'. {e}"),
                        "Grounding contract violated",
                    )
                })?;
        }

        // 8. Commit
        transaction.commit("Failed to commit refresh transaction")?;

        drop(bootstrap_conn);

        debug!(
            "refresh_namespace: Refreshed namespace '{}' with {} entities",
            namespace, entity_count
        );

        Ok(entity_count)
    }
}

#[cfg(test)]
mod schema_mount_recording_tests {
    //! The mounted
    //! engine schema is a RECORDED per-mount fact. Mount qualification is read
    //! from `mount`; cartridge.source_ns remains source metadata and is never
    //! a second policy authority. A mount given a specific schema records THAT
    //! schema (and introspects it); a bare mount records unqualified policy and
    //! the creation target resolves the engine default downstream.
    use crate::creation_target::DurablePlacement;
    use crate::system::ReadySystem;
    use delightql_types::factory::ConnectionComponents;
    use delightql_types::introspect::{
        DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity,
    };
    use delightql_types::test_utils::{MockDatabaseConnection, MockSchemaProvider};
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    /// Whether an entity named `name` is activated in the namespace `fq` —
    /// the catalog fact, read by identity.
    fn activated_in(system: &ReadySystem, fq: &str, name: &str) -> bool {
        let conn = system.get_bootstrap_connection();
        let guard = conn.lock().expect("bootstrap lock");
        guard
            .query_row(
                "SELECT EXISTS (
                     SELECT 1 FROM activated_entity ae
                     JOIN entity e ON e.id = ae.entity_id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE n.fq_name = ?1 AND e.name = ?2 COLLATE NOCASE)",
                rusqlite::params![fq, name],
                |row| row.get::<_, bool>(0),
            )
            .expect("activation probe")
    }

    /// Discovers ONE entity named after the schema it was built for — the
    /// mock analog of `FatboyIntrospector` introspecting its own bound
    /// schema. Proves the schema flows to INTROSPECTION, not merely to the
    /// recorded fact.
    struct SchemaEchoIntrospector {
        schema: Option<String>,
    }
    impl DatabaseIntrospector for SchemaEchoIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            let s = self.schema.as_deref().unwrap_or("public");
            Ok(vec![DiscoveredEntity {
                name: format!("in_{s}").into(),
                entity_type_id: 10,
                attributes: vec![DiscoveredAttribute {
                    name: "id".into(),
                    data_type: "INTEGER".to_string(),
                    position: 0,
                    is_nullable: true,
                }],
            }])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> ReadySystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(
            conn,
            Box::new(SchemaEchoIntrospector { schema: None }),
            "sqlite",
        )
        .expect("fresh in-memory system should build")
    }

    /// A postgres-typed mock mount whose `ConnectionComponents.mounted_schema`
    /// and introspector schema AGREE (the wiring `create_fatboy_system_components`
    /// builds).
    fn pg_components(mounted_schema: Option<&str>) -> ConnectionComponents {
        ConnectionComponents {
            connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(SchemaEchoIntrospector {
                schema: mounted_schema.map(str::to_string),
            }),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: None,
            mounted_schema: mounted_schema.map(str::to_string),
        }
    }

    fn shared_pg_components(
        mounted_schema: &str,
        identity: &str,
        connection: Arc<Mutex<dyn delightql_types::DatabaseConnection>>,
    ) -> ConnectionComponents {
        ConnectionComponents {
            connection,
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(SchemaEchoIntrospector {
                schema: Some(mounted_schema.to_string()),
            }),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: Some(identity.to_string()),
            mounted_schema: Some(mounted_schema.to_string()),
        }
    }

    /// The durable placement a creation target reads for a namespace.
    fn durable_placement(system: &ReadySystem, fq: &str) -> DurablePlacement {
        let conn = system.bootstrap_connection.lock().unwrap();
        crate::creation_target::DataTarget::read(
            &*conn,
            crate::definition_catalog::NamespaceKey::Fq(fq),
            "a placement probe",
        )
        .expect("a data-backed namespace")
        .durable()
        .clone()
    }

    /// A mount given a SPECIFIC schema records it on `mount`; the creation
    /// target reads that recorded fact VERBATIM (type 3 would have DERIVED
    /// `public`); and the mount introspected THAT schema's entity. A
    /// connection-type derivation alone would ignore the recorded schema and
    /// answer `public` here regardless.
    #[test]
    fn mount_records_the_engine_schema_and_the_target_reads_it() {
        let mut system = fresh_system();
        let (_conn_id, count) = system
            .register_external_connection(pg_components(Some("reporting")), "rep", "mock://rep")
            .expect("register the reporting-schema mount");
        assert_eq!(count, 1, "the schema's one entity is introspected");
        assert_eq!(
            durable_placement(&system, "rep"),
            DurablePlacement::Schema("reporting".to_string()),
            "the target reads the RECORDED schema, not the connection-type default"
        );
        // Introspection followed the schema: the discovered entity is named
        // for 'reporting' and is registered in the namespace.
        assert!(
            activated_in(&system, "rep", "in_reporting"),
            "the mount introspected the 'reporting' schema's entity"
        );
    }

    /// A BARE postgres mount records unqualified policy; its durable
    /// placement is the engine default spelled out, since an unspelled
    /// schema would resolve through search_path.
    #[test]
    fn bare_postgres_mount_places_durable_objects_in_public() {
        let mut system = fresh_system();
        system
            .register_external_connection(pg_components(None), "plain", "mock://plain")
            .expect("register the bare mount");
        assert_eq!(
            durable_placement(&system, "plain"),
            DurablePlacement::Schema("public".to_string()),
        );
        // The default introspection discovered the default schema's entity.
        assert!(activated_in(&system, "plain", "in_public"));
    }

    /// Qualified mounts create catalog topology, rather than storing the full
    /// path in a flat leaf row. This is shared by URI mounts and SQLite ATTACH
    /// mounts, so the cheap mock path pins the engine-level invariant while the
    /// corpus exercises the real file mount.
    #[test]
    fn qualified_mount_materializes_and_protects_its_container_parent() {
        let mut system = fresh_system();
        system
            .register_external_connection(
                pg_components(Some("reporting")),
                "client::reporting",
                "mock://nested-reporting",
            )
            .expect("register a qualified external mount");

        let bootstrap = system.get_bootstrap_connection();
        let conn = bootstrap.lock().unwrap();
        let root_id: i32 = conn
            .query_row("SELECT id FROM namespace WHERE fq_name = '_'", [], |row| {
                row.get(0)
            })
            .unwrap();
        let (parent_id, parent_name, parent_pid, parent_kind): (i32, String, Option<i32>, String) =
            conn.query_row(
                "SELECT id, name, pid, kind FROM namespace WHERE fq_name = 'client'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let (leaf_name, leaf_pid, leaf_kind): (String, Option<i32>, String) = conn
            .query_row(
                "SELECT name, pid, kind FROM namespace WHERE fq_name = 'client::reporting'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(parent_name, "client");
        assert_eq!(parent_pid, Some(root_id));
        assert_eq!(parent_kind, "container");
        assert_eq!(leaf_name, "reporting", "leaf name is one path segment");
        assert_eq!(leaf_pid, Some(parent_id));
        assert_eq!(leaf_kind, "data");

        let wrapper_count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM entity e
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.source_uri = 'catalog://sys::meta'
                   AND e.name IN ('client::', 'client::reporting::')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(wrapper_count, 2, "parent and leaf each have one wrapper");
        drop(conn);

        let err = system
            .unconsult_namespace("client")
            .expect_err("a structural mount parent is not a consulted library");
        assert!(err.to_string().contains("structural container"));
    }

    #[test]
    fn shared_connection_survives_until_the_last_tree_binding_is_unmounted() {
        let mut system = fresh_system();
        let shared: Arc<Mutex<dyn delightql_types::DatabaseConnection>> =
            Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let (connection_id, _) = system
            .register_external_connection(
                shared_pg_components("a", "pg-system-id:tree", Arc::clone(&shared)),
                "tree::a",
                "mock://tree",
            )
            .expect("mount first schema");
        let (same_connection_id, _) = system
            .register_external_connection(
                shared_pg_components("b", "pg-system-id:tree", Arc::clone(&shared)),
                "tree::b",
                "mock://tree",
            )
            .expect("mount second schema");
        assert_eq!(connection_id, same_connection_id);

        system
            .unmount_database("tree::a")
            .expect("unmount first leaf");
        assert!(
            system.get_connection(connection_id).is_ok(),
            "a sibling mount row still owns the shared live connection"
        );

        system
            .unmount_database("tree::b")
            .expect("unmount final leaf");
        assert!(
            system.get_connection(connection_id).is_err(),
            "the last mount row releases the shared live connection"
        );
    }

    struct FailingIntrospector;
    impl DatabaseIntrospector for FailingIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Err(crate::diagnostic::DelightQLError::from(
                crate::diagnostic::Runtime::General {
                    message: "induced external introspection failure".to_string(),
                    details: "schema_mount_recording_tests".to_string(),
                },
            ))
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    #[test]
    fn failed_external_mount_rolls_back_connection_namespace_and_binding() {
        let mut system = fresh_system();
        let components = ConnectionComponents {
            connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(FailingIntrospector),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: Some("pg-system-id:failing".to_string()),
            mounted_schema: Some("public".to_string()),
        };

        system
            .register_external_connection(components, "failed", "mock://failed")
            .expect_err("the injected introspection failure must abort the mount");

        let conn = system.bootstrap_connection.lock().unwrap();
        let leftovers: i64 = conn
            .query_row(
                "SELECT
                    (SELECT count(*) FROM namespace WHERE fq_name = 'failed') +
                    (SELECT count(*) FROM connection WHERE resource_uri = 'mock://failed') +
                    (SELECT count(*) FROM cartridge WHERE source_uri = 'mock://failed') +
                    (SELECT count(*) FROM mount m JOIN namespace n ON n.id = m.namespace_id
                     WHERE n.fq_name = 'failed')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            leftovers, 0,
            "external mount failure must leave no catalog fragment"
        );
    }
}

#[cfg(test)]
mod detach_guard_tests {
    //! The alias guard's contract:
    //! Armed from immediately after ATTACH to just after COMMIT, it must
    //! DETACH on every early exit and stay silent once disarmed.

    use super::DetachOnDrop;
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::DatabaseConnection;
    use std::sync::{Arc, Mutex};

    #[test]
    fn armed_guard_detaches_and_disarmed_guard_does_not() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();

        // Armed guard dropped (any early return after ATTACH) → DETACH runs.
        {
            let _guard = DetachOnDrop {
                connection: Arc::clone(&conn),
                alias: "_imported_991",
                armed: true,
            };
        }
        assert!(
            mock.lock()
                .unwrap()
                .assert_executed("DETACH DATABASE '_imported_991'"),
            "armed guard must DETACH its alias on drop"
        );

        // Disarmed guard (post-COMMIT) → no DETACH.
        {
            let mut guard = DetachOnDrop {
                connection: Arc::clone(&conn),
                alias: "_imported_992",
                armed: true,
            };
            guard.armed = false;
        }
        assert!(
            !mock.lock().unwrap().assert_executed("_imported_992"),
            "disarmed guard must not DETACH"
        );
    }

    #[test]
    fn explicit_rollback_detaches_without_relying_on_drop() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut guard = DetachOnDrop {
            connection: Arc::clone(&conn),
            alias: "_imported_993",
            armed: true,
        };

        guard.rollback().expect("explicit inverse should detach");
        assert!(mock
            .lock()
            .unwrap()
            .assert_executed("DETACH DATABASE '_imported_993'"));
        drop(guard);
        assert_eq!(
            mock.lock()
                .unwrap()
                .get_executed_queries()
                .iter()
                .filter(|query| query.sql.contains("_imported_993"))
                .count(),
            1,
            "Drop must not retry an explicitly completed inverse"
        );
    }

    #[test]
    fn failed_explicit_rollback_is_disarmed_for_health_recovery() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        mock.lock()
            .unwrap()
            .expect_error("DETACH DATABASE '_imported_994'", "scripted detach failure");
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut guard = DetachOnDrop {
            connection: Arc::clone(&conn),
            alias: "_imported_994",
            armed: true,
        };

        let error = guard
            .rollback()
            .expect_err("scripted inverse must be observable");
        assert!(error.to_string().contains("Failed to detach"));
        drop(guard);
        assert_eq!(
            mock.lock()
                .unwrap()
                .get_executed_queries()
                .iter()
                .filter(|query| query.sql.contains("_imported_994"))
                .count(),
            1,
            "Drop must not retry a failed explicit inverse"
        );
    }
}

#[cfg(test)]
mod mount_new_database_tests {
    //! `mount_new!` (EFFECT-ALGEBRA §6): PROVISION a fresh, valid, empty SQLite
    //! database and bind it — the create-intent counterpart of `mount!`. These
    //! pin the three new behaviors (materialization, clobber refusal, v1
    //! SQLite-only scope) + reserved-name inheritance. The end-to-end
    //! round-trip (mount_new! then mount! the same file, with a real read-back)
    //! is the CLI `mount_new_roundtrip` integration test.

    use crate::system::{LiminalProgramKind, ReadySystem};
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> ReadySystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// A valid empty SQLite database has the 16-byte header magic.
    fn is_valid_sqlite(path: &std::path::Path) -> bool {
        use std::io::Read;
        let mut header = [0u8; 16];
        std::fs::File::open(path)
            .and_then(|mut f| f.read_exact(&mut header))
            .map(|()| &header == b"SQLite format 3\0")
            .unwrap_or(false)
    }

    /// mount_new! on a MISSING path materializes a valid, non-zero SQLite
    /// database (header-bearing) and the namespace resolves.
    #[test]
    fn mount_new_provisions_a_valid_database_and_binds_the_namespace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fresh.db");
        assert!(!path.exists(), "path must start missing");

        let mut system = fresh_system();
        system
            .mount_new_database(path.to_str().unwrap(), "freshns")
            .expect("mount_new! should provision + bind");

        // File exists, non-zero, valid SQLite header.
        assert!(path.exists(), "database file must exist after mount_new!");
        let len = std::fs::metadata(&path).expect("metadata").len();
        assert!(
            len > 0,
            "materialized db must be non-empty, got {len} bytes"
        );
        assert!(
            is_valid_sqlite(&path),
            "materialized db must carry the SQLite header"
        );

        // The namespace is registered and reachable: enlist_namespace requires
        // the namespace to exist (the enlisted-guard-classification test's
        // proof-of-registration pattern). The CLI round-trip test proves an
        // in-session read end-to-end.
        system
            .enlist_namespace("freshns")
            .expect("mount_new!'s namespace must be registered + reachable");
    }

    /// CLOBBER: mount_new! on a path holding a REAL database refuses with the
    /// substring, and the existing database is left untouched.
    #[test]
    fn mount_new_refuses_to_clobber_an_existing_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("existing.db");
        // A real db with a table + row.
        {
            let conn = rusqlite::Connection::open(&path).expect("seed db");
            conn.execute_batch("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (7);")
                .expect("seed schema");
        }
        let before = std::fs::read(&path).expect("read before");

        let mut system = fresh_system();
        let err = system
            .mount_new_database(path.to_str().unwrap(), "clobberns")
            .expect_err("mount_new! must refuse to clobber a non-empty path");
        let msg = format!("{err}");
        assert!(
            msg.contains("already exists; use mount!() to attach it"),
            "clobber message must teach mount!(): {msg}"
        );

        // The existing database is byte-for-byte untouched.
        let after = std::fs::read(&path).expect("read after");
        assert_eq!(
            before, after,
            "existing db must be untouched on clobber refusal"
        );
    }

    /// A 0-byte file is NOT content — mount_new! materializes over it.
    #[test]
    fn mount_new_materializes_over_a_zero_byte_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.db");
        std::fs::write(&path, b"").expect("touch 0-byte file");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        let mut system = fresh_system();
        system
            .mount_new_database(path.to_str().unwrap(), "zerons")
            .expect("mount_new! should materialize over a 0-byte file");
        assert!(is_valid_sqlite(&path), "0-byte file must become a valid db");
    }

    /// The external journal distinguishes a caller-owned zero-byte placeholder
    /// from an absent path. Aborting the program restores the placeholder; it
    /// must not delete it as though mount_new! had created the path itself.
    #[test]
    fn aborted_program_restores_zero_byte_mount_new_placeholder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("placeholder.db");
        std::fs::write(&path, b"").expect("touch 0-byte file");

        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        system
            .mount_new_database(path.to_str().unwrap(), "temporary_mount")
            .expect("mount_new inside program");
        assert!(std::fs::metadata(&path).unwrap().len() > 0);

        system.rollback_liminal_external_effects();
        system
            .end_liminal_program(false)
            .expect("rollback program catalog");

        assert!(path.exists(), "caller-owned placeholder must remain");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            0,
            "placeholder must return to its exact pre-program state"
        );
    }

    /// v1 SCOPE: a URI target (postgres://, …) refuses cleanly with the
    /// SQLite-only substring — no file is created.
    #[test]
    fn mount_new_refuses_non_sqlite_targets() {
        let mut system = fresh_system();
        let err = system
            .mount_new_database("postgres://localhost/db", "pgns")
            .expect_err("mount_new! is SQLite-only in v1");
        let msg = format!("{err}");
        assert!(
            msg.contains("mount_new!() creates a new SQLite database"),
            "v1-scope message must state SQLite-only: {msg}"
        );
    }

    /// Reserved-name refusal is inherited from mount_database: a `_`-prefixed
    /// target is refused BEFORE any file is materialized.
    #[test]
    fn mount_new_inherits_the_reserved_name_refusal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("reserved.db");

        let mut system = fresh_system();
        let err = system
            .mount_new_database(path.to_str().unwrap(), "_secret")
            .expect_err("mount_new! must refuse a reserved namespace");
        assert!(
            format!("{err}").contains("reserved"),
            "reserved-name refusal must survive: {err}"
        );
        // No file materialized for a refused target.
        assert!(
            !path.exists(),
            "no db must be created for a reserved-name refusal"
        );
    }
}
