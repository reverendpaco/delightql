// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The pristine native world: constructed once, frozen, instantiated privately.
//!
//! A native `DelightQLSystem` answers over an in-memory SQLite catalog. That
//! catalog's invariant content — schema, enum and error registries, the
//! builtin inventory, the universal stdlib overlays, the canonical seed
//! facts, the two connection-role rows and the session-ledger definitions —
//! is the same for every session a binary will ever open. Deriving it from
//! source is the whole cost of a session reset; this module derives it ONCE.
//!
//! The lifecycle is a chain of closed carriers, each consumed whole by the
//! next transition, and completeness is a fact of the TYPE at every step:
//!
//! ```text
//! CanonicalCatalog::construct ─► CanonicalCatalog  (sealed; no overlays yet)
//!   into_construction         ─► Construction      (a host on the construction
//!                                                   connection; no image, no reset)
//!   finalize                  ─► Finalized         (overlays, seeds, catalog
//!                                                   cartridge — THEN the image
//!                                                   is frozen from that world)
//!   publish                   ─► ReadySystem       (the host running on an
//!                                                   instance, owning the image)
//! ReadySystem::reinit_bootstrap                     (instantiate + install)
//! ```
//!
//! Laws this module is the authority for:
//!
//! - **One procedural constructor.** `CanonicalCatalog::construct` is the
//!   only code that runs schema DDL, seeds registries, synchronizes builtin
//!   cartridges or registers the system entities. Reset never reaches it.
//! - **Finalization is the only mint of an image.** `PristineImage` has one
//!   private constructor, and the only code that calls it is the transition
//!   that has just consulted the universal overlays, run the seed programs
//!   and installed the catalog cartridge. An image of an unfinalized
//!   catalog is not a value this module can produce. The catalog-cartridge
//!   identity is a required fact of the image, never optional.
//! - **Nothing incomplete can reset.** The host type `DelightQLSystem`
//!   owns no image and has no reset; the construction carriers own no
//!   image and their whole method set is the next transition. The image
//!   and the reset live only on `ReadySystem`, which cannot exist until
//!   `Finalized::publish` has instantiated the finalized image.
//! - **The image is immutable and every instance is private.** The freeze
//!   copies SQLite's page image into an owned buffer; `instantiate` hands
//!   SQLite a fresh private copy of it, writable. The template is never a
//!   shared writable buffer and never mutated after it is frozen.
//! - **Connection state is restored explicitly.** Pages carry no
//!   connection-local policy and no Rust objects. Instantiation reapplies the
//!   one configuration judgment (`bootstrap::configure_connection`), restamps
//!   the world's clocks, recovers the identities the image bakes in, seals
//!   the guard, and returns all of it as one value.
//! - **Baked identities are reconciled, never re-registered.** The image
//!   carries the `connection` rows for the bootstrap and primary
//!   connections and the catalog cartridge; instantiation reads them back
//!   and checks them against the image's own facts. Registering again would
//!   mint a third connection; clearing the Rust mirror and letting a lazy
//!   road rebuild the catalog would be a second construction authority.
//! - **Publication is atomic.** `ReadyWorld::install_into` is the only code
//!   that lays a bootstrap connection into a host, and it lays the guard,
//!   the routing map, the schema provider and the catalog-cartridge mirror
//!   in the same act, from the same `ReadyWorld`. No signature outside this
//!   module accepts a connection beside independently chosen identities,
//!   and no signature anywhere accepts a host beside an image.
//!
//! This module is a child of `system` so it can construct and lay fields
//! into `DelightQLSystem`; the parent cannot see this module's private
//! fields, so the pairing of host, image, connection, guard and identities
//! cannot be re-assembled elsewhere.

use super::population::ensure_catalog_initialized;
use super::DelightQLSystem;
use crate::bin_cartridge::registry::BinCartridgeRegistry;
use crate::bootstrap::guard::BootstrapGuard;
use crate::bootstrap::{
    setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
    setup_finding_table_on_bootstrap, SourceType,
};
use crate::diagnostic::{Internal, Runtime};
use crate::error::{DelightQLError, Result};
use crate::external_effects::SessionHealth;
use delightql_types::DatabaseConnection;
use rusqlite::Connection;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

mod sys_tables;

/// The pristine world's transient ledgers begin empty. Compilation carried
/// out to construct the world — stdlib overlays, seed programs — is not any
/// session's history, and a ledger frozen with it would present the same
/// stale activity in every instance. `stack` cascades from `compilation`;
/// the sequence row goes too, so a fresh instance numbers its compilations
/// exactly as a newly constructed world does.
///
/// The effect ledgers (`effect_plan`, `effect_guard`, `effect_run`,
/// `effect_requirement`) persist the LAST plan's rows for post-mortem
/// inspection and are cleared by the next compile; a seed program is the
/// last compile before freezing, so its plan is cleared here the same way.
const NORMALIZE_TRANSIENT_LEDGERS: &str = "\
    DELETE FROM stack; \
    DELETE FROM compilation; \
    DELETE FROM sqlite_sequence WHERE name = 'compilation'; \
    DELETE FROM effect_requirement; \
    DELETE FROM effect_run; \
    DELETE FROM effect_guard; \
    DELETE FROM effect_plan;";

/// Every captured clock the schema declares (`DEFAULT (strftime(...))`,
/// found by definition — `captured_defaults_are_exactly_the_ruled_set`
/// pins the census) is given its per-world law here. `cartridge` and
/// `activated_entity` rows are FACTS of the world whose clocks describe the
/// world's own creation and activation; an instance is created now, exactly
/// as a constructed world is, so they are restamped. The third clock,
/// `compilation.timestamp`, lives on a ledger that is empty in the image.
///
/// This runs BEFORE the guard is sealed: `activated_entity` is a
/// definition-family table, fenced against row writes outside a catalog
/// window once the connection is sealed.
const RESTAMP_WORLD_CLOCKS: &str = "\
    UPDATE cartridge SET creation_time = strftime('%s', 'now'); \
    UPDATE activated_entity SET activation_time = strftime('%s', 'now');";

/// THE CATALOG KNOWS EVERY NAMESPACE A MENTION CAN REACH. Each embedded
/// module's row is seeded here, empty, as the system namespace it is (THE
/// SESSION START: `std` is the language's library, system territory); the
/// module's consult at image construction finds the row and fills it. A
/// module whose namespace a bin cartridge already created keeps that row's
/// identity and is stamped system, so no consult claims it as a library.
fn seed_embedded_module_namespaces(conn: &Connection) -> Result<()> {
    for (namespace_fq, _) in crate::stdlib_manifest::STDLIB_MODULES {
        let mut specs = crate::import::namespace::parse_namespace_path(conn, namespace_fq)
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to construct module namespace '{namespace_fq}': {e}"),
                    e.to_string(),
                )
            })?;
        for spec in &mut specs {
            if spec.fq_name == *namespace_fq {
                spec.kind = crate::namespace::NamespaceKind::System;
                spec.provenance = Some("bootstrap".into());
                spec.source_path = Some(format!("embedded://{namespace_fq}"));
            }
        }
        crate::import::namespace::create_namespace_hierarchy(conn, &specs).map_err(|e| {
            Runtime::catalog(
                format!("Failed to seed module namespace '{namespace_fq}': {e}"),
                e.to_string(),
            )
        })?;
        conn.execute(
            "UPDATE namespace SET kind = ?2, provenance = 'bootstrap', source_path = ?3
             WHERE fq_name = ?1 AND (kind IS NULL OR kind = 'unknown')",
            rusqlite::params![
                namespace_fq,
                crate::namespace::NamespaceKind::System.spelling(),
                format!("embedded://{namespace_fq}")
            ],
        )
        .map_err(|e| catalog_error(&format!("stamp module namespace '{namespace_fq}' as a system namespace"), e))?;
    }
    Ok(())
}

fn catalog_error(operation: &str, e: impl std::fmt::Display) -> DelightQLError {
    Runtime::catalog(operation, format!("SQLite error: {}", e))
}

/// The identities the page image bakes in: catalog rows that name objects
/// the image does NOT carry (the connections) or that a Rust-side mirror
/// caches (the catalog cartridge). Read from a world, never chosen. Every
/// fact is required: a world missing one is not a world this module
/// answers for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorldFacts {
    bootstrap_connection_id: i64,
    primary_connection_id: i64,
    catalog_cartridge_id: i32,
}

impl WorldFacts {
    fn read(conn: &Connection) -> Result<Self> {
        let connection_id = |uri: &str| -> Result<i64> {
            conn.query_row(
                "SELECT id FROM connection WHERE resource_uri = ?1",
                [uri],
                |row| row.get(0),
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("the world has no '{uri}' connection row"),
                    e.to_string(),
                )
            })
        };
        let bootstrap_connection_id = connection_id("session:bootstrap")?;
        let primary_connection_id = connection_id("session:primary")?;
        let catalog_cartridge_id = conn
            .query_row(
                "SELECT id FROM cartridge
                 WHERE source_uri = 'catalog://sys::meta' AND source_ns = 'sys::meta'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("the world has no catalog cartridge", e.to_string()))?;
        Ok(WorldFacts {
            bootstrap_connection_id,
            primary_connection_id,
            catalog_cartridge_id,
        })
    }
}

// =============================================================================
// Canonical construction
// =============================================================================

/// The sealed canonical catalog: the product of the one procedural
/// construction, before the compiler has run over it. Its only consumer is
/// `into_construction`, which wraps it in the construction carrier that
/// finalizes it. It has no image and no reset.
pub(crate) struct CanonicalCatalog {
    connection: Connection,
    guard: BootstrapGuard,
    primary_connection_id: i64,
    universal_namespaces: Vec<String>,
}

impl CanonicalCatalog {
    /// The ONE procedural construction of the native bootstrap catalog:
    /// schema and registries, the two connection-role rows (bootstrap `1`,
    /// primary `2`), the bootstrap cartridge and namespaces, the builtin
    /// cartridge inventory, the session-ledger entities and the curated
    /// `sys::` tables — sealed. The `main` namespace is left empty for an
    /// explicit mount.
    pub(crate) fn construct(bin_registry: &BinCartridgeRegistry, db_type: &str) -> Result<Self> {
        let bootstrap_conn = Connection::open_in_memory().map_err(|e| {
            Runtime::catalog(
                "Failed to create _bootstrap metadata store",
                format!("SQLite error: {}", e),
            )
        })?;

        // Initialize _bootstrap schema and seed data
        crate::bootstrap::initialize_bootstrap_db(&bootstrap_conn).map_err(|e| {
            Runtime::catalog(
                format!("Failed to initialize _bootstrap schema: {}", e),
                e.to_string(),
            )
        })?;

        // Session-ledger definitions and canonical defaults (assertions,
        // danger, finding): part of the pristine world; their rows are not.
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_finding_table_on_bootstrap(&bootstrap_conn)?;

        // Register bootstrap connection (id=1) BEFORE installing cartridge
        // (cartridge has FK to connection)
        let bootstrap_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:bootstrap",
            "in-process",
            None,
            5, // bootstrap connection type
            "Internal engine metadata store",
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to register bootstrap connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Sanity check: bootstrap connection should always be id=1
        if bootstrap_conn_id != 1 {
            return Err(Runtime::catalog(
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
            Runtime::catalog(
                format!("Failed to install bootstrap cartridge: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::create_bootstrap_namespaces(&bootstrap_conn).map_err(|e| {
            Runtime::catalog(
                format!("Failed to create bootstrap namespaces: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::activate_bootstrap_entities(&bootstrap_conn, cartridge_id).map_err(|e| {
            Runtime::catalog(
                format!("Failed to activate bootstrap entities: {}", e),
                e.to_string(),
            )
        })?;

        // Sync all bin cartridges to bootstrap metadata
        let universal_namespaces =
            crate::bootstrap::sync_bin_cartridges_to_bootstrap(&bootstrap_conn, bin_registry)
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Failed to sync bin cartridges to bootstrap: {}", e),
                        e.to_string(),
                    )
                })?;
        seed_embedded_module_namespaces(&bootstrap_conn)?;
        // Register the primary (user) connection in bootstrap metadata.
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
                return Err(DelightQLError::from(Runtime::General {
                    message: "Unsupported database type".to_string(),
                    details: format!("Database type '{}' is not supported", db_type),
                }));
            }
        };

        let user_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:primary",
            if db_type_lower == "sqlite" {
                "in-process"
            } else {
                "fatboy"
            },
            None,
            connection_type,
            "User target database (pre-mount placeholder)",
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to register user connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // "main" namespace is created empty by create_bootstrap_namespaces().
        // No user cartridge, no introspection — the CLI sends mount!("path", "main")(*)
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
                Runtime::catalog(format!("Failed to create sys session cartridge: {}", e), e.to_string())
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
                Runtime::catalog(
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
                Runtime::catalog(
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
                    Runtime::catalog(
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
                Runtime::catalog(
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
                Runtime::catalog(
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
                    Runtime::catalog(
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
                Runtime::catalog(
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
                Runtime::catalog(
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
                    Runtime::catalog(
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
                Runtime::catalog(
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
            Runtime::catalog(
                format!("Failed to activate sys.assertions in sys namespace: {}", e),
                e.to_string(),
            )
        })?;

        // Register the burned identifier table (rows authored in
        // bootstrap/schema.sql) as sys::identifiers.identifier. Its own cartridge so the bulk
        // activation above cannot leak it into bare `sys`.
        sys_tables::register_sys_identifier_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::diagnostics.finding: the session's own refusals and selftest
        // findings, queryable. Own cartridge for the same reason.
        sys_tables::register_sys_diagnostics_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::format: the burned formatter style-bundle table (book row
        // = frozen defaults).
        sys_tables::register_sys_format_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::config: the keys core declares, and the settings table the
        // host's boot rows and the session's rows are written into.
        crate::settings::register_sys_config_tables(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::connections: the curated safe-subset `connection` entity
        // (non-secret columns only). Own cartridge so the bulk activation
        // above cannot leak it into bare `sys`.
        sys_tables::register_sys_connection_table(&bootstrap_conn, bootstrap_conn_id)?;
        // sys::ns: curated public-column `namespace` entity (the physical
        // table carries the internal mount relation).
        sys_tables::register_sys_ns_namespace_table(&bootstrap_conn, bootstrap_conn_id)?;

        // Installation is complete: SEAL the catalog. Everything the
        // canonical schema authority and the registrations above created is
        // now protected against structural DDL, whatever SQL road reaches
        // this connection. The finalization that follows (stdlib overlays,
        // seeds, catalog views) is row DML and passes untouched.
        let guard = BootstrapGuard::seal(&bootstrap_conn)?;

        Ok(CanonicalCatalog {
            connection: bootstrap_conn,
            guard,
            primary_connection_id: user_conn_id,
            universal_namespaces,
        })
    }

    /// Wrap the sealed catalog in the construction carrier: a host running
    /// on this very connection, so the compiler can finalize the catalog it
    /// stands on. The host owns no image — there is nothing yet to freeze —
    /// and the carrier's whole method set is `finalize`.
    pub(crate) fn into_construction(
        self,
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        bin_registry: Arc<BinCartridgeRegistry>,
        db_type: &str,
    ) -> Construction {
        let mut connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>> = HashMap::new();
        connection_map.insert(self.primary_connection_id, Arc::clone(&connection));
        let bootstrap_connection = Arc::new(Mutex::new(self.connection));
        let schema = Box::new(crate::bootstrap_schema::BootstrapBackedSchema::new(
            bootstrap_connection.clone(),
        ));
        // The one sized instant: constructed with an empty tail, boxed, and
        // unsized by the coercion in the same expression. From here on no
        // road holds a `DelightQLSystem` as a value.
        let host: Box<DelightQLSystem> = Box::new(DelightQLSystem::<[(); 0]> {
            #[cfg(not(target_arch = "wasm32"))]
            capabilities: crate::host::HostCapabilities::native(),
            #[cfg(target_arch = "wasm32")]
            capabilities: crate::host::HostCapabilities::browser(),
            connection,
            bootstrap_connection,
            schema: Some(schema),
            connection_map,
            introspector,
            bin_registry,
            connection_factory: None,
            schema_map: HashMap::new(),
            introspector_map: HashMap::new(),
            catalog_cartridge_id: Cell::new(None),
            db_type: db_type.to_string(),
            effects_executed: Cell::new(0),
            active_liminal_program: RefCell::new(None),
            session_health: SessionHealth::default(),
            byte_bindings: HashMap::new(),
            bootstrap_guard: self.guard,
            standing: [],
        });
        Construction {
            host,
            universal_namespaces: self.universal_namespaces,
        }
    }
}

// =============================================================================
// Construction and finalization
// =============================================================================

/// THE CONSTRUCTION CARRIER: a host running on the construction connection
/// over a catalog that is sealed but not finalized — no embedded module
/// consulted, no seed docs written, no catalog cartridge. It owns no image and has
/// no reset; the one thing that can be done with it is `finalize`, and
/// nothing can take the host out of it.
pub(crate) struct Construction {
    host: Box<DelightQLSystem>,
    universal_namespaces: Vec<String>,
}

impl Construction {
    /// THE TRANSITION THAT PROVES CANONICAL FINALIZATION: consult every
    /// embedded module, write the seed docs, install the catalog cartridge —
    /// the startup effects — and
    /// then, from the very connection they ran on, freeze the image. This
    /// is the only road to a `PristineImage`, so every image is the image
    /// of a finalized world. A failure here is a startup failure: a world
    /// missing an overlay would otherwise be frozen into every session.
    pub(crate) fn finalize(self, boot: &crate::settings::Admitted) -> Result<Finalized> {
        self.finalize_after(boot, |_, _| {})
    }

    /// TEST-ONLY: finalize, then let a test alter the finalized world on
    /// its own connection before it is frozen — to force clocks or plant
    /// ledger rows so a restamp or a normalization is attributable. The
    /// alteration runs after every finalization effect, never instead of
    /// one; production has no such hook.
    #[cfg(test)]
    pub(crate) fn finalize_tampered(
        self,
        tamper: impl FnOnce(&Connection, &BootstrapGuard),
    ) -> Result<Finalized> {
        let boot = crate::settings::BootSettings::new()
            .state(crate::settings::BASE_DIRECTORY, None)
            .admit()?;
        self.finalize_after(&boot, tamper)
    }

    fn finalize_after(
        self,
        boot: &crate::settings::Admitted,
        before_freeze: impl FnOnce(&Connection, &BootstrapGuard),
    ) -> Result<Finalized> {
        let Construction {
            mut host,
            universal_namespaces,
        } = self;
        // Every embedded module is populated in the pristine image: the
        // session starts with the language's library in the tree, and no
        // selection runs a population act.
        let embedded = crate::stdlib_manifest::STDLIB_MODULES.iter().map(|(ns, _)| ns.to_string());
        let populated: Vec<String> = universal_namespaces
            .iter()
            .cloned()
            .chain(embedded.filter(|ns| !universal_namespaces.contains(ns)))
            .collect();
        for ns in &populated {
            match host.ensure_stdlib_loaded(ns) {
                super::StdlibLoad::Loaded
                | super::StdlibLoad::AlreadyLoaded
                | super::StdlibLoad::NotAModule => {}
                super::StdlibLoad::Failed { error, .. } => {
                    return Err(Runtime::catalog(
                        format!(
                            "stdlib overlay '{ns}' failed while constructing the pristine world"
                        ),
                        error.to_string(),
                    ));
                }
            }
        }
        host.write_seed_docs()?;
        let image = {
            let bootstrap_conn = host.bootstrap_connection.lock().map_err(|e| {
                Runtime::poisoned(
                    "Failed to acquire bootstrap lock to finalize the pristine world",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            // The catalog cartridge is a closed fact of the pristine image.
            {
                let _catalog_window = host.bootstrap_guard.catalog_window();
                ensure_catalog_initialized(&host.catalog_cartridge_id, &bootstrap_conn)?;
            }
            // The host's boot rows are part of the image, so every reset
            // restores them and no session row survives one.
            boot.write_boot(&bootstrap_conn)?;
            before_freeze(&bootstrap_conn, &host.bootstrap_guard);
            PristineImage::freeze(&bootstrap_conn)?
        };
        Ok(Finalized { host, image })
    }
}

/// A FINALIZED CANONICAL WORLD WITH ITS FROZEN IMAGE, not yet published:
/// the host still runs on the construction connection. Its whole method
/// set is `publish`; it has no reset, and neither half can leave it
/// separately.
pub(crate) struct Finalized {
    host: Box<DelightQLSystem>,
    image: PristineImage,
}

impl Finalized {
    /// Publish: instantiate the image and install that instance into the
    /// host, so the first exposed system is a world of the same kind every
    /// reset installs, running on a private copy of the image it owns. The
    /// construction connection is dropped with the instance's arrival.
    pub(crate) fn publish(self) -> Result<ReadySystem> {
        let Finalized { mut host, image } = self;
        image.instantiate()?.install_into(&mut host)?;
        Ok(ReadySystem { host, image })
    }
}

// =============================================================================
// The ready system
// =============================================================================

/// THE NATIVE SYSTEM A HOST IS GIVEN: the compiler host running on an
/// instance of the pristine world, and the image that world was
/// instantiated from — the ONLY source of every world a reset installs.
/// This is the only reset-capable type, and it cannot exist without its
/// image: the two are laid together by `Finalized::publish` and by nothing
/// else. The image is derived from this system's exact builtin registry
/// and backend category and lives and dies with it.
///
/// Every session operation is the host's, reached through `Deref` and
/// `DerefMut`. The host is UNSIZED (`DelightQLSystem`'s tail), so what
/// those lend is a place to operate on, never a value: no safe code can
/// move the host out, replace it, or swap it with another ready system's
/// host under the other's image. The box is this carrier's alone.
pub(crate) struct ReadySystem {
    host: Box<DelightQLSystem>,
    image: PristineImage,
}

impl ReadySystem {
    pub(crate) fn compiler_host_mut(&mut self) -> &mut DelightQLSystem {
        &mut self.host
    }

    /// Create a native DelightQL system from an injected connection.
    ///
    /// Native startup is the one place the pristine world is CONSTRUCTED:
    /// the canonical catalog (schema, registries, builtins, system entities)
    /// is built procedurally exactly once, finalized through the universal
    /// stdlib overlays and the embedded seed programs, frozen as an owned
    /// page image, and then PUBLISHED by instantiating that image.
    ///
    /// # Arguments
    /// * `connection` - User database connection trait object (for execution)
    /// * `introspector` - Backend-specific introspector for discovering schema
    /// * `db_type` - Database type string ("sqlite", "duckdb", "postgres")
    pub(crate) fn booted(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
        boot: &crate::settings::Admitted,
    ) -> Result<Self> {
        Self::construct(connection, introspector, db_type)?
            .finalize(boot)?
            .publish()
    }

    /// A system for a test, booted as a host with no filesystem: its base
    /// directory is stated as none, so a relative path refuses.
    #[cfg(test)]
    pub(crate) fn new(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
    ) -> Result<Self> {
        Self::stating(
            connection,
            introspector,
            db_type,
            crate::settings::BootSettings::new().state(crate::settings::BASE_DIRECTORY, None),
        )
    }

    /// A world booted on the settings a test states.
    #[cfg(test)]
    pub(crate) fn stating(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
        boot: crate::settings::BootSettings,
    ) -> Result<Self> {
        Self::booted(connection, introspector, db_type, &boot.admit()?)
    }

    /// The construction phase of `new`: the canonical catalog, wrapped for
    /// finalization. Tests take it to finalize under their own tampering
    /// and to compare the finalized world with an instance of its image.
    fn construct(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
    ) -> Result<Construction> {
        let bin_registry = Arc::new(super::builtin_registry());
        let catalog = CanonicalCatalog::construct(&bin_registry, db_type)?;
        Ok(catalog.into_construction(connection, introspector, bin_registry, db_type))
    }

    /// Reset: replace the session's world with a fresh instance of the
    /// pristine image. Everything that is NOT the image (the mounts on the
    /// user connection, pending external effects, the liminal context, the
    /// health latch) is handled here, in the order that keeps a failure
    /// from publishing anything partial:
    ///
    /// 1. pending external-effect compensation is retried; a still-failing
    ///    inverse keeps the quarantine and refuses the reset;
    /// 2. the candidate world is instantiated (no observable effect);
    /// 3. imported schemas are DETACHed from the user connection; a failed
    ///    DETACH aborts with the old catalog intact and consistent with the
    ///    attachment state;
    /// 4. the candidate is installed, and only then is the health latch
    ///    cleared.
    ///
    /// Reset contains no schema DDL, no registry seeding, no builtin
    /// synchronization, no overlay consultation and no seed execution: the
    /// image already holds all of it.
    pub(crate) fn reinit_bootstrap(&mut self) -> Result<()> {
        // A quarantined reset first retries every pending inverse. If any
        // inverse still fails, leave the incident and its inventory intact;
        // replacing the catalog cannot make uncertain external state safe.
        self.host.recover_pending_external_effects()?;

        let candidate = self.image.instantiate()?;

        self.host.detach_imported_schemas()?;
        self.host.drop_session_pool()?;

        candidate.install_into(&mut self.host)?;

        // Reset is the recovery boundary. Do not clear a quarantine before
        // the world above has been installed.
        self.host.active_liminal_program.replace(None);
        self.host.session_health = SessionHealth::Healthy;

        Ok(())
    }

    /// TEST-ONLY: the image this system owns, for proving that instances
    /// are private and the template does not move.
    #[cfg(test)]
    pub(crate) fn image_for_test(&self) -> &PristineImage {
        &self.image
    }

    /// TEST-ONLY: replace the image with one whose pages are not a
    /// database, for proving that a failed reset publishes nothing.
    #[cfg(test)]
    pub(crate) fn corrupt_image_for_test(&mut self) {
        self.image = self.image.corrupt_for_test();
    }
}

impl Deref for ReadySystem {
    type Target = DelightQLSystem;

    fn deref(&self) -> &DelightQLSystem {
        &self.host
    }
}

impl DerefMut for ReadySystem {
    fn deref_mut(&mut self) -> &mut DelightQLSystem {
        &mut self.host
    }
}

impl DelightQLSystem {
    /// DETACH every imported schema from the user connection, keeping
    /// `main`, `temp` and `sys` (the in-memory ATTACH that carries session
    /// tables; its rows are cleared by the world that replaces them).
    fn detach_imported_schemas(&self) -> Result<()> {
        let user_conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire user connection lock for reinit",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let schemas: Vec<String> = match user_conn.query_all_rows("PRAGMA database_list", &[]) {
            Ok((_cols, rows)) => rows
                .iter()
                .filter_map(|row| row.get(1).and_then(|v| v.as_wire_text()))
                .filter(|s| s != "main" && s != "temp" && s != "sys")
                .collect(),
            Err(_) => Vec::new(),
        };
        for schema in &schemas {
            // A failed DETACH must ABORT the reinit: proceeding would replace
            // the catalog — and every recorded cleanup identity — while the
            // database stays physically attached.
            if let Err(e) = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]) {
                return Err(Runtime::catalog(
                    format!(
                        "reset aborted: could not DETACH '{}' — the session \
                         catalog is left intact: {}",
                        schema, e
                    ),
                    e.to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Drop every table and view in the user connection's session pool, its
    /// TEMP schema: the world a reset installs records no session object,
    /// so one left behind would answer a later statement's unqualified
    /// spelling of its name. SQLite lists its pool in `temp.sqlite_master`; a
    /// connection of another engine keeps its pool. A failed drop aborts the
    /// reset, as a failed DETACH does.
    fn drop_session_pool(&self) -> Result<()> {
        if !self.db_type.eq_ignore_ascii_case("sqlite") {
            return Ok(());
        }
        let user_conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire user connection lock for reinit",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // One object at a time, through the one row read every connection
        // answers: each drop removes the row the next read would return.
        let mut dropped: Vec<String> = Vec::new();
        while let Some(row) = user_conn
            .query_row_values("SELECT type, name FROM temp.sqlite_master WHERE type IN ('table', 'view') LIMIT 1", &[])
            .map_err(|e| Runtime::catalog("reset aborted: could not read the session pool", e.to_string()))?
        {
            let (Some(kind), Some(name)) = (
                row.first().and_then(|v| v.as_wire_text()),
                row.get(1).and_then(|v| v.as_wire_text()),
            ) else {
                return Err(Runtime::catalog(
                    "reset aborted: a session pool row names no object",
                    format!("{row:?}"),
                ));
            };
            if dropped.contains(&name) {
                return Err(Runtime::catalog("reset aborted: a dropped session object is still listed", name));
            }
            let object = if kind == "view" { "VIEW" } else { "TABLE" };
            let statement = format!("DROP {object} IF EXISTS temp.\"{}\"", name.replace('"', "\"\""));
            user_conn.execute(&statement, &[]).map_err(|e| {
                Runtime::catalog(
                    format!("reset aborted: could not drop the session object '{name}'"),
                    e.to_string(),
                )
            })?;
            dropped.push(name);
        }
        Ok(())
    }

    /// The pristine image's entity docs: the doc act's facts, written once
    /// at construction from `seed/docs.tsv` (target, tab, doc per line).
    fn write_seed_docs(&mut self) -> Result<()> {
        const SEED_DOCS: &str = include_str!("../../seed/docs.tsv");
        let mut entries = Vec::new();
        for line in SEED_DOCS.lines().filter(|l| !l.is_empty() && !l.starts_with('#')) {
            let Some((target, doc)) = line.split_once('\t') else {
                return Err(Runtime::catalog(
                    format!("seed doc line without a tab: '{line}'"),
                    "Seed docs",
                ));
            };
            entries.push((target.to_string(), doc.to_string()));
        }
        self.set_entity_docs_atomic(&entries).map(|_| ())
    }
}

#[cfg(test)]
mod standing_tests {
    //! The host is not a value. Sizedness is asked of the TYPE by
    //! autoref specialization: a `Sized` type answers through the by-value
    //! impl, an unsized one only through the reference impl.
    use super::*;
    use std::marker::PhantomData;

    struct Probe<T: ?Sized>(PhantomData<T>);
    trait SizedProbe {
        fn is_sized(&self) -> bool;
    }
    trait UnsizedProbe {
        fn is_sized(&self) -> bool;
    }
    impl<T: Sized> SizedProbe for Probe<T> {
        fn is_sized(&self) -> bool {
            true
        }
    }
    impl<T: ?Sized> UnsizedProbe for &Probe<T> {
        fn is_sized(&self) -> bool {
            false
        }
    }
    macro_rules! is_sized {
        ($t:ty) => {
            (&Probe::<$t>(PhantomData)).is_sized()
        };
    }

    /// The probe tells the two apart, and the host is on the unsized
    /// side: `std::mem::swap::<DelightQLSystem>` does not exist, while a
    /// `ReadySystem` — host and image together — remains a value.
    #[test]
    fn the_host_is_unsized_and_the_ready_system_is_a_value() {
        assert!(is_sized!(u8));
        assert!(!is_sized!([()]));
        assert!(!is_sized!(DelightQLSystem));
        assert!(is_sized!(ReadySystem));
        assert!(is_sized!(Construction));
        assert!(is_sized!(Finalized));
    }
}

// =============================================================================
// The image
// =============================================================================

/// An owned, immutable serialization of a FINALIZED world's SQLite page
/// image, with the closed identity facts needed to instantiate it. Its one
/// constructor is private and called only by `Construction::finalize`
/// after every finalization effect, so no image of an unfinalized catalog
/// can be made. Nothing can write through the buffer.
pub(crate) struct PristineImage {
    pages: Arc<[u8]>,
    facts: WorldFacts,
}

impl PristineImage {
    /// Freeze the world on `conn`: normalize its transient ledgers, read the
    /// identities it bakes in, and copy its page image into an owned buffer.
    /// `serialize` may return SQLite's own buffer without copying; the copy
    /// is what makes the image outlive and stay independent of `conn`.
    fn freeze(conn: &Connection) -> Result<Self> {
        conn.execute_batch(NORMALIZE_TRANSIENT_LEDGERS)
            .map_err(|e| catalog_error("normalize transient ledgers before freezing", e))?;
        let facts = WorldFacts::read(conn)?;
        let pages: Arc<[u8]> = conn
            .serialize(rusqlite::MAIN_DB)
            .map_err(|e| catalog_error("serialize the pristine world", e))?
            .to_vec()
            .into();
        Ok(PristineImage { pages, facts })
    }

    /// Create one fresh, private, writable world from this image and
    /// restore everything the pages cannot carry. Fails closed: a world
    /// that cannot recover the identities its image states is not a world.
    fn instantiate(&self) -> Result<ReadyWorld> {
        let mut connection = Connection::open_in_memory()
            .map_err(|e| catalog_error("open a connection for the pristine world", e))?;
        // `false` = writable. SQLite receives its own private copy of the
        // pages (allocated and freed by SQLite); the template is untouched.
        connection
            .deserialize_read_exact(
                rusqlite::MAIN_DB,
                std::io::Cursor::new(&self.pages[..]),
                self.pages.len(),
                false,
            )
            .map_err(|e| catalog_error("instantiate the pristine world", e))?;
        crate::bootstrap::configure_connection(&connection)?;
        connection
            .execute_batch(RESTAMP_WORLD_CLOCKS)
            .map_err(|e| catalog_error("restamp the instantiated world's clocks", e))?;
        let facts = WorldFacts::read(&connection)?;
        if facts != self.facts {
            return Err(Internal::invariant(
                "system::world",
                "the instantiated world's identities differ from its image's",
            ));
        }
        let guard = BootstrapGuard::seal(&connection)?;
        Ok(ReadyWorld {
            connection,
            guard,
            facts,
        })
    }

    /// TEST-ONLY: an image whose pages are not a database, for proving that
    /// a failed instantiation publishes nothing.
    #[cfg(test)]
    fn corrupt_for_test(&self) -> Self {
        PristineImage {
            pages: Arc::from(&b"not a database"[..]),
            facts: self.facts,
        }
    }

    /// TEST-ONLY: the page bytes, for proving the template does not move.
    #[cfg(test)]
    pub(crate) fn pages_for_test(&self) -> &[u8] {
        &self.pages
    }
}

// =============================================================================
// The ready world
// =============================================================================

/// The indivisible result of instantiation: the restored connection with
/// its policy applied, the sealed guard, and the identities read back out
/// of it and checked against the image's. Consumed whole.
struct ReadyWorld {
    connection: Connection,
    guard: BootstrapGuard,
    facts: WorldFacts,
}

impl ReadyWorld {
    /// TEST-ONLY: the restored connection, for comparing an instance with
    /// the world it was frozen from without publishing it.
    #[cfg(test)]
    fn connection_for_test(&self) -> &Connection {
        &self.connection
    }

    /// Publish this world as `host`'s: the connection, its guard, the
    /// routing map for the primary connection, a fresh schema provider and
    /// the catalog-cartridge mirror, in one act. The only fallible step is
    /// taking the bootstrap lock, and it comes first, so a failure changes
    /// nothing.
    fn install_into(self, host: &mut DelightQLSystem) -> Result<()> {
        let ReadyWorld {
            connection,
            guard,
            facts,
        } = self;
        *host.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock to publish the world",
                format!("Connection was poisoned: {}", e),
            )
        })? = connection;
        host.bootstrap_guard = guard;
        host.connection_map.clear();
        host.connection_map
            .insert(facts.primary_connection_id, Arc::clone(&host.connection));
        host.schema_map.clear();
        host.introspector_map.clear();
        host.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(host.bootstrap_connection.clone()),
        ));
        host.catalog_cartridge_id
            .set(Some(facts.catalog_cartridge_id));
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// A foreign key the bootstrap schema declares: `connection.connection_type`
    /// references `connection_type_enum(id)`; no such variant exists, so the
    /// insert is refused exactly when enforcement is on.
    const VIOLATES_FK: &str =
        "INSERT INTO connection (id, resource_uri, mechanism, connection_type)
         VALUES (9001, 'probe://fk', 'in-process', 424242)";

    fn built() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        conn
    }

    /// The negative control, without which every enforcement assertion is
    /// vacuous: with enforcement OFF this connection ACCEPTS the violating
    /// insert, so the statement really violates a declared key and
    /// enforcement is a connection setting rather than a property of the
    /// schema.
    #[test]
    fn enforcement_can_actually_be_off() {
        let conn = built();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        let accepted = conn.execute(VIOLATES_FK, []);
        assert!(
            accepted.is_ok(),
            "with enforcement off the violating insert was still refused: {accepted:?}"
        );
    }

    /// The configuration judgment turns enforcement back ON from a
    /// connection where it is off — the part a compile-time default cannot
    /// fake.
    #[test]
    fn configure_connection_re_enables_enforcement() {
        let conn = built();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();
        crate::bootstrap::configure_connection(&conn).unwrap();
        assert!(
            conn.execute(VIOLATES_FK, []).is_err(),
            "configure_connection did not re-enable foreign-key enforcement"
        );
    }

    /// A freshly constructed catalog enforces. The pragma no longer lives in
    /// the schema file; this pins that construction still receives it.
    #[test]
    fn a_constructed_catalog_enforces_foreign_keys() {
        let conn = built();
        assert!(conn.execute(VIOLATES_FK, []).is_err());
    }

    /// The census of captured clocks is taken from the schema's own
    /// definitions, not from column names: every `DEFAULT (` whose
    /// expression calls a function is a value the page image would freeze.
    /// Each one must have a per-world law (`RESTAMP_WORLD_CLOCKS` or
    /// `NORMALIZE_TRANSIENT_LEDGERS`); a new one fails here until it does.
    #[test]
    fn captured_defaults_are_exactly_the_ruled_set() {
        let conn = built();
        let mut statement = conn
            .prepare("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND sql IS NOT NULL")
            .unwrap();
        let tables: Vec<(String, String)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let mut captured = Vec::new();
        for (table, sql) in tables {
            for line in sql.lines() {
                let upper = line.to_ascii_uppercase();
                let Some(at) = upper.find("DEFAULT (") else {
                    continue;
                };
                let expression = &line[at + "DEFAULT (".len()..];
                // A parenthesized literal is a constant; a call captures a
                // value at insert time.
                if expression.contains('(') {
                    let column = line.trim().split_whitespace().next().unwrap().to_string();
                    captured.push(format!("{table}.{column}"));
                }
            }
        }
        captured.sort();
        assert_eq!(
            captured,
            vec![
                "activated_entity.activation_time",
                "cartridge.creation_time",
                "compilation.timestamp",
            ],
            "a captured default without a per-world law: give it one in \
             RESTAMP_WORLD_CLOCKS or NORMALIZE_TRANSIENT_LEDGERS"
        );
    }

    /// Bytes that are not a database are refused at instantiation, before
    /// anything is published.
    #[test]
    fn a_corrupt_image_is_refused() {
        let finalized = pristine_world_tests::finalized();
        assert!(finalized.image.corrupt_for_test().instantiate().is_err());
    }

    /// An image states every identity an instance must recover, the
    /// catalog cartridge among them, and an instance recovers exactly
    /// those. There is no test that an unfinalized catalog is refused at
    /// instantiation: no value of `PristineImage` can be made from one.
    #[test]
    fn an_instance_recovers_the_identities_its_image_states() {
        let finalized = pristine_world_tests::finalized();
        let instance = finalized.image.instantiate().unwrap();
        assert_eq!(instance.facts, finalized.image.facts);
        assert_eq!(instance.facts.bootstrap_connection_id, 1);
        assert_eq!(instance.facts.primary_connection_id, 2);
        let cartridge: i32 = instance
            .connection_for_test()
            .query_row(
                "SELECT id FROM cartridge WHERE source_uri = 'catalog://sys::meta'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cartridge, instance.facts.catalog_cartridge_id);
    }

    /// A captured import must be keyed by a load ACTIVATED IN its namespace:
    /// no catalog write can publish a capture under a namespace other than
    /// the load's owner. Taken over a fully instantiated catalog, whose
    /// `activated_entity` rows are the real pairings: a `(namespace, load)`
    /// the catalog activates is accepted; the same load under a DIFFERENT
    /// namespace is refused by the trigger; a NULL load (a facade) is
    /// exempt. This is the publication half of the sealed-load relationship.
    #[test]
    fn a_capture_must_be_keyed_by_a_load_in_its_namespace() {
        let finalized = pristine_world_tests::finalized();
        let instance = finalized.image.instantiate().unwrap();
        let conn = instance.connection_for_test();
        // A real pairing and a second, DIFFERENT namespace that the same
        // load does not activate.
        let (owner_ns, load): (i64, i64) = conn
            .query_row(
                "SELECT namespace_id, cartridge_id FROM activated_entity LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let foreign_ns: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE id <> ?1
                 AND id NOT IN (SELECT namespace_id FROM activated_entity WHERE cartridge_id = ?2)
                 LIMIT 1",
                [owner_ns, load],
                |r| r.get(0),
            )
            .unwrap();

        let owned = conn.execute(
            "INSERT INTO lexical_import (namespace_id, cartridge_id, imported_namespace_id)
             VALUES (?1, ?2, ?1)",
            [owner_ns, load],
        );
        assert!(
            owned.is_ok(),
            "a load activated in the namespace is accepted: {owned:?}"
        );
        let mismatched = conn.execute(
            "INSERT INTO lexical_import (namespace_id, cartridge_id, imported_namespace_id)
             VALUES (?1, ?2, ?1)",
            [foreign_ns, load],
        );
        assert!(
            mismatched.is_err(),
            "a load that activates nothing in the namespace must be refused"
        );
        let facade = conn.execute(
            "INSERT INTO lexical_import (namespace_id, cartridge_id, imported_namespace_id)
             VALUES (?1, NULL, ?1)",
            [foreign_ns],
        );
        assert!(facade.is_ok(), "a facade's NULL load is exempt: {facade:?}");
    }
}

#[cfg(test)]
mod pristine_world_tests {
    //! The behavioral discriminators of the image cut, stated against
    //! semantic rows and behavior rather than private names: a reset world
    //! is a fresh, private, complete instance of the one pristine world,
    //! with its connection policy restored and its identities reconciled.
    use super::super::population::ensure_catalog_initialized;
    use super::*;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn user_connection() -> Arc<Mutex<dyn DatabaseConnection>> {
        Arc::new(Mutex::new(MockDatabaseConnection::new()))
    }

    pub(super) fn fresh_system() -> ReadySystem {
        ReadySystem::new(user_connection(), Box::new(EmptyIntrospector), "sqlite").expect("system")
    }

    /// The construction carrier, before finalization.
    fn construction() -> Construction {
        ReadySystem::construct(user_connection(), Box::new(EmptyIntrospector), "sqlite")
            .expect("construction")
    }

    /// A finalized world with its frozen image, not yet published.
    pub(super) fn finalized() -> Finalized {
        let boot = crate::settings::BootSettings::new()
            .state(crate::settings::BASE_DIRECTORY, None)
            .admit()
            .expect("boot");
        construction().finalize(&boot).expect("finalize")
    }

    /// A foreign key the schema declares: `connection.connection_type`
    /// references `connection_type_enum(id)`; this variant does not exist.
    const VIOLATES_FK: &str =
        "INSERT INTO connection (id, resource_uri, mechanism, connection_type)
         VALUES (9001, 'probe://fk', 'in-process', 424242)";

    /// Every table's rows in a deterministic order, with the two per-world
    /// clocks masked — the ratified clock policy applied.
    fn world_dump(conn: &Connection) -> Vec<String> {
        let mut tables = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .unwrap();
        let names: Vec<String> = tables
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let mut dump = Vec::new();
        for table in names {
            let mut info = conn
                .prepare(&format!("PRAGMA table_info(\"{table}\")"))
                .unwrap();
            let columns: Vec<String> = info
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .filter(|c| c != "creation_time" && c != "activation_time")
                .collect();
            let projection = columns
                .iter()
                .map(|c| format!("quote(\"{c}\")"))
                .collect::<Vec<_>>()
                .join(" || '|' || ");
            let mut rows = conn
                .prepare(&format!("SELECT {projection} FROM \"{table}\" ORDER BY 1"))
                .unwrap();
            for row in rows.query_map([], |r| r.get::<_, String>(0)).unwrap() {
                dump.push(format!("{table}|{}", row.unwrap()));
            }
        }
        dump
    }

    fn sequences(conn: &Connection) -> Vec<(String, i64)> {
        let mut st = conn
            .prepare("SELECT name, seq FROM sqlite_sequence ORDER BY name")
            .unwrap();
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    /// INITIAL READINESS + CANONICAL CONTENT: the first exposed system is
    /// already the complete world — builtins, overlays, seed facts, the
    /// catalog cartridge — and a reset installs the same world.
    #[test]
    fn the_first_world_is_complete_and_a_reset_is_the_same_world() {
        let mut system = fresh_system();
        let first = world_dump(&system.bootstrap_connection.lock().unwrap());
        assert!(first
            .iter()
            .any(|r| r.starts_with("cartridge|") && r.contains("catalog://sys::meta")));
        assert!(first
            .iter()
            .any(|r| r.starts_with("cartridge|") && r.contains("embedded://std::prelude")));
        assert!(
            first.iter().any(|r| r.starts_with("entity_type_enum|")),
            "registries are in the world"
        );
        system.reinit_bootstrap().expect("reset");
        let reset = world_dump(&system.bootstrap_connection.lock().unwrap());
        assert_eq!(first, reset);
    }

    /// ONE IMAGE: the world `new` publishes, the finalized world it was
    /// frozen from, and every reset agree — the transition that proved
    /// finalization produced the one image both roads instantiate.
    #[test]
    fn the_published_world_is_an_instance_of_the_finalized_image() {
        let finalized = finalized();
        let constructed = world_dump(&finalized.host.bootstrap_connection.lock().unwrap());
        let mut system = finalized.publish().expect("publish");
        assert_eq!(
            constructed,
            world_dump(&system.bootstrap_connection.lock().unwrap())
        );
        system.reinit_bootstrap().expect("reset");
        assert_eq!(
            constructed,
            world_dump(&system.bootstrap_connection.lock().unwrap())
        );
    }

    /// CANONICAL CONTENT: the finalized world and an instance of its image
    /// agree table-for-table under the clock policy.
    #[test]
    fn a_constructed_world_and_its_instance_agree_table_for_table() {
        let finalized = finalized();
        let conn = finalized.host.bootstrap_connection.lock().unwrap();
        let constructed = world_dump(&conn);
        let instance = finalized.image.instantiate().expect("instantiate");
        assert_eq!(constructed, world_dump(instance.connection_for_test()));
        assert_eq!(sequences(&conn), sequences(instance.connection_for_test()));
    }

    /// CLOCK POLICY: cartridge and activation clocks describe the instance,
    /// not the template's construction. The template's clocks are forced to
    /// an impossible past so the restamp is attributable.
    #[test]
    fn instantiation_restamps_the_worlds_clocks() {
        let finalized = construction()
            .finalize_tampered(|conn, guard| {
                conn.execute("UPDATE cartridge SET creation_time = 1", [])
                    .unwrap();
                let _window = guard.catalog_window();
                conn.execute("UPDATE activated_entity SET activation_time = 1", [])
                    .unwrap();
            })
            .expect("finalize");
        let instance = finalized.image.instantiate().expect("instantiate");
        let c = instance.connection_for_test();
        assert!(count(c, "SELECT min(creation_time) FROM cartridge") > 1);
        assert!(count(c, "SELECT min(activation_time) FROM activated_entity") > 1);
    }

    /// CLOCK POLICY: the compilation ledgers begin empty in every instance,
    /// with their sequence as a newly constructed world's. The finalized
    /// world is given history before it is frozen, so the normalization is
    /// attributable; and a session's own history does not survive a reset.
    #[test]
    fn transient_ledgers_begin_empty_in_every_instance() {
        const LEDGER: &str =
            "INSERT INTO compilation (dql_input, sql_output) VALUES ('_(1)', 'SELECT 1')";
        let finalized = construction()
            .finalize_tampered(|conn, _| {
                conn.execute(LEDGER, []).unwrap();
                conn.execute(
                    "INSERT INTO stack (compilation_id, function_name, max_depth)
                     VALUES (last_insert_rowid(), 'probe', 1)",
                    [],
                )
                .unwrap();
            })
            .expect("finalize");
        {
            let instance = finalized.image.instantiate().expect("instantiate");
            let c = instance.connection_for_test();
            assert_eq!(count(c, "SELECT count(*) FROM compilation"), 0);
            assert_eq!(count(c, "SELECT count(*) FROM stack"), 0);
            assert!(!sequences(c).iter().any(|(name, _)| name == "compilation"));
        }

        let mut system = fresh_system();
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.execute(LEDGER, []).unwrap();
            assert_eq!(count(&conn, "SELECT count(*) FROM compilation"), 1);
        }
        system.reinit_bootstrap().expect("reset");
        let conn = system.bootstrap_connection.lock().unwrap();
        assert_eq!(count(&conn, "SELECT count(*) FROM compilation"), 0);
        assert!(!sequences(&conn)
            .iter()
            .any(|(name, _)| name == "compilation"));
    }

    /// FOREIGN-KEY ENFORCEMENT on a reset world: the violating insert is
    /// refused, and it is the connection setting that refuses it — with the
    /// setting off, the same connection accepts the row.
    #[test]
    fn a_reset_world_enforces_foreign_keys() {
        let mut system = fresh_system();
        system.reinit_bootstrap().expect("reset");
        let conn = system.bootstrap_connection.lock().unwrap();
        assert!(
            conn.execute(VIOLATES_FK, []).is_err(),
            "a reset world accepted a foreign-key violation"
        );
        conn.execute_batch("PRAGMA foreign_keys = OFF").unwrap();
        assert!(
            conn.execute(VIOLATES_FK, []).is_ok(),
            "the probe does not violate what it claims"
        );
    }

    /// PRIVATE MUTATION: a change to one instance reaches no other instance,
    /// and the template does not move.
    #[test]
    fn an_instance_mutation_is_private_and_the_template_unchanged() {
        let mut system = fresh_system();
        let template_before = system.image_for_test().pages_for_test().to_vec();
        const PROBE: &str = "SELECT count(*) FROM danger WHERE uri = 'probe://private'";
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.execute(
                "INSERT INTO danger (uri, state, cli_overridable, description)
                 VALUES ('probe://private', 'ON', 1, 'one instance only')",
                [],
            )
            .unwrap();
            assert_eq!(count(&conn, PROBE), 1);
        }
        let other = system
            .image_for_test()
            .instantiate()
            .expect("a second instance");
        assert_eq!(count(other.connection_for_test(), PROBE), 0);
        system.reinit_bootstrap().expect("reset");
        assert_eq!(
            count(&system.bootstrap_connection.lock().unwrap(), PROBE),
            0
        );
        assert_eq!(template_before, system.image_for_test().pages_for_test());
    }

    /// SESSION ISOLATION: mounted, asserted, found and armed session state
    /// does not survive a reset; the world after is the world before.
    #[test]
    fn reset_leaves_no_session_facts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.sqlite");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE t(x INTEGER)")
            .unwrap();
        let mut system = fresh_system();
        let pristine = world_dump(&system.bootstrap_connection.lock().unwrap());
        system
            .mount_database(path.to_str().unwrap(), "sessionns")
            .expect("mount");
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.execute(
                "INSERT INTO assertions (body, outcome, run_id) VALUES ('1 = 1', 'PASS', 'r')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO finding (occurred_at, kind, uri, message, provider)
                 VALUES ('now', 'error', 'delightql-error://probe', 'm', 'test')",
                [],
            )
            .unwrap();
            conn.execute("UPDATE danger SET state = 'ON'", []).unwrap();
            assert!(count(&conn, "SELECT count(*) FROM mount") > 0);
            assert_ne!(world_dump(&conn), pristine);
        }
        system.reinit_bootstrap().expect("reset");
        assert_eq!(
            world_dump(&system.bootstrap_connection.lock().unwrap()),
            pristine
        );
    }

    /// IDENTITY RECONCILIATION: the connection rows the image bakes in are
    /// the ones routing uses; nothing is registered a second time, and the
    /// catalog cartridge is reconciled from the image rather than rebuilt.
    #[test]
    fn identities_are_reconciled_not_re_registered() {
        let mut system = fresh_system();
        for _ in 0..3 {
            system.reinit_bootstrap().expect("reset");
        }
        assert!(system.get_connection(2).is_ok(), "primary route");
        let conn = system.bootstrap_connection.lock().unwrap();
        let mut st = conn
            .prepare("SELECT id, resource_uri FROM connection ORDER BY id")
            .unwrap();
        let rows: Vec<(i64, String)> = st
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (1, "session:bootstrap".to_string()),
                (2, "session:primary".to_string())
            ]
        );
        let cached = system
            .catalog_cartridge_id
            .get()
            .expect("the catalog mirror is reconciled at install");
        let cartridges = count(&conn, "SELECT count(*) FROM cartridge");
        let id = ensure_catalog_initialized(&system.catalog_cartridge_id, &conn).expect("catalog");
        assert_eq!(id, cached);
        assert_eq!(count(&conn, "SELECT count(*) FROM cartridge"), cartridges);
        assert_eq!(
            count(
                &conn,
                "SELECT count(*) FROM cartridge WHERE source_uri = 'catalog://sys::meta'"
            ),
            1
        );
    }

    /// REPEATED RESET: consecutive resets neither grow nor drift the world.
    #[test]
    fn repeated_resets_do_not_grow_the_world() {
        let mut system = fresh_system();
        let (first, seq) = {
            let conn = system.bootstrap_connection.lock().unwrap();
            (world_dump(&conn), sequences(&conn))
        };
        for _ in 0..5 {
            system.reinit_bootstrap().expect("reset");
        }
        let conn = system.bootstrap_connection.lock().unwrap();
        assert_eq!(world_dump(&conn), first);
        assert_eq!(sequences(&conn), seq);
    }

    /// FAILURE ATOMICITY: a candidate that cannot be instantiated publishes
    /// nothing — the live world, its seal, its routing and the health latch
    /// are exactly as they were — and a later reset from a sound image
    /// still restores the complete world with its connection policy.
    #[test]
    fn a_failed_instantiation_publishes_nothing() {
        let mut system = fresh_system();
        const PROBE: &str = "SELECT count(*) FROM danger WHERE uri = 'probe://survives'";
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.execute(
                "INSERT INTO danger (uri, state, cli_overridable, description)
                 VALUES ('probe://survives', 'ON', 1, 'the live world')",
                [],
            )
            .unwrap();
        }
        system.corrupt_image_for_test();
        assert!(
            system.reinit_bootstrap().is_err(),
            "a corrupt image cannot reset"
        );
        let conn = system.bootstrap_connection.lock().unwrap();
        assert_eq!(count(&conn, PROBE), 1, "the live world was not replaced");
        assert!(
            conn.execute_batch("DROP TABLE danger").is_err(),
            "the live world is still sealed"
        );
        assert!(
            conn.execute(VIOLATES_FK, []).is_err(),
            "the live world still enforces foreign keys"
        );
        drop(conn);
        assert!(system.get_connection(2).is_ok());
        assert!(system.health_incident().is_none());
    }
}

#[cfg(test)]
mod seed_doc_tests {
    //! The pristine image's entity docs are written once at construction
    //! from `seed/docs.tsv`; every line names an entity the image holds.

    use super::ReadySystem;
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

    #[test]
    fn the_seed_docs_land_in_the_pristine_image() {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("construction writes every seed doc");
    }
}
