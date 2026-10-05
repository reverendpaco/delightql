// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The population act for lazily populated namespaces: embedded autoload
//! modules, and the `sys::meta` catalog with its per-namespace wrappers.

use super::registration::LexicalCapture;
use super::{embedded_module, CatalogSavepoint, DelightQLSystem, LoadPhase, StdlibLoad};
use crate::bootstrap::SourceType;
use crate::diagnostic::Runtime;
use crate::error::{DelightQLError, Result};
use log::debug;
use rusqlite::Connection;
use std::cell::Cell;

/// Embedded DQL source for the sys::meta generator HO view.
/// This is the sole definition of the catalog functor join logic.
const SYS_META_SOURCE: &str = include_str!("../../autoload/sys/meta.dql");

/// Register a thin catalog wrapper view for a namespace in sys::meta.
///
/// Creates an entity like `main::` with definition `sys::meta.generator("main")(*)`
/// so that `main::(*)` resolves through normal HO view expansion.
pub(super) fn register_catalog_wrapper(
    conn: &Connection,
    ns_fq: &str,
    sys_meta_ns_id: i32,
    cartridge_id: i32,
) -> Result<()> {
    let entity_name = format!("{}::", ns_fq);
    // The wrapper is addressed by its entity name (`ns::`); the clause
    // subject is never how it is reached. Exact `_` is reserved deixis and
    // refuses at definition admission, so the stored subject is an ordinary
    // longer-underscore compiler spelling.
    let definition = format!(r#"_wrapper(*) :- sys::meta.generator("{}")(*)"#, ns_fq);

    // Catalog initialization registers wrappers for every namespace already
    // present. Mount paths also call this function after lazy initialization
    // so that later namespaces receive a wrapper. Make that overlap explicitly
    // idempotent: a namespace has exactly one wrapper in the catalog cartridge.
    let already_registered: bool = conn
        .query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE e.name = ?1
                   AND e.cartridge_id = ?2
                   AND ae.namespace_id = ?3
             )",
            rusqlite::params![&entity_name, cartridge_id, sys_meta_ns_id],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to check catalog wrapper '{}': {}", entity_name, e),
                e.to_string(),
            )
        })?;
    if already_registered {
        return Ok(());
    }

    // The wrapper is a compiler-synthesized definition family: an authored
    // KIND (the resolver opens its body), activated in sys::meta like any
    // other family.
    conn.execute(
        "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![&entity_name, 4, cartridge_id], // type 4 = DqlTemporaryViewExpression
    )
    .map_err(|e| {
        Runtime::catalog(
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
        Runtime::catalog(
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
        Runtime::catalog(
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
    // Parse and register the generator HO view via consult_file_inner.
    // Shared DDL front end — the same parsing/cleaning seam every DDL entry
    // point uses (autoload, inline DDL, consult!).
    let consulted = crate::bin_cartridge::prelude::consult::Consulted::read_without_directives(
        SYS_META_SOURCE,
        "sys::meta",
    )?;
    // Embedded system modules have no liminal space: they are created by
    // other means, so their liminal is empty.
    let definitions = consulted.into_definitions();
    let count = definitions.len();
    DelightQLSystem::consult_file_inner(
        bootstrap_conn,
        "embedded://sys::meta",
        "sys::meta",
        definitions,
        count,
        None,
        false,
        &LexicalCapture::none(),
    )?;

    // Create a separate cartridge for the catalog wrapper entities
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (1, ?1, 'catalog://sys::meta', 'sys::meta', 1, 1, 0)",
            rusqlite::params![SourceType::FileBin.as_i32()],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to create catalog wrapper cartridge: {}", e), e.to_string())
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
            Runtime::catalog(
                format!("Failed to query sys::meta namespace: {}", e),
                e.to_string(),
            )
        })?;

    // Register a catalog wrapper for every existing namespace
    let mut stmt = bootstrap_conn
        .prepare("SELECT fq_name FROM namespace ORDER BY id")
        .map_err(|e| Runtime::catalog("Failed to prepare namespace query", e.to_string()))?;
    let ns_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| Runtime::catalog("Failed to query namespaces", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);

    for ns_fq in &ns_names {
        register_catalog_wrapper(bootstrap_conn, ns_fq, sys_meta_ns_id, catalog_cartridge_id)?;
    }

    // Auto-enlist sys::meta into `home` — the interactive scope
    // (enlistment edges are owned by the environment they extend).
    let home_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'home'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query home namespace for enlist: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
             VALUES (?1, ?2)",
            [sys_meta_ns_id, home_ns_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to enlist sys::meta into home: {}", e),
                e.to_string(),
            )
        })?;

    // Auto-enlist `main` into `home`: the
    // interactive session's scope is `home`, and bare table names keep
    // working BECAUSE `main` — the default data namespace — is enlisted
    // into it. The edge direction matters: an inverted home→main edge
    // would pretend the session itself was `main`.
    if let Ok(main_ns_id) = bootstrap_conn.query_row(
        "SELECT id FROM namespace WHERE fq_name = 'main'",
        [],
        |row| row.get::<_, i32>(0),
    ) {
        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                [main_ns_id, home_ns_id],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to enlist main into home: {}", e),
                    e.to_string(),
                )
            })?;
    }

    debug!(
        "register_catalog_views: Registered {} catalog wrappers, enlisted sys::meta + main into home",
        ns_names.len()
    );

    Ok(catalog_cartridge_id)
}

/// Lazily initialize catalog views. Uses Cell for interior mutability so
/// callers holding &self (the population act) can trigger initialization.
pub(super) fn ensure_catalog_initialized(
    catalog_cartridge_id: &Cell<Option<i32>>,
    bootstrap_conn: &Connection,
) -> Result<i32> {
    if let Some(id) = catalog_cartridge_id.get() {
        // Validate before reuse: the Cell can be set inside a mount savepoint that later
        // ROLLS BACK — and SQLite may then REUSE the freed rowid for an
        // unrelated cartridge, so existence of the id alone proves
        // nothing. The row must also carry the catalog cartridge's
        // identity markers (the exact values register_catalog_views
        // stamps). Anything else: drop the cache and re-initialize.
        let is_catalog: bool = bootstrap_conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM cartridge
                 WHERE id = ?1
                   AND source_uri = 'catalog://sys::meta'
                   AND source_ns = 'sys::meta')",
                [id],
                |row| row.get(0),
            )
            .map_err(|e| {
                Runtime::catalog("Failed to validate catalog cartridge cache", e.to_string())
            })?;
        if is_catalog {
            return Ok(id);
        }
        catalog_cartridge_id.set(None);
    }
    let id = register_catalog_views(bootstrap_conn)?;
    catalog_cartridge_id.set(Some(id));
    Ok(id)
}

impl DelightQLSystem {

    /// Load the embedded module the manifest lists under `namespace_fq`: the
    /// manifest's own roads — the universal overlays of the pristine world
    /// and the `autoloads` diagnostic — which name a module, not a mention.
    ///
    /// Returns a [`StdlibLoad`] rather than a bool: the old boolean crushed
    /// "not a stdlib namespace", "already loaded", and "failed to load" into
    /// one `false`, so a broken autoload was indistinguishable from an
    /// absent one and surfaced only as a misleading `Table not found`. The
    /// `Failed` variant carries the parse/consult cause so callers can
    /// surface it.
    pub fn ensure_stdlib_loaded(&self, namespace_fq: &str) -> StdlibLoad {
        let Some(source) = embedded_module(namespace_fq) else {
            return StdlibLoad::NotAModule;
        };
        let bootstrap_conn = match self.bootstrap_connection.lock() {
            Ok(c) => c,
            Err(_) => return StdlibLoad::NotAModule,
        };
        match self.consult_module_on(&bootstrap_conn, namespace_fq, source) {
            ModuleConsult::Loaded => StdlibLoad::Loaded,
            ModuleConsult::AlreadyLoaded => StdlibLoad::AlreadyLoaded,
            ModuleConsult::Failed { phase, error } => StdlibLoad::Failed { phase, error },
        }
    }

    /// Consult one embedded module into its seeded namespace row, once.
    fn consult_module_on(
        &self,
        bootstrap_conn: &Connection,
        namespace_fq: &str,
        source: &'static str,
    ) -> ModuleConsult {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();

        let source_uri = format!("embedded://{}", namespace_fq);
        let already_loaded: bool = bootstrap_conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM cartridge WHERE source_uri = ?1 AND source_ns = ?2",
                rusqlite::params![&source_uri, namespace_fq],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if already_loaded {
            return ModuleConsult::AlreadyLoaded;
        }

        // Consult the module. Route through the shared DDL front end
        // so autoloads parse identically to
        // consult!() files — same whitespace handling, and embedded
        // directives are refused loudly rather than silently misparsed.
        let consulted =
            match crate::bin_cartridge::prelude::consult::Consulted::read_without_directives(
                source,
                &format!("autoload module '{namespace_fq}'"),
            ) {
                Ok(d) => d,
                Err(e) => {
                    report_stdlib_load_failure(namespace_fq, &e);
                    return ModuleConsult::Failed {
                        phase: LoadPhase::Parse,
                        error: e,
                    };
                }
            };

        // Autoload modules have no liminal space: they are created by other
        // means, so their liminal is empty.
        let definitions = consulted.into_definitions();
        let count = definitions.len();
        let path = format!("embedded://{}", namespace_fq);

        let transaction = match CatalogSavepoint::begin(
            bootstrap_conn,
            "dql_stdlib_load",
            "Failed to begin stdlib load transaction",
        ) {
            Ok(transaction) => transaction,
            Err(error) => {
                report_stdlib_load_failure(namespace_fq, &error);
                return ModuleConsult::Failed {
                    phase: LoadPhase::Consult,
                    error,
                };
            }
        };

        match Self::consult_file_inner(
            bootstrap_conn,
            &path,
            namespace_fq,
            definitions,
            count,
            None,
            false,
            &LexicalCapture::none(),
        ) {
            Ok(_) => {
                // Register catalog wrapper for the newly-loaded stdlib namespace
                if let Ok(catalog_id) =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, bootstrap_conn)
                {
                    if let Ok(sys_meta_ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let _ = register_catalog_wrapper(
                            bootstrap_conn,
                            namespace_fq,
                            sys_meta_ns_id,
                            catalog_id,
                        );
                    }
                }
                match transaction.commit("Failed to commit stdlib load transaction") {
                    Ok(()) => ModuleConsult::Loaded,
                    Err(error) => {
                        report_stdlib_load_failure(namespace_fq, &error);
                        ModuleConsult::Failed {
                            phase: LoadPhase::Consult,
                            error,
                        }
                    }
                }
            }
            Err(e) => {
                drop(transaction);
                report_stdlib_load_failure(namespace_fq, &e);
                ModuleConsult::Failed {
                    phase: LoadPhase::Consult,
                    error: e,
                }
            }
        }
    }
}

/// What consulting one embedded module into its row did. A module that is
/// not in the manifest never reaches the consult, so no outcome says so.
enum ModuleConsult {
    Loaded,
    AlreadyLoaded,
    Failed {
        phase: LoadPhase,
        error: DelightQLError,
    },
}

/// Report an autoload module load failure. Always logs at warn; on dev
/// builds it also prints to stderr, because otherwise a broken autoload
/// surfaces only as a misleading `Table not found` with the real cause
/// hidden behind `RUST_LOG=warn`. The build-time `every_stdlib_module_parses`
/// test keeps this path unreachable for shipped modules.
fn report_stdlib_load_failure(namespace_fq: &str, err: &DelightQLError) {
    log::warn!("Failed to load stdlib '{}': {}", namespace_fq, err);
    if cfg!(debug_assertions) {
        eprintln!(
            "delightql: autoload module '{}' failed to load and was skipped:\n  {}",
            namespace_fq, err
        );
    }
}

#[cfg(test)]
mod stdlib_load_tests {
    //! Autoload modules are static `include_str!` content: a shipped binary
    //! must never carry an unparseable one. The population act only
    //! consults the modules a session's selections reach, so a
    //! per-session run can miss a broken module — this
    //! test parses every one, unconditionally, at `cargo test` time.
    //!
    //! Regression guard for the silent-autoload-failure class: a syntax
    //! error in any `autoload/**/*.dql` fails here with the module name and
    //! the parse error, instead of surfacing later as a misleading
    //! `Table not found`.

    #[test]
    fn every_stdlib_module_parses() {
        for (ns, src) in crate::stdlib_manifest::STDLIB_MODULES {
            if let Err(e) = crate::bin_cartridge::prelude::consult::Consulted::read(src) {
                panic!("autoload module '{ns}' failed to parse: {e}");
            }
        }
    }
}
