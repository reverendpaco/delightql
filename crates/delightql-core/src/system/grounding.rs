// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Carrying a library onto a data world: `ground!` publishes a derived world;
//! `imprint!` compiles through an ephemeral one, materializes on the target
//! connection, and archives the source as an inert blueprint.

use super::entity_rows;
use super::population::{ensure_catalog_initialized, register_catalog_wrapper};
use super::{
    ensure_namespace_available, validate_user_namespace_target, CatalogSavepoint, DelightQLSystem,
    ImprintMode, PRIMARY_CONNECTION_ID,
};
use crate::bootstrap::SourceType;
use crate::ddl::lifecycle::{admit_live_kind, Catalog, ImprintSource, LiveNamespace, LiveVerb};
use crate::diagnostic::{Ground, Internal, Runtime};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use delightql_types::DatabaseConnection;
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// RAII rollback for the imprint target transaction.
///
/// Holds a shared reference to the target connection (behind its `MutexGuard`)
/// and, unless `committed` is flipped, issues `ROLLBACK` on drop. This makes
/// every `?` early-return in the drop/create/CTAS sequence undo the whole
/// materialization automatically, so replace-mode cannot leave the old tables
/// destroyed with nothing in their place (pinned: `cli tests/imprint_atomicity.rs`).
/// `execute` takes `&self`, so re-borrowing the guarded connection
/// for the DDL statements alongside this borrow is sound.
struct TargetTxnGuard<'a> {
    conn: &'a dyn DatabaseConnection,
    committed: bool,
}

impl Drop for TargetTxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.execute("ROLLBACK", &[]);
        }
    }
}

/// Next blueprint version under `target_ns_id`: `MAX(existing N) + 1` over
/// `_N_blueprint` children, NOT `COUNT`.
///
/// `COUNT` reuses an N whenever a `_N_blueprint` child was ever removed
/// (`namespace.fq_name` carries no UNIQUE constraint, so the resulting duplicate
/// is silent). Parsing the N and taking `MAX+1` is monotone
/// regardless of gaps. A failed query is a loud error (`?`) — `.unwrap_or(0)`
/// would silently produce a wrong version instead. The `GLOB` is a coarse
/// pre-filter; the Rust parse below is authoritative. Pinned by
/// `imprint_version_tests::next_blueprint_version_is_max_plus_one`.
//
// NOTE: a UNIQUE index on `namespace.fq_name` would make the collision
// impossible at the schema level; not added here.
fn next_blueprint_version(conn: &Connection, target_ns_id: i32) -> Result<i64> {
    let mut stmt = conn
        .prepare("SELECT name FROM namespace WHERE pid = ?1 AND name GLOB '_[0-9]*_blueprint'")
        .map_err(|e| Runtime::catalog("prepare blueprint version scan", e.to_string()))?;
    let rows = stmt
        .query_map([target_ns_id], |r| r.get::<_, String>(0))
        .map_err(|e| Runtime::catalog("scan blueprint versions", e.to_string()))?;
    let mut max_n: Option<i64> = None;
    for name in rows {
        let name = name.map_err(|e| Runtime::catalog("read blueprint name", e.to_string()))?;
        // `name` is `_<N>_blueprint`; take the N between the leading `_` and the
        // `_blueprint` suffix. Anything that doesn't parse is ignored.
        if let Some(inner) = name
            .strip_prefix('_')
            .and_then(|s| s.strip_suffix("_blueprint"))
        {
            if let Ok(parsed) = inner.parse::<i64>() {
                max_n = Some(max_n.map_or(parsed, |m: i64| m.max(parsed)));
            }
        }
    }
    Ok(max_n.map_or(0, |m| m + 1))
}

/// Linear imprint: consume the source lib namespace into a versioned blueprint
/// archive under the target, freeing the source path.
///
/// `imprint!` is linear: after a successful
/// materialization the source is *moved, not destroyed* to
/// `{target}::_{N}_blueprint`. The move is a rename/re-parent of the source
/// namespace (and its `_internal`/descendants), which both vacates the original
/// path — so use-after-imprint errors and the path is free to re-consult
/// (delete-and-reuse) — and creates the archive. The archive is visible
/// (a catalog wrapper is registered for it) but inert (`kind='blueprint'`,
/// enlistment removed). Returns the blueprint fq_name.
///
/// The source arrives as an [`ImprintSource`] and the catalog written is the
/// one it was judged in: only a judged live library can be consumed, so an
/// archive can never be re-archived under a new target by an entrance that
/// skipped the judgment or judged it elsewhere.
fn consume_source_to_blueprint(
    source: &ImprintSource<'_>,
    target_ns: &str,
    target_ns_id: i32,
    sys_meta_ns_id: i32,
    catalog_id: i32,
) -> Result<String> {
    let conn: &Connection = source.catalog();
    let source_ns = source.fq();
    let source_ns_id = source.id();
    // Version N = MAX(existing N)+1 over `_N_blueprint` children (loud on
    // query failure). See `next_blueprint_version` for why not COUNT.
    let n = next_blueprint_version(conn, target_ns_id)?;
    let bp_name = format!("_{}_blueprint", n);
    let bp_fq = format!("{}::{}", target_ns, bp_name);

    // Descendants (e.g. `_internal`), captured before renaming so their
    // fq_names, cartridges and catalog wrappers can be rewritten. A
    // descendant left behind would stay live at its old path, and
    // re-consulting there would mint a duplicate fq_name.
    let descendants: Vec<(i64, String)> = crate::namespace::Subtree::of(conn, source_ns)?
        .ok_or_else(|| {
            Internal::invariant(
                "consume_source_to_blueprint",
                format!("the judged imprint source '{source_ns}' has no namespace row"),
            )
        })?
        .descendants()
        .iter()
        .map(|node| (node.id(), node.fq().to_string()))
        .collect();

    // Drop a namespace's sys::meta catalog wrapper (entity named `{fq}::`).
    // `?`-loud, not `let _ =`: consume runs inside imprint's bootstrap txn,
    // which rolls back cleanly on any error, so a failed wrapper cleanup
    // aborts the whole catalog update instead of leaving a half-renamed
    // blueprint behind — `let _ =` here would swallow that failure and let
    // the half-rename stand.
    let drop_wrapper = |wrapper_name: &str| -> Result<()> {
        let wrappers: Vec<i64> = conn
            .prepare("SELECT id FROM entity WHERE name = ?1")
            .and_then(|mut statement| {
                statement
                    .query_map([wrapper_name], |row| row.get(0))?
                    .collect::<rusqlite::Result<Vec<i64>>>()
            })
            .map_err(|e| Runtime::catalog("find a namespace's catalog wrapper", e.to_string()))?;
        for wrapper in wrappers {
            entity_rows::retire_entity(conn, wrapper)?;
        }
        Ok(())
    };

    for (id, old_fq) in &descendants {
        let new_fq = crate::namespace::rerooted(old_fq, source_ns, &bp_fq).ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: "rename descendant ns".to_string(),
                details: format!(
                    "descendant '{}' is not under source '{}'",
                    old_fq, source_ns
                ),
            })
        })?;
        conn.execute(
            "UPDATE namespace SET fq_name = ?1 WHERE id = ?2",
            rusqlite::params![new_fq, id],
        )
        .map_err(|e| Runtime::catalog("rename descendant ns", e.to_string()))?;
        conn.execute(
            "UPDATE cartridge SET source_ns = ?1 WHERE source_ns = ?2",
            rusqlite::params![new_fq, old_fq],
        )
        .map_err(|e| Runtime::catalog("move descendant cartridge", e.to_string()))?;
        drop_wrapper(&format!("{}::", old_fq))?;
    }

    // Root: rename, re-parent under target, mark inert.
    conn.execute(
        "UPDATE namespace SET name = ?1, fq_name = ?2, pid = ?3, kind = 'blueprint' WHERE id = ?4",
        rusqlite::params![bp_name, bp_fq, target_ns_id, source_ns_id],
    )
    .map_err(|e| Runtime::catalog("rename source ns to blueprint", e.to_string()))?;
    conn.execute(
        "UPDATE cartridge SET source_ns = ?1 WHERE source_ns = ?2",
        rusqlite::params![bp_fq, source_ns],
    )
    .map_err(|e| Runtime::catalog("move source cartridge", e.to_string()))?;
    drop_wrapper(&format!("{}::", source_ns))?;

    // Remove all enlistment of the consumed namespaces (root + descendants), in
    // BOTH directions, and clean enlisted_entity too — mirroring unmount (:4188).
    // enlist! writes the enlisted ns as `from_namespace_id` (:3665) and the
    // resolver serves it via `from_namespace_id` (resolution/registry.rs:597), so
    // `WHERE to_namespace_id = ?` alone deletes nothing: an enlisted source's
    // archived rules would stay resolvable UNQUALIFIED after imprint.
    // Pinned by companion_linear--68.
    let mut consumed_ns_ids: Vec<i64> = descendants.iter().map(|(id, _)| *id).collect();
    consumed_ns_ids.push(i64::from(source_ns_id));
    for ns_id in &consumed_ns_ids {
        conn.execute(
            "DELETE FROM enlisted_entity WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [ns_id],
        )
        .map_err(|e| Runtime::catalog("delist consumed entity", e.to_string()))?;
        conn.execute(
            "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [ns_id],
        )
        .map_err(|e| Runtime::catalog("delist consumed ns", e.to_string()))?;
    }

    // D2: register a catalog wrapper for the blueprint so it is visible.
    register_catalog_wrapper(conn, &bp_fq, sys_meta_ns_id, catalog_id)?;

    Ok(bp_fq)
}

/// THE IMPRINT'S DERIVED WORLD, standing in the catalog only while the
/// imprint compiles its entities. It is derived under a savepoint that is
/// only ever rolled back — on success, on a refusal, and while unwinding —
/// so no part of it is published. The root is a top-level `_` namespace,
/// which no user road can create.
struct EphemeralGrounding<'s> {
    system: &'s DelightQLSystem,
    root: String,
    open: bool,
}

const EPHEMERAL_GROUNDING_DISCARD: &str =
    "ROLLBACK TO dql_imprint_world; RELEASE dql_imprint_world;";

impl<'s> EphemeralGrounding<'s> {
    /// Derive and close the grounded world of `source_id` on `target_id`.
    fn derive(
        system: &'s DelightQLSystem,
        source_id: i32,
        target_id: i32,
        target_fq: &str,
    ) -> Result<Self> {
        let conn = Catalog::open(
            system,
            "Failed to acquire bootstrap lock for the imprint's world",
        )?;
        conn.execute_batch("SAVEPOINT dql_imprint_world")
            .map_err(|e| Runtime::catalog("Failed to open the imprint's world", e.to_string()))?;
        let root = format!("_imprint_{source_id}");
        let derived = (|| -> Result<()> {
            conn.execute(
                "INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance)
                 VALUES (?1, NULL, ?1, ?2, 'grounded', 'ground')",
                rusqlite::params![&root, target_fq],
            )
            .map_err(|e| Runtime::catalog("Failed to root the imprint's world", e.to_string()))?;
            let root_id = conn.last_insert_rowid();
            crate::defuse::grounded_world::DerivedWorld::derive(
                &conn,
                root_id,
                source_id as i64,
                target_id as i64,
            )?
            .close(&conn)
        })();
        match derived {
            Ok(()) => Ok(EphemeralGrounding {
                system,
                root,
                open: true,
            }),
            Err(error) => {
                let _ = conn.execute_batch(EPHEMERAL_GROUNDING_DISCARD);
                Err(error)
            }
        }
    }

    fn root(&self) -> &str {
        &self.root
    }

    /// Discard the world before anything else takes the catalog.
    fn discard(mut self) -> Result<()> {
        self.open = false;
        let conn = self
            .system
            .lock_bootstrap("Failed to acquire bootstrap lock to discard the imprint's world")?;
        conn.execute_batch(EPHEMERAL_GROUNDING_DISCARD)
            .map_err(|e| Runtime::catalog("Failed to discard the imprint's world", e.to_string()))
    }
}

impl Drop for EphemeralGrounding<'_> {
    fn drop(&mut self) {
        if self.open {
            if let Ok(conn) = self.system.lock_bootstrap("discard the imprint's world") {
                let _ = conn.execute_batch(EPHEMERAL_GROUNDING_DISCARD);
            }
        }
    }
}

/// The prompt spelling of one imprint entity's rule, `<root>.<entity>(*)`,
/// the entity stropped when it was authored stropped or is not a plain name.
fn imprint_entity_reference(root: &str, entity: &str, stropped: bool) -> String {
    let plain = entity
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && entity
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain && !stropped {
        format!("{root}.{entity}(*)")
    } else {
        format!("{root}.`{entity}`(*)")
    }
}

/// Imprint entities that read one another in a cycle have no creation order.
fn imprint_cycle(entities: &[String]) -> DelightQLError {
    DelightQLError::from(Runtime::General {
        message: format!(
            "imprint!() entities {} read one another, so none can be created before the \
             others — an imprinted entity reads the objects its siblings become",
            entities
                .iter()
                .map(|entity| format!("'{entity}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        details: "Imprint entities read one another".to_string(),
    })
}

/// Wrap an identifier in double quotes, doubling any internal `"` per SQL's
/// quoted-identifier escaping. Used at every imprint DDL build site that
/// interpolates a schema alias or entity name (the qualified-name builder,
/// CTAS/VIEW CREATE, the INSERT target, replace-mode DROP, the clash pre-flight
/// `sqlite_master`/`sqlite_temp_master`, and the `table_info`/`foreign_key_check`
/// PRAGMAs). The schema alias is the load-bearing case: it comes from a mount
/// file path / ATTACH alias, not a DQL identifier, so it cannot be validated
/// away. Entity names are additionally forbidden a `"` at manifest-read
/// (`manifest::validate_entity_name`), because the declared-table branch routes
/// its CREATE through the DDL generator, which this helper cannot reach.
/// Pinned by `system::imprint_helper_tests::quote_ident_doubles_internal_quote`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build the imprint clash-probe query for one entity name. `master` is the
/// (possibly schema-qualified) `sqlite_master` relation; `name_lit` is the
/// entity name already escaped for a single-quoted SQL string literal. The
/// query UNIONs `sqlite_master` with the connection-local, always-unqualified
/// `sqlite_temp_master` so a temp object of the same name is not missed by the
/// strict-clash / replace-drop pre-flight. Pinned by
/// `system::imprint_helper_tests::clash_probe_sees_temp_object`.
fn imprint_clash_probe_sql(master: &str, name_lit: &str) -> String {
    format!(
        "SELECT type FROM {m} WHERE name = '{n}' \
         UNION ALL SELECT type FROM sqlite_temp_master WHERE name = '{n}'",
        m = master,
        n = name_lit
    )
}

impl DelightQLSystem {
    /// The rules a live library's `_internal` namespace declares, by name,
    /// or `None` when there is no such library or it declares no
    /// `_internal` namespace. An inert library refuses: an archive's
    /// manifest is not read.
    pub(crate) fn companion_rules_of(&self, library_fq: &str) -> Result<Option<Vec<String>>> {
        let conn = Catalog::open(self, "Failed to read a library's companion rules")?;
        if LiveNamespace::admit(&conn, library_fq)?.is_none() {
            return Ok(None);
        }
        let internal_fq = format!("{library_fq}::_internal");
        let internal_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [&internal_fq],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to look up _internal namespace for '{library_fq}'"),
                    e.to_string(),
                )
            })?;
        let Some(internal_id) = internal_id else {
            return Ok(None);
        };
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT e.name FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1",
            )
            .map_err(|e| Runtime::catalog("Failed to list companion rules", e.to_string()))?;
        let names = stmt
            .query_map([internal_id], |row| row.get::<_, String>(0))
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
            .map_err(|e| Runtime::catalog("Failed to list companion rules", e.to_string()))?;
        Ok(Some(names))
    }

    /// A read the new middle compiles and runs on its connection, answered
    /// with its published heading beside its rows. The caller holds no
    /// catalog lock: compilation takes it.
    pub(crate) fn query_in_system(
        &self,
        source: &str,
    ) -> Result<crate::ddl::manifest::CompanionAnswer> {
        let (heading, rows) = crate::pipeline::middle::api::query_rows(self, source)?;
        Ok(crate::ddl::manifest::CompanionAnswer { heading, rows })
    }

    /// Ground a lib namespace into a new namespace, binding it to a data namespace
    ///
    /// Derives the reachable lexical definition closure of `lib_ns` — its
    /// families and, as derivatives under the new namespace, every library
    /// it reaches — bound to `data_ns`, and admits every reference of every
    /// derivative before anything is published (see
    /// `defuse::grounded_world`). The new namespace has `default_data_ns`
    /// set so its bodies' data holes read `data_ns`.
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
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: `new_ns_name` is the
        // USER-TYPED creation target. (`data_ns`/`lib_ns` must already exist, so
        // they are validated by lookup below, not by this creation guard.)
        validate_user_namespace_target(new_ns_name)?;

        // The library's manifest, queried and judged before the catalog is
        // locked: each companion query takes the lock itself.
        let lib_manifest = crate::ddl::manifest::ManifestRows::read(self, lib_ns)?;
        // Each manifest entity's TEMP table, compiled by the new middle where
        // the companion rules stand, before the catalog is locked: the
        // compilation reads the catalog itself. A refusal is spent where the
        // entity is created.
        let mut declared: HashMap<String, Result<Option<crate::ddl_pipeline::ManifestCreateResult>>> = HashMap::new();
        if let Some(companions) = &lib_manifest {
            for entity_name in companions.schema_entities()? {
                let table = crate::ddl_pipeline::create_temp_table_from_manifest(self, companions, &entity_name);
                declared.insert(entity_name, table);
            }
        }
        let mut declared_table = |entity_name: &str| declared.remove(entity_name).unwrap_or(Ok(None));

        // The catalog this grounding is judged and written in: locked under
        // a shared borrow of the system for the operation's extent.
        let bootstrap_conn =
            Catalog::open(self, "Failed to acquire bootstrap database lock for ground")?;

        // 1–2. Both namespaces must be LIVE: present, and neither an imprint
        // archive nor nested in one. Grounding animates lib_ns's rules
        // against data_ns's data; an archived side would resurrect the inert
        // archive — rules going live from lib_ns, or archived data read from
        // data_ns. (new_ns_name cannot be a blueprint:
        // ensure_namespace_available below refuses any existing name, and a
        // blueprint fq always already exists.)
        let data_live = LiveNamespace::admit(&bootstrap_conn, data_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "Data namespace '{}' not found. Mount it first with mount!().",
                    data_ns
                ),
                details: "Namespace not found".to_string(),
            })
        })?;
        let data_ns_id: i32 = data_live.id();
        let lib_live = LiveNamespace::admit(&bootstrap_conn, lib_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "Library namespace '{}' not found. Consult it first with consult!().",
                    lib_ns
                ),
                details: "Namespace not found".to_string(),
            })
        })?;
        let lib_ns_id: i32 = lib_live.id();

        // 3. Validate new_ns_name does NOT exist
        ensure_namespace_available(&bootstrap_conn, new_ns_name)?;

        // 4. Retrieve all entities from lib_ns
        let entities: Vec<(i32, String, bool, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.name_stropped, e.type, e.doc
                     FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE n.fq_name = ?1",
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to query lib namespace entities", e.to_string())
                })?;

            let rows = match stmt.query_map([lib_ns], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            }) {
                Ok(r) => r,
                Err(e) => {
                    return Err(Runtime::catalog(
                        "Failed to query lib namespace entities",
                        e.to_string(),
                    ));
                }
            };
            rows.flatten().collect()
        };

        // 4b. Discover manifest-only entities from _internal (if lib_ns has
        // none of its own).
        let manifest_entity_names: Vec<String> = if entities.is_empty() {
            if let Some(companions) = &lib_manifest {
                companions.schema_entities()?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // 5. NO INTERSECTION: a name defined by BOTH the library and the
        //    data namespace would make every use of that name two-headed,
        //    so grounding refuses it by name and creates nothing. This is a
        //    namespace-level law over the two name sets; it classifies no
        //    reference — the lexical-link / data-hole judgment is the
        //    authority's, applied to the derived world below.
        for (_, entity_name, _entity_stropped, _entity_type, _doc) in &entities {
            let intersects: bool = bootstrap_conn
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM entity e
                        JOIN activated_entity ae ON ae.entity_id = e.id
                        WHERE ae.namespace_id = ?1 AND e.name = ?2 COLLATE NOCASE
                    )",
                    rusqlite::params![data_ns_id, entity_name],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            if intersects {
                return Err(DelightQLError::from(Ground::NameIntersection {
                    message: format!(
                        "ground!() refuses: '{entity_name}' is defined by BOTH the \
                         library '{lib_ns}' and the data namespace '{data_ns}'. A \
                         shared name makes every use of it ambiguous — grounding is \
                         refused whole and nothing is created (No intersection)."
                    ),
                }));
            }
        }
        // THE DERIVATION IS ONE TRANSACTION: the namespace, its lexical
        // graph, its families, and the admission judgment land together or
        // not at all — a refusal below rolls the derivation back whole.
        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_ground_namespace",
            "Failed to begin ground transaction",
        )?;

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
                    Runtime::catalog("Failed to create grounded namespace", e.to_string())
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // 7. THE DERIVED WORLD: the root's families, the library's declared
        // lexical graph with every derivable target rewired to its own
        // derivative, and every reachable dependency derived the same way
        // under this one data world — the closure the catalog records,
        // one `grounding` row per derivative.
        let world = crate::defuse::grounded_world::DerivedWorld::derive(
            &bootstrap_conn,
            new_ns_id as i64,
            lib_ns_id as i64,
            data_ns_id as i64,
        )?;
        // 8. The root's manifest (`_internal`) companions: a TEMP table
        // for each derived family the manifest describes.
        let mut count = world.root_families();
        for (_, entity_name, _, _, _) in &entities {
            // If entity has manifest data in _internal, create TEMP table from it
            if lib_manifest.is_some() {
                if let Some(result) = declared_table(entity_name)? {
                    bootstrap_conn
                        .execute_batch(&result.create_sql)
                        .map_err(|e| {
                            Runtime::catalog(
                                format!(
                                    "Failed to CREATE TEMP TABLE for '{}': {}",
                                    entity_name, result.create_sql
                                ),
                                e.to_string(),
                            )
                        })?;
                }
            }
        }

        // 8b. Create manifest-only entities (discovered from _internal, no entity in lib_ns)
        if lib_manifest.is_some() && !manifest_entity_names.is_empty() {
            // They register under the root's derivation cartridge, minted
            // here when the root derived no family of its own — a
            // cartridge exists only where entities stand under it.
            let cartridge_id = match world.root_cartridge() {
                Some(cartridge_id) => cartridge_id,
                None => crate::defuse::grounded_world::derivation_cartridge(
                    &bootstrap_conn,
                    lib_ns,
                    data_ns,
                )?,
            };
            for entity_name in &manifest_entity_names {
                let result = match declared_table(entity_name)? {
                    Some(r) => r,
                    None => continue,
                };
                let crate::ddl_pipeline::ManifestCreateResult {
                    create_sql,
                    schema_rows,
                } = result;
                bootstrap_conn.execute_batch(&create_sql).map_err(|e| {
                    Runtime::catalog(
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
                            EntityType::DbTemporaryTable.as_i32(),
                            cartridge_id,
                            format!("Grounded from {} manifest", lib_ns),
                        ],
                    )
                    .map_err(|e| {
                        Runtime::catalog(format!("Failed to create grounded entity '{}'", entity_name), e.to_string())
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
                            Runtime::catalog(format!("Failed to register attribute '{}' for '{}'", sr.name, entity_name), e.to_string())
                        })?;
                }

                // Activate entity in grounded namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![new_entity_id, new_ns_id, cartridge_id],
                    )
                    .map_err(|e| {
                        Runtime::catalog(format!("Failed to activate grounded entity '{}'", entity_name), e.to_string())
                    })?;

                count += 1;
            }
        }

        // A world that derives no family anywhere — not in the root, not in
        // any dependency, not from a manifest — has nothing to ground; a
        // pure facade over derivable children is not empty.
        if world.families() == 0 && manifest_entity_names.is_empty() {
            return Err(DelightQLError::from(Runtime::General {
                message: format!("Library namespace '{}' has no entities to ground", lib_ns),
                details: "Empty namespace".to_string(),
            }));
        }

        // 9. ADMISSION: every reference every derivative recorded is judged
        // by the one lexical-link / data-hole judgment body opening uses,
        // under the derivative's own reach and bound to the data namespace;
        // a qualified reference reaching a derivable namespace derives it.
        // A refusal rolls the derivation back whole.
        world.admit(&bootstrap_conn)?;

        transaction.commit("Failed to commit ground transaction")?;
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
    /// [`ImprintMode::Strict`] (imprint!): a pre-flight clash on any target
    /// object fails the whole operation before anything is created.
    /// [`ImprintMode::Replace`] (imprint_replace!): each clashing target object
    /// is dropped first, then recreated. Either way the check/drop happens up
    /// front, atomically.
    pub fn imprint_namespace(
        &mut self,
        source_ns: &str,
        target_ns: &str,
        mode: ImprintMode,
    ) -> Result<Vec<(String, String, String)>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let replace = matches!(mode, ImprintMode::Replace);
        // --- Phase 0: judge, under the catalog lock ---
        // The lock is released before Phase 1 reads the manifest and
        // compiles (both lock the bootstrap store internally), so the proofs
        // judged here die with it; Phase 2 judges the source again under the
        // lock that consumes it.
        let bootstrap_conn = Catalog::open(self, "Failed to acquire bootstrap lock for imprint")?;

        // 1. The source: a live library — present, not an imprint archive
        // nor nested in one, lib/scratch kind, not borrowed by a grounding.
        // The proof borrows the lock and does not outlive it.
        let source = ImprintSource::admit(&bootstrap_conn, source_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "Source namespace '{}' not found. Consult it first with consult!().",
                    source_ns
                ),
                details: "Namespace not found".to_string(),
            })
        })?;
        let source_ns_id = source.id();

        // 2. The target: a live data namespace. A mount that an earlier
        // consume relocated under an archive is inert with it — not a place
        // new objects may land.
        let target = LiveNamespace::admit(&bootstrap_conn, target_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "Target namespace '{}' not found. Mount it first with mount!().",
                    target_ns
                ),
                details: "Namespace not found".to_string(),
            })
        })?;
        admit_live_kind(LiveVerb::ImprintTarget, &target)?;
        let target_ns_id = target.id();

        // 3. WHERE THE IMPRINT CREATES: the target's backing, judged by the
        // one creation-target judgment from the namespace's own row — its
        // connection and the schema its durable objects live in. There is
        // no connection fallback and no alias recovered from the engine.
        let target_data = crate::creation_target::DataTarget::read(
            &*bootstrap_conn,
            crate::definition_catalog::NamespaceKey::Id(i64::from(target_ns_id)),
            &format!("imprint!({source_ns}, {target_ns})"),
        )?;
        let connection_id = target_data.connection_id();
        let target_read_schema = target_data.read_schema().map(str::to_string);
        let target_schema_alias: Option<String> = match target_data.durable() {
            crate::creation_target::DurablePlacement::Schema(schema) => Some(schema.clone()),
            crate::creation_target::DurablePlacement::EngineDefault => None,
        };
        let target_conn = if connection_id == PRIMARY_CONNECTION_ID {
            Arc::clone(&self.connection)
        } else {
            self.get_connection(connection_id)?
        };

        // 5. The source's own entities, by name: a listed entity that names
        // one is populated from it (a table) or is it (a view). Whether the
        // name was authored stropped decides how the compile below spells it.
        let source_rules: HashMap<String, bool> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.name, e.name_stropped FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     WHERE ae.namespace_id = ?1",
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to list the source's rules", e.to_string())
                })?;
            let rows = stmt
                .query_map([source_ns_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
                })
                .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
                .map_err(|e| {
                    Runtime::catalog("Failed to list the source's rules", e.to_string())
                })?;
            rows.into_iter().collect()
        };

        drop(bootstrap_conn);

        // --- Phase 1: read and compile, with no catalog lock held ---
        // The manifest is queried through the whole system and every entity
        // is compiled before the target is touched; each query and compile
        // takes the catalog lock itself.
        use crate::ddl::manifest;

        let source_manifest = manifest::ManifestRows::read(self, source_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "imprint!() source '{}' has no schema definitions to \
                         materialize.",
                    source_ns
                ),
                details: "No _internal namespace".to_string(),
            })
        })?;

        // The entities: the `imprinting` rows, or, when there are none, every
        // entity the `schema` rows describe.
        struct ManifestData {
            name: String,
            materialization: manifest::Materialization,
            extent: manifest::Extent,
            schema_rows: Vec<manifest::SchemaRow>,
            constraint_rows: Vec<manifest::ConstraintRow>,
            default_rows: Vec<manifest::DefaultRow>,
            has_rule: bool,
        }

        let entity_todos: Vec<(String, manifest::Materialization, manifest::Extent)> =
            if !source_manifest.imprinting().is_empty() {
                source_manifest
                    .imprinting()
                    .iter()
                    .map(|row| (row.entity.clone(), row.materialization, row.extent))
                    .collect()
            } else {
                source_manifest
                    .schema_entities()?
                    .into_iter()
                    .map(|name| {
                        (
                            name,
                            manifest::Materialization::Table,
                            manifest::Extent::Permanent,
                        )
                    })
                    .collect()
            };

        if entity_todos.is_empty() {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "imprint!() source '{}' has no manifest entities \
                     (no schema() or imprinting() definitions in _internal)",
                    source_ns
                ),
                details: "No manifest entities".to_string(),
            }));
        }

        let manifest_items: Vec<ManifestData> = entity_todos
            .into_iter()
            .map(|(name, materialization, extent)| ManifestData {
                schema_rows: source_manifest.schema(&name),
                constraint_rows: source_manifest.constraints(&name),
                default_rows: source_manifest.defaults(&name),
                has_rule: source_rules.contains_key(&name),
                name,
                materialization,
                extent,
            })
            .collect();

        for item in &manifest_items {
            let temp = item.extent == manifest::Extent::Temporary;
            let entity_name = &item.name;
            // TEMP extent + a mounted/aliased target is invalid in SQLite:
            // `CREATE TEMP TABLE "alias"."x"` fails with "temporary table name
            // must be unqualified". Refuse cleanly at prepare time (before any
            // mutation) rather than letting it surface as a raw exec error
            // mid-imprint. Pinned by
            // system::imprint_helper_tests + companion_linear--77.
            if temp && target_schema_alias.is_some() {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "imprint!() entity '{}' is temporary but the target namespace is a \
                         mounted (aliased) database — SQLite requires a temporary table/view \
                         name to be unqualified, so it cannot be created in an attached schema. \
                         Imprint it as permanent, or imprint into the primary (unmounted) target. \
                         (A future fix can create it in the mounted connection's own temp schema.)",
                        entity_name
                    ),
                    details: "Temporary imprint into a mounted target".to_string(),
                }));
            }
        }

        // THE IMPRINT'S GROUNDED WORLD: the blueprint grounded on the target
        // as it will be — derived in the catalog, compiled from, and
        // discarded. Every declared table is declared up front from the
        // correspondence that generates its `CREATE`; a view or a bare-rule
        // table is declared from its compiled rule.
        let derived = EphemeralGrounding::derive(self, source_ns_id, target_ns_id, target_ns)?;
        let world = std::rc::Rc::new(crate::pipeline::middle::api::ImprintWorld::new(
            derived.root().to_string(),
            target_ns.to_string(),
            connection_id,
            target_read_schema,
            "main".to_string(),
            manifest_items.iter().map(|item| {
                (
                    item.name.clone(),
                    item.materialization == manifest::Materialization::View,
                )
            }),
        ));
        for item in &manifest_items {
            if item.materialization == manifest::Materialization::Table
                && !item.schema_rows.is_empty()
            {
                let correspondence = crate::ddl::correspondence::Correspondence::decide(
                    &item.name,
                    item.schema_rows.clone(),
                    None,
                )?;
                world.declare(
                    &item.name,
                    crate::pipeline::middle::api::ImprintDeclaration::of_columns(
                        correspondence
                            .physical()
                            .iter()
                            .map(|row| (row.name.clone(), Some(row.col_type.clone())))
                            .collect(),
                    ),
                );
            }
        }

        // Every entity's rule, compiled WHOLE as `<root>.<entity>(*)` in the
        // imprint's world. A compile that reads an entity not yet declared
        // stops; that entity is compiled first.
        let mut bodies: HashMap<String, (String, Vec<Option<String>>)> = HashMap::new();
        for item in &manifest_items {
            if !item.has_rule || bodies.contains_key(&item.name) {
                continue;
            }
            let mut stack = vec![item.name.clone()];
            while let Some(entity) = stack.last().cloned() {
                world.begin(&entity);
                let spelled = imprint_entity_reference(
                    world.root(),
                    &entity,
                    source_rules.get(&entity).copied().unwrap_or(false),
                );
                match crate::pipeline::middle::api::imprint_body(self, &spelled, std::rc::Rc::clone(&world)) {
                    Ok(body) => {
                        stack.pop();
                        if !world.is_declared(&entity) {
                            world.declare(
                                &entity,
                                crate::pipeline::middle::api::ImprintDeclaration::of_heading(&body.1),
                            );
                        }
                        bodies.insert(entity, body);
                    }
                    Err(error) => {
                        let Some(read) = world.take_pending() else {
                            return Err(error);
                        };
                        if let Some(at) = stack.iter().position(|pending| *pending == read) {
                            return Err(imprint_cycle(&stack[at..]));
                        }
                        if !source_rules.contains_key(&read) {
                            return Err(error);
                        }
                        stack.push(read);
                    }
                }
            }
        }

        derived.discard()?;

        // The creation order: each entity after every entity its rule reads.
        let creation = {
            let mut order: Vec<usize> = Vec::with_capacity(manifest_items.len());
            let mut placed: HashSet<&str> = HashSet::new();
            while order.len() < manifest_items.len() {
                let next = manifest_items.iter().enumerate().find(|(at, item)| {
                    !order.contains(at)
                        && world
                            .reads_of(&item.name)
                            .iter()
                            .all(|read| placed.contains(read.as_str()))
                });
                let Some((at, item)) = next else {
                    // Every unplaced entity reads an unplaced one, so walking
                    // those reads from any of them must revisit an entity:
                    // the walk from its first visit is the cycle.
                    let mut walk: Vec<String> = Vec::new();
                    let mut current = manifest_items
                        .iter()
                        .enumerate()
                        .find(|(at, _)| !order.contains(at))
                        .map(|(_, item)| item.name.clone())
                        .unwrap_or_default();
                    while !walk.contains(&current) {
                        walk.push(current.clone());
                        current = world
                            .reads_of(&current)
                            .into_iter()
                            .find(|read| !placed.contains(read.as_str()))
                            .unwrap_or_default();
                    }
                    let from = walk
                        .iter()
                        .position(|entity| *entity == current)
                        .unwrap_or(0);
                    return Err(imprint_cycle(&walk[from..]));
                };
                order.push(at);
                placed.insert(&item.name);
            }
            order
        };

        // How each prepared entity is materialized. A single enum instead of
        // shadow flags (a `materialization` string + a boolean CTAS flag + an
        // optional insert SQL + `effective_schema`-emptiness-as-type-tag)
        // makes the three variants exhaustive and the
        // discriminator un-driftable: a new materialization kind is a new
        // enum variant, never another boolean. Payload carries exactly what the catalog
        // pass needs per variant — DeclaredTable knows its columns up front (no
        // PRAGMA readback) and may carry an INSERT…SELECT; View/CtasTable read
        // their columns back from the committed object.
        enum Materialized {
            /// `CREATE VIEW … AS <select>`. entity_type = DbPermanentView; attrs read back.
            View,
            /// `CREATE TABLE … AS SELECT`. entity_type = DbPermanentTable; attrs read back.
            CtasTable,
            /// Typed `CREATE TABLE` from a schema()/constraints()/defaults()
            /// declaration (always non-empty schema), optionally populated by a
            /// trailing INSERT…SELECT when the entity carries a rule body.
            DeclaredTable {
                schema: Vec<manifest::SchemaRow>,
                insert: Option<String>,
            },
        }

        struct PreparedEntity {
            name: String,
            qualified_create: String,
            materialized: Materialized,
        }

        let mut prepared: Vec<PreparedEntity> = Vec::new();

        for item in &manifest_items {
            let entity_name = &item.name;

            let temp = item.extent == manifest::Extent::Temporary;

            // The compiled rule — used by view, CTAS, and the declared-path
            // INSERT. It carries its published heading, which is what routes
            // its values into a declared table.
            let compiled_body = bodies.remove(entity_name);

            let qualified_table = |name: &str| -> String {
                if let Some(schema_name) = target_schema_alias.as_deref() {
                    format!("{}.{}", quote_ident(schema_name), quote_ident(name))
                } else {
                    quote_ident(name)
                }
            };

            // Does the entity carry a declaration (schema / constraints / defaults)?
            let has_decl = !item.schema_rows.is_empty()
                || !item.constraint_rows.is_empty()
                || !item.default_rows.is_empty();

            // --- View materialization: a stored query, evaluated live on read;
            // no own data. Orthogonal to extent (TEMP applies as it does to tables). ---
            if item.materialization == manifest::Materialization::View {
                // A view cannot carry a declaration — no column types/constraints/
                // defaults on a view. v1 requires a bare rule.
                if has_decl {
                    return Err(DelightQLError::from(Runtime::General {
    message: format!(
                            "imprint!() entity '{}' is a view but declares schema/constraints/defaults — \
                             a view cannot carry them; drop the companions or materialize it as a table",
                            entity_name
                        ),
    details: "View with a declaration".to_string(),
}));
                }
                let select_sql = compiled_body.map(|compiled| compiled.0).ok_or_else(|| {
                    DelightQLError::from(Runtime::General {
                        message: format!(
                            "imprint!() view '{}' has no rule body — a view is a query",
                            entity_name
                        ),
                        details: "View without a rule body".to_string(),
                    })
                })?;
                let temp_kw = if temp { "TEMP " } else { "" };
                let qualified_create = format!(
                    "CREATE {}VIEW {} AS {}",
                    temp_kw,
                    qualified_table(entity_name),
                    select_sql
                );
                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::View,
                });
                continue;
            }

            // --- Table materialization forks on the declaration signal:
            //   declared  → typed CREATE TABLE (from schema) [+ INSERT … SELECT]
            //   bare rule → CREATE TABLE … AS SELECT (engine derives real types)
            // Constraints/defaults require a
            // schema() so column types are always declared, not guessed.
            if item.schema_rows.is_empty()
                && (!item.constraint_rows.is_empty() || !item.default_rows.is_empty())
            {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "imprint!() entity '{}' declares constraints/defaults but no schema() — \
                         declare column types in a schema(\"{}\") companion",
                        entity_name, entity_name
                    ),
                    details: "Constraints/defaults without a schema declaration".to_string(),
                }));
            }

            if has_decl {
                // Declared path: typed CREATE from schema/constraints/defaults,
                // then INSERT … SELECT to populate if there is a rule body.
                // ONE CORRESPONDENCE decides both the created table's column
                // order and which schema column each body value reaches.
                let correspondence = crate::ddl::correspondence::Correspondence::decide(
                    entity_name,
                    item.schema_rows.clone(),
                    compiled_body
                        .as_ref()
                        .map(|compiled| compiled.1.as_slice()),
                )?;
                let table = crate::ddl_pipeline::assemble_manifest::assemble_from_manifest(
                    entity_name,
                    temp,
                    correspondence.physical(),
                    &item.constraint_rows,
                    &item.default_rows,
                )?;
                let create_sql =
                    crate::pipeline::middle::api::declared_table(self, source_manifest.namespace(), &table)?;

                // The generator emits `CREATE TABLE "<name>"` via its own
                // `write_quoted` (raw quotes). Since manifest-read forbids a `"`
                // in an entity name (manifest::validate_entity_name),
                // `quote_ident(name)` is byte-identical to what the generator
                // produced, so the search pattern matches and the replacement
                // qualifies the name with the (escaped) schema alias.
                let qualified_create = if let Some(schema_name) = target_schema_alias.as_deref() {
                    create_sql.replacen(
                        &format!("CREATE TABLE {}", quote_ident(entity_name)),
                        &format!(
                            "CREATE TABLE {}.{}",
                            quote_ident(schema_name),
                            quote_ident(entity_name)
                        ),
                        1,
                    )
                } else {
                    create_sql
                };

                let insert = match (compiled_body, correspondence.insert_columns()) {
                    (Some(compiled), Some(columns)) => Some(format!(
                        "INSERT INTO {} ({}) {}",
                        qualified_table(entity_name),
                        columns
                            .iter()
                            .map(|column| quote_ident(column))
                            .collect::<Vec<_>>()
                            .join(", "),
                        compiled.0
                    )),
                    (None, None) => None,
                    _ => unreachable!("the correspondence routes exactly when a body exists"),
                };

                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::DeclaredTable {
                        schema: correspondence.physical().to_vec(),
                        insert,
                    },
                });
            } else if let Some(compiled) = compiled_body {
                let select_sql = compiled.0;
                // Bare rule, no declaration: CREATE TABLE … AS SELECT. The engine
                // derives real column types; attributes are read back post-create.
                let temp_kw = if temp { "TEMP " } else { "" };
                let qualified_create = format!(
                    "CREATE {}TABLE {} AS {}",
                    temp_kw,
                    qualified_table(entity_name),
                    select_sql
                );

                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::CtasTable,
                });
            } else {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "imprint!() entity '{}' has neither a schema() nor a rule body — \
                         nothing to materialize",
                        entity_name
                    ),
                    details: "No schema and no rule body".to_string(),
                }));
            }
        }

        // --- Phase 2: Execute (re-acquire bootstrap + target locks) ---
        let bootstrap_conn = Catalog::open(
            self,
            "Failed to re-acquire bootstrap lock for imprint execution",
        )?;
        // The source is judged AGAIN under the lock that will consume it:
        // the Phase 0 proof died with its lock, and this one is the only
        // value the consume accepts. It must be the same row Phase 0 read —
        // a namespace unconsulted and re-consulted at the path in between is
        // a different library than the manifest being materialized.
        let source = ImprintSource::admit(&bootstrap_conn, source_ns)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "imprint!() source '{}' disappeared while the imprint was being prepared",
                    source_ns
                ),
                details: "Namespace not found".to_string(),
            })
        })?;
        if source.id() != source_ns_id {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "imprint!() source '{}' was replaced while the imprint was being prepared; \
                     re-run the imprint against the current library",
                    source_ns
                ),
                details: "Source namespace replaced".to_string(),
            }));
        }
        let target_conn_guard = target_conn.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire target connection lock for imprint",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Enable FK enforcement on the shared target connection. This sets the
        // persistent, connection-global flag ON; the destructive DDL below runs
        // inside a transaction where `defer_foreign_keys` (not a toggle of this
        // flag) relaxes drop/create ordering, so no error path can leave
        // enforcement disabled (companion_linear--66).
        let _ = target_conn_guard.execute("PRAGMA foreign_keys = ON", &[]);

        // --- Pre-flight clash pass (READ-ONLY, before any mutation) ---
        // imprint! (replace=false)(*) fails if ANY target object already exists;
        // imprint_replace! (replace=true)(*) records each clashing object to drop.
        // This pass only *reads* the catalog: a strict clash returns here with
        // the target byte-for-byte untouched, so the strict path's atomicity is
        // by-construction, not by-rollback (pinned: companion_linear--67).
        //
        // Both sqlite_master AND sqlite_temp_master are consulted: a temp object
        // is connection-local and sqlite_master never lists it, so it would
        // otherwise bypass both the strict-clash refusal and the replace-mode
        // drop. sqlite_temp_master is unqualified (temp
        // objects are never in an attached schema; temp+mounted is refused at
        // prepare above), so it is queried as-is regardless of the target alias.
        // The SQL shape is pinned by
        // system::imprint_helper_tests::clash_probe_sees_temp_object; it is not
        // reachable through a ball because a temp-extent imprint is itself always
        // refused (every real target carries an alias), so no imprint ever
        // creates a temp object to clash with — the guard is defensive.
        let mut to_drop: Vec<(String, String)> = Vec::new(); // (name, type)
        {
            let master = match target_schema_alias.as_deref() {
                Some(a) => format!("{}.sqlite_master", quote_ident(a)),
                None => "sqlite_master".to_string(),
            };
            let mut clashes: Vec<String> = Vec::new();
            for entity in &prepared {
                let name_lit = entity.name.replace('\'', "''");
                let sql = imprint_clash_probe_sql(&master, &name_lit);
                let existing_type = target_conn_guard
                    .query_all_rows(&sql, &[])
                    .ok()
                    .and_then(|(_c, rows)| rows.first().and_then(|r| r.first()?.as_wire_text()));
                if let Some(ty) = existing_type {
                    if replace {
                        to_drop.push((entity.name.clone(), ty));
                    } else {
                        clashes.push(entity.name.clone());
                    }
                }
            }
            if !clashes.is_empty() {
                return Err(Runtime::catalog(
                    format!(
                        "imprint!() target object(s) already exist in '{}': {} — \
                         use imprint_replace!() to overwrite",
                        target_ns,
                        clashes.join(", ")
                    ),
                    "Imprint target clash",
                ));
            }
        }

        // --- Phase 2a: target transaction — every destructive/constructive
        // statement on the target connection (replace-mode drops + CREATEs +
        // CTAS INSERTs) commits or rolls back as a unit. A partial failure (a
        // later CREATE/CTAS erroring at exec time) rolls the whole thing back,
        // so replace-mode can never destroy the old tables and leave nothing in
        // their place (pinned: cli tests/imprint_atomicity.rs).
        //
        // `defer_foreign_keys = ON` set *inside* the txn makes drop/create
        // ordering FK-agnostic without touching the persistent `foreign_keys`
        // flag; SQLite auto-resets it at COMMIT/ROLLBACK, so no error path can
        // leak it OFF. At COMMIT the recreated CTAS tables carry no
        // FK constraints of their own, so there are no deferred constraints for
        // *this* txn to violate; any orphaned child of a replaced parent is
        // surfaced by the post-commit foreign_key_check below, not silently
        // accepted.
        target_conn_guard
            .execute("BEGIN IMMEDIATE", &[])
            .map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: "imprint: failed to open target transaction".to_string(),
                    details: e.to_string(),
                })
            })?;
        let mut target_txn = TargetTxnGuard {
            conn: &*target_conn_guard,
            committed: false,
        };
        let _ = target_conn_guard.execute("PRAGMA defer_foreign_keys = ON", &[]);

        // Replace-mode: drop the clashing objects (rolled back on any later error).
        for (name, ty) in &to_drop {
            let qualified = match target_schema_alias.as_deref() {
                Some(a) => format!("{}.{}", quote_ident(a), quote_ident(name)),
                None => quote_ident(name),
            };
            let kw = if ty == "view" { "VIEW" } else { "TABLE" };
            target_conn_guard
                .execute(&format!("DROP {} IF EXISTS {}", kw, qualified), &[])
                .map_err(|e| {
                    Runtime::catalog(
                        format!("imprint_replace!() failed to drop existing '{}'", name),
                        e.to_string(),
                    )
                })?;
        }

        // Create + populate each entity, in creation order. A failure here
        // (`?`) drops `target_txn`, which ROLLs BACK the drops above — the old
        // tables survive intact.
        for entity in creation.iter().map(|&at| &prepared[at]) {
            let entity_name = &entity.name;
            target_conn_guard
                .execute(&entity.qualified_create, &[])
                .map_err(|e| {
                    Runtime::catalog(
                        format!(
                            "Failed to execute CREATE TABLE for '{}': {}",
                            entity_name, entity.qualified_create,
                        ),
                        e.to_string(),
                    )
                })?;
            if let Materialized::DeclaredTable {
                insert: Some(insert),
                ..
            } = &entity.materialized
            {
                target_conn_guard.execute(insert, &[]).map_err(|e| {
                    Runtime::catalog(
                        format!(
                            "Failed to execute CTAS INSERT for '{}': {}",
                            entity_name, insert,
                        ),
                        e.to_string(),
                    )
                })?;
            }
        }

        target_conn_guard.execute("COMMIT", &[]).map_err(|e| {
            DelightQLError::from(Runtime::General {
                message: "imprint: failed to commit target transaction".to_string(),
                details: e.to_string(),
            })
        })?;
        target_txn.committed = true;

        // Post-commit FK audit (replace mode only). Recreated CTAS tables carry
        // none of the replaced tables' constraints; a child row that referenced
        // an old row now dangling is a silent orphan. We do NOT fail the imprint
        // (the data is committed) — we make it loud. NOTE: this
        // warning itself is not yet test-pinned (needs an external-FK-child
        // fixture).
        if !to_drop.is_empty() {
            let fk_check_sql = match target_schema_alias.as_deref() {
                Some(a) => format!("PRAGMA {}.foreign_key_check", quote_ident(a)),
                None => "PRAGMA foreign_key_check".to_string(),
            };
            if let Ok((_c, rows)) = target_conn_guard.query_all_rows(&fk_check_sql, &[]) {
                if !rows.is_empty() {
                    let mut by_table: std::collections::BTreeMap<String, usize> =
                        std::collections::BTreeMap::new();
                    for r in &rows {
                        if let Some(t) = r.first().and_then(|v| v.as_wire_text()) {
                            *by_table.entry(t).or_insert(0) += 1;
                        }
                    }
                    for (table, n) in by_table {
                        log::warn!(
                            "imprint_replace!() into '{}': {} orphaned foreign-key row(s) in \
                             table '{}' after replace — recreated tables carry no constraints; \
                             the rows were accepted, not rejected",
                            target_ns,
                            n,
                            table
                        );
                    }
                }
            }
        }

        // --- Phase 2b: bootstrap catalog — ordered AFTER the target commit, in
        // its own transaction on the (separate) bootstrap connection. The target
        // is already durable; if cataloging fails we roll the catalog back and
        // report loudly that the target WAS materialized (re-running the same
        // imprint is idempotent). Two connections cannot share one txn, so
        // "materialized-but-not-cataloged" is the worst residual window, never
        // "data destroyed".
        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::from(Runtime::General {
                message: "imprint: failed to begin catalog transaction".to_string(),
                details: e.to_string(),
            })
        })?;

        let catalog_result = (|| -> Result<(Vec<(String, String, String)>, String)> {
            // Deregister stale bootstrap entities for replaced names, else
            // re-materializing duplicates their columns (id/id_2/…).
            for (name, _ty) in &to_drop {
                let stale_ids: Vec<i64> = {
                    let mut stmt = bootstrap_conn
                        .prepare(
                            "SELECT e.id FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE ae.namespace_id = ?1 AND e.name = ?2",
                        )
                        .map_err(|e| {
                            Runtime::catalog("prepare stale entity lookup", e.to_string())
                        })?;
                    let rows = stmt
                        .query_map(rusqlite::params![target_ns_id, name], |r| r.get(0))
                        .map_err(|e| Runtime::catalog("query stale entity", e.to_string()))?;
                    rows.collect::<rusqlite::Result<_>>()
                        .map_err(|e| Runtime::catalog("read stale entity", e.to_string()))?
                };
                for stale in stale_ids {
                    entity_rows::retire_entity(&bootstrap_conn, stale)?;
                }
            }

            // Create a cartridge for the imprinted entities.
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
                    Runtime::catalog("Failed to create imprint cartridge", e.to_string())
                })?;
            let imprint_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

            let mut results: Vec<(String, String, String)> = Vec::new();
            for entity in &prepared {
                let entity_name = &entity.name;

                // Register the new entity in the target namespace. The
                // imprinted objects are engine tables/views on the target
                // database — served rows, never authored families.
                let entity_type = match entity.materialized {
                    Materialized::View => EntityType::DbPermanentView.as_i32(),
                    Materialized::CtasTable | Materialized::DeclaredTable { .. } => {
                        EntityType::DbPermanentTable.as_i32()
                    }
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
                        Runtime::catalog(format!("Failed to register imprinted entity '{}'", entity_name), e.to_string())
                    })?;
                let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Attribute (name, type) pairs. DeclaredTable knows its columns
                // from the manifest schema; View and CtasTable let the engine
                // choose the columns, so read them back from the now-committed
                // object (PRAGMA table_info works on both tables and views).
                let attr_cols: Vec<(String, String)> = match &entity.materialized {
                    Materialized::DeclaredTable { schema, .. } => schema
                        .iter()
                        .map(|sr| (sr.name.clone(), sr.col_type.clone()))
                        .collect(),
                    Materialized::View | Materialized::CtasTable => {
                        let pragma = match target_schema_alias.as_deref() {
                            Some(schema_name) => format!(
                                "PRAGMA {}.table_info({})",
                                quote_ident(schema_name),
                                quote_ident(entity_name)
                            ),
                            None => format!("PRAGMA table_info({})", quote_ident(entity_name)),
                        };
                        let (_cols, rows) = target_conn_guard
                            .query_all_rows(&pragma, &[])
                            .map_err(|e| {
                                Runtime::catalog(
                                    format!(
                                        "Failed to read back schema for materialized entity '{}'",
                                        entity_name
                                    ),
                                    e.to_string(),
                                )
                            })?;
                        // table_info columns: cid(0), name(1), type(2), …
                        rows.iter()
                            .filter_map(|r| {
                                Some((r.get(1)?.as_wire_text()?, r.get(2)?.as_wire_text()?))
                            })
                            .collect()
                    }
                };

                // Register entity attributes
                for (position, (col_name, col_type)) in attr_cols.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
                             VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                            rusqlite::params![new_entity_id, col_name, col_type, position as i32 + 1],
                        )
                        .map_err(|e| {
                            Runtime::catalog(format!("Failed to register attribute '{}' for '{}'", col_name, entity_name), e.to_string())
                        })?;
                }

                // Activate entity in target namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![new_entity_id, target_ns_id, imprint_cartridge_id],
                    )
                    .map_err(|e| {
                        Runtime::catalog(format!("Failed to activate imprinted entity '{}'", entity_name), e.to_string())
                    })?;

                // Populated = the CREATE also loaded rows: a CTAS table, or a
                // DeclaredTable with a trailing INSERT…SELECT. A bare View or an
                // unpopulated DeclaredTable is only "created".
                let status = match &entity.materialized {
                    Materialized::CtasTable
                    | Materialized::DeclaredTable {
                        insert: Some(_), ..
                    } => "created+populated",
                    Materialized::View | Materialized::DeclaredTable { insert: None, .. } => {
                        "created"
                    }
                };
                results.push((
                    entity_name.clone(),
                    status.to_string(),
                    entity.qualified_create.clone(),
                ));
            }

            // Linear imprint: consume the source into a blueprint archive under
            // the target. Moves the source namespace,
            // vacating its path (use-after-imprint = error; path free to
            // re-consult) and leaving the tables as the single source of truth.
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
                        "Failed to query sys::meta namespace for imprint consume",
                        e.to_string(),
                    )
                })?;
            let blueprint_fq = consume_source_to_blueprint(
                &source,
                target_ns,
                target_ns_id,
                sys_meta_ns_id,
                catalog_id,
            )?;

            Ok((results, blueprint_fq))
        })();

        let (results, blueprint_fq) = match catalog_result {
            Ok(v) => {
                bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                    DelightQLError::from(Runtime::General {
                        message: "imprint: failed to commit catalog transaction".to_string(),
                        details: e.to_string(),
                    })
                })?;
                v
            }
            Err(e) => {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "imprint: target '{}' WAS materialized, but cataloging it failed: {}. \
                         The data is safe; re-run as imprint_replace!() to finish the catalog \
                         (strict imprint! would now refuse — the materialized tables count as \
                         a clash).",
                        target_ns, e
                    ),
                    details: "Imprint cataloging failed after materialization".to_string(),
                }));
            }
        };

        // Release the target-txn guard's borrow before the connection guards are
        // dropped below (COMMIT already ran; this drop is a no-op ROLLBACK-skip).
        drop(target_txn);

        drop(target_conn_guard);
        drop(bootstrap_conn);

        debug!(
            "imprint_namespace: Consumed '{}' → archived at '{}'",
            source_ns, blueprint_fq
        );

        debug!(
            "imprint_namespace: Materialized {} entities from '{}' into '{}'",
            results.len(),
            source_ns,
            target_ns
        );

        Ok(results)
    }
}

#[cfg(test)]
mod imprint_version_tests {
    //! Blueprint versioning: the version N chosen for a new
    //! `{target}::_N_blueprint` must be `MAX(existing N)+1`, not `COUNT`, so a
    //! removed blueprint never causes the next imprint to reuse a live N; and a
    //! failed version query must be loud, never a silent 0.
    use super::next_blueprint_version;
    use rusqlite::Connection;

    fn ns_conn() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, pid INTEGER, fq_name TEXT);
             INSERT INTO namespace (id, name, pid, fq_name) VALUES (1, 'main', NULL, 'main');",
        )
        .unwrap();
        c
    }

    #[test]
    fn next_blueprint_version_is_max_plus_one() {
        let c = ns_conn();
        // No blueprints yet → 0.
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 0);

        c.execute(
            "INSERT INTO namespace (id, name, pid, fq_name)
             VALUES (2, '_0_blueprint', 1, 'main::_0_blueprint'),
                    (3, '_1_blueprint', 1, 'main::_1_blueprint')",
            [],
        )
        .unwrap();
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 2);

        // Delete _0_blueprint: COUNT would now return 1 — a collision with the
        // surviving _1_blueprint. MAX+1 stays 2.
        c.execute("DELETE FROM namespace WHERE id = 2", []).unwrap();
        assert_eq!(
            next_blueprint_version(&c, 1).unwrap(),
            2,
            "MAX+1 must not reuse an existing N after a gap (finding 8)"
        );

        // Non-blueprint children and blueprints of other parents are ignored.
        c.execute(
            "INSERT INTO namespace (id, name, pid, fq_name)
             VALUES (4, '_internal', 1, 'main::_internal'),
                    (5, '_9_blueprint', 99, 'other::_9_blueprint')",
            [],
        )
        .unwrap();
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 2);
    }

    #[test]
    fn next_blueprint_version_errors_on_missing_table() {
        // A failed version query is a loud error (`?`), not the silent 0 that
        // `.unwrap_or(0)` masked.
        let c = Connection::open_in_memory().unwrap();
        assert!(next_blueprint_version(&c, 1).is_err());
    }
}

#[cfg(test)]
mod imprint_helper_tests {
    //! Identifier quoting for the imprint DDL path. The schema
    //! alias is the load-bearing case (it comes from a mount path / ATTACH
    //! alias, not a validatable identifier); entity names are additionally
    //! forbidden a `"` at manifest-read (manifest::validate_entity_name).
    use super::{imprint_clash_probe_sql, quote_ident};
    use rusqlite::Connection;

    #[test]
    fn quote_ident_doubles_internal_quote() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        // The empty and all-quotes edge cases stay well-formed.
        assert_eq!(quote_ident(""), "\"\"");
        assert_eq!(quote_ident("\""), "\"\"\"\"");
    }

    #[test]
    fn quote_ident_output_is_valid_sql_identifier() {
        // The doubled form must round-trip through SQLite as the literal name,
        // not a truncated/injected one. Create a table whose name embeds a `"`
        // and read it back — proof the escaping is real, not cosmetic.
        let c = Connection::open_in_memory().unwrap();
        let ident = quote_ident("we\"ird");
        c.execute_batch(&format!("CREATE TABLE {ident} (x INTEGER)"))
            .unwrap();
        let name: String = c
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(name, "we\"ird");
    }

    #[test]
    fn clash_probe_sees_temp_object() {
        // A temp object is invisible to sqlite_master but
        // must still register as a clash. The probe UNIONs sqlite_temp_master.
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TEMP TABLE foo (x INTEGER)")
            .unwrap();

        // sqlite_master alone misses it (the blind spot the clash probe closes)...
        let master_only: Option<String> = c
            .query_row(
                "SELECT type FROM sqlite_master WHERE name = 'foo'",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(master_only, None);

        // ...the clash probe catches it.
        let probe = imprint_clash_probe_sql("sqlite_master", "foo");
        assert!(probe.contains("sqlite_temp_master"), "{}", probe);
        let ty: String = c.query_row(&probe, [], |r| r.get(0)).unwrap();
        assert_eq!(ty, "table");
    }
}
