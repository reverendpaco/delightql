// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A load, prepared by the liminal walk and published exactly once: its
//! families, declared lexical edges and docs land in one transaction, and a
//! replacement deletes and respends the namespace's load whole. The
//! lexical-edge acts (enlist, alias, expose) and the evidence they answer
//! with live here.

use super::liminal::LiminalProgramKind;
use super::population::{ensure_catalog_initialized, register_catalog_wrapper};
use super::registration::LexicalCapture;
use super::{CatalogSavepoint, DelightQLSystem, LiminalRow};
use crate::ddl::lifecycle::{
    admit_live_kind, refuse_if_blueprint, Catalog, LiveNamespace, LiveVerb,
};
use crate::diagnostic::{Constraint, Runtime};
use crate::error::{DelightQLError, Result};
use log::debug;
use rusqlite::{Connection, OptionalExtension};

/// EVIDENCE OF ONE LEXICAL-EDGE ACT, WHOLE. Minted only by the act that
/// performed it — [`PreparedLoad::enlist`], [`PreparedLoad::alias`],
/// [`PreparedLoad::expose`], through this module's private acts — carrying
/// the KIND the act performed, the shorthand it registered (an alias), and
/// the namespace it selected. The value never leaves this module: an act
/// records it in the load it was performed for, so a holder can neither
/// reclassify an enlistment as an exposure, pair an alias target with a
/// shorthand its act did not register, drop it, nor move it to another
/// load — what a load declares is exactly what its acts performed.
#[derive(Debug)]
struct DeclaredEdge(LexicalAct);

#[derive(Debug)]
enum LexicalAct {
    Enlist { target: i64 },
    Alias { shorthand: String, target: i64 },
    Expose { target: i64 },
}

/// THE LOAD ONE LIMINAL PROGRAM EXECUTION CONSTRUCTS: its destination
/// namespace, its definitions, its ledger rows, its deferred `doc!`s, and
/// the edges its directive acts answered with, in authored order. The
/// walk builds it through the mutators below; publication SPENDS it —
/// [`DelightQLSystem::publish`] takes it by value and answers with the
/// [`PublishedLoad`] — so a load is published exactly once, for the
/// destination and under the publication semantics it owns.
/// WHERE A LOAD COMES FROM — fixed when the load is begun, never chosen at
/// publication. A file load's path names its cartridge; an inline block (a
/// scratch namespace's `(~~ddl ~~)`) has no file, and it alone receives the
/// session's ambient data world, as the scratch law grants.
enum LoadSource {
    File { path: String },
    Inline,
}

/// HOW A LOAD LANDS — fixed when the load is begun. A fresh consultation
/// registers into its namespace as it stands; a replacement first deletes
/// the namespace's current load whole, then rebuilds every derived world
/// that depends on it, inside the same transaction.
enum LoadMode {
    Fresh,
    Replacement,
}

pub(crate) struct PreparedLoad {
    namespace: String,
    source: LoadSource,
    mode: LoadMode,
    rows: Vec<crate::bin_cartridge::prelude::consult::PreparedRow>,
    definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    deferred_docs: Vec<(String, String)>,
    edges: Vec<DeclaredEdge>,
}

impl PreparedLoad {
    fn empty(namespace: &str, source: LoadSource, mode: LoadMode) -> Self {
        PreparedLoad {
            namespace: namespace.to_string(),
            source,
            mode,
            rows: Vec::new(),
            definitions: Vec::new(),
            deferred_docs: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// An empty load bound for `namespace`, from the file at `path`, for
    /// the liminal walk to fill — fresh, or the replacement of the
    /// namespace's current load, as the walk's own mode says.
    pub(crate) fn from_file(
        namespace: &str,
        path: &str,
        mode: crate::bin_cartridge::prelude::consult::LiminalDirectiveMode,
    ) -> Self {
        use crate::bin_cartridge::prelude::consult::LiminalDirectiveMode;
        let mode = match mode {
            LiminalDirectiveMode::Fresh => LoadMode::Fresh,
            LiminalDirectiveMode::Replay => LoadMode::Replacement,
        };
        Self::empty(
            namespace,
            LoadSource::File {
                path: path.to_string(),
            },
            mode,
        )
    }

    /// A load with no liminal space — an inline DDL block: definitions
    /// only, no ledger, no docs, no edges; fresh into its scratch
    /// namespace.
    pub(crate) fn inline(
        namespace: &str,
        definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    ) -> Self {
        let mut load = Self::empty(namespace, LoadSource::Inline, LoadMode::Fresh);
        load.definitions = definitions;
        load
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn define(&mut self, clause: crate::pipeline::asts::ddl::ClauseDecl) {
        self.definitions.push(clause);
    }

    pub(crate) fn settle(&mut self, row: crate::bin_cartridge::prelude::consult::PreparedRow) {
        self.rows.push(row);
    }

    pub(crate) fn doc(&mut self, target: String, doc: String) {
        self.deferred_docs.push((target, doc));
    }

    /// THE LEXICAL-EDGE ACTS ARE THE LOAD'S. Each performs the session
    /// effect the directive means and records the edge it performed in
    /// THIS load, in one step: the act cannot succeed without changing the
    /// load under construction, and its evidence exists nowhere else to be
    /// dropped or attributed to another load.
    pub(crate) fn enlist(&mut self, system: &mut DelightQLSystem, target: &str) -> Result<()> {
        self.edges.push(system.perform_enlist(target)?);
        Ok(())
    }

    pub(crate) fn alias(
        &mut self,
        system: &mut DelightQLSystem,
        shorthand: &str,
        target: &str,
    ) -> Result<()> {
        self.edges.push(system.perform_alias(shorthand, target)?);
        Ok(())
    }

    pub(crate) fn expose(&mut self, system: &DelightQLSystem, child_fq: &str) -> Result<()> {
        let edge = system.perform_expose(&self.namespace, child_fq)?;
        self.edges.push(edge);
        Ok(())
    }

    /// SPEND THE LOAD into the catalog on `conn`, inside the publication
    /// transaction: register its definitions under the cartridge its source
    /// names, apply its `doc!`s, and record its declared edges — checking
    /// that each selected target still stands and that an exposure names a
    /// child (the facade law), selecting nothing again. Consumes the load;
    /// the answer is the proof that the complete load — families AND
    /// lexical graph — stands together.
    fn spend_on(self, conn: &Connection, default_data_ns: Option<&str>) -> Result<PublishedLoad> {
        let PreparedLoad {
            namespace,
            source,
            mode,
            rows,
            definitions,
            deferred_docs,
            edges,
        } = self;
        let path = match &source {
            LoadSource::File { path } => path.as_str(),
            LoadSource::Inline => "(inline)",
        };
        let replacing = matches!(mode, LoadMode::Replacement);
        let count = definitions.len();
        // THE LEXICAL WORLD THIS LOAD CAPTURES, judged from the load itself
        // before anything is registered: a file's own declared enlistments
        // and aliases, an inline block's session enlist set and session
        // aliases. They land beside the load's cartridge, so a body admitted
        // in this load selects and routes through exactly this world for as
        // long as the load stands.
        let capture = lexical_capture_on(conn, &source, &edges)?;
        let registered = DelightQLSystem::consult_file_inner(
            conn,
            path,
            &namespace,
            definitions,
            count,
            default_data_ns,
            replacing,
            &capture,
        )?;
        for (target, doc) in &deferred_docs {
            let candidates = [
                format!("{}.{}", namespace, target),
                format!("{}.{}!", namespace, target),
                target.clone(),
            ];
            let mut last_err = None;
            let mut done = false;
            for candidate in &candidates {
                match DelightQLSystem::set_entity_doc_on(conn, candidate, doc) {
                    Ok(()) => {
                        done = true;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if !done {
                return Err(last_err.expect("candidates is non-empty"));
            }
        }
        let namespace_id: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [&namespace],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("namespace lookup for declared graph", e.to_string()))?;
        record_declared_edges_on(conn, namespace_id, &namespace, edges)?;
        Ok(PublishedLoad {
            namespace_id,
            definitions_loaded: registered.definitions_loaded,
            replaced_entities: registered.replaced_entities,
            relational_families: registered.relational_families,
            rows,
        })
    }
}

/// THE LEXICAL WORLD A LOAD CAPTURES AT ADMISSION: for a file, the
/// namespaces its own `enlist!` acts selected and the shorthands its own
/// `alias!` acts registered; for an inline block, the session's enlist set
/// and the session's aliases as they stand now. Captured by IDENTITY — the
/// namespaces themselves, not their contents and not the exposures they
/// grant, both of which the reach reads current through the captured
/// identity. Nothing later (a `delist!`, a further `enlist!`, a shorthand
/// given to another namespace) reaches into the capture.
fn lexical_capture_on(
    conn: &Connection,
    source: &LoadSource,
    edges: &[DeclaredEdge],
) -> Result<LexicalCapture> {
    let (mut imports, mut aliases): (Vec<i64>, Vec<(String, i64)>) = match source {
        LoadSource::File { .. } => {
            let mut imports = Vec::new();
            let mut aliases = Vec::new();
            for DeclaredEdge(act) in edges {
                match act {
                    LexicalAct::Enlist { target } => imports.push(*target),
                    LexicalAct::Alias { shorthand, target } => {
                        aliases.push((shorthand.clone(), *target))
                    }
                    LexicalAct::Expose { .. } => {}
                }
            }
            (imports, aliases)
        }
        LoadSource::Inline => {
            let mut stmt = conn
                .prepare(
                    "SELECT en.from_namespace_id FROM enlisted_namespace en
                     JOIN namespace home ON home.id = en.to_namespace_id
                     WHERE home.fq_name = 'home'",
                )
                .map_err(|e| Runtime::catalog("prepare session import capture", e.to_string()))?;
            let imports = stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(|e| Runtime::catalog("run session import capture", e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Runtime::catalog("decode session import capture", e.to_string()))?;
            let mut stmt = conn
                .prepare("SELECT alias, target_namespace_id FROM namespace_alias")
                .map_err(|e| Runtime::catalog("prepare session alias capture", e.to_string()))?;
            let aliases = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(|e| Runtime::catalog("run session alias capture", e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Runtime::catalog("decode session alias capture", e.to_string()))?;
            (imports, aliases)
        }
    };
    imports.sort_unstable();
    imports.dedup();
    // A shorthand names one namespace at a time, so a repeat is the same
    // binding declared again.
    aliases.sort();
    aliases.dedup();
    Ok(LexicalCapture { imports, aliases })
}

/// Record a spent load's declared edges as namespace-local edges. Every
/// edge is an act's whole answer; this checks only that its selected
/// target still stands (a load that destroyed what it enlisted refuses)
/// and, for an exposure, the facade law — then writes by identity.
fn record_declared_edges_on(
    conn: &Connection,
    namespace_id: i64,
    namespace: &str,
    edges: Vec<DeclaredEdge>,
) -> Result<()> {
    let standing = |target: i64, edge: &str| -> Result<String> {
        conn.query_row(
            "SELECT fq_name FROM namespace WHERE id = ?1",
            [target],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| Runtime::catalog("selected target lookup", e.to_string()))?
        .ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!(
                    "'{namespace}' declares {edge} of a namespace its own load no longer \
                     holds — the target the directive selected was destroyed before \
                     publication"
                ),
                details: "declared target destroyed".to_string(),
            })
        })
    };
    for DeclaredEdge(act) in edges {
        match act {
            LexicalAct::Enlist { target } => {
                // The enlistment was captured as the load's import when the
                // load was spent; here only the target's standing is judged.
                standing(target, "an enlistment")?;
            }
            LexicalAct::Alias { target, .. } => {
                // The alias was captured as the load's route when the load
                // was spent; here only the target's standing is judged.
                standing(target, "an alias")?;
            }
            LexicalAct::Expose { target } => {
                let target_fq = standing(target, "an exposure")?;
                if !crate::namespace::is_under(&target_fq, namespace) {
                    return Err(DelightQLError::from(Runtime::General {
                        message: format!(
                            "Cannot expose '{target_fq}' through '{namespace}': not a child \
                             namespace"
                        ),
                        details: "Invalid expose target".to_string(),
                    }));
                }
                conn.execute(
                    "INSERT OR IGNORE INTO exposed_namespace \
                     (exposing_namespace_id, exposed_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![namespace_id, target],
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Failed to expose namespace '{target_fq}': {e}"),
                        e.to_string(),
                    )
                })?;
            }
        }
    }
    Ok(())
}

/// THE PROOF THAT A LOAD IS PUBLISHED COMPLETE: its families AND its
/// declared lexical graph (local enlistments, aliases, exposures, docs)
/// stand in the catalog together, inside the load's transaction. Minted
/// only by [`DelightQLSystem::publish`], which spent the load; the
/// derived-world rebuild accepts nothing else, so a dependent world can
/// never derive from a source whose edges are still to come. It carries
/// the ledger rows the load prepared, for the witnesses that run after
/// publication.
pub(crate) struct PublishedLoad {
    namespace_id: i64,
    definitions_loaded: usize,
    replaced_entities: Vec<String>,
    relational_families: Vec<String>,
    rows: Vec<crate::bin_cartridge::prelude::consult::PreparedRow>,
}

impl PublishedLoad {
    pub(crate) fn namespace_id(&self) -> i64 {
        self.namespace_id
    }

    pub(crate) fn definitions_loaded(&self) -> usize {
        self.definitions_loaded
    }

    /// Entity names an inline block replaced (drop-and-replace).
    pub(crate) fn replaced_entities(&self) -> &[String] {
        &self.replaced_entities
    }

    /// The relational families the load registered, by name: their heads
    /// are judged where they are declared, once the whole load stands.
    pub(crate) fn relational_families(&self) -> &[String] {
        &self.relational_families
    }

    /// The ledger rows the load prepared, for the witnesses that run once
    /// the load stands.
    pub(crate) fn into_ledger(self) -> Vec<crate::bin_cartridge::prelude::consult::PreparedRow> {
        self.rows
    }
}

impl DelightQLSystem {
    /// Consult a DQL file containing definitions (functions and views)
    ///
    /// Load definitions from a parsed DDL file into the bootstrap metadata system.
    ///
    /// For each definition: creates an entity row, activates it in the namespace.
    /// The bootstrap DB is the single source of truth — no in-memory cache.
    ///
    /// PUBLISH ONE LOAD — the one road by which a prepared load becomes
    /// catalog state. The load owns everything publication needs: its
    /// destination, its source (a file's path, or an inline block — which
    /// alone receives the ambient data world), and its mode. A FRESH load
    /// registers inside one savepoint; a REPLACEMENT first deletes the
    /// namespace's current load whole, spends the new one, records its
    /// source path, and rebuilds every derived world that depends on the
    /// namespace — a refusal anywhere rolls the deletion, the replacement,
    /// and the rebuilds back together. Nothing about source, ambient
    /// license, or fresh-vs-replacement is chosen here.
    ///
    /// # Returns
    /// The published load: definitions loaded, replaced entity names, and
    /// the ledger rows for the witnesses that follow.
    pub(crate) fn publish(&mut self, load: PreparedLoad) -> Result<PublishedLoad> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let namespace = load.namespace().to_string();
        let namespace = namespace.as_str();
        debug!(
            "publish: {} definitions into namespace '{}'",
            load.definitions.len(),
            namespace
        );

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for consult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        if matches!(load.mode, LoadMode::Replacement) {
            let transaction = CatalogSavepoint::begin(
                &bootstrap_conn,
                "dql_reconsult_namespace",
                "Failed to begin reconsult transaction",
            )?;
            let ns_id: i64 = bootstrap_conn
                .query_row(
                    "SELECT id FROM namespace WHERE fq_name = ?1",
                    [namespace],
                    |row| row.get(0),
                )
                .map_err(|_| {
                    DelightQLError::from(Runtime::General {
                        message: format!("Namespace '{}' not found", namespace),
                        details: "Namespace not found".to_string(),
                    })
                })?;
            // DELETE the old load whole — its families, declared edges, and
            // ledger — inside this savepoint. A failure anywhere below rolls
            // the deletion back with the partial replacement, so the prior
            // load stands whole; a statement already compiling holds its
            // own catalog read and is not here to observe either.
            Self::delete_namespace_load(&bootstrap_conn, ns_id)?;
            let source_path = match &load.source {
                LoadSource::File { path } => Some(path.clone()),
                LoadSource::Inline => None,
            };
            // SPEND THE LOAD: families, doc!s, and declared edges land
            // together. The answer is the proof the replacement is COMPLETE
            // — only it can ask dependent derived worlds to rebuild, so no
            // rebuild ever reads a source whose edges are still to come.
            let published = load.spend_on(&bootstrap_conn, None)?;
            if let Some(path) = source_path {
                bootstrap_conn
                    .execute(
                        "UPDATE namespace SET source_path = ?1 WHERE id = ?2",
                        rusqlite::params![&path, ns_id],
                    )
                    .map_err(|e| Runtime::catalog("Failed to update source_path", e.to_string()))?;
            }
            // Every derived world that derives from this namespace — as its
            // root's source or as a transitive dependency — is rebuilt whole
            // from the COMPLETE replacement and re-admitted; a refusal rolls
            // the whole reload back, so a published world is never left
            // broken by a replacement it cannot admit.
            crate::defuse::grounded_world::rebuild_dependents(&bootstrap_conn, &published)
            .map_err(|e| {
                Runtime::catalog(
                    format!("Grounding contract violation: lib '{namespace}'. {e}"),
                    "Grounding contract violated",
                )
            })?;
            transaction.commit("Failed to commit reconsult transaction")?;
            return Ok(published);
        }

        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_consult_file",
            "Failed to begin consult transaction",
        )?;

        // THE AMBIENT DATA WORLD is the inline load's alone: a scratch
        // namespace's views read the primary data namespace (typically
        // "main") without explicit grounding, as the scratch law grants;
        // a file load receives none.
        let ambient_data_ns = if matches!(load.source, LoadSource::Inline) {
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

        // THE LOAD IS SPENT HERE: definitions, doc!s, and declared edges land
        // in this one transaction — any failure rolls the whole consultation
        // back.
        let result = load.spend_on(&bootstrap_conn, ambient_data_ns.as_deref());

        if result.is_ok() {
            transaction.commit("Failed to commit consult transaction")?;

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
                        // The structural prefixes the consultation created
                        // are namespaces like any other.
                        let segments: Vec<&str> = namespace.split("::").collect();
                        for end in 1..segments.len() {
                            let _ = register_catalog_wrapper(
                                &bootstrap_conn,
                                &segments[..end].join("::"),
                                sys_meta_ns_id,
                                catalog_id,
                            );
                        }
                    }
                }
            }
        } else {
            drop(transaction);
        }

        drop(bootstrap_conn);

        result
    }

    /// THE LIMINAL RELATION: persist one load's ledger, one row per TOP-LEVEL
    /// FORM, in file-appearance order (rowid = insertion order — the
    /// engine-courtesy contract, no sequence column).
    ///
    /// Called AFTER registration and after the relational goals are proved,
    /// because a witness may name what the load defines and a row cannot be
    /// written before its verdict exists. It runs inside the liminal
    /// program's savepoint, so an abort anywhere still rolls the ledger away
    /// with the namespace (pinned by `liminal_ledger_abort_leaves_no_ledger`
    /// and `liminal_ledger_registration_refusal_rolls_ledger_back`). A
    /// deferred liminal `doc!` keeps its FILE position: the rows were
    /// collected in one pass over the file's forms, before deferral (pinned
    /// by `liminal_ledger_doc_keeps_file_position`). A repeat consult into an
    /// existing namespace APPENDS; reconsult REPLACES whole via
    /// `clear_namespace_contents`.
    pub(crate) fn record_liminal_ledger(&self, namespace: &str, rows: &[LiminalRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for the liminal ledger",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let namespace_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                rusqlite::params![namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message: format!("the ledger's namespace '{namespace}' is not in the catalog"),
                    details: e.to_string(),
                })
            })?;
        for row in rows {
            bootstrap_conn
                .execute(
                    "INSERT INTO liminal_receipt (namespace_id, operation, echoes, receipt)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        namespace_id,
                        row.operation(),
                        row.echoes_json(),
                        row.receipt_json()
                    ],
                )
                .map_err(|e| Runtime::catalog("Failed to record ledger row", e.to_string()))?;
        }
        Ok(())
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
        self.perform_enlist(namespace).map(|_| ())
    }

    /// THE ENLISTMENT ACT: enlist `namespace` at the session and answer with
    /// the edge performed. Private — a load's [`PreparedLoad::enlist`] is
    /// the only holder of the answer; the prompt-level directive discards
    /// it through [`Self::enlist_namespace`], because a prompt is not a
    /// load.
    fn perform_enlist(&mut self, namespace: &str) -> Result<DeclaredEdge> {
        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for enlist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // THE ARGUMENT IS AN EXACT PATH. A one-segment spelling names a
        // top-level namespace and nothing else: `enlist!("scr")` never
        // discovers `home::scr` behind it — the child is spelled in full.
        let (from_namespace_id, resolved_fq): (i32, String) = match bootstrap_conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [namespace],
            |row| row.get::<_, i32>(0),
        ) {
            Ok(id) => (id, namespace.to_string()),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "Namespace '{}' not found. Make sure to mount!() it first.",
                        namespace
                    ),
                    details: "namespace not found".to_string(),
                }));
            }
            Err(e) => {
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "Namespace '{}' not found. Make sure to mount!() it first.",
                        namespace
                    ),
                    details: e.to_string(),
                }));
            }
        };

        // Blueprint inertness: enlisting an archived blueprint (or a
        // descendant of one) would make its inert rules resolvable UNQUALIFIED
        // — the opposite of "consumed and archived". Refuse. Checks the RESOLVED
        // fq so a plain-name enlist of an archived-blueprint child stays refused.
        refuse_if_blueprint(&bootstrap_conn, &resolved_fq)?;

        // The interactive session's scope is `home`: a
        // prompt-level enlist attaches its edge to home, never to the
        // `main` data namespace.
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'home'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message:
                        "Session namespace 'home' not found in bootstrap (database corruption)"
                            .to_string(),
                    details: e.to_string(),
                })
            })?;

        // Check for ER-context name collisions with already-enlisted namespaces.
        // Two enlisted namespaces with the same context name create ambiguity for
        // `under ctx:` lookups that search enlisted namespaces.
        {
            let mut collision_stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT new_er.context_name, existing_ns.fq_name
                     FROM join_edge new_er
                     JOIN entity new_e ON new_e.id = new_er.entity_id
                     JOIN activated_entity new_ae ON new_ae.entity_id = new_e.id
                        AND new_ae.namespace_id = ?1
                     JOIN join_edge existing_er ON existing_er.context_name = new_er.context_name
                     JOIN entity existing_e ON existing_e.id = existing_er.entity_id
                     JOIN activated_entity existing_ae ON existing_ae.entity_id = existing_e.id
                     JOIN namespace existing_ns ON existing_ns.id = existing_ae.namespace_id
                     JOIN enlisted_namespace en ON en.from_namespace_id = existing_ns.id
                        AND en.to_namespace_id = ?2
                     WHERE existing_ns.id != ?1",
                )
                .map_err(|e| {
                    Runtime::catalog(
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
                    Runtime::catalog("Failed to check ER-context collisions", e.to_string())
                })?
                .filter_map(|r| r.ok())
                .collect();

            if !collisions.is_empty() {
                let details: Vec<String> = collisions
                    .iter()
                    .map(|(ctx, ns)| format!("context '{}' (already enlisted from '{}')", ctx, ns))
                    .collect();
                return Err(DelightQLError::from(Runtime::General {
                    message: format!(
                        "Cannot enlist namespace '{}': ER-context name collision — {}. \
                         Use qualified access (ns.view(*)) instead of enlist to avoid ambiguity.",
                        namespace,
                        details.join(", "),
                    ),
                    details: "ER-context collision on enlist".to_string(),
                }));
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
                Runtime::catalog(
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

        // THE ACT ANSWERS WITH THE EDGE IT PERFORMED: an enlistment of the
        // namespace it selected, whole — the only evidence a load can
        // declare.
        Ok(DeclaredEdge(LexicalAct::Enlist {
            target: i64::from(from_namespace_id),
        }))
    }

    /// Register a namespace alias (e.g., "l" → "lib::math")
    ///
    /// Creates a namespace_alias record in bootstrap, allowing a short alias
    /// to be used in place of a fully-qualified namespace path.
    pub fn register_namespace_alias(&mut self, alias: &str, namespace: &str) -> Result<()> {
        self.perform_alias(alias, namespace).map(|_| ())
    }

    /// THE ALIAS ACT: register `alias` → `namespace` at the session and
    /// answer with the edge performed, shorthand included. Private, as
    /// [`Self::perform_enlist`] is.
    fn perform_alias(&mut self, alias: &str, namespace: &str) -> Result<DeclaredEdge> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
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
                DelightQLError::from(Runtime::General {
                    message: format!(
                        "Namespace '{}' not found. Cannot create alias '{}'.",
                        namespace, alias
                    ),
                    details: e.to_string(),
                })
            })?;

        // A shorthand that names an EXISTING namespace makes every lookup
        // for that name two-headed: the entity resolution query ORs the
        // exact-fq and alias branches with no ordering, so which head
        // wins is scan order. Refuse the collision at registration.
        let collision: Option<i64> = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [alias],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Runtime::catalog("Failed to check alias collision", e.to_string()))?;
        if collision.is_some() {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "alias!() shorthand '{}' collides with an existing namespace of \
                     the same name — lookups for '{}' would be ambiguous. Choose a \
                     different shorthand.",
                    alias, alias
                ),
            }));
        }

        // Colliding with an existing SHORTHAND is the same two-headed
        // ambiguity as colliding with a namespace, and gets the same
        // refusal. Re-binding by replacement was a silent last-writer-wins
        // collision policy; a taken shorthand refuses, naming its holder.
        // Re-declaring the SAME binding stays idempotent.
        let taken: Option<(i64, String)> = bootstrap_conn
            .query_row(
                "SELECT a.target_namespace_id, n.fq_name FROM namespace_alias a
                 JOIN namespace n ON n.id = a.target_namespace_id
                 WHERE a.alias = ?1",
                [alias],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| Runtime::catalog("Failed to check alias holder", e.to_string()))?;
        if let Some((holder_id, holder_fq)) = taken {
            if i64::from(ns_id) == holder_id {
                // Idempotent: the same binding, the same edge performed.
                return Ok(DeclaredEdge(LexicalAct::Alias {
                    shorthand: alias.to_string(),
                    target: i64::from(ns_id),
                }));
            }
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "alias!() shorthand '{}' is already taken by '{}' — lookups for \
                     '{}' would be ambiguous. Choose a different shorthand.",
                    alias, holder_fq, alias
                ),
            }));
        }

        bootstrap_conn
            .execute(
                "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![alias, ns_id],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!(
                        "Failed to register namespace alias '{}' → '{}': {}",
                        alias, namespace, e
                    ),
                    e.to_string(),
                )
            })?;

        debug!("register_namespace_alias: '{}' → '{}'", alias, namespace);

        drop(bootstrap_conn);
        // THE ACT ANSWERS WITH THE EDGE IT PERFORMED: the shorthand it
        // registered, bound to the namespace it selected — no caller pairs
        // them afterwards.
        Ok(DeclaredEdge(LexicalAct::Alias {
            shorthand: alias.to_string(),
            target: i64::from(ns_id),
        }))
    }

    /// SELECT THE TARGET OF AN EXPOSURE a consulted file declares: the
    /// child namespace `child_fq` names, exactly — the file's own child by
    /// the facade law — as it stands when the directive executes. The
    /// exposing namespace itself may not have its row yet (a fresh
    /// consult creates it at registration); the law is a relationship of
    /// names, checked here and again at publication.
    fn perform_expose(&self, exposing_fq: &str, child_fq: &str) -> Result<DeclaredEdge> {
        if !crate::namespace::is_under(child_fq, exposing_fq) {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "Cannot expose '{child_fq}' through '{exposing_fq}': not a child namespace"
                ),
                details: "Invalid expose target".to_string(),
            }));
        }
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock for expose")?;
        let id: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [child_fq],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::from(Runtime::General {
                    message: format!("Namespace '{child_fq}' not found for expose"),
                    details: "Namespace not found".to_string(),
                })
            })?;
        Ok(DeclaredEdge(LexicalAct::Expose { target: id }))
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
            Runtime::poisoned(
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
                DelightQLError::from(Runtime::General {
                    message: format!("Namespace '{}' not found", namespace),
                    details: e.to_string(),
                })
            })?;

        // The interactive session's scope is `home`: a
        // prompt-level enlist attaches its edge to home, never to the
        // `main` data namespace.
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'home'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::from(Runtime::General {
                    message:
                        "Session namespace 'home' not found in bootstrap (database corruption)"
                            .to_string(),
                    details: e.to_string(),
                })
            })?;

        // Delete enlisted_namespace record
        let rows_affected = bootstrap_conn
            .execute(
                "DELETE FROM enlisted_namespace
                 WHERE from_namespace_id = ?1 AND to_namespace_id = ?2",
                [from_namespace_id, to_namespace_id],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to delist namespace '{}': {}", namespace, e),
                    e.to_string(),
                )
            })?;

        if rows_affected == 0 {
            return Err(DelightQLError::from(Runtime::UseAfterFree {
                message: format!(
                    "Namespace '{}' is not currently enlisted — delist!() requires a prior \
                     enlist!() on the same namespace",
                    namespace
                ),
            }));
        } else {
            debug!("delist_namespace: Delisted namespace '{}'", namespace);
        }

        // DELIST REVOKES BREVITY, NEVER REACH: an alias is an exact route,
        // not an enlistment, so the session's aliases into this namespace
        // stay, and no load's captured world is touched.

        // Explicitly drop the bootstrap connection lock
        drop(bootstrap_conn);

        Ok(())
    }

    /// Snapshot the current enlisted_namespace state.
    /// Returns all (from_namespace_id, to_namespace_id) rows for later restoration.
    pub fn save_enlisted_state(&self) -> Result<Vec<(i32, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for save_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT from_namespace_id, to_namespace_id FROM enlisted_namespace")
            .map_err(|e| {
                Runtime::catalog(
                    "Failed to prepare enlisted_namespace snapshot",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| Runtime::catalog("Failed to snapshot enlisted_namespace", e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore the enlisted_namespace state from a previous snapshot.
    /// Deletes all current rows and re-inserts the saved ones.
    pub fn restore_enlisted_state(&mut self, saved: &[(i32, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for restore_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM enlisted_namespace", [])
            .map_err(|e| {
                Runtime::catalog(
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
                    Runtime::catalog("Failed to restore enlisted_namespace row", e.to_string())
                })?;
        }

        Ok(())
    }

    /// Snapshot the current namespace_alias state.
    /// Returns all (alias, target_namespace_id) rows for later restoration.
    pub fn save_alias_state(&self) -> Result<Vec<(String, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for save_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT alias, target_namespace_id FROM namespace_alias")
            .map_err(|e| {
                Runtime::catalog("Failed to prepare namespace_alias snapshot", e.to_string())
            })?;

        let rows: Vec<(String, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| Runtime::catalog("Failed to snapshot namespace_alias", e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore namespace_alias to a previously saved state.
    pub fn restore_alias_state(&mut self, saved: &[(String, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for restore_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM namespace_alias", [])
            .map_err(|e| {
                Runtime::catalog("Failed to clear namespace_alias for restore", e.to_string())
            })?;

        for (alias, target_id) in saved {
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![alias, target_id],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to restore namespace_alias row", e.to_string())
                })?;
        }

        Ok(())
    }

    /// DELETE a namespace's current load whole — its definition families
    /// (with every sub-table), its declared enlist/alias/exposure edges,
    /// and its liminal ledger — inside the caller's savepoint, so the
    /// replacement that follows lands atomically with the deletion or the
    /// prior load stands untouched.
    fn delete_namespace_load(bootstrap_conn: &Connection, namespace_id: i64) -> Result<()> {
        Self::clear_namespace_contents(bootstrap_conn, namespace_id)?;
        bootstrap_conn
            .execute(
                "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete exposed_namespace", e.to_string()))?;
        Ok(())
    }

    /// Reconsult a lib/scratch namespace by re-reading and re-parsing its source file.
    /// An imprint archive, and anything inside one, refuses as inert.
    ///
    /// Clears all entity definitions and re-loads from the same (or new) source file.
    /// Preserves namespace identity, enlistments, aliases. If grounded namespaces
    /// borrow from this lib, validates the grounding contract and auto-rebuilds.
    pub fn reconsult_namespace(
        &mut self,
        namespace: &str,
        new_file_path: Option<&str>,
    ) -> Result<usize> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // A fresh consultation may not reload a caller-owned namespace as a
        // side effect. During reconsult, the same shape is intentional tree
        // replay: existing children reload atomically under the outer
        // savepoint, so it stays allowed.
        if let Some((_, LiminalProgramKind::Consult)) = self.active_liminal_program() {
            self.refuse_preexisting_namespace_mutation_in_program(
                namespace,
                "reloading",
                |message| crate::diagnostic::Directive::ReconsultUncompensable { message }.into(),
            )?;
        }
        let bootstrap_conn = Catalog::open(
            self,
            "Failed to acquire bootstrap database lock for reconsult",
        )?;

        // 1. The namespace must be LIVE — an imprint archive, and anything
        //    inside one, refuses as inert — and of a kind reconsult! reloads.
        let live = LiveNamespace::admit(&bootstrap_conn, namespace)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!("Namespace '{}' not found", namespace),
                details: "Namespace not found".to_string(),
            })
        })?;
        admit_live_kind(LiveVerb::Reconsult, &live)?;
        let ns_id = i64::from(live.id());
        let source_path: Option<String> = bootstrap_conn
            .query_row(
                "SELECT source_path FROM namespace WHERE id = ?1",
                [ns_id],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("Failed to read the namespace's source path", e))?;

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
                            return Err(DelightQLError::from(Runtime::General {
                                message: format!(
                                    "Cannot determine source file for namespace '{}'. \
                                     Provide a file path: reconsult!(\"ns\", \"path/to/file.dql\")",
                                    namespace
                                ),
                                details: "No source file".to_string(),
                            }));
                        }
                    }
                }
            }
        };

        // 3. Read + parse new file
        drop(bootstrap_conn);

        // A relative path resolves against the base directory in force; there is
        // no fallback to the process directory.
        let resolved_path = self.resolve_path(&file_path)?;
        let file_path = resolved_path.display().to_string();

        let source = std::fs::read_to_string(&file_path).map_err(|e| {
            DelightQLError::from(Runtime::Io {
                message: format!("reconsult!() failed to read file '{}': {}", file_path, e),
            })
        })?;

        // ONE PARSE PER CONSULTED SUBMISSION, reconsult's as much as consult's.
        let consulted = crate::bin_cartridge::prelude::consult::Consulted::read(&source).map_err(
            |e| match e {
                DelightQLError::Parse(_) => Runtime::catalog(
                    format!("reconsult!() failed to parse '{}': {}", file_path, e),
                    "Parse error",
                ),
                other => other,
            },
        )?;

        // The shared loader owns the program savepoint and caller-state
        // restoration. Reconsult supplies only its replacement body.
        crate::bin_cartridge::prelude::consult::run_liminal_program(
            self,
            LiminalProgramKind::Reconsult,
            |this| {
                let self_ = this;

                // The ONE liminal interpreter supplies the same validation,
                // deferral, ordering, receipts, and entity dispatch as consult.
                // Replay mode's only distinction is that an existing nested
                // consult! child reloads.
                let crate::bin_cartridge::prelude::consult::Consulted {
                    forms,
                    ddl_blocks: _,
                } = consulted;
                // A file declaring only liminal directives — a facade that
                // consults and exposes children — is a lawful consult, so it
                // is a lawful reconsult: its load is its lexical graph.
                let load = crate::bin_cartridge::prelude::consult::execute_liminal_forms(
                    self_,
                    forms,
                    namespace,
                    &file_path,
                    crate::bin_cartridge::prelude::consult::LiminalDirectiveMode::Replay,
                )?;

                // 4. PUBLISH THE REPLACEMENT: the load is born in replacement
                // mode, so publication deletes the current load whole, spends
                // the new one, records its source, and rebuilds every
                // dependent derived world — one transaction, one road.
                let published = self_.publish(load)?;
                crate::pipeline::middle::api::judge_declared_heads(
                    self_,
                    namespace,
                    published.relational_families(),
                )?;
                let entity_count = published.definitions_loaded();

                // THE RELOADED FILE'S OWN LEDGER, whole. `delete_namespace_load`
                // dropped the previous one, and the witnesses prove against the
                // definitions this reload just registered — reconsult REPLACES a
                // namespace's ledger, because the record describes THE load.
                let rows = crate::bin_cartridge::prelude::consult::prove_witnesses(
                    self_,
                    namespace,
                    published.into_ledger(),
                )?;
                self_.record_liminal_ledger(namespace, &rows)?;

                debug!(
                    "reconsult_namespace: Reconsulted namespace '{}' from '{}' with {} entities",
                    namespace, file_path, entity_count
                );

                Ok(entity_count)
            },
        )
    }

    /// Test-inspection: the namespace's ledger `operation` column in
    /// insertion (= file-appearance) order; None if no such namespace.
    /// Serves the liminal_ledger_* pins in consult.rs.
    #[cfg(test)]
    pub(crate) fn liminal_ledger_operations(&self, ns_fq: &str) -> Result<Option<Vec<String>>> {
        let conn = self.bootstrap_connection.lock().expect("bootstrap lock");
        let ns_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [ns_fq],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Runtime::catalog("ns lookup", e.to_string()))?;
        let Some(ns_id) = ns_id else { return Ok(None) };
        let mut stmt = conn
            .prepare("SELECT operation FROM liminal_receipt WHERE namespace_id = ?1 ORDER BY id")
            .map_err(|e| Runtime::catalog("ledger read", e.to_string()))?;
        let ops = stmt
            .query_map([ns_id], |row| row.get::<_, String>(0))
            .map_err(|e| Runtime::catalog("ledger read", e.to_string()))?
            .flatten()
            .collect();
        Ok(Some(ops))
    }

    /// Test-inspection: total ledger rows across ALL namespaces — proves an
    /// aborted or unconsulted load left no orphan receipts behind.
    #[cfg(test)]
    pub(crate) fn liminal_receipt_row_count(&self) -> i64 {
        let conn = self.bootstrap_connection.lock().expect("bootstrap lock");
        conn.query_row("SELECT COUNT(*) FROM liminal_receipt", [], |row| row.get(0))
            .unwrap_or(-1)
    }
}
