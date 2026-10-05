// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The definition-registration writer: catalog rows from assembled definition
//! groups for every entrance (a load, an autoload module, the `sys::meta`
//! generator), the laws that read other catalog rows while writing, docs, and
//! `retract!`.

use super::entity_rows;
use super::{ensure_namespace_available, CatalogSavepoint, DelightQLSystem};
use crate::bootstrap::SourceType;
use crate::diagnostic::{Constraint, Ddl, Directive, EffectMain, Runtime};
use crate::error::{DelightQLError, Result};
use log::debug;
use rusqlite::{Connection, OptionalExtension};

/// THE LEXICAL WORLD A LOAD CAPTURES AT ADMISSION, by identity.
pub(super) struct LexicalCapture {
    /// The namespaces a body may select bare after its own namespace misses.
    pub(super) imports: Vec<i64>,
    /// The one-segment qualifier shorthands a body routes through, each with
    /// the namespace it named at admission.
    pub(super) aliases: Vec<(String, i64)>,
}

impl LexicalCapture {
    /// An embedded module's world: its bodies name their own and qualified
    /// entities.
    pub(super) fn none() -> Self {
        LexicalCapture {
            imports: Vec::new(),
            aliases: Vec::new(),
        }
    }
}

/// Record a load's capture beside its cartridge — `None` for a
/// definition-free facade, which mints no cartridge and is read by namespace.
/// Written after the load's families are activated, so the catalog triggers
/// can confirm the load owns its namespace.
fn record_capture_on(
    conn: &Connection,
    namespace_id: i64,
    load: Option<i64>,
    capture: &LexicalCapture,
) -> Result<()> {
    for imported in &capture.imports {
        conn.execute(
            "INSERT OR IGNORE INTO lexical_import \
             (namespace_id, cartridge_id, imported_namespace_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![namespace_id, load, imported],
        )
        .map_err(|e| Runtime::catalog("Failed to record a captured import", e.to_string()))?;
    }
    for (alias, target) in &capture.aliases {
        conn.execute(
            "INSERT INTO namespace_local_alias \
             (namespace_id, cartridge_id, alias, target_namespace_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![namespace_id, load, alias, target],
        )
        .map_err(|e| Runtime::catalog("Failed to record a captured alias", e.to_string()))?;
    }
    Ok(())
}

pub(super) struct ConsultResult {
    /// Number of definitions loaded.
    pub definitions_loaded: usize,
    /// Entity names that were replaced (non-empty only for inline DDL drop-and-replace).
    pub replaced_entities: Vec<String>,
    /// The relational families the load registered, by name.
    pub relational_families: Vec<String>,
}

/// Validate value-function clause discipline for a multi-clause definition —
/// the FUNCTIONAL half of "The Two Algebras".
///
/// # The fork this function sits on
///
/// A colon-functor `f:(…)` is a VALUE FUNCTION: the grammar parses it as
/// `function_definition`/`constant_definition` → `DefKind::Function` → entity
/// type 1 (`DqlFunctionExpression`). Its clauses are ORDERED first-match
/// alternatives, compiled to a CASE expression, BECAUSE a function is
/// deterministic — it must return exactly one value per input.
///
/// A plain-functor `f(…)` (with a boolean body) is a SIGMA PREDICATE: the
/// grammar parses it as `sigma_definition` → `DefKind::Sigma` → entity
/// type 9 (`DqlTemporarySigmaRule`). Its clauses OR together (the RELATIONAL
/// algebra — independent truths about membership) and are never expanded
/// here.
///
/// The two are DISTINCT ENTITY TYPES decided SYNTACTICALLY at parse time (the
/// colon), NOT one definition used context-dependently. So gating this check on
/// `DefKind::Function` is exact: sigma predicates never reach it, and the
/// multi-clause sigma OR (ddl/320) is untouched.
///
/// # The rules
///
/// The ordered-clause law itself — at most ONE unguarded clause, and that
/// clause last — is `judge_clause_order`, the one authority a query-scoped
/// clause family spends too, and the fault's one refusal names its
/// `ddl/head` leaf. A clause is guarded exactly when `Clause::guard` answers
/// one, as the family-body door reads it at every use. Fires even
/// when NO clause is guarded — an entirely unguarded multi-clause group is
/// exactly as ambiguous as a partially-guarded one with two unguarded arms
/// — and so catches duplicate constants (`nl :- …` twice: a constant is a
/// zero-arity value function).
fn validate_function_clause_discipline(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Result<()> {
    use crate::pipeline::asts::core::judge_clause_order;
    use crate::pipeline::asts::ddl::DefKind;

    // Fork gate: only value functions (colon-functors). Sigma predicates and any
    // non-function head are validated on their own paths.
    if group.kind() != DefKind::Function {
        return Ok(());
    }

    // A constant's empty parameter list is vacuously unguarded. Every clause
    // is a function clause: the group is one declared kind by construction.
    let guarded: Vec<bool> = group
        .clauses()
        .iter()
        .map(|clause| clause.guard().is_some())
        .collect();

    judge_clause_order(&guarded).map_err(|fault| fault.refusal(&group.name()))
}

/// ONE EFFECT RULE THIS LOAD REGISTERED, and the directives its clauses
/// demand, for the load's cycle judgment.
struct LoadedEffectRule {
    entity: i64,
    name: String,
    demands: Vec<crate::pipeline::asts::effects::DirectiveInvocation>,
}

impl LoadedEffectRule {
    fn of(entity: i64, group: &crate::pipeline::asts::ddl::DefinitionGroup) -> Result<Self> {
        let rule = crate::pipeline::asts::effects::EffectRule::from_definition_group(group)?;
        let demands = rule
            .clauses
            .iter()
            .flat_map(|clause| {
                crate::pipeline::asts::effects::demanded_directive_names(&clause.body)
            })
            .collect();
        Ok(LoadedEffectRule {
            entity,
            name: rule.name,
            demands,
        })
    }
}

/// NO RECURSION ACROSS ONE LOAD (R6): the effect rules one consultation
/// registered form no cycle. A demand is keyed by the rule it names, never
/// by its spelling: an unqualified user demand names this load's own rule
/// of that name, and a qualified one names the rule its route selects from
/// this load's own declaration world — so `na`'s `ping!` demanding
/// `nb.ping!` demands another rule. A demand spelled like a built-in names
/// the built-in. A cycle that passes through another load's rule is refused
/// by the invocation's re-entry guard at use, since that rule may not exist
/// when this load is consulted; a rule's unqualified demand of itself is
/// refused where its family is assembled.
fn judge_load_recursion(
    conn: &Connection,
    namespace_id: i64,
    rules: &[LoadedEffectRule],
) -> Result<()> {
    use crate::pipeline::asts::effects::DirectiveCategory;
    use std::collections::HashMap;
    type RuleKey = (i64, String);

    let Some(first) = rules.first() else {
        return Ok(());
    };
    let mut graph: HashMap<RuleKey, Vec<RuleKey>> = HashMap::new();
    for rule in rules {
        let mut demanded = Vec::new();
        for demand in &rule.demands {
            if demand.category != DirectiveCategory::User {
                continue;
            }
            let namespace = match &demand.qualifier {
                None => Some(namespace_id),
                Some(qualifier) => {
                    crate::pipeline::middle::api::route_in_body(conn, first.entity, qualifier)?
                }
            };
            if let Some(namespace) = namespace {
                demanded.push((namespace, demand.name.clone()));
            }
        }
        graph.insert((namespace_id, rule.name.clone()), demanded);
    }

    fn visit<'a>(
        node: &'a RuleKey,
        graph: &'a HashMap<RuleKey, Vec<RuleKey>>,
        path: &mut Vec<&'a RuleKey>,
    ) -> Option<Vec<String>> {
        if let Some(pos) = path.iter().position(|n| *n == node) {
            let mut cycle: Vec<String> = path[pos..].iter().map(|(_, name)| name.clone()).collect();
            cycle.push(node.1.clone());
            return Some(cycle);
        }
        let (key, demands) = graph.get_key_value(node)?;
        path.push(key);
        for next in demands {
            if graph.contains_key(next) {
                if let Some(cycle) = visit(next, graph, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        None
    }

    for rule in rules {
        let key = (namespace_id, rule.name.clone());
        let mut path = Vec::new();
        if let Some(cycle) = visit(&key, &graph, &mut path) {
            return Err(crate::pipeline::asts::effects::effect_recursion(
                &rule.name,
                &format!("through {}", cycle.join(" -> ")),
            ));
        }
    }
    Ok(())
}

impl DelightQLSystem {
    /// Register one source's definitions into a namespace, inside the
    /// caller's catalog savepoint. `replacing` is the reconsult window: the
    /// namespace's previous definitions were deleted in the same savepoint,
    /// so the replacement lands whole or not at all.
    pub(super) fn consult_file_inner(
        bootstrap_conn: &Connection,
        path: &str,
        namespace: &str,
        definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
        count: usize,
        default_data_ns: Option<&str>,
        replacing: bool,
        capture: &LexicalCapture,
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
                    Runtime::catalog("Failed to check namespace existence", e.to_string())
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
                            let (ns_kind, ns_source): (Option<String>, Option<String>) =
                                bootstrap_conn
                                    .query_row(
                                        "SELECT kind, source_path FROM namespace WHERE id = ?1",
                                        [id],
                                        |row| Ok((row.get(0)?, row.get(1)?)),
                                    )
                                    .unwrap_or((None, None));
                            let ns_kind = crate::namespace::NamespaceKind::decode(
                                namespace,
                                ns_kind.as_deref(),
                            )?;
                            let source_info = ns_source
                                .map(|s| format!(" (from {})", s))
                                .unwrap_or_default();
                            return Err(DelightQLError::from(Runtime::General {
                                details: "definition target".to_string(),
                                message: format!(
                                    "Cannot write definitions to namespace '{}' — \
                                     it is a {} namespace{} and is not writable. \
                                     Use (~~ddl:\"name\" ~~) to create a scratch namespace instead.",
                                    namespace, ns_kind, source_info
                                ),
                            }));
                        }
                    }

                    // ONE CONSULTED SOURCE OWNS ONE NAMESPACE. A second
                    // source landing in a namespace that already holds
                    // authored definitions is cross-source append, whatever
                    // entrance reached here — the surface directive died
                    // with consult_concat_into_ns!, and the registration
                    // writer refuses the capability itself. The lawful
                    // arrivals at an existing namespace row are its FIRST
                    // source (pre-created hierarchy and seeded module rows
                    // hold no definitions), the replacement window
                    // (reconsult, which deleted the prior definitions
                    // first), and scratch inline blocks (write protection
                    // above guards those).
                    // A structural node an explicit consultation claims
                    // takes the library's backing; its identity and children
                    // stay.
                    if path != "(inline)" && !replacing {
                        bootstrap_conn
                            .execute(
                                "UPDATE namespace SET kind = 'lib', provenance = 'file', source_path = ?2
                                 WHERE id = ?1 AND (kind IS NULL OR kind = 'unknown') AND source_path IS NULL",
                                rusqlite::params![id, path],
                            )
                            .map_err(|e| Runtime::catalog("Failed to claim a structural namespace", e.to_string()))?;
                    }
                    if path != "(inline)" && !replacing {
                        let has_source: bool = bootstrap_conn
                            .query_row(
                                "SELECT EXISTS(
                                     SELECT 1 FROM activated_entity ae
                                     JOIN entity e ON e.id = ae.entity_id
                                     WHERE ae.namespace_id = ?1
                                       AND e.type IN (1, 2, 3, 4, 8, 9, 16, 17, 20))",
                                [id],
                                |row| row.get(0),
                            )
                            .unwrap_or(false);
                        if has_source {
                            return Err(DelightQLError::from(Directive::ConsultExists {
    message: format!(
                                    "consult! creates namespace '{namespace}' from one source, and it \
                                     already holds one. Reload the same source with \
                                     reconsult!(\"{namespace}\") or remove it first with \
                                     unconsult!(\"{namespace}\") — one consulted source owns one \
                                     namespace, and a second consult is never a merge"
                                ),
}));
                        }
                    }
                    id
                }
                None => {
                    ensure_namespace_available(&bootstrap_conn, namespace)?;
                    super::create_structural_prefixes(bootstrap_conn, namespace)?;
                    use crate::namespace::NamespaceKind;
                    let (ns_kind, ns_provenance, ns_source, ns_writable) = if path == "(inline)" {
                        (NamespaceKind::Scratch, "scratch", None, 1i32)
                    } else if path.starts_with("embedded://") {
                        (NamespaceKind::System, "bootstrap", Some(path), 0i32)
                    } else {
                        (NamespaceKind::Lib, "file", Some(path), 0i32)
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
                                ns_kind.spelling(),
                                ns_provenance,
                                ns_source,
                                ns_writable
                            ],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to create consult namespace", e.to_string())
                        })?;
                    bootstrap_conn.last_insert_rowid() as i32
                }
            }
        };

        // For inline DDL: drop-and-replace conflicting entities by name.
        // Only entities whose names match a definition in the new DDL block are
        // removed; other entities from earlier inline blocks are preserved.
        let replaced_entities: Vec<String> = if path == "(inline)" {
            // Collect entity names from the incoming clauses
            let new_names_deduped: std::collections::HashSet<String> =
                definitions.iter().map(|d| d.front().name()).collect();

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
                            Runtime::catalog(
                                "Failed to query conflicting inline entities",
                                e.to_string(),
                            )
                        })?;
                    let rows = stmt
                        .query_map(rusqlite::params![name, namespace_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .map_err(|e| {
                            Runtime::catalog(
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
                    entity_rows::retire_entity(bootstrap_conn, *entity_id)?;

                    // Clean up cartridge if it has no remaining entities
                    let remaining: i64 = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) FROM entity WHERE cartridge_id = ?1",
                            [cartridge_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(1);
                    if remaining == 0 {
                        // The replaced load's capture goes with its emptied
                        // cartridge; the replacing block records its own.
                        entity_rows::retire_load(bootstrap_conn, *cartridge_id)?;
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

        // A load without definitions — a facade file of liminal directives
        // only — registers no families and mints no cartridge: the lifecycle
        // reaches a cartridge only through the entities activated under it,
        // so an entity-free one would be a row nothing could ever remove.
        // Its captured imports carry a NULL load and are read by namespace,
        // sound because a facade is its namespace's sole load; recorded here
        // so the facade's own liminal goals reach them.
        if definitions.is_empty() {
            record_capture_on(bootstrap_conn, i64::from(namespace_id), None, capture)?;
            return Ok(ConsultResult {
                definitions_loaded: 0,
                replaced_entities,
                relational_families: Vec::new(),
            });
        }

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
                    Runtime::catalog("Failed to insert consult cartridge", e.to_string())
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Group clauses by the SUBJECT'S OWN IDENTITY to support disjunctive
        // definitions. Multiple clauses under one subject (multi-clause sigma
        // predicates, guarded functions) become a single entity whose per-
        // clause sources are stored in authored order. Keying on the catalog
        // SPELLING instead would fold nothing: `Counter` and `counter` are
        // one unstropped name by the identifier law, and registering them
        // apart leaves two entity rows the unqualified lookup reaches at
        // once. The liminal ledger's DEFINE rows read the same authority.
        let families = crate::pipeline::asts::ddl::ClauseFamily::gather(definitions);

        // This load's effect rules and what each demands, judged for cycles
        // once the whole load stands (the consult is one transaction — a
        // refusal rolls back every registration). See `judge_load_recursion`.
        let mut effect_rules: Vec<LoadedEffectRule> = Vec::new();
        let mut relational_families: Vec<String> = Vec::new();

        for family in families {
            let subject = family.subject().catalog_name();
            if family.clauses().len() > 1 {
                debug!(
                    "consult_file: Grouping {} clauses for '{}' into single entity",
                    family.clauses().len(),
                    subject
                );
            }

            // THE CLAUSES ARE ALREADY BUILT. Assembly runs over the
            // declarations this consultation normalized, so registration
            // parses nothing it has already read. The catalog stores the
            // AUTHORED clause sources; a family holding facts is judged by
            // the compile road where this load is published.
            let clause_sources: Vec<String> = family
                .clauses()
                .iter()
                .map(|d| d.full_source().to_string())
                .collect();
            let ddl_group =
                crate::pipeline::asts::ddl::DefinitionGroup::assemble(family).map_err(|e| {
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) propagate directly to
                    // preserve their specific URI subcategory.
                    if matches!(&e, DelightQLError::Semantic(_)) {
                        return e;
                    }
                    DelightQLError::from(Constraint::General {
                        message: format!("DDL definition '{subject}' has an invalid body: {e}"),
                    })
                })?;
            // THE BODY LAWS, where a consulted family is declared, and where
            // its bodies declare their own effect families: the new middle's
            // judgment, the one both necks meet.
            crate::pipeline::middle::api::judge_declared_family(
                &ddl_group,
                ddl_group.kind() == crate::pipeline::asts::ddl::DefKind::Effect,
            )?;
            if matches!(
                ddl_group.kind(),
                crate::pipeline::asts::ddl::DefKind::View
                    | crate::pipeline::asts::ddl::DefKind::HoView
                    | crate::pipeline::asts::ddl::DefKind::Fact
            ) {
                relational_families.push(ddl_group.name().to_string());
            }

            // foo/foo! name collision: a namespace may not hold both a
            // functor `foo` and
            // an effect rule `foo!`. Enforced at registration time in BOTH
            // directions, before either insert branch below (normal and
            // deferred-HO alike). Same-file collisions are caught too:
            // earlier groups of this consult are already inserted and
            // activated on this connection when later groups arrive. This is
            // the prerequisite for making doc! targets explicit — the `!`
            // fallback in consult_body stays sound only while the two names
            // cannot coexist. Pinned by effects-ball
            // rules--47_name_collision_effect_second and
            // rules--48_name_collision_entity_second.
            {
                let group_name = ddl_group.name();
                let group_name = group_name.as_str();
                super::refuse_entity_named_like_child(bootstrap_conn, namespace, group_name)?;
                if let Some(base) = group_name.strip_suffix('!') {
                    // The effect rule arrives second: refuse if ANY entity
                    // named `foo` (view/table/function/fact/…) is active in
                    // this namespace — the whole functor namespace collides.
                    let base_exists: bool = bootstrap_conn
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE e.name = ?1 AND ae.namespace_id = ?2)",
                            rusqlite::params![base, namespace_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                    if base_exists {
                        return Err(DelightQLError::from(
                            crate::diagnostic::EffectRule::NameCollision {
                                message: format!(
                                    "cannot register effect rule '{}': namespace '{}' \
                                 already holds an entity named '{}' — a namespace \
                                 may not hold both '{}' and '{}'.",
                                    group_name, namespace, base, base, group_name
                                ),
                            },
                        ));
                    }
                } else {
                    // The plain entity arrives second: refuse if an EFFECT
                    // RULE named `foo!` (entity type 20) is active in this
                    // namespace. Restricted to type 20 so `!`-named built-in
                    // pseudo-predicates can never block a plain name.
                    let banged = format!("{}!", group_name);
                    let effect_rule_exists: bool = bootstrap_conn
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE e.name = ?1 AND e.type = 20 AND ae.namespace_id = ?2)",
                            rusqlite::params![&banged, namespace_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                    if effect_rule_exists {
                        return Err(DelightQLError::from(
                            crate::diagnostic::EffectRule::NameCollision {
                                message: format!(
                                    "cannot register '{}': namespace '{}' already \
                                 holds an effect rule '{}' — a namespace may not \
                                 hold both '{}' and '{}'.",
                                    group_name, namespace, banged, group_name, banged
                                ),
                            },
                        ));
                    }
                }
            }

            // Subject, declared kind, parameter arity, the whole head algebra
            // and the effect-body laws — `main!`'s one clause among them —
            // were decided by `DefinitionGroup::assemble` before this group
            // existed. What remains here is discipline no assembler can own —
            // clause ORDER for value functions, and the namespace's one
            // program.

            // Value-function clause discipline: at most one unguarded clause
            // (RULE 2, the fix for the all-unguarded defect that the old
            // `has_any_guard` gate let slip through) and the unguarded clause
            // must be last. Gated on DefKind::Function inside the helper, so
            // sigma predicates (the relational OR path) are untouched.
            validate_function_clause_discipline(&ddl_group)?;

            let group_name = ddl_group.name();

            // F2: main! is its namespace's program — the rule `run!` and
            // `run_namespace!` demand — so a namespace has at most one. A
            // second file consulted into the same namespace must not smuggle
            // in another main!.
            if group_name == "main!" {
                let already_has_main: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM entity e
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE e.name = 'main!' AND ae.namespace_id = ?1)",
                        [namespace_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);
                if already_has_main {
                    return Err(DelightQLError::from(EffectMain::Duplicate {
                        message: format!(
                            "namespace '{}' already has a main! — at most one main! \
                             per namespace (EFFECT-ALGEBRA F2).",
                            namespace
                        ),
                    }));
                }
            }

            debug!(
                "consult_file: Registering {:?} '{}' ({} clause{})",
                ddl_group.kind(),
                group_name,
                ddl_group.clauses().len(),
                if ddl_group.clauses().len() > 1 {
                    "s"
                } else {
                    ""
                }
            );

            // The catalog reads the GROUP's identity. Nothing here picks a
            // clause and nothing here re-parses a head: everything below is
            // what the assembler already made every clause agree on, and a
            // deferred body does not make a group any less assembled.
            let entity_type = ddl_group.entity_type().as_i32();
            // Insert entity (without definition — clauses go into entity_clause).
            // The name's strop bit is identity, so the catalog keeps it.
            let name_stropped = ddl_group
                .name_identifier()
                .is_some_and(delightql_types::SqlIdentifier::is_stropped);
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, name_stropped, type, cartridge_id, doc) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        &group_name,
                        name_stropped,
                        entity_type,
                        cartridge_id,
                        &ddl_group.doc(),
                    ],
                )
                .map_err(|e| Runtime::catalog("Failed to insert consult entity", e.to_string()))?;
            let entity_id = bootstrap_conn.last_insert_rowid() as i32;
            if ddl_group.kind() == crate::pipeline::asts::ddl::DefKind::Effect {
                effect_rules.push(LoadedEffectRule::of(i64::from(entity_id), &ddl_group)?);
            }

            // Insert each clause into entity_clause: each authored clause's
            // source, in authored order.
            for (ordinal, src) in clause_sources.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, ?2, ?3)",
                        rusqlite::params![entity_id, (ordinal + 1) as i32, src],
                    )
                    .map_err(|e| {
                        Runtime::catalog("Failed to insert entity clause", e.to_string())
                    })?;
            }

            // Record input parameters as entity attributes. The parameter's
            // ROLE is a catalog fact: a `f:()`-spelled code formal writes
            // 'code_param', a value formal 'input_param' — so a call site
            // can partition its members and resolve its actuals BEFORE the
            // body is admitted and opened. The row is the family's declared
            // row, whatever order its clauses were written in.
            {
                use crate::pipeline::asts::ddl::HoParam;
                let signature =
                    crate::pipeline::resolver::grounding::FamilySignature::of(&ddl_group)?;
                for (position, param) in signature.params().iter().enumerate() {
                    let HoParam::Scalar { name, callable, .. } = param else {
                        continue;
                    };
                    let attribute_type = if *callable {
                        "code_param"
                    } else {
                        "input_param"
                    };
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![entity_id, name.as_str(), attribute_type, position],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to insert entity attribute", e.to_string())
                        })?;
                }
            }

            // A relational rule with a declared head and no parameter row
            // publishes the head's names as its columns (FN.2: the catalog
            // answers as data), as the new middle decides the family's
            // heading from its heads. A head column is not a stored table's
            // output column, which table resolution reads. A parameterized
            // rule's attribute rows are its parameters, by position; its
            // head names would stand at the same positions.
            if ddl_group.kind() == crate::pipeline::asts::ddl::DefKind::View {
                let names = crate::pipeline::middle::api::declared_columns(&ddl_group)?.unwrap_or_default();
                {
                    for (position, name) in names.iter().enumerate() {
                        bootstrap_conn
                            .execute(
                                "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, 'head_column', ?3)",
                                rusqlite::params![entity_id, name.as_str(), position],
                            )
                            .map_err(|e| Runtime::catalog("Failed to insert head column", e.to_string()))?;
                    }
                }
            }

            // For HO views, write structured param metadata with cross-clause position analysis
            {
                let positions =
                    crate::pipeline::resolver::grounding::build_ho_position_analysis(&ddl_group);
                if !positions.is_empty() {
                    Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
                }
            }

            // For edges, write the selection keys to join_edge: the two
            // ground terms as NAKED canonical spellings plus the context
            // symbol — the match key and the stored key are the same
            // bytes. The subject holds the pair in sorted order, which is
            // what makes lookup symmetric.
            if let crate::pipeline::asts::ddl::DefSubject::Edge {
                left,
                right,
                context,
            } = ddl_group.subject()
            {
                for idx in 0..ddl_group.clauses().len() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO join_edge (entity_id, left_spelling, right_spelling, context_name, clause_ordinal) VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![entity_id, left, right, context, (idx + 1) as i32],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to insert join_edge", e.to_string())
                        })?;
                }
            }

            // For a fact function, write THE DECLARED MODE. Callable
            // selection consumes these typed rows; relational capability is
            // already fixed in the group's entity type.
            if let Some(mode) = ddl_group.declared_mode() {
                Self::write_functional_dependency(bootstrap_conn, entity_id as i64, mode)?;
            }

            // Extract references from ALL clauses (union of references)
            {
                // ONE CENSUS for the whole group: every clause, under the
                // group's own declared parameters and every declaration
                // each body opens inside itself. A body it cannot read
                // refuses here rather than being recorded as a definition
                // with no dependencies.
                let all_refs = crate::ddl::analyzer::census_of_group(&ddl_group)
                    .finish()
                    .map_err(|e| {
                        DelightQLError::from(Constraint::General {
                            message: format!("HO view '{group_name}' body has a syntax error: {e}"),
                        })
                    })?;

                for ext_ref in &all_refs {
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
                            Runtime::catalog("Failed to insert referenced entity", e.to_string())
                        })?;
                }

                debug!(
                    "consult_file: Extracted {} references from '{}' ({} clause{})",
                    all_refs.len(),
                    group_name,
                    ddl_group.clauses().len(),
                    if ddl_group.clauses().len() > 1 {
                        "s"
                    } else {
                        ""
                    }
                );
            }

            // Register interior schemas for tree group columns
            {
                use crate::pipeline::asts::ddl::DdlBody;
                for ddl_def in ddl_group.clauses() {
                    if let DdlBody::Relational(query) = &ddl_def.body {
                        register_interior_schemas_from_query(bootstrap_conn, entity_id, query)?;
                    }
                }
            }

            // Activate in namespace. ONE canonical name identifies one
            // definition family in a namespace: the judgment is a catalog
            // read made here, as the clause-agreement teaching. The store's
            // `definition_family_identity` trigger guards the same law for
            // every other road and is never read back as prose.
            let same_named_family: bool = bootstrap_conn
                .query_row(
                    "SELECT EXISTS (
                         SELECT 1 FROM activated_entity ae
                         JOIN entity e ON e.id = ae.entity_id
                         WHERE ae.namespace_id = ?2
                           AND (SELECT type FROM entity WHERE id = ?1) IN (1, 2, 3, 4, 8, 9, 16, 17, 20)
                           AND e.type IN (1, 2, 3, 4, 8, 9, 16, 17, 20)
                           AND (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END)
                               = (SELECT CASE WHEN name_stropped = 1 THEN name ELSE lower(name) END
                                  FROM entity WHERE id = ?1))",
                    rusqlite::params![entity_id, namespace_id],
                    |row| row.get(0),
                )
                .map_err(|e| Runtime::catalog("Failed to read the definition families of a namespace", e))?;
            if same_named_family {
                return Err(DelightQLError::from(Ddl::FamilyOneNameOneEntity {
                    message: format!(
                        "'{group_name}' is already defined in this source — one fully \
                         qualified name identifies one entity, and category or arity \
                         never selects among same-named definitions (heads-law, CLAUSE \
                         AGREEMENT). Same-kind clauses of one entity belong under one \
                         head; a different definition needs a different name."
                    ),
                }));
            }
            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, namespace_id, cartridge_id],
                )
                .map_err(|e| Runtime::catalog("Failed to activate consult entity", e))?;
        }

        // THE LOAD'S CAPTURED IMPORTS, keyed by THIS cartridge — recorded
        // AFTER its families are activated, so the catalog trigger can
        // confirm the load owns its namespace. Two inline blocks in one
        // scratch namespace each get their own rows here, so a body reads
        // the imports its OWN load captured; they survive a session delist
        // and are replaced whole when the namespace's load is reconsulted.
        record_capture_on(
            bootstrap_conn,
            i64::from(namespace_id),
            Some(i64::from(cartridge_id)),
            capture,
        )?;

        // NO RECURSION ACROSS THIS LOAD, judged once the load stands: its
        // families are activated and its captured world is recorded, so a
        // qualified demand routes here as a body of this load routes it at
        // use. Forward references between the load's rules are legal.
        judge_load_recursion(bootstrap_conn, i64::from(namespace_id), &effect_rules)?;

        debug!(
            "consult_file: Successfully loaded {} definitions into '{}'",
            count, namespace
        );
        Ok(ConsultResult {
            definitions_loaded: count,
            replaced_entities,
            relational_families,
        })
    }

    /// RETRACT THE FAMILY A COMPILED STATEMENT SELECTED: the entity
    /// `entity_id`, activated in `namespace` under `name`. The act removes it
    /// whole — every clause, its activation, its load's cartridge when
    /// nothing else stands under it — and refuses what the world forbids: a
    /// family that is not a session-authored definition in a writable
    /// namespace, one a grounding borrows, one another definition's mention
    /// selects.
    pub(crate) fn retract_family(&mut self, entity_id: i64, namespace: &str, name: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let display = format!("{namespace}.{name}");
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for retract",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let found: Option<(i64, i32, String, bool)> = conn
            .query_row(
                "SELECT e.cartridge_id, e.type, c.source_uri, n.writable
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE e.id = ?1 AND n.fq_name = ?2",
                rusqlite::params![entity_id, namespace],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get::<_, i32>(3)? != 0)),
            )
            .optional()
            .map_err(|e| Runtime::catalog("retract ownership lookup", e.to_string()))?;
        let Some((cartridge_id, kind, source_uri, writable)) = found else {
            return Err(DelightQLError::from(Directive::RetractMissing {
                message: format!("retract!({display}) selects nothing: '{display}' is no longer defined"),
            }));
        };
        let authored = crate::enums::EntityType::from_i32(kind).is_ok_and(|k| k.is_authored_definition());
        // A block a consulted file declares stands beneath that file's
        // library and is its source's, however its load was registered.
        let consulted_above: Vec<String> = {
            let mut statement = conn
                .prepare("SELECT fq_name FROM namespace WHERE kind = 'lib' AND provenance = 'file' AND fq_name IS NOT NULL")
                .map_err(|e| Runtime::catalog("retract source lookup", e.to_string()))?;
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| Runtime::catalog("retract source lookup", e.to_string()))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| Runtime::catalog("retract source lookup", e.to_string()))?
        };
        let beneath_a_source = consulted_above.iter().any(|library| crate::namespace::is_under(namespace, library));
        if !authored || source_uri != "file://(inline)" || !writable || beneath_a_source {
            return Err(DelightQLError::from(Directive::RetractOwnership {
                message: format!(
                    "retract!({display}) selects {display}, which was not authored in this session: a consulted \
                     source's definition is replaced by reconsult! or removed with unconsult!, never retracted"
                ),
            }));
        }
        let borrowed: bool = conn
            .query_row(
                "SELECT EXISTS (
                     SELECT 1 FROM grounding g
                     JOIN namespace n ON n.id = g.lib_namespace_id
                     WHERE n.fq_name = ?1)",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("retract borrow lookup", e.to_string()))?;
        if borrowed {
            return Err(DelightQLError::from(Directive::RetractDependency {
                message: format!("retract!({display}) refuses: a grounding borrows '{namespace}', which holds {display}"),
            }));
        }
        let dependents = crate::pipeline::middle::api::dependents(&*conn, entity_id, name)?;
        if !dependents.is_empty() {
            return Err(DelightQLError::from(Directive::RetractDependency {
                message: format!(
                    "retract!({display}) refuses: {} depend{} on {display}",
                    dependents.join(", "),
                    if dependents.len() == 1 { "s" } else { "" }
                ),
            }));
        }
        let transaction = CatalogSavepoint::begin(&conn, "dql_retract", "begin retract")?;
        entity_rows::retire_entity(&conn, entity_id)?;
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM entity WHERE cartridge_id = ?1",
                [cartridge_id],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("retract cartridge census", e.to_string()))?;
        if remaining == 0 {
            // The family's own load goes WHOLE — its captured imports with
            // its cartridge, so no capture outlives the load that owns it.
            entity_rows::retire_load(&conn, cartridge_id)?;
        }
        transaction.commit("commit retract")?;
        Ok(())
    }

    /// Write HO parameter metadata to bootstrap from cross-clause position analysis.
    ///
    /// Inserts rows into ho_param and ho_param_column.
    /// based on the unified HoPositionInfo computed by `build_ho_position_analysis`.
    fn write_ho_params_to_bootstrap(
        bootstrap_conn: &Connection,
        entity_id: i32,
        positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    ) -> Result<()> {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundPattern};

        for pos_info in positions {
            let kind_str = match &pos_info.column_kind {
                HoColumnKind::TableGlob => "glob",
                HoColumnKind::TableArgumentative(_) => "argumentative",
                HoColumnKind::Rule(_) => "rule",
                HoColumnKind::Scalar => match &pos_info.ground_pattern {
                    Some(HoGroundPattern::AllClauses) => "ground_scalar",
                    Some(HoGroundPattern::SomeClauses) | None => "scalar",
                },
            };

            // The declared identifier names the row, strop bit beside its
            // bytes; a position no clause names falls back to its ordinal.
            let param_name_owned;
            let (param_name, stropped) = match &pos_info.column_name {
                Some(name) => (name.as_str(), name.is_stropped()),
                None => {
                    param_name_owned = format!("_pos{}", pos_info.position);
                    (param_name_owned.as_str(), false)
                }
            };
            let column_name = pos_info.column_name.as_ref().map(|name| name.as_str());

            bootstrap_conn
                .execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, column_name, stropped) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![entity_id, param_name, pos_info.position as i32, kind_str, column_name, stropped],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to insert ho_param", e.to_string())
                })?;
            let ho_param_id = bootstrap_conn.last_insert_rowid() as i32;

            // Write argumentative columns
            if let HoColumnKind::TableArgumentative(ref columns) = pos_info.column_kind {
                for (col_pos, col_name) in columns.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO ho_param_column (ho_param_id, column_name, column_position, stropped) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![ho_param_id, col_name.as_str(), col_pos as i32, col_name.is_stropped()],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to insert ho_param_column", e.to_string())
                        })?;
                }
            }
        }

        Ok(())
    }

    /// THE DECLARED MODE, written as typed rows.
    ///
    /// Ordered by role and position, with the authored identifier's stropping
    /// bit beside its bytes — a stropped name compares verbatim and an
    /// unstropped one folds, and a pick agrees with a declared output only by
    /// that comparison.
    fn write_functional_dependency(
        conn: &Connection,
        entity_id: i64,
        mode: &crate::pipeline::asts::core::FactFunctionMode<
            crate::pipeline::asts::core::Unresolved,
        >,
    ) -> Result<()> {
        let rows = mode
            .inputs
            .iter()
            .map(|name| ("input", name))
            .enumerate()
            .chain(mode.outputs.iter().map(|name| ("output", name)).enumerate());
        for (position, (role, name)) in rows {
            conn.execute(
                "INSERT INTO functional_dependency \
                 (entity_id, role, position, attribute_name, stropped) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    role,
                    position as i64,
                    name.as_str(),
                    name.is_stropped() as i64,
                ],
            )
            .map_err(|e| {
                Runtime::catalog("Failed to insert functional_dependency", e.to_string())
            })?;
        }
        Ok(())
    }

    /// Set the `doc` string on a catalog entity, addressed by its
    /// fully-qualified name (e.g. `"sys::identifiers.identifier"`).
    ///
    /// The fq name is `<namespace fq_name>.<entity name>`, so it is matched
    /// against `n.fq_name || '.' || e.name` over activated entities — the same
    /// namespace/activated_entity join every other catalog lookup uses. Only
    /// activated entities are considered; if the name still resolves to more
    /// than one entity (a same-name collision within one namespace across
    /// cartridges) it is reported as ambiguous.
    ///
    /// Session-scoped: writes the in-memory bootstrap catalog for this session.
    /// Conn-level doc write: the deferred
    /// liminal doc!s apply INSIDE the consult transaction. Same candidate
    /// resolution as `set_entity_doc`, against the provided connection.
    pub(super) fn set_entity_doc_on(
        conn: &rusqlite::Connection,
        target: &str,
        doc: &str,
    ) -> Result<()> {
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT e.id FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE n.fq_name || '.' || e.name = ?1",
            )
            .map_err(|e| {
                Runtime::catalog("Failed to prepare entity lookup for doc!()", e.to_string())
            })?;
        let ids: Vec<i64> = stmt
            .query_map([target], |row| row.get(0))
            .map_err(|e| Runtime::catalog("Failed to resolve doc!() target", e.to_string()))?
            .collect::<std::result::Result<Vec<i64>, _>>()
            .map_err(|e| Runtime::catalog("Failed to resolve doc!() target", e.to_string()))?;
        drop(stmt);
        match ids.as_slice() {
            [] => Err(DelightQLError::from(Runtime::General {
                message: format!("no such entity '{}'", target),
                details: "doc!() target not found".to_string(),
            })),
            [entity_id] => {
                conn.execute(
                    "UPDATE entity SET doc = ?1 WHERE id = ?2",
                    rusqlite::params![doc, entity_id],
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!("Failed to set doc on entity '{}'", target),
                        e.to_string(),
                    )
                })?;
                Ok(())
            }
            many => Err(Runtime::catalog(
                format!(
                    "ambiguous doc!() target '{}' resolves to {} entities",
                    target,
                    many.len()
                ),
                "Ambiguous entity reference",
            )),
        }
    }

    pub(crate) fn set_entity_docs_atomic(
        &mut self,
        entries: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|error| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for doc! batch",
                format!("Connection was poisoned: {error}"),
            )
        })?;
        bootstrap_conn
            .execute_batch("BEGIN")
            .map_err(|error| Runtime::catalog("doc! batch BEGIN", error.to_string()))?;
        for (target, doc) in entries {
            if let Err(error) = Self::set_entity_doc_on(&bootstrap_conn, target, doc) {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        }
        bootstrap_conn
            .execute_batch("COMMIT")
            .map_err(|error| Runtime::catalog("doc! batch COMMIT", error.to_string()))?;
        Ok(entries.to_vec())
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
    use crate::pipeline::ast_visit::walk_visit_query;

    let mut registrar = InteriorSchemaRegistrar { conn, entity_id };
    walk_visit_query(&mut registrar, query)?;
    Ok(())
}

struct InteriorSchemaRegistrar<'a> {
    conn: &'a Connection,
    entity_id: i32,
}

impl crate::pipeline::ast_visit::AstVisit<crate::pipeline::asts::core::Unresolved>
    for InteriorSchemaRegistrar<'_>
{
    /// A tree group's interior schema is filed under the name its PUBLICATION
    /// gave it. An unnamed one files nothing: there is no name to file it
    /// under, and inventing one would answer a question the author declined.
    fn enter_out_item(
        &mut self,
        item: &crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::ast_visit::Descent;
        use crate::pipeline::asts::core::{DomainExpression, FunctionApplication, OutItem};

        let OutItem::One(one) = item else {
            return Ok(Descent::Continue);
        };
        let Some(naming) = one.naming.as_ref() else {
            return Ok(Descent::Continue);
        };
        if let DomainExpression::Application(FunctionApplication::Enclyph(
            crate::pipeline::asts::core::Enclyph::Record(record),
        )) = &one.expr
        {
            register_tree_group(self.conn, self.entity_id, &record.members, naming.as_str())?;
            // register_record_members owns the nested tree-group recursion for
            // this schema. Do not register a nested record a second time as if
            // it were another top-level result column.
            return Ok(Descent::SkipSubtree);
        }
        Ok(Descent::Continue)
    }
}

/// Register one aliased RECORD discovered anywhere in the unresolved query.
fn register_tree_group(
    conn: &Connection,
    entity_id: i32,
    members: &[crate::pipeline::asts::core::RecordMember<
        crate::pipeline::asts::core::Unresolved,
    >],
    alias: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
        rusqlite::params![entity_id, alias],
    )
    .map_err(|e| Runtime::catalog("Failed to insert interior_entity", e.to_string()))?;
    let interior_entity_id = conn.last_insert_rowid() as i32;

    register_record_members(conn, interior_entity_id, entity_id, members)?;

    Ok(())
}

/// Register record members as interior_entity_attribute rows.
/// Handles nesting: an induced member is a level of its own, and recurses.
fn register_record_members(
    conn: &Connection,
    interior_entity_id: i32,
    parent_entity_id: i32,
    members: &[crate::pipeline::asts::core::RecordMember<
        crate::pipeline::asts::core::Unresolved,
    >],
) -> Result<()> {
    use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember};

    for (position, member) in members.iter().enumerate() {
        match member {
            RecordMember::SelfKeyed(NamedReference(column)) => {
                let column = &column.name;
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, column.as_str(), position as i32],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to insert interior_entity_attribute", e.to_string())
                })?;
            }
            RecordMember::Induced { key, value } => {
                {
                    // Nested tree group: create a child interior_entity
                    if let Enclyph::Record(child) = value.as_ref() {
                        let child_members = &child.members;
                        // Insert child interior_entity (no alias needed for nested)
                        conn.execute(
                            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
                            rusqlite::params![parent_entity_id, key.as_str()],
                        )
                        .map_err(|e| {
                            Runtime::catalog("Failed to insert child interior_entity", e.to_string())
                        })?;
                        let child_interior_entity_id = conn.last_insert_rowid() as i32;

                        // Register child members recursively
                        register_record_members(
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
                            Runtime::catalog("Failed to insert interior_entity_attribute (nested)", e.to_string())
                        })?;
                    }
                }
            }
            RecordMember::Keyed { key, .. } => {
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, key.as_str(), position as i32],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to insert interior_entity_attribute", e.to_string())
                })?;
            }
            // A metadata member's interior keys are data, so there is no
            // static child heading to register; the attribute row alone.
            RecordMember::Metadata { key, .. } => {
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, key.as_str(), position as i32],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to insert interior_entity_attribute", e.to_string())
                })?;
            }
            // A spread is spent at resolution and this registrar reads the
            // authored tree, so the columns it stands for are not yet known.
            RecordMember::Spread(_) => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod function_clause_discipline_tests {
    //! The FUNCTIONAL half of "The Two Algebras". A value function's clauses are
    //! ordered first-match alternatives — at most one may be unguarded (the
    //! default), and it must be last. The chokepoint is
    //! `validate_function_clause_discipline`, gated on `DefKind::Function`, so
    //! sigma predicates (the relational OR path) are exempt.
    use super::validate_function_clause_discipline;
    use crate::ddl::reconstruct;

    fn discipline(source: &str) -> crate::error::Result<()> {
        let group = reconstruct::group(source).expect("source should build");
        validate_function_clause_discipline(&group)
    }

    #[test]
    fn two_unguarded_value_fn_clauses_refuse() {
        // The defect: N unguarded clauses would emit `CASE ELSE <last> END`.
        let err = discipline("f:(x) :- x + 1\nf:(x) :- x * 10")
            .expect_err("two unguarded value-function clauses must refuse");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_multiplicity"
        );
        let msg = format!("{err}");
        assert!(msg.contains("value function"), "msg: {msg}");
        assert!(msg.contains('f'), "should name the entity: {msg}");
    }

    #[test]
    fn duplicate_constants_refuse() {
        // A constant is a zero-arity value function; two are indistinguishable.
        let err = discipline("nl :- char:(10)\nnl :- char:(13)")
            .expect_err("duplicate constants must refuse");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_multiplicity"
        );
    }

    #[test]
    fn guarded_with_single_unguarded_default_ok() {
        // fizzbuzz family (ddl/321): guards + one trailing default.
        discipline(
            "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"\n\
             fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"\n\
             fizzbuzz:(n | (n % 5) = 0) :- \"buzz\"\n\
             fizzbuzz:(n) :- n",
        )
        .expect("guarded function with one trailing default is legal");
    }

    #[test]
    fn all_guarded_no_default_ok() {
        // Zero unguarded clauses: a CASE with no ELSE — legal.
        discipline("sign:(x | x > 0) :- 1\nsign:(x | x < 0) :- -1")
            .expect("all-guarded function is legal");
    }

    #[test]
    fn single_clause_function_ok() {
        discipline("double:(x) :- x * 2").expect("single-clause function is legal");
    }

    #[test]
    fn single_constant_ok() {
        discipline("nl :- char:(10)").expect("single constant is legal");
    }

    #[test]
    fn unguarded_not_last_refuses_on_position() {
        // Rule 4 still fires (currently parse/general; RULE 3 will rebadge it).
        let err = discipline(
            "fizzbuzz:(n) :- n\n\
             fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"",
        )
        .expect_err("unguarded-not-last must refuse");
        let msg = format!("{err}");
        assert!(msg.contains("must be the last clause"), "msg: {msg}");
    }

    #[test]
    fn multi_clause_sigma_is_exempt() {
        // Plain-functor sigma predicate (ddl/320): DefKind::Sigma, not
        // Function — the helper is a no-op, clauses OR together elsewhere.
        discipline("empty(column) :- null = column\nempty(column) :- trim:(column) = \"\"")
            .expect("multi-clause sigma predicate must NOT be touched by this check");
    }
}

// =============================================================================
// The live W9 tree-group registration walker is structurally
// incomplete (system.rs `walk_relational_for_
// tree_groups`). It inspects ONLY `Group::Reduce.reductions`, so a tree group
// (a named record) in `keys` — a legal, surface-constructible
// position (`%({region, "kids": ~> {order_id}} as grp ~> count:(*))`,
// TreeGroupLocation::InKeys) — is never registered, leaving absent
// interior_entity metadata and later drill-down failures.
//
// The keys tree group IS constructible (verified
// against the parser), so this is a real hole, not a fabricated shape. RED
// today — the walker skips keys.
// =============================================================================
#[cfg(test)]
mod red5_w9_tree_group_tests {
    use rusqlite::Connection;

    /// A minimal bootstrap: just the two interior-schema tables the W9 walker
    /// writes into (FKs are unenforced by default, so no `entity` row needed).
    fn interior_schema_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory conn");
        conn.execute_batch(
            "CREATE TABLE interior_entity (
                 id INTEGER PRIMARY KEY,
                 parent_entity_id INTEGER NOT NULL,
                 column_name TEXT NOT NULL
             );
             CREATE TABLE interior_entity_attribute (
                 id INTEGER PRIMARY KEY,
                 interior_entity_id INTEGER NOT NULL,
                 attribute_name TEXT NOT NULL,
                 position INTEGER NOT NULL,
                 child_interior_entity_id INTEGER
             );",
        )
        .expect("create interior tables");
        conn
    }

    fn parse_one(
        dql: &str,
    ) -> crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Unresolved> {
        let tree = crate::pipeline::parse::query_sequence(dql).expect("parse");
        let normalized = crate::pipeline::parse::normalize_sequence(&tree).expect("normalize");
        let mut queries = normalized.into_queries();
        assert_eq!(queries.len(), 1, "one statement expected");
        queries.remove(0).into_query()
    }

    fn interior_column_names(conn: &Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT column_name FROM interior_entity ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .flatten()
            .collect()
    }

    /// Control: a tree group in `reductions` DOES register today — proves the
    /// harness (parse → register → interior_entity read) works end to end.
    #[test]
    fn reducing_on_tree_group_registers_control() {
        let conn = interior_schema_conn();
        let query = parse_one("orders(*) |> %(region ~> {order_id} as kids)");
        super::register_interior_schemas_from_query(&conn, 1, &query).expect("registration walk");
        assert!(
            interior_column_names(&conn).iter().any(|c| c == "kids"),
            "reductions tree group must register (control): {:?}",
            interior_column_names(&conn)
        );
    }

    /// A tree group in `keys` must ALSO register its interior
    /// schema — the W9 walker skips it today (it only descends reductions), so
    /// its drill-down metadata is silently lost. RED until W9 is migrated onto
    /// the shared `AstVisit` walk or its input boundary enforced.
    #[test]
    fn reducing_by_tree_group_registers_interior_schema() {
        let conn = interior_schema_conn();
        let query =
            parse_one("orders(*) |> %({region, \"kids\": ~> {order_id}} as grp ~> count:(*) as c)");
        super::register_interior_schemas_from_query(&conn, 1, &query).expect("registration walk");
        let names = interior_column_names(&conn);
        assert!(
            names.iter().any(|c| c == "grp"),
            "keys tree group `grp` was NOT registered — W9 \
             (walk_relational_for_tree_groups) inspects only reductions, dropping \
             keys tree groups and their interior_entity metadata; reached: {:?}",
            names
        );
    }
}
