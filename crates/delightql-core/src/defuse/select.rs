// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Exhaustive selection of published definition families.
//!
//! One entrance serves every consulted lookup: it enumerates the COMPLETE
//! candidate set for a canonical name in a namespace scope and answers with
//! a closed outcome. SQL row order, `query_row`, first-match returns, and
//! row-decode drops may not decide multiplicity — an undecodable candidate
//! is an error, never a silent narrowing, and two candidates are an
//! ambiguity carrying both, never a scan-order winner.
//!
//! The selected family is opaque: its body opens through [`LinkedFamily`],
//! in the declaration environment the family itself names. No caller
//! receives a raw body string beside a namespace string to re-pair.

use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::resolution::HoParamInfo;

/// One published, source-authored definition family, selected exhaustively
/// from the statement's catalog state and carrying its
/// declaration-environment anchor. Fields are private: consumers observe
/// facts through methods and open the body through the family, so
/// namespace, kind, and body cannot be re-paired by a call site.
///
/// THE FAMILY CARRIES THE READ IT WAS SELECTED UNDER. Its lifetime is the
/// catalog read's, so a selected family — and every carrier built on it —
/// cannot outlive the statement's shared borrow of the system: admission,
/// body opening, and resolution/invocation all happen under the read that
/// selected it, and no `&mut` catalog mutation can be reached while one
/// stands. The declaration environment and every nested selection derive
/// from this same read; no road accepts a second registry beside a family.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct LinkedFamily<'s> {
    catalog: super::CatalogRead<'s>,
    name: delightql_types::SqlIdentifier,
    kind: EntityType,
    definition: String,
    params: Vec<HoParamInfo>,
    namespace: String,
}

/// An ENGINE-SERVED catalog row: a bin entity, an introspected database
/// object, a materialization product, or a reflected syntax directive. It
/// has a name and a kind and NO authored body to open — a typed road, so
/// a served row can never pose as an authored family.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct ServedEntity {
    name: delightql_types::SqlIdentifier,
    kind: EntityType,
    /// The namespace the row is activated in — a served relation's rows
    /// live in that namespace's data world.
    namespace: String,
}

impl ServedEntity {
    pub(crate) fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub(crate) fn kind(&self) -> EntityType {
        self.kind
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// What one selection selected: a source-authored family, or an
/// engine-served row. The distinction is the catalog's, not a caller
/// convention — only an authored family can open a body or key an
/// instance.
#[derive(Debug, Clone)]
pub(in crate::defuse) enum Selected<'s> {
    Authored(LinkedFamily<'s>),
    Served(ServedEntity),
}

impl Selected<'_> {
    pub(crate) fn name(&self) -> &delightql_types::SqlIdentifier {
        match self {
            Selected::Authored(family) => family.name(),
            Selected::Served(served) => served.name(),
        }
    }

    pub(crate) fn kind(&self) -> EntityType {
        match self {
            Selected::Authored(family) => family.kind(),
            Selected::Served(served) => served.kind(),
        }
    }
}

impl<'s> LinkedFamily<'s> {
    /// Constructed only by the selection road in this module.
    fn linked(
        catalog: super::CatalogRead<'s>,
        name: delightql_types::SqlIdentifier,
        kind: EntityType,
        definition: String,
        params: Vec<HoParamInfo>,
        namespace: String,
    ) -> Self {
        LinkedFamily {
            catalog,
            name,
            kind,
            definition,
            params,
            namespace,
        }
    }

    /// The catalog read this family was selected under — the read its
    /// declaration environment, nested selections, and invocation reuse.
    pub(crate) fn catalog(&self) -> super::CatalogRead<'s> {
        self.catalog
    }

    pub(crate) fn name(&self) -> &delightql_types::SqlIdentifier {
        &self.name
    }

    pub(crate) fn kind(&self) -> EntityType {
        self.kind
    }

    /// The family's stored clause text — the reconstruction INPUT, handed
    /// to the one reconstruction door inside the use entrance. No caller
    /// outside the authority reads it.
    pub(in crate::defuse) fn definition(&self) -> &str {
        &self.definition
    }

    pub(crate) fn params(&self) -> &[HoParamInfo] {
        &self.params
    }

    /// The declaration-environment anchor: the declaring namespace, fully
    /// qualified. A body opened from this family resolves HERE, never in
    /// the caller's ambient scope — and only the use entrance applies it.
    pub(in crate::defuse) fn namespace(&self) -> &str {
        &self.namespace
    }
}

/// One enumerated candidate, for ambiguity reporting.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct Candidate {
    pub(crate) namespace: String,
    pub(crate) kind: EntityType,
    /// The authoring source URI, for an authored family.
    pub(crate) source_uri: Option<String>,
}

/// The closed outcome of one exhaustive selection.
#[derive(Debug)]
pub(in crate::defuse) enum Selection<'s> {
    Unique(Selected<'s>),
    Missing,
    /// The complete candidate set with provenance, in deterministic
    /// (namespace) order.
    Ambiguous(Vec<Candidate>),
}

impl<'s> Selection<'s> {
    /// The unique family, or `None` for a miss — refusing ambiguity with
    /// the complete candidate set. This is the one lawful narrowing of a
    /// selection to an `Option`: absence stays a miss for the caller's
    /// fallback ladder, while multiplicity can never pass as either.
    pub(in crate::defuse) fn unique_or_refuse(self, name: &str) -> Result<Option<Selected<'s>>> {
        match self {
            Selection::Unique(selected) => Ok(Some(selected)),
            Selection::Missing => Ok(None),
            Selection::Ambiguous(candidates) => Err(ambiguity_refusal(name, &candidates)),
        }
    }
}

/// The shared ambiguity refusal: every road reports the complete candidate
/// set the selection retained.
pub(in crate::defuse) fn ambiguity_refusal(name: &str, candidates: &[Candidate]) -> DelightQLError {
    let listed: Vec<String> = candidates
        .iter()
        .map(|c| {
            let mut description = format!("{} ({}", c.namespace, c.kind.variant_name());
            if let Some(uri) = &c.source_uri {
                description.push_str(&format!(", from {uri}"));
            }
            description.push(')');
            description
        })
        .collect();
    DelightQLError::validation_error_categorized(
        "resolution/ambiguous",
        format!(
            "Ambiguous entity '{}': found in namespaces {}. \
             Qualify the reference to choose one.",
            name,
            listed.join(", ")
        ),
        "Ambiguous definition selection",
    )
}

/// Judge a complete candidate set against one use position's capability.
///
/// The position's expected category filters CAPABILITY, never identity: an
/// incapable candidate perturbs nothing, one capable candidate answers, and
/// several capable candidates refuse with the set they form. This is how a
/// wrong-kind candidate cannot change lawful selection while one name with
/// two capable owners never picks by scan order.
pub(in crate::defuse) fn judge_position<'s>(
    name: &str,
    candidates: Vec<Enumerated<'s>>,
    capable: impl Fn(EntityType) -> bool,
) -> Result<PositionOutcome<'s>> {
    let (mut fit, unfit): (Vec<Enumerated<'s>>, Vec<Enumerated<'s>>) = candidates
        .into_iter()
        .partition(|c| capable(c.selected.kind()));
    match fit.len() {
        0 if unfit.is_empty() => Ok(PositionOutcome::Missing),
        0 => Ok(PositionOutcome::WrongKind(
            unfit.into_iter().map(|c| c.provenance).collect(),
        )),
        1 => Ok(PositionOutcome::Selected(fit.remove(0).selected)),
        _ => {
            let listed: Vec<Candidate> = fit.into_iter().map(|c| c.provenance).collect();
            Err(ambiguity_refusal(name, &listed))
        }
    }
}

/// One enumerated candidate: the selected carrier beside its complete
/// provenance, so a position judgment can report either without asking
/// the carrier to expose separable facts.
#[derive(Debug)]
pub(in crate::defuse) struct Enumerated<'s> {
    selected: Selected<'s>,
    provenance: Candidate,
}

impl<'s> Enumerated<'s> {
    pub(crate) fn kind(&self) -> EntityType {
        self.selected.kind()
    }

    pub(in crate::defuse) fn namespace(&self) -> &str {
        &self.provenance.namespace
    }

    pub(in crate::defuse) fn into_selected(self) -> Selected<'s> {
        self.selected
    }

    pub(in crate::defuse) fn into_provenance(self) -> Candidate {
        self.provenance
    }
}

/// The closed outcome of judging a complete candidate set against one use
/// position's capability. A wrong-kind family stays DISTINGUISHABLE from a
/// true absence: only `Missing` may reach the open target provider, and
/// `WrongKind` carries the complete candidate provenance for the teaching.
#[derive(Debug)]
pub(in crate::defuse) enum PositionOutcome<'s> {
    Selected(Selected<'s>),
    WrongKind(Vec<Candidate>),
    Missing,
}

/// THE LEXICAL-LINK / DATA-HOLE JUDGMENT of one unqualified name in one
/// lexical world — the ONE judgment shared by body opening (the relation
/// ladder's catalog steps) and grounding admission. A name the world's
/// reach answers is a lexical link, whatever it is (an authored family or
/// a served relation); a name nothing in the reach answers is a data hole,
/// which only the world's bound data namespace may fill. Nothing else
/// classifies that relationship.
#[derive(Debug)]
pub(in crate::defuse) enum Link<'s> {
    /// The reach answers: the COMPLETE candidate set, for the position to
    /// judge.
    Lexical(Vec<Enumerated<'s>>),
    /// A data hole: the bound data namespace's selection, or `None` when
    /// the world binds no data namespace.
    Hole(Option<Selection<'s>>),
}

/// The ER-rule selection scope: the namespaces one lexical world reaches.
#[cfg(not(target_arch = "wasm32"))]
fn er_namespace_filter(reach: &super::environment::DeclarationReach) -> String {
    format!(" AND n.id IN {}", reach.namespace_id_list())
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::defuse) use native::{
    enumerate_in_reach, judge_link_on, qualifier_target, query_er_rule_single,
    query_er_rules_multi, select_declared_mode, select_qualified_in_reach, select_qualified_on,
    select_runtime_served_view,
};

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    use crate::defuse::CatalogRead;

    /// Select the definition family a (possibly aliased) qualified
    /// reference names, exhaustively, over the captured reach.
    ///
    /// The qualifier names a namespace exactly, or through an alias the
    /// world declared (the session's aliases at the prompt, the declaring
    /// namespace's own local aliases inside a body). A miss on the exact
    /// pair retries once with the §IV plain-qualifier expansion.
    /// Archived-blueprint namespaces are deliberately invisible: their
    /// candidates are filtered as a visibility law, not a winner choice.
    /// The named namespace answers with the families it currently
    /// publishes; a namespace published after capture (a lazily loaded
    /// system module) is observed at first use.
    pub(in crate::defuse) fn select_qualified_in_reach<'s>(
        catalog: CatalogRead<'s>,
        name: &str,
        name_stropped: bool,
        namespace_fq: &str,
        reach: &super::super::environment::DeclarationReach,
    ) -> Result<Selection<'s>> {
        let system = catalog.system();
        // Lazy-load stdlib module if needed (no-op for non-std:: namespaces)
        system.ensure_stdlib_loaded(namespace_fq);

        // Catalog functor: name like "std::string::" lives in sys::meta but
        // refers to namespace "std::string". Lazy-load that namespace first
        // so its catalog wrapper gets registered before we look it up.
        if namespace_fq == "sys::meta" {
            if let Some(ns) = name.strip_suffix("::") {
                system.ensure_stdlib_loaded(ns);
            }
            system.ensure_catalog_loaded();
        }

        let conn =
            catalog.connection("Failed to acquire bootstrap lock for definition selection")?;
        select_qualified_on(&conn, catalog, name, name_stropped, namespace_fq, reach)
    }

    /// The qualified selection itself, on an open bootstrap connection —
    /// the same entrance for the resolver (through the read's lock) and
    /// for grounding admission (which already holds the connection inside
    /// the lifecycle transaction).
    pub(in crate::defuse) fn select_qualified_on<'s>(
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'s>,
        name: &str,
        name_stropped: bool,
        namespace_fq: &str,
        reach: &super::super::environment::DeclarationReach,
    ) -> Result<Selection<'s>> {
        // The identifier law's agreement: an unstropped spelling folds, a
        // stropped one keeps its authored bytes.
        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };

        let Some(target) = qualifier_target(conn, reach, namespace_fq)? else {
            return Ok(Selection::Missing);
        };
        // Inside a derived world, a source the grounding derived is read
        // as its derivative: the world's closure, never the source.
        let target = reach.derivative_of(&target);

        let rows = enumerate_in_namespace(conn, &canonical, &target)?;

        // Blueprint inertness SAFETY NET: a candidate whose resolved
        // namespace is an archived blueprint (or nested under one) is
        // INVISIBLE — deliberately inert, so filtering it narrows nothing
        // a live reference could lawfully reach. The loud badged refusals
        // live at the front doors.
        let mut visible = Vec::with_capacity(rows.len());
        for row in rows {
            if crate::system::blueprint_shadowing(&conn, &row.namespace)?.is_none() {
                visible.push(row);
            }
        }

        match visible.len() {
            0 => Ok(Selection::Missing),
            1 => {
                let row = visible.into_iter().next().expect("len checked");

                // §IV plain-qualifier SHADOW: exact hit on a top-level
                // namespace with an enlisted `home::{fq}` child shadowed
                // behind it — warn that the full path is needed.
                if row.namespace == namespace_fq
                    && crate::system::home_child_shadows(&conn, namespace_fq)
                {
                    log::warn!(
                        "plain qualifier '{n}' resolved to the top-level namespace '{n}'; an \
                         enlisted scratch child 'home::{n}' is shadowed behind it — spell \
                         'home::{n}' to reach it",
                        n = namespace_fq
                    );
                }

                Ok(Selection::Unique(realize(conn, catalog, row)?))
            }
            _ => Ok(Selection::Ambiguous(
                visible.iter().map(CandidateRow::provenance).collect(),
            )),
        }
    }

    /// THE QUALIFIER'S NAMESPACE, as the world names it: an alias this
    /// world declared, else the exact spelling, else the §IV
    /// plain-qualifier expansion, else a namespace published after the
    /// world was captured. `None` when nothing bears the name. Grounding
    /// admission asks this same question of a reference before judging it,
    /// so the namespace a derivation follows and the namespace a selection
    /// reads are one answer.
    pub(in crate::defuse) fn qualifier_target(
        conn: &rusqlite::Connection,
        reach: &super::super::environment::DeclarationReach,
        namespace_fq: &str,
    ) -> Result<Option<super::super::environment::reach::ReachedNamespace>> {
        let mut target = match reach.alias_target(namespace_fq) {
            Some(aliased) => Some(aliased.clone()),
            None => reach.namespace(namespace_fq).cloned(),
        };
        if target.is_none() {
            if let Some(expanded) = crate::system::expand_plain_namespace(conn, namespace_fq)? {
                target = reach.namespace(&expanded).cloned();
                if target.is_none() {
                    target = observe_namespace(conn, &expanded)?;
                }
            }
        }
        if target.is_none() {
            target = observe_namespace(conn, namespace_fq)?;
        }
        Ok(target)
    }

    /// A namespace the reach did not capture, observed now: the road for
    /// system modules published lazily after the world was built.
    fn observe_namespace(
        conn: &rusqlite::Connection,
        fq: &str,
    ) -> Result<Option<super::super::environment::reach::ReachedNamespace>> {
        let observed = super::super::environment::reach::capture_on(
            conn,
            fq,
            super::super::environment::reach::World::Session,
        )?;
        Ok(observed.namespace(fq).cloned())
    }

    /// Enumerate the COMPLETE candidate set for one unqualified name over
    /// the captured reach: every entity — authored family or served row —
    /// activated in the reach's namespaces. Kind never filters the
    /// enumeration; the caller judges position capability over the whole
    /// set.
    pub(in crate::defuse) fn enumerate_in_reach<'s>(
        catalog: CatalogRead<'s>,
        name: &str,
        name_stropped: bool,
        reach: &super::super::environment::DeclarationReach,
    ) -> Result<Vec<Enumerated<'s>>> {
        let conn = catalog.connection("Failed to acquire bootstrap lock for enlisted selection")?;
        enumerate_in_reach_on(&conn, catalog, name, name_stropped, reach)
    }

    /// The reach enumeration itself, on an open bootstrap connection.
    pub(in crate::defuse) fn enumerate_in_reach_on<'s>(
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'s>,
        name: &str,
        name_stropped: bool,
        reach: &super::super::environment::DeclarationReach,
    ) -> Result<Vec<Enumerated<'s>>> {
        let canonical = if name_stropped {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        };
        let rows = enumerate_rows(conn, &canonical, &reach.namespace_id_list())?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let provenance = row.provenance();
            candidates.push(Enumerated {
                selected: realize(conn, catalog, row)?,
                provenance,
            });
        }
        Ok(candidates)
    }

    /// THE LEXICAL-LINK / DATA-HOLE JUDGMENT ([`Link`]): the reach first,
    /// and only a reach miss falls to the world's bound data namespace.
    /// Body opening consumes it through the read's lock; grounding
    /// admission consumes it on the lifecycle transaction's connection.
    pub(in crate::defuse) fn judge_link_on<'s>(
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'s>,
        reach: &super::super::environment::DeclarationReach,
        data_ns: Option<&str>,
        name: &str,
        name_stropped: bool,
    ) -> Result<Link<'s>> {
        let candidates = enumerate_in_reach_on(conn, catalog, name, name_stropped, reach)?;
        if !candidates.is_empty() {
            return Ok(Link::Lexical(candidates));
        }
        Ok(Link::Hole(match data_ns {
            Some(data_ns) => Some(select_qualified_on(
                conn,
                catalog,
                name,
                name_stropped,
                data_ns,
                reach,
            )?),
            None => None,
        }))
    }

    /// The candidates one namespace currently holds for a canonical name.
    fn enumerate_in_namespace(
        conn: &rusqlite::Connection,
        canonical: &str,
        namespace: &super::super::environment::reach::ReachedNamespace,
    ) -> Result<Vec<CandidateRow>> {
        enumerate_rows(
            conn,
            canonical,
            &super::super::environment::reach::int_list(std::iter::once(namespace.id)),
        )
    }

    /// THE COMPLETE CANDIDATE ENUMERATION: every entity activated in the
    /// named namespaces under the canonical name, authored families and
    /// served rows alike — the catalog holds exactly the current
    /// families, so activation IS publication. Every row decodes or the
    /// selection errors; ORDER BY makes a reported candidate set
    /// deterministic.
    fn enumerate_rows(
        conn: &rusqlite::Connection,
        canonical: &str,
        namespaces: &str,
    ) -> Result<Vec<CandidateRow>> {
        let sql = format!(
            "SELECT e.id, e.name, e.name_stropped, e.type,
                    (SELECT GROUP_CONCAT(ec.definition, char(10))
                     FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                    ) as definition,
                    n.fq_name, c.source_uri
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             LEFT JOIN cartridge c ON c.id = e.cartridge_id
             WHERE (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END) = ?1
               AND ae.namespace_id IN {namespaces}
             ORDER BY n.fq_name, e.id"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error("Failed to prepare reach selection", e.to_string())
        })?;
        let mapped = stmt
            .query_map(rusqlite::params![canonical], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to run reach selection", e.to_string())
            })?;
        // EVERY CANDIDATE, OR NONE: a row that fails to decode is an
        // error, because dropping it here would turn an ambiguous lookup
        // into a unique winner.
        let raw: Vec<_> = mapped.collect::<rusqlite::Result<Vec<_>>>().map_err(|e| {
            DelightQLError::database_error(
                "Failed to decode a reach-selection candidate",
                e.to_string(),
            )
        })?;
        let mut rows = Vec::with_capacity(raw.len());
        for (entity_id, name, stropped, kind_raw, definition, namespace, source_uri) in raw {
            let kind = EntityType::from_i32(kind_raw).map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "catalog row for '{name}' in '{namespace}' carries unknown entity type {kind_raw}"
                    ),
                    "catalog corruption",
                )
            })?;
            rows.push(CandidateRow {
                entity_id,
                name,
                stropped,
                kind,
                definition: definition.unwrap_or_default(),
                namespace,
                source_uri: if kind.is_authored_definition() {
                    source_uri
                } else {
                    None
                },
            });
        }
        Ok(rows)
    }

    /// Select the fact family that DECLARES A MODE for one name — the
    /// callable face of a fact function. Qualified narrows the reach to
    /// one namespace; unqualified takes the world's captured reach.
    pub(in crate::defuse) fn select_declared_mode<'s>(
        catalog: CatalogRead<'s>,
        name: &str,
        namespace: Option<&str>,
        reach: &super::super::environment::DeclarationReach,
    ) -> Result<Option<(LinkedFamily<'s>, crate::resolution::DeclaredMode)>> {
        let conn =
            catalog.connection("Failed to acquire bootstrap lock for declared mode selection")?;
        let namespaces = match namespace {
            Some(fq) => {
                let target = match reach.alias_target(fq) {
                    Some(aliased) => Some(aliased.clone()),
                    None => reach.namespace(fq).cloned(),
                };
                let target = match target {
                    Some(target) => Some(target),
                    None => observe_namespace(&conn, fq)?,
                };
                match target {
                    Some(target) => super::super::environment::reach::int_list(std::iter::once(
                        reach.derivative_of(&target).id,
                    )),
                    None => return Ok(None),
                }
            }
            None => reach.namespace_id_list(),
        };
        let sql = format!(
            "SELECT e.id, e.name, e.type,
                    (SELECT GROUP_CONCAT(ec.definition, char(10))
                     FROM (SELECT definition FROM entity_clause WHERE entity_id = e.id ORDER BY ordinal) ec
                    ) as definition,
                    n.fq_name
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             WHERE e.name = ?1 COLLATE NOCASE AND e.type IN (?2, ?3)
               AND ae.namespace_id IN {namespaces}
               AND EXISTS (SELECT 1 FROM functional_dependency fd WHERE fd.entity_id = e.id)"
        );
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error("Failed to prepare declared mode lookup", e.to_string())
        })?;
        // EVERY CANDIDATE, OR NONE. A row that will not decode is not
        // evidence of absence: dropping it here would turn an ambiguous
        // lookup into a unique winner, and a corrupt catalog would read as
        // a decision. The cardinality judgment below is only sound over
        // the complete candidate set.
        let rows: Vec<(i32, String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![
                    name,
                    EntityType::DqlFactExpression.as_i32(),
                    EntityType::DqlDefaultFactFunctionExpression.as_i32()
                ],
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
                let (entity_id, entity_name, entity_type_id, definition, ns) =
                    rows.into_iter().next().expect("len checked");
                let entity_type = EntityType::from_i32(entity_type_id).map_err(|error| {
                    DelightQLError::database_error(
                        "corrupt catalog: declared-mode entity type is unknown",
                        error.to_string(),
                    )
                })?;
                // THE ENTITY HAS ALREADY ADVERTISED THE CAPABILITY — it was
                // selected BY having declaration rows — so a declaration
                // that will not read whole is corruption, not absence.
                let mode =
                    crate::resolution::ConsultRegistry::query_declared_mode(&conn, entity_id)?;
                Ok(Some((
                    LinkedFamily::linked(
                        catalog,
                        entity_name.into(),
                        entity_type,
                        definition.unwrap_or_default(),
                        Vec::new(),
                        ns,
                    ),
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
    pub(in crate::defuse) fn select_runtime_served_view<'s>(
        catalog: CatalogRead<'s>,
        name: &delightql_types::SqlIdentifier,
        namespace_fq: Option<&str>,
        reach: &super::super::environment::DeclarationReach,
    ) -> crate::error::Result<Option<LinkedFamily<'s>>> {
        use crate::bootstrap::enums::EntityType;
        use crate::error::DelightQLError;
        let canonical = if name.is_stropped() {
            name.as_str().to_string()
        } else {
            name.as_str().to_ascii_lowercase()
        };
        let conn = catalog
            .connection("bootstrap connection lock poisoned during runtime-served lookup")?;
        let (ns_filter, ns_param) = match namespace_fq {
            Some(fq) => ("n.fq_name = ?4".to_string(), fq.to_string()),
            None => (
                format!("n.id IN {} AND ?4 = ?4", reach.namespace_id_list()),
                String::new(),
            ),
        };
        let sql = format!(
            "SELECT (SELECT GROUP_CONCAT(ec.definition, char(10))
                     FROM (SELECT definition FROM entity_clause
                           WHERE entity_id = e.id ORDER BY ordinal) ec),
                    n.fq_name, e.type
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
        let candidates: Vec<(Option<String>, String, i32)> = stmt
            .query_map(
                rusqlite::params![
                    canonical,
                    EntityType::DqlTemporaryViewExpression.as_i32(),
                    EntityType::DqlPermanentViewExpression.as_i32(),
                    ns_param,
                ],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                },
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
        let mut found: Vec<LinkedFamily> = Vec::with_capacity(candidates.len());
        for (definition, fq, kind_raw) in candidates {
            match definition {
                Some(definition) => {
                    let kind = crate::enums::EntityType::from_i32(kind_raw).map_err(|_| {
                        DelightQLError::database_error(
                            format!(
                                "catalog row for '{}' in '{fq}' carries unknown entity type {kind_raw}",
                                name.as_str()
                            ),
                            "catalog corruption",
                        )
                    })?;
                    found.push(LinkedFamily::linked(
                        catalog,
                        name.clone(),
                        kind,
                        definition,
                        Vec::new(),
                        fq,
                    ));
                }
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
                let mut namespaces: Vec<&str> = found.iter().map(|f| f.namespace()).collect();
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

    /// Realize one candidate row as what it IS: a source-authored family
    /// (with its params and positions) or an engine-served row. The kind
    /// decides; nothing narrows to a miss or an anonymous family.
    fn realize<'s>(
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'s>,
        row: CandidateRow,
    ) -> Result<Selected<'s>> {
        let name = if row.stropped {
            delightql_types::SqlIdentifier::stropped(row.name.clone())
        } else {
            delightql_types::SqlIdentifier::new(row.name.clone())
        };
        if !row.kind.is_authored_definition() {
            return Ok(Selected::Served(ServedEntity {
                name,
                kind: row.kind,
                namespace: row.namespace,
            }));
        }
        let is_ho = row.kind == EntityType::DqlHoTemporaryViewExpression;
        // Effect rules declare scalar parameters too: the invocation's
        // glob/arity law reads them as a FAMILY FACT, before any opening.
        let takes_params = row.kind.is_fn() || is_ho || row.kind == EntityType::DqlEffectRule;
        let reconstructed = if is_ho {
            Some(crate::ddl::reconstruct::group(&row.definition)?)
        } else {
            None
        };
        let params = if let Some(group) = reconstructed.as_ref() {
            group.params().to_vec()
        } else if takes_params {
            crate::resolution::ConsultRegistry::query_params(conn, row.entity_id, row.kind)
        } else {
            Vec::new()
        };
        Ok(Selected::Authored(LinkedFamily::linked(
            catalog,
            name,
            row.kind,
            row.definition,
            params,
            row.namespace,
        )))
    }

    /// Query a single ER-rule by (context, table_a, table_b) with scope filtering.
    /// Returns at most one rule; errors on cross-namespace ambiguity.
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::defuse) fn query_er_rule_single<'s>(
        catalog: CatalogRead<'s>,
        context: &str,
        table_a: &str,
        table_b: &str,
        reach: &super::super::environment::DeclarationReach,
    ) -> std::result::Result<Option<LinkedFamily<'s>>, DelightQLError> {
        let conn = catalog.connection("Failed to acquire bootstrap lock")?;

        // Canonical ordering: alphabetical
        let (left, right) = if table_a <= table_b {
            (table_a, table_b)
        } else {
            (table_b, table_a)
        };

        let ns_join_cond = er_namespace_filter(reach);

        let sql = format!(
            "SELECT e.name, e.type, ec.definition, n.fq_name
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id{ns_join_cond}
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

        let rows: Vec<(String, i32, Option<String>, String)> = stmt
            .query_map(
                rusqlite::params![context, left, right, entity_type],
                row_mapper,
            )
            .map_err(|e| DelightQLError::database_error("Failed to query ER-rules", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to decode an ER-rule candidate",
                    e.to_string(),
                )
            })?;

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
            Some((entity_name, entity_type, definition, namespace)) => {
                let kind = EntityType::from_i32(entity_type).map_err(|e| {
                    DelightQLError::database_error(
                        "corrupt catalog: unknown entity_type",
                        e.to_string(),
                    )
                })?;
                Ok(Some(LinkedFamily::linked(
                    catalog,
                    entity_name.into(),
                    kind,
                    definition.unwrap_or_default(),
                    Vec::new(),
                    namespace,
                )))
            }
        }
    }

    /// Query all ER-rules in a context with scope filtering.
    /// Returns (left_table, right_table, entity) tuples.
    pub(in crate::defuse) fn query_er_rules_multi<'s>(
        catalog: CatalogRead<'s>,
        context: &str,
        reach: &super::super::environment::DeclarationReach,
    ) -> std::result::Result<Vec<(String, String, LinkedFamily<'s>)>, DelightQLError> {
        let conn = catalog.connection("Failed to acquire bootstrap lock")?;

        let ns_join_cond = er_namespace_filter(reach);

        let sql = format!(
            "SELECT e.name, e.type, ec.definition, n.fq_name,
                    er.left_spelling, er.right_spelling
             FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id{ns_join_cond}
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

        let rows: Vec<(String, i32, Option<String>, String, String, String)> = stmt
            .query_map(rusqlite::params![context, entity_type], row_mapper)
            .map_err(|e| DelightQLError::database_error("Failed to query ER-rules", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to decode an ER-rule candidate",
                    e.to_string(),
                )
            })?;

        // Unknown entity_type is catalog corruption and refuses rather
        // than disappearing from the listing.
        let mut out = Vec::with_capacity(rows.len());
        for (entity_name, entity_type, definition, namespace, left, right) in rows {
            let kind = EntityType::from_i32(entity_type).map_err(|e| {
                DelightQLError::database_error(
                    "corrupt catalog: unknown entity_type",
                    e.to_string(),
                )
            })?;
            out.push((
                left,
                right,
                LinkedFamily::linked(
                    catalog,
                    entity_name.into(),
                    kind,
                    definition.unwrap_or_default(),
                    Vec::new(),
                    namespace,
                ),
            ));
        }
        Ok(out)
    }

    struct CandidateRow {
        entity_id: i32,
        name: String,
        stropped: bool,
        kind: EntityType,
        definition: String,
        namespace: String,
        source_uri: Option<String>,
    }

    impl CandidateRow {
        fn provenance(&self) -> Candidate {
            Candidate {
                namespace: self.namespace.clone(),
                kind: self.kind,
                source_uri: self.source_uri.clone(),
            }
        }
    }
}

/// THE AUTHORITY'S OWN REGISTRY DOORS. Selection reaches the catalog
/// through the registry's one defuse capability, and every question is
/// asked OVER ONE LEXICAL WORLD'S CAPTURED REACH; every method here is
/// visible ONLY inside `crate::defuse` — production code cannot select a
/// family and compose a use by hand.
impl<'s> crate::resolution::registry::ConsultRegistry<'s> {
    /// Select the definition family a qualified reference names,
    /// exhaustively: the complete candidate set is enumerated and the
    /// outcome is closed.
    pub(in crate::defuse) fn select_entity(
        &self,
        name: &str,
        name_stropped: bool,
        namespace_fq: &str,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Selection<'s>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (name, name_stropped, namespace_fq, reach);
            Ok(Selection::Missing)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(Selection::Missing);
            };
            select_qualified_in_reach(catalog, name, name_stropped, namespace_fq, reach)
        }
    }

    /// Loud front door for the FUNCTION-inlining route: refuse a
    /// namespace-qualified consulted lookup whose path is an archived
    /// blueprint, with the badged `imprint/blueprint/inert` error.
    pub(in crate::defuse) fn refuse_if_blueprint_fq(&self, fq: &str) -> Result<()> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = fq;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(());
            };
            let Ok(conn) = catalog.connection("blueprint refusal") else {
                return Ok(());
            };
            crate::system::refuse_if_blueprint(&conn, fq)
        }
    }

    /// Enumerate the complete candidate set for an unqualified name over
    /// the world's captured reach. Kind never filters the enumeration;
    /// position capability is judged over the whole set.
    pub(in crate::defuse) fn select_enlisted(
        &self,
        name: &str,
        name_stropped: bool,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Vec<Enumerated<'s>>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (name, name_stropped, reach);
            Ok(Vec::new())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(Vec::new());
            };
            enumerate_in_reach(catalog, name, name_stropped, reach)
        }
    }

    /// Select the fact family that declares a mode for one name (the
    /// callable face of a fact function).
    pub(in crate::defuse) fn select_declared_mode(
        &self,
        name: &str,
        namespace: Option<&str>,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Option<(LinkedFamily<'s>, crate::resolution::registry::DeclaredMode)>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (name, namespace, reach);
            Ok(None)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(None);
            };
            select_declared_mode(catalog, name, namespace, reach)
        }
    }

    /// Select a runtime-served view family for the effect executor's
    /// pre-splice.
    pub(in crate::defuse) fn select_runtime_served_view(
        &self,
        name: &delightql_types::SqlIdentifier,
        namespace_fq: Option<&str>,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Option<LinkedFamily<'s>>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (name, namespace_fq, reach);
            Ok(None)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(None);
            };
            select_runtime_served_view(catalog, name, namespace_fq, reach)
        }
    }

    /// The unique ER rule for (context, left, right) in the reach.
    pub(in crate::defuse) fn lookup_er_rule(
        &self,
        context: &str,
        left: &str,
        right: &str,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Option<LinkedFamily<'s>>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (context, left, right, reach);
            Ok(None)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(None);
            };
            query_er_rule_single(catalog, context, left, right, reach)
        }
    }

    /// Every ER rule of a context in the reach.
    pub(in crate::defuse) fn lookup_er_rules_in_context(
        &self,
        context: &str,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Vec<(String, String, LinkedFamily<'s>)>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (context, reach);
            Ok(Vec::new())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(Vec::new());
            };
            query_er_rules_multi(catalog, context, reach)
        }
    }

    /// Whether a context has at least one declared edge in the reach.
    pub(in crate::defuse) fn er_context_known(
        &self,
        context: &str,
        reach: &super::environment::DeclarationReach,
    ) -> Result<bool> {
        Ok(!self.lookup_er_rules_in_context(context, reach)?.is_empty())
    }

    /// All contexts with at least one declared edge in the reach — the
    /// unknown-context teaching enumerates these.
    pub(in crate::defuse) fn list_er_contexts(
        &self,
        reach: &super::environment::DeclarationReach,
    ) -> Result<Vec<String>> {
        #[cfg(target_arch = "wasm32")]
        {
            let _ = reach;
            Ok(Vec::new())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(catalog) = self.catalog() else {
                return Ok(Vec::new());
            };
            let conn = catalog.connection("Failed to acquire bootstrap lock")?;
            let sql = format!(
                "SELECT DISTINCT er.context_name
                 FROM join_edge er
                 JOIN activated_entity ae ON ae.entity_id = er.entity_id
                 WHERE ae.namespace_id IN {}
                 ORDER BY er.context_name",
                reach.namespace_id_list()
            );
            let mut stmt = conn.prepare(&sql).map_err(|e| {
                DelightQLError::database_error("Failed to prepare context listing", e.to_string())
            })?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| {
                    DelightQLError::database_error("Failed to list contexts", e.to_string())
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(rows)
        }
    }
}
