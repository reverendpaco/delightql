// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE DERIVED WORLD — what `ground!` constructs, and the one closure fact
//! every later road reads.
//!
//! `ground!(data, rules, derived)` derives the complete REACHABLE LEXICAL
//! DEFINITION CLOSURE of `rules`: every namespace `rules` reaches through
//! its declared lexical graph (local enlistments, local aliases, exposures)
//! or names through a qualified reference, transitively, provided that
//! namespace publishes no data world of its own — a library, not a data
//! namespace, an already-grounded world, or a scratch namespace with an
//! ambient world. Each such SOURCE is derived ONCE into a DERIVATIVE: a
//! `grounded` namespace under the derived root, addressed
//! `derived::_<source leaf>_<source id>` (a readable label beside the
//! identity; the `grounding` row, never the spelling, pairs it with its
//! source) whose families are the source's families and whose declared
//! edges are the
//! source's edges with every derivable target rewired to ITS derivative,
//! all bound to the one explicit data world. Cycles re-enter the derivative
//! already made for the source.
//!
//! The closure is therefore a fact of the catalog itself — one `grounding`
//! row per derivative (derivative, data world, source, root) beside the
//! derivative namespaces and their rewired edges — and every road reads
//! that same fact rather than re-deriving it:
//!
//! - selection reaches derivatives through the rewired edges, and a
//!   qualified reference from inside the world maps its target through the
//!   world's closure (`DeclarationReach::derivative_of`), so a body opened
//!   from a derivative reaches derivatives, never a source;
//! - body opening binds a derivative's holes to the data world its OWN
//!   namespace row publishes — no opener hands a data world down, and a
//!   session that enlists the root reaches the root's exposed derivatives
//!   directly, grounded, while the sources stay ungrounded;
//! - `reconsult!` of any source rebuilds every root deriving from it, and
//!   `refresh!` of the data world re-admits every root bound to it, through
//!   this same derivation and admission.
//!
//! ADMISSION judges every recorded reference of every derivative by THE
//! judgment body opening applies — [`super::select::judge_link_on`] under
//! the derivative's own declaration reach, and
//! [`super::select::select_qualified_on`] for a qualified name — so a name
//! the reach answers is a lexical link, a name nothing answers is a data
//! hole the data world must answer uniquely, and a qualified name must
//! select where it points. A qualified reference that reaches a derivable
//! namespace not yet derived derives it then and there: derivation and
//! admission are one worklist, and every derivative is admitted exactly
//! once. Nothing else classifies that relationship.
//!
//! Everything runs on the lifecycle transaction's own connection. An
//! unadmitted world is a value only this module can consume, and a refusal
//! rolls the derivation back whole: nothing unadmitted is ever published.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::error::{DelightQLError, Result};
use rusqlite::OptionalExtension;

use super::environment::reach;
use super::select::{Link, Selected, Selection};
use super::CatalogRead;

/// One derivative: the exact source namespace it derives and the grounded
/// namespace it derives it into.
#[derive(Debug, Clone)]
struct Derivative {
    source_id: i64,
    source_fq: String,
    derived_id: i64,
    derived_fq: String,
}

/// A derived world under construction or re-admission. Its members are
/// keyed by the exact source namespace each derives, so one source has
/// one derivative and a cycle re-enters it. The value cannot be published:
/// [`DerivedWorld::admit`] consumes it, and only a lifecycle transaction
/// that commits after admission makes the world visible.
pub(crate) struct DerivedWorld {
    root: Derivative,
    data_id: i64,
    data_fq: String,
    members: BTreeMap<i64, Derivative>,
    /// Derivatives not yet admitted, in derivation order.
    unadmitted: VecDeque<i64>,
    /// On a rebuild, the derivative row the closure record already pairs
    /// with each source — identity kept across the rebuild.
    reuse: HashMap<i64, i64>,
    /// On a rebuild, every derivative row the previous closure held; the
    /// ones the re-derived closure does not reach are destroyed once
    /// admission has finished deriving.
    previous: Vec<i64>,
    root_cartridge: Option<i32>,
    root_families: usize,
    /// Families derived across the whole world.
    families: usize,
}

impl DerivedWorld {
    /// DERIVE the closure of `lib_id` into the (already created, empty)
    /// root namespace `root_id`, bound to `data_id`. The root's families,
    /// its rewired edges, and every reachable derivative land on `conn`.
    pub(crate) fn derive(
        conn: &rusqlite::Connection,
        root_id: i64,
        lib_id: i64,
        data_id: i64,
    ) -> Result<Self> {
        Self::derive_reusing(conn, root_id, lib_id, data_id, HashMap::new())
    }

    fn derive_reusing(
        conn: &rusqlite::Connection,
        root_id: i64,
        lib_id: i64,
        data_id: i64,
        reuse: HashMap<i64, i64>,
    ) -> Result<Self> {
        let (root_fq, _, _) = namespace_facts(conn, root_id)?;
        let (lib_fq, _, _) = namespace_facts(conn, lib_id)?;
        let (data_fq, _, _) = namespace_facts(conn, data_id)?;
        let root = Derivative {
            source_id: lib_id,
            source_fq: lib_fq,
            derived_id: root_id,
            derived_fq: root_fq,
        };
        let mut world = DerivedWorld {
            root: root.clone(),
            data_id,
            data_fq,
            members: BTreeMap::new(),
            unadmitted: VecDeque::new(),
            previous: reuse.values().copied().collect(),
            reuse,
            root_cartridge: None,
            root_families: 0,
            families: 0,
        };
        world.record(conn, &root)?;
        let (cartridge, families) = world.copy_families(conn, &root)?;
        world.root_cartridge = cartridge;
        world.root_families = families;
        world.copy_edges(conn, &root)?;
        Ok(world)
    }

    /// REBUILD the world rooted at `root_id` from its sources as they now
    /// are: every derivative is emptied (the root keeps its identity, a
    /// member keeps the row the closure record pairs with its source), the
    /// closure is re-derived, and a member whose source the closure no
    /// longer reaches is destroyed. Reached only through
    /// [`rebuild_dependents`], which holds the proof that the source's
    /// replacement load is complete.
    fn rebuild(conn: &rusqlite::Connection, root_id: i64) -> Result<Self> {
        let (lib_id, data_id): (i64, i64) = conn
            .query_row(
                "SELECT lib_namespace_id, data_namespace_id FROM grounding
                 WHERE grounded_namespace_id = ?1 AND root_namespace_id = ?1",
                [root_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "corrupt catalog: a grounded root has no grounding row",
                    e.to_string(),
                )
            })?;
        // The closure record IS the identity relationship: which row stood
        // for which source. Read it before clearing, so the re-derivation
        // reuses each row for the same source and nothing is paired by
        // spelling.
        let previous: HashMap<i64, i64> = {
            let mut stmt = conn
                .prepare(
                    "SELECT lib_namespace_id, grounded_namespace_id FROM grounding
                     WHERE root_namespace_id = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error("prepare closure listing", e.to_string())
                })?;
            let rows = stmt
                .query_map([root_id], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(|e| DelightQLError::database_error("list the closure", e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| DelightQLError::database_error("decode the closure", e.to_string()))?;
            rows.into_iter().collect()
        };
        for derived_id in previous.values() {
            crate::system::DelightQLSystem::clear_namespace_contents(conn, *derived_id)?;
            conn.execute(
                "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1",
                [derived_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("clear a derivative's exposures", e.to_string())
            })?;
        }
        conn.execute(
            "DELETE FROM grounding WHERE root_namespace_id = ?1",
            [root_id],
        )
        .map_err(|e| DelightQLError::database_error("clear the closure record", e.to_string()))?;

        Self::derive_reusing(conn, root_id, lib_id, data_id, previous)
    }

    /// The world rooted at `root_id` as the catalog holds it, with every
    /// derivative awaiting re-admission — the road a data-world refresh
    /// takes.
    pub(crate) fn current(conn: &rusqlite::Connection, root_id: i64) -> Result<Self> {
        let mut stmt = conn
            .prepare(
                "SELECT g.grounded_namespace_id, g.lib_namespace_id, g.data_namespace_id
                 FROM grounding g
                 WHERE g.root_namespace_id = ?1
                 ORDER BY g.id",
            )
            .map_err(|e| DelightQLError::database_error("prepare closure load", e.to_string()))?;
        let rows: Vec<(i64, i64, i64)> = stmt
            .query_map([root_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(|e| DelightQLError::database_error("load the closure", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| DelightQLError::database_error("decode the closure", e.to_string()))?;
        let mut root = None;
        let mut data_id = None;
        let mut members = BTreeMap::new();
        let mut unadmitted = VecDeque::new();
        for (derived_id, source_id, data) in rows {
            let (source_fq, _, _) = namespace_facts(conn, source_id)?;
            let (derived_fq, _, _) = namespace_facts(conn, derived_id)?;
            let derivative = Derivative {
                source_id,
                source_fq,
                derived_id,
                derived_fq,
            };
            if derived_id == root_id {
                root = Some(derivative.clone());
            }
            data_id = Some(data);
            unadmitted.push_back(source_id);
            members.insert(source_id, derivative);
        }
        let (Some(root), Some(data_id)) = (root, data_id) else {
            return Err(DelightQLError::database_error(
                "corrupt catalog: a grounded root has no grounding row",
                format!("namespace id {root_id}"),
            ));
        };
        let (data_fq, _, _) = namespace_facts(conn, data_id)?;
        Ok(DerivedWorld {
            root,
            data_id,
            data_fq,
            members,
            unadmitted,
            reuse: HashMap::new(),
            previous: Vec::new(),
            root_cartridge: None,
            root_families: 0,
            families: 0,
        })
    }

    /// The cartridge the root's derived families were registered under;
    /// `None` when the root derived no family (a pure facade).
    pub(crate) fn root_cartridge(&self) -> Option<i32> {
        self.root_cartridge
    }

    /// How many families the root derived — the receipt's count.
    pub(crate) fn root_families(&self) -> usize {
        self.root_families
    }

    /// How many families the whole world derived, every derivative
    /// counted: a world deriving none has nothing to ground.
    pub(crate) fn families(&self) -> usize {
        self.families
    }

    /// ADMIT the world: judge every recorded reference of every derivative,
    /// deriving what a qualified reference reaches, until no derivative
    /// remains unadmitted. Consumes the world — an admitted world is the
    /// catalog's, an unadmitted one is nobody's.
    pub(crate) fn admit(
        mut self,
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'_>,
    ) -> Result<()> {
        while let Some(source_id) = self.unadmitted.pop_front() {
            let member = self
                .members
                .get(&source_id)
                .cloned()
                .expect("an unadmitted source is a member");
            self.admit_member(conn, catalog, &member)?;
        }
        // Only now is the closure complete — admission may have derived
        // what a qualified reference reaches — so only now can a
        // previously held derivative be known to have no source left to
        // stand for. Destroy it rather than leave an empty grounded
        // namespace behind.
        let kept: std::collections::HashSet<i64> =
            self.members.values().map(|m| m.derived_id).collect();
        for stale in self.previous.iter().filter(|id| !kept.contains(id)) {
            destroy_derivative(conn, *stale)?;
        }
        Ok(())
    }

    /// The derivative of `source_id`, deriving it now if the world does not
    /// hold one yet: its namespace row, its closure record, its families,
    /// and its rewired edges (which may derive further).
    ///
    /// IDENTITY IS THE SOURCE ID, never a spelling. A rebuild reuses the
    /// row the closure record already pairs with this source; a fresh
    /// derivative takes an address whose leaf is `_<source leaf>_<source
    /// id>` — the leaf is a readable label, the id is what makes two
    /// distinct sources (`a::b`, `a__b`) two distinct derivatives. Nothing
    /// ever recovers a source from a derivative's spelling: the `grounding`
    /// row is the only relationship.
    fn member_of(&mut self, conn: &rusqlite::Connection, source_id: i64) -> Result<i64> {
        if let Some(member) = self.members.get(&source_id) {
            return Ok(member.derived_id);
        }
        let (source_fq, _, _) = namespace_facts(conn, source_id)?;
        let (derived_id, derived_fq) = match self.reuse.get(&source_id).copied() {
            Some(id) => {
                conn.execute(
                    "UPDATE namespace SET pid = ?1, default_data_ns = ?2, kind = 'grounded',
                            provenance = 'ground'
                     WHERE id = ?3",
                    rusqlite::params![self.root.derived_id, &self.data_fq, id],
                )
                .map_err(|e| {
                    DelightQLError::database_error("rebind a derivative", e.to_string())
                })?;
                let (fq, _, _) = namespace_facts(conn, id)?;
                (id, fq)
            }
            None => {
                let leaf = source_fq
                    .rsplit("::")
                    .next()
                    .expect("a namespace's name has a leaf");
                let name = format!("_{leaf}_{source_id}");
                let derived_fq = format!("{}::{name}", self.root.derived_fq);
                let taken: Option<i64> = conn
                    .query_row(
                        "SELECT id FROM namespace WHERE fq_name = ?1",
                        [&derived_fq],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "look up a derivative address",
                            e.to_string(),
                        )
                    })?;
                if taken.is_some() {
                    // The address is minted from the source id under a
                    // fresh or emptied root, and `_` names refuse user
                    // creation: an occupant is a defect, not a collision to
                    // escape.
                    return Err(DelightQLError::database_error(
                        format!(
                            "ground!() derivation defect: address '{derived_fq}' is occupied \
                             by a namespace the closure record does not pair with '{source_fq}'"
                        ),
                        "derivative address occupied",
                    ));
                }
                conn.execute(
                    "INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance)
                     VALUES (?1, ?2, ?3, ?4, 'grounded', 'ground')",
                    rusqlite::params![name, self.root.derived_id, &derived_fq, &self.data_fq],
                )
                .map_err(|e| {
                    DelightQLError::database_error("create a derivative", e.to_string())
                })?;
                (conn.last_insert_rowid(), derived_fq)
            }
        };
        let member = Derivative {
            source_id,
            source_fq,
            derived_id,
            derived_fq,
        };
        self.record(conn, &member)?;
        self.copy_families(conn, &member)?;
        self.copy_edges(conn, &member)?;
        Ok(derived_id)
    }

    /// Record one derivative in the closure and queue it for admission.
    fn record(&mut self, conn: &rusqlite::Connection, member: &Derivative) -> Result<()> {
        conn.execute(
            "INSERT INTO grounding
                 (grounded_namespace_id, data_namespace_id, lib_namespace_id, root_namespace_id)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                member.derived_id,
                self.data_id,
                member.source_id,
                self.root.derived_id
            ],
        )
        .map_err(|e| DelightQLError::database_error("record a derivative", e.to_string()))?;
        self.members.insert(member.source_id, member.clone());
        self.unadmitted.push_back(member.source_id);
        Ok(())
    }

    /// Copy the source's families into the derivative, under one cartridge
    /// naming the derivation. Answers the cartridge and the family count —
    /// and NO cartridge for a family-less source: the lifecycle finds a
    /// cartridge through the entities activated under it, so an empty one
    /// would be a row nothing can ever remove.
    fn copy_families(
        &mut self,
        conn: &rusqlite::Connection,
        member: &Derivative,
    ) -> Result<(Option<i32>, usize)> {
        let entities: Vec<(i32, String, bool, i32, Option<String>)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT e.id, e.name, e.name_stropped, e.type, e.doc
                     FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     WHERE ae.namespace_id = ?1
                     ORDER BY e.id",
                )
                .map_err(|e| {
                    DelightQLError::database_error("prepare source family listing", e.to_string())
                })?;
            let rows = stmt
                .query_map([member.source_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .map_err(|e| DelightQLError::database_error("list source families", e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| {
                    DelightQLError::database_error("decode a source family", e.to_string())
                })?;
            rows
        };
        if entities.is_empty() {
            return Ok((None, 0));
        }
        let cartridge_id = derivation_cartridge(conn, &member.source_fq, &self.data_fq)?;
        for (old_entity_id, name, stropped, kind, doc) in &entities {
            conn.execute(
                "INSERT INTO entity (name, name_stropped, type, cartridge_id, doc)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![name, stropped, kind, cartridge_id, doc],
            )
            .map_err(|e| {
                DelightQLError::database_error(format!("derive family '{name}'"), e.to_string())
            })?;
            let new_entity_id = conn.last_insert_rowid() as i32;
            crate::system::DelightQLSystem::copy_entity_subtables(
                conn,
                *old_entity_id,
                new_entity_id,
            )?;
            conn.execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![new_entity_id, member.derived_id, cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("activate derived family '{name}'"),
                    e.to_string(),
                )
            })?;
        }
        self.families += entities.len();
        Ok((Some(cartridge_id), entities.len()))
    }

    /// Copy the source's declared lexical graph onto the derivative, every
    /// derivable target rewired to its derivative.
    fn copy_edges(&mut self, conn: &rusqlite::Connection, member: &Derivative) -> Result<()> {
        let enlists = id_list(
            conn,
            "SELECT enlisted_namespace_id FROM namespace_local_enlist WHERE namespace_id = ?1
             ORDER BY enlisted_namespace_id",
            member.source_id,
        )?;
        for target in enlists {
            let target = self.rewired(conn, target)?;
            conn.execute(
                "INSERT OR IGNORE INTO namespace_local_enlist (namespace_id, enlisted_namespace_id)
                 VALUES (?1, ?2)",
                rusqlite::params![member.derived_id, target],
            )
            .map_err(|e| DelightQLError::database_error("derive an enlistment", e.to_string()))?;
        }
        let aliases: Vec<(String, i64)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT alias, target_namespace_id FROM namespace_local_alias
                     WHERE namespace_id = ?1 ORDER BY alias",
                )
                .map_err(|e| {
                    DelightQLError::database_error("prepare alias listing", e.to_string())
                })?;
            let rows = stmt
                .query_map([member.source_id], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| DelightQLError::database_error("list aliases", e.to_string()))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| DelightQLError::database_error("decode an alias", e.to_string()))?;
            rows
        };
        for (alias, target) in aliases {
            let target = self.rewired(conn, target)?;
            conn.execute(
                "INSERT OR IGNORE INTO namespace_local_alias (namespace_id, alias, target_namespace_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![member.derived_id, alias, target],
            )
            .map_err(|e| DelightQLError::database_error("derive an alias", e.to_string()))?;
        }
        let exposures = id_list(
            conn,
            "SELECT exposed_namespace_id FROM exposed_namespace WHERE exposing_namespace_id = ?1
             ORDER BY exposed_namespace_id",
            member.source_id,
        )?;
        for target in exposures {
            let target = self.rewired(conn, target)?;
            conn.execute(
                "INSERT OR IGNORE INTO exposed_namespace (exposing_namespace_id, exposed_namespace_id)
                 VALUES (?1, ?2)",
                rusqlite::params![member.derived_id, target],
            )
            .map_err(|e| DelightQLError::database_error("derive an exposure", e.to_string()))?;
        }
        Ok(())
    }

    /// Where an edge of the derivative points: the target's derivative when
    /// the target is derivable, the target itself otherwise (a data world,
    /// a system module, an already-grounded world, an ambient scratch).
    fn rewired(&mut self, conn: &rusqlite::Connection, target: i64) -> Result<i64> {
        if derivable(conn, target)?.is_some() {
            self.member_of(conn, target)
        } else {
            Ok(target)
        }
    }

    /// Judge every recorded reference of one derivative.
    fn admit_member(
        &mut self,
        conn: &rusqlite::Connection,
        catalog: CatalogRead<'_>,
        member: &Derivative,
    ) -> Result<()> {
        let mut reach = reach::capture_on(conn, &member.derived_fq, reach::World::Declaration)?;

        // Every reference every derived definition recorded, in a
        // deterministic order so a refusal names the same reference every
        // time.
        let references: Vec<(String, String, Option<String>)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT e.name, re.name, re.namespace
                     FROM referenced_entity re
                     JOIN entity e ON e.id = re.containing_entity_id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     WHERE ae.namespace_id = ?1
                     ORDER BY e.id, re.id",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to prepare grounding admission",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([member.derived_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to run grounding admission",
                        e.to_string(),
                    )
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to decode a grounding admission reference",
                        e.to_string(),
                    )
                })?;
            rows
        };

        for (entity_name, ref_name, ref_namespace) in references {
            let entity = format!("{}.{}", member.source_fq, entity_name);
            match ref_namespace {
                None => {
                    match super::select::judge_link_on(
                        conn,
                        catalog,
                        &reach,
                        Some(&self.data_fq),
                        &ref_name,
                        false,
                    )? {
                        // The reach answers: a lexical link — and every
                        // authored family it reaches must already stand
                        // inside this world, because the derivative's edges
                        // are the rewired ones.
                        Link::Lexical(candidates) => {
                            for candidate in candidates {
                                self.reached_inside(conn, &candidate.into_selected())?;
                            }
                        }
                        Link::Hole(Some(Selection::Unique(_))) => {}
                        Link::Hole(Some(Selection::Ambiguous(candidates))) => {
                            return Err(super::select::ambiguity_refusal(&ref_name, &candidates));
                        }
                        Link::Hole(Some(Selection::Missing)) | Link::Hole(None) => {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "ground!() validation failed: entity '{entity}' references \
                                     '{ref_name}' which does not exist in data namespace '{}'",
                                    self.data_fq
                                ),
                                "Unresolved reference",
                            ));
                        }
                    }
                }
                Some(display) => {
                    // The reference catalog stores a path's DISPLAY rendering
                    // (leaf-first, dot-joined); recover the fq spelling.
                    let fq = {
                        let mut parts: Vec<&str> = display.split('.').collect();
                        parts.reverse();
                        parts.join("::")
                    };
                    // A system-reserved surface (sys::*, std::*) is the
                    // engine's to serve and is not judged here.
                    if fq == "sys"
                        || fq.starts_with("sys::")
                        || fq == "std"
                        || fq.starts_with("std::")
                    {
                        continue;
                    }
                    // A qualified reference reaches a namespace exactly as
                    // a declared edge does: a derivable target it names
                    // joins the closure, and the reach is recaptured so the
                    // world's closure holds the new derivative.
                    if let Some(target) = super::select::qualifier_target(conn, &reach, &fq)? {
                        if !self.members.contains_key(&target.id)
                            && derivable(conn, target.id)?.is_some()
                        {
                            self.member_of(conn, target.id)?;
                            reach = reach::capture_on(
                                conn,
                                &member.derived_fq,
                                reach::World::Declaration,
                            )?;
                        }
                    }
                    match super::select::select_qualified_on(
                        conn, catalog, &ref_name, false, &fq, &reach,
                    )? {
                        Selection::Unique(selected) => self.reached_inside(conn, &selected)?,
                        Selection::Ambiguous(candidates) => {
                            return Err(super::select::ambiguity_refusal(&ref_name, &candidates));
                        }
                        Selection::Missing => {
                            return Err(DelightQLError::validation_error_categorized(
                                crate::uri_registry::subcat::GROUND_UNRESOLVED_REFERENCE,
                                format!(
                                    "ground!() validation failed: entity '{entity}' \
                                     references '{fq}.{ref_name}', which resolves to \
                                     nothing in this session. Strict validation covers \
                                     qualified references too — nothing is created."
                                ),
                                "unresolved qualified reference",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// An authored family a derivative reaches must itself be a derivative
    /// (or stand in a world that publishes its own data binding). Reaching
    /// a source — derived by this world or not — is a derivation defect,
    /// refused loudly rather than published. THE PROOF IS POSITIVE: the
    /// reached namespace is one of THIS world's derivatives, or it
    /// publishes a data binding of its own (a data world, an ambient
    /// scratch, another grounded world); a derivable source is neither,
    /// and its membership as a SOURCE of this closure proves nothing about
    /// what selection handed back.
    fn reached_inside(&self, conn: &rusqlite::Connection, selected: &Selected<'_>) -> Result<()> {
        let Selected::Authored(family) = selected else {
            return Ok(());
        };
        let namespace_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [family.namespace()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error("look up a reached namespace", e.to_string())
            })?;
        let Some(id) = namespace_id else {
            return Err(DelightQLError::database_error(
                format!(
                    "corrupt catalog: '{}' selected from namespace '{}', which has no row",
                    family.name(),
                    family.namespace()
                ),
                "reached namespace missing",
            ));
        };
        let is_derivative = self.members.values().any(|member| member.derived_id == id);
        if is_derivative || derivable(conn, id)?.is_none() {
            return Ok(());
        }
        Err(DelightQLError::database_error(
            format!(
                "ground!() derivation defect: '{}' of the source '{}' was reached from '{}' \
                 instead of a derivative",
                family.name(),
                family.namespace(),
                self.root.derived_fq
            ),
            "source reached from a derived world",
        ))
    }
}

/// The cartridge a derivation registers families under: one per
/// derivative that has families, named for the source and the data world.
/// The lifecycle reaches it only through the entities activated under it,
/// which is why a derivation mints one only when it has entities to hold.
pub(crate) fn derivation_cartridge(
    conn: &rusqlite::Connection,
    source_fq: &str,
    data_fq: &str,
) -> Result<i32> {
    conn.execute(
        "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
         VALUES (1, ?1, ?2, NULL, 1, 1, 0)",
        rusqlite::params![
            crate::bootstrap::SourceType::File.as_i32(),
            &format!("ground://{source_fq}<-{data_fq}"),
        ],
    )
    .map_err(|e| DelightQLError::database_error("create a derivation cartridge", e.to_string()))?;
    Ok(conn.last_insert_rowid() as i32)
}

/// REBUILD every derived world that derives from a namespace whose
/// replacement load is COMPLETE — families and declared lexical graph
/// published together — and re-admit each. The token is minted only by
/// the road that completes a load, so no rebuild can read a source whose
/// edges are still to come.
pub(crate) fn rebuild_dependents(
    conn: &rusqlite::Connection,
    catalog: CatalogRead<'_>,
    load: &crate::system::PublishedLoad,
) -> Result<()> {
    for root_id in roots_deriving_from(conn, load.namespace_id())? {
        DerivedWorld::rebuild(conn, root_id)?.admit(conn, catalog)?;
    }
    Ok(())
}

/// The roots of every derived world that derives from `source_id`.
fn roots_deriving_from(conn: &rusqlite::Connection, source_id: i64) -> Result<Vec<i64>> {
    id_list(
        conn,
        "SELECT DISTINCT root_namespace_id FROM grounding WHERE lib_namespace_id = ?1
         ORDER BY root_namespace_id",
        source_id,
    )
}

/// The roots of every derived world bound to the data world `data_id`.
pub(crate) fn roots_bound_to(conn: &rusqlite::Connection, data_id: i64) -> Result<Vec<i64>> {
    id_list(
        conn,
        "SELECT DISTINCT root_namespace_id FROM grounding WHERE data_namespace_id = ?1
         ORDER BY root_namespace_id",
        data_id,
    )
}

/// The closure `namespace_id` belongs to, as (source, derivative) pairs;
/// empty when the namespace is no derivative.
pub(in crate::defuse) fn closure_of(
    conn: &rusqlite::Connection,
    namespace_id: i64,
) -> Result<Vec<(i64, i64)>> {
    let mut stmt = conn
        .prepare(
            "SELECT lib_namespace_id, grounded_namespace_id FROM grounding
             WHERE root_namespace_id = (SELECT root_namespace_id FROM grounding
                                        WHERE grounded_namespace_id = ?1)",
        )
        .map_err(|e| DelightQLError::database_error("prepare closure lookup", e.to_string()))?;
    let rows = stmt
        .query_map([namespace_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| DelightQLError::database_error("look up the closure", e.to_string()))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| DelightQLError::database_error("decode the closure", e.to_string()))?;
    Ok(rows)
}

/// Whether a namespace is DERIVABLE — a lexical definition world publishing
/// no data binding of its own — answering its fq name when it is. A data
/// namespace, a system module, a structural container, an already-grounded
/// world, a scratch namespace with an ambient world, and an archived
/// blueprint are not derived: their names stand for themselves.
fn derivable(conn: &rusqlite::Connection, namespace_id: i64) -> Result<Option<String>> {
    let (fq, kind, default_data_ns) = namespace_facts(conn, namespace_id)?;
    if default_data_ns.is_some() || !matches!(kind.as_str(), "lib" | "scratch" | "unknown") {
        return Ok(None);
    }
    if crate::system::blueprint_shadowing(conn, &fq)?.is_some() {
        return Ok(None);
    }
    Ok(Some(fq))
}

fn namespace_facts(
    conn: &rusqlite::Connection,
    namespace_id: i64,
) -> Result<(String, String, Option<String>)> {
    conn.query_row(
        "SELECT fq_name, kind, default_data_ns FROM namespace WHERE id = ?1",
        [namespace_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!("corrupt catalog: namespace id {namespace_id} has no row"),
            e.to_string(),
        )
    })
}

fn id_list(conn: &rusqlite::Connection, sql: &str, param: i64) -> Result<Vec<i64>> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| DelightQLError::database_error("prepare an id listing", e.to_string()))?;
    let rows = stmt
        .query_map([param], |row| row.get::<_, i64>(0))
        .map_err(|e| DelightQLError::database_error("run an id listing", e.to_string()))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| DelightQLError::database_error("decode an id listing", e.to_string()))?;
    Ok(rows)
}

/// Destroy an emptied derivative the closure no longer reaches: the edges
/// naming it, then its row.
fn destroy_derivative(conn: &rusqlite::Connection, derived_id: i64) -> Result<()> {
    for sql in [
        "DELETE FROM namespace_local_alias WHERE namespace_id = ?1 OR target_namespace_id = ?1",
        "DELETE FROM namespace_local_enlist WHERE namespace_id = ?1 OR enlisted_namespace_id = ?1",
        "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
        "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1 OR exposed_namespace_id = ?1",
        "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
        "DELETE FROM grounding WHERE grounded_namespace_id = ?1",
        "DELETE FROM namespace WHERE id = ?1",
    ] {
        conn.execute(sql, [derived_id]).map_err(|e| {
            DelightQLError::database_error("destroy a stale derivative", e.to_string())
        })?;
    }
    Ok(())
}
