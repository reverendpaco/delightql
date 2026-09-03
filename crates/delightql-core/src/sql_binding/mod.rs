// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Where a semantic port meets the physical column a statement emits.
//!
//! A PORT is an output position of a relation. A SLOT is an output column
//! of one emitted statement. They are different things, and the map from
//! one to the other is recorded at the moment the statement's output list
//! is laid out — never recovered afterwards from lineage, from emitted
//! characters, from the value a column carries, or from there happening to
//! be one plausible candidate left.
//!
//! # What binds, and where
//!
//! A compound (a bag run) is the first family bound here. Its evidence is
//! the authority's: each step's contribution table says what every arm
//! puts through every result position, and each arm record carries the
//! ordered interface that arm published. Lowering supplies the one thing
//! the authority cannot know — the ordered columns each emitted branch
//! actually selects — and [`SqlBindingMap::bind_run`] pairs the two by
//! position, which is what an ordered interface being emitted MEANS.
//!
//! The pairing is therefore not a guess to be checked: a branch whose
//! width disagrees with its arm's recorded interface is not that arm being
//! emitted, and it refuses.
//!
//! # What a caller cannot do
//!
//! [`SqlSlotId`] has a private payload and is minted only here, and no
//! entrance takes a port and a column side by side. What a caller supplies
//! is a [`BranchLayout`] — a closed value the transformer's republication
//! road produces from the act of laying a branch out, carrying the ARM
//! RELATION it emits. Binding refuses a layout whose arm is not the arm the
//! evidence names, so a caller has no spelling for "this arm is that
//! statement" either.
//!
//! The methods here are `pub(crate)`, because the `Registry` that owns the
//! map lives in another module. The wall is [`BranchLayout`]: its one
//! producer is fenced to the file that lays branches out.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry};
use crate::pipeline::transformer::BranchLayout;
use crate::relation::{BuilderMark, Contribution, PaddingId, PortId, SemanticRelation};

/// One physical output column of one emitted statement.
///
/// Opaque, with a private payload: nothing outside this authority turns a
/// column into a slot, so a slot in a binding is a slot this authority put
/// there.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SqlSlotId(ColId);

/// What one branch emits for one result position.
///
/// Closed, and padding is a MEMBER rather than an absence: a corresponding
/// union's branch that reaches a position with nothing emits a typed null,
/// and a reader must be able to tell that from a binding nobody recorded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SqlOutput {
    /// The branch selects this physical column into the position.
    Slot(SqlSlotId),
    /// The branch emits the typed null this padding stands for.
    Pad(PaddingId),
}

impl SqlSlotId {
    /// The emitted column. READ-ONLY, and there is no inverse.
    pub(crate) fn column(self) -> ColId {
        self.0
    }
}

/// One compound the map has bound. A KEY, not the binding: the record
/// itself stays in the compilation's map, so a handle grants nothing on its
/// own and cannot be assembled from parts.
///
/// It carries the EPOCH of the map that issued it. Without that, a handle
/// is an index, and index zero of one compilation's map selects index zero
/// of another's — deterministically, not by luck.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RunBinding(BuilderMark, usize);

/// One emitted occurrence of one semantic relation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SqlSiteId(BuilderMark, usize);

/// One compilation's semantic-to-physical bindings.
///
/// Lives on the [`Registry`] for the reason the relation store does: a
/// registry IS one compilation, so a binding and the semantic evidence it
/// elaborates cannot come from two epochs, and a binding recorded here
/// outlives the wrapper, the refinement rebuild and the emission that
/// occasioned it. It is OPAQUE there — every method is private to
/// `crate::sql_binding` — so holding the registry binds nothing.
///
/// The set family is the first family recorded here. Later vertical packets
/// extend the map with their own construction roads; none of them acquires
/// a way to pair a port with a column by hand.
pub struct SqlBindingMap {
    /// The compilation this map binds for. Taken from the relation store
    /// rather than drawn again, so a binding and the semantic evidence it
    /// elaborates carry ONE epoch rather than two that agree.
    epoch: BuilderMark,
    /// Every compound bound in this compilation: its result positions, in
    /// publication order, and what each branch emits for each of them.
    ///
    /// TOTAL — one entry per position per branch, none of them optional —
    /// so a reader asks by key and there is no absence to interpret.
    compounds: RefCell<Vec<Vec<(PortId, Vec<SqlOutput>)>>>,
    sites: RefCell<Vec<SiteLayout>>,
}

struct SiteLayout {
    /// Present only while this site still realizes one complete semantic
    /// interface. A shape-changing physical operation clears the claim; its
    /// exact result is bound by the semantic operation that owns it.
    relation: Option<crate::relation::RelationId>,
    columns: Vec<SqlSlotId>,
    ports: HashMap<PortId, usize>,
    /// Ports this site's own relation publishes, excluding translated
    /// ancestor aliases added when the row closes.
    direct_ports: std::collections::HashSet<PortId>,
    /// Construction-recorded input ports carried only until an operation's
    /// physical predicate is spent. They are addressable at the site but are
    /// not positions of the relation's published interface.
    support: HashMap<PortId, usize>,
    physical: HashMap<ColId, usize>,
}

impl SqlBindingMap {
    pub(crate) fn new(epoch: BuilderMark) -> Self {
        SqlBindingMap {
            epoch,
            compounds: RefCell::new(Vec::new()),
            sites: RefCell::new(Vec::new()),
        }
    }

    /// Record an emitted site that realizes NO semantic relation.
    ///
    /// A physical wrap is still an emission: the columns it selects are the
    /// ones a reference to what it stands over must be re-anchored through.
    /// Without a site there is nothing to chain, and a reference written
    /// against the pre-wrap occurrence qualifies by a FROM entry the
    /// statement no longer has.
    pub(crate) fn bind_physical(&self, columns: &[ColId]) -> SqlSiteId {
        let physical = columns
            .iter()
            .copied()
            .enumerate()
            .map(|(position, column)| (column, position))
            .collect();
        let layout = SiteLayout {
            relation: None,
            columns: columns.iter().copied().map(SqlSlotId).collect(),
            ports: HashMap::new(),
            direct_ports: std::collections::HashSet::new(),
            support: HashMap::new(),
            physical,
        };
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        SqlSiteId(self.epoch, sites.len() - 1)
    }

    /// BEGIN BINDING THE ROW ONE LOWERING OPERATION EMITTED.
    ///
    /// There is no entrance here that takes a relation beside a column
    /// list. A row is built one STATED position at a time — this slot
    /// publishes that port, this one realizes that dependency, this one is
    /// the compiler's own — and closing it is what makes the site. Equal
    /// width is not evidence and is never asked for: a position is a port
    /// because the operation emitting it said so.
    /// BIND A RELATION TO THE ROW OF ITS OWN INTERFACE.
    ///
    /// A level that emits exactly what the relation publishes, under the
    /// occurrences the relation publishes it under. There is no column list
    /// here to get wrong: the row IS the interface, read from the
    /// authority's own record, position for position.
    pub(crate) fn bind_interface(
        &self,
        sealed: &crate::relation::Relations,
        relation: &SemanticRelation,
    ) -> Result<SqlSiteId> {
        let mut row = self.emitting(sealed, relation)?;
        for port in sealed.interface(relation)?.ports().to_vec() {
            row.publishes(port.column(), port)?;
        }
        row.close(sealed)
    }

    pub(crate) fn emitting(
        &self,
        sealed: &crate::relation::Relations,
        relation: &SemanticRelation,
    ) -> Result<BindingRow<'_>> {
        Ok(BindingRow {
            map: self,
            relation: *relation,
            owed: sealed.interface(relation)?.ports().to_vec(),
            columns: Vec::new(),
            ports: HashMap::new(),
            support: HashMap::new(),
            physical: HashMap::new(),
        })
    }

    /// The physical slot this exact site emits for an exact semantic port.
    pub(crate) fn at(&self, site: SqlSiteId, port: PortId) -> Result<ColId> {
        self.maybe_at(site, port)?.ok_or_else(|| {
            let sites = self.sites.borrow();
            let layout = &sites[site.1];
            unbound(&format!(
                "semantic port {port:?} is absent from site {site:?} for {:?}, which binds {:?}",
                layout.relation,
                layout.ports.keys().copied().collect::<Vec<_>>()
            ))
        })
    }

    /// The physical slot for a port when this exact site binds it.
    pub(crate) fn maybe_at(&self, site: SqlSiteId, port: PortId) -> Result<Option<ColId>> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        Ok(layout
            .ports
            .get(&port)
            .or_else(|| layout.support.get(&port))
            .map(|position| layout.columns[*position].column()))
    }

    /// The output position this exact semantic port occupies at this site.
    ///
    /// A positional SQL operation asks the binding laid down with the site;
    /// it does not rediscover the position from the emitted column's name or
    /// lineage.
    pub(crate) fn slot_at(&self, site: SqlSiteId, port: PortId) -> Result<usize> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        layout
            .ports
            .get(&port)
            .or_else(|| layout.support.get(&port))
            .copied()
            .ok_or_else(|| {
                unbound(&format!(
                    "semantic port {port:?} is absent from site {site:?} for {:?}",
                    layout.relation
                ))
            })
    }

    pub(crate) fn physical_at(&self, site: SqlSiteId, column: ColId) -> Result<Option<ColId>> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        Ok(layout
            .physical
            .get(&column)
            .map(|position| layout.columns[*position].column()))
    }

    /// Record source occurrences a physical merge emits through one retained
    /// output position.
    pub(crate) fn merge_aliases(
        &self,
        site: SqlSiteId,
        aliases: &[(ColId, ColId)],
    ) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let mut physical = prior.physical.clone();
        for (source, retained) in aliases {
            let position = prior.physical.get(retained).copied().ok_or_else(|| {
                unbound("a physical merge retains a position its output does not emit")
            })?;
            match physical.insert(*source, position) {
                Some(prior) if prior != position => {
                    return Err(unbound(
                        "a merged source occurrence names two physical output positions",
                    ))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: prior.relation,
            columns: prior.columns.clone(),
            ports: prior.ports.clone(),
            direct_ports: prior.direct_ports.clone(),
            support: prior.support.clone(),
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// Carry every exact physical occurrence recognized by operand sites
    /// onto the positions a new operation retained from them.
    pub(crate) fn carry_physical_aliases(
        &self,
        site: SqlSiteId,
        operands: &[SqlSiteId],
    ) -> Result<SqlSiteId> {
        if site.0 != self.epoch || operands.iter().any(|operand| operand.0 != self.epoch) {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let target = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let mut physical = target.physical.clone();
        let mut reached: HashMap<
            ColId,
            (
                std::collections::BTreeSet<usize>,
                std::collections::BTreeSet<usize>,
            ),
        > = HashMap::new();
        for operand in operands {
            let source = sites
                .get(operand.1)
                .ok_or_else(|| unbound("a SQL operand site this compilation never recorded"))?;
            for (alias, source_position) in &source.physical {
                if target.physical.contains_key(alias) {
                    continue;
                }
                let emitted = source.columns[*source_position].column();
                let Some(target_position) = target.physical.get(&emitted).copied() else {
                    continue;
                };
                let entry = reached.entry(*alias).or_default();
                entry.0.insert(target_position);
                if source
                    .direct_ports
                    .iter()
                    .any(|port| port.column() == *alias)
                {
                    entry.1.insert(target_position);
                }
            }
        }
        for (alias, (all, direct)) in reached {
            let positions = if direct.is_empty() { all } else { direct };
            let mut positions = positions.into_iter();
            let (Some(position), None) = (positions.next(), positions.next()) else {
                continue;
            };
            physical.insert(alias, position);
        }
        let layout = SiteLayout {
            relation: target.relation,
            columns: target.columns.clone(),
            ports: target.ports.clone(),
            direct_ports: target.direct_ports.clone(),
            support: target.support.clone(),
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// The output position occupied by this exact physical occurrence.
    pub(crate) fn physical_slot_at(&self, site: SqlSiteId, column: ColId) -> Result<Option<usize>> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        Ok(layout.physical.get(&column).copied())
    }

    /// Whether a published semantic position stands at this physical
    /// occurrence's place at this exact site.
    ///
    /// A hygienic column that IS a port — a row-number witness a bound
    /// filters on, a correlation carrier — is part of the relation's
    /// interface and has to be emitted; one that is not is physical support
    /// no caller addresses.
    pub(crate) fn binds_a_port(&self, site: SqlSiteId, column: ColId) -> Result<bool> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let Some(position) = layout.physical.get(&column) else {
            return Ok(false);
        };
        Ok(layout.ports.values().any(|port| port == position))
    }

    /// Whether this physical occurrence realizes an operation dependency
    /// rather than a published semantic position at this exact site.
    pub(crate) fn is_support(&self, site: SqlSiteId, column: ColId) -> Result<bool> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let layout = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let Some(position) = layout.physical.get(&column) else {
            return Ok(false);
        };
        Ok(layout.support.values().any(|support| support == position))
    }

    /// Rebind the same ordered semantic interface at a new physical site.
    ///
    /// SQL-only wrapping changes aliases and column occurrences, but it does
    /// not create a semantic relation. The old site's ordered ports are the
    /// complete evidence for the new site; lowering supplies only the new
    /// ordered slots. No value or name correspondence participates.
    pub(crate) fn rebind_site(&self, site: SqlSiteId, columns: &[ColId]) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        if prior.columns.len() != columns.len() {
            return Err(unbound(
                "a physical wrap changed the width of the semantic interface",
            ));
        }
        let mut physical = prior.physical.clone();
        for (position, slot) in columns.iter().copied().enumerate() {
            match physical.insert(slot, position) {
                Some(prior) if prior != position => {
                    return Err(unbound(
                        &format!("physical occurrence {slot:?} names positions {prior} and {position} at one SQL site"),
                    ))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: prior.relation,
            columns: columns.iter().copied().map(SqlSlotId).collect(),
            ports: prior.ports.clone(),
            direct_ports: prior.direct_ports.clone(),
            support: prior.support.clone(),
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// Record the names a physical body's output positions had before an
    /// enclosing SQL binding renamed them. The site's emitted columns stay
    /// canonical; these aliases exist only to re-anchor later physical uses.
    pub(crate) fn alias_site(&self, site: SqlSiteId, aliases: &[ColId]) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        if prior.columns.len() != aliases.len() {
            return Err(unbound(
                "a physical alias list and its emitted layout have different widths",
            ));
        }
        let mut physical = prior.physical.clone();
        for (position, alias) in aliases.iter().copied().enumerate() {
            match physical.insert(alias, position) {
                Some(prior) if prior != position => {
                    return Err(unbound(&format!(
                        "physical occurrence {alias:?} names positions {prior} and {position} at one SQL site"
                    )))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: prior.relation,
            columns: prior.columns.clone(),
            ports: prior.ports.clone(),
            direct_ports: prior.direct_ports.clone(),
            support: prior.support.clone(),
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// Rebind an existing semantic interface through a SQL-only layer that
    /// appends physical support columns. The semantic positions remain the
    /// exact prefix; support slots are addressable only as physical columns
    /// and never become semantic ports.
    pub(crate) fn extend_site(&self, site: SqlSiteId, columns: &[ColId]) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        if prior.columns.len() > columns.len() {
            return Err(unbound(
                "a physical support layer dropped a semantic output position",
            ));
        }
        let mut physical = prior.physical.clone();
        for (position, column) in columns.iter().copied().enumerate() {
            match physical.insert(column, position) {
                Some(prior) if prior != position => {
                    return Err(unbound(
                        &format!("physical occurrence {column:?} names positions {prior} and {position} at one SQL site"),
                    ))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: prior.relation,
            columns: columns.iter().copied().map(SqlSlotId).collect(),
            ports: prior.ports.clone(),
            direct_ports: prior.direct_ports.clone(),
            support: prior.support.clone(),
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// Re-layout a physical site by exact old positions.
    ///
    /// A physical projection SELECTS positions. A port whose position is not
    /// selected stops being emitted at this site and stops being addressable
    /// there — which is what pruning an internal carrier means, and what a
    /// reference to it must then discover. The SEMANTIC interface is
    /// untouched: this edits one site's binding, and nothing here can reach
    /// the relation's recorded interface.
    pub(crate) fn project_site(
        &self,
        site: SqlSiteId,
        selected: &[usize],
        columns: &[ColId],
    ) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        if selected.len() != columns.len() {
            return Err(unbound(
                "a physical projection and its emitted layout have different widths",
            ));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let mut old_to_new = HashMap::new();
        for (new, old) in selected.iter().copied().enumerate() {
            if old >= prior.columns.len() || old_to_new.insert(old, new).is_some() {
                return Err(unbound(
                    "a physical projection selected an absent or repeated input slot",
                ));
            }
        }
        let ports: HashMap<_, _> = prior
            .ports
            .iter()
            .filter_map(|(port, old)| old_to_new.get(old).map(|new| (*port, *new)))
            .collect();
        let support = prior
            .support
            .iter()
            .filter_map(|(port, old)| old_to_new.get(old).map(|new| (*port, *new)))
            .collect();
        let mut physical = HashMap::new();
        for (column, old) in &prior.physical {
            if let Some(new) = old_to_new.get(old) {
                physical.insert(*column, *new);
            }
        }
        for (new, column) in columns.iter().copied().enumerate() {
            match physical.insert(column, new) {
                Some(prior) if prior != new => {
                    return Err(unbound(
                        &format!("physical occurrence {column:?} names positions {prior} and {new} at one SQL site"),
                    ))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: prior.relation,
            columns: columns.iter().copied().map(SqlSlotId).collect(),
            ports,
            direct_ports: prior
                .direct_ports
                .iter()
                .copied()
                .filter(|port| old_to_new.contains_key(&prior.ports[port]))
                .collect(),
            support,
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// Lay out a physical operation that carries every semantic position and
    /// may append SQL-only outputs. `sources[new]` names the exact old slot
    /// carried into that output; `None` is a new physical support slot.
    pub(crate) fn reshape_site(
        &self,
        site: SqlSiteId,
        sources: &[Option<usize>],
        columns: &[ColId],
    ) -> Result<SqlSiteId> {
        if site.0 != self.epoch {
            return Err(unbound("a SQL site another compilation issued"));
        }
        if sources.len() != columns.len() {
            return Err(unbound(
                "a physical layout and its emitted columns have different widths",
            ));
        }
        let sites = self.sites.borrow();
        let prior = sites
            .get(site.1)
            .ok_or_else(|| unbound("a SQL site this compilation never recorded"))?;
        let mut old_to_new = HashMap::new();
        for (new, source) in sources.iter().copied().enumerate() {
            let Some(old) = source else { continue };
            if old >= prior.columns.len() || old_to_new.insert(old, new).is_some() {
                return Err(unbound(
                    "a physical layout selected an absent or repeated input slot",
                ));
            }
        }
        let mut complete = true;
        let ports = prior
            .ports
            .iter()
            .filter_map(|(port, old)| match old_to_new.get(old).copied() {
                Some(new) => Some((*port, new)),
                None => {
                    complete = false;
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        let support = prior
            .support
            .iter()
            .filter_map(|(port, old)| old_to_new.get(old).map(|new| (*port, *new)))
            .collect();
        let mut physical = HashMap::new();
        for (column, old) in &prior.physical {
            if let Some(new) = old_to_new.get(old) {
                physical.insert(*column, *new);
            }
        }
        for (new, column) in columns.iter().copied().enumerate() {
            match physical.insert(column, new) {
                Some(prior) if prior != new => {
                    return Err(unbound(
                        &format!("physical occurrence {column:?} names positions {prior} and {new} at one SQL site"),
                    ))
                }
                _ => {}
            }
        }
        let layout = SiteLayout {
            relation: complete.then_some(prior.relation).flatten(),
            columns: columns.iter().copied().map(SqlSlotId).collect(),
            ports,
            direct_ports: prior
                .direct_ports
                .iter()
                .copied()
                .filter(|port| old_to_new.contains_key(&prior.ports[port]))
                .collect(),
            support,
            physical,
        };
        drop(sites);
        let mut sites = self.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.epoch, sites.len() - 1))
    }

    /// One branch's whole column of a compound's binding: every result
    /// position, in publication order, with what this branch emits for it.
    ///
    /// EXACT KEYED LOOKUP. A binding this compilation never recorded, or a
    /// branch this compound does not have, is a refusal — never a search
    /// for something nearby. The positions come back WITH the outputs so a
    /// caller emitting a select list can check it is emitting the ones it
    /// bound rather than trusting a count.
    pub(crate) fn branch(
        &self,
        binding: RunBinding,
        branch: usize,
    ) -> Result<Vec<(PortId, SqlOutput)>> {
        if binding.0 != self.epoch {
            return Err(unbound("a binding another compilation issued"));
        }
        self.compounds
            .borrow()
            .get(binding.1)
            .ok_or_else(|| unbound("a binding this compilation never recorded"))?
            .iter()
            .map(|(port, row)| {
                row.get(branch)
                    .copied()
                    .map(|output| (*port, output))
                    .ok_or_else(|| unbound("a set branch this compound's binding does not have"))
            })
            .collect()
    }

    /// Bind one left-nested bag RUN to the branches that emit it.
    ///
    /// `steps` is one semantic result per bag operator, innermost first;
    /// `branches` is one laid-out branch per emitted statement, so a run of
    /// `n` steps has `n + 1` of them. Step `j` merges whatever step `j - 1`
    /// produced with branch `j + 1`, which is the shape the author wrote
    /// and the shape SQL stacks.
    ///
    /// ONE ACT: the branches and the result positions are bound together
    /// from the authority's evidence, so no caller can bind a branch and
    /// then decide separately what a position draws from it. Each layout
    /// must name the arm the step's evidence names — a branch belonging to
    /// another arm is refused, not zipped.
    pub(crate) fn bind_run(
        &self,
        registry: &Registry,
        steps: &[SemanticRelation],
        branches: &[BranchLayout],
    ) -> Result<RunBinding> {
        if steps.is_empty() || branches.len() != steps.len() + 1 {
            return Err(unbound(
                "a bag run has one step per operator and one branch per arm",
            ));
        }
        // Carried: for each of the result positions reached so far, what
        // every branch bound so far emits. The inner step's row is exactly
        // what the outer step's left cell stands for, so the composition
        // happens once, here, rather than in a lowerer flattening the run.
        let mut carried: Vec<(PortId, Vec<SqlOutput>)> = Vec::new();
        for (index, step) in steps.iter().enumerate() {
            let matrix = crate::relation::contributions(registry, step)?
                .ok_or_else(|| unbound("a bag run step whose table nobody recorded"))?;
            // A SET PUBLISHES POSITIONS. Every arm states its heading with
            // its relation, so a step publishing none is a compiler fault
            // rather than a shape to accommodate — there is nothing for its
            // branches to stack under and nothing to bind.
            if matrix.outputs().is_empty() {
                return Err(unbound("a bag run step that publishes no position"));
            }
            let arms = matrix.arms();
            if arms.len() != 2 {
                return Err(unbound("a bag run's step merges exactly two arms"));
            }
            let mut arms = arms.iter();
            let from_below = arms.next().expect("two arms");
            let right = site(
                registry,
                arms.next().expect("two arms"),
                &branches[index + 1],
            )?;
            // THE RUN MUST ACTUALLY NEST. Step `j`'s left arm is what step
            // `j - 1` produced; a sequence that does not is not one run, and
            // composing its rows would carry a branch's columns into a
            // compound that never had them.
            let left: Option<HashMap<PortId, SqlSlotId>> = match index.checked_sub(1) {
                None => Some(site(registry, from_below, &branches[0])?),
                Some(beneath) if from_below.relation() == steps[beneath].relation() => None,
                Some(_) => return Err(unbound("a bag step does not merge the step beneath it")),
            };
            let width = index + 1;
            let mut next = Vec::with_capacity(matrix.outputs().len());
            for output in matrix.outputs() {
                let mut cells = output.by_arm().iter();
                let from_left = cells.next().expect("two cells");
                let from_right = cells.next().expect("two cells");
                let mut row = match (&left, from_left) {
                    (Some(site), cell) => vec![resolve(site, cell)?],
                    // The left operand is the run so far: its own row says
                    // what each branch beneath it emits, and a padded inner
                    // result pads every one of them.
                    (None, Contribution::Port(port)) => carried
                        .iter()
                        .find(|(recorded, _)| recorded == port)
                        .map(|(_, row)| row.clone())
                        .ok_or_else(|| {
                            unbound("a bag step draws from a position its operand never published")
                        })?,
                    (None, Contribution::Padding(pad)) => vec![SqlOutput::Pad(*pad); width],
                };
                if row.len() != width {
                    return Err(unbound(
                        "a bag step's operand binds a different branch count",
                    ));
                }
                row.push(resolve(&right, from_right)?);
                next.push((output.result(), row));
            }
            carried = next;
        }
        Ok(self.record(carried))
    }

    /// Bind a MINUS to the one branch that emits it.
    ///
    /// A minus exports its left operand and probes its right, so there is
    /// one emitting branch and no padding. The pairing is not re-derived
    /// here: the authority's exact-heading map already says, in result
    /// order, which of the left operand's ports each result position
    /// carries, and the layout says where that operand's ports are emitted.
    ///
    /// The same road as [`SqlBindingMap::bind_run`], reached with one arm
    /// instead of two. A minus that reaches lowering without the pairing
    /// its construction proved is refused rather than zipped.
    pub(crate) fn bind_export(
        &self,
        registry: &Registry,
        result: &SemanticRelation,
        branch: &BranchLayout,
    ) -> Result<RunBinding> {
        let pairing = crate::relation::anti_match(registry, result)?
            .ok_or_else(|| unbound("a minus whose exact pairing nobody recorded"))?;
        let left = site(registry, &pairing.left_arm(), branch)?;
        let ports = crate::relation::published_ports(registry, result)?;
        if ports.len() != pairing.pairs().len() {
            return Err(unbound(
                "a minus publishes a different width than the pairing it exports",
            ));
        }
        let carried = ports
            .iter()
            .zip(pairing.pairs())
            .map(|(result_port, pair)| {
                resolve(&left, &Contribution::Port(pair.left()))
                    .map(|output| (*result_port, vec![output]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.record(carried))
    }

    fn record(&self, carried: Vec<(PortId, Vec<SqlOutput>)>) -> RunBinding {
        let mut compounds = self.compounds.borrow_mut();
        compounds.push(carried);
        RunBinding(self.epoch, compounds.len() - 1)
    }
}

/// One branch's site: every port its arm published, at the column the
/// branch emits it as.
///
/// The pairing is POSITIONAL because that is what emitting an ordered
/// interface means — the kth position of the heading is the kth item of the
/// select list. A branch of another width is not this arm being emitted,
/// and it refuses.
///
/// AN ARM THAT PUBLISHES NOTHING BINDS NOTHING. Its interface is empty
/// because it is opaque or because it is still on the predecessor
/// occurrence road, where the positions arrive in the registry heading
/// after the authority handed the carrier back. Either way it contributes
/// no port to any result position — every cell it fills is a padding — so
/// there is nothing here to pair, and a lookup that reaches this site
/// refuses. Widening the check to such an arm would refuse a set that binds
/// nothing and needs nothing bound.
fn site(
    registry: &Registry,
    arm: &crate::relation::SetArmRecord,
    branch: &BranchLayout,
) -> Result<HashMap<PortId, SqlSlotId>> {
    // AN ARM THAT PUBLISHES NOTHING BINDS NOTHING. Its interface is empty
    // because it is opaque or because it is still on the predecessor
    // occurrence road, where the positions arrive in the registry heading
    // after the authority handed the carrier back. Every cell it fills is a
    // padding, so there is nothing here to pair.
    if arm.ports().is_empty() {
        return Ok(HashMap::new());
    }
    // WHAT A BRANCH EMITS IS ITS OWN RELATION'S INTERFACE, in order,
    // because that is what emitting an ordered interface means. AND IT MUST
    // BE THE SAME OPERAND: where refinement replaced the one the evidence
    // names, it said so and the authority checked it, and the map it
    // recorded is what an old port is translated through. Where nobody said
    // so, these are two operands, and pairing them by position is the guess
    // this authority exists to refuse.
    let emitted = crate::relation::published_ports(registry, branch.arm())?;
    let replacement = match branch.arm().relation() == arm.relation() {
        true => None,
        false => Some(
            crate::relation::replacement(registry, arm.relation(), branch.arm())?
                .ok_or_else(|| unbound(WRONG_ARM))?,
        ),
    };
    if emitted.len() != branch.columns().len() || emitted.len() != arm.ports().len() {
        return Err(unbound(WRONG_ARM));
    }
    let mut fields = HashMap::with_capacity(branch.columns().len());
    for ((recorded, emits), column) in arm.ports().iter().zip(&emitted).zip(branch.columns()) {
        let carries = match &replacement {
            None => *recorded,
            Some(map) => map.answer(*recorded).ok_or_else(|| unbound(WRONG_ARM))?,
        };
        if carries != *emits || fields.insert(*recorded, SqlSlotId(*column)).is_some() {
            return Err(unbound(WRONG_ARM));
        }
    }
    Ok(fields)
}

/// ONE EMITTED ROW BEING BOUND, position by position.
///
/// Positions arrive in EMISSION ORDER and each says what it realizes. The
/// row closes only when every position of the relation's interface has been
/// placed, so a site cannot exist that answers for a heading it does not
/// emit — and there is no road here that hands the authority a relation
/// beside a column list to zip it against.
pub(crate) struct BindingRow<'a> {
    map: &'a SqlBindingMap,
    relation: SemanticRelation,
    /// The interface positions still to be placed. Emptied by
    /// [`BindingRow::publishes`]; a leftover is a refusal at close.
    owed: Vec<PortId>,
    columns: Vec<SqlSlotId>,
    ports: HashMap<PortId, usize>,
    support: HashMap<PortId, usize>,
    physical: HashMap<ColId, usize>,
}

impl BindingRow<'_> {
    /// THIS SLOT PUBLISHES THAT PORT.
    pub(crate) fn publishes(&mut self, slot: ColId, port: PortId) -> Result<()> {
        let Some(at) = self.owed.iter().position(|owed| *owed == port) else {
            return Err(unbound(
                "an emitted position claims a port the relation it realizes does not publish",
            ));
        };
        self.owed.remove(at);
        let position = self.place(slot);
        if self.ports.insert(port, position).is_some() {
            return Err(unbound("one semantic port is published at two positions"));
        }
        Ok(())
    }

    /// THIS SLOT REALIZES A DEPENDENCY the operation still owes — a
    /// predicate's operand, a correlation carrier. It is not a position of
    /// what the relation publishes.
    pub(crate) fn supports(&mut self, slot: ColId, port: PortId) -> Result<()> {
        let position = self.place(slot);
        if self.support.insert(port, position).is_some() {
            return Err(unbound("one dependency is realized at two positions"));
        }
        Ok(())
    }

    /// A POSITION THAT REALIZES NO OCCURRENCE: the compiler's own value in
    /// the row. It still has a slot, because width, ordering, wrapping and
    /// dialect rewrites all act on it.
    pub(crate) fn scaffolds(&mut self, slot: ColId) {
        self.place(slot);
    }

    /// TRANSLATE what construction recorded, then close.
    ///
    /// A reference answered against a port an earlier rebuild replaced
    /// reaches the position that replacement landed at. The evidence is the
    /// authority's own replacement record; nothing here searches lineage.
    pub(crate) fn close(mut self, sealed: &crate::relation::Relations) -> Result<SqlSiteId> {
        if !self.owed.is_empty() {
            return Err(unbound(
                "a SQL site was closed without emitting every position of the relation it realizes",
            ));
        }
        // A SITE ANSWERS FOR ONE COLUMN OR FOR NONE. An ancestor port carried
        // into two of this site's positions — `(q.*, q.*)` — names neither of
        // them here: there is no column a reference to it could mean, and
        // choosing the lower position would make emission order an addressing
        // law. It stays unbound, so asking refuses instead of guessing.
        let direct_ports = self.ports.keys().copied().collect();
        let mut reached: HashMap<PortId, std::collections::BTreeSet<usize>> = HashMap::new();
        for (old, new) in sealed.translated_ports(&self.relation)? {
            let position = self.ports.get(&new).copied().ok_or_else(|| {
                unbound("a construction-owned translation targets a port absent from its site")
            })?;
            reached.entry(old).or_default().insert(position);
        }
        for (old, at) in reached {
            let mut only = at.into_iter();
            let (Some(position), None) = (only.next(), only.next()) else {
                continue;
            };
            match self.ports.insert(old, position) {
                Some(prior) if prior != position => {
                    return Err(unbound(
                        "one semantic port translates to two physical positions",
                    ))
                }
                _ => {}
            }
        }
        for (port, position) in &self.ports {
            self.physical.entry(port.column()).or_insert(*position);
        }
        let layout = SiteLayout {
            relation: Some(self.relation.relation()),
            columns: self.columns,
            ports: self.ports,
            direct_ports,
            support: self.support,
            physical: self.physical,
        };
        let mut sites = self.map.sites.borrow_mut();
        sites.push(layout);
        Ok(SqlSiteId(self.map.epoch, sites.len() - 1))
    }

    /// Append one emitted position and answer where it stands.
    fn place(&mut self, slot: ColId) -> usize {
        let position = self.columns.len();
        self.columns.push(SqlSlotId(slot));
        self.physical.insert(slot, position);
        position
    }
}

/// One refusal for every way a branch can fail to be the arm the evidence
/// names: another relation, another width, another order, or one position
/// claimed twice. They are one fact — this statement is not that operand
/// being emitted — and splitting them would invite a caller to handle the
/// near miss.
const WRONG_ARM: &str = "a set branch is not the arm this step's evidence names being emitted";

fn resolve(site: &HashMap<PortId, SqlSlotId>, cell: &Contribution) -> Result<SqlOutput> {
    match cell {
        Contribution::Port(port) => site
            .get(port)
            .copied()
            .map(SqlOutput::Slot)
            .ok_or_else(|| unbound("a set arm contributes a port its branch does not emit")),
        Contribution::Padding(pad) => Ok(SqlOutput::Pad(*pad)),
    }
}

fn unbound(what: &str) -> DelightQLError {
    DelightQLError::transformation_error(
        format!("{what}: SQL lowering binds recorded ports to emitted columns and has no road to recover one it was not given"),
        "sql binding",
    )
}

#[cfg(test)]
mod tests;
