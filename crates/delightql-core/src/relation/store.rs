// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What each relation publishes, recorded where only the authority can
//! reach it.
//!
//! The interface is NOT re-read from the registry's heading state. A
//! heading is mutable for as long as any column-minting road survives, so
//! a carrier that answered by re-reading one would answer differently
//! after an unrelated caller published a position — which is to say its
//! interface would not be a property of the relation at all.
//!
//! The store therefore holds the ordered interface the authority derived,
//! write-once, and answers from that record. A relation's interface is
//! decided at the moment the relation exists and never again.
//!
//! The store also OWNS the epoch and the seal, because both are properties
//! of one compilation rather than of the transient builder that reaches
//! it: two authorities over one registry must agree on both, and a seal
//! that only closed one builder would close nothing.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::carrier::{BuilderMark, SemanticRelation};
use super::form::{DefinitionId, DefinitionKind, StorageId};
use super::minus::ExactHeadingMap;
use super::port::{Interface, PortId, RelationId, ValueId};
use super::set::ContributionMatrix;
use crate::error::{DelightQLError, Result};

/// A carried position's closed occurrence effect, as its birth states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Occurrence {
    /// The output continues the exact occurrence this source position
    /// stands for.
    Continues(PortId),
    /// The output is an occurrence of its own.
    Own,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PlanRole {
    Scratch,
    HigherOrder,
}

/// Minted, not derived from an address.
///
/// A pointer answers "where does this registry sit right now", which a move
/// changes and a later allocation can reuse. An epoch answers "which
/// compilation is this", which is the question the carrier's origin check
/// actually asks.
static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1);

/// One compilation's relation records.
///
/// Lives on the [`crate::names::Registry`] because that object IS one
/// compilation, and is reachable only from `crate::relation`: the registry
/// allocates it and has no accessor of its own for anything inside it.
pub struct RelationStore {
    inner: RefCell<Inner>,
}

struct Inner {
    epoch: BuilderMark,
    /// Every semantic occurrence and its ordered interface. The index is
    /// the distinct relation identity; no naming scope can be converted to
    /// it.
    records: Vec<Interface>,
    /// The evidence behind every set result: what each arm contributes to
    /// each of the result's OWN ports.
    ///
    /// Kept rather than checked and dropped. A table computed to justify a
    /// heading and then discarded proves the heading was lawful once; a
    /// table a consumer can still read proves which arm each position draws
    /// from, which is the question lowering a union actually asks.
    sets: HashMap<RelationId, ContributionMatrix>,
    /// The exact heading map behind every minus result.
    ///
    /// Kept for the same reason the set table is: the pairing decides both
    /// what the result publishes AND what the anti-match predicate compares,
    /// and two roads computing it separately are two authorities that can
    /// disagree.
    anti: HashMap<RelationId, ExactHeadingMap>,
    /// Where a relation that REPLACED another sent each of its positions.
    ///
    /// Keyed by the exact pair, so a reader asks about the two relations it
    /// holds and gets the map or nothing. There is no walk from one to the
    /// other and no "closest" answer.
    replacements: HashMap<(RelationId, RelationId), super::builder::TotalPortMap>,
    /// Construction-owned port translations into a relation's output.
    /// Every pair was minted by the same operation that minted the output;
    /// it is not reconstructed from value identity or names.
    translations: HashMap<RelationId, Vec<(PortId, PortId)>>,
    /// Immediate construction ancestry of every carried output port.
    lineage: HashMap<PortId, Vec<PortId>>,
    /// Exact port reuse recorded by resolution: an output port a binding
    /// minted, paired with the live BARE port whose value it reuses. The
    /// edge is written by the act that minted the output, while the
    /// complete live bare interface was in hand; the join that owns the
    /// left port consumes it. Nothing recovers it later from spellings,
    /// ordinals, ancestry, or source kinds.
    reuses: Vec<(PortId, PortId)>,
    /// The closed residual row token carried by a hygienic position. The
    /// value is the exact token port minted at construction. Ordinary carry
    /// acts propagate it, so joins can correlate two spends of that same
    /// residual without recovering identity from names or scalar values.
    residual_row_tokens: HashMap<PortId, PortId>,
    /// The exact configured-value port each carried support position
    /// realizes. Equal values captured by different closures stay distinct.
    residual_capture_values: HashMap<PortId, PortId>,
    /// The scalar value class carried by each semantic position.  This map
    /// is intentionally one-way: no value-to-port index exists.
    values: HashMap<PortId, ValueId>,
    /// THE OCCURRENCE EDGE: the exact ORIGIN each carried position stands
    /// for, assigned once at the position's birth by the carry act that
    /// minted it — its source's origin when it CONTINUES the source, itself
    /// when it is an occurrence of its own. A position minted fresh (never
    /// carried) has no entry and is its own origin. A direct record, never
    /// a walk and never revised: a value class shared by republications
    /// cannot choose a position, and this edge never chooses among two.
    continues: HashMap<PortId, PortId>,
    next_value: u32,
    /// The exact semantic occurrence that first published each port.
    port_relations: HashMap<PortId, RelationId>,
    /// Exact semantic operands of each constructed result.
    inputs: HashMap<RelationId, Vec<SemanticRelation>>,
    /// Exact non-output ports an operation must still read while lowering.
    dependencies: HashMap<RelationId, Vec<PortId>>,
    /// Physical storage read by a semantic occurrence, when it has one.
    storages: HashMap<RelationId, StorageId>,
    definitions: HashMap<RelationId, DefinitionId>,
    instances: HashMap<RelationId, DefinitionId>,
    /// The exact reusable-definition operation that created each use.
    /// Lowering reads this record instead of recovering a definition kind
    /// from the naming scope or its parent.
    instance_kinds: HashMap<RelationId, DefinitionKind>,
    /// The catalog entity this exact occurrence reads, when it has one.
    /// Construction propagates this through unary forms; no scope-origin
    /// walk is permitted to recover it later.
    entities: HashMap<RelationId, crate::names::EntityId>,
    /// The exact catalog-source occurrence from which an entity-backed
    /// relation descends. SQL binds that occurrence's ports to the table's
    /// physical columns; it never walks naming scopes to recover the base.
    /// The READ each relation stands on: the source, anonymous body,
    /// definition instance or scratch its construction descends to. A
    /// derived relation carries its operand's, so the FROM entry a
    /// pattern emits is the read the authority recorded rather than one
    /// recovered from a scope kind.
    read_sources: HashMap<RelationId, SemanticRelation>,
    mutation_marks: HashMap<RelationId, Vec<(crate::names::ScopeId, crate::names::Spelling)>>,
    row_bounded: std::collections::HashSet<RelationId>,
    materialized_once: std::collections::HashSet<RelationId>,
    plan_roles: HashMap<RelationId, PlanRole>,
    /// One storage identity per catalog entity in this compilation.
    catalog_storages: HashMap<crate::names::registry::CatalogStorageKey, StorageId>,
    definition_storages: HashMap<RelationId, StorageId>,
    next_definition: u32,
    next_storage: u32,
    /// The exact interior relation atomically attached to an owning port.
    interiors: HashMap<PortId, SemanticRelation>,
    /// An output position merged contributions with incompatible interior
    /// interfaces. The conflict belongs to the semantic port, not to copied
    /// value facts or a naming scope.
    interior_conflicts: std::collections::HashSet<PortId>,
    /// The semantic relation scope each port reports for structural metadata.
    owners: HashMap<PortId, crate::names::ScopeId>,
    /// How many structural landings this compilation has reserved. A
    /// landing is a name; the carrier it names lives in the record of the
    /// act that bound it, never here.
    structural_landings: u32,
    sealed: bool,
}

impl RelationStore {
    pub(crate) fn new() -> Self {
        RelationStore {
            inner: RefCell::new(Inner {
                epoch: BuilderMark(NEXT_EPOCH.fetch_add(1, Ordering::Relaxed)),
                records: Vec::new(),
                sets: HashMap::new(),
                anti: HashMap::new(),
                replacements: HashMap::new(),
                translations: HashMap::new(),
                lineage: HashMap::new(),
                reuses: Vec::new(),
                residual_row_tokens: HashMap::new(),
                residual_capture_values: HashMap::new(),
                values: HashMap::new(),
                continues: HashMap::new(),
                next_value: 0,
                port_relations: HashMap::new(),
                inputs: HashMap::new(),
                dependencies: HashMap::new(),
                storages: HashMap::new(),
                definitions: HashMap::new(),
                instances: HashMap::new(),
                instance_kinds: HashMap::new(),
                entities: HashMap::new(),
                read_sources: HashMap::new(),
                mutation_marks: HashMap::new(),
                row_bounded: std::collections::HashSet::new(),
                materialized_once: std::collections::HashSet::new(),
                plan_roles: HashMap::new(),
                catalog_storages: HashMap::new(),
                definition_storages: HashMap::new(),
                next_definition: 0,
                next_storage: 0,
                interiors: HashMap::new(),
                interior_conflicts: std::collections::HashSet::new(),
                owners: HashMap::new(),
                structural_landings: 0,
                sealed: false,
            }),
        }
    }

    pub(super) fn reserve_structural(
        &self,
        part: super::form::HoPart,
    ) -> super::carrier::StructuralRelation {
        let mut inner = self.inner.borrow_mut();
        let id = inner.structural_landings;
        inner.structural_landings += 1;
        super::carrier::StructuralRelation {
            id,
            mark: inner.epoch,
            part,
        }
    }

    pub(super) fn epoch(&self) -> BuilderMark {
        self.inner.borrow().epoch
    }

    /// Record a relation and the exact ordered interface it publishes, and
    /// hand back the carrier for the pair.
    ///
    /// THE ONE PRODUCER of a [`SemanticRelation`]. The store allocates the
    /// relation identity and records its interface in the same act.
    pub(super) fn fix(
        &self,
        scope: crate::names::ScopeId,
        interface: Interface,
    ) -> Result<SemanticRelation> {
        let mut inner = self.inner.borrow_mut();
        if inner.sealed {
            return Err(sealed_error());
        }
        let relation = RelationId(inner.records.len() as u32);
        for port in interface.ports() {
            if !inner.values.contains_key(port) {
                let value = ValueId(inner.next_value);
                inner.next_value += 1;
                inner.values.insert(*port, value);
            }
            inner.port_relations.insert(*port, relation);
        }
        inner.records.push(interface);
        Ok(SemanticRelation::pair(relation, scope, inner.epoch))
    }

    /// The interface a relation publishes, where the authority fixed one.
    ///
    /// `None` says the relation is still on the occurrence road, which is
    /// the caller's cue to read the registry heading and the only reason
    /// that road is still reachable.
    pub(super) fn interface(&self, relation: RelationId) -> Option<Interface> {
        let inner = self.inner.borrow();
        inner.records.get(relation.0 as usize).cloned()
    }

    pub(super) fn relation_of(&self, port: PortId) -> Option<RelationId> {
        self.inner.borrow().port_relations.get(&port).copied()
    }

    pub(super) fn record_carried_value(&self, output: PortId, source: PortId) {
        let value = self
            .value(source)
            .expect("every carried semantic port has a construction-recorded value");
        self.inner.borrow_mut().values.insert(output, value);
    }

    pub(super) fn record_new_value(&self, output: PortId) {
        let mut inner = self.inner.borrow_mut();
        let value = ValueId(inner.next_value);
        inner.next_value += 1;
        inner.values.insert(output, value);
    }

    pub(super) fn value(&self, port: PortId) -> Option<ValueId> {
        self.inner.borrow().values.get(&port).copied()
    }

    /// Record the contribution table a set result was built from.
    ///
    /// Written in the same act as the interface, from the same ports: the
    /// matrix's rows ARE this relation's positions.
    pub(super) fn record_set(&self, relation: RelationId, matrix: ContributionMatrix) {
        self.inner.borrow_mut().sets.insert(relation, matrix);
    }

    /// What every arm contributes to every position of a set result.
    ///
    /// `None` says this relation is not a set, which is a different answer
    /// from a set with no contributions — there is no such set.
    pub(super) fn contributions(&self, relation: RelationId) -> Option<ContributionMatrix> {
        self.inner.borrow().sets.get(&relation).cloned()
    }

    /// Record that one relation replaced another, keeping every position.
    pub(super) fn record_replacement(
        &self,
        old: RelationId,
        new: RelationId,
        map: super::builder::TotalPortMap,
    ) {
        self.inner.borrow_mut().replacements.insert((old, new), map);
    }

    pub(super) fn record_interior(&self, owner: PortId, interior: SemanticRelation) {
        self.inner.borrow_mut().interiors.insert(owner, interior);
    }

    pub(super) fn interior(&self, owner: PortId) -> Option<SemanticRelation> {
        self.inner.borrow().interiors.get(&owner).copied()
    }

    pub(super) fn record_interior_conflict(&self, owner: PortId) {
        self.inner.borrow_mut().interior_conflicts.insert(owner);
    }

    pub(super) fn interior_conflict(&self, owner: PortId) -> bool {
        self.inner.borrow().interior_conflicts.contains(&owner)
    }

    /// Where `old`'s positions went when `new` replaced it, if a refinement
    /// said so. `None` is "nobody recorded that these two are one operand".
    pub(super) fn replacement(
        &self,
        old: RelationId,
        new: RelationId,
    ) -> Option<super::builder::TotalPortMap> {
        self.inner.borrow().replacements.get(&(old, new)).cloned()
    }

    pub(super) fn replacements_into(&self, new: RelationId) -> Vec<super::builder::TotalPortMap> {
        self.inner
            .borrow()
            .replacements
            .iter()
            .filter(|((_, candidate), _)| *candidate == new)
            .map(|(_, map)| map.clone())
            .collect()
    }

    pub(super) fn record_translations(
        &self,
        relation: RelationId,
        pairs: impl IntoIterator<Item = (PortId, PortId)>,
    ) {
        self.inner
            .borrow_mut()
            .translations
            .entry(relation)
            .or_default()
            .extend(pairs);
    }

    /// Record that resolution decided `output` REUSES the live bare port
    /// `left`. Construction-side: refuses once the store is sealed, like
    /// every other record.
    pub(super) fn record_reuse(&self, output: PortId, left: PortId) -> Result<()> {
        self.check_open()?;
        self.inner.borrow_mut().reuses.push((output, left));
        Ok(())
    }

    /// The whole reuse ledger, for the one consumer that must land each
    /// edge on the exact operand interfaces it holds.
    pub(super) fn reuse_ledger(&self) -> Vec<(PortId, PortId)> {
        self.inner.borrow().reuses.clone()
    }

    pub(super) fn mark_residual_row_token(&self, port: PortId) {
        let mut inner = self.inner.borrow_mut();
        assert!(
            inner.residual_row_tokens.insert(port, port).is_none(),
            "a residual row token is assigned once, at construction"
        );
    }

    pub(super) fn carry_residual_row_token(&self, output: PortId, source: PortId) {
        let token = self
            .inner
            .borrow()
            .residual_row_tokens
            .get(&source)
            .copied();
        if let Some(token) = token {
            let prior = self
                .inner
                .borrow_mut()
                .residual_row_tokens
                .insert(output, token);
            assert!(
                prior.is_none(),
                "a carried residual row token is assigned once"
            );
        }
    }

    pub(super) fn residual_row_token(&self, port: PortId) -> Option<PortId> {
        self.inner.borrow().residual_row_tokens.get(&port).copied()
    }

    pub(super) fn mark_residual_capture_value(&self, port: PortId) {
        let prior = self
            .inner
            .borrow_mut()
            .residual_capture_values
            .insert(port, port);
        assert!(prior.is_none() || prior == Some(port));
    }

    pub(super) fn carry_residual_capture_value(&self, output: PortId, source: PortId) {
        let captured = self
            .inner
            .borrow()
            .residual_capture_values
            .get(&source)
            .copied();
        if let Some(captured) = captured {
            let prior = self
                .inner
                .borrow_mut()
                .residual_capture_values
                .insert(output, captured);
            assert!(prior.is_none() || prior == Some(captured));
        }
    }

    pub(super) fn is_residual_capture_value(&self, port: PortId) -> bool {
        self.residual_capture_value(port).is_some()
    }

    pub(super) fn residual_capture_value(&self, port: PortId) -> Option<PortId> {
        self.inner
            .borrow()
            .residual_capture_values
            .get(&port)
            .copied()
    }

    pub(super) fn record_lineage(&self, output: PortId, source: PortId) {
        self.inner
            .borrow_mut()
            .lineage
            .entry(output)
            .or_default()
            .push(source);
    }

    pub(super) fn lineage(&self, output: PortId) -> Vec<PortId> {
        self.inner
            .borrow()
            .lineage
            .get(&output)
            .cloned()
            .unwrap_or_default()
    }

    /// ASSIGN A CARRIED POSITION'S OCCURRENCE EFFECT — once, at its birth.
    /// The origin is written directly (the source's own origin, or the port
    /// itself for an occurrence of its own); it is never walked and never
    /// revised. Only the one carry act writes here, for the port it just
    /// minted, so a second assignment is a construction fault and refuses
    /// loudly rather than replacing a recorded relationship.
    pub(super) fn record_occurrence(&self, output: PortId, effect: Occurrence) {
        let mut inner = self.inner.borrow_mut();
        let origin = match effect {
            Occurrence::Continues(source) => {
                inner.continues.get(&source).copied().unwrap_or(source)
            }
            Occurrence::Own => output,
        };
        assert!(
            inner.continues.insert(output, origin).is_none(),
            "an occurrence effect is assigned once, at the port's birth"
        );
    }

    /// The exact origin a position continues — itself, when no act
    /// continued it.
    pub(super) fn origin(&self, port: PortId) -> PortId {
        self.inner
            .borrow()
            .continues
            .get(&port)
            .copied()
            .unwrap_or(port)
    }

    pub(super) fn carried_from(&self, destination: PortId) -> Vec<PortId> {
        self.inner
            .borrow()
            .translations
            .values()
            .flatten()
            .filter_map(|(source, at)| (*at == destination).then_some(*source))
            .collect()
    }

    pub(super) fn translations_into(&self, relation: RelationId) -> Vec<(PortId, PortId)> {
        self.inner
            .borrow()
            .translations
            .get(&relation)
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn record_inputs(
        &self,
        relation: RelationId,
        inputs: impl IntoIterator<Item = SemanticRelation>,
    ) {
        self.inner
            .borrow_mut()
            .inputs
            .insert(relation, inputs.into_iter().collect());
    }

    pub(super) fn inputs(&self, relation: RelationId) -> Vec<SemanticRelation> {
        self.inner
            .borrow()
            .inputs
            .get(&relation)
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn record_dependencies(
        &self,
        relation: RelationId,
        dependencies: impl IntoIterator<Item = PortId>,
    ) {
        self.inner
            .borrow_mut()
            .dependencies
            .insert(relation, dependencies.into_iter().collect());
    }

    pub(super) fn dependencies(&self, relation: RelationId) -> Vec<PortId> {
        self.inner
            .borrow()
            .dependencies
            .get(&relation)
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn storage_for_entity(
        &self,
        entity: crate::names::EntityId,
        registry: &crate::names::Registry,
    ) -> StorageId {
        let mut inner = self.inner.borrow_mut();
        let key = registry.catalog_storage_key(entity);
        if let Some(storage) = inner.catalog_storages.get(&key) {
            return *storage;
        }
        let storage = StorageId(inner.next_storage);
        inner.next_storage += 1;
        inner.catalog_storages.insert(key, storage);
        storage
    }

    pub(super) fn record_storage(&self, relation: RelationId, storage: StorageId) {
        self.inner.borrow_mut().storages.insert(relation, storage);
    }

    pub(super) fn storage_for_definition(&self, template: RelationId) -> StorageId {
        let mut inner = self.inner.borrow_mut();
        if let Some(storage) = inner.definition_storages.get(&template) {
            return *storage;
        }
        let storage = StorageId(inner.next_storage);
        inner.next_storage += 1;
        inner.definition_storages.insert(template, storage);
        storage
    }

    pub(super) fn definition_for(&self, template: RelationId) -> DefinitionId {
        let mut inner = self.inner.borrow_mut();
        if let Some(definition) = inner.definitions.get(&template) {
            return *definition;
        }
        let definition = DefinitionId(inner.next_definition);
        inner.next_definition += 1;
        inner.definitions.insert(template, definition);
        definition
    }

    pub(super) fn record_instance(
        &self,
        instance: RelationId,
        definition: DefinitionId,
        kind: DefinitionKind,
    ) {
        let mut inner = self.inner.borrow_mut();
        inner.instances.insert(instance, definition);
        inner.instance_kinds.insert(instance, kind);
    }

    pub(super) fn instance_kind(&self, instance: RelationId) -> Option<DefinitionKind> {
        self.inner.borrow().instance_kinds.get(&instance).copied()
    }

    #[cfg(test)]
    pub(super) fn instance_definition(&self, instance: RelationId) -> Option<DefinitionId> {
        self.inner.borrow().instances.get(&instance).copied()
    }

    pub(super) fn storage(&self, relation: RelationId) -> Option<StorageId> {
        self.inner.borrow().storages.get(&relation).copied()
    }

    pub(super) fn record_entity(&self, relation: RelationId, entity: crate::names::EntityId) {
        self.inner.borrow_mut().entities.insert(relation, entity);
    }

    pub(super) fn record_read_source(&self, relation: RelationId, source: SemanticRelation) {
        self.inner
            .borrow_mut()
            .read_sources
            .insert(relation, source);
    }

    pub(super) fn entity(&self, relation: RelationId) -> Option<crate::names::EntityId> {
        self.inner.borrow().entities.get(&relation).copied()
    }

    pub(super) fn read_source(&self, relation: RelationId) -> Option<SemanticRelation> {
        self.inner.borrow().read_sources.get(&relation).copied()
    }

    pub(super) fn mark_mutation_target(
        &self,
        relation: RelationId,
        scope: crate::names::ScopeId,
        spelling: crate::names::Spelling,
    ) {
        let mut inner = self.inner.borrow_mut();
        let marks = inner.mutation_marks.entry(relation).or_default();
        if !marks.iter().any(|(marked, _)| *marked == scope) {
            marks.push((scope, spelling));
        }
    }

    pub(super) fn record_mutation_marks(
        &self,
        relation: RelationId,
        marks: impl IntoIterator<Item = (crate::names::ScopeId, crate::names::Spelling)>,
    ) {
        let mut inner = self.inner.borrow_mut();
        let output = inner.mutation_marks.entry(relation).or_default();
        for mark in marks {
            if !output.iter().any(|(scope, _)| *scope == mark.0) {
                output.push(mark);
            }
        }
    }

    pub(super) fn mutation_marks(
        &self,
        relation: RelationId,
    ) -> Vec<(crate::names::ScopeId, crate::names::Spelling)> {
        self.inner
            .borrow()
            .mutation_marks
            .get(&relation)
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn mark_row_bounded(&self, relation: RelationId) {
        self.inner.borrow_mut().row_bounded.insert(relation);
    }

    pub(super) fn is_row_bounded(&self, relation: RelationId) -> bool {
        self.inner.borrow().row_bounded.contains(&relation)
    }

    pub(super) fn mark_materialized_once(&self, relation: RelationId) {
        self.inner.borrow_mut().materialized_once.insert(relation);
    }

    pub(super) fn is_materialized_once(&self, relation: RelationId) -> bool {
        self.inner.borrow().materialized_once.contains(&relation)
    }

    pub(super) fn record_plan_role(&self, relation: RelationId, role: PlanRole) {
        self.inner.borrow_mut().plan_roles.insert(relation, role);
    }

    pub(super) fn plan_role(&self, relation: RelationId) -> Option<PlanRole> {
        self.inner.borrow().plan_roles.get(&relation).copied()
    }

    pub(super) fn record_owner(&self, port: PortId, owner: crate::names::ScopeId) {
        self.inner.borrow_mut().owners.insert(port, owner);
    }

    pub(super) fn owner(&self, port: PortId) -> Option<crate::names::ScopeId> {
        self.inner.borrow().owners.get(&port).copied()
    }

    /// Record the exact heading map a minus result was built from.
    pub(super) fn record_anti_match(&self, relation: RelationId, map: ExactHeadingMap) {
        self.inner.borrow_mut().anti.insert(relation, map);
    }

    /// The one pairing behind a minus: which left dimension answers which
    /// right one.
    ///
    /// `None` says this relation is not a minus result.
    pub(super) fn anti_match(&self, relation: RelationId) -> Option<ExactHeadingMap> {
        self.inner.borrow().anti.get(&relation).cloned()
    }

    /// Close the epoch. Nothing constructs after this, in this compilation,
    /// from any builder.
    pub(super) fn seal(&self) {
        self.inner.borrow_mut().sealed = true;
    }

    /// CLOSE CONSTRUCTION FOR THE LENGTH OF ONE ACT, and answer whether it
    /// was open before.
    ///
    /// The effect planner lowers each statement as it discovers the next, so
    /// its lowering runs before the plan as a whole can be sealed. This is
    /// what makes that lowering a CLOSED epoch anyway: the store refuses
    /// construction for exactly as long as the lowering runs, and the
    /// capability that could reopen it has been moved out of reach by the
    /// caller. Only [`Planning::lowering`] pairs the two, and there is no
    /// road from a `&Registry` to either half.
    pub(super) fn close_for_one_act(&self) -> bool {
        let was = self.inner.borrow().sealed;
        self.inner.borrow_mut().sealed = true;
        was
    }

    /// Restore what [`RelationStore::close_for_one_act`] found.
    pub(super) fn reopen_after_one_act(&self, was_sealed: bool) {
        self.inner.borrow_mut().sealed = was_sealed;
    }

    /// WHETHER THIS COMPILATION STILL ADMITS CONSTRUCTION.
    ///
    /// Asked at the ENTRANCE, before anything is minted. A refusal that
    /// arrives at the last step of a derivation has already grown a scope
    /// and its columns, so the compilation carries naming state for a
    /// relation that does not exist — the flag would refuse the act and
    /// keep its residue. This is the defence-in-depth backstop behind the
    /// type; the type is the boundary.
    pub(super) fn check_open(&self) -> Result<()> {
        if self.inner.borrow().sealed {
            return Err(sealed_error());
        }
        Ok(())
    }
}

fn sealed_error() -> DelightQLError {
    DelightQLError::transformation_error(
        "the semantic epoch is sealed: relations are constructed through \
         refinement and bound to physical slots after it, and nothing past \
         the seal mints either half of one",
        "semantic relation",
    )
}

#[cfg(test)]
mod tests {
    use super::super::form::{AnonymousShape, AnonymousSpec};
    use super::super::{Planning, RelForm};
    use crate::names::Registry;

    /// Two published dimensions, through the one entrance.
    fn named_pair(
        registry: &Planning,
        first: &str,
        second: &str,
    ) -> crate::relation::SemanticRelation {
        use super::super::form::{AnonymousShape, AnonymousSlot, AnonymousSpec};
        let slots = [
            AnonymousSlot::Declared {
                position: 0,
                named: Some(registry.intern(first, false)),
            },
            AnonymousSlot::Declared {
                position: 1,
                named: Some(registry.intern(second, false)),
            },
        ];
        registry
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &slots,
                answers_to: None,
            }))
            .expect("an anonymous relation is built")
    }

    /// Mint one more column into a relation's SCOPE, behind the authority's
    /// back — which is exactly what the surviving predecessor roads do.
    fn grow(registry: &Registry, relation: crate::relation::SemanticRelation, name: &str) {
        registry.sql_column(
            relation.scope(),
            Some(registry.intern(name, false)),
            crate::names::Addressing::Published,
        );
    }

    fn anonymous(registry: &Planning) -> crate::relation::SemanticRelation {
        registry
            .authority()
            .derive(RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &[],
                answers_to: None,
            }))
            .expect("an anonymous relation is built")
    }

    /// PORT FANOUT DOES NOT FAN OUT THE VALUE.  Two projection positions are
    /// two addressable occurrences even when both carry the same source;
    /// a computed neighbor is a third position carrying a different value.
    #[test]
    fn repeated_publication_has_distinct_ports_and_one_value() {
        use super::super::form::{Naming, ProjectSlot, ProjectSpec, ProjectWhy};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let source = named_pair(&registry, "a", "b");
        let source_port = registry
            .authority()
            .interface(&source)
            .expect("the source interface")
            .ports()[0];
        let slots = [
            ProjectSlot::Carried {
                source: source_port,
                naming: Naming::Inherited,
            },
            ProjectSlot::Carried {
                source: source_port,
                naming: Naming::Anonymous,
            },
            ProjectSlot::Computed {
                naming: Naming::Anonymous,
                shape: crate::names::ValueShape::Unknown,
            },
        ];
        let result = registry
            .authority()
            .derive(RelForm::Project(ProjectSpec {
                input: source,
                why: ProjectWhy::Stage,
                slots: &slots,
                dependencies: &[],
            }))
            .expect("the projection is built");
        let ports = registry
            .authority()
            .interface(&result)
            .expect("the projection interface");
        assert_ne!(ports.ports()[0], ports.ports()[1]);
        assert_eq!(
            registry.relations().value(ports.ports()[0]),
            registry.relations().value(ports.ports()[1]),
        );
        assert_ne!(
            registry.relations().value(ports.ports()[1]),
            registry.relations().value(ports.ports()[2]),
        );
    }

    /// A cover keeps the addressed position relationship but writes a new
    /// scalar value; exporting that result carries the new value onward.
    #[test]
    fn a_cover_starts_one_value_and_an_export_carries_it() {
        use super::super::form::{CoverCell, CoverKind, CoverSpec, ExportSpec, ExportWhy, Naming};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let source = named_pair(&registry, "a", "b");
        let source_port = registry
            .authority()
            .interface(&source)
            .expect("the source interface")
            .ports()[0];
        let cells = [CoverCell {
            covered: source_port,
            naming: Naming::Inherited,
            writes: true,
        }];
        let covered = registry
            .authority()
            .derive(RelForm::Cover(CoverSpec {
                input: source,
                kind: CoverKind::Map,
                cells: &cells,
            }))
            .expect("the cover is built");
        let covered_port = registry
            .authority()
            .interface(&covered)
            .expect("the cover interface")
            .ports()[0];
        assert_ne!(
            registry.relations().value(source_port),
            registry.relations().value(covered_port),
        );

        let exported = registry
            .authority()
            .derive(RelForm::Export(ExportSpec {
                input: covered,
                why: ExportWhy::EmissionAlias,
            }))
            .expect("the export is built");
        let exported_port = registry
            .authority()
            .interface(&exported)
            .expect("the export interface")
            .ports()[0];
        assert_eq!(
            registry.relations().value(covered_port),
            registry.relations().value(exported_port),
        );
    }

    #[test]
    fn row_bounds_follow_exact_construction_and_not_scope_ancestry() {
        use super::super::form::{ExportSpec, ExportWhy, JoinKind, JoinSpec};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let authority = registry.authority();
        let base = anonymous(&registry);
        let earlier = authority
            .derive(RelForm::Export(ExportSpec {
                input: base,
                why: ExportWhy::Alias {
                    answer: registry.intern("earlier", false),
                },
            }))
            .expect("the earlier occurrence is constructed");
        authority
            .mark_row_bounded(&base)
            .expect("the base belongs to this authority");
        let later = authority
            .derive(RelForm::Export(ExportSpec {
                input: base,
                why: ExportWhy::Alias {
                    answer: registry.intern("later", false),
                },
            }))
            .expect("the later occurrence is constructed");
        let joined = authority
            .derive(RelForm::Join(JoinSpec {
                left: anonymous(&registry),
                right: later,
                kind: JoinKind::Inner,
                merged: &[],
            }))
            .expect("the join is constructed");
        assert!(authority.is_row_bounded(&base).unwrap());
        assert!(!authority.is_row_bounded(&earlier).unwrap());
        assert!(authority.is_row_bounded(&later).unwrap());
        assert!(authority.is_row_bounded(&joined).unwrap());
    }

    #[test]
    fn mutation_marks_follow_exact_inputs_and_remain_per_occurrence() {
        use super::super::form::{ExportSpec, ExportWhy, JoinKind, JoinSpec};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let authority = registry.authority();
        let marked = anonymous(&registry);
        let unmarked = anonymous(&registry);
        let first = registry.intern("first", false);
        let second = registry.intern("second", false);
        authority.mark_mutation_target(&marked, first).unwrap();
        authority.mark_mutation_target(&marked, first).unwrap();
        authority.mark_mutation_target(&unmarked, second).unwrap();
        let carried = authority
            .derive(RelForm::Export(ExportSpec {
                input: marked,
                why: ExportWhy::Alias {
                    answer: registry.intern("carried", false),
                },
            }))
            .expect("the marked occurrence is carried");
        let joined = authority
            .derive(RelForm::Join(JoinSpec {
                left: carried,
                right: unmarked,
                kind: JoinKind::Inner,
                merged: &[],
            }))
            .expect("the two marked inputs are joined");
        assert_eq!(authority.mutation_marks(&marked).unwrap().len(), 1);
        assert_eq!(authority.mutation_marks(&carried).unwrap().len(), 1);
        assert_eq!(authority.mutation_marks(&joined).unwrap().len(), 2);
    }

    #[test]
    fn each_definition_use_has_fresh_relation_and_ports_but_shared_definition_storage() {
        use super::super::form::{DefinitionKind, InstanceSpec};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let authority = registry.authority();
        let template = named_pair(&registry, "left", "right");
        let first = authority
            .derive(RelForm::Instantiate(InstanceSpec {
                kind: DefinitionKind::Cte,
                template,
                answers_to: Some(registry.intern("first", false)),
            }))
            .expect("the first use is constructed");
        let second = authority
            .derive(RelForm::Instantiate(InstanceSpec {
                kind: DefinitionKind::Cte,
                template,
                answers_to: Some(registry.intern("second", false)),
            }))
            .expect("the second use is constructed");

        assert_ne!(first, second);
        let first_ports = authority.interface(&first).unwrap();
        let second_ports = authority.interface(&second).unwrap();
        assert!(first_ports
            .ports()
            .iter()
            .all(|port| !second_ports.ports().contains(port)));
        assert_eq!(
            registry.relations().storage(first.relation()),
            registry.relations().storage(second.relation())
        );
        assert_eq!(
            registry.relations().instance_definition(first.relation()),
            registry.relations().instance_definition(second.relation())
        );
    }

    /// TWO COMPILATIONS ARE TWO EPOCHS even when the allocator hands the
    /// second registry the first one's address.
    #[test]
    fn an_epoch_survives_address_reuse() {
        let first_mark = {
            let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
            registry.authority().epoch_for_test()
        };
        let second = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        assert_ne!(
            first_mark,
            second.authority().epoch_for_test(),
            "a dropped compilation's epoch cannot be reissued to the next one"
        );
    }

    /// The interface answers from the record, not from the heading the
    /// registry still lets a predecessor road grow.
    #[test]
    fn a_fixed_interface_does_not_follow_the_heading() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let relation = anonymous(&registry);
        let before = registry
            .authority()
            .interface(&relation)
            .expect("its own epoch reads it");
        registry.sql_column(relation.scope(), None, crate::names::Addressing::Latent);
        let after = registry
            .authority()
            .interface(&relation)
            .expect("its own epoch reads it");
        assert_eq!(
            before.width(),
            after.width(),
            "a column minted into the scope afterwards is not an output position \
             of a relation whose interface was already derived"
        );
    }

    /// THE SET FAMILY JUDGES THE FIXED INTERFACE, not the scope.
    ///
    /// The witness the store's own stability test could not give: growing a
    /// scope after its relation was derived must not widen what a set built
    /// from it publishes. A construction road reading `Registry::heading`
    /// passes the stability test and fails this one.
    #[test]
    fn a_set_judges_its_operands_fixed_interfaces() {
        use super::super::form::{SetAlignment, SetArm, SetSpec};
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let left = named_pair(&registry, "a", "b");
        let right = named_pair(&registry, "a", "b");
        grow(&registry, left, "c");

        let result = registry
            .authority()
            .derive(RelForm::Set(SetSpec {
                alignment: SetAlignment::Corresponding,
                arms: &[
                    SetArm {
                        relation: left,
                        correlated: false,
                    },
                    SetArm {
                        relation: right,
                        correlated: false,
                    },
                ],
            }))
            .expect("two two-column arms correspond");
        assert_eq!(
            registry
                .authority()
                .interface(&result)
                .expect("its own epoch reads it")
                .width(),
            2,
            "a column minted into an arm's scope after the arm was derived is \
             not one of the arm's dimensions"
        );
    }

    /// The same for minus, whose exactness refuses on width.
    #[test]
    fn a_minus_judges_its_operands_fixed_interfaces() {
        use super::super::form::MinusSpec;
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let left = named_pair(&registry, "a", "b");
        let right = named_pair(&registry, "a", "b");
        grow(&registry, right, "c");

        assert!(
            registry
                .authority()
                .derive(RelForm::Minus(MinusSpec { left, right }))
                .is_ok(),
            "a column minted into the right operand's scope afterwards does not \
             make the two operands disagree in width"
        );
    }

    /// THE RESULT'S PORTS ARE ITS OWN, and the table says so cell by cell.
    ///
    /// The differential the old lowering could not survive: it aliased an
    /// ARM's column into a result slot by matching names, so a slot no arm
    /// answered to was decided by the matcher rather than by the operation.
    /// Here the result's ports belong to no arm, and an arm that reaches a
    /// slot with nothing is `Padding` — a cell, not an absence and not a
    /// borrowed port.
    #[test]
    fn a_set_results_ports_belong_to_no_arm_and_every_gap_is_a_padding() {
        use super::super::form::{SetAlignment, SetArm, SetSpec};
        use super::super::set::Contribution;
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let left = named_pair(&registry, "a", "b");
        let right = named_pair(&registry, "a", "c");
        let result = registry
            .authority()
            .derive(RelForm::Set(SetSpec {
                alignment: SetAlignment::Corresponding,
                arms: &[
                    SetArm {
                        relation: left,
                        correlated: false,
                    },
                    SetArm {
                        relation: right,
                        correlated: false,
                    },
                ],
            }))
            .expect("two arms sharing one name correspond");

        let matrix = registry
            .relations()
            .contributions(result.relation())
            .expect("a set result records its table");
        assert_eq!(matrix.outputs().len(), 3, "a, b and c are three slots");

        let arm_ports: Vec<_> = [left, right]
            .iter()
            .flat_map(|arm| {
                registry
                    .authority()
                    .interface(arm)
                    .expect("its own epoch reads it")
                    .ports()
                    .to_vec()
            })
            .collect();
        for output in matrix.outputs() {
            assert!(
                !arm_ports.contains(&output.result()),
                "a set result that publishes an arm's port has not published \
                 a position of its own"
            );
        }

        let pads = matrix
            .outputs()
            .iter()
            .flat_map(|output| output.by_arm().iter())
            .filter(|cell| matches!(cell, Contribution::Padding(_)))
            .count();
        assert_eq!(
            pads, 2,
            "`b` is absent from the right arm and `c` from the left, and each \
             absence is one padded cell"
        );
        assert!(
            matrix
                .outputs()
                .iter()
                .all(|output| output.by_arm().len() == 2),
            "every slot has one cell per arm, padded or not"
        );
    }

    /// A SEALED COMPILATION REFUSES BEFORE IT MINTS.
    ///
    /// The type is the boundary: `Planning` is not `Clone` and
    /// `Planning::seal` consumes it, so after the transition there is no
    /// capability left to reach the store with — the compile-fail probes
    /// carry that. What THIS pins is the backstop behind the type, and the
    /// property that makes a backstop worth having: the refusal arrives at
    /// the entrance, so nothing is half-built behind it. A guard that fired
    /// at the last step would leave the compilation naming a scope and its
    /// columns for a relation that does not exist.
    #[test]
    fn a_sealed_compilation_refuses_before_it_mints() {
        let names = crate::names::Registry::new(&[]);
        names.relations().seal();
        let before = names.scopes_minted();
        let refused = super::super::builder::SemanticBuilder::new(&names).derive(
            RelForm::Anonymous(AnonymousSpec {
                shape: AnonymousShape::Tabular,
                slots: &[],
                answers_to: None,
            }),
        );
        assert!(refused.is_err(), "a sealed store constructs nothing");
        assert_eq!(
            names.scopes_minted(),
            before,
            "and it left no scope behind for a relation it refused to build"
        );
    }
}
