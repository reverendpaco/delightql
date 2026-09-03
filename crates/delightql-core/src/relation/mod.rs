// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The semantic relation authority.
//!
//! One entrance from "a row-producing operation happened" to "this relation
//! publishes exactly these ordered positions". The pair is constructed
//! atomically and there is no road that supplies either half on its own, so
//! a relation cannot acquire a foreign interface and an interface cannot be
//! attached to a different relation.
//!
//! # What a caller says, and what it cannot say
//!
//! A caller states the OPERATION — this is a witness, this is a drill, this
//! is a corresponding set of these arms. It has no spelling for the owner,
//! the destination scope, the birth, the republication class, the boundary
//! kind, the addressing policy, or the value facts: every one of those is
//! derived here from the operation's output law.
//!
//! # The total judgment
//!
//! Adding a row-producing form extends [`law::law_of`], the wildcard-free
//! judgment that decides what the operation does to outputs. Refinement maps
//! and SQL sites are evidence attached by the operations that actually create
//! them, rather than parallel classification tables serving no consumer.
//!
//! # The phase boundary
//!
//! The builder stays open through refinement, because refinement still
//! reshapes relations, and [`Planning::seal`] closes it before SQL
//! lowering. A [`Relations`] has no constructor and no interior
//! mutability: lowering binds ports to physical slots and has nothing to
//! mint a port with.

mod alignment;
pub mod builder;
pub mod carrier;
#[cfg(test)]
mod fences;
pub mod form;
pub mod law;
pub mod minus;
pub(crate) mod pending;
pub mod port;
pub mod set;
mod store;

pub use builder::{Refinement, Relations, SemanticBuilder, TotalPortMap};
pub use carrier::StructuralRelation;
pub use store::RelationStore;

/// THE OPEN SEMANTIC EPOCH — the capability to construct relations.
///
/// A phase that holds one may build; a phase that does not, cannot, and
/// there is no road from what lowering holds to one of these. The only
/// producer is [`Planning::open`], and it takes the registry BY VALUE: a
/// `Rc<Registry>` — which is what every lowering context is handed — is not
/// an owned registry and has no conversion to this. So the transformer, the
/// SQL rewriters and the generator have nothing to construct with, and a
/// future edit cannot restore construction there by reaching for the
/// registry it already holds.
///
/// The capability ENDS at [`Planning::seal`], which consumes it and closes
/// the store. What comes back is a [`Relations`] — a reader with no
/// constructor, no builder road, and no claim about anything except the
/// records it can answer from.
/// NOT `Clone`, and that is the whole transition. A capability that can be
/// copied cannot be spent: sealing a copy leaves the original open beside
/// the reader it produced, which is the state this type exists to make
/// unrepresentable. Phases that need it BORROW the one value; the phase
/// boundary CONSUMES it.
pub(crate) struct Planning {
    registry: std::rc::Rc<crate::names::Registry>,
}

impl Planning {
    /// Whether `port` CONTINUES the same exact occurrence as `of`: both
    /// stand for one original position, by the continuation edge every
    /// continuing act writes at construction. A republication of the same
    /// value under a second position does not continue it. The reader a
    /// body's formal spend and a higher-order call's dispatch witness
    /// select the occurrence by; no spelling, ordinal, value class, or
    /// ancestry takes part.
    pub(crate) fn continues_occurrence(&self, port: PortId, of: PortId) -> bool {
        let relations = self.registry.relations();
        relations.origin(port) == relations.origin(of)
    }

    /// Open the semantic epoch over a registry this call OWNS.
    ///
    /// Taking the registry by value is the whole fence: a shared handle
    /// cannot be turned back into one, so the capability exists exactly
    /// where a compilation is created and nowhere a later phase can reach.
    pub(crate) fn open(registry: crate::names::Registry) -> Self {
        Planning {
            registry: std::rc::Rc::new(registry),
        }
    }

    /// The authority over this compilation's identities.
    ///
    /// Not a second construction road: the epoch mark is the REGISTRY's, so
    /// every authority over one compilation is the same authority reached
    /// from another phase, and a relation built against a different
    /// registry refuses at the entrance either way.
    pub(crate) fn authority(&self) -> SemanticBuilder<'_> {
        SemanticBuilder::new(&self.registry)
    }

    /// The shared naming handle, for phases that only read names and for
    /// the lowering contexts that hold nothing else.
    pub(crate) fn names(&self) -> std::rc::Rc<crate::names::Registry> {
        std::rc::Rc::clone(&self.registry)
    }

    /// The same handle, borrowed.
    pub(crate) fn shared(&self) -> &std::rc::Rc<crate::names::Registry> {
        &self.registry
    }

    /// End construction for this compilation and hand lowering its reader.
    ///
    /// Consumes the capability and closes the store, so a handle that
    /// outlived this call refuses at the entrance rather than minting into
    /// a compilation that has already been lowered.
    pub(crate) fn seal(self) -> Relations {
        self.registry.relations().seal();
        let mark = self.registry.relations().epoch();
        Relations {
            registry: self.registry,
            mark,
        }
    }

    /// LOWER ONE STATEMENT WITH THIS CAPABILITY SPENT.
    ///
    /// The effect planner discovers and lowers statement by statement, so
    /// its lowering runs before the plan as a whole can be sealed. This is
    /// the only road there, and it CLOSES the epoch for the length of the
    /// act: the capability is MOVED IN, so nothing that could extend the
    /// epoch is reachable while the lowering runs, and the store refuses
    /// construction for exactly that window. What the lowering holds is a
    /// [`Relations`] over a store that is closed while it holds it.
    ///
    /// The capability comes back only after the lowering has answered, so
    /// there is no arrangement of these lines in which a lowering and a
    /// live constructor exist at once.
    pub(crate) fn lowering<T>(self, lower: impl FnOnce(&Relations) -> T) -> (Self, T) {
        let was_sealed = self.registry.relations().close_for_one_act();
        let reader = Relations {
            registry: std::rc::Rc::clone(&self.registry),
            mark: self.registry.relations().epoch(),
        };
        let answer = lower(&reader);
        self.registry.relations().reopen_after_one_act(was_sealed);
        (self, answer)
    }
}

impl std::ops::Deref for Planning {
    type Target = crate::names::Registry;

    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

/// The one pairing behind a minus: which left dimension answers which right
/// one.
///
/// A READER, and the reason the map is kept rather than checked and dropped.
/// The pairing decides what the result publishes and what the anti-match
/// predicate compares; asking here is what stops those two from being
/// computed twice and disagreeing.
pub(crate) fn anti_match(
    registry: &crate::names::Registry,
    result: &SemanticRelation,
) -> crate::error::Result<Option<ExactHeadingMap>> {
    builder::check_epoch(registry, result)?;
    Ok(registry.relations().anti_match(result.relation()))
}

/// The contribution table a set result was built from, read against this
/// compilation's epoch.
///
/// The evidence the PHYSICAL binding elaborates. It says what every arm
/// puts through every result position and, in each arm record, the ordered
/// interface that arm published — the two facts a branch's output list has
/// to be paired against. `Ok(None)` says the relation is not a set, which
/// is a different answer from a set with no contributions: there is no
/// such set.
pub(crate) fn contributions(
    registry: &crate::names::Registry,
    result: &SemanticRelation,
) -> crate::error::Result<Option<ContributionMatrix>> {
    builder::check_epoch(registry, result)?;
    Ok(registry.relations().contributions(result.relation()))
}

/// The exact correspondence resolution RECORDED for a join's two operands.
///
/// Right-operand resolution wrote, at each binding it minted, the
/// exactly-one live bare port that spelling reuses. This reader lands each
/// recorded edge on the two exact interfaces the join holds: the left port
/// must be one the left operand PUBLISHES (a port carried onward belongs to
/// the join that already merged it), and the minted output must land in the
/// right operand's interface through the construction record. No spelling,
/// ordinal, ancestry, or source kind takes part.
pub(crate) fn recorded_correspondence(
    registry: &crate::names::Registry,
    left: &SemanticRelation,
    right: &SemanticRelation,
) -> crate::error::Result<Vec<form::MergedKey>> {
    builder::check_epoch(registry, left)?;
    builder::check_epoch(registry, right)?;
    let left_ports = published_ports(registry, left)?;
    let right_ports = published_ports(registry, right)?;
    let mut pairs: Vec<form::MergedKey> = Vec::new();
    crate::probe::probe!(
        using,
        "reuse ledger={:?} left={left_ports:?} right={right_ports:?}",
        registry.relations().reuse_ledger()
    );
    for (output, reused) in registry.relations().reuse_ledger() {
        if !left_ports.contains(&reused) {
            continue;
        }
        let Some(landed) = landed_in(registry, &right_ports, output)? else {
            continue;
        };
        if !pairs
            .iter()
            .any(|pair| pair.left == reused && pair.right == landed)
        {
            pairs.push(form::MergedKey {
                left: reused,
                right: landed,
            });
        }
    }
    Ok(pairs)
}

/// The catalog entity attached during exact semantic construction.
pub(crate) fn entity(
    registry: &crate::names::Registry,
    relation: &SemanticRelation,
) -> crate::error::Result<Option<crate::names::EntityId>> {
    builder::check_epoch(registry, relation)?;
    Ok(registry.relations().entity(relation.relation()))
}

/// This compilation's semantic epoch, for the physical binding authority to
/// tie itself and its handles to.
///
/// The store is asked rather than a second counter, so a binding and the
/// semantic evidence it elaborates carry ONE epoch by construction rather
/// than two that agree.
pub(crate) fn epoch_of(store: &RelationStore) -> carrier::BuilderMark {
    store.epoch()
}

/// Where a relation's positions went when another REPLACED it.
///
/// Exact keyed lookup on the pair the caller holds. `Ok(None)` says no
/// refinement reported that these two are one operand — which is a refusal
/// for whoever needed them to be, never an invitation to look for a
/// relation nearby.
pub(crate) fn replacement(
    registry: &crate::names::Registry,
    old: RelationId,
    new: &SemanticRelation,
) -> crate::error::Result<Option<TotalPortMap>> {
    // The OLD side arrives as a bare occurrence because it comes from
    // evidence this compilation already answered for; the NEW side is a
    // carrier and owes the epoch check every reader owes.
    builder::check_epoch(registry, new)?;
    Ok(registry.relations().replacement(old, new.relation()))
}

/// The ports a relation publishes, from the authority's RECORD.
///
/// A READER, and the distinction the registry heading cannot make: a
/// predecessor road may grow a scope after the authority derived its
/// interface, so a heading of three columns and an interface of none are
/// both true of the same relation. What a set may stand at is the record.
pub(crate) fn published_ports(
    registry: &crate::names::Registry,
    relation: &SemanticRelation,
) -> crate::error::Result<Vec<PortId>> {
    Ok(SemanticBuilder::new(registry)
        .interface(relation)?
        .ports()
        .to_vec())
}

/// Whether one carried position is compiler support rather than part of the
/// language-visible heading. Row-token provenance is decisive even across a
/// publication boundary that legitimately changes the column's address role.
pub(crate) fn is_higher_order_support(registry: &crate::names::Registry, port: PortId) -> bool {
    registry.addressing(port.column()) == crate::names::Addressing::Hygienic
        || registry.relations().residual_row_token(port).is_some()
        || registry.relations().is_residual_capture_value(port)
}

/// The owner selected when this exact semantic port was constructed.
///
/// Port ownership is semantic evidence.  Reading it here keeps callers from
/// recovering an owner from the registry column that happens to back the
/// port, which is only a physical identity used by later naming and SQL.
pub(crate) fn owner(
    registry: &crate::names::Registry,
    port: PortId,
) -> crate::error::Result<crate::names::ScopeId> {
    SemanticBuilder::new(registry).owner(port)
}

/// Whether any relation in the lexical environment has an interface which
/// cannot be enumerated without executing it.
pub(crate) fn any_interface_opaque(
    registry: &crate::names::Registry,
    relations: &[SemanticRelation],
) -> crate::error::Result<bool> {
    let authority = SemanticBuilder::new(registry);
    for relation in relations {
        if authority.interface(relation)?.is_opaque() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether an exact semantic result is built from a named scope.
///
/// This follows construction-recorded operands only. A scope's naming
/// origin is neither ownership nor semantic ancestry and is never searched.
pub(crate) fn contains_scope(
    registry: &crate::names::Registry,
    relation: &SemanticRelation,
    candidate: crate::names::ScopeId,
) -> crate::error::Result<bool> {
    builder::check_epoch(registry, relation)?;
    fn contains(
        registry: &crate::names::Registry,
        relation: SemanticRelation,
        candidate: crate::names::ScopeId,
        active: &mut Vec<RelationId>,
    ) -> bool {
        if relation.scope() == candidate {
            return true;
        }
        if active.contains(&relation.relation()) {
            return false;
        }
        active.push(relation.relation());
        let found = registry
            .relations()
            .inputs(relation.relation())
            .into_iter()
            .any(|input| contains(registry, input, candidate, active));
        active.pop();
        found
    }
    Ok(contains(registry, *relation, candidate, &mut Vec::new()))
}

/// Whether one exact port stands where another one did.
///
/// A qualified reference reaches the lexical binding as well as the heading
/// standing over it, and a step that republished that binding's position
/// offers BOTH — one column, at two levels. Which of the two a reference
/// means is a construction-recorded fact: the carry that made the later
/// position wrote down what it carried. Sibling positions carrying one
/// source are NOT related this way, which is what keeps `q.*, q.*` two
/// answers and therefore a refusal.
pub(crate) fn stands_where(
    registry: &crate::names::Registry,
    output: PortId,
    ancestor: PortId,
) -> bool {
    let store = registry.relations();
    let mut frontier = vec![output];
    let mut seen = vec![output];
    while let Some(port) = frontier.pop() {
        for source in store.lineage(port) {
            if source == ancestor {
                return true;
            }
            if !seen.contains(&source) {
                seen.push(source);
                frontier.push(source);
            }
        }
    }
    false
}

/// Which position of a heading stands where an already-resolved one did.
///
/// A SELECTOR ADDRESSES THIS HEADING. `u.id` resolves against the lexical
/// binding, which is the alias's own position; the step standing over it
/// carried that position into one of its own, and it is the carried one a
/// removal removes and a rename renames. `Ok(None)` says the heading does
/// not stand on it at all — a refusal for whoever needed it to. Two
/// positions standing where one did is a heading that publishes the same
/// occurrence twice; the caller cannot be told which, so it is refused
/// rather than guessed.
pub(crate) fn landed_in(
    registry: &crate::names::Registry,
    available: &[PortId],
    resolved: PortId,
) -> crate::error::Result<Option<PortId>> {
    if available.contains(&resolved) {
        return Ok(Some(resolved));
    }
    let landed: Vec<PortId> = available
        .iter()
        .copied()
        .filter(|port| stands_where(registry, *port, resolved))
        .collect();
    match landed.as_slice() {
        [] => Ok(None),
        [port] => Ok(Some(*port)),
        _ => Err(crate::error::DelightQLError::transformation_error(
            "an addressed position stands at more than one position of this heading",
            "semantic relation",
        )),
    }
}

/// The interior relation atomically attached to an owning semantic port.
pub(crate) fn interior(
    registry: &crate::names::Registry,
    owner: PortId,
) -> crate::error::Result<Option<SemanticRelation>> {
    let relation = registry.relations().interior(owner);
    if let Some(relation) = relation {
        builder::check_epoch(registry, &relation)?;
    }
    Ok(relation)
}

/// Whether construction found incompatible interior interfaces at this
/// exact semantic output position.
pub(crate) fn interior_conflict(registry: &crate::names::Registry, owner: PortId) -> bool {
    registry.relations().interior_conflict(owner)
}

/// One compilation opened and immediately closed, for a LOWERING test that
/// needs a reader and no capability.
///
/// The lowering phases have no spelling for the capability, so a test of
/// one cannot open an epoch either; this hands back exactly what the
/// pipeline hands lowering.
#[cfg(test)]
pub(crate) fn sealed_empty() -> (std::rc::Rc<crate::names::Registry>, Relations) {
    let planning = Planning::open(crate::names::Registry::new(&[]));
    let names = planning.names();
    (names, planning.seal())
}

/// One anonymous relation, for a test that needs a row-producing result
/// and does not care which.
///
/// Goes through the same one entrance production does: a test cannot forge
/// a carrier either, which is what makes the fences mean anything.
#[cfg(test)]
pub(crate) fn any_relation(registry: &crate::names::Registry) -> SemanticRelation {
    SemanticBuilder::new(registry)
        .derive(RelForm::Anonymous(form::AnonymousSpec {
            shape: form::AnonymousShape::Tabular,
            slots: &[],
            answers_to: None,
        }))
        .expect("an anonymous relation takes no input to refuse")
}

#[cfg(test)]
pub(crate) fn named_port(registry: &crate::names::Registry, name: &str) -> PortId {
    let named = registry.intern(name, false);
    let relation = SemanticBuilder::new(registry)
        .derive(RelForm::Anonymous(form::AnonymousSpec {
            shape: form::AnonymousShape::Tabular,
            slots: &[form::AnonymousSlot::Binder {
                position: 0,
                named,
                declared_type: None,
                shape: crate::names::ValueShape::Unknown,
            }],
            answers_to: None,
        }))
        .expect("a named test position is a complete anonymous interface");
    published_ports(registry, &relation).expect("the derived interface")[0]
}

/// One plan-lifetime scratch relation, for a test that needs a
/// compiler-owned storage origin.
#[cfg(test)]
pub(crate) fn any_scratch(registry: &crate::names::Registry) -> ScratchRow {
    SemanticBuilder::new(registry)
        .scratch_row(form::ScratchSpec::stating(
            form::ScratchWhy::Snapshot,
            None,
            &[],
        ))
        .expect("a scratch takes no input to refuse")
}

pub use carrier::{BuilderMark, CarrierRow, NamedScratch, ScratchRow, SemanticRelation};
pub use form::RelForm;
pub use minus::ExactHeadingMap;
pub use port::{PaddingId, PortId, RelationId};
pub use set::{Contribution, ContributionMatrix, SetArmRecord};
