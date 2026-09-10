// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The one semantic construction authority.
//!
//! [`SemanticBuilder::derive`] is the only producer of a
//! [`SemanticRelation`] in the process. It receives the OPERATION, judges
//! its output law, derives the birth and the boundary the law implies, and
//! returns the relation and its interface as one value. There is no
//! entrance that accepts a birth, a boundary kind, an owner, a destination,
//! an addressing policy, or a pre-built interface.
//!
//! The builder stays open through refinement, because refinement still
//! reshapes relations, and SEALS before SQL lowering. A sealed store has no
//! constructor and no interior mutability, so lowering can bind ports to
//! physical slots and cannot mint a port to bind.

use super::carrier::{BuilderMark, SemanticRelation};
use super::form::*;
use super::law::{law_of, FixedShape, HeadingEdit, InterfaceLaw};
use super::minus::{ExactHeadingMap, ExactPair};
use super::port::{Interface, PortId, RelationId};
use super::set::{Contribution, ContributionMatrix, SetMode, SetOutput, Vec2};
use super::support::SupportLaw;
use crate::diagnostic::{Constraint, Internal, Resolution, ResolutionSetop, SetOperation};
use crate::error::{DelightQLError, Result};
use crate::names::{
    Addressing, ColId, CteLabel, CteRole, HoRole, Registry, ScopeId, ScratchRole, ValueFacts,
    WrapReason,
};

pub(crate) struct SemanticConstruction(());

/// A HEAD THAT READS A RELATION THIS COMPILATION ALREADY BUILT.
///
/// One variant per ground form that carries no body of its own. The variant
/// fixes the AST payload, so the relation an arm names cannot end up under
/// a head built for a different kind of read.
pub(crate) enum ReadHead<P: crate::pipeline::asts::core::Phase> {
    /// `users(*)` after resolution has spent the mention: the read names
    /// nothing and continues the relation the mention resolved to.
    Ground {
        outer: bool,
        published: SemanticRelation,
    },
    /// A callable relation standing as a head, and the result its
    /// application produced.
    Call {
        call: crate::pipeline::asts::core::SealedCall<P>,
        alias: P::StageName,
        published: SemanticRelation,
    },
    /// An anonymous table written here, and the rows it publishes.
    Anonymous {
        relation: crate::pipeline::asts::core::AnonRelation<P>,
        published: SemanticRelation,
    },
}

/// WHAT A PRODUCING STEP DOES.
///
/// ONE description. Each variant decides BOTH the continuation the tree
/// stores and the output law the relation is derived under, so there is no
/// call that stores a join and derives a preserve. The operand is never a
/// field: [`SemanticBuilder::extend`] reads it out of the chain the step
/// lands on.
pub(crate) enum StepOp<'a, P: crate::pipeline::asts::core::Phase> {
    /// §5's comma: another relation joined to the chain so far. The result
    /// publishes both headings, in operand order.
    Join {
        rhs: crate::pipeline::asts::core::Chain<P>,
        correlation: P::MemberCorr,
        join_type: Option<crate::pipeline::asts::core::JoinType>,
        right: SemanticRelation,
        kind: super::form::JoinKind,
        merged: &'a [super::form::MergedKey],
    },
    /// THE DIMENSIONS ASKED OF THE RELATION TO THE LEFT.
    ///
    /// The slots decide the interface and the stored ask IS that interface
    /// — one binding slot per position the derivation published — so the
    /// tree cannot say the access selects one thing while the relation
    /// publishes another.
    Access {
        shape: super::form::AccessShape,
        slots: &'a [super::form::ProjectSlot],
        dependencies: &'a [PortId],
    },
    /// A PROJECTION THAT REPUBLISHES WHAT AN EXACT OPERATION PUBLISHES.
    ///
    /// An ER boundary exports its endpoints; a source projection keeps the
    /// columns a call site asked for. In both the result names nothing
    /// anew: each item carries one source occurrence into the position the
    /// derivation just minted for it, so the items are the interface and
    /// the caller states only which source each position carries. The
    /// operation is one of the two REPUBLISHING forms — not an arbitrary
    /// form a caller could pair with sources it does not publish.
    Republish {
        of: Republishing<'a>,
        sources: Vec<PortId>,
    },
}

/// The exact operations a republishing step stands over.
pub(crate) enum Republishing<'a> {
    /// The boundary of an ER edge: schema(A) + schema(B), each position
    /// answering to its endpoint.
    ErBoundary(super::form::ErBoundarySpec<'a>),
    /// A projection keeping exactly the stated positions of its operand.
    Project(super::form::ProjectSpec<'a>),
    /// A new occurrence publishing its input's whole heading, one-to-one.
    Export(super::form::ExportSpec),
}

/// ONE BINDING SLOT PER POSITION A DERIVED INTERFACE PUBLISHES.
///
/// The stored ask IS the interface, so the tree cannot say the access
/// selects one thing while the relation publishes another. A pattern whose
/// every position is discarded or constrained publishes nothing, asks for
/// no dimension at all, and `Unasked` is what that means.
fn binding_access<P>(ports: &[PortId]) -> crate::pipeline::asts::core::Access<P>
where
    P: crate::pipeline::asts::core::Phase<Binder = PortId>,
{
    match crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
        ports
            .iter()
            .copied()
            .map(crate::pipeline::asts::core::Slot::Bind)
            .collect(),
    ) {
        Some(slots) => crate::pipeline::asts::core::Access::Slots(slots),
        None => crate::pipeline::asts::core::Access::Unasked,
    }
}

/// WHAT BOUNDARY A HEAD PUBLISHES OVER ITS OWN BODY.
///
/// Both are derived FROM the body standing inside the head, so neither is a
/// relation a caller could have got from somewhere else.
pub(crate) enum Boundary {
    /// A definition invoked here: a fresh instance of what the body
    /// publishes, answering to the name it was invoked by.
    Instance {
        kind: super::form::DefinitionKind,
        answers_to: Option<crate::names::Spelling>,
    },
    /// An interior relation addressed by its own written name. The boundary
    /// spends the body's correlation support outward: its level emits what
    /// the body owed, and nothing over it owes it further.
    Interior { answer: crate::names::Spelling },
}

/// The relation a wrapping ground form's own body publishes.
///
/// Exhaustive on the ground taxonomy, so a new head kind must decide here
/// whether it wraps a body or is built by an exact operation. `None` says
/// there is no body — not that one could not be found.
fn wrapped_body<P>(form: &crate::pipeline::asts::core::GroundForm<P>) -> Option<SemanticRelation>
where
    P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
{
    use crate::pipeline::asts::core::expressions::{GroundForm, InnerRelationPattern, Relation};
    match form {
        GroundForm::Reference(Relation::ConsultedView { body, .. }) => {
            Some(body.body.semantic_relation())
        }
        GroundForm::Reference(Relation::InnerRelation { pattern, .. }) => match pattern {
            InnerRelationPattern::Indeterminate { subquery, .. }
            | InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. }
            | InnerRelationPattern::CorrelatedScalarJoin { subquery, .. }
            | InnerRelationPattern::CorrelatedGroupJoin { subquery, .. } => {
                Some(subquery.semantic_relation())
            }
        },
        GroundForm::Reference(Relation::Ground { .. } | Relation::FunctorCall { .. })
        | GroundForm::Literal(_) => None,
    }
}

/// WHETHER A CARRIED POSITION STILL HOLDS ITS SOURCE'S VALUE. Facts that
/// belong to the value — the static interior a tree-valued position owns —
/// travel only with the same value; a rewritten position starts without
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CarriedValue {
    Same,
    Rewritten,
}

/// THE OCCURRENCE EFFECT of one carried position, stated by the act that
/// carries it and assigned ONCE at the port's birth: the output CONTINUES
/// its source's exact occurrence, or it REPUBLISHES the value as an
/// occurrence of its own. There is no default; every carry names its
/// effect, and the classification is the form's:
///
/// - continues: an export, an order, an instance's read, a join's operand
///   runs, a rename, a reposition, a removal, an extension's operand run,
///   an ER boundary's exports, a minus's left export, an explosion's
///   carried context, an unwritten cover cell, a caller pattern's selected
///   dimensions, a publication's one carrier of a position (or its one
///   inherited carrier among several), and a set slot every arm of which
///   continues one origin;
/// - republishes: a plan scratch holding a select list, an extension's
///   added slots, a written cover cell, a second authored carry beside an
///   inherited one, several renamings of one position, and a set slot
///   whose arms disagree or pad.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Continuity {
    Continues,
    Republishes,
}

/// How a publication's slots decide continuity: from the slots themselves
/// (a position continues into the one slot that carries it, or the one
/// inherited slot among several), or all republish — the added slots of an
/// extension, whose operand run already continued every position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotContinuity {
    Decide,
    Republish,
}

#[derive(Clone, Copy)]
enum CarryOwner {
    New,
    Preserve,
}
/// A total old-to-new port correspondence.
///
/// Total in one direction on purpose: every OLD port has an answer, because
/// predicate motion and correlation motion have to be able to rewrite any
/// reference the old relation could carry. A new port with no old
/// counterpart is lawful — a rebuild may add positions — so the map does
/// not claim to be onto.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TotalPortMap {
    from: RelationId,
    to: RelationId,
    pairs: Vec<(PortId, PortId)>,
}

impl TotalPortMap {
    /// Where an old port went. Every old port has an answer.
    pub fn answer(&self, old: PortId) -> Option<PortId> {
        self.pairs
            .iter()
            .find(|(o, _)| *o == old)
            .map(|(_, new)| *new)
    }

    pub(crate) fn pairs(&self) -> &[(PortId, PortId)] {
        &self.pairs
    }

    pub(crate) fn from_relation(&self) -> RelationId {
        self.from
    }
}

/// ONE REBUILD OF ONE OPERAND, performed by the authority.
///
/// Opened by [`SemanticBuilder::rebuilding`] over the occurrences the
/// operand stands over, in the order the rebuild consumes them. Every join
/// is derived HERE. What the rebuild admits from outside is one realization
/// per stated occurrence — the occurrence itself, a recorded replacement of
/// it, or a republication of it by the record — and the replacement this
/// rebuild certifies, for a resolver-era intermediate or for the operand,
/// names only the product it derived over those realizations. There is no
/// entrance that pairs a finished relation with another as a candidate.
pub(crate) struct Rebuild<'r> {
    authority: SemanticBuilder<'r>,
    operand: SemanticRelation,
    occurrences: Vec<SemanticRelation>,
    /// Each operand position's stated sources, when the operand stands over
    /// exactly the stated occurrences. `None` says it does not, and this
    /// rebuild then derives its product and certifies nothing.
    operand_sources: Option<Vec<(PortId, Vec<PortId>)>>,
    /// How many stated occurrences the product stands over so far.
    consumed: usize,
    /// The relation the product stands at, once the rebuild has begun.
    product: Option<SemanticRelation>,
}

impl<'r> Rebuild<'r> {
    /// THE REBUILD BEGINS WITH THE FIRST OCCURRENCE. The chain admitted here
    /// is not derived by the rebuild: it is the occurrence itself, a
    /// relation a rebuild recorded as replacing it, or a republication of
    /// it by the record — never a relation that merely carries its
    /// positions beside another operand.
    pub(crate) fn begins_with<P>(
        &mut self,
        first: &crate::pipeline::asts::core::Chain<P>,
    ) -> Result<()>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        if self.product.is_some() {
            return Err(replacement_error("a rebuild begins once"));
        }
        let relation = first.semantic_relation();
        if !self.authority.stands_for(&self.occurrences[0], &relation)? {
            return Err(replacement_error(
                "a rebuild begins with the first occurrence it stands over, or a relation recorded as standing in its place",
            ));
        }
        self.product = Some(relation);
        self.consumed = 1;
        Ok(())
    }

    /// JOIN THE NEXT OCCURRENCE onto the product.
    ///
    /// The left operand is the product this rebuild derived; the right is
    /// the next stated occurrence or a relation recorded as standing in its
    /// place. A correspondence written against the operands the resolver
    /// saw is translated onto the operands as they stand here, through the
    /// records rebuilds wrote, so the join stands on positions its operands
    /// publish. Anything else is refused: this is the one road by which a
    /// rebuild's product comes to exist, and it consumes only what the
    /// rebuild was opened over.
    pub(crate) fn join<P>(
        &mut self,
        left: crate::pipeline::asts::core::Chain<P>,
        rhs: crate::pipeline::asts::core::Chain<P>,
        correlation: crate::pipeline::asts::core::MemberCorrelation<P>,
        join_type: Option<crate::pipeline::asts::core::JoinType>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<
            Scope = SemanticRelation,
            Output = PortId,
            Binder = PortId,
            Col = crate::pipeline::asts::core::ColumnOccurrence,
            Correspondence = crate::pipeline::asts::core::Correspondence,
            MemberCorr = crate::pipeline::asts::core::MemberCorrelation<P>,
        >,
    {
        use crate::pipeline::asts::core::{JoinType, MemberCorrelation};
        let Some(product) = self.product else {
            return Err(replacement_error("a rebuild joins after it has begun"));
        };
        let left_relation = left.semantic_relation();
        if left_relation != product {
            return Err(replacement_error(
                "a rebuild joins onto the product it derived, not onto a relation assembled elsewhere",
            ));
        }
        let Some(next) = self.occurrences.get(self.consumed) else {
            return Err(replacement_error(
                "a rebuild joins no more occurrences than it was opened over",
            ));
        };
        let right_relation = rhs.semantic_relation();
        if !self.authority.stands_for(next, &right_relation)? {
            return Err(replacement_error(
                "a rebuild joins the next occurrence it stands over, or a relation recorded as standing in its place",
            ));
        }
        let correlation = match correlation {
            MemberCorrelation::Correspond(correspondence) => {
                let pairs = correspondence
                    .pairs
                    .into_iter()
                    .map(|pair| {
                        Ok(MergedKey {
                            left: self.authority.port_in(&left_relation, pair.left)?,
                            right: self.authority.port_in(&right_relation, pair.right)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                MemberCorrelation::Correspond(crate::pipeline::asts::core::Correspondence::new(
                    pairs,
                ))
            }
            other => other,
        };
        // A CORRESPONDENCE MERGES POSITIONS, and the join it stands on is
        // the one the resolver already derived. Rebuilding with an empty
        // merge list would publish the right operand's shared columns a
        // second time, so the rebuilt relation would not stand where the
        // resolved one stood and every reference through it would move.
        let merged = match &correlation {
            MemberCorrelation::Correspond(correspondence) => correspondence.pairs.clone(),
            MemberCorrelation::Condition(_) | MemberCorrelation::Cartesian(_) => Vec::new(),
        };
        let join_type = join_type.unwrap_or(JoinType::Inner);
        let kind = match join_type {
            JoinType::LeftOuter => JoinKind::LeftOuter,
            JoinType::RightOuter => JoinKind::RightOuter,
            JoinType::FullOuter => JoinKind::FullOuter,
            JoinType::Inner => JoinKind::Inner,
        };
        let chain = self.authority.extend(
            left,
            StepOp::Join {
                rhs,
                correlation,
                join_type: Some(join_type),
                right: right_relation,
                kind,
                merged: &merged,
            },
        )?;
        self.product = Some(chain.semantic_relation());
        self.consumed += 1;
        Ok(chain)
    }

    /// CERTIFY THAT THE PRODUCT SO FAR REPLACES A RESOLVER-ERA INTERMEDIATE.
    ///
    /// The intermediate must stand over exactly the occurrences consumed so
    /// far, judged from the record; the product does, by construction. Its
    /// positions pair with the product's by the stated sources each stands
    /// over. An intermediate that stands over something else, or whose
    /// positions do not pair one to one, is certified for nothing.
    pub(crate) fn replaces<P>(
        &self,
        intermediate: SemanticRelation,
        product: &crate::pipeline::asts::core::Chain<P>,
    ) -> Result<()>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        check_mark(self.authority.mark, &intermediate)?;
        let Some(derived) = self.product else {
            return Err(replacement_error("a rebuild certifies after it has begun"));
        };
        if product.semantic_relation() != derived {
            return Err(replacement_error(
                "a rebuild certifies only the product it derived",
            ));
        }
        if intermediate == derived {
            return Ok(());
        }
        let stated = &self.occurrences[..self.consumed];
        let Some(sources) = self.authority.standing_over(&intermediate, stated)? else {
            return Ok(());
        };
        self.certify(intermediate, sources, stated)
    }

    /// CLOSE THE REBUILD, certifying that its product replaces the operand.
    ///
    /// Every stated occurrence must have been consumed, the chain must
    /// publish the product this rebuild derived, and the operand must have
    /// been judged at the opening to stand over exactly the stated
    /// occurrences; otherwise the product is returned uncertified.
    pub(crate) fn finish<P>(
        self,
        product: crate::pipeline::asts::core::Chain<P>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let now = product.semantic_relation();
        if now == self.operand {
            return Ok(product);
        }
        if self.product != Some(now) {
            return Err(replacement_error(
                "a rebuild certifies only the product it derived",
            ));
        }
        if self.consumed != self.occurrences.len() {
            return Err(replacement_error(
                "a rebuild consumes every occurrence it was opened over before it closes",
            ));
        }
        if let Some(sources) = self.operand_sources.clone() {
            self.certify(self.operand, sources, &self.occurrences)?;
        }
        Ok(product)
    }

    /// Pair an old relation's positions with the product's by the stated
    /// sources each stands over, and record the map. Every old position
    /// must be met by exactly one product position; anything else is not a
    /// replacement and nothing is recorded.
    fn certify(
        &self,
        old: SemanticRelation,
        old_sources: Vec<(PortId, Vec<PortId>)>,
        stated: &[SemanticRelation],
    ) -> Result<()> {
        let Some(product) = self.product else {
            return Ok(());
        };
        // THE PRODUCT IS JUDGED BY THE SAME STANDING AS THE OPERAND: every
        // position over at least one stated occurrence and over nothing
        // else. It is derived here over the stated occurrences, so this
        // holds by construction; asking again is what makes the certificate
        // account for every relationship it grants rather than for the
        // slots it happens to pair.
        let Some(product_sources) = self.authority.standing_over(&product, stated)? else {
            return Ok(());
        };
        let standing: Vec<(Vec<PortId>, PortId)> = product_sources
            .into_iter()
            .map(|(port, from)| (from, port))
            .collect();
        let mut pairs = Vec::with_capacity(old_sources.len());
        for (port, from) in old_sources {
            let mut met = standing
                .iter()
                .filter(|(stands_for, _)| *stands_for == from);
            let (Some((_, landed)), None) = (met.next(), met.next()) else {
                return Ok(());
            };
            pairs.push((port, *landed));
        }
        self.authority.registry.relations().record_replacement(
            old.relation(),
            product.relation(),
            TotalPortMap {
                from: old.relation(),
                to: product.relation(),
                pairs,
            },
        );
        Ok(())
    }
}

/// WHAT A SHAPE-CHANGING REFINEMENT PRODUCED.
///
/// One typed outcome, and the ONLY producer is
/// [`SemanticBuilder::refine_relation`]. A caller cannot assemble one: it
/// holds neither the old relation, nor the new one, nor the map, until the
/// authority hands back the outcome that carries them together.
/// What the construction record says one refinement did to its operand.
enum Made {
    /// It published the operand itself.
    Preserved,
    /// It derived a new relation from the operand, carrying every
    /// position; the map is the record that derivation wrote.
    Rebuilt(TotalPortMap),
    /// It stood the operand aside and built over the operand's own
    /// SOURCES. No position of the operand was carried into the result, so
    /// there is nothing to record and nothing to translate through.
    Resited,
}

pub enum Refinement<P: crate::pipeline::asts::core::Phase> {
    /// The refinement published the relation it was given. There is
    /// nothing to translate and nothing recorded.
    Preserved(crate::pipeline::asts::core::Chain<P>),
    /// The refinement built a NEW relation from the operand. The map is
    /// the lineage that build wrote down, total over the operand's ports.
    Rebuilt {
        chain: crate::pipeline::asts::core::Chain<P>,
        map: TotalPortMap,
    },
}

/// A REBUILD OF AN OPERAND THE AUTHORITY JUDGED: the chain now standing
/// where the operand stood, and the operand it replaces, as ONE value.
///
/// Minted only by [`SemanticBuilder::rebuilt`], which ran the rewrite and
/// judged its product against the construction record — the relation it was
/// handed, or one derived from it carrying every position the operand
/// published, recorded as that operand's replacement; a product built beside
/// the operand refuses. Private fields: the chain and the operand it replaces
/// are paired nowhere else, so a carrier handed one of these knows the
/// replacement was judged FOR the operand it names, and can refuse one that
/// names another.
pub struct Replacement<P: crate::pipeline::asts::core::Phase> {
    chain: crate::pipeline::asts::core::Chain<P>,
    of: SemanticRelation,
}

impl<P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>> Replacement<P> {
    /// The operand this replaces.
    pub(crate) fn of(&self) -> SemanticRelation {
        self.of
    }

    /// The relation the replacement publishes.
    pub(crate) fn published(&self) -> SemanticRelation {
        self.chain.semantic_relation()
    }

    /// The chain, for the position that stands it where the operand stood.
    pub(crate) fn into_chain(self) -> crate::pipeline::asts::core::Chain<P> {
        self.chain
    }
}

/// The semantic epoch, closed.
///
/// A READER. It has no `derive`, no registry, and no way to mint anything:
/// SQL lowering receives one of these and can ask what a relation
/// publishes, which is exactly the capability lowering needs and the only
/// one it may have.
#[derive(Clone)]
pub struct Relations {
    pub(super) registry: std::rc::Rc<Registry>,
    pub(super) mark: BuilderMark,
}

impl Relations {
    /// The naming handle these records are indexed by.
    pub(crate) fn names(&self) -> &std::rc::Rc<Registry> {
        &self.registry
    }

    /// The interface a relation publishes, read from the record the
    /// authority built.
    ///
    /// A READER. There is no `derive` here and no way to reach one: a
    /// lowering that wanted to mint a semantic port has nothing to mint it
    /// with.
    ///
    /// The registry is the READER'S, not an argument. A reader, a relation
    /// and a store that could arrive separately are three things a caller
    /// can mismatch: checking the relation's epoch against this reader's
    /// mark says nothing about which compilation's records were then read.
    /// One borrow makes the three one epoch by construction.
    pub fn interface(&self, relation: &SemanticRelation) -> Result<Interface> {
        check_mark(self.mark, relation)?;
        Ok(interface_of(&self.registry, relation))
    }

    /// The immediate source ports carried into each output position.
    ///
    /// Ordered by the output interface, and recorded by the same authority
    /// act that minted those outputs. A position can have no source (a new
    /// computation) or several sources (a merged join key), so the shape
    /// does not pretend every operation is a one-to-one projection.
    /// Every construction-recorded carry edge into ONE port, across all
    /// relations — the reverse read a total operand map walks when a
    /// moved predicate's reference stands several construction levels
    /// above the operand that realizes it.
    pub(crate) fn carried_from(&self, destination: PortId) -> Vec<PortId> {
        self.registry.relations().carried_from(destination)
    }

    pub(crate) fn carried_sources(
        &self,
        relation: &SemanticRelation,
    ) -> Result<Vec<(PortId, Vec<PortId>)>> {
        check_mark(self.mark, relation)?;
        let carried = self
            .registry
            .relations()
            .translations_into(relation.relation());
        Ok(self
            .interface(relation)?
            .ports()
            .iter()
            .copied()
            .map(|output| {
                let sources = carried
                    .iter()
                    .filter_map(|(source, destination)| (*destination == output).then_some(*source))
                    .collect();
                (output, sources)
            })
            .collect())
    }

    /// The exact semantic operands recorded with this construction.
    pub(crate) fn inputs(&self, relation: &SemanticRelation) -> Result<Vec<SemanticRelation>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().inputs(relation.relation()))
    }

    /// Exact input ports this operation consumes without publishing, in the
    /// order its level emits them after its heading: a pattern's constrained
    /// positions, then the correlation support it keeps readable — or, for
    /// a join, its operands' runs left then right.
    pub(crate) fn dependencies(&self, relation: &SemanticRelation) -> Result<Vec<PortId>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().support_run(relation.relation()))
    }

    /// The correlation support this relation's level emits beside its
    /// heading. Owed by construction: a level that does not emit it has
    /// dropped an obligation, which is a refusal and never a scaffold.
    pub(crate) fn correlation_support(&self, relation: &SemanticRelation) -> Result<Vec<PortId>> {
        check_mark(self.mark, relation)?;
        Ok(self
            .registry
            .relations()
            .support_emitted(relation.relation()))
    }

    /// Whether a position is emitted by no level of its own — carried
    /// toward an interior boundary unread, or the boundary's own position
    /// the enclosing join computes — so the level publishing it binds it
    /// to no slot.
    pub(crate) fn unemitted_here(&self, port: PortId) -> bool {
        self.registry.relations().unemitted_here(port)
    }

    /// EVERY OCCURRENCE ONE POSITION CONTINUES, by the carry edges the acts
    /// that minted it and its ancestors wrote — the same record
    /// [`super::stands_where`] reads. For a support slot, which realizes a
    /// position some level below minted: a reference to the occurrence that
    /// position continues means this slot.
    pub(crate) fn continued_occurrences(&self, port: PortId) -> Vec<PortId> {
        let store = self.registry.relations();
        let mut frontier = vec![port];
        let mut seen: Vec<PortId> = Vec::new();
        while let Some(here) = frontier.pop() {
            for source in store.lineage(here) {
                if !seen.contains(&source) {
                    seen.push(source);
                    frontier.push(source);
                }
            }
        }
        seen
    }

    /// The physical storage this exact semantic occurrence reads, if any.
    pub(crate) fn storage(&self, relation: &SemanticRelation) -> Result<Option<StorageId>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().storage(relation.relation()))
    }

    /// The exact reusable-definition operation that created this occurrence.
    pub(crate) fn instance_kind(
        &self,
        relation: &SemanticRelation,
    ) -> Result<Option<DefinitionKind>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().instance_kind(relation.relation()))
    }

    /// Translate one exact ancestor port into this relation's interface.
    pub(crate) fn translated_port(
        &self,
        relation: &SemanticRelation,
        source: PortId,
    ) -> Result<Option<PortId>> {
        if self.interface(relation)?.ports().contains(&source) {
            return Ok(Some(source));
        }
        let mut answers = self
            .translated_ports(relation)?
            .into_iter()
            .filter_map(|(old, new)| (old == source).then_some(new));
        match (answers.next(), answers.next()) {
            (Some(port), None) => Ok(Some(port)),
            (None, _) => Ok(None),
            (Some(_), Some(_)) => Err(replacement_error(
                "one ancestor port translates to several positions of one relation",
            )),
        }
    }

    /// The semantic owner this exact port reports to structural metadata.
    pub(crate) fn owner(&self, port: PortId) -> Result<ScopeId> {
        self.registry
            .relations()
            .owner(port)
            .ok_or_else(|| replacement_error("a semantic port has no construction-recorded owner"))
    }

    /// The catalog entity construction attached to this exact occurrence.
    pub fn entity(&self, relation: &SemanticRelation) -> Result<Option<crate::names::EntityId>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().entity(relation.relation()))
    }

    /// The exact READ occurrence this relation's construction descends to.
    ///
    /// A pattern stands on one read — a catalog source, an anonymous body,
    /// a definition instance — and its own access and answering-name export
    /// are its construction, not another read. This is what the FROM entry
    /// names; nothing recovers it from a scope kind or a parent walk.
    pub(crate) fn read_source(
        &self,
        relation: &SemanticRelation,
    ) -> Result<Option<SemanticRelation>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().read_source(relation.relation()))
    }

    /// The exact semantic interface attached to a tree-valued output port.
    pub(crate) fn interior(&self, owner: PortId) -> Result<Option<SemanticRelation>> {
        let relation = self.registry.relations().interior(owner);
        if let Some(relation) = relation {
            check_mark(self.mark, &relation)?;
        }
        Ok(relation)
    }

    pub fn is_row_bounded(&self, relation: &SemanticRelation) -> Result<bool> {
        check_mark(self.mark, relation)?;
        Ok(self
            .registry
            .relations()
            .is_row_bounded(relation.relation()))
    }

    pub fn is_materialized_once(&self, relation: &SemanticRelation) -> Result<bool> {
        check_mark(self.mark, relation)?;
        Ok(self
            .registry
            .relations()
            .is_materialized_once(relation.relation()))
    }

    /// Every construction-recorded ancestor port translated to this
    /// relation's exact output port.
    ///
    /// Refinement can rebuild more than once.  A physical site realizes the
    /// final relation, while scalar expressions in the refined tree may still
    /// name any preserved operand port.  The replacement graph is the total
    /// evidence for that translation; lowering never searches column lineage.
    pub(crate) fn translated_ports(
        &self,
        relation: &SemanticRelation,
    ) -> Result<Vec<(PortId, PortId)>> {
        check_mark(self.mark, relation)?;
        translated_ports_for(&self.registry, relation)
    }

    /// Every exact ancestor port construction carried into one output.
    pub(crate) fn ancestors_into(
        &self,
        relation: &SemanticRelation,
        output: PortId,
    ) -> Result<Vec<PortId>> {
        check_mark(self.mark, relation)?;
        if !self.interface(relation)?.ports().contains(&output) {
            return Err(replacement_error(
                "an output ancestry query names a port outside its relation",
            ));
        }
        let mut ancestors = vec![output];
        ancestors.extend(
            self.translated_ports(relation)?
                .into_iter()
                .filter_map(|(source, destination)| (destination == output).then_some(source)),
        );
        ancestors.sort_unstable();
        ancestors.dedup();
        Ok(ancestors)
    }
}

/// Which of a relation's own output positions each port of an operand is
/// carried into.
///
/// A RELATION, not a function. `(q.*, q.*)` carries one source into two
/// positions, and collapsing that to "no answer" would erase the shared
/// ancestry the two siblings actually have. Consumers that need a single
/// answer say so where they ask.
type PortReach = std::collections::HashMap<PortId, std::collections::BTreeSet<PortId>>;

fn translated_ports_for(
    registry: &Registry,
    relation: &SemanticRelation,
) -> Result<Vec<(PortId, PortId)>> {
    let interface = interface_of(registry, relation);
    let identity: PortReach = interface
        .ports()
        .iter()
        .copied()
        .map(|port| (port, std::iter::once(port).collect()))
        .collect();
    let mut translated = std::collections::BTreeSet::new();
    let mut active = std::collections::HashSet::new();
    collect_translations(
        registry,
        relation.relation(),
        &identity,
        &mut translated,
        &mut active,
    )?;
    Ok(translated
        .into_iter()
        .filter(|(old, new)| old != new)
        .collect())
}

fn collect_translations(
    registry: &Registry,
    relation: RelationId,
    into_final: &PortReach,
    translated: &mut std::collections::BTreeSet<(PortId, PortId)>,
    active: &mut std::collections::HashSet<RelationId>,
) -> Result<()> {
    if !active.insert(relation) {
        return Err(replacement_error(
            "the construction-owned relation replacement graph contains a cycle",
        ));
    }
    let mut carried_by_relation: std::collections::HashMap<RelationId, PortReach> =
        std::collections::HashMap::new();
    for (old, new) in registry.relations().translations_into(relation) {
        let Some(finals) = into_final.get(&new) else {
            continue;
        };
        translated.extend(finals.iter().map(|final_port| (old, *final_port)));
        if let Some(source_relation) = registry.relations().relation_of(old) {
            carried_by_relation
                .entry(source_relation)
                .or_default()
                .entry(old)
                .or_default()
                .extend(finals.iter().copied());
        }
    }
    for (source_relation, through) in carried_by_relation {
        if registry.relations().interface(source_relation).is_some() {
            collect_translations(registry, source_relation, &through, translated, active)?;
        }
    }
    for map in registry.relations().replacements_into(relation) {
        let mut through = PortReach::new();
        for (old, new) in map.pairs() {
            let Some(finals) = into_final.get(new) else {
                continue;
            };
            translated.extend(finals.iter().map(|final_port| (*old, *final_port)));
            through
                .entry(*old)
                .or_default()
                .extend(finals.iter().copied());
        }
        collect_translations(registry, map.from_relation(), &through, translated, active)?;
    }
    active.remove(&relation);
    Ok(())
}

/// What a relation publishes, from the authority's record.
fn interface_of(registry: &Registry, relation: &SemanticRelation) -> Interface {
    registry
        .relations()
        .interface(relation.relation())
        .expect("every semantic relation is born with an interface")
}

/// Whether a relation belongs to the compilation this registry IS.
///
/// The check every reader owes before elaborating a relation's evidence:
/// a relation names identities its own compilation issued, and reading it
/// against another's would index records that never heard of it.
/// THE NODE AND THE PORTS IT PUBLISHES, in interface order — what every
/// bound operation answers with, produced by one act.
pub(crate) type BoundStep = (
    crate::pipeline::asts::core::Step<crate::pipeline::asts::core::Resolved>,
    Vec<PortId>,
);

/// One structural run step, as the tree stores it. The stage name is the
/// POSITION's and is spent above, so a resolved structural payload carries
/// none.
fn structural(
    form: crate::pipeline::asts::resolved::StructuralForm,
) -> crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Resolved> {
    crate::pipeline::asts::core::Continuation::Structural(
        crate::pipeline::asts::core::StructuralStep { form, named: () },
    )
}

/// THE STORED ITEM FOR ONE STATED POSITION, standing at the port the act
/// that minted it wrote there.
fn published_position(
    token: &SemanticConstruction,
    position: super::pending::Position,
    output: Option<PortId>,
) -> crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Resolved> {
    use crate::pipeline::asts::core::{OneOut, OutItem};
    match (position.into_stated(), output) {
        (Some((expr, naming)), Some(output)) => {
            OutItem::one(OneOut::published(token, expr, naming, output))
        }
        (None, None) => OutItem::Whole,
        (Some(_), None) => unreachable!("one stated value publishes one semantic port"),
        (None, Some(_)) => unreachable!("a whole operand publishes through its expansion"),
    }
}

pub(super) fn check_epoch(registry: &Registry, relation: &SemanticRelation) -> Result<()> {
    check_mark(registry.relations().epoch(), relation)
}

fn replacement_error(what: &str) -> DelightQLError {
    Internal::invariant("semantic rewrite", what)
}

/// The refusal a form gives when it cannot keep a correlation's interior
/// occurrence readable. `what` names the form, in the author's terms.
fn unrealizable_support(what: &str) -> DelightQLError {
    DelightQLError::from(crate::diagnostic::Interior::CorrelationSupport {
        message: format!(
            "a correlated interior cannot carry its correlation through {what}: the \
             correlation is evaluated at the enclosing join, which must still read the \
             interior column it names, and this operation cannot keep that column \
             readable without changing what the interior means — group by the \
             correlated column, keep it in the interior's heading, or state the \
             correlation outside the interior"
        ),
    })
}

/// WHAT A FORM DOES WITH THE CORRELATION SUPPORT ITS OPERANDS OWE.
///
/// Total over the vocabulary, and fail-closed by construction: a form that
/// republishes its operand's dimensions or adds beside them CARRIES, a
/// grouping TRANSLATES through a key that stands on the occurrence, and
/// every form that would change what an operand position means REFUSES
/// while anything is owed. There is no arm that owes nothing by default.
fn support_law_of(form: &RelForm<'_>) -> SupportLaw {
    match form {
        // No operand to owe for: the read is where a correlation's interior
        // occurrences are born, and it publishes every one of them.
        RelForm::Source(_) | RelForm::Anonymous(_) | RelForm::Opaque => SupportLaw::Carries,
        // A join's row is the concatenation of its operands' rows, support
        // columns included.
        RelForm::Join(_) => SupportLaw::Concatenates,
        // Each republishes the operand's own dimensions, or adds beside
        // them, and its level can emit any operand position it does not
        // publish.
        RelForm::Access(_)
        | RelForm::Project(_)
        | RelForm::Embed(_)
        | RelForm::Rename(_)
        | RelForm::Reposition(_)
        | RelForm::ProjectOut(_)
        | RelForm::Cover(_)
        | RelForm::Order(_) => SupportLaw::Carries,
        RelForm::Export(spec) => match spec.why {
            ExportWhy::Alias { .. }
            | ExportWhy::Bound { .. }
            | ExportWhy::Stage
            | ExportWhy::EmissionAlias => SupportLaw::Carries,
            // A binding is a complete statement: nothing inside it can be
            // evaluated at a join outside it.
            ExportWhy::Cte { .. } => SupportLaw::Refuses("a query-local binding"),
        },
        RelForm::Group(spec) => SupportLaw::Translates {
            keys: spec.keys.len(),
        },
        RelForm::Set(_) => SupportLaw::Refuses("a set operation"),
        RelForm::Minus(_) => SupportLaw::Refuses("a set difference"),
        RelForm::Witness(_) => SupportLaw::Refuses("a witness"),
        RelForm::SignedWitness(_) => SupportLaw::Refuses("a signed witness"),
        RelForm::Meta(_) => SupportLaw::Refuses("a schema reflection"),
        RelForm::Destructure(_) => SupportLaw::Refuses("a destructuring"),
        RelForm::Drill(_) => SupportLaw::Refuses("a drill"),
        RelForm::Narrow(_) => SupportLaw::Refuses("a narrowing"),
        RelForm::Interior(_) => SupportLaw::Refuses("a nested interior"),
        RelForm::ErBoundary(_) => SupportLaw::Refuses("an ER edge"),
        RelForm::Instantiate(_) => SupportLaw::Refuses("a definition instance"),
        RelForm::PlanRead(_) => SupportLaw::Refuses("a plan read"),
        RelForm::Scratch(_) => SupportLaw::Refuses("a scratch"),
    }
}

fn check_mark(mark: BuilderMark, relation: &SemanticRelation) -> Result<()> {
    if relation.origin() == mark {
        return Ok(());
    }
    Err(Internal::invariant(
        "semantic relation",
        "a semantic relation built against another compilation's registry \
         cannot be read here: it names identities this epoch never issued",
    ))
}

/// The open construction authority.
///
/// Interior mutability rather than `&mut`: the authority is reached from
/// resolution, refinement, and the effect planner, and threading a unique
/// borrow through all three would spread the plumbing the design exists to
/// remove. The mutability ENDS at [`SemanticBuilder::seal`] — after it the
/// builder refuses to construct and the sealed store has none at all.
pub struct SemanticBuilder<'r> {
    registry: &'r Registry,
    mark: BuilderMark,
    construction: SemanticConstruction,
    /// THE ACTIVE OUTPUT BOUNDARY — set by [`Self::derive`] from the form
    /// being executed, applied by the ONE mint/carry entrance to every
    /// position the derivation publishes. No arm chooses an address role
    /// against it: an arm states a proposal (the publication's own facts),
    /// and a dequalifying stage crosses every proposal into its bare
    /// publication. Saved and restored around each derivation, so a nested
    /// derive judges its own form.
    boundary: std::cell::Cell<OutputBoundary>,
}

impl<'r> SemanticBuilder<'r> {
    /// The authority over one compilation's identities.
    ///
    /// The mark is the compilation's MINTED epoch, drawn once when its
    /// record store was created, so every authority over one registry is
    /// one epoch and a relation built against a different compilation
    /// refuses at the entrance. Constructing a second authority over the
    /// same registry is therefore not a second road: it is the same road,
    /// reached from another phase.
    pub(super) fn new(registry: &'r Registry) -> Self {
        let mark = registry.relations().epoch();
        SemanticBuilder {
            registry,
            mark,
            construction: SemanticConstruction(()),
            boundary: std::cell::Cell::new(OutputBoundary::Publishing),
        }
    }

    /// EXPORT WHAT A CHAIN PUBLISHES UNDER AN AUTHORED LEXICAL ALIAS.
    ///
    /// The export is derived HERE from the relation the chain's outermost
    /// node publishes, and it replaces that node's result in the same act.
    /// A caller never holds the export, so it has nothing to attach to
    /// another node; the only road to this operation is the authority's.
    pub(crate) fn alias_result<P>(
        &self,
        chain: &mut crate::pipeline::asts::core::Chain<P>,
        answer: crate::names::Spelling,
    ) -> Result<SemanticRelation>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let input = chain.semantic_relation();
        let result = self.derive(RelForm::Export(ExportSpec {
            input,
            why: ExportWhy::Alias { answer },
        }))?;
        chain.restate_outermost(&self.construction, result);
        Ok(result)
    }

    /// THE ONE BOUNDARY ACT. Every position a derivation publishes — carried
    /// or minted, by any arm — crosses this judgment: a publishing boundary
    /// keeps the arm's proposal; a PIPE STAGE dequalifies it. Every PIPE
    /// FORM is SCOPE-DEQUALIFYING (fundamentals), so an addressable
    /// proposal crosses as the stage's own bare publication and nothing
    /// else — no role exists that could carry the operand's qualifier
    /// across; the compiler's hygiene and the author's latent dimensions
    /// are not addresses and cross unchanged.
    fn published_role(&self, proposed: Addressing) -> Addressing {
        match self.boundary.get() {
            OutputBoundary::Publishing => proposed,
            OutputBoundary::PipeStage => match proposed {
                Addressing::Hygienic => Addressing::Hygienic,
                Addressing::Latent => Addressing::Latent,
                Addressing::Published
                | Addressing::Bare
                | Addressing::BareUnder
                | Addressing::BareStage => Addressing::BareStage,
            },
        }
    }

    /// A position MINTED anew, through the same one boundary act a carried
    /// position crosses.
    fn mint(
        &self,
        scope: ScopeId,
        published: Option<crate::names::Spelling>,
        proposed: Addressing,
        facts: ValueFacts,
    ) -> PortId {
        PortId(self.registry.mint_new_semantic_port(
            &self.construction,
            scope,
            published,
            self.published_role(proposed),
            facts,
        ))
    }

    /// THE ONE CARRY ACT, and it names its occurrence effect: there is no
    /// entrance that carries a position without saying whether the output
    /// continues it.
    fn carry(
        &self,
        owner: CarryOwner,
        scope: ScopeId,
        source: ColId,
        published: Option<crate::names::Spelling>,
        addressing: Addressing,
        continuity: Continuity,
    ) -> PortId {
        self.carry_with(
            owner,
            scope,
            source,
            published,
            addressing,
            continuity,
            true,
            CarriedValue::Same,
            |_| {},
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn carry_with(
        &self,
        owner: CarryOwner,
        scope: ScopeId,
        source: ColId,
        published: Option<crate::names::Spelling>,
        addressing: Addressing,
        continuity: Continuity,
        preserve_higher_order_support: bool,
        value: CarriedValue,
        update: impl FnOnce(&mut ValueFacts),
    ) -> PortId {
        // THE ONE BOUNDARY ACT over a carried position: the arm proposed
        // the publication's own facts; the active boundary judges the role.
        let addressing = self.published_role(addressing);
        let output = PortId(self.registry.mint_semantic_port(
            &self.construction,
            source,
            scope,
            published,
            addressing,
            update,
        ));
        let source = PortId(source);
        self.registry.relations().record_lineage(output, source);
        self.registry
            .relations()
            .record_carried_value(output, source);
        self.registry
            .relations()
            .carry_residual_row_token(output, source);
        self.registry
            .relations()
            .carry_frontier_actual(output, source);
        if preserve_higher_order_support {
            self.registry
                .relations()
                .carry_residual_capture_value(output, source);
        }
        // THE OCCURRENCE EFFECT IS ASSIGNED HERE, ONCE, for the port this
        // act just minted: the output continues its source's exact origin,
        // or it is an occurrence of its own. No later act can revise it.
        self.registry.relations().record_occurrence(
            output,
            match continuity {
                Continuity::Continues => super::store::Occurrence::Continues(source),
                Continuity::Republishes => super::store::Occurrence::Own,
            },
        );
        // THE CHILD TRAVELS WITH THE VALUE: a position that keeps its
        // value keeps the exact interior it owned; a position a cover
        // rewrote holds a different value, which owns no static child.
        if matches!(value, CarriedValue::Same) {
            if let Some(interior) = self.registry.relations().interior(source) {
                self.registry.relations().record_interior(output, interior);
            }
            if self.registry.relations().interior_conflict(source) {
                self.registry.relations().record_interior_conflict(output);
            }
        }
        let owner = match owner {
            CarryOwner::Preserve => self
                .registry
                .relations()
                .owner(source)
                .expect("every semantic source port has a construction-recorded owner"),
            CarryOwner::New => scope,
        };
        self.registry.relations().record_owner(output, owner);
        output
    }

    fn carry_interface(
        &self,
        input: &SemanticRelation,
        scope: ScopeId,
        owner: CarryOwner,
        continuity: Continuity,
    ) -> Vec<PortId> {
        self.operand_heading(input)
            .into_iter()
            .map(|source| {
                self.carry(
                    owner,
                    scope,
                    source,
                    self.registry.published(source),
                    self.registry.addressing(source),
                    continuity,
                )
            })
            .collect()
    }

    /// Reserve a PROFFER placeholder: a landing a consulted body is parsed
    /// against before any call supplies a carrier. No carrier ever answers
    /// to it; a real carrier's landing is reserved by [`Self::bind_carrier`].
    pub fn reserve_proffer(&self) -> super::StructuralRelation {
        self.registry
            .relations()
            .reserve_structural(HoPart::Proffer)
    }

    /// BIND A STRUCTURAL CARRIER, in one act and only for the carrier
    /// authority, whose witness this takes: the landing is reserved and
    /// the body instantiated under it here, and the two leave together as
    /// one [`super::CarrierRow`]. The body's exact ordered interface is the
    /// template; there is no empty relation to grow, no caller-selected
    /// destination scope, and no landing that exists before its carrier.
    pub fn bind_carrier(
        &self,
        _witness: crate::defuse::carriers::CarrierBind,
        part: HoPart,
        body: &SemanticRelation,
    ) -> Result<super::CarrierRow> {
        self.registry.relations().check_open()?;
        check_mark(self.mark, body)?;
        let landing = self.registry.relations().reserve_structural(part);
        let relation = self.derive(RelForm::Instantiate(InstanceSpec {
            kind: DefinitionKind::HigherOrder(part),
            template: *body,
            answers_to: None,
        }))?;
        Ok(super::CarrierRow::bound(landing, relation))
    }

    /// ALLOCATE A SCRATCH ROW: derived from its spec, and the receipt of
    /// the allocation minted in the same act.
    pub fn scratch_row(&self, spec: super::form::ScratchSpec<'_>) -> Result<super::ScratchRow> {
        Ok(super::ScratchRow::minted(
            self.derive(RelForm::Scratch(spec))?,
        ))
    }

    /// The semantic owner recorded when this port was constructed.
    pub(crate) fn owner(&self, port: PortId) -> Result<ScopeId> {
        self.registry
            .relations()
            .owner(port)
            .ok_or_else(|| replacement_error("a semantic port has no construction-recorded owner"))
    }

    /// The catalog entity construction attached to this exact occurrence.
    pub fn entity(&self, relation: &SemanticRelation) -> Result<Option<crate::names::EntityId>> {
        check_mark(self.mark, relation)?;
        Ok(self.registry.relations().entity(relation.relation()))
    }

    #[cfg(test)]
    pub fn is_row_bounded(&self, relation: &SemanticRelation) -> Result<bool> {
        check_mark(self.mark, relation)?;
        Ok(self
            .registry
            .relations()
            .is_row_bounded(relation.relation()))
    }

    /// The registry this authority constructs against, for per-family
    /// payload helpers that need naming answers mid-act. A reader, not a
    /// construction capability.
    pub(crate) fn names(&self) -> &Registry {
        self.registry
    }

    pub fn is_plan_scratch(&self, relation: &SemanticRelation) -> Result<bool> {
        check_mark(self.mark, relation)?;
        Ok(matches!(
            self.registry.relations().plan_role(relation.relation()),
            Some(super::store::PlanRole::Scratch)
        ))
    }

    pub fn mark_mutation_target(
        &self,
        relation: &SemanticRelation,
        spelling: crate::names::Spelling,
    ) -> Result<()> {
        self.registry.relations().check_open()?;
        check_mark(self.mark, relation)?;
        self.registry.relations().mark_mutation_target(
            relation.relation(),
            relation.scope(),
            spelling,
        );
        Ok(())
    }

    pub fn mutation_marks(
        &self,
        relation: &SemanticRelation,
    ) -> Result<Vec<(ScopeId, crate::names::Spelling)>> {
        check_mark(self.mark, relation)?;
        Ok(self
            .registry
            .relations()
            .mutation_marks(relation.relation()))
    }

    pub fn mark_row_bounded(&self, relation: &SemanticRelation) -> Result<()> {
        self.registry.relations().check_open()?;
        check_mark(self.mark, relation)?;
        self.registry
            .relations()
            .mark_row_bounded(relation.relation());
        Ok(())
    }

    /// Require one physical evaluation of this exact reusable relation.
    /// Closed configured values use this positive construction fact; no
    /// lowering pass infers volatility or recognizes a function spelling.
    pub fn mark_materialized_once(&self, relation: &SemanticRelation) -> Result<()> {
        self.registry.relations().check_open()?;
        check_mark(self.mark, relation)?;
        self.registry
            .relations()
            .mark_materialized_once(relation.relation());
        Ok(())
    }

    /// Mark the one hygienic position that identifies a closed residual's
    /// construction row. Carry acts propagate this positive fact.
    pub(crate) fn mark_residual_row_token(&self, port: PortId) -> Result<()> {
        self.registry.relations().check_open()?;
        self.registry.relations().mark_residual_row_token(port);
        Ok(())
    }

    pub(crate) fn residual_row_token(&self, port: PortId) -> Option<PortId> {
        self.registry.relations().residual_row_token(port)
    }

    pub(crate) fn mark_residual_capture_value(&self, port: PortId) -> Result<()> {
        self.registry.relations().check_open()?;
        self.registry.relations().mark_residual_capture_value(port);
        Ok(())
    }

    pub(crate) fn is_residual_capture_value(&self, port: PortId) -> bool {
        self.registry.relations().is_residual_capture_value(port)
    }

    pub(crate) fn residual_capture_value(&self, port: PortId) -> Option<PortId> {
        self.registry.relations().residual_capture_value(port)
    }

    /// The caller actual this hygienic position carries through a
    /// parameterized fixpoint, when it is such a support position.
    pub(crate) fn frontier_actual(&self, port: PortId) -> Option<PortId> {
        self.registry.relations().frontier_actual(port)
    }

    /// This compilation's epoch, for the test that proves two compilations
    /// never share one.
    #[cfg(test)]
    pub(super) fn epoch_for_test(&self) -> BuilderMark {
        self.mark
    }

    /// THE ONE ENTRANCE.
    ///
    /// The caller states the operation. The birth, the boundary kind, the
    /// owner disposition, the addressing, and the value facts are derived
    /// here from the operation's output law — a caller has no spelling for
    /// any of them.
    pub fn derive(&self, form: RelForm<'_>) -> Result<SemanticRelation> {
        // BEFORE ANYTHING IS MINTED. A sealed compilation refuses at the
        // entrance, so a rejected derivation leaves no scope, no column, no
        // value and no lineage edge behind it.
        self.registry.relations().check_open()?;
        let inputs: Vec<_> = inputs_of(&form).into_iter().cloned().collect();
        for input in &inputs {
            check_mark(self.mark, input)?;
        }
        let law = law_of(&form);
        // THE BOUNDARY IS THE FORM'S, judged once and in force for exactly
        // this derivation. A nested derive inside an arm judges its own
        // form and restores this one on the way out.
        let prior = self.boundary.replace(output_boundary(&form));
        let result = self.execute(&form, law);
        self.boundary.set(prior);
        let result = result?;
        if !inputs
            .iter()
            .any(|input| input.relation() == result.relation())
        {
            self.registry
                .relations()
                .record_inputs(result.relation(), inputs.iter().copied());
            // THE FORM SAYS WHETHER ITS ROWS ARE ITS OPERAND'S. Recorded
            // here, by the act that knows the form, so that no later reader
            // has to infer from carried positions whether a relation
            // republishes its operand or combines it with something else.
            if republishes(&form) {
                self.registry
                    .relations()
                    .record_republication(result.relation());
            }
            if matches!(form, RelForm::Join(_)) {
                self.registry.relations().record_join(result.relation());
            }
        }
        let dependencies = self.dependencies_of(&form);
        if !dependencies.is_empty() {
            self.registry
                .relations()
                .record_dependencies(result.relation(), dependencies);
        }
        // CORRELATION SUPPORT IS JUDGED HERE, under the form's own law, from
        // what its operands owe — and refused here when the form cannot keep
        // an owed occurrence readable. A form that answers with one of its
        // own operands changed nothing that relation owes.
        if !inputs
            .iter()
            .any(|input| input.relation() == result.relation())
        {
            self.registry
                .relations()
                .record_support_law(result.relation(), support_law_of(&form));
            self.judge_support(result)?;
        }
        if let Some(storage) = self.storage_of(&form) {
            self.registry
                .relations()
                .record_storage(result.relation(), storage);
        }
        if let Some(entity) = self.entity_of(&form, &inputs) {
            self.registry
                .relations()
                .record_entity(result.relation(), entity);
        }
        let read_source = self.read_source_of(&form, result);
        if let Some(source) = read_source {
            self.registry
                .relations()
                .record_read_source(result.relation(), source);
        }
        if !inputs
            .iter()
            .any(|input| input.relation() == result.relation())
        {
            let marks: Vec<_> = inputs
                .iter()
                .flat_map(|input| self.registry.relations().mutation_marks(input.relation()))
                .collect();
            self.registry
                .relations()
                .record_mutation_marks(result.relation(), marks);
            if inputs
                .iter()
                .any(|input| self.registry.relations().is_row_bounded(input.relation()))
            {
                self.registry
                    .relations()
                    .mark_row_bounded(result.relation());
            }
        }
        let plan_role = match &form {
            RelForm::Scratch(_) => Some(super::store::PlanRole::Scratch),
            RelForm::Instantiate(InstanceSpec {
                kind: DefinitionKind::HigherOrder(_),
                ..
            }) => Some(super::store::PlanRole::HigherOrder),
            _ if inputs.len() == 1 => self.registry.relations().plan_role(inputs[0].relation()),
            _ => None,
        };
        if let Some(role) = plan_role {
            self.registry
                .relations()
                .record_plan_role(result.relation(), role);
        }
        Ok(result)
    }

    /// THE ONE ENTRANCE FOR A SET STEP.
    ///
    /// The operator IS the operation: `;` corresponds, `||` aligns by
    /// ordinal, `|;|` aligns by name over an agreed set of names, and `-`
    /// subtracts. The judgment from operator to form lives here and has no
    /// wildcard arm, so a fifth operator does not compile until someone
    /// says what it does to outputs.
    ///
    /// A minus is not a set with a subtracting mode. Its right operand
    /// contributes no rows and therefore no positions — it reaches the
    /// anti-match evidence and stops there — so it takes its own form
    /// rather than a fourth entry in the alignment vocabulary.
    ///
    /// Answers with the operator and the result as ONE value: there is no
    /// road that hands back a set result on its own for a caller to pair
    /// with whichever operator it happens to be holding.
    /// WHAT A FORM OWES BESIDE ITS HEADING for a predicate standing right
    /// above it — a caller pattern's constrained positions.
    ///
    /// A support position an operand still owes crosses a BOUNDARY — an
    /// alias, a stage, an emission wrap, a reordering, a renaming — because
    /// each publishes the operand's own dimensions. Every other form STATES a
    /// heading, and what stood under it is spent there. A `WITH` binding pays
    /// its own debts: it is a complete statement, so the operation that reads
    /// a support position stands inside it.
    ///
    /// This is NOT correlation support, which is owed until the interior
    /// boundary and judged fail-closed by [`support_law_of`]: a form that
    /// spends a constraint position here still carries a correlation's.
    ///
    /// Total over the vocabulary. A form added without an answer here would
    /// silently owe nothing.
    fn dependencies_of(&self, form: &RelForm<'_>) -> Vec<PortId> {
        let carried =
            |input: &SemanticRelation| self.registry.relations().dependencies(input.relation());
        match form {
            // AN ACCESS STATES ITS COMPLETE SUPPORT. A caller pattern's
            // constraint positions are its own; what stood under it is spent.
            RelForm::Access(spec) => spec.dependencies.to_vec(),
            RelForm::Join(spec) => [&spec.left, &spec.right]
                .into_iter()
                .flat_map(carried)
                .collect(),
            // A PROJECTION SPENDS WHAT STOOD UNDER IT. The heading it
            // publishes is the whole answer; a position its operand needed
            // for a predicate already applied is not this relation's to owe.
            RelForm::Project(_) => Vec::new(),
            // AN EMBED KEEPS THE OPERAND WHOLE and adds. What the operand
            // owed it still owes.
            RelForm::Embed(spec) => carried(&spec.input),
            RelForm::Export(ExportSpec {
                why: ExportWhy::Cte { .. },
                ..
            }) => Vec::new(),
            RelForm::Export(spec) => carried(&spec.input),
            RelForm::Order(input) => carried(input),
            RelForm::Rename(spec) => carried(&spec.input),
            RelForm::Reposition(spec) => carried(&spec.input),
            RelForm::Source(_)
            | RelForm::Anonymous(_)
            | RelForm::Opaque
            | RelForm::ErBoundary(_)
            | RelForm::ProjectOut(_)
            | RelForm::Cover(_)
            | RelForm::Group(_)
            | RelForm::Set(_)
            | RelForm::Minus(_)
            | RelForm::Witness(_)
            | RelForm::SignedWitness(_)
            | RelForm::Instantiate(_)
            | RelForm::PlanRead(_)
            | RelForm::Destructure(_)
            | RelForm::Drill(_)
            | RelForm::Narrow(_)
            | RelForm::Interior(_)
            | RelForm::Meta(_)
            | RelForm::Scratch(_) => Vec::new(),
        }
    }

    /// The physical object a form's rows LIVE in, where one exists.
    ///
    /// Storage is not occurrence: a shared CTE or scratch keeps one
    /// `StorageId` while every read of it is a distinct relation.
    fn storage_of(&self, form: &RelForm<'_>) -> Option<StorageId> {
        match form {
            RelForm::Source(spec) => match spec.origin {
                SourceOrigin::Catalog { entity } | SourceOrigin::TableValued { entity } => Some(
                    self.registry
                        .relations()
                        .storage_for_entity(entity, self.registry),
                ),
            },
            RelForm::Instantiate(spec) => Some(
                self.registry
                    .relations()
                    .storage_for_definition(spec.template.relation()),
            ),
            RelForm::PlanRead(spec) => Some(
                self.registry
                    .relations()
                    .storage(spec.template.relation())
                    .unwrap_or_else(|| {
                        self.registry
                            .relations()
                            .storage_for_definition(spec.template.relation())
                    }),
            ),
            RelForm::Anonymous(_)
            | RelForm::Opaque
            | RelForm::Order(_)
            | RelForm::Export(_)
            | RelForm::Access(_)
            | RelForm::Project(_)
            | RelForm::ErBoundary(_)
            | RelForm::Embed(_)
            | RelForm::Rename(_)
            | RelForm::Reposition(_)
            | RelForm::ProjectOut(_)
            | RelForm::Cover(_)
            | RelForm::Group(_)
            | RelForm::Join(_)
            | RelForm::Set(_)
            | RelForm::Minus(_)
            | RelForm::Witness(_)
            | RelForm::SignedWitness(_)
            | RelForm::Destructure(_)
            | RelForm::Drill(_)
            | RelForm::Narrow(_)
            | RelForm::Interior(_)
            | RelForm::Meta(_)
            | RelForm::Scratch(_) => None,
        }
    }

    /// The catalog entity a form's rows come from, where they come from one.
    ///
    /// A single-operand form carries its operand's entity: a filter over
    /// `users` is still `users`. A form standing on two relations, or on
    /// none, is nobody's entity.
    fn entity_of(
        &self,
        form: &RelForm<'_>,
        inputs: &[SemanticRelation],
    ) -> Option<crate::names::EntityId> {
        let carried = || {
            let [only] = inputs else {
                return None;
            };
            self.registry.relations().entity(only.relation())
        };
        match form {
            RelForm::Source(spec) => match spec.origin {
                SourceOrigin::Catalog { entity } | SourceOrigin::TableValued { entity } => {
                    Some(entity)
                }
            },
            RelForm::Instantiate(_) | RelForm::PlanRead(_) | RelForm::Scratch(_) => None,
            RelForm::Anonymous(_)
            | RelForm::Opaque
            | RelForm::Order(_)
            | RelForm::Export(_)
            | RelForm::Access(_)
            | RelForm::Project(_)
            | RelForm::ErBoundary(_)
            | RelForm::Embed(_)
            | RelForm::Rename(_)
            | RelForm::Reposition(_)
            | RelForm::ProjectOut(_)
            | RelForm::Cover(_)
            | RelForm::Group(_)
            | RelForm::Join(_)
            | RelForm::Set(_)
            | RelForm::Minus(_)
            | RelForm::Witness(_)
            | RelForm::SignedWitness(_)
            | RelForm::Destructure(_)
            | RelForm::Drill(_)
            | RelForm::Narrow(_)
            | RelForm::Interior(_)
            | RelForm::Meta(_) => carried(),
        }
    }

    /// THE READ A RELATION STANDS ON.
    ///
    /// A pattern's own construction — an access, an answering-name export —
    /// descends to exactly one read, and the FROM entry it emits is that
    /// read. Recorded where the descent is known rather than recovered from a
    /// scope kind. Every other form COMPUTES rows, and what it computes is
    /// not the relation a FROM entry names.
    fn read_source_of(
        &self,
        form: &RelForm<'_>,
        result: SemanticRelation,
    ) -> Option<SemanticRelation> {
        match form {
            RelForm::Source(_)
            | RelForm::Anonymous(_)
            | RelForm::Opaque
            | RelForm::Instantiate(_)
            | RelForm::PlanRead(_)
            | RelForm::Scratch(_) => Some(result),
            // A `WITH` BINDING IS A READ. Whatever computed its rows, a
            // reader stands on the binding itself: that is the name the FROM
            // entry writes and the heading the reader addresses.
            RelForm::Export(ExportSpec {
                why: ExportWhy::Cte { .. },
                ..
            }) => Some(result),
            RelForm::Access(AccessSpec { input, .. })
            | RelForm::Export(ExportSpec { input, .. }) => {
                self.registry.relations().read_source(input.relation())
            }
            RelForm::Order(_)
            | RelForm::Project(_)
            | RelForm::ErBoundary(_)
            | RelForm::Embed(_)
            | RelForm::Rename(_)
            | RelForm::Reposition(_)
            | RelForm::ProjectOut(_)
            | RelForm::Cover(_)
            | RelForm::Group(_)
            | RelForm::Join(_)
            | RelForm::Set(_)
            | RelForm::Minus(_)
            | RelForm::Witness(_)
            | RelForm::SignedWitness(_)
            | RelForm::Destructure(_)
            | RelForm::Drill(_)
            | RelForm::Narrow(_)
            | RelForm::Interior(_)
            | RelForm::Meta(_) => None,
        }
    }

    pub fn set_step(
        &self,
        operator: crate::pipeline::asts::core::SetOperator,
        arms: &[SemanticRelation],
    ) -> Result<super::set::SetStep> {
        use crate::pipeline::asts::core::SetOperator;
        let alignment = match operator {
            SetOperator::UnionCorresponding => SetAlignment::Corresponding,
            SetOperator::UnionAllPositional => SetAlignment::Positional,
            SetOperator::SmartUnionAll => SetAlignment::Smart,
            SetOperator::MinusCorresponding => {
                let [left, right] = arms else {
                    return Err(Internal::invariant(
                        "set",
                        "a minus has exactly two operands",
                    ));
                };
                let result = self.derive(RelForm::Minus(MinusSpec {
                    left: *left,
                    right: *right,
                }))?;
                return Ok(super::set::SetStep::of(operator, result));
            }
        };
        let arms: Vec<SetArm> = arms
            .iter()
            .map(|relation| SetArm {
                relation: *relation,
                correlated: false,
            })
            .collect();
        let result = self.derive(RelForm::Set(SetSpec {
            alignment,
            arms: &arms,
        }))?;
        Ok(super::set::SetStep::of(operator, result))
    }

    /// PAIR ONE PRODUCING STEP WITH WHAT IT PUBLISHES.
    ///
    /// PRIVATE. The exact operation family is the public road; this is the
    /// one place a derived relation and the form it was derived for meet,
    /// and neither half arrives from a caller on its own.
    fn produced_step<P>(
        &self,
        form: crate::pipeline::asts::core::Continuation<P>,
        operation: RelForm<'_>,
    ) -> Result<crate::pipeline::asts::core::Step<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let result = self.derive(operation)?;
        Ok(crate::pipeline::asts::core::Step::derived(
            &self.construction,
            form,
            result,
        ))
    }

    /// A GROUND READ OF A RELATION THIS COMPILATION ALREADY BUILT, AND THE
    /// ACCESS ITS OWN PARENS ASKED FOR.
    ///
    /// The mention is spent, so the read names nothing: it continues the
    /// relation it is given, and the access standing on it continues that.
    /// Both nodes are paired here, in one act.
    pub(crate) fn ground_read<P>(
        &self,
        access: crate::pipeline::asts::core::Access<P>,
        outer: bool,
        of: SemanticRelation,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation, Mention = ()>,
    {
        let head = self.reading(ReadHead::Ground {
            outer,
            published: of,
        })?;
        self.read_asking(crate::pipeline::asts::core::Chain::ground(head), access)
    }

    /// THE ACCESS A READ'S OWN PARENS ASKED FOR.
    ///
    /// A read's resolution already answered what its parens select — the
    /// interface the mention or the application resolved to IS that answer
    /// — so the access standing on it publishes what the read publishes,
    /// RESTATED at the step. Nothing is derived here and nothing is chosen:
    /// there is no argument for a relation, and the one the step gets is
    /// the head's own.
    ///
    /// REFUSES over anything but a bare read. An access standing on a step
    /// asks of that step's result and publishes a heading of its own; that
    /// one is derived, through [`StepOp::Access`].
    pub(crate) fn read_asking<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        asks: crate::pipeline::asts::core::Access<P>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        if !chain.continuations().is_empty() {
            return Err(replacement_error(
                "an access asked of a step's result is not a read's own parens",
            ));
        }
        let published = chain.semantic_relation();
        check_mark(self.mark, &published)?;
        let step = crate::pipeline::asts::core::Step::derived(
            &self.construction,
            crate::pipeline::asts::core::Continuation::Access {
                access: asks,
                named: P::no_stage_name(),
            },
            published,
        );
        Ok(chain.then_derived(&self.construction, step))
    }

    /// EXPORT EVERY NODE OF A TAIL UNDER AN AUTHORED ANSWERING NAME.
    ///
    /// A rename REPLACES an answer, so each node from `from` on that
    /// answers to the old name publishes an export of what IT published.
    /// Every export is derived from that node's own relation and written
    /// back at that node — the form never moves to another prefix, and the
    /// caller holds neither relation. A node answering to something else
    /// is left exactly as it is.
    ///
    /// `interior` rebuilds an operand standing INSIDE a node — a member's
    /// right arm carries references that answer to the old name too. It
    /// cannot change what the node publishes: the same continuation kind
    /// comes back or the rebuild refuses.
    pub(crate) fn realias_tail<P>(
        &self,
        mut chain: crate::pipeline::asts::core::Chain<P>,
        from: usize,
        old: crate::names::Sym,
        answer: crate::names::Spelling,
        mut interior: impl FnMut(
            crate::pipeline::asts::core::Continuation<P>,
        ) -> Result<crate::pipeline::asts::core::Continuation<P>>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        use crate::pipeline::asts::core::Continuation;
        for at in from..chain.continuations().len() {
            let standing = &chain.continuations()[at];
            let rebuilt = match standing.form() {
                Continuation::Member { .. } => {
                    let was = std::mem::discriminant(standing.form());
                    let form = interior(standing.form().clone())?;
                    if std::mem::discriminant(&form) != was {
                        return Err(Internal::invariant(
                            "semantic relation",
                            "an interior rewrite changed which continuation a step is",
                        ));
                    }
                    Some(form)
                }
                Continuation::Restrict { .. } => None,
                _ => continue,
            };
            let input = *standing.result();
            let result = self.realias(input, old, answer)?;
            chain.restate_step(&self.construction, at, rebuilt, result);
        }
        Ok(chain)
    }

    /// The same act at the head, over a head payload the caller rebuilt in
    /// its interior.
    pub(crate) fn realias_head<P>(
        &self,
        chain: &mut crate::pipeline::asts::core::Chain<P>,
        form: Option<crate::pipeline::asts::core::GroundForm<P>>,
        old: crate::names::Sym,
        answer: crate::names::Spelling,
    ) -> Result<()>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let input = *chain.head().result();
        let result = self.realias(input, old, answer)?;
        chain.restate_head(&self.construction, form, result);
        Ok(())
    }

    fn realias(
        &self,
        input: SemanticRelation,
        old: crate::names::Sym,
        answer: crate::names::Spelling,
    ) -> Result<Option<SemanticRelation>> {
        if self.registry.answers_to(input.scope()) != Some(old) {
            return Ok(None);
        }
        Ok(Some(self.derive(RelForm::Export(ExportSpec {
            input,
            why: ExportWhy::Alias { answer },
        }))?))
    }

    /// A BAG STEP AND THE SET RESULT THIS AUTHORITY DERIVED.
    ///
    /// The relation arrives inside a [`super::set::SetStep`], which only
    /// [`SemanticBuilder::set_step`] produces — so there is no relation
    /// here that a caller chose, and the operator behind it is the one the
    /// alignment act was performed under. The operator is read back OUT of
    /// that record rather than taken beside it, so the step's spelling and
    /// the alignment the arms were merged under are one fact.
    pub(crate) fn bag<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        step: super::set::SetStep,
        arm: crate::pipeline::asts::core::Chain<P>,
        correlation: P::Corr,
    ) -> crate::pipeline::asts::core::Chain<P>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let form = crate::pipeline::asts::core::Continuation::BagOp {
            operator: step.operator(),
            arm,
            correlation,
        };
        chain.then_derived(
            &self.construction,
            crate::pipeline::asts::core::Step::derived(&self.construction, form, step.result()),
        )
    }

    /// RESTATE A HEAD'S PAYLOAD, KEEPING WHAT IT PUBLISHES.
    ///
    /// A snapshot replaces a catalog read with the rows it read: the
    /// payload changes kind and the relation does not — the same heading,
    /// the same positions, the same answers. The relation is not an
    /// argument here, so there is nothing to mispair; the authority is the
    /// road because replacing what a bound node holds is its act.
    pub(crate) fn restate_payload<P>(
        &self,
        chain: &mut crate::pipeline::asts::core::Chain<P>,
        form: crate::pipeline::asts::core::GroundForm<P>,
    ) where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        chain.restate_head(&self.construction, Some(form), None);
    }

    /// DERIVE, AND HAND EACH PUBLISHED PORT TO THE POSITION THAT PUBLISHES
    /// AT IT.
    ///
    /// ONE ACT. The operation decides the interface and the positions are
    /// built over the ports that act produced, in the order it produced
    /// them — so there is no moment at which a caller holds a loose port
    /// to pick an item for, and no zip of two lists that are the same
    /// length by luck rather than by law.
    ///
    /// `at` receives the construction token, which is why a BOUND
    /// publication item can be built here and nowhere else. A position
    /// with no port left REFUSES: it published nothing this operation
    /// made.
    pub(crate) fn publishing<T, O>(
        &self,
        operation: RelForm<'_>,
        positions: Vec<T>,
        mut at: impl FnMut(&SemanticConstruction, T, PortId) -> Result<O>,
    ) -> Result<(SemanticRelation, Vec<O>)> {
        let relation = self.derive(operation)?;
        let ports = self.interface(&relation)?.ports().to_vec();
        if ports.len() < positions.len() {
            return Err(Internal::invariant(
                "publication",
                "a publication position stands where the derived interface has none",
            ));
        }
        let built = positions
            .into_iter()
            .zip(ports)
            .map(|(position, port)| at(&self.construction, position, port))
            .collect::<Result<Vec<_>>>()?;
        Ok((relation, built))
    }

    /// BIND A PENDING OPERATION: ONE DESCRIPTION IN, ONE NODE OUT.
    ///
    /// The caller states the operation and nothing else. This match is the
    /// one place that turns a stated operation into the exact form, the
    /// output law, the ports and the stored payload — four facts derived
    /// from ONE description rather than four a caller could choose apart.
    /// Adding a semantic operation adds an arm here; there is no arm for
    /// "some other operation the caller assembled".
    ///
    /// What comes back is the NODE and the ports it publishes, in interface
    /// order — the two things every resolver answers with, produced by the
    /// same act so no caller re-reads them apart.
    pub(crate) fn bind(&self, pending: super::pending::Pending) -> Result<BoundStep> {
        use super::pending::Pending;
        match pending {
            Pending::Publication {
                input,
                publishes,
                why,
                positions,
            } => self.bind_publication(input, publishes, why, positions),
            Pending::CallerPattern(row) => self.bind_caller_pattern(row),
            Pending::Access { input, access } => self.bind_access(input, access),
            Pending::ProjectOut {
                input,
                selector,
                removed,
            } => self.bind_project_out(input, selector, removed),
            Pending::Rename { input, renames } => self.bind_rename(input, renames),
            Pending::Reposition { input, moves } => self.bind_reposition(input, moves),
            Pending::Witness { input, polarity } => self.bind_witness(input, polarity),
            Pending::SignedWitness { input } => self.bound(
                RelForm::SignedWitness(SignedWitnessSpec { input }),
                structural(crate::pipeline::asts::resolved::StructuralForm::SignedWitness),
            ),
            Pending::Meta { input } => self.bound(
                RelForm::Meta(MetaSpec { subject: input }),
                structural(crate::pipeline::asts::resolved::StructuralForm::Meta),
            ),
            Pending::Drill { input, drill } => self.bind_drill(input, drill),
            Pending::Narrow {
                input,
                nest,
                pattern,
            } => self.bind_narrow(input, nest, pattern),
            Pending::Destructure {
                input,
                source,
                mode,
                pattern,
            } => self.bind_destructure(input, source, mode, pattern),
            Pending::MapCover {
                input,
                selector,
                guard,
                cells,
            } => self.bind_map_cover(input, selector, guard, cells),
            Pending::EmbedMapCover {
                input,
                naming,
                selector,
                cells,
            } => self.bind_embed_map_cover(input, naming, selector, cells),
            Pending::Transform {
                input,
                items,
                guard,
            } => self.bind_transform(input, items, guard),
            Pending::Group { input, keys, shape } => self.bind_group(input, keys, shape),
            // THE ORDERING IS A PIPE FORM (fundamentals): it republishes
            // its operand's whole heading through the stage export, so the
            // one boundary act dequalifies what it publishes. Re-ordering
            // rows is the payload; the interface crosses the stage.
            // The bound the ordering consumed is the SAME act: the stage
            // publishes the chosen members, and the by-position fact is
            // stamped on that publication here, where the act is derived,
            // so no later reader goes looking for a LIMIT.
            Pending::Ordering {
                input,
                specs,
                bound,
            } => {
                let bounded = bound.is_some();
                // A BOUNDED ORDERING OVER A CORRELATED RELATION is evaluated
                // per correlation class, at the interior's top where the
                // hoisting realization ranks the class: the occurrences its
                // specs read are owed from here like the correlation's own,
                // to the boundary. Stated on the operand, so the stage
                // translates them as it does every position it republishes.
                // The ranking witness that reads them is a refinement of the
                // body the boundary was judged over, so the body's obligation
                // is left as the boundary recorded it rather than spent.
                if bounded
                    && !self
                        .registry
                        .relations()
                        .support_owed(input.relation())
                        .is_empty()
                {
                    let read = super::support::ports_read(specs.iter().map(|spec| &spec.column));
                    self.registry
                        .relations()
                        .mark_correlation(input.relation(), &read);
                    self.judge_support(input)?;
                }
                let (step, output) = self.bound(
                    RelForm::Export(ExportSpec {
                        input,
                        why: ExportWhy::Stage,
                    }),
                    structural(crate::pipeline::asts::resolved::StructuralForm::Ordering {
                        specs,
                        bound,
                    }),
                )?;
                if bounded {
                    self.mark_row_bounded(step.result())?;
                }
                Ok((step, output))
            }
            Pending::Requalify { input, access } => self.bound(
                RelForm::Order(input),
                crate::pipeline::asts::core::Continuation::Access { access, named: () },
            ),
            Pending::CrossingCarrierInjection {
                replaces,
                carriers,
                items,
                stored,
            } => self.bind_crossing_carrier_injection(replaces, carriers, items, stored),
            Pending::FrontierActualInjection {
                replaces,
                support,
                items,
                stored,
            } => self.bind_frontier_actual_injection(
                replaces,
                support.carriers().to_vec(),
                items,
                stored,
            ),
            Pending::WindowWitness {
                input,
                partition,
                ordering,
            } => self.bind_window_witness(input, partition, ordering),
        }
    }

    /// THE END OF EVERY ARM: derive the operation, pair the stored payload
    /// with the relation it produced, and answer with the node and its
    /// ports. The scaffold every family used to copy, written once.
    fn bound(
        &self,
        of: RelForm<'_>,
        form: crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Resolved>,
    ) -> Result<BoundStep> {
        let result = self.derive(of)?;
        self.paired(form, result)
    }

    /// Pair a finished payload with the relation its own arm derived.
    fn paired(
        &self,
        form: crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Resolved>,
        result: SemanticRelation,
    ) -> Result<BoundStep> {
        let output = self.interface(&result)?.ports().to_vec();
        Ok((
            crate::pipeline::asts::core::Step::derived(&self.construction, form, result),
            output,
        ))
    }

    fn bind_publication(
        &self,
        input: SemanticRelation,
        publishes: super::pending::Publishes,
        why: super::form::ProjectWhy,
        positions: Vec<super::pending::Position>,
    ) -> Result<BoundStep> {
        use super::pending::Publishes;
        use crate::pipeline::asts::core::{Continuation, OutItem, PipeOp};

        // THE SLOTS ARE READ OFF THE POSITIONS, so the description that
        // decides the interface is the description the tree stores. An EDIT
        // supplies the operand's own heading as its leading run, so the
        // engine's expansion of the author's glob restates a position the
        // edit already carries and mints no slot of its own.
        let mut slots = Vec::with_capacity(positions.len());
        let mut publishing = Vec::with_capacity(positions.len());
        // THE POSITIONS THE ENCLOSING JOIN COMPUTES, by stated position,
        // each with the interior occurrences its value reads — what the
        // relation owes until the boundary.
        let mut deferring: Vec<(usize, Vec<PortId>)> = Vec::new();
        for (index, position) in positions.iter().enumerate() {
            let Some(value) = position.value() else {
                continue;
            };
            publishing.push(index);
            if publishes == Publishes::Edited && position.is_engine_expansion() {
                continue;
            }
            let slot = match position.evaluation() {
                super::pending::Evaluation::Here => {
                    self.publication_slot(value, position.naming(), &input)?
                }
                // A VALUE THAT READS THE ENCLOSING ROW is a new value of
                // its own — never a carry of the enclosing occurrence into
                // this heading. In place, the target evaluates it beside
                // the operand's row; at a hoisted interior the enclosing
                // join computes it, and the interior carries the position
                // unread until then — a row past that join is one no join
                // evaluating the interior can read.
                super::pending::Evaluation::Enclosing {
                    correlations,
                    reach,
                } => {
                    let (slot, reads) = self.enclosing_slot(value, position.naming(), &input)?;
                    if *correlations == crate::pipeline::resolver::Correlations::Hoisted {
                        if *reach == super::pending::EnclosingReach::PastTheJoin {
                            return Err(super::enclosing_position_refusal(
                                "a value reading a row two interiors out",
                            ));
                        }
                        deferring.push((index, reads));
                    }
                    slot
                }
            };
            let slot = match slot {
                ProjectSlot::Carried {
                    source,
                    naming: Naming::Inherited,
                } if !position.is_engine_expansion()
                    && self.registry.addressing(source.column()) == Addressing::Hygienic =>
                {
                    ProjectSlot::Carried {
                        source,
                        naming: Naming::Anonymous,
                    }
                }
                slot => slot,
            };
            slots.push(slot);
        }
        if slots.is_empty() {
            return Err(DelightQLError::from(Constraint::General {
                message: "Projection matched no columns - would create empty table".to_string(),
            }));
        }
        let spec = ProjectSpec {
            input,
            why,
            slots: &slots,
        };
        let result = self.derive(match publishes {
            Publishes::Anew => RelForm::Project(spec),
            Publishes::Edited => RelForm::Embed(spec),
        })?;
        let ports = self.interface(&result)?.ports().to_vec();
        let mut outputs = vec![None; positions.len()];
        for (index, port) in publishing.into_iter().zip(ports) {
            outputs[index] = Some(port);
        }
        if !deferring.is_empty() {
            // THE DEFERRAL AND THE SUPPORT ARE THE PUBLICATION'S OWN RECORD,
            // written in the act that minted the position: the position is
            // computed at the boundary, and the interior occurrences its
            // value reads stay readable there. Judged at once, as the
            // correlation act judges its own marks.
            let store = self.registry.relations();
            for (index, reads) in &deferring {
                let port = outputs[*index].expect("one stated value publishes one semantic port");
                store.mark_deferred(result.relation(), port);
                store.mark_correlation(result.relation(), reads);
            }
            self.judge_support(result)?;
        }
        let items: Vec<OutItem<_>> = positions
            .into_iter()
            .zip(outputs)
            .map(|(position, output)| published_position(&self.construction, position, output))
            .collect();
        let Some(items) = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items) else {
            return Err(DelightQLError::from(Constraint::General {
                message: "Projection matched no columns - would create empty table".to_string(),
            }));
        };
        self.paired(
            Continuation::Pipe {
                operator: match publishes {
                    Publishes::Anew => PipeOp::Project(items),
                    Publishes::Edited => PipeOp::Embed(items),
                },
                named: (),
            },
            result,
        )
    }

    fn bind_caller_pattern(&self, row: super::pending::SlotRow) -> Result<BoundStep> {
        use super::pending::PatternPosition;
        use crate::pipeline::asts::core::{Access, Continuation, Slot};
        let (input, answers_to, positions, carriers) = row.into_parts();

        // THE READ'S SPEC IS THE POSITIONS. What each written slot does
        // decides whether it is a dimension the read asks for or a column
        // the read depends on; a caller has no second list to disagree with
        // the one it wrote.
        let mut slots = Vec::with_capacity(positions.len());
        let mut dependencies = carriers;
        for position in &positions {
            match position {
                PatternPosition::Binds { source, naming, .. } => slots.push(ProjectSlot::Carried {
                    source: *source,
                    naming: *naming,
                }),
                PatternPosition::Publishes { source, .. } => slots.push(ProjectSlot::Carried {
                    source: *source,
                    naming: Naming::Inherited,
                }),
                PatternPosition::Constrains { source, .. } => dependencies.push(*source),
                PatternPosition::Skips { .. } => {}
            }
        }
        // TWO ACTS, ONE OPERATION. The dimensions are asked of the operand
        // and the answer is exported under the name the call site wrote;
        // the export stands on what the ask published, so neither half is a
        // relation a caller chose.
        let selected = self.derive(RelForm::Access(AccessSpec {
            input,
            shape: AccessShape::Named,
            slots: &slots,
            dependencies: &dependencies,
        }))?;
        let result = self.derive(RelForm::Export(ExportSpec {
            input: selected,
            why: ExportWhy::Bound { answer: answers_to },
        }))?;
        // ONE PORT PER SLOT, in order, BY LAW: an export is one-to-one over
        // what the ask published, and the ask published one position per
        // slot the positions asked for. So the run of minted ports and the
        // run of publishing positions are the same run, and there is
        // nothing here to check at runtime.
        let mut minted = self.interface(&result)?.ports().to_vec().into_iter();
        let mut occurrences = Vec::with_capacity(positions.len());
        for position in positions {
            occurrences.push(match position {
                PatternPosition::Binds {
                    qualified, reuses, ..
                } => {
                    let Some(column) = minted.next() else {
                        unreachable!("the ask published one position per slot")
                    };
                    // THE BIND ACT WRITES THE REUSE EDGE. Resolution decided
                    // the exactly-one live bare port this spelling reuses;
                    // the record pairs it with the port minted here, and the
                    // join that owns the left port consumes it.
                    if let Some(left) = reuses {
                        self.registry.relations().record_reuse(column, left)?;
                    }
                    if qualified {
                        Slot::Reuse(crate::pipeline::asts::core::NamedReference(
                            crate::pipeline::asts::core::ColumnOccurrence::engine_qualified(column),
                        ))
                    } else {
                        Slot::Bind(column)
                    }
                }
                PatternPosition::Publishes { stored, .. } => {
                    if minted.next().is_none() {
                        unreachable!("the ask published one position per slot")
                    }
                    stored
                }
                PatternPosition::Constrains { stored, .. } | PatternPosition::Skips { stored } => {
                    stored
                }
            });
        }
        let Some(occurrences) = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(occurrences)
        else {
            return Err(DelightQLError::from(Constraint::General {
                message: "A positional pattern bound no slots".to_string(),
            }));
        };
        self.paired(
            Continuation::Access {
                access: Access::Slots(occurrences),
                named: (),
            },
            result,
        )
    }

    /// Explode an interior relation column into rows. The bound drill is
    /// the one description: the interior column, the selected positions
    /// and the groundings, from which the semantic form is read.
    fn bind_drill(
        &self,
        input: SemanticRelation,
        drill: crate::pipeline::asts::core::operators::BoundDrill,
    ) -> Result<BoundStep> {
        let result = self.derive(RelForm::Drill(DrillSpec {
            input,
            interior_of: drill.column,
            selected: &drill.columns,
            selection: drill.selection,
        }))?;
        self.paired(
            structural(crate::pipeline::asts::resolved::StructuralForm::Drill { drill }),
            result,
        )
    }

    /// The dimensions a read asks of its operand: the ask publishes the
    /// operand's positions, restated as a fresh occurrence, and the
    /// authored access is what the tree stores.
    fn bind_access(
        &self,
        input: SemanticRelation,
        access: crate::pipeline::asts::resolved::Access,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::Access;
        let shape = match &access {
            Access::All | Access::DequalifyAll => AccessShape::Whole,
            Access::Slots(_) | Access::Dequalify(_) => AccessShape::Named,
            Access::Unasked => AccessShape::Empty,
        };
        let slots: Vec<_> = self
            .interface(&input)?
            .ports()
            .iter()
            .copied()
            .map(|source| ProjectSlot::Carried {
                source,
                naming: Naming::Inherited,
            })
            .collect();
        self.bound(
            RelForm::Access(AccessSpec {
                input,
                shape,
                slots: &slots,
                dependencies: &[],
            }),
            crate::pipeline::asts::core::Continuation::Access { access, named: () },
        )
    }

    /// The heading minus the addressed positions. The stored selector may
    /// keep an unexpanded spread (a docket hold), so the removals ride
    /// beside it as the same resolution act's answer.
    fn bind_project_out(
        &self,
        input: SemanticRelation,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        removed: Vec<PortId>,
    ) -> Result<BoundStep> {
        self.bound(
            RelForm::ProjectOut(ProjectOutSpec {
                input,
                removed: &removed,
            }),
            crate::pipeline::asts::core::Continuation::Pipe {
                operator: crate::pipeline::asts::core::PipeOp::ProjectOut(selector),
                named: (),
            },
        )
    }

    /// The positions are untouched; the names change. The stored specs are
    /// written FROM the renames, so the operation and its payload cannot
    /// name different columns.
    fn bind_rename(&self, input: SemanticRelation, renames: Vec<RenameSlot>) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{
            ColumnOccurrence, NamedReference, Reference, RenameSource,
        };
        let result = self.derive(RelForm::Rename(RenameSpec {
            input,
            why: super::form::ProjectWhy::Stage,
            renames: &renames,
        }))?;
        let specs: Vec<_> = renames
            .into_iter()
            .map(|slot| crate::pipeline::asts::resolved::RenameSpec {
                from: RenameSource::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence::engine(slot.source),
                ))),
                to: slot.to,
            })
            .collect();
        let specs = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(specs)
            .expect("a rename source that matches nothing refused during resolution");
        self.paired(
            crate::pipeline::asts::core::Continuation::Pipe {
                operator: crate::pipeline::asts::core::PipeOp::Rename(specs),
                named: (),
            },
            result,
        )
    }

    /// The names are untouched; the positions change. The layout — which
    /// position every column ends at, and the refusals over a crowded or
    /// out-of-range target — is this arm's arithmetic over the stated
    /// moves, so there is no second list for a caller to hold beside them.
    fn bind_reposition(
        &self,
        input: SemanticRelation,
        moves: Vec<super::pending::Move>,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{ColumnOccurrence, NamedReference, Reference};
        let ports = self.interface(&input)?.ports().to_vec();
        let count = ports.len();
        let mut layout: Vec<Option<PortId>> = vec![None; count];
        let mut moved = Vec::new();
        let mut resolved_moves = Vec::new();
        for spell in moves {
            let Reference::Named(NamedReference(ColumnOccurrence { column, .. })) =
                &spell.reference
            else {
                return Err(DelightQLError::from(Constraint::General {
                    message: "Reposition only supports columns and ordinals".to_string(),
                }));
            };
            let column = *column;
            let source = ports
                .iter()
                .position(|candidate| *candidate == column)
                .ok_or_else(|| {
                    DelightQLError::from(Resolution::General {
                        message: "Reposition column is not in the input".to_string(),
                    })
                })?;
            if moved.contains(&source) {
                return Err(DelightQLError::from(Constraint::General {
                    message: "A column appears multiple times in reposition".to_string(),
                }));
            }
            let target = if spell.position < 0 {
                count as i32 + spell.position
            } else {
                spell.position - 1
            };
            if target < 0 || target >= count as i32 {
                return Err(DelightQLError::from(Constraint::General {
                    message: format!(
                        "Position {} is out of range for {} columns",
                        spell.position, count
                    ),
                }));
            }
            let target = target as usize;
            if layout[target].is_some() {
                return Err(DelightQLError::from(Constraint::General {
                    message: "Multiple columns cannot target the same position".to_string(),
                }));
            }
            layout[target] = Some(column);
            moved.push(source);
            resolved_moves.push(crate::pipeline::asts::resolved::RepositionSpec {
                column: spell.reference,
                position: spell.position,
            });
        }
        let remaining: Vec<_> = ports
            .iter()
            .enumerate()
            .filter(|(index, _)| !moved.contains(index))
            .map(|(_, column)| *column)
            .collect();
        let mut remaining = remaining.into_iter();
        for slot in &mut layout {
            if slot.is_none() {
                *slot = remaining.next();
            }
        }
        let semantic_moves: Vec<_> = layout
            .into_iter()
            .flatten()
            .enumerate()
            .filter_map(|(to, source)| {
                (ports.get(to) != Some(&source)).then_some(RepositionSlot {
                    source,
                    to: to as u32,
                })
            })
            .collect();
        self.bound(
            RelForm::Reposition(RepositionSpec {
                input,
                moves: &semantic_moves,
            }),
            structural(
                crate::pipeline::asts::resolved::StructuralForm::Reposition {
                    moves: resolved_moves,
                },
            ),
        )
    }

    /// Existence reified. One authored polarity decides the output law and
    /// the stored form both.
    fn bind_witness(
        &self,
        input: SemanticRelation,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> Result<BoundStep> {
        let semantic = match polarity {
            crate::pipeline::asts::core::Polarity::Positive => WitnessPolarity::Positive,
            crate::pipeline::asts::core::Polarity::Negative => WitnessPolarity::Negative,
        };
        self.bound(
            RelForm::Witness(WitnessSpec {
                input,
                polarity: semantic,
            }),
            structural(crate::pipeline::asts::resolved::StructuralForm::Witness { polarity }),
        )
    }

    /// Iterate the array a nest carries. The pattern is the one
    /// description: the keys it reads and the names it publishes are
    /// extracted here, in the act that mints their ports, and the stored
    /// pattern is converted over exactly those ports.
    fn bind_narrow(
        &self,
        input: SemanticRelation,
        nest: crate::pipeline::asts::core::ColumnOccurrence,
        pattern: crate::pipeline::asts::unresolved::RecordPattern,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{NamedReference, Reference, TreePattern};
        let declared = TreePattern::Record(pattern);
        let mappings =
            crate::pipeline::resolver::resolving::predicates::extract_key_mappings_from_unresolved_pattern(
                &declared,
            )?;
        let bound: Vec<_> = mappings
            .iter()
            .map(|(_, published)| ProjectSlot::Computed {
                naming: Naming::Authored(self.registry.intern(published, false)),
                shape: crate::names::ValueShape::Unknown,
            })
            .collect();
        let result = self.derive(RelForm::Narrow(NarrowSpec {
            input,
            nest: nest.column,
            bound: &bound,
        }))?;
        let ports = self.interface(&result)?.ports().to_vec();
        let mut columns = std::collections::HashMap::new();
        let mut schema = Vec::with_capacity(mappings.len());
        for ((json_key, published), column) in mappings.into_iter().zip(ports.iter().copied()) {
            columns.insert(
                self.registry
                    .canonical(self.registry.intern(&published, false)),
                column,
            );
            schema.push(crate::pipeline::asts::core::DestructureMapping { json_key, column });
        }
        let converted =
            crate::pipeline::resolver::resolving::predicates::convert_destructure_pattern_to_resolved(
                declared, &columns, self.registry,
            )?;
        let TreePattern::Record(pattern) = converted else {
            unreachable!("a record pattern converts to a record pattern");
        };
        self.paired(
            structural(crate::pipeline::asts::resolved::StructuralForm::Narrow {
                nest: Reference::Named(NamedReference(nest)),
                pattern,
                schema,
            }),
            result,
        )
    }

    /// Read fields out of a document, or iterate and explode rows. The
    /// pattern is the one description, exactly as for the narrowing; the
    /// expansion publishes the operand whole and then the positions the
    /// pattern's keys mint.
    fn bind_destructure(
        &self,
        input: SemanticRelation,
        source: crate::pipeline::asts::resolved::DomainExpression,
        mode: crate::pipeline::asts::core::DestructureMode,
        pattern: crate::pipeline::asts::core::TreePattern<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<BoundStep> {
        let mappings =
            crate::pipeline::resolver::resolving::predicates::extract_key_mappings_from_unresolved_pattern(
                &pattern,
            )?;
        let bound: Vec<_> = mappings
            .iter()
            .map(|(_, column_name)| ProjectSlot::Computed {
                naming: Naming::Authored(self.registry.intern(column_name, false)),
                shape: crate::names::ValueShape::Unknown,
            })
            .collect();
        let input_width = self.interface(&input)?.width();
        let result = self.derive(RelForm::Destructure(DestructureSpec {
            input,
            mode: match mode {
                crate::pipeline::asts::core::DestructureMode::Scalar => DestructureMode::Scalar,
                crate::pipeline::asts::core::DestructureMode::Aggregate => {
                    DestructureMode::Aggregate
                }
            },
            bound: &bound,
        }))?;
        let ports = self.interface(&result)?.ports().to_vec();
        let added = &ports[input_width..];
        let mut columns = std::collections::HashMap::new();
        let mut key_mappings = Vec::new();
        for ((json_key, column_name), column) in mappings.into_iter().zip(added.iter().copied()) {
            let published = self.registry.intern(&column_name, false);
            columns.insert(self.registry.canonical(published), column);
            key_mappings.push(crate::pipeline::asts::core::DestructureMapping { json_key, column });
        }
        let pattern =
            crate::pipeline::resolver::resolving::predicates::convert_destructure_pattern_to_resolved(
                pattern, &columns, self.registry,
            )?;
        self.paired(
            crate::pipeline::asts::core::Continuation::Destructure {
                source: Box::new(source),
                pattern,
                mode,
                schema: key_mappings,
            },
            result,
        )
    }

    /// A callable applied over each covered cell, writing the cell in
    /// place. The applied cells are the one description: the semantic
    /// cover is read off them.
    fn bind_map_cover(
        &self,
        input: SemanticRelation,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        guard: Option<Box<crate::pipeline::asts::resolved::TruthExpression>>,
        cells: Vec<
            crate::pipeline::asts::core::operators::AppliedCell<
                crate::pipeline::asts::core::Resolved,
            >,
        >,
    ) -> Result<BoundStep> {
        let semantic = cells
            .iter()
            .map(|cell| {
                Ok(CoverCell {
                    covered: self.port_in(&input, cell.column)?,
                    naming: Naming::Inherited,
                    // A map cover applies a FUNCTION to the slot, so it
                    // always puts a different value there.
                    writes: true,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.bound(
            RelForm::Cover(CoverSpec {
                input,
                kind: CoverKind::Map,
                cells: &semantic,
            }),
            crate::pipeline::asts::core::Continuation::Pipe {
                operator: crate::pipeline::asts::core::PipeOp::MapCover(
                    crate::pipeline::asts::core::operators::MapCover {
                        callable: (),
                        selector,
                        guard,
                        cells,
                    },
                ),
                named: (),
            },
        )
    }

    /// A callable applied over each covered cell, appended beside the
    /// operand's heading. The authored naming template is expanded here,
    /// over the positions this act appends.
    fn bind_embed_map_cover(
        &self,
        input: SemanticRelation,
        naming: Option<crate::pipeline::asts::core::operators::ColumnAlias>,
        selector: Vec<crate::pipeline::asts::resolved::SelectorItem>,
        cells: Vec<
            crate::pipeline::asts::core::operators::AppliedCell<
                crate::pipeline::asts::core::Resolved,
            >,
        >,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::operators::ColumnAlias;
        let width = self.interface(&input)?.width();
        let mut semantic = Vec::with_capacity(cells.len());
        for (offset, cell) in cells.iter().enumerate() {
            let published = match &naming {
                Some(ColumnAlias::Template(template)) => self.registry.expand_template(
                    cell.column.column(),
                    &template.template,
                    width + offset + 1,
                ),
                Some(ColumnAlias::Literal(name)) => Some(self.registry.intern(name, false)),
                None => None,
            };
            semantic.push(CoverCell {
                covered: self.port_in(&input, cell.column)?,
                naming: published.map_or(Naming::Hygienic, Naming::Authored),
                // An embed cover ADDS a position; the covered slot keeps
                // its own value, so nothing is written over.
                writes: false,
            });
        }
        self.bound(
            RelForm::Cover(CoverSpec {
                input,
                kind: CoverKind::EmbedMap,
                cells: &semantic,
            }),
            crate::pipeline::asts::core::Continuation::Pipe {
                operator: crate::pipeline::asts::core::PipeOp::EmbedMapCover(
                    crate::pipeline::asts::core::operators::EmbedMapCover {
                        callable: (),
                        naming,
                        selector,
                        cells,
                    },
                ),
                named: (),
            },
        )
    }

    /// Authored expressions written over covered cells. Whether a cell
    /// WRITES is read off its expression — a cell handed back its own
    /// column is the same value under the same name — and each item's
    /// position is the operand's own, followed through the construction
    /// record.
    fn bind_transform(
        &self,
        input: SemanticRelation,
        items: Vec<super::pending::TransformItem>,
        guard: Option<Box<crate::pipeline::asts::resolved::TruthExpression>>,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{
            ColumnOccurrence, DomainExpression, NamedReference, Reference,
        };
        let input_ports = self.interface(&input)?.ports().to_vec();
        let mut semantic = Vec::with_capacity(items.len());
        let mut positions = Vec::with_capacity(items.len());
        for item in &items {
            let spelled = match &item.qualifier {
                Some(qualifier) => format!("{qualifier}.{}", item.naming),
                None => item.naming.to_string(),
            };
            let carried = self.port_in(&input, item.covered).map_err(|_| {
                DelightQLError::from(Resolution::Column {
                    column: spelled.clone().to_string(),
                    context: "as a transform target".to_string(),
                })
            })?;
            let position = input_ports
                .iter()
                .position(|candidate| *candidate == carried)
                .ok_or_else(|| {
                    DelightQLError::from(Resolution::Column {
                        column: spelled.to_string(),
                        context: "as a transform target".to_string(),
                    })
                })?;
            // A cover that hands the slot back its own column writes
            // nothing: it is the same value under the same name.
            let writes = !matches!(
                &item.expr,
                DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence { column, .. },
                ))) if *column == item.covered
            );
            positions.push(position);
            semantic.push(CoverCell {
                covered: carried,
                naming: Naming::Inherited,
                writes,
            });
        }
        let result = self.derive(RelForm::Cover(CoverSpec {
            input,
            kind: CoverKind::Transform,
            cells: &semantic,
        }))?;
        let ports = self.interface(&result)?.ports().to_vec();
        let finished: Vec<_> = items
            .into_iter()
            .zip(&positions)
            .map(|(item, position)| {
                crate::pipeline::asts::core::NamedOutItem::published(
                    &self.construction,
                    item.expr,
                    item.naming,
                    item.qualifier,
                    ports[*position],
                )
            })
            .collect();
        self.paired(
            crate::pipeline::asts::core::Continuation::Pipe {
                operator: crate::pipeline::asts::core::PipeOp::Transform {
                    items: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(finished)
                        .expect("one transform item resolves to one item"),
                    guard,
                },
                named: (),
            },
            result,
        )
    }

    /// `%(keys)` / `%(keys ~> reductions)`. The stated events are the one
    /// list: each event contributes its semantic slots and receives its
    /// ports from the same iteration, so there is no port-to-event
    /// correspondence to check after the fact.
    fn bind_group(
        &self,
        input: SemanticRelation,
        keys: Vec<super::pending::Position>,
        shape: super::pending::GroupShape,
    ) -> Result<BoundStep> {
        use super::pending::{GroupShape, Reduction};
        use crate::pipeline::asts::core::{Continuation, PipeOp};
        match shape {
            GroupShape::Distinct => {
                let mut semantic_keys = Vec::new();
                let mut publishing = Vec::new();
                for (position, item) in keys.iter().enumerate() {
                    if item.value().is_some() {
                        semantic_keys.push(self.evaluated_slot(item, &input, "a grouping key")?);
                        publishing.push(position);
                    }
                }
                let result = self.derive(RelForm::Group(GroupSpec {
                    input,
                    kind: GroupKind::Distinct,
                    keys: &semantic_keys,
                    reductions: &[],
                }))?;
                let ports = self.interface(&result)?.ports().to_vec();
                let mut assignments = vec![None; keys.len()];
                for (position, port) in publishing.into_iter().zip(ports.iter().copied()) {
                    assignments[position] = Some(port);
                }
                let finished: Vec<_> = keys
                    .into_iter()
                    .zip(assignments)
                    .map(|(item, port)| published_position(&self.construction, item, port))
                    .collect();
                self.paired(
                    Continuation::Pipe {
                        operator: PipeOp::Group(crate::pipeline::asts::core::GroupSpec::Distinct {
                            keys: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(finished)
                                .expect("the authored distinct keys were nonempty"),
                        }),
                        named: (),
                    },
                    result,
                )
            }
            GroupShape::Reduce {
                mut reductions,
                delegates,
            } => {
                let mut semantic_keys = Vec::new();
                let mut key_publishing = Vec::new();
                for (position, item) in keys.iter().enumerate() {
                    if item.value().is_some() {
                        semantic_keys.push(self.evaluated_slot(item, &input, "a grouping key")?);
                        key_publishing.push(position);
                    }
                }
                // ONE LOOP writes the semantic slots and remembers which
                // event each publishing position belongs to. The ports the
                // derivation mints follow the slots BY LAW, so the same
                // walk hands them back out.
                let mut semantic_reductions = Vec::new();
                let mut reduction_publishing = Vec::new();
                for (position, item) in reductions.iter().enumerate() {
                    match item {
                        Reduction::Pivot(pivot) => {
                            for value in &pivot.values {
                                semantic_reductions.push(ReductionSlot::PivotValue {
                                    naming: Naming::Authored(self.registry.intern(value, false)),
                                });
                                reduction_publishing.push(None);
                            }
                        }
                        Reduction::Out(out) => {
                            let slot = self.evaluated_slot(out, &input, "a reduction")?;
                            let is_group = out.value().is_some_and(|expression| {
                                matches!(
                                    expression,
                                    crate::pipeline::asts::core::DomainExpression::Application(
                                        crate::pipeline::asts::core::FunctionApplication::Enclyph(
                                            crate::pipeline::asts::core::Enclyph::Record(_)
                                                | crate::pipeline::asts::core::Enclyph::EmptyRecord(
                                                    _
                                                )
                                        )
                                    )
                                )
                            });
                            if is_group {
                                semantic_reductions.push(ReductionSlot::Group {
                                    naming: match slot {
                                        ProjectSlot::Carried { naming, .. }
                                        | ProjectSlot::Computed { naming, .. } => naming,
                                    },
                                });
                            } else {
                                semantic_reductions.push(ReductionSlot::Value { slot });
                            }
                            reduction_publishing.push(Some(position));
                        }
                        Reduction::Metadata { naming, .. } => {
                            semantic_reductions.push(ReductionSlot::Group {
                                naming: naming.as_ref().map_or(Naming::Anonymous, |name| {
                                    Naming::Authored(
                                        self.registry.intern(name.as_str(), name.is_stropped()),
                                    )
                                }),
                            });
                            reduction_publishing.push(Some(position));
                        }
                    }
                }
                // A PAYLOAD POSITION THAT IS ALREADY A KEY PUBLISHES
                // NOTHING. `%(country ~> (*) <~ …)` names every column of
                // the row, and `country` among them is the position the
                // group is BY — emitted once, in key position.
                let grouped: Vec<PortId> = semantic_keys
                    .iter()
                    .filter_map(|slot| match slot {
                        ProjectSlot::Carried {
                            source,
                            naming: Naming::Inherited,
                        } => Some(*source),
                        _ => None,
                    })
                    .collect();
                let mut delegate_slots = Vec::new();
                let mut delegate_publishing = Vec::new();
                let mut elided: Vec<Vec<bool>> = delegates
                    .iter()
                    .map(|delegate| vec![false; delegate.payload.len()])
                    .collect();
                for (delegate_index, delegate) in delegates.iter().enumerate() {
                    for (item_index, item) in delegate.payload.iter().enumerate() {
                        if item.value().is_none() {
                            continue;
                        }
                        let slot = self.evaluated_slot(item, &input, "a delegate's payload")?;
                        if matches!(
                            slot,
                            ProjectSlot::Carried {
                                source,
                                naming: Naming::Inherited,
                            } if grouped.contains(&source)
                        ) {
                            elided[delegate_index][item_index] = true;
                            continue;
                        }
                        delegate_slots.push(ReductionSlot::Delegate { slot });
                        delegate_publishing.push((delegate_index, item_index));
                    }
                }
                let mut all_reductions = semantic_reductions;
                all_reductions.extend(delegate_slots);
                let result = self.derive(RelForm::Group(GroupSpec {
                    input,
                    kind: GroupKind::Reduce,
                    keys: &semantic_keys,
                    reductions: &all_reductions,
                }))?;
                let ports = self.interface(&result)?.ports().to_vec();
                let mut port_run = ports.iter().copied();
                let mut take = || {
                    port_run
                        .next()
                        .expect("the group law publishes one port per stated event")
                };
                let mut key_assignments = vec![None; keys.len()];
                for position in key_publishing {
                    key_assignments[position] = Some(take());
                }
                let mut reduction_assignments = vec![None; reductions.len()];
                for event in reduction_publishing {
                    match event {
                        // A pivot's positions occupy the interface and no
                        // stored item stands at any one of them.
                        None => {
                            take();
                        }
                        Some(position) => reduction_assignments[position] = Some(take()),
                    }
                }
                let mut delegate_assignments: Vec<Vec<Option<PortId>>> = delegates
                    .iter()
                    .map(|delegate| vec![None; delegate.payload.len()])
                    .collect();
                for (delegate_index, item_index) in delegate_publishing {
                    delegate_assignments[delegate_index][item_index] = Some(take());
                }
                drop(take);
                let mut by: Vec<_> = keys
                    .into_iter()
                    .zip(key_assignments)
                    .map(|(item, output)| published_position(&self.construction, item, output))
                    .collect();
                let mut on: Vec<_> =
                    reductions
                        .drain(..)
                        .zip(reduction_assignments)
                        .map(|(item, output)| {
                            Ok(match item {
                                Reduction::Out(item) => {
                                    if let (Some(port), Some(expression)) =
                                        (output, item.value().cloned())
                                    {
                                        self.attach_record_interior(port, &expression)?;
                                    }
                                    crate::pipeline::asts::core::ReductionItem::Out(
                                        published_position(&self.construction, item, output),
                                    )
                                }
                                Reduction::Metadata { group, naming } => {
                                    let output = output.expect("metadata publishes one group port");
                                    // A metadata group yields a record keyed by
                                    // data: a nested payload whose heading is
                                    // not static, so the fact alone travels.
                                    self.registry.mark_nested_payload(output.column());
                                    crate::pipeline::asts::core::ReductionItem::Metadata(
                                        crate::pipeline::asts::core::MetadataOut::published(
                                            &self.construction,
                                            group,
                                            naming,
                                            output,
                                        ),
                                    )
                                }
                                Reduction::Pivot(pivot) => {
                                    crate::pipeline::asts::core::ReductionItem::Pivot(pivot)
                                }
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                let plan =
                    crate::pipeline::resolver::resolving::tree_group_analysis::analyze_tree_groups_for_ctes(
                        &mut by, &mut on,
                    )?;
                for ((delegate, assignments), elided) in
                    delegates.into_iter().zip(delegate_assignments).zip(elided)
                {
                    let payload: Vec<_> = delegate
                        .payload
                        .into_iter()
                        .zip(assignments)
                        .zip(elided)
                        .filter(|(_, elided)| !elided)
                        .map(|((item, output), _)| {
                            published_position(&self.construction, item, output)
                        })
                        .collect();
                    on.push(crate::pipeline::asts::core::ReductionItem::Delegate(
                        crate::pipeline::asts::core::DelegateSpec {
                            payload,
                            order: delegate.order,
                        },
                    ));
                }
                self.paired(
                    Continuation::Pipe {
                        operator: PipeOp::Group(crate::pipeline::asts::core::GroupSpec::Reduce {
                            keys: by,
                            reductions: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(on)
                                .expect("the authored reduction was nonempty"),
                            plan,
                        }),
                        named: (),
                    },
                    result,
                )
            }
        }
    }

    /// Rebuild a projection with hygienic positions that remain semantic
    /// until their closed-value lifecycle spends them.
    fn bind_crossing_carrier_injection(
        &self,
        replaces: SemanticRelation,
        carriers: Vec<PortId>,
        items: Vec<crate::pipeline::asts::resolved::OutItem>,
        stored: super::pending::Publishes,
    ) -> Result<BoundStep> {
        self.bind_hidden_carriers_beside(replaces, carriers, items, stored, |_, _| {})
    }

    /// THE FIXPOINT'S SUPPORT, PUBLISHED BESIDE A CLAUSE'S PROJECTION: each
    /// hidden position carries one landed caller actual and is marked with
    /// the exact actual it stands for, in the same act that mints it.
    fn bind_frontier_actual_injection(
        &self,
        replaces: SemanticRelation,
        carriers: Vec<(PortId, PortId)>,
        items: Vec<crate::pipeline::asts::resolved::OutItem>,
        stored: super::pending::Publishes,
    ) -> Result<BoundStep> {
        let actuals: Vec<PortId> = carriers.iter().map(|(_, actual)| *actual).collect();
        let landed: Vec<PortId> = carriers.into_iter().map(|(landed, _)| landed).collect();
        self.bind_hidden_carriers_beside(replaces, landed, items, stored, |index, output| {
            self.registry
                .relations()
                .mark_frontier_actual(output, actuals[index]);
        })
    }

    fn bind_hidden_carriers_beside(
        &self,
        replaces: SemanticRelation,
        carriers: Vec<PortId>,
        mut items: Vec<crate::pipeline::asts::resolved::OutItem>,
        stored: super::pending::Publishes,
        mark: impl Fn(usize, PortId),
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{
            ColumnOccurrence, Continuation, DomainExpression, NamedReference, OneOut, OutItem,
            PipeOp, Reference,
        };
        let slots: Vec<_> = carriers
            .iter()
            .copied()
            .map(|source| ProjectSlot::Carried {
                source,
                naming: Naming::Hygienic,
            })
            .collect();
        let result = self.derive(RelForm::Embed(ProjectSpec {
            input: replaces,
            why: super::form::ProjectWhy::Stage,
            slots: &slots,
        }))?;
        for item in items.iter_mut() {
            let OutItem::One(one) = item else {
                continue;
            };
            let landed = self.followed(&result, *one.output())?;
            one.reland(&self.construction, landed);
        }
        let outputs = self.interface(&result)?.ports().to_vec();
        let hidden = &outputs[outputs.len() - carriers.len()..];
        for (index, (source, output)) in
            carriers.into_iter().zip(hidden.iter().copied()).enumerate()
        {
            mark(index, output);
            items.push(OutItem::one(OneOut::published(
                &self.construction,
                DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence::engine(source),
                ))),
                None,
                output,
            )));
        }
        self.record_replacement_of(replaces, &result)?;
        let items = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items)
            .expect("crossing injection preserves a nonempty projection");
        self.paired(
            Continuation::Pipe {
                operator: match stored {
                    super::pending::Publishes::Anew => PipeOp::Project(items),
                    super::pending::Publishes::Edited => PipeOp::Embed(items),
                },
                named: (),
            },
            result,
        )
    }

    /// A compiler-written ranking witness beside the operand's whole
    /// heading: `row_number()` over the stated partition and ordering,
    /// standing at the one hygienic position this act mints.
    fn bind_window_witness(
        &self,
        input: SemanticRelation,
        partition: Vec<crate::pipeline::asts::resolved::DomainExpression>,
        ordering: Vec<crate::pipeline::asts::resolved::OrderingSpec>,
    ) -> Result<BoundStep> {
        use crate::pipeline::asts::core::{
            Continuation, DomainExpression, FunctionApplication, OneOut, OutItem, PipeOp,
        };
        let slots = [ProjectSlot::Computed {
            naming: Naming::Hygienic,
            shape: crate::names::ValueShape::Unknown,
        }];
        let result = self.derive(RelForm::Embed(ProjectSpec {
            input,
            why: super::form::ProjectWhy::Restate,
            slots: &slots,
        }))?;
        let ports = self.interface(&result)?.ports().to_vec();
        let port = *ports
            .last()
            .expect("the window projection appends one row-number port");
        let witness = OutItem::one(OneOut::published(
            &self.construction,
            DomainExpression::Application(FunctionApplication::Standard(
                crate::pipeline::asts::core::StandardApplication {
                    call: crate::pipeline::asts::core::PureCall::from_inner(
                        crate::pipeline::asts::core::FunctorCall::<
                            crate::pipeline::asts::core::Resolved,
                        > {
                            callee: self.registry.mint_function(
                                self.registry.intern("row_number", false),
                                Vec::new(),
                            ),
                            arguments:
                                crate::pipeline::asts::core::operators::CallArguments::Scalar(
                                    Vec::new(),
                                ),
                            marks: Default::default(),
                        },
                    ),
                    guard: None,
                    window: Some(crate::pipeline::asts::core::WindowSpec {
                        partition,
                        ordering,
                        frame: None,
                    }),
                },
            )),
            // A compiler-minted witness answers to no authored name.
            None,
            port,
        ));
        let items =
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![OutItem::Whole, witness])
                .expect("the window projection carries the whole and the window item");
        self.paired(
            Continuation::Pipe {
                operator: PipeOp::Embed(items),
                named: (),
            },
            result,
        )
    }

    /// THE SEMANTIC SLOT ONE STATED POSITION ASKS FOR.
    ///
    /// A direct reference CARRIES the operand's position; anything that
    /// computes a value mints one. The name comes from the position, and
    /// the scalar expression under it supplies value shape only —
    /// computation classification is not identity.
    pub(super) fn publication_slot<P>(
        &self,
        value: &crate::pipeline::asts::core::DomainExpression<P>,
        naming: Option<&delightql_types::SqlIdentifier>,
        input: &SemanticRelation,
    ) -> Result<ProjectSlot>
    where
        P: crate::pipeline::asts::core::Phase<
            Col = crate::pipeline::asts::core::ColumnOccurrence,
            ColumnOrdinal = crate::pipeline::asts::vocabulary::Never,
            PhysicalColumn = crate::pipeline::asts::vocabulary::Never,
            Entity = crate::names::CallableId,
        >,
    {
        use crate::pipeline::asts::core::{
            ColumnOccurrence, DomainExpression, NamedReference, Reference,
        };
        let authored = |name: &delightql_types::SqlIdentifier| {
            Naming::Authored(self.registry.intern(name.as_str(), name.is_stropped()))
        };
        let computed =
            |naming: Option<&delightql_types::SqlIdentifier>, shape| ProjectSlot::Computed {
                naming: naming.map_or(Naming::Anonymous, authored),
                shape,
            };
        Ok(match value {
            // A RESTATEMENT CARRIES: a position the enclosing join computes
            // continues here unread, and the carry record says so.
            DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
                column,
                ..
            }))) => ProjectSlot::Carried {
                source: self.port_in(input, *column)?,
                naming: naming.map_or(Naming::Inherited, authored),
            },
            DomainExpression::Reference(Reference::Ordinal(ordinal)) => match *ordinal {},
            DomainExpression::Reference(Reference::Physical(physical)) => match *physical {},
            // A COMPUTATION READS.
            domain @ DomainExpression::Application(_) => {
                self.refusing_deferred_reads(domain, "a value inside the interior")?;
                computed(naming, self.value_shape(domain))
            }
        })
    }

    /// THE SLOT OF A STATED POSITION IN A FORM THAT EVALUATES IT WHERE IT
    /// STANDS — a grouping's key, a reduction, a delegate's payload. A value
    /// evaluated here takes the publication's slot; one that reads the
    /// enclosing row is, in place, the target's own correlated value, and
    /// in a hoisted interior a value the form cannot compute inside the
    /// boundary — the form refuses, naming itself.
    fn evaluated_slot(
        &self,
        position: &super::pending::Position,
        input: &SemanticRelation,
        what: &str,
    ) -> Result<ProjectSlot> {
        let value = position
            .value()
            .expect("a slot is asked of a position that states a value");
        match position.evaluation() {
            super::pending::Evaluation::Here => {
                self.publication_slot(value, position.naming(), input)
            }
            super::pending::Evaluation::Enclosing { correlations, .. } => {
                if *correlations == crate::pipeline::resolver::Correlations::Hoisted {
                    return Err(super::enclosing_position_refusal(what));
                }
                Ok(self.enclosing_slot(value, position.naming(), input)?.0)
            }
        }
    }

    /// A POSITION WHOSE VALUE READS THE ENCLOSING ROW: a new value, named
    /// as the author asked or — for a bare reference — as the occurrence it
    /// reads publishes, and the interior occurrences the value reads, as
    /// positions of the operand. Nothing here carries the enclosing
    /// occurrence: a carry edge from a row this heading does not stand on
    /// would make the two occurrences interchangeable to every reader of
    /// the record.
    fn enclosing_slot<P>(
        &self,
        value: &crate::pipeline::asts::core::DomainExpression<P>,
        naming: Option<&delightql_types::SqlIdentifier>,
        input: &SemanticRelation,
    ) -> Result<(ProjectSlot, Vec<PortId>)>
    where
        P: crate::pipeline::asts::core::Phase<
            Col = crate::pipeline::asts::core::ColumnOccurrence,
            ColumnOrdinal = crate::pipeline::asts::vocabulary::Never,
            PhysicalColumn = crate::pipeline::asts::vocabulary::Never,
            Entity = crate::names::CallableId,
        >,
    {
        use crate::pipeline::asts::core::{
            ColumnOccurrence, DomainExpression, NamedReference, Reference,
        };
        self.refusing_deferred_reads(value, "a value inside the interior")?;
        let naming = match (naming, value) {
            (Some(name), _) => {
                Naming::Authored(self.registry.intern(name.as_str(), name.is_stropped()))
            }
            (
                None,
                DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
                    column,
                    ..
                }))),
            ) => self
                .inheritable_publication(*column)
                .map_or(Naming::Anonymous, Naming::Authored),
            (None, _) => Naming::Anonymous,
        };
        let reads: Vec<PortId> = super::support::ports_read_deep(value)
            .into_iter()
            .filter_map(|port| self.port_in(input, port).ok())
            .collect();
        Ok((
            ProjectSlot::Computed {
                naming,
                shape: self.value_shape(value),
            },
            reads,
        ))
    }

    /// A VALUE MAY NOT READ A POSITION THE ENCLOSING JOIN COMPUTES: the
    /// position has no value until the boundary is crossed.
    fn refusing_deferred_reads<P>(
        &self,
        value: &crate::pipeline::asts::core::DomainExpression<P>,
        what: &str,
    ) -> Result<()>
    where
        P: crate::pipeline::asts::core::Phase<Col = crate::pipeline::asts::core::ColumnOccurrence>,
    {
        let store = self.registry.relations();
        if super::support::ports_read_deep(value)
            .into_iter()
            .any(|port| store.is_deferred_port(port))
        {
            return Err(super::enclosing_position_refusal(what));
        }
        Ok(())
    }

    /// THE SHAPE A COMPUTED VALUE HAS, where the computation says so.
    fn value_shape<P>(
        &self,
        expr: &crate::pipeline::asts::core::DomainExpression<P>,
    ) -> crate::names::ValueShape
    where
        P: crate::pipeline::asts::core::Phase<Entity = crate::names::CallableId>,
    {
        use crate::pipeline::asts::core::{DomainExpression, Enclyph, FunctionApplication};
        match expr {
            DomainExpression::Application(FunctionApplication::Enclyph(
                Enclyph::Record(_) | Enclyph::EmptyRecord(_),
            )) => crate::names::ValueShape::Record,
            DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Tuple(_))) => {
                crate::names::ValueShape::Tuple
            }
            DomainExpression::Application(FunctionApplication::Standard(application)) => {
                let function = application.call().callee;
                let intrinsic = matches!(
                    self.registry.function_origin(function),
                    crate::names::FnOrigin::Intrinsic(crate::names::Intrinsic::JsonObject)
                );
                let mut name = String::new();
                let named = self
                    .registry
                    .write_function_name(function, &mut crate::names::sink::Teaching(&mut name))
                    .is_ok()
                    && name.eq_ignore_ascii_case("json_object");
                if intrinsic || named {
                    crate::names::ValueShape::Record
                } else {
                    crate::names::ValueShape::Unknown
                }
            }
            _ => crate::names::ValueShape::Unknown,
        }
    }

    /// THE AUTHORED OWNER OF A STAGE, stamped on the relation a crossing
    /// produced. An identity fact of that occurrence — what the metadata
    /// view reports as its scope — and not a route: whether `s.x` reaches
    /// the relation at a later position is the lexical frontier's answer,
    /// and the frontier is born from the crossed relation, not from this
    /// stamp. A relation that already answers to a name refuses.
    pub(crate) fn own_stage(
        &self,
        relation: &SemanticRelation,
        answer: crate::names::Spelling,
    ) -> Result<()> {
        check_mark(self.mark, relation)?;
        self.registry.adopt_stage_owner(relation.scope(), answer)
    }

    /// A HEAD THAT READS COMPILER-OWNED PLAN STORAGE THROUGH ITS OWN BODY.
    ///
    /// A redirected authored access has two identities — the scratch object
    /// in the inner FROM and the caller-facing occurrence outside it — and
    /// this is where the second one is made. Which KIND of plan object it
    /// is comes from the body, not from the caller.
    pub(crate) fn plan_read_head<P>(
        &self,
        form: crate::pipeline::asts::core::GroundForm<P>,
        answers_to: crate::names::Spelling,
    ) -> Result<crate::pipeline::asts::core::Grelex<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let Some(template) = wrapped_body(&form) else {
            return Err(Internal::invariant(
                "semantic relation",
                "a head with no body of its own cannot read plan storage through it",
            ));
        };
        check_mark(self.mark, &template)?;
        let kind = if self.is_plan_scratch(&template)? {
            super::form::PlanReadKind::Scratch
        } else {
            super::form::PlanReadKind::HigherOrder
        };
        let result = self.derive(RelForm::PlanRead(super::form::PlanReadSpec {
            kind,
            template,
            answers_to,
        }))?;
        Ok(crate::pipeline::asts::core::Grelex::derived(
            &self.construction,
            form,
            result,
        ))
    }

    /// A HEAD THAT WRAPS A CHAIN AND PUBLISHES WHAT THE CHAIN PUBLISHES.
    ///
    /// A derived table standing around a chain is a BOUNDARY, not a new
    /// relation: every reference above it was already answered against the
    /// chain's own result, so the wrap RESTATES that result rather than
    /// deriving one. There is no argument here for a relation at all — the
    /// head reads it out of the body standing inside it — so a wrap cannot
    /// answer for rows it does not contain.
    pub(crate) fn wrapping_head<P>(
        &self,
        form: crate::pipeline::asts::core::GroundForm<P>,
    ) -> Result<crate::pipeline::asts::core::Grelex<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let Some(body) = wrapped_body(&form) else {
            return Err(Internal::invariant(
                "semantic relation",
                "a head with no body of its own wraps nothing",
            ));
        };
        check_mark(self.mark, &body)?;
        Ok(crate::pipeline::asts::core::Grelex::derived(
            &self.construction,
            form,
            body,
        ))
    }

    /// A HEAD THAT PUBLISHES A BOUNDARY DERIVED OVER ITS OWN BODY.
    ///
    /// A consulted expansion is addressed through the name it was invoked
    /// by, and that answer is an INSTANCE of what the body publishes. The
    /// boundary is derived HERE, from the body standing inside the form, so
    /// the head and what it publishes come from one description and no
    /// caller can hand in a boundary some other body produced.
    pub(crate) fn boundary_head<P>(
        &self,
        form: crate::pipeline::asts::core::GroundForm<P>,
        boundary: Boundary,
    ) -> Result<crate::pipeline::asts::core::Grelex<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let Some(body) = wrapped_body(&form) else {
            return Err(Internal::invariant(
                "semantic relation",
                "a head with no body of its own has no boundary to publish",
            ));
        };
        check_mark(self.mark, &body)?;
        let interior = matches!(boundary, Boundary::Interior { .. });
        let boundary = self.derive(match boundary {
            Boundary::Instance { kind, answers_to } => {
                RelForm::Instantiate(super::form::InstanceSpec {
                    kind,
                    template: body,
                    answers_to,
                })
            }
            Boundary::Interior { answer } => RelForm::Export(super::form::ExportSpec {
                input: body,
                why: super::form::ExportWhy::Alias { answer },
            }),
        })?;
        // THE INTERIOR BOUNDARY IS WHERE A HOISTED CORRELATION IS SPENT. The
        // enclosing join evaluates the correlation, so the boundary's level
        // still emits every interior occurrence the body owed — and nothing
        // standing over the boundary owes it any further.
        if interior {
            let store = self.registry.relations();
            store.spend_support_outward(boundary.relation());
            // AND WHERE A DEFERRED POSITION IS COMPUTED: the boundary hands
            // the enclosing join every position the body carried toward it,
            // and nothing over the boundary defers them.
            store.spend_deferral_outward(boundary.relation());
        }
        Ok(crate::pipeline::asts::core::Grelex::derived(
            &self.construction,
            form,
            boundary,
        ))
    }

    /// A HEAD THAT READS A RELATION THIS COMPILATION ALREADY BUILT.
    ///
    /// The exact operation IS the relation: a spent ground mention, a
    /// callable's applied result, an anonymous table's rows. The variant
    /// the caller reaches for fixes the AST form, so a form built for one
    /// operation cannot be handed the relation another one produced.
    pub(crate) fn reading<P>(
        &self,
        of: ReadHead<P>,
    ) -> Result<crate::pipeline::asts::core::Grelex<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation, Mention = ()>,
    {
        use crate::pipeline::asts::core::{GroundForm, Relation};
        let (form, published) = match of {
            ReadHead::Ground { outer, published } => {
                (GroundForm::Reference(Relation::ground(outer)), published)
            }
            ReadHead::Call {
                call,
                alias,
                published,
            } => (
                GroundForm::Reference(Relation::FunctorCall { call, alias }),
                published,
            ),
            ReadHead::Anonymous {
                relation,
                published,
            } => (GroundForm::Literal(relation), published),
        };
        check_mark(self.mark, &published)?;
        let result = self.derive(RelForm::Order(published))?;
        Ok(crate::pipeline::asts::core::Grelex::derived(
            &self.construction,
            form,
            result,
        ))
    }

    /// RE-APPEND A STEP OVER THE RELATION THAT REPLACED ITS OPERAND.
    ///
    /// A step that published its operand's OWN relation stands unchanged
    /// on whatever replaced that operand, and what it publishes is the new
    /// operand's — restated here, never carried over. The step's RECORDED
    /// result is the evidence that it published its operand's relation:
    /// a step publishing a heading of its own refuses rather than being
    /// silently re-based onto rows it never saw.
    ///
    /// The caller cannot nominate a relation. It states which step it is
    /// re-appending and which relation was replaced; both halves of what
    /// the step will publish come from the chain it lands on.
    pub(crate) fn continue_over<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        step: crate::pipeline::asts::core::Step<P>,
        replaced: SemanticRelation,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let standing = chain.semantic_relation();
        check_mark(self.mark, &standing)?;
        if *step.result() != replaced {
            // A STAGE REPUBLICATION stands on its operand through its own
            // derivation: the ordering published the replaced relation's
            // whole heading one-to-one through the stage export. Re-appending
            // it over the replacement derives a FRESH stage there — the same
            // operation over the new operand — rather than restating a
            // relation whose operand is gone. Anything else that publishes
            // its own heading still refuses: it saw rows the replacement
            // never showed it.
            let store = self.registry.relations();
            let stands_on = store.inputs(step.result().relation());
            // THE STEP MUST HAVE REPUBLISHED ITS OPERAND, by the record of
            // the act that derived its result — not merely stood on it. A
            // grouping stands on one input too, and re-deriving it as a
            // stage export would record a distinct bag as its operand
            // republished.
            let republishes = stands_on.as_slice() == [replaced]
                && store.is_republication(step.result().relation());
            if !republishes {
                return Err(replacement_error(
                    "a step re-appended over a replaced relation did not republish it",
                ));
            }
            let old_stage = *step.result();
            let stage = self.derive(RelForm::Export(ExportSpec {
                input: standing,
                why: ExportWhy::Stage,
            }))?;
            // WHERE THE OLD STAGE'S POSITIONS LAND, recorded in the same
            // act: each crossed the old stage from an operand position, the
            // operand's replacement says where that position went, and the
            // fresh stage carries it — three construction records composed,
            // never a name or an ordinal.
            let operand_map = self
                .registry
                .relations()
                .replacement(replaced.relation(), standing.relation())
                .ok_or_else(|| {
                    replacement_error(
                        "a stage republication was re-derived over a relation its operand's \
                         replacement never recorded",
                    )
                })?;
            let store = self.registry.relations();
            let new_ports = self.interface(&stage)?.ports().to_vec();
            let mut pairs = Vec::new();
            for old_port in self.interface(&old_stage)?.ports().iter().copied() {
                let sources = store.lineage(old_port);
                let [source] = sources.as_slice() else {
                    return Err(replacement_error(
                        "a stage republication carries a position with no one source",
                    ));
                };
                let Some(new_source) = operand_map.answer(*source) else {
                    return Err(replacement_error(
                        "a stage position's source has no landing in the operand's replacement",
                    ));
                };
                let Some(new_port) = new_ports
                    .iter()
                    .copied()
                    .find(|port| store.lineage(*port).as_slice() == [new_source])
                else {
                    return Err(replacement_error(
                        "a re-derived stage does not carry a position the old stage published",
                    ));
                };
                pairs.push((old_port, new_port));
            }
            store.record_replacement(
                old_stage.relation(),
                stage.relation(),
                TotalPortMap {
                    from: old_stage.relation(),
                    to: stage.relation(),
                    pairs,
                },
            );
            let landed = crate::pipeline::asts::core::Step::derived(
                &self.construction,
                step.into_form(),
                stage,
            );
            return Ok(chain.then_derived(&self.construction, landed));
        }
        let landed = crate::pipeline::asts::core::Step::derived(
            &self.construction,
            step.into_form(),
            standing,
        );
        Ok(chain.then_derived(&self.construction, landed))
    }

    /// Whether one relation was BUILT FROM another, by the construction
    /// record alone.
    ///
    /// Follows the operands each derivation wrote down. Nothing here reads
    /// a name, an ordinal or a heading: a relation that happens to look
    /// like the operand is not the operand.
    fn stands_on(&self, derived: SemanticRelation, operand: SemanticRelation) -> bool {
        if derived == operand {
            return true;
        }
        let store = self.registry.relations();
        let mut frontier = vec![derived];
        let mut seen = vec![derived.relation()];
        while let Some(here) = frontier.pop() {
            for input in store.inputs(here.relation()) {
                if input == operand {
                    return true;
                }
                if !seen.contains(&input.relation()) {
                    seen.push(input.relation());
                    frontier.push(input);
                }
            }
        }
        false
    }

    /// LAND A STEP BACK ON AN OPERAND IT WAS DERIVED OVER.
    ///
    /// A pass that WRAPS a chain — sealing a bounded branch in a subquery
    /// so a set operation can stand over it — or that moves a transparent
    /// step out from under a run leaves the steps above standing on an
    /// operand that still publishes what it published. This is the one
    /// road that puts such a step back, and it states the condition
    /// instead of assuming it: the step's relation must DESCEND from the
    /// relation the operand publishes, by the construction record.
    ///
    /// It cannot relate two unrelated relations, which is the whole point:
    /// there is no argument here for a result, and the step keeps the one
    /// its own derivation gave it.
    pub(crate) fn reland<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        step: crate::pipeline::asts::core::Step<P>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let operand = chain.semantic_relation();
        check_mark(self.mark, &operand)?;
        if !self.stands_on(*step.result(), operand) {
            return Err(replacement_error(
                "a step was landed on an operand its relation was not derived over",
            ));
        }
        // A CORRELATED RESTRICTION LANDS ONLY WHERE IT WAS MINTED. Its
        // payload carries the relation the act recorded the obligation on;
        // descent is not enough, because the step would then claim a
        // relation the rows under it are not, and the step's own result is
        // not consulted, because the payload is what cannot be re-paired.
        if let crate::pipeline::asts::core::Continuation::Correlated(correlated) = step.form() {
            if P::correlated(correlated).standing() != operand {
                return Err(replacement_error(
                    "a correlated restriction was landed on a relation other than the one its \
                     correlation act recorded the obligation on",
                ));
            }
        }
        // THE STEP RE-JUDGES WHAT IT OWES from the operand it now stands on,
        // under the law its own derivation stated: a correlation placed
        // under an already-derived run reaches the run's relations here.
        self.judge_support(*step.result())?;
        chain.landed(&self.construction, step)
    }

    /// THE ONE CORRELATION ACT. A restriction that reads the enclosing row
    /// stands on the chain's relation; that relation now OWES the interior
    /// occurrences the restriction reads, because the hoisting realization
    /// evaluates the restriction at the enclosing join and every operation
    /// up to the interior boundary must keep them readable.
    ///
    /// The act takes the restriction's two operands and DERIVES everything
    /// else: which of the condition's occurrences the relation publishes
    /// (the interior ones, owed) and which it does not (the enclosing
    /// row's). There is no argument for the support, so no caller can pair
    /// a condition with occurrences somebody else read, and the obligation,
    /// the sealed [`super::Correlated`] value and the step are written in
    /// the same breath — a chain cannot carry the step without the record.
    /// The lexical proof says only that a lookup judged the condition a
    /// correlation; the act refuses one that reads no interior occurrence
    /// (it constrains nothing about the relation it stands on) or no
    /// enclosing one (it is not a correlation at all).
    pub(crate) fn correlate(
        &self,
        chain: crate::pipeline::asts::resolved::Chain,
        condition: crate::pipeline::asts::resolved::TruthExpression,
        _judged: crate::pipeline::resolver::Terminal,
    ) -> Result<crate::pipeline::asts::resolved::Chain> {
        let standing = chain.semantic_relation();
        check_mark(self.mark, &standing)?;
        self.registry.relations().check_open()?;
        let published = self.interface(&standing)?.ports().to_vec();
        // READ AT ANY DEPTH: a witness's body or a scalar subquery inside
        // the condition is evaluated where the condition is, at the
        // enclosing join, so an interior occurrence it reads is one the
        // hoisted condition reads. The nested relation's own positions come
        // along and fall to `outer`, which only the invariant below reads.
        let read = super::support::ports_read_by_deep(&condition);
        if read
            .iter()
            .any(|port| self.registry.relations().is_deferred_port(*port))
        {
            return Err(super::enclosing_position_refusal(
                "a restriction inside the interior",
            ));
        }
        let (inner, outer): (Vec<PortId>, Vec<PortId>) =
            read.into_iter().partition(|port| published.contains(port));
        if inner.is_empty() {
            return Err(DelightQLError::from(
                crate::diagnostic::Resolution::CorrelationUncorrelatedPredicate {
                    message: "every name in this condition was answered by the enclosing \
                              relation, so it constrains nothing about the relation it is \
                              attached to — an interior relation reads the heading its \
                              source PUBLISHES, and a name absent from that heading reaches \
                              outward"
                        .to_string(),
                },
            ));
        }
        if outer.is_empty() {
            return Err(Internal::invariant(
                "correlation",
                "a restriction reading no enclosing occurrence was presented as a correlation",
            ));
        }
        self.registry
            .relations()
            .mark_correlation(standing.relation(), &inner);
        self.judge_support(standing)?;
        chain.correlated(
            &self.construction,
            super::Correlated::recorded(condition, inner, standing),
        )
    }

    /// THE SUPPORT ONE RELATION OWES AND EMITS, judged from what its
    /// operands owe under the law its own derivation stated.
    ///
    /// Total and fail-closed. An owed occurrence CONTINUES in the one
    /// published position standing on it; where none does, it RIDES beside
    /// the heading under its own identity if the form can emit an operand
    /// position, and REFUSES if it cannot. Nothing here drops an occurrence,
    /// and nothing downstream is asked to find one again: the positions this
    /// level emits are recorded for lowering, and what its consumers owe is
    /// recorded for their own judgment.
    fn judge_support(&self, relation: SemanticRelation) -> Result<()> {
        let store = self.registry.relations();
        let Some(law) = store.support_law(relation.relation()) else {
            return Ok(());
        };
        let ports = self.interface(&relation)?.ports().to_vec();
        // JUDGED FIRST: A POSITION THE ENCLOSING JOIN COMPUTES travels the
        // other way under the same law — a form that reads one refuses for
        // that before the question of what it keeps readable is asked: it CONTINUES, unread, in every published position
        // standing on it — a level that republishes carries it, a join's
        // row concatenates it — or it is dropped; a grouping keyed on it, or
        // a form that would change what the position means, refuses. And a
        // boundary that SPENT such a position outward is consumed by the
        // join that computes it and by nothing else: a level between them
        // would emit a value nobody has yet.
        let mut defers: Vec<PortId> = Vec::new();
        for input in store.inputs(relation.relation()) {
            if !matches!(law, SupportLaw::Concatenates)
                && self
                    .interface(&input)?
                    .ports()
                    .iter()
                    .any(|port| store.is_realized_port(*port))
            {
                return Err(super::enclosing_position_refusal(
                    "a step between the interior and the join that computes its positions",
                ));
            }
            for deferred in store.deferred(input.relation()) {
                let standing: Vec<PortId> = ports
                    .iter()
                    .copied()
                    .filter(|published| {
                        *published == deferred || store.lineage(*published).contains(&deferred)
                    })
                    .collect();
                match law {
                    SupportLaw::Carries | SupportLaw::Concatenates => {
                        for continued in standing {
                            if !defers.contains(&continued) {
                                defers.push(continued);
                            }
                        }
                    }
                    SupportLaw::Translates { keys } => {
                        let keys = ports.get(..keys).unwrap_or(&ports);
                        if standing.iter().any(|key| keys.contains(key)) {
                            return Err(super::enclosing_position_refusal(
                                "a grouping that groups by it",
                            ));
                        }
                    }
                    SupportLaw::Refuses(what) => {
                        if !standing.is_empty() {
                            return Err(super::enclosing_position_refusal(what));
                        }
                    }
                }
            }
        }
        for mark in store.deferred_marks(relation.relation()) {
            if !defers.contains(&mark) {
                defers.push(mark);
            }
        }
        store.record_deferral(relation.relation(), defers);
        let mut owed: Vec<PortId> = Vec::new();
        for input in store.inputs(relation.relation()) {
            for port in store.support_owed(input.relation()) {
                if !owed.contains(&port) {
                    owed.push(port);
                }
            }
        }
        let mut owes: Vec<PortId> = Vec::new();
        for port in owed {
            let standing: Vec<PortId> = ports
                .iter()
                .copied()
                .filter(|published| *published == port || store.lineage(*published).contains(&port))
                .collect();
            let carried = match law {
                SupportLaw::Carries | SupportLaw::Concatenates => match standing.as_slice() {
                    [continued] => *continued,
                    // Several positions continuing one occurrence are one
                    // value each; the occurrence keeps its own identity
                    // rather than having one of them chosen for it.
                    _ => port,
                },
                SupportLaw::Translates { keys } => {
                    let keys = ports.get(..keys).unwrap_or(&ports);
                    // Two keys standing on one occurrence group by one value
                    // twice; the first in the authored key order stands for
                    // it, and either would filter the same groups.
                    match standing.iter().copied().find(|key| keys.contains(key)) {
                        Some(key) => key,
                        None => {
                            return Err(unrealizable_support(
                                "a grouping that does not group by it",
                            ))
                        }
                    }
                }
                SupportLaw::Refuses(what) => return Err(unrealizable_support(what)),
            };
            if !owes.contains(&carried) {
                owes.push(carried);
            }
        }
        for mark in store.support_marks(relation.relation()) {
            if !owes.contains(&mark) {
                owes.push(mark);
            }
        }
        let emits: Vec<PortId> = owes
            .iter()
            .copied()
            .filter(|port| !ports.contains(port))
            .collect();
        // THE RUN THE LEVEL EMITS AFTER ITS HEADING, in emission order. A
        // join's row re-emits its operands' runs whole, left then right;
        // every other level emits its own constraint dependencies and then
        // the correlation support it owes. Only the latter — what THIS level
        // owes and does not publish — is what binding refuses to leave
        // unbound: a level over a join may drop the run the join already
        // spent.
        let run = match law {
            SupportLaw::Concatenates => store
                .inputs(relation.relation())
                .into_iter()
                .flat_map(|input| store.support_run(input.relation()))
                .collect(),
            SupportLaw::Carries | SupportLaw::Translates { .. } | SupportLaw::Refuses(_) => {
                let mut run = store.dependencies(relation.relation());
                for port in &emits {
                    if !run.contains(port) {
                        run.push(*port);
                    }
                }
                run
            }
        };
        store.record_support(relation.relation(), owes, emits, run);
        Ok(())
    }

    /// Land a whole suffix back, innermost first. See
    /// [`SemanticBuilder::reland`].
    pub(crate) fn reland_all<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        steps: impl IntoIterator<Item = crate::pipeline::asts::core::Step<P>>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        steps
            .into_iter()
            .try_fold(chain, |chain, step| self.reland(chain, step))
    }

    /// ONE PRODUCING STEP, appended to the chain it consumes.
    ///
    /// The operation family is the ONE description: its variant decides
    /// both the output law and the continuation the tree stores, so a
    /// projection cannot be stored as a projection and derived as a
    /// preserve. The operand is the chain's, never an argument.
    pub(crate) fn extend<P>(
        &self,
        chain: crate::pipeline::asts::core::Chain<P>,
        op: StepOp<'_, P>,
    ) -> Result<crate::pipeline::asts::core::Chain<P>>
    where
        P: crate::pipeline::asts::core::Phase<
            Scope = SemanticRelation,
            Output = PortId,
            Binder = PortId,
            Col = crate::pipeline::asts::core::ColumnOccurrence,
        >,
    {
        use crate::pipeline::asts::core::Continuation;
        let operand = chain.semantic_relation();
        check_mark(self.mark, &operand)?;
        let step = match op {
            StepOp::Join {
                rhs,
                correlation,
                join_type,
                right,
                kind,
                merged,
            } => {
                check_mark(self.mark, &right)?;
                self.produced_step(
                    Continuation::Member {
                        rhs,
                        correlation,
                        join_type,
                    },
                    RelForm::Join(super::form::JoinSpec {
                        left: operand,
                        right,
                        kind,
                        merged,
                    }),
                )?
            }
            StepOp::Access {
                shape,
                slots,
                dependencies,
            } => {
                let result = self.derive(RelForm::Access(super::form::AccessSpec {
                    input: operand,
                    shape,
                    slots,
                    dependencies,
                }))?;
                let access = binding_access(self.interface(&result)?.ports());
                crate::pipeline::asts::core::Step::derived(
                    &self.construction,
                    Continuation::Access {
                        access,
                        named: P::no_stage_name(),
                    },
                    result,
                )
            }
            StepOp::Republish { of, sources } => {
                let of = match of {
                    Republishing::ErBoundary(spec) => RelForm::ErBoundary(spec),
                    Republishing::Project(spec) => RelForm::Project(spec),
                    Republishing::Export(spec) => RelForm::Export(spec),
                };
                let (result, items) =
                    self.publishing(of, sources, |authority, source, output| {
                        Ok(crate::pipeline::asts::core::OneOut::published(
                            authority,
                            crate::pipeline::asts::core::DomainExpression::Reference(
                                crate::pipeline::asts::core::Reference::Named(
                                    crate::pipeline::asts::core::NamedReference(
                                        crate::pipeline::asts::core::ColumnOccurrence::engine(
                                            source,
                                        ),
                                    ),
                                ),
                            ),
                            None,
                            output,
                        ))
                    })?;
                let Some(items) = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(
                    items
                        .into_iter()
                        .map(crate::pipeline::asts::core::OutItem::one)
                        .collect(),
                ) else {
                    return Err(Internal::invariant(
                        "publication",
                        "a republishing projection carries no position",
                    ));
                };
                crate::pipeline::asts::core::Step::derived(
                    &self.construction,
                    Continuation::Pipe {
                        operator: crate::pipeline::asts::core::PipeOp::Project(items),
                        named: Default::default(),
                    },
                    result,
                )
            }
        };
        Ok(chain.then_derived(&self.construction, step))
    }

    /// REFINE ONE OPERAND THROUGH THE AUTHORITY.
    ///
    /// The authority HOLDS the operand, runs the refinement, and reads what
    /// the refinement published. What comes back is one typed outcome, and
    /// it is decided from the CONSTRUCTION RECORD rather than judged
    /// afterwards:
    ///
    /// * the refinement published the operand — PRESERVED, and there is
    ///   nothing to record;
    /// * the refinement derived a new relation FROM the operand — REBUILT,
    ///   and the total map is the lineage that derivation wrote down as it
    ///   built, read here rather than reconstructed.
    ///
    /// A relation with no recorded descent from the operand REFUSES. There
    /// is no spelling, ordinal or addressing comparison here to invent a
    /// lineage from, and no entrance that takes an old relation, a new one
    /// and a map: a caller supplies none of the three.
    ///
    /// THE OPERAND IS THE NODE, not a relation beside it. What is being
    /// refined arrives whole and the authority reads its relation out of it,
    /// so a caller cannot name one operand while refining another.
    pub(crate) fn refine_relation<P, Q>(
        &self,
        node: crate::pipeline::asts::core::Chain<P>,
        refine: impl FnOnce(
            crate::pipeline::asts::core::Chain<P>,
        ) -> Result<crate::pipeline::asts::core::Chain<Q>>,
    ) -> Result<Refinement<Q>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
        Q: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let operand = node.semantic_relation();
        check_mark(self.mark, &operand)?;
        let produced = refine(node)?;
        match self.replacement_made(operand, produced.semantic_relation())? {
            Made::Preserved => Ok(Refinement::Preserved(produced)),
            Made::Rebuilt(map) => Ok(Refinement::Rebuilt {
                chain: produced,
                map,
            }),
            // NOT A THIRD OUTCOME. This caller said it depends on the
            // correspondence by taking this road; a refinement that stood
            // its operand aside rather than rebuilding it leaves nothing to
            // depend on, and that is a refusal, not a quieter success.
            Made::Resited => Err(replacement_error(
                "a refinement published a relation its operand did not build",
            )),
        }
    }

    /// REBUILD AN OPERAND IN PLACE, JUDGED.
    ///
    /// The one road that lets a rewrite stand a DIFFERENT relation where an
    /// operand stood while the node above keeps its identity: the product
    /// is the operand itself, or a relation derived from it that carries
    /// every position the operand published — recorded here as the
    /// operand's replacement, so the boundary standing over it translates
    /// through the record rather than through a resemblance — and anything
    /// else refuses. The answer names the operand it replaces, so the
    /// carrier that admits it can refuse a replacement judged for some
    /// other operand.
    pub(crate) fn rebuilt<P>(
        &self,
        operand: crate::pipeline::asts::core::Chain<P>,
        rebuild: impl FnOnce(
            crate::pipeline::asts::core::Chain<P>,
        ) -> Result<crate::pipeline::asts::core::Chain<P>>,
    ) -> Result<Replacement<P>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let of = operand.semantic_relation();
        let chain = match self.refine_relation(operand, rebuild)? {
            Refinement::Preserved(chain) | Refinement::Rebuilt { chain, .. } => chain,
        };
        Ok(Replacement { chain, of })
    }

    /// REFINE A CHAIN WHOSE SHAPE THE REFINER MAY REPLACE WHOLE.
    ///
    /// The routing hub's road, and a DIFFERENT operation from refining an
    /// operand. A chain reaching it may come back as the same relation, as
    /// one derived from it, or as a statement built over the operand's own
    /// SOURCES: the FAR cycle flattens a chain into the tables and
    /// predicates it is made of and builds over those, which stands one
    /// level DOWN from the operand rather than above it — so no position
    /// of the operand was carried into it and there is no map to write.
    ///
    /// A replacement is recorded where the construction record shows one
    /// and absent where it does not. Nothing is invented either way, and
    /// the hub answers with the node alone because a router has no use for
    /// a map: the boundary that needs the translation asks the store.
    /// A caller that DEPENDS on the correspondence takes
    /// [`SemanticBuilder::refine_relation`], which refuses without one.
    pub(crate) fn refine_segment<P, Q>(
        &self,
        node: crate::pipeline::asts::core::Chain<P>,
        refine: impl FnOnce(
            crate::pipeline::asts::core::Chain<P>,
        ) -> Result<crate::pipeline::asts::core::Chain<Q>>,
    ) -> Result<crate::pipeline::asts::core::Chain<Q>>
    where
        P: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
        Q: crate::pipeline::asts::core::Phase<Scope = SemanticRelation>,
    {
        let operand = node.semantic_relation();
        check_mark(self.mark, &operand)?;
        let produced = refine(node)?;
        let _ = self.replacement_made(operand, produced.semantic_relation())?;
        Ok(produced)
    }

    /// WHAT A REFINEMENT DID TO THE OPERAND IT WAS GIVEN, from the
    /// construction record and nothing else.
    ///
    /// Every derivation wrote down the relations it consumed, so "was this
    /// built from that" is a recorded edge, not a resemblance. And the
    /// carry edges say where each position went. A rebuild that descends
    /// from its operand and does not carry every one of its positions is
    /// not a replacement of it, and says so rather than being completed
    /// with a guess.
    ///
    /// Recording happens HERE, once, in the same act that decides there is
    /// something to record.
    fn replacement_made(&self, operand: SemanticRelation, now: SemanticRelation) -> Result<Made> {
        if now == operand {
            return Ok(Made::Preserved);
        }
        // DID EVERY POSITION OF THE OPERAND LAND SOMEWHERE IN THE RESULT?
        // The carry edges construction wrote are the whole answer — a
        // rebuild carries its operand's positions and a resited segment
        // carries none of them, because it was built over the operand's
        // SOURCES rather than over the operand. Not total is not a map:
        // one position with no recorded landing means these two are not
        // one operand rebuilt, and there is nothing here to guess with.
        let Some(map) = self.descent(operand, now)? else {
            return Ok(Made::Resited);
        };
        self.registry.relations().record_replacement(
            operand.relation(),
            now.relation(),
            map.clone(),
        );
        Ok(Made::Rebuilt(map))
    }

    /// Where one operand port landed in a rebuilt relation, from the record
    /// the rebuild wrote.
    ///
    /// ONE recorded shape, and no second. The rebuild DESCENDS from the
    /// operand — it wrapped, joined or exported it — and the carry edges
    /// construction wrote say where each position went. A position with no
    /// recorded landing answers `None`, and that is a refusal for whoever
    /// needed the two related.
    ///
    /// NOTHING IS SEARCHED HERE. A lineage walk over the ports two finished
    /// relations happen to share is not evidence that one replaced the
    /// other; it is a resemblance, and a relationship recovered from one is
    /// the class of bug this authority exists to make unrepresentable.
    /// WHERE A POSITION OF THE OPERAND LANDED in what an operation
    /// publishes — the carry edge the derivation just wrote, position by
    /// position. An embed ADDS positions, so counting them asks a
    /// different question from following them, and a position the
    /// operation does not carry REFUSES rather than landing at whatever
    /// sits at its old ordinal.
    fn followed(&self, result: &SemanticRelation, source: PortId) -> Result<PortId> {
        self.landed_port(result, source)?.ok_or_else(|| {
            Internal::invariant(
                "publication",
                "a rebuilt operation does not carry a position the one it replaces published",
            )
        })
    }

    /// THIS OPERATION REPLACES THAT RELATION, and here is where each of
    /// its positions landed.
    ///
    /// PRODUCED WHILE THE REPLACEMENT IS BEING CONSTRUCTED, from the carry
    /// edges the derivation has just written — not asked afterwards about
    /// two relations that are both already finished. Every position of the
    /// replaced relation must land somewhere in this one, and a position
    /// that does not REFUSES: not total is not a map. The record is
    /// written in the same act, so a later reader that needs the
    /// translation asks the store rather than reconstructing it.
    fn record_replacement_of(
        &self,
        was: SemanticRelation,
        now: &SemanticRelation,
    ) -> Result<TotalPortMap> {
        check_mark(self.mark, &was)?;
        let before = self.interface(&was)?;
        let mut pairs = Vec::with_capacity(before.width());
        for port in before.ports().iter().copied() {
            pairs.push((port, self.followed(now, port)?));
        }
        let map = TotalPortMap {
            from: was.relation(),
            to: now.relation(),
            pairs,
        };
        self.registry
            .relations()
            .record_replacement(was.relation(), now.relation(), map.clone());
        Ok(map)
    }

    fn landed_port(&self, relation: &SemanticRelation, source: PortId) -> Result<Option<PortId>> {
        let interface = self.interface(relation)?;
        if interface.ports().contains(&source) {
            return Ok(Some(source));
        }
        let mut answers = translated_ports_for(self.registry, relation)?
            .into_iter()
            .filter_map(|(old, new)| (old == source).then_some(new));
        match (answers.next(), answers.next()) {
            (Some(port), None) => return Ok(Some(port)),
            (Some(_), Some(_)) => {
                return Err(replacement_error(
                    "one operand port translates to several positions of one rebuild",
                ))
            }
            (None, _) => {}
        }
        Ok(None)
    }

    /// WHICH STATED SOURCE POSITIONS A POSITION STANDS OVER.
    ///
    /// The walk descends every carry the record wrote into a position and
    /// stops the moment it reaches a position of the stated set, so what it
    /// concludes over is that set and the record between — never everything
    /// a lineage happens to touch. A merged key carries SEVERAL, and the
    /// several are the answer rather than an ambiguity: which sources a
    /// position stands for is exactly what tells it from its siblings. The
    /// answer is ordered by the stated set so two of them compare.
    ///
    /// A READ IS WHERE AN OCCURRENCE BEGINS, AND THE WALK ENDS THERE. A read
    /// of a definition may itself be a stated source, reached above; behind
    /// it lies the definition, which every read of it shares, and two reads
    /// meeting there would pair positions of one row stream with another's.
    ///
    /// This pairs POSITIONS. It is not the judgment of what an operation is
    /// made of — that is [`Self::operation_over`], read from operands.
    fn reach(&self, port: PortId, out_of: &[PortId]) -> Vec<PortId> {
        let store = self.registry.relations();
        let mut reached: Vec<PortId> = Vec::new();
        let mut seen = vec![port];
        let mut walking = vec![port];
        while let Some(next) = walking.pop() {
            if out_of.contains(&next) {
                if !reached.contains(&next) {
                    reached.push(next);
                }
                continue;
            }
            if store.is_read_boundary(next) {
                continue;
            }
            // EVERY CARRY THE RECORD WROTE INTO THIS POSITION, not lineage
            // alone: a merged key is carried from its left half and the
            // join's translation act records its right half.
            let mut back = store.carried_from(next);
            // A SET POSITION STANDS OVER EVERY ARM. Its carry edge names the
            // opening arm alone; the contribution table the set was built
            // from names what each arm puts through it.
            if let Some(owner) = store.relation_of(next) {
                if let Some(matrix) = store.contributions(owner) {
                    for output in matrix.outputs() {
                        if output.result() != next {
                            continue;
                        }
                        for cell in output.by_arm().iter() {
                            if let Contribution::Port(port) = cell {
                                back.push(*port);
                            }
                        }
                    }
                }
                // A SOURCE MAY ITSELF HAVE BEEN REBUILT. An inner segment
                // states what it replaced while it builds, so the position a
                // rebuilt table publishes reaches the position the operand
                // stood on through that record — the same construction
                // evidence the carry edges are, one relation down.
                for map in store.replacements_into(owner) {
                    for (was, now) in map.pairs() {
                        if *now == next {
                            back.push(*was);
                        }
                    }
                }
            }
            for source in back {
                if !seen.contains(&source) {
                    seen.push(source);
                    walking.push(source);
                }
            }
        }
        let mut answer: Vec<_> = out_of
            .iter()
            .copied()
            .filter(|source| reached.contains(source))
            .collect();
        answer.dedup();
        answer
    }

    /// WHETHER AN OPERATION IS MADE OF EXACTLY THE STATED OCCURRENCES, by
    /// the operands each deriving act recorded.
    ///
    /// The walk descends the recorded operands. A node that stands for a
    /// stated occurrence — the occurrence itself, a recorded replacement of
    /// it, or a republication of it — is a leaf and marks that occurrence
    /// reached. A join descends into both operands and a republication into
    /// its one; every other operation — a set, a minus, a group, a read, a
    /// destructure — is a participant the rebuild does not reproduce, and
    /// the answer is no. Nothing here reads positions: an operand whose
    /// column a projection dropped still determines the rows.
    fn operation_over(
        &self,
        node: SemanticRelation,
        stated: &[SemanticRelation],
        reached: &mut Vec<usize>,
        seen: &mut Vec<RelationId>,
    ) -> Result<bool> {
        for (index, occurrence) in stated.iter().enumerate() {
            if self.stands_for(occurrence, &node)? {
                if !reached.contains(&index) {
                    reached.push(index);
                }
                return Ok(true);
            }
        }
        if seen.contains(&node.relation()) {
            return Ok(false);
        }
        seen.push(node.relation());
        let store = self.registry.relations();
        let inputs = store.inputs(node.relation());
        if store.is_join(node.relation()) {
            let [left, right] = inputs.as_slice() else {
                return Ok(false);
            };
            return Ok(self.operation_over(*left, stated, reached, seen)?
                && self.operation_over(*right, stated, reached, seen)?);
        }
        if store.is_republication(node.relation()) {
            let [input] = inputs.as_slice() else {
                return Ok(false);
            };
            return self.operation_over(*input, stated, reached, seen);
        }
        Ok(false)
    }

    /// WHETHER A RELATION STANDS OVER EXACTLY THE STATED OCCURRENCES.
    ///
    /// The judgment is the operation's: its recorded operand tree is made of
    /// joins and republications whose leaves stand for the stated
    /// occurrences, and every stated occurrence is a leaf. Only then are the
    /// relation's positions paired with what they stand over — the stated
    /// positions each one's recorded carries reach — for the map a rebuild
    /// writes. `Some` carries that pairing; `None` says these occurrences
    /// are not what the relation stands over, so nothing about them may be
    /// certified. A position that reaches no stated position cannot be
    /// paired, and the answer is likewise `None`.
    fn standing_over(
        &self,
        relation: &SemanticRelation,
        occurrences: &[SemanticRelation],
    ) -> Result<Option<Vec<(PortId, Vec<PortId>)>>> {
        let mut reached = Vec::new();
        if !self.operation_over(*relation, occurrences, &mut reached, &mut Vec::new())?
            || reached.len() != occurrences.len()
        {
            return Ok(None);
        }
        let mut sources = Vec::new();
        for occurrence in occurrences {
            sources.extend_from_slice(self.interface(occurrence)?.ports());
        }
        let mut standing = Vec::new();
        for port in self.interface(relation)?.ports().iter().copied() {
            let from = self.reach(port, &sources);
            if from.is_empty() {
                return Ok(None);
            }
            standing.push((port, from));
        }
        Ok(Some(standing))
    }

    /// WHETHER ONE RELATION IS A REPUBLICATION OF ANOTHER, by the record:
    /// the relation itself, or a row-preserving single-operand derivation
    /// whose operand is (recursively) a republication of it. Read from what
    /// each deriving act recorded about its form and its operand — never
    /// from the positions carried, which a set's first arm or a join's left
    /// side carry just as well without republishing anything.
    fn republication_of(&self, original: &SemanticRelation, relation: &SemanticRelation) -> bool {
        let store = self.registry.relations();
        let mut current = *relation;
        let mut seen: Vec<RelationId> = Vec::new();
        loop {
            if current == *original {
                return true;
            }
            if seen.contains(&current.relation()) || !store.is_republication(current.relation()) {
                return false;
            }
            seen.push(current.relation());
            let inputs = store.inputs(current.relation());
            let [input] = inputs.as_slice() else {
                return false;
            };
            current = *input;
        }
    }

    /// Whether a relation IS a stated occurrence or stands in its place by
    /// record: the occurrence itself, a relation a rebuild recorded as
    /// replacing it, or a republication of it. Never a resemblance, never a
    /// sibling, never a relation that merely carries the occurrence's
    /// positions beside something else.
    fn stands_for(
        &self,
        occurrence: &SemanticRelation,
        relation: &SemanticRelation,
    ) -> Result<bool> {
        if occurrence == relation {
            return Ok(true);
        }
        if self
            .registry
            .relations()
            .replacement(occurrence.relation(), relation.relation())
            .is_some()
        {
            return Ok(true);
        }
        Ok(self.republication_of(occurrence, relation))
    }

    /// TWO ZERO-WIDTH NARROWINGS OF ONE GRID ARE ONE OPERAND. A rebuild
    /// re-derives the resolver's all-consumed narrowing of an anonymous read
    /// over the same grid; neither publishes a position, so the replacement
    /// carries no map and grants no alias — it says only that the later
    /// narrowing stands where the earlier one stood. Anything wider, or
    /// narrowed from anything else, is refused.
    pub(crate) fn narrowed_again(
        &self,
        was: SemanticRelation,
        now: &SemanticRelation,
    ) -> Result<()> {
        check_mark(self.mark, &was)?;
        check_mark(self.mark, now)?;
        if !self.interface(&was)?.ports().is_empty() || !self.interface(now)?.ports().is_empty() {
            return Err(replacement_error(
                "a re-derived narrowing replaces a narrowing only when both publish nothing",
            ));
        }
        let store = self.registry.relations();
        // A NARROWING IS A REPUBLICATION OF ITS GRID, and the record says
        // which derivations were. Publishing nothing is not the act: a
        // grouping to no columns publishes nothing too, and one row of it
        // stands for many rows of the grid.
        if !store.is_republication(was.relation()) || !store.is_republication(now.relation()) {
            return Err(replacement_error(
                "a re-derived narrowing replaces a narrowing only when both republish the grid",
            ));
        }
        let (before, after) = (store.inputs(was.relation()), store.inputs(now.relation()));
        match (before.as_slice(), after.as_slice()) {
            ([grid], [again]) if grid == again => {}
            _ => {
                return Err(replacement_error(
                    "a re-derived narrowing replaces a narrowing of the same one grid",
                ))
            }
        }
        store.record_replacement(
            was.relation(),
            now.relation(),
            TotalPortMap {
                from: was.relation(),
                to: now.relation(),
                pairs: Vec::new(),
            },
        );
        Ok(())
    }

    /// OPEN A REBUILD of one operand over the occurrences it stands over.
    ///
    /// The FAR cycle flattens a segment into the tables it is made of and
    /// builds over THOSE, so what it publishes is the operand's sibling
    /// rather than its descendant, and no carry edge runs between them. What
    /// relates them is the occurrences both stand over, and the rebuild is
    /// the act that consumes them again — so the rebuild is performed HERE,
    /// as one operation of the authority, and the replacement it certifies
    /// is evidence that operation produced: the operand was judged to stand
    /// over exactly these occurrences, the product is derived by this
    /// operation over exactly these occurrences in this order, and nothing
    /// assembled elsewhere can be handed in as the product. Independently
    /// valid ingredients — a finished relation, a source list, a chain —
    /// cannot be paired into a replacement from outside.
    ///
    /// An operand that does not stand over exactly the stated occurrences
    /// opens a rebuild that certifies nothing: the product is still
    /// derived, and a reference the refiner cannot translate onto it then
    /// refuses where it is written.
    pub(crate) fn rebuilding(
        self,
        operand: SemanticRelation,
        occurrences: &[SemanticRelation],
    ) -> Result<Rebuild<'r>> {
        check_mark(self.mark, &operand)?;
        for occurrence in occurrences {
            check_mark(self.mark, occurrence)?;
        }
        if occurrences.is_empty() {
            return Err(replacement_error(
                "a rebuild stands over at least one occurrence",
            ));
        }
        let operand_sources = self.standing_over(&operand, occurrences)?;
        Ok(Rebuild {
            authority: self,
            operand,
            occurrences: occurrences.to_vec(),
            operand_sources,
            consumed: 0,
            product: None,
        })
    }

    /// The total old-to-new map a rebuild WROTE DOWN while it built.
    ///
    /// Every position of the operand is looked up in the record the
    /// rebuilding derivations left — the carried-source and replacement
    /// edges each `derive` writes. A position with no recorded descent, or
    /// one that reached two positions of the result, refuses: neither is a
    /// translation a later reader could act on.
    fn descent(
        &self,
        old: SemanticRelation,
        new: SemanticRelation,
    ) -> Result<Option<TotalPortMap>> {
        check_mark(self.mark, &new)?;
        // A REBUILD THAT ALREADY SAID WHAT IT REPLACED is the record itself:
        // the map the operation wrote, not one reconstructed from carries.
        if let Some(map) = self
            .registry
            .relations()
            .replacement(old.relation(), new.relation())
        {
            return Ok(Some(map));
        }
        // CARRYING EVERY POSITION IS NOT DESCENDING. A set carries its first
        // arm's positions and a join its left side's; neither republishes
        // its operand. Short of a recorded replacement, the record of each
        // deriving act must say the road from the new relation down to the
        // old one is republication all the way; only then is there a map to
        // write.
        if !self.republication_of(&old, &new) {
            return Ok(None);
        }
        let before = self.interface(&old)?;
        let mut pairs = Vec::with_capacity(before.width());
        for port in before.ports().iter().copied() {
            let Some(landed) = self.landed_port(&new, port)? else {
                // NOT TOTAL IS NOT A MAP. One position with no recorded
                // landing means the two relations are not one operand
                // rebuilt, and there is nothing here to guess with.
                return Ok(None);
            };
            pairs.push((port, landed));
        }
        Ok(Some(TotalPortMap {
            from: old.relation(),
            to: new.relation(),
            pairs,
        }))
    }

    /// Whether `new` is the same construction-owned occurrence as `old`.
    /// A recorded descent proves a direct rebuild. Otherwise every published
    /// position must retain the same origin in the same order; sharing an
    /// input relation or an entity name is deliberately insufficient.
    pub(crate) fn continues_exactly(
        &self,
        old: SemanticRelation,
        new: SemanticRelation,
    ) -> Result<bool> {
        if old == new || self.descent(old, new)?.is_some() {
            return Ok(true);
        }
        let old_ports = self.interface(&old)?.ports().to_vec();
        let new_ports = self.interface(&new)?.ports().to_vec();
        if old_ports.len() == new_ports.len()
            && old_ports.iter().zip(&new_ports).all(|(old, new)| {
                self.registry.relations().origin(*old) == self.registry.relations().origin(*new)
            })
        {
            return Ok(true);
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(crate) fn report_replacement_for_test(
        &self,
        old: SemanticRelation,
        new: SemanticRelation,
    ) -> Result<()> {
        let map = self.descent(old, new)?.ok_or_else(|| {
            replacement_error("a replacement carries none of the operand's positions")
        })?;
        self.registry
            .relations()
            .record_replacement(old.relation(), new.relation(), map);
        Ok(())
    }

    /// The interface a relation publishes, read from the record.
    pub fn interface(&self, relation: &SemanticRelation) -> Result<Interface> {
        check_mark(self.mark, relation)?;
        Ok(interface_of(self.registry, relation))
    }

    /// The exact current position carrying an already-resolved reference.
    pub(crate) fn port_in(
        &self,
        relation: &SemanticRelation,
        referenced: PortId,
    ) -> Result<PortId> {
        check_mark(self.mark, relation)?;
        if self.interface(relation)?.ports().contains(&referenced) {
            return Ok(referenced);
        }
        translated_ports_for(self.registry, relation)?
            .into_iter()
            .find_map(|(source, output)| (source == referenced).then_some(output))
            .ok_or_else(|| {
                Internal::invariant(
                    "semantic relation",
                    "a resolved reference is not carried by its input relation",
                )
            })
    }

    /// Whether this exact relation occurrence carries a resolved port.
    pub(crate) fn carries(&self, relation: &SemanticRelation, referenced: PortId) -> Result<bool> {
        check_mark(self.mark, relation)?;
        if self.interface(relation)?.ports().contains(&referenced) {
            return Ok(true);
        }
        if translated_ports_for(self.registry, relation)?
            .into_iter()
            .any(|(source, _)| source == referenced)
        {
            return Ok(true);
        }
        // THE SUPPORT IT EMITS IS CARRIED TOO. A hoisted correlation names
        // an interior occurrence the boundary keeps readable beside its
        // heading — under that occurrence's own identity, or under a
        // position that continues it.
        Ok(self
            .registry
            .relations()
            .support_emitted(relation.relation())
            .into_iter()
            .any(|emitted| {
                emitted == referenced || super::stands_where(self.registry, emitted, referenced)
            }))
    }

    /// Every exact ancestor port construction carried into one output.
    pub(crate) fn ancestors_into(
        &self,
        relation: &SemanticRelation,
        output: PortId,
    ) -> Result<Vec<PortId>> {
        check_mark(self.mark, relation)?;
        if !self.interface(relation)?.ports().contains(&output) {
            return Err(replacement_error(
                "an output ancestry query names a port outside its relation",
            ));
        }
        let mut ancestors = vec![output];
        ancestors.extend(
            translated_ports_for(self.registry, relation)?
                .into_iter()
                .filter_map(|(source, destination)| (destination == output).then_some(source)),
        );
        ancestors.sort_unstable();
        ancestors.dedup();
        Ok(ancestors)
    }

    /// Close the epoch: hand out a reader and nothing else.
    ///
    /// The closure is a STATE of the compilation, not of this builder. A
    /// module that reaches the registry afterwards and asks for an
    /// authority receives one that refuses to construct, so the seal cannot
    /// be walked around by building a second one. The source fence still
    /// asserts that no module past refinement asks.
    // ------------------------------------------------------------ interior

    /// Record the relation and the exact ordered interface this derivation
    /// built for it, as one act.
    ///
    /// The interface is what the authority answers with from here on. It is
    /// NOT re-read from the scope's heading: a heading keeps growing for as
    /// long as any column-minting road survives, and an interface that
    /// followed it would be a property of the registry's current state
    /// rather than of the relation.
    fn store(&self, scope: ScopeId, ports: Interface) -> Result<SemanticRelation> {
        let owners: Vec<_> = ports
            .ports()
            .iter()
            .copied()
            .map(|port| (port, self.registry.relations().owner(port).unwrap_or(scope)))
            .collect();
        let carried: Vec<_> = ports
            .ports()
            .iter()
            .flat_map(|port| {
                self.registry
                    .relations()
                    .lineage(*port)
                    .into_iter()
                    .map(|source| (source, *port))
            })
            .collect();
        let relation = self.registry.relations().fix(scope, ports)?;
        self.registry
            .relations()
            .record_translations(relation.relation(), carried.iter().copied());
        for (port, owner) in owners {
            self.registry.relations().record_owner(port, owner);
        }
        Ok(relation)
    }

    fn execute(&self, form: &RelForm<'_>, law: InterfaceLaw<'_>) -> Result<SemanticRelation> {
        match law {
            InterfaceLaw::New => self.new_heading(form),
            // THE PRESERVE LAW IS THE SAME VALUE. A transparent step does
            // not create an occurrence, so it does not create an interface
            // either — returning the input is the law, not a shortcut.
            InterfaceLaw::Preserve { input } => Ok(input.clone()),
            InterfaceLaw::Export { input, why } => self.export(input, why),
            InterfaceLaw::Project { input, slots } => self.project(form, input, slots),
            InterfaceLaw::ErBoundary { input, exports } => self.er_boundary(input, exports),
            InterfaceLaw::Edit { input, edit } => self.edit(form, input, edit),
            InterfaceLaw::Group {
                input,
                kind,
                keys,
                reductions,
            } => self.group(input, kind, keys, reductions),
            InterfaceLaw::Concatenate {
                left,
                right,
                merged,
            } => self.concatenate(left, right, merged),
            InterfaceLaw::Merge { alignment, arms } => self.merge(alignment, arms),
            InterfaceLaw::MinusLeft { left, right } => self.minus(left, right),
            InterfaceLaw::Instantiate { template } => self.instantiate(form, template),
            InterfaceLaw::Explode {
                input,
                interior_of,
                selected,
                selection,
            } => self.explode(input, interior_of, selected, selection),
            InterfaceLaw::Narrow { input, nest, bound } => {
                if !self.interface(input)?.ports().contains(&nest) {
                    return Err(replacement_error(
                        "a narrowing's nest is absent from its exact input interface",
                    ));
                }
                self.project(form, input, bound)
            }
            InterfaceLaw::Interior { owner, body } => self.interior(owner, body),
            InterfaceLaw::Materialize { why, publishes } => self.materialize(form, why, publishes),
            InterfaceLaw::Fixed { shape } => self.fixed(shape),
            InterfaceLaw::Opaque => {
                let scope = self.registry.opaque_scope();
                self.store(scope, Interface::opaque())
            }
        }
    }

    fn new_heading(&self, form: &RelForm<'_>) -> Result<SemanticRelation> {
        let (scope, ports) = match form {
            RelForm::Source(spec) => {
                let entity = match spec.origin {
                    SourceOrigin::Catalog { entity } | SourceOrigin::TableValued { entity } => {
                        Some(entity)
                    }
                };
                let scope = match (entity, spec.answers_to) {
                    (Some(entity), Some(answer)) => self.registry.base_table_scope(entity, answer),
                    _ => self.registry.anonymous_scope(spec.answers_to),
                };
                let ports = spec
                    .slots
                    .iter()
                    .map(|slot| {
                        let addressing = match slot.named {
                            Some(_) => Addressing::Published,
                            None => Addressing::Latent,
                        };
                        self.mint(
                            scope,
                            slot.named,
                            addressing,
                            ValueFacts {
                                declared_type: slot.declared_type.clone(),
                                tree_valued: slot.interior,
                                ..ValueFacts::default()
                            },
                        )
                    })
                    .collect();
                (scope, ports)
            }
            RelForm::Anonymous(spec) => {
                let scope = self.registry.anonymous_scope(spec.answers_to());
                let mut ports = Vec::with_capacity(spec.slots().len());
                for (index, slot) in spec.slots().iter().enumerate() {
                    let (_position, named, addressing, declared_type, shape) = match slot {
                        AnonymousSlot::Binder {
                            position,
                            named,
                            declared_type,
                            shape,
                        } => (
                            *position,
                            Some(*named),
                            // The binder is the caller's own bare lvar;
                            // under an alias its complete name is
                            // qualified. Which name is the lexical
                            // frontier's fact.
                            if spec.answers_to().is_some() {
                                Addressing::BareUnder
                            } else {
                                Addressing::Bare
                            },
                            declared_type.clone(),
                            *shape,
                        ),
                        AnonymousSlot::Literal {
                            position,
                            declared_type,
                            shape,
                        }
                        | AnonymousSlot::Inferred {
                            position,
                            declared_type,
                            shape,
                        } => (
                            *position,
                            None,
                            Addressing::Published,
                            declared_type.clone(),
                            *shape,
                        ),
                        AnonymousSlot::Constraint {
                            position,
                            declared_type,
                            shape,
                        } => (
                            *position,
                            None,
                            Addressing::Hygienic,
                            declared_type.clone(),
                            *shape,
                        ),
                        AnonymousSlot::Declared { position, named } => (
                            *position,
                            *named,
                            if named.is_some() {
                                Addressing::Published
                            } else {
                                Addressing::Latent
                            },
                            None,
                            crate::names::ValueShape::Unknown,
                        ),
                    };
                    let port = self.mint(
                        scope,
                        named,
                        addressing,
                        ValueFacts {
                            declared_type,
                            shape,
                            ..ValueFacts::default()
                        },
                    );
                    // THE MINT ACT WRITES THE REUSE EDGE, as the caller
                    // pattern's bind act does: the lexical authority decided
                    // the live port this position reuses and the spec carries
                    // that decision beside the slot it was judged for; the
                    // record pairs it with the port minted here, and the join
                    // that owns the reused port consumes the record.
                    if let Some(reused) = spec.reuse_at(index) {
                        self.registry.relations().record_reuse(port, reused)?;
                    }
                    ports.push(port);
                }
                (scope, ports)
            }
            _ => unreachable!("only a source or an anonymous relation takes the New law"),
        };
        self.store(scope, Interface::of(ports))
    }

    fn export(&self, input: &SemanticRelation, why: ExportWhy) -> Result<SemanticRelation> {
        let from = input.scope();
        let kind = boundary_of_export(why);
        let scope = scope_of_export(self.registry, from, why);
        let source = self.interface(input)?;
        if source.is_opaque() {
            // THE OPACITY CROSSES TOO. A boundary over a relation whose
            // dimensions are unenumerable must not turn "the heading is
            // unknown" into "the heading has none".
            return self.store(scope, Interface::opaque());
        }
        let mut ports = Vec::new();
        for column in source.ports().iter().map(|port| port.column()) {
            let addressing = self.registry.addressing(column);
            let published = if matches!(why, ExportWhy::EmissionAlias) {
                None
            } else {
                self.registry.published(column)
            };
            let addressing = match why {
                ExportWhy::EmissionAlias => Addressing::Bare,
                _ => addressing,
            };
            ports.push(self.carry(
                kind,
                scope,
                column,
                published,
                addressing,
                Continuity::Continues,
            ));
        }
        self.store(scope, Interface::of(ports))
    }

    /// AN EDGE PUBLISHES ITS ENDPOINTS' COLUMNS, and each keeps answering
    /// to the endpoint it belongs to. That answering channel is what a
    /// composed path pairs on and what a caller's `A.x` reaches through, so
    /// it travels with the position instead of being recovered afterwards
    /// from a name.
    fn er_boundary(
        &self,
        input: &SemanticRelation,
        exports: &[super::form::ErExport],
    ) -> Result<SemanticRelation> {
        let scope = self
            .registry
            .wrap_scope(input.scope(), WrapReason::Projection);
        let mut ports = Vec::with_capacity(exports.len());
        for export in exports {
            let column = export.source.column();
            let published = self.registry.published(column);
            // The position keeps publishing its own name under its
            // endpoint's. That it is reachable as `users_t.name` is a fact
            // about the endpoint the edge stands over, held by the lexical
            // frontier that binds the endpoint's name — never by the column.
            ports.push(self.carry(
                CarryOwner::Preserve,
                scope,
                column,
                published,
                Addressing::BareUnder,
                Continuity::Continues,
            ));
        }
        self.store(scope, Interface::of(ports))
    }

    fn project(
        &self,
        form: &RelForm<'_>,
        input: &SemanticRelation,
        slots: &[ProjectSlot],
    ) -> Result<SemanticRelation> {
        let scope = self.registry.wrap_scope(input.scope(), wrap_of(form));
        // AN ACCESS OF THE WHOLE HEADING IS TRANSPARENT TO THE RELATION IT
        // READS. `t *` asks for every dimension in order and asks for
        // nothing else, so the positions still BELONG to the relation the
        // author named — reading it does not make them a new relation's.
        // A named access is the opposite: it states which dimensions this
        // read has, and those are its own.
        let whole_read = matches!(
            form,
            RelForm::Access(spec) if spec.shape == super::form::AccessShape::Whole
        );
        let owner = if whole_read {
            CarryOwner::Preserve
        } else {
            CarryOwner::New
        };
        let ports = self.publish_slots(
            scope,
            slots,
            !matches!(form, RelForm::Project(_)),
            owner,
            SlotContinuity::Decide,
        );
        self.store(scope, Interface::of(ports))
    }

    fn edit(
        &self,
        form: &RelForm<'_>,
        input: &SemanticRelation,
        edit: HeadingEdit<'_>,
    ) -> Result<SemanticRelation> {
        let from = input.scope();
        let scope = self.registry.wrap_scope(from, wrap_of(form));
        let mut ports: Vec<PortId> = Vec::new();
        match edit {
            HeadingEdit::Rename(renames) => {
                // A RENAMING TOUCHES NAMES, NOT PLACES — and the rename
                // stage is a PIPE FORM: it dequalifies the whole heading it
                // publishes, so every named position crosses as a BARE live
                // lvar. Hygienic and latent positions keep their roles; the
                // stage neither reveals nor activates them.
                for column in self.operand_heading(input) {
                    let published = renames
                        .iter()
                        .find(|slot| slot.source.0 == column)
                        .map(|slot| slot.to)
                        .or_else(|| self.registry.published(column));
                    // The PROPOSAL is the publication's own fact; the
                    // boundary act then crosses it as the stage's own bare
                    // publication.
                    let addressing = match self.registry.addressing(column) {
                        Addressing::Hygienic => Addressing::Hygienic,
                        Addressing::Latent => Addressing::Latent,
                        _ => Addressing::Published,
                    };
                    ports.push(self.carry(
                        CarryOwner::New,
                        scope,
                        column,
                        published,
                        addressing,
                        Continuity::Continues,
                    ));
                }
            }
            HeadingEdit::Reposition(moves) => {
                let mut heading = self.operand_heading(input);
                let mut ordered: Vec<ColId> = Vec::with_capacity(heading.len());
                for slot in moves {
                    if let Some(at) = heading.iter().position(|c| *c == slot.source.0) {
                        ordered.push(heading.remove(at));
                    }
                }
                ordered.extend(heading);
                for column in ordered {
                    let published = self.registry.published(column);
                    let addressing = self.registry.addressing(column);
                    ports.push(self.carry(
                        CarryOwner::New,
                        scope,
                        column,
                        published,
                        addressing,
                        Continuity::Continues,
                    ));
                }
            }
            HeadingEdit::Remove(removed) => {
                for column in self.operand_heading(input) {
                    if removed.iter().any(|port| port.0 == column) {
                        continue;
                    }
                    let published = self.registry.published(column);
                    let addressing = self.registry.addressing(column);
                    ports.push(self.carry(
                        CarryOwner::New,
                        scope,
                        column,
                        published,
                        addressing,
                        Continuity::Continues,
                    ));
                }
            }
            HeadingEdit::Cover { kind, cells } => {
                for column in self.operand_heading(input) {
                    let cell = cells.iter().find(|cell| cell.covered.0 == column);
                    let published = self.registry.published(column);
                    let addressing = self.registry.addressing(column);
                    let written = cell.is_some_and(|cell| cell.writes)
                        && matches!(kind, CoverKind::Map | CoverKind::Transform);
                    let port = self.carry_with(
                        CarryOwner::New,
                        scope,
                        column,
                        published,
                        addressing,
                        // A written slot holds a DIFFERENT value: the
                        // position's identity stays, the occurrence of the
                        // value it held does not continue.
                        if written {
                            Continuity::Republishes
                        } else {
                            Continuity::Continues
                        },
                        !written,
                        if written {
                            CarriedValue::Rewritten
                        } else {
                            CarriedValue::Same
                        },
                        |facts| {
                            // A COVER KEEPS THE SLOT'S IDENTITY and writes
                            // a different value into it, so downstream
                            // references still find the position while the
                            // fact that it was written travels with the
                            // value.
                            if written {
                                facts.written_by_a_cover = true;
                            }
                        },
                    );
                    if written {
                        self.registry.relations().record_new_value(port);
                    }
                    ports.push(port);
                }
                if matches!(kind, CoverKind::EmbedMap) {
                    for cell in cells {
                        ports.push(self.mint(
                            scope,
                            naming_spelling(cell.naming),
                            proposed_role(cell.naming),
                            ValueFacts::default(),
                        ));
                    }
                }
            }
            HeadingEdit::Extend(added) => {
                // AN EXTENSION LEAVES THE OPERAND WHERE IT WAS. An embed and
                // a destructuring both add positions beside the whole
                // operand. Whether the operand's qualifier still reaches
                // them is the lexical frontier's answer at the position the
                // reference is written — a pipe's far side has none.
                for column in self.operand_heading(input) {
                    let published = self.registry.published(column);
                    let addressing = self.registry.addressing(column);
                    ports.push(self.carry(
                        CarryOwner::New,
                        scope,
                        column,
                        published,
                        addressing,
                        Continuity::Continues,
                    ));
                }
                // Every added position is NEW beside the operand run that
                // already continued the operand's positions.
                ports.extend(self.publish_slots(
                    scope,
                    added,
                    true,
                    CarryOwner::New,
                    SlotContinuity::Republish,
                ));
            }
        }
        self.store(scope, Interface::of(ports))
    }

    fn group(
        &self,
        input: &SemanticRelation,
        _kind: GroupKind,
        keys: &[ProjectSlot],
        reductions: &[ReductionSlot],
    ) -> Result<SemanticRelation> {
        let scope = self
            .registry
            .wrap_scope(input.scope(), WrapReason::Aggregate);
        let mut ports =
            self.publish_slots(scope, keys, true, CarryOwner::New, SlotContinuity::Decide);
        for reduction in reductions {
            match reduction {
                ReductionSlot::Value { slot } | ReductionSlot::Delegate { slot } => {
                    ports.extend(self.publish_slots(
                        scope,
                        std::slice::from_ref(slot),
                        true,
                        CarryOwner::New,
                        SlotContinuity::Decide,
                    ));
                }
                ReductionSlot::Group { naming } | ReductionSlot::PivotValue { naming } => {
                    ports.push(self.mint(
                        scope,
                        naming_spelling(*naming),
                        proposed_role(*naming),
                        ValueFacts::default(),
                    ));
                }
            }
        }
        self.store(scope, Interface::of(ports))
    }

    fn concatenate(
        &self,
        left: &SemanticRelation,
        right: &SemanticRelation,
        merged: &[MergedKey],
    ) -> Result<SemanticRelation> {
        let scope = self.registry.join_scope();
        let left_heading = self.operand_heading(left);
        let right_heading = self.operand_heading(right);
        // A MERGED KEY NAMES ONE POSITION OF EACH OPERAND. A pair written
        // against operands a rebuild has since replaced is translated onto
        // the rebuilt operands before it arrives here. Accepting a position
        // neither operand publishes would silently drop the pair from the
        // record and publish a join that merged nothing.
        for key in merged {
            if !left_heading.contains(&key.left.0) || !right_heading.contains(&key.right.0) {
                return Err(Internal::invariant(
                    "join",
                    format!(
                        "a merged key {:?} names a position its operands do not publish",
                        key
                    ),
                ));
            }
        }
        // A JOIN CONSUMES NOTHING. Its arms are still the statement's FROM
        // entries, so the columns it publishes still belong to the
        // relations they came from.
        let mut ports = Vec::new();
        for column in left_heading {
            let published = self.registry.published(column);
            let addressing = self.registry.addressing(column);
            ports.push(self.carry(
                CarryOwner::Preserve,
                scope,
                column,
                published,
                addressing,
                Continuity::Continues,
            ));
        }
        for column in right_heading {
            // A merged key publishes ONE position standing for a port of
            // each operand, so the right side's half does not publish
            // again.
            if merged.iter().any(|key| key.right.0 == column) {
                continue;
            }
            let published = self.registry.published(column);
            let addressing = self.registry.addressing(column);
            ports.push(self.carry(
                CarryOwner::Preserve,
                scope,
                column,
                published,
                addressing,
                Continuity::Continues,
            ));
        }
        let relation = self.store(scope, Interface::of(ports))?;
        let left_to_output: std::collections::HashMap<_, _> = self
            .registry
            .relations()
            .translations_into(relation.relation())
            .into_iter()
            .collect();
        self.registry.relations().record_translations(
            relation.relation(),
            merged.iter().filter_map(|key| {
                left_to_output
                    .get(&key.left)
                    .copied()
                    .map(|output| (key.right, output))
            }),
        );
        Ok(relation)
    }

    /// THE SET LAW'S EXECUTION.
    ///
    /// One result occurrence, one fresh port per output slot, and one
    /// contribution table saying what every arm puts in every slot. The
    /// table is built ON the ports the result publishes, so the evidence
    /// and the heading are the same fact rather than two that happen to
    /// agree — a slot cannot be justified by an arm's port that no result
    /// position carries.
    ///
    /// A set result NEVER continues an arm. Returning the left operand
    /// would say that a union publishes the left arm's positions, which is
    /// false in every mode: the rows of the other arms flow through those
    /// positions too, and a reference rewritten onto an arm's port names a
    /// relation that has half the rows.
    fn merge(&self, alignment: SetAlignment, arms: &[SetArm]) -> Result<SemanticRelation> {
        let Some(first) = arms.first() else {
            return Err(Internal::invariant(
                "set",
                "a set operation has two or more arms",
            ));
        };
        let mode = match alignment {
            SetAlignment::Positional => SetMode::Positional,
            SetAlignment::Corresponding => SetMode::Corresponding,
            SetAlignment::Smart => SetMode::Smart,
        };
        let headings: Vec<Vec<ColId>> = arms
            .iter()
            .map(|arm| self.operand_heading(&arm.relation))
            .collect();
        // THE ARM'S HEADING IS RECORDED WITH THE TABLE. A branch emits an
        // ordered list, and the place a port holds here is the place that
        // branch emits it at — the physical binding reads this rather than
        // relating the two by lineage afterwards.
        let arm_relations = Vec2::try_from_vec(
            arms.iter()
                .zip(&headings)
                .map(|(arm, heading)| {
                    super::set::SetArmRecord::of(
                        arm.relation.relation(),
                        heading.iter().copied().map(PortId).collect(),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .ok_or_else(|| {
            Internal::invariant(
                "set",
                "a set operation has two or more arms; one arm is not a set",
            )
        })?;

        let scope = self
            .registry
            .wrap_scope(first.relation.scope(), WrapReason::SetOperation);
        let slots = match mode {
            SetMode::Positional | SetMode::Smart => self.exact_slots(mode, &headings)?,
            SetMode::Corresponding => self.corresponding_arm_slots(&headings)?,
        };

        // EVERY SLOT IS BORN ONCE EVERY ARM HAS CONTRIBUTED, with the effect
        // the contributions decide: a position CONTINUES an occurrence only
        // when every arm continues one origin — its rows come through every
        // arm — and a padded or disagreeing slot is an occurrence of its
        // own. The port is minted with that effect; nothing revises it.
        let mut ports = Vec::with_capacity(slots.len());
        let mut outputs = Vec::with_capacity(slots.len());
        for slot in slots {
            let mut origins = slot.cells.iter().map(|cell| match cell {
                Contribution::Port(port) => Some(self.registry.relations().origin(*port)),
                Contribution::Padding(_) => None,
            });
            let continuity = match origins.next() {
                // A slot no arm fills is not a set slot; a padded first
                // cell, or any later cell that pads or continues another
                // origin, makes the slot an occurrence of its own.
                None | Some(None) => Continuity::Republishes,
                Some(Some(first)) => {
                    if origins.all(|origin| origin == Some(first)) {
                        Continuity::Continues
                    } else {
                        Continuity::Republishes
                    }
                }
            };
            let published = self.registry.published(slot.source);
            let addressing = self.registry.addressing(slot.source);
            let result = self.carry(
                CarryOwner::New,
                scope,
                slot.source,
                published,
                addressing,
                continuity,
            );
            self.judge_set_child(result, &slot.cells)?;
            ports.push(result);
            let cells = Vec2::try_from_vec(slot.cells).ok_or_else(|| {
                Internal::invariant("set", "a set output has one contribution per arm")
            })?;
            outputs.push(SetOutput::of(result, cells));
        }
        let matrix = ContributionMatrix::build(mode, arm_relations, outputs).map_err(|error| {
            Internal::invariant(
                "set",
                format!("a set operation's contribution matrix is malformed: {error:?}"),
            )
        })?;
        let relation = self.store(scope, Interface::of(ports))?;
        self.registry
            .relations()
            .record_set(relation.relation(), matrix);
        Ok(relation)
    }

    /// The exact modes: position `k` of every arm is one slot, and the
    /// first arm decides what the slot is called.
    ///
    /// The width disagreement refuses HERE rather than downstream, because
    /// a slot whose cells cannot be filled is not a narrower result — it is
    /// a set that was never lawful.
    fn exact_slots(&self, mode: SetMode, headings: &[Vec<ColId>]) -> Result<Vec<PendingSlot>> {
        let width = headings[0].len();
        for heading in headings {
            if heading.len() != width {
                return Err(exact_width_error(width, heading.len()));
            }
        }
        // WHICH COLUMN OF EACH ARM FILLS SLOT k. Positional says the kth,
        // by ordinal. Smart says the one answering to the slot's name, and
        // the two are different answers whenever an arm writes the same
        // names in another order — which is the whole reason smart exists.
        let filling: Vec<Vec<ColId>> = match mode {
            SetMode::Positional => headings.to_vec(),
            SetMode::Smart => {
                let mut arms = Vec::with_capacity(headings.len());
                for heading in headings {
                    let matched = super::alignment::stable_name_alignment(
                        self.registry,
                        &headings[0],
                        heading,
                    )?;
                    arms.push(
                        matched
                            .into_iter()
                            .collect::<Option<Vec<ColId>>>()
                            .ok_or_else(smart_name_error)?,
                    );
                }
                arms
            }
            SetMode::Corresponding => {
                unreachable!("the corresponding mode discovers its slots arm by arm")
            }
        };
        let mut slots = Vec::with_capacity(width);
        for position in 0..width {
            slots.push(PendingSlot {
                source: headings[0][position],
                cells: filling
                    .iter()
                    .map(|arm| Contribution::Port(PortId(arm[position])))
                    .collect(),
            });
        }
        Ok(slots)
    }

    /// The corresponding mode: slots are discovered arm by arm in
    /// first-appearance order, and an arm that reaches a slot late is
    /// padded for every arm before it.
    ///
    /// Matching is the registry's tiered correspondence, asked against the
    /// slots ALREADY published rather than against the arms' own columns:
    /// the slot is what a later arm has to answer, and two arms that both
    /// carry a repeated name are ranked against it once.
    fn corresponding_arm_slots(&self, headings: &[Vec<ColId>]) -> Result<Vec<PendingSlot>> {
        let arms = headings.len();
        let mut slots: Vec<PendingSlot> = Vec::new();
        // The columns that OPENED each slot, in slot order: a later arm
        // answers the slot's name, which is the opening column's name. No
        // result port exists yet — the slot is born only once every arm
        // has said what it contributes.
        let mut opened: Vec<ColId> = Vec::new();
        for (arm, heading) in headings.iter().enumerate() {
            let matched = super::alignment::stable_name_alignment(self.registry, &opened, heading)?;
            for (index, (slot, contribution)) in
                slots.iter_mut().zip(matched.iter().copied()).enumerate()
            {
                slot.cells.push(match contribution {
                    Some(column) => Contribution::Port(PortId(column)),
                    // AN ARM THAT CONTRIBUTES NOTHING HERE IS PADDED,
                    // never omitted: a missing cell would leave a reader
                    // deciding what an absence meant.
                    None => Contribution::Padding(padding(index, arm, arms)),
                });
            }
            let taken: Vec<ColId> = matched.iter().flatten().copied().collect();
            for column in heading {
                if taken.contains(column) {
                    continue;
                }
                let index = slots.len();
                let mut cells: Vec<Contribution> = (0..arm)
                    .map(|earlier| Contribution::Padding(padding(index, earlier, arms)))
                    .collect();
                cells.push(Contribution::Port(PortId(*column)));
                opened.push(*column);
                slots.push(PendingSlot {
                    source: *column,
                    cells,
                });
            }
        }
        Ok(slots)
    }

    /// THE CHILD A SET SLOT OWNS, decided from EVERY arm's evidence at once:
    /// every arm establishes a child of one shape, or every arm establishes
    /// none, or the slot records a conflict — one arm with a child beside
    /// one without is a conflict, never the first arm's silent default. The
    /// carry that minted the slot copied the opening column's child; this
    /// judgment keeps it only when all arms agree, and a padded arm
    /// establishes no child. The relation store owns every link read here,
    /// so no scope heading or copied value fact participates.
    fn judge_set_child(&self, slot: PortId, cells: &[Contribution]) -> Result<()> {
        let mut children = Vec::with_capacity(cells.len());
        for cell in cells {
            match cell {
                Contribution::Port(port) => {
                    if self.registry.relations().interior_conflict(*port) {
                        self.registry.relations().record_interior_conflict(slot);
                        return Ok(());
                    }
                    children.push(self.registry.relations().interior(*port));
                }
                Contribution::Padding(_) => children.push(None),
            }
        }
        let mut agreed = true;
        if let Some((first, rest)) = children.split_first() {
            for other in rest {
                agreed &= match (first, other) {
                    (None, None) => true,
                    (Some(left), Some(right)) => self.same_interior_shape(left, right)?,
                    (Some(_), None) | (None, Some(_)) => false,
                };
            }
        }
        if !agreed {
            self.registry.relations().record_interior_conflict(slot);
        }
        Ok(())
    }

    fn same_interior_shape(
        &self,
        left: &SemanticRelation,
        right: &SemanticRelation,
    ) -> Result<bool> {
        check_mark(self.mark, left)?;
        check_mark(self.mark, right)?;
        let left = self.operand_heading(left);
        let right = self.operand_heading(right);
        if left.len() != right.len() {
            return Ok(false);
        }
        for (left, right) in left.into_iter().zip(right) {
            if self.registry.published_sym(left) != self.registry.published_sym(right) {
                return Ok(false);
            }
            match (
                self.registry.relations().interior(PortId(left)),
                self.registry.relations().interior(PortId(right)),
            ) {
                (Some(left), Some(right)) if !self.same_interior_shape(&left, &right)? => {
                    return Ok(false)
                }
                (None, None) | (Some(_), Some(_)) => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    fn minus(&self, left: &SemanticRelation, right: &SemanticRelation) -> Result<SemanticRelation> {
        let left_heading = self.operand_heading(left);
        let right_heading = self.operand_heading(right);
        let left_opaque = self.operand_is_opaque(left);
        let right_opaque = self.operand_is_opaque(right);
        // EXACT HEADING, PAIRED BY STABLE PUBLISHED NAME. The pairing is
        // proved before any occurrence is minted, so a minus whose
        // exactness is merely probable has nothing to publish.
        let mut pairs = Vec::with_capacity(left_heading.len());
        for column in &left_heading {
            let name = self.registry.published_sym(*column);
            if let Some(partner) = right_heading
                .iter()
                .find(|other| name.is_some() && self.registry.published_sym(**other) == name)
            {
                pairs.push(ExactPair::of(PortId(*column), PortId(*partner)));
            }
        }
        let left_ports: Vec<PortId> = left_heading.iter().copied().map(PortId).collect();
        let right_ports: Vec<PortId> = right_heading.iter().copied().map(PortId).collect();
        let anti_match = ExactHeadingMap::build(
            left.relation(),
            right.relation(),
            &left_ports,
            &right_ports,
            left_opaque,
            right_opaque,
            pairs,
        )
        .map_err(|error| {
            DelightQLError::from(ResolutionSetop::MinusHeading {
                message: match error {
                    super::minus::ExactHeadingError::DegreeMismatch { .. } => {
                        "a minus requires the two operands to publish the same \
                         dimensions; they do not agree in width"
                    }
                    super::minus::ExactHeadingError::NotBijective => {
                        "a minus requires each dimension on one side to answer \
                         exactly one on the other"
                    }
                    super::minus::ExactHeadingError::OpaqueHeading => {
                        "a minus requires an enumerable heading on both sides"
                    }
                }
                .to_string(),
            })
        })?;

        // ONLY THE LEFT IS EXPORTED. The right ports occur in the pairs the
        // anti-match predicate is built from and nowhere else, so no result
        // position can carry one.
        let scope = self
            .registry
            .wrap_scope(left.scope(), WrapReason::SetOperation);
        let mut ports = Vec::with_capacity(anti_match.pairs().len());
        for pair in anti_match.pairs() {
            let source = pair.left().0;
            let published = self.registry.published(source);
            ports.push(self.carry(
                CarryOwner::New,
                scope,
                source,
                published,
                Addressing::Published,
                Continuity::Continues,
            ));
        }
        let relation = self.store(scope, Interface::of(ports))?;
        self.registry
            .relations()
            .record_anti_match(relation.relation(), anti_match);
        Ok(relation)
    }

    fn instantiate(
        &self,
        form: &RelForm<'_>,
        template: &SemanticRelation,
    ) -> Result<SemanticRelation> {
        if !matches!(form, RelForm::Instantiate(_) | RelForm::PlanRead(_)) {
            unreachable!("only a definition or plan use takes the Instantiate law")
        }
        let scope = scope_of_form(self.registry, form);
        let kind = match form {
            RelForm::Instantiate(spec) => Some(spec.kind),
            RelForm::PlanRead(_) => None,
            _ => unreachable!("only a definition or plan use takes the Instantiate law"),
        };
        // A definition or plan read is an entity boundary: a bare lvar
        // bound inside the body was the body's own binding, and carrying it
        // through as bare would let it silently unify with the caller's
        // lvars. Past the boundary it is an ordinary published column of the
        // instance — `p.city` and a caller's bare `city` are different lvars
        // and their relations cross. Higher-order carriers stay verbatim:
        // they hold caller-authored text, whose bare bindings are the
        // caller's own.
        let requalifies = matches!(form, RelForm::PlanRead(_))
            || matches!(kind, Some(DefinitionKind::Cte | DefinitionKind::View));
        let preserves_hygiene = matches!(kind, Some(DefinitionKind::HigherOrder(_)))
            || matches!(
                self.registry.relations().plan_role(template.relation()),
                Some(super::store::PlanRole::HigherOrder)
            );
        // EVERY USE IS A FRESH RELATION. A shared CTE keeps one storage
        // object; each read of it publishes its own occurrences, which is
        // what makes two instances of one definition two relations.
        let ports: Vec<_> = self
            .operand_heading(template)
            .into_iter()
            .filter(|column| {
                preserves_hygiene
                    || self.registry.addressing(*column) != Addressing::Hygienic
                    || self
                        .registry
                        .relations()
                        .residual_row_token(PortId(*column))
                        .is_some()
                    // A fixpoint's support crosses every read of it: the
                    // recursive clause reads the caller's actual from the
                    // frontier row, and the expansion boundary is the one
                    // place it is dropped.
                    || self
                        .registry
                        .relations()
                        .frontier_actual(PortId(*column))
                        .is_some()
            })
            .map(|port| {
                let published = self.registry.published(port);
                let addressing = match self.registry.addressing(port) {
                    Addressing::Bare | Addressing::BareUnder | Addressing::BareStage
                        if requalifies =>
                    {
                        Addressing::Published
                    }
                    addressing => addressing,
                };
                let output = self.carry(
                    CarryOwner::New,
                    scope,
                    port,
                    published,
                    addressing,
                    Continuity::Continues,
                );
                // THE READ IS WHERE THIS OCCURRENCE BEGINS. What stands
                // behind the position is the definition every read of it
                // shares; a rebuild judged over that would find two reads of
                // one definition standing over the same thing.
                self.registry.relations().record_read_boundary(output);
                output
            })
            .collect();
        let result = self.store(scope, Interface::of(ports))?;
        let Some(kind) = kind else {
            return Ok(result);
        };
        let definition = self
            .registry
            .relations()
            .definition_for(template.relation());
        self.registry
            .relations()
            .record_instance(result.relation(), definition, kind);
        Ok(result)
    }

    fn explode(
        &self,
        input: &SemanticRelation,
        interior_of: PortId,
        selected: &[PortId],
        selection: super::form::DrillSelection,
    ) -> Result<SemanticRelation> {
        let from = input.scope();
        let scope = self.registry.wrap_scope(from, WrapReason::Projection);
        // Context rides through, while the exploded container is consumed.
        // Selected interior positions follow the surviving context, which
        // is the whole difference from a narrowing.
        //
        // RIDING THROUGH KEEPS THE OCCURRENCE. The drill stands at a scope
        // of its own; that `x.*` after `… as x .nest(*)` still names the
        // context columns is the lexical frontier keeping the container's
        // binding across a scope-preserving postfix form, decided where
        // the reference is written and never recorded on the column. A
        // context position under a named container keeps the role of a
        // binding published under a name.
        let container_named = self.registry.answers_to(from).is_some();
        let mut ports: Vec<PortId> = Vec::new();
        for column in self.operand_heading(input) {
            if column == interior_of.column() {
                continue;
            }
            let published = self.registry.published(column);
            let addressing = match self.registry.addressing(column) {
                Addressing::Published if container_named => Addressing::BareUnder,
                addressing => addressing,
            };
            ports.push(self.carry(
                CarryOwner::New,
                scope,
                column,
                published,
                addressing,
                Continuity::Continues,
            ));
        }
        for port in selected {
            let published = self.registry.published(port.0);
            // A BINDER PUBLISHES BARE: the name is the author's for this
            // drill, an argumentative binding a later bare occurrence
            // reuses. A position taken whole keeps publishing its own name
            // under the nest's: whether `people.first_name` reaches it is
            // the lexical frontier's binding of the nest name, not a fact
            // of the column.
            let addressing = match selection {
                super::form::DrillSelection::Bound => Addressing::Bare,
                super::form::DrillSelection::Whole
                    if self.registry.published(interior_of.column()).is_some() =>
                {
                    Addressing::BareUnder
                }
                super::form::DrillSelection::Whole => self.registry.addressing(port.0),
            };
            ports.push(self.carry(
                CarryOwner::New,
                scope,
                port.0,
                published,
                addressing,
                Continuity::Continues,
            ));
        }
        self.store(scope, Interface::of(ports))
    }

    fn interior(&self, owner: PortId, body: &SemanticRelation) -> Result<SemanticRelation> {
        // ATOMIC WITH THE BACK-LINK: the owning column records this scope
        // as its interior, and a column owns exactly one.
        let scope = self.registry.interior_scope(owner.0);
        let carried = self.carry_interface(body, scope, CarryOwner::New, Continuity::Continues);
        let relation = self.store(scope, Interface::of(carried))?;
        self.registry.relations().record_interior(owner, relation);
        Ok(relation)
    }

    fn materialize(
        &self,
        form: &RelForm<'_>,
        _why: ScratchWhy,
        publishes: &super::form::ScratchInterface<'_>,
    ) -> Result<SemanticRelation> {
        let scope = scope_of_form(self.registry, form);
        let interface = match publishes {
            // The created table's columns come from exactly this select
            // list, so the scratch REPUBLISHES those occurrences: a stored
            // table's positions are occurrences of their own.
            super::form::ScratchInterface::Holds(emitted) => Interface::of(
                interface_of(self.registry, emitted)
                    .ports()
                    .iter()
                    .map(|port| {
                        self.carry(
                            CarryOwner::New,
                            scope,
                            port.column(),
                            self.registry.published(port.column()),
                            self.registry.addressing(port.column()),
                            Continuity::Republishes,
                        )
                    })
                    .collect(),
            ),
            super::form::ScratchInterface::States(slots) => Interface::of(
                slots
                    .iter()
                    .map(|slot| {
                        self.mint(
                            scope,
                            Some(slot.named),
                            Addressing::Published,
                            ValueFacts::default(),
                        )
                    })
                    .collect(),
            ),
        };
        self.store(scope, interface)
    }

    fn fixed(&self, shape: FixedShape<'_>) -> Result<SemanticRelation> {
        let (scope, ports) = match shape {
            FixedShape::Witness => {
                let scope = self.registry.carrier_scope("witness");
                let met = self.registry.intern("met", false);
                let port = self.mint(
                    scope,
                    Some(met),
                    Addressing::Published,
                    ValueFacts::default(),
                );
                (scope, vec![port])
            }
            FixedShape::SignedWitness { input } => {
                let from = input.scope();
                let scope = self.registry.wrap_scope(from, WrapReason::Witness);
                let mut ports =
                    self.carry_interface(input, scope, CarryOwner::New, Continuity::Continues);
                // `met` IS APPENDED LAST. The total ledger's shape is the
                // input's heading and then the answer, in that order.
                let met = self.registry.intern("met", false);
                ports.push(self.mint(
                    scope,
                    Some(met),
                    Addressing::Published,
                    ValueFacts::default(),
                ));
                (scope, ports)
            }
            FixedShape::Meta { subject } => {
                let scope = self.registry.wrap_scope(subject.scope(), WrapReason::Meta);
                let ports = ["scope", "column_name", "ordinal"]
                    .into_iter()
                    .map(|name| {
                        let spelling = self.registry.intern(name, false);
                        self.mint(
                            scope,
                            Some(spelling),
                            Addressing::Published,
                            ValueFacts::default(),
                        )
                    })
                    .collect();
                (scope, ports)
            }
        };
        self.store(scope, Interface::of(ports))
    }

    /// The publication a carried position INHERITS.
    ///
    /// A name two positions of one heading both publish leaves neither
    /// addressable by it — the authored case refuses outright, and a glob's
    /// repetition loses the name instead. The loss is MONOTONIC: a step that
    /// carries one of those positions forward carries the OCCURRENCE, not
    /// the name it stopped answering to, so selecting the second of three
    /// repeated publications cannot give `a` back merely because the other
    /// two did not come along.
    ///
    /// The question is asked of the SOURCE's own interface, which is what
    /// makes it monotonic. Asking it of whatever heading is being emitted
    /// would recompute the collision every time and hand the name back the
    /// moment the siblings stop travelling together.
    fn inheritable_publication(&self, source: PortId) -> Option<crate::names::Spelling> {
        let published = self.registry.published(source.column())?;
        let store = self.registry.relations();
        let Some(relation) = store.relation_of(source) else {
            return Some(published);
        };
        let Some(interface) = store.interface(relation) else {
            return Some(published);
        };
        let wanted = self.registry.canonical(published);
        // A JOIN'S TWO `id`s ARE TWO CELLS, and each keeps its name — the
        // qualifier tells them apart. What loses the name is one CELL
        // published twice: the repeated glob and the repeated ordinal span
        // put the same occurrence in two positions, and neither answers to
        // the name after that.
        let from = store.lineage(source);
        let twin = interface.ports().iter().any(|port| {
            *port != source
                && self.registry.published_sym(port.column()) == Some(wanted)
                && store.lineage(*port) == from
        });
        (!twin).then_some(published)
    }

    fn publish_slots(
        &self,
        scope: ScopeId,
        slots: &[ProjectSlot],
        inherit_addressing: bool,
        owner: CarryOwner,
        continuity: SlotContinuity,
    ) -> Vec<PortId> {
        // THE CONTINUATION LAW OF A PUBLICATION: a position carried into
        // exactly one of these slots continues there. Carried into several,
        // it continues into the one slot that keeps its own publication
        // (the operand's heading run, an inherited name) and the others
        // republish its value; several keeping it, or several renaming it,
        // continue nothing. Decided from the slots this act publishes —
        // never from spelling, order, or the value.
        let carried_into = |wanted: PortId, inherited_only: bool| {
            slots
                .iter()
                .filter(|slot| match slot {
                    ProjectSlot::Carried { source, naming } => {
                        *source == wanted
                            && (!inherited_only || matches!(naming, Naming::Inherited))
                    }
                    ProjectSlot::Computed { .. } => false,
                })
                .count()
        };
        slots
            .iter()
            .map(|slot| match slot {
                ProjectSlot::Carried { source, naming } => {
                    let continues = match continuity {
                        SlotContinuity::Republish => false,
                        SlotContinuity::Decide => {
                            carried_into(*source, false) == 1
                                || (matches!(naming, Naming::Inherited)
                                    && carried_into(*source, true) == 1)
                        }
                    };
                    let published = match naming {
                        Naming::Authored(spelling) | Naming::Bound(spelling) => Some(*spelling),
                        Naming::Inherited => self.inheritable_publication(*source),
                        Naming::Anonymous | Naming::Hygienic => None,
                    };
                    // The PROPOSAL — the publication's own facts. The one
                    // boundary act at the carry judges the role a
                    // dequalifying stage gives it.
                    let addressing = match naming {
                        Naming::Inherited
                            if matches!(
                                self.registry.addressing(source.0),
                                Addressing::Hygienic | Addressing::Latent
                            ) =>
                        {
                            self.registry.addressing(source.0)
                        }
                        Naming::Inherited if inherit_addressing => {
                            self.registry.addressing(source.0)
                        }
                        Naming::Inherited if published.is_some() => Addressing::Published,
                        Naming::Inherited => Addressing::Bare,
                        naming => proposed_role(*naming),
                    };
                    self.carry_with(
                        owner,
                        scope,
                        source.0,
                        published,
                        addressing,
                        if continues {
                            Continuity::Continues
                        } else {
                            Continuity::Republishes
                        },
                        matches!(naming, Naming::Inherited | Naming::Hygienic),
                        CarriedValue::Same,
                        |_| {},
                    )
                }
                ProjectSlot::Computed { naming, shape } => self.mint(
                    scope,
                    naming_spelling(*naming),
                    proposed_role(*naming),
                    ValueFacts {
                        shape: *shape,
                        ..ValueFacts::default()
                    },
                ),
            })
            .collect()
    }

    /// What an OPERAND publishes, from the authority's record.
    ///
    /// Not the scope's heading. A relation whose interface the authority
    /// fixed publishes exactly those positions however the scope later
    /// grows, and an operation that read the scope instead would let an
    /// unrelated caller's column become one of its operand's dimensions —
    /// which is the difference between an immutable answer being
    /// representable and its being authoritative.
    fn operand_heading(&self, relation: &SemanticRelation) -> Vec<ColId> {
        interface_of(self.registry, relation)
            .ports()
            .iter()
            .map(|port| port.column())
            .collect()
    }

    /// Whether an operand's dimensions are unenumerable, from the same
    /// record.
    fn operand_is_opaque(&self, relation: &SemanticRelation) -> bool {
        interface_of(self.registry, relation).is_opaque()
    }
}

/// Allocate the lexical scope an exact form requires. Exhaustive: callers
/// name operations and cannot nominate a scope policy.
fn scope_of_form(registry: &Registry, form: &RelForm<'_>) -> ScopeId {
    match form {
        // A CATALOG READ ANSWERS TO ITS RELATION. A compiler source, or
        // one nothing answers to, is anonymous: there is no name for a
        // reference to reach it by.
        RelForm::Source(spec) => match (source_entity(spec.origin), spec.answers_to) {
            (Some(entity), Some(answer)) => registry.base_table_scope(entity, answer),
            (Some(_), None) | (None, Some(_)) | (None, None) => {
                registry.anonymous_scope(spec.answers_to)
            }
        },
        RelForm::Anonymous(spec) => registry.anonymous_scope(spec.answers_to()),
        RelForm::Opaque => registry.opaque_scope(),
        // A transparent step creates no occurrence at all, so asking for
        // one is asking the wrong question; the anonymous birth is what an
        // empty enclosure means.
        RelForm::Order(input) => return input.scope(),
        RelForm::Export(spec) => scope_of_export(registry, spec.input.scope(), spec.why),
        RelForm::Access(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Project(spec) | RelForm::Embed(spec) => {
            registry.wrap_scope(spec.input.scope(), wrap_of(form))
        }
        RelForm::ErBoundary(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Rename(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Reposition(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::ProjectOut(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Cover(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Group(spec) => registry.wrap_scope(spec.input.scope(), WrapReason::Aggregate),
        RelForm::Join(_) => registry.join_scope(),
        RelForm::Set(spec) => registry.wrap_scope(
            spec.arms
                .first()
                .map(|arm| arm.relation.scope())
                .unwrap_or_else(|| unreachable!("a set operation has two or more arms")),
            WrapReason::SetOperation,
        ),
        RelForm::Minus(spec) => registry.wrap_scope(spec.left.scope(), WrapReason::SetOperation),
        RelForm::Witness(_) => registry.carrier_scope("witness"),
        RelForm::SignedWitness(spec) => {
            registry.wrap_scope(spec.input.scope(), WrapReason::Witness)
        }
        RelForm::Instantiate(spec) => match spec.kind {
            // A HIGHER-ORDER CARRIER IS ITS ROLE. What it stands for is
            // what the binding road reads back, so the role is the birth.
            DefinitionKind::HigherOrder(part) => registry.higher_order_scope(ho_role(part), "ho"),
            DefinitionKind::Fact | DefinitionKind::Cte | DefinitionKind::View => {
                spec.answers_to.map_or_else(
                    || registry.carrier_scope(prefix_of_definition(spec.kind)),
                    |answer| registry.alias_scope(spec.template.scope(), answer),
                )
            }
        },
        RelForm::PlanRead(spec) => match spec.kind {
            PlanReadKind::Scratch | PlanReadKind::HigherOrder => {
                registry.alias_scope(spec.template.scope(), spec.answers_to)
            }
        },
        RelForm::Destructure(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Drill(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Narrow(spec) => registry.wrap_scope(spec.input.scope(), wrap_of(form)),
        RelForm::Interior(spec) => registry.interior_scope(spec.owner.column()),
        RelForm::Meta(spec) => registry.wrap_scope(spec.subject.scope(), WrapReason::Meta),
        RelForm::Scratch(spec) => match spec.base {
            // An EXACT base is a physical name several statements must
            // agree on; a prefixed one answers to nothing at all.
            Some(base) => registry.exact_scratch_scope(scratch_role(spec.why), base),
            None => registry.scratch_scope(scratch_role(spec.why), scratch_prefix(spec.why)),
        },
    }
}

fn source_entity(origin: SourceOrigin) -> Option<crate::names::EntityId> {
    match origin {
        SourceOrigin::Catalog { entity } | SourceOrigin::TableValued { entity } => Some(entity),
    }
}

fn scratch_role(why: ScratchWhy) -> ScratchRole {
    match why {
        ScratchWhy::Snapshot | ScratchWhy::DmlSource => ScratchRole::Snapshot,
        ScratchWhy::Result => ScratchRole::Result,
        ScratchWhy::Tee => ScratchRole::Tee,
        ScratchWhy::Insert => ScratchRole::Insert,
        ScratchWhy::Barrier => ScratchRole::Barrier,
    }
}

/// Which operands a form reads. Exhaustive over the vocabulary, so a form
/// that takes an input cannot skip the entrance check by omission.
fn inputs_of<'a>(form: &'a RelForm<'a>) -> Vec<&'a SemanticRelation> {
    match form {
        RelForm::Source(_) | RelForm::Anonymous(_) | RelForm::Opaque => Vec::new(),
        RelForm::Order(input) => vec![input],
        RelForm::Export(spec) => vec![&spec.input],
        RelForm::Access(spec) => vec![&spec.input],
        RelForm::Project(spec) | RelForm::Embed(spec) => vec![&spec.input],
        RelForm::ErBoundary(spec) => vec![&spec.input],
        RelForm::Rename(spec) => vec![&spec.input],
        RelForm::Reposition(spec) => vec![&spec.input],
        RelForm::ProjectOut(spec) => vec![&spec.input],
        RelForm::Cover(spec) => vec![&spec.input],
        RelForm::Group(spec) => vec![&spec.input],
        RelForm::Join(spec) => vec![&spec.left, &spec.right],
        RelForm::Set(spec) => spec.arms.iter().map(|arm| &arm.relation).collect(),
        RelForm::Minus(spec) => vec![&spec.left, &spec.right],
        RelForm::Witness(spec) => vec![&spec.input],
        RelForm::SignedWitness(spec) => vec![&spec.input],
        RelForm::Instantiate(spec) => vec![&spec.template],
        RelForm::PlanRead(spec) => vec![&spec.template],
        RelForm::Destructure(spec) => vec![&spec.input],
        RelForm::Drill(spec) => vec![&spec.input],
        RelForm::Narrow(spec) => vec![&spec.input],
        RelForm::Interior(spec) => vec![&spec.body],
        RelForm::Meta(spec) => vec![&spec.subject],
        RelForm::Scratch(spec) => match spec.interface() {
            super::form::ScratchInterface::Holds(input) => vec![input],
            super::form::ScratchInterface::States(_) => Vec::new(),
        },
    }
}

/// Whether a form REPUBLISHES its one operand: the same rows, under this
/// relation's own positions — a projection, export, access, rename,
/// reposition, cover, ordering or ER boundary. Everything that combines
/// operands, changes the bag, begins a new occurrence or has no operand
/// answers no. A witness answers no: an existence witness collapses its
/// operand to one verdict row, and a signed witness totalizes an empty
/// operand into a proxy row. Exhaustive: a new form says which it is.
fn republishes(form: &RelForm<'_>) -> bool {
    match form {
        RelForm::Export(_)
        | RelForm::Access(_)
        | RelForm::Project(_)
        | RelForm::Embed(_)
        | RelForm::ErBoundary(_)
        | RelForm::Rename(_)
        | RelForm::Reposition(_)
        | RelForm::ProjectOut(_)
        | RelForm::Cover(_)
        | RelForm::Order(_) => true,
        RelForm::Witness(_)
        | RelForm::SignedWitness(_)
        | RelForm::Source(_)
        | RelForm::Anonymous(_)
        | RelForm::Opaque
        | RelForm::Group(_)
        | RelForm::Join(_)
        | RelForm::Set(_)
        | RelForm::Minus(_)
        | RelForm::Instantiate(_)
        | RelForm::PlanRead(_)
        | RelForm::Destructure(_)
        | RelForm::Drill(_)
        | RelForm::Narrow(_)
        | RelForm::Interior(_)
        | RelForm::Meta(_)
        | RelForm::Scratch(_) => false,
    }
}

/// The lexical scope an export reason determines. Exhaustive: a new road
/// has no scope until it states one.
fn scope_of_export(registry: &Registry, input: ScopeId, why: ExportWhy) -> ScopeId {
    match why {
        ExportWhy::Alias { answer } => registry.alias_scope(input, answer),
        ExportWhy::Bound { answer } => registry.bound_row_scope(input, answer),
        ExportWhy::Stage => registry.stage_scope(input),
        ExportWhy::Cte { role, label } => {
            registry.cte_scope(input, cte_role(role), cte_label(label))
        }
        ExportWhy::EmissionAlias => registry.emission_alias_scope(input),
    }
}

/// The ownership disposition an export's reason determines. A caller names
/// the road; it has no spelling for what the road does to owners.
fn boundary_of_export(why: ExportWhy) -> CarryOwner {
    match why {
        ExportWhy::EmissionAlias => CarryOwner::Preserve,
        ExportWhy::Alias { .. } | ExportWhy::Bound { .. } => CarryOwner::New,
        ExportWhy::Stage | ExportWhy::Cte { .. } => CarryOwner::New,
    }
}

fn cte_role(role: CteWhy) -> CteRole {
    match role {
        CteWhy::TreeGroup => CteRole::TreeGroup,
        CteWhy::GroupCarrier => CteRole::GroupCarrier,
        CteWhy::Recursive => CteRole::Recursive,
        CteWhy::Reachability => CteRole::Reachability,
        CteWhy::Materialize => CteRole::Materialize,
    }
}

fn cte_label(label: CteLabelWhy) -> CteLabel {
    match label {
        CteLabelWhy::Answering(spelling) => CteLabel::Answering(spelling),
        CteLabelWhy::Prefixed(prefix) => CteLabel::Prefixed(prefix),
    }
}

/// Why the projection-family forms re-stage. Exhaustive over the forms that
/// take the Project and Edit laws.
fn wrap_of(form: &RelForm<'_>) -> WrapReason {
    match form {
        RelForm::Group(_) => WrapReason::Aggregate,
        RelForm::Access(_)
        | RelForm::Project(_)
        | RelForm::ErBoundary(_)
        | RelForm::Embed(_)
        | RelForm::Rename(_)
        | RelForm::Reposition(_)
        | RelForm::ProjectOut(_)
        | RelForm::Cover(_)
        | RelForm::Destructure(_)
        | RelForm::Narrow(_) => WrapReason::Projection,
        RelForm::Source(_)
        | RelForm::Anonymous(_)
        | RelForm::Opaque
        | RelForm::Order(_)
        | RelForm::Export(_)
        | RelForm::Join(_)
        | RelForm::Set(_)
        | RelForm::Minus(_)
        | RelForm::Witness(_)
        | RelForm::SignedWitness(_)
        | RelForm::Instantiate(_)
        | RelForm::PlanRead(_)
        | RelForm::Drill(_)
        | RelForm::Interior(_)
        | RelForm::Meta(_)
        | RelForm::Scratch(_) => WrapReason::Projection,
    }
}

fn naming_spelling(naming: Naming) -> Option<crate::names::Spelling> {
    match naming {
        Naming::Authored(spelling) | Naming::Bound(spelling) => Some(spelling),
        Naming::Inherited | Naming::Anonymous | Naming::Hygienic => None,
    }
}

/// THE ADDRESS LAW OF A PUBLISHING BOUNDARY. Two independent facts meet at
/// every published position — where its spelling came from ([`Naming`]) and
/// what address role it holds ([`Addressing`]) — and the OPERATION that
/// publishes the position derives both atomically. There is no context-free
/// naming-to-addressing map: a spelling alone cannot say whether it is
/// qualified, and a caller has no spelling for addressability at all.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OutputBoundary {
    /// An authored pipe stage. Every PIPE FORM is SCOPE-DEQUALIFYING
    /// (fundamentals), so an authored or inherited spelling it publishes
    /// is a BARE live lvar a later bare occurrence reuses.
    PipeStage,
    /// An entity-facing publication or compiler republication: a named
    /// position answers through its own publication.
    Publishing,
}

/// Which address law a form's boundary carries. Exhaustive over the forms
/// whose ports travel through [`SemanticBuilder::publish_slots`] and the
/// projection-family mint sites; a new form does not compile until its
/// boundary is stated here.
fn output_boundary(form: &RelForm<'_>) -> OutputBoundary {
    match form {
        // THE SURFACE PIPE ALGEBRA, whole (fundamentals: PIPE OPERATORS are
        // exhaustively PROJECT, PROJECT-OUT, RENAME, EMBED, MAP-COVER,
        // EMBED-MAP-COVER, TRANSFORM, GROUP, ORDERING, REPOSITION, and the
        // NARROWING forms; every PIPE FORM is SCOPE-DEQUALIFYING). A
        // compiler restatement spelled through the same carrier is its own
        // operation and never takes the stage disposition.
        RelForm::Project(spec) | RelForm::Embed(spec) => match spec.why {
            super::form::ProjectWhy::Stage => OutputBoundary::PipeStage,
            super::form::ProjectWhy::Restate => OutputBoundary::Publishing,
        },
        RelForm::Rename(spec) => match spec.why {
            super::form::ProjectWhy::Stage => OutputBoundary::PipeStage,
            super::form::ProjectWhy::Restate => OutputBoundary::Publishing,
        },
        RelForm::Reposition(_)
        | RelForm::ProjectOut(_)
        | RelForm::Cover(_)
        | RelForm::Group(_)
        | RelForm::Narrow(_) => OutputBoundary::PipeStage,
        // A DRILL KEEPS THE CONTAINER'S NAMES: its context rides through
        // under the container's binding and its pattern binds the interior
        // where it stands, so the roles it publishes are the ones it
        // proposed. The pipe spelling `|> .t.g(*)` is a drill followed by
        // a stage projection, and that projection is the act that
        // dequalifies.
        RelForm::Drill(_) => OutputBoundary::Publishing,
        // The ordering stage republishes through the stage export; every
        // other export reason is an entity-facing boundary.
        RelForm::Export(spec) => match spec.why {
            super::form::ExportWhy::Stage => OutputBoundary::PipeStage,
            super::form::ExportWhy::Alias { .. }
            | super::form::ExportWhy::Bound { .. }
            | super::form::ExportWhy::Cte { .. }
            | super::form::ExportWhy::EmissionAlias => OutputBoundary::Publishing,
        },
        RelForm::Source(_)
        | RelForm::Anonymous(_)
        | RelForm::Opaque
        | RelForm::Order(_)
        | RelForm::Access(_)
        | RelForm::ErBoundary(_)
        | RelForm::Join(_)
        | RelForm::Set(_)
        | RelForm::Minus(_)
        | RelForm::Witness(_)
        | RelForm::SignedWitness(_)
        | RelForm::Instantiate(_)
        | RelForm::PlanRead(_)
        | RelForm::Destructure(_)
        | RelForm::Interior(_)
        | RelForm::Meta(_)
        | RelForm::Scratch(_) => OutputBoundary::Publishing,
    }
}

/// The PROPOSAL a naming makes for a minted position — the publication's
/// own facts, before the one boundary act judges the role. Not an address
/// decision: every mint crosses [`SemanticBuilder::published_role`], where
/// the active boundary is applied.
fn proposed_role(naming: Naming) -> Addressing {
    match naming {
        Naming::Authored(_) | Naming::Anonymous | Naming::Inherited => Addressing::Published,
        Naming::Bound(_) => Addressing::Bare,
        Naming::Hygienic => Addressing::Hygienic,
    }
}

fn prefix_of_definition(kind: DefinitionKind) -> &'static str {
    match kind {
        DefinitionKind::Fact => "fact",
        DefinitionKind::HigherOrder(_) => "ho",
        DefinitionKind::Cte => "cte",
        DefinitionKind::View => "view",
    }
}

/// The emission base a scratch's position determines.
fn scratch_prefix(why: ScratchWhy) -> &'static str {
    match why {
        ScratchWhy::DmlSource => "dml_source",
        ScratchWhy::Snapshot
        | ScratchWhy::Result
        | ScratchWhy::Tee
        | ScratchWhy::Insert
        | ScratchWhy::Barrier => "scratch",
    }
}

fn ho_role(part: HoPart) -> HoRole {
    match part {
        HoPart::Argument => HoRole::Argument,
        HoPart::PipeSource => HoRole::PipeSource,
        HoPart::ScalarInput => HoRole::ScalarInput,
        HoPart::Proffer => HoRole::Proffer,
    }
}

/// The stable published-name order a corresponding set aligns on: first
/// appearance across the arms, in arm order.
///
/// Names, not characters: the comparison is over canonical identities the
/// registry already decided, so an emitted spelling never decides an
/// alignment.
/// One result position under construction: the port the result publishes
/// and what each arm puts through it.
///
/// The port is minted BEFORE the row is closed, which is what makes the
/// matrix's row identity the result's own position rather than a stand-in
/// borrowed from whichever arm happened to be first.
/// One slot of a set result BEFORE it is born: the arm column that opened
/// it (its name and value class) and what every arm contributes to it. The
/// result port is minted only once every contribution is known, with the
/// occurrence effect the contributions decide.
struct PendingSlot {
    source: ColId,
    cells: Vec<Contribution>,
}

/// The identity of one padded cell.
///
/// Row-major over the whole table, so two padded cells of one slot are two
/// paddings and not one repeated: an arm's absence is its own, and a reader
/// asking which arm was padded gets an answer.
fn padding(slot: usize, arm: usize, arms: usize) -> super::port::PaddingId {
    super::port::PaddingId((slot * arms + arm) as u32)
}

/// A name disagreement under the smart alignment.
///
/// Smart aligns by NAME and pads nothing: an arm that does not publish
/// every name the result does has no column for a slot, and there is no
/// typed null for this operator to put there.
fn smart_name_error() -> DelightQLError {
    DelightQLError::from(SetOperation::ColumnNameMismatch {
        message: "smart union (|;|) requires every operand to publish the same names, \
         and one operand does not publish every name the result has"
            .to_string(),
    })
}

/// A width disagreement under an exact alignment.
///
/// Raised by the LAW, at the one place a set is constructed. The message
/// and category are the set operator's own, because the operator is what
/// the author wrote and what a report has to name.
fn exact_width_error(left: usize, right: usize) -> DelightQLError {
    DelightQLError::from(SetOperation::ColumnCountMismatch {
        message: format!(
            "Set operation requires both sides to have the same number of columns, \
             but left has {left} and right has {right}"
        ),
    })
}
