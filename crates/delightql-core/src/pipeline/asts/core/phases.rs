// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The phase family: one parameter that selects what the tree's fields HOLD.
//!
//! A phase marker does not ride beside the data, it CHOOSES the data. Where
//! a slot has no value yet the phase says `()` — there is nothing to read,
//! so there is nothing to fake — and where a form cannot exist the slot is
//! `Never`, so its variant cannot be constructed and its arm need not be
//! written.
//!
//! The associated types are bounded once, here. Every node in the tree can
//! therefore keep deriving `Clone`, `Debug`, `PartialEq`, and `ToLispy`
//! without each deriving site restating what a payload can do: a derive
//! carries the type's declared bounds, and the declared bound is `P: Phase`.

use crate::diagnostic::Internal;
use crate::lispy::ToLispy;
use std::fmt::Debug;

/// What the tree's phase-selected fields hold.
///
/// Tripwire (design §11, Gate A): more than three extension-style slots on
/// any one enum, or a refinement that must rebuild topology this shared
/// shape cannot say, stops the work for a re-decision toward separate IRs.
pub trait Phase: Clone + Debug + PartialEq + Sized + 'static {
    /// The relation a node publishes: nothing before resolution, an
    /// occurrence after. Heading questions go to the registry, which
    /// answers `Known` or `Opaque`; the tree carries no heading cache.
    type Scope: Clone + Debug + PartialEq + ToLispy;
    /// WHAT A CTE BINDING'S BODY IS at this phase: one authored clause
    /// before the clauses of a definition are grouped.
    ///
    /// THE DECISION IS THE BODY. There is no recursion slot beside a chain,
    /// because a decision stored beside a body is a second description of
    /// one fact and two descriptions are free to disagree.
    type CteBody: crate::pipeline::bindings::CteBodyCarrier<Self>;
    /// The head and provenance judgments resolution spends from an authored
    /// CTE binding (`CteAuthority`): present before resolution, and DELETED
    /// by the phase system after — a bound phase cannot carry a spent copy.
    type CteAuthority: Clone + Debug + PartialEq + ToLispy;
    /// The closed binding representation for this phase. Unresolved frontier
    /// bindings use their own atomic variant; decided phases have only the
    /// bound body-and-relation carrier.
    type CteBindingState: crate::pipeline::bindings::CteBindingState<Self>;
    /// The resolver's per-expression output decision: which occurrence this
    /// expression publishes, or that it publishes none.
    type Output: Clone + Debug + PartialEq + ToLispy;
    /// The ONE column a scalarized relation publishes.
    ///
    /// CARDINALITY IS AUTHORED, DEGREE IS JUDGED. The compression the author
    /// spelled proves at-most-one-ROW; the one-COLUMN guarantee is nobody's
    /// spelling, so resolution asks the registry once at the value admission
    /// and the answer is stored HERE. Nothing before resolution has an
    /// occurrence to hold, and nothing after may lack one.
    type ScalarOutput: Clone + Debug + PartialEq + ToLispy;
    /// The interior drill's payload: authored names before binding, bound
    /// occurrences after.
    type Drill: Clone + Debug + PartialEq + ToLispy;
    /// What a call names: the written reference before resolution, the
    /// referent it resolved to after.
    ///
    /// Resolution does not ask whether the name exists. DelightQL does not
    /// require a catalog entry to call something — an unrecognised name is
    /// handed to the target, which is the default transpilation rule — so a
    /// written call becomes a function IDENTITY carrying its spelling, not
    /// a claim that the function was found.
    type Entity: Clone + Debug + PartialEq + ToLispy;
    /// The correlation a bag step carries: which OTHER arm of its run it
    /// constrains, and how. Absent where no pass has settled one.
    type Corr: Clone + Debug + PartialEq + ToLispy;
    /// Which ARM a whole-heading correlation names: the stage name the
    /// author wrote, and the scope resolution answered it with.
    type CorrelationArm: Clone + Debug + PartialEq + ToLispy;
    /// The columns a member CORRESPONDS on.
    ///
    /// Synthesized at resolution from the access, the anonymous header, or
    /// the positional pattern that directs it, so the authored phase holds
    /// `Never`: a correspondence cannot be built before the access it comes
    /// from has been read, and no consumer needs an arm for one that was.
    type Correspondence: Clone + Debug + PartialEq + ToLispy;
    /// THE COMMA'S DECIDED RELATIONSHIP. The authored phase holds what the
    /// author left open — a stated condition, or nothing yet. Resolution
    /// decides, and the decided phases hold the TOTAL judgment: a
    /// correspondence, a condition, or a deliberate Cartesian. `None` stops
    /// existing at the boundary, so a missing decision cannot travel into
    /// lowering and surface as an unconditioned join.
    type MemberCorr: Clone + Debug + PartialEq + ToLispy;
    /// WHAT A MEMBER STEP SAYS ABOUT ITS JOIN. Before refinement: the roles
    /// its syntax fixed — the member's `?`, and whether it completes an
    /// optional lead. After: the join type the rebuild decided over the
    /// whole run from those same roles. No phase holds both, so a decided
    /// type cannot drift from the roles it was decided from.
    type MemberJoin: Clone + Debug + PartialEq + ToLispy;
    /// The witness that a member's Cartesian arm was DECIDED: uninhabited
    /// before resolution, so no authored tree can state "this crosses"
    /// before the live bare interface was enumerated.
    type Decided: Clone + Debug + PartialEq + ToLispy;
    /// A consulted view's already-bound output boundary.
    /// A positional column reference — `|2|`, `|-1|` — and its authored
    /// qualification.
    type ColumnOrdinal: Clone + Debug + PartialEq + ToLispy;
    /// A definition-owned scalar reference — an authored `$.x` or a
    /// ground-head dispatch — uninhabited after resolution.
    type FormalSelector: Clone + Debug + PartialEq + ToLispy;
    fn into_argument(selector: Self::FormalSelector) -> super::definitions::FormalSelector;
    fn admit_argument(
        selector: super::definitions::FormalSelector,
    ) -> crate::error::Result<Self::FormalSelector>;

    /// A physical slot introduced after semantic construction has sealed.
    /// Uninhabited before refinement, so no parser, resolver, or refiner can
    /// manufacture SQL-layout evidence in a semantic tree.
    type PhysicalColumn: Clone + Debug + PartialEq + ToLispy;
    fn into_physical(column: Self::PhysicalColumn) -> crate::error::Result<crate::names::ColId>;
    fn admit_physical(column: crate::names::ColId) -> crate::error::Result<Self::PhysicalColumn>;

    fn cte_binding_of_authored(
        binding: crate::pipeline::bindings::AuthoredBinding<Self>,
    ) -> crate::error::Result<crate::pipeline::bindings::CteBinding<Self>>;
    /// A positional column RANGE — `|1:3|` — in projection position.
    type ColumnRange: Clone + Debug + PartialEq + ToLispy;
    /// The witness an authored ENUMERATION carries.
    ///
    /// A spread expands at the container that admits it, into the columns
    /// it addresses. Resolution SPENDS it — so a resolved tree holds none,
    /// not an expanded one and not an empty one — and this slot is what
    /// makes that structural: uninhabited after resolution, every arm of
    /// `Spread` becomes unbuildable and every consumer's arm becomes
    /// unwritable.
    type Enumeration: Clone + Debug + PartialEq + ToLispy;
    /// The zero-width record produced when resolution spends a spread that
    /// addresses no columns. The authored phase makes this uninhabited: the
    /// grammar supplies a nonempty `Record`, never the generated result.
    type EmptyRecord: Clone + Debug + PartialEq + ToLispy;
    /// What a column reference names: the characters written before
    /// resolution, the occurrence something bound it to after.
    type Col: Clone + Debug + PartialEq + ToLispy;
    /// The name a caller-pattern slot OFFERS: the written name before
    /// resolution, the column it bound after. A slot that binds stays a
    /// slot that binds — a phase change selects the payload, never the
    /// variant.
    type Binder: Clone + Debug + PartialEq + ToLispy;
    /// A pattern member that binds the like-named key (`{first_name}`):
    /// the written name before resolution. Binding spends the name into the
    /// key the member reads and the occurrence it publishes — a keyed
    /// member — so a bound phase has no such member at all.
    type PatternBinder: Clone + Debug + PartialEq + ToLispy;
    /// What a pattern's reach publishes under: the authored name (or none)
    /// before resolution, the occurrence its destructure or narrowing minted
    /// for it after.
    type ReachBinder: Clone + Debug + PartialEq + ToLispy;
    /// The name a rename asks its target to answer to. Authored, it is a
    /// literal or a template; resolution expands it against the matched
    /// column and what survives is the minted spelling — a phase change
    /// selects the payload, never the position.
    type RenameTarget: Clone + Debug + PartialEq + ToLispy;
    /// The open leaf — `@` and `_` — standing where an OPEN body leaves a
    /// slot. The position that applies the body spends it during
    /// resolution, so no closed resolved or refined expression can carry
    /// one: the payload is uninhabited after resolution, not refused late.
    type OpenLeaf: Clone + Debug + PartialEq + ToLispy;
    /// A cover's callable, as AUTHORED. Resolution applies it per covered
    /// cell — the applying position spends the body's open leaf — so a
    /// bound cover carries applied cells and no callable at all.
    type CoverCallable: Clone + Debug + PartialEq + ToLispy;
    /// The `@` that names which formal receives a piped relation. The
    /// invocation that reads it consumes it, so it never reaches the
    /// ordinary resolved query tree.
    type Placeholder: Clone + Debug + PartialEq + ToLispy;
    /// The `..` that selects a call's context mode. Instantiation consumes
    /// it, so no resolved argument row can still be carrying one.
    type ContextMarker: Clone + Debug + PartialEq + ToLispy;
    /// An `&` / `&&` edge. The resolver expands one into ordinary members,
    /// so a resolved chain cannot still be standing on an edge.
    type ErJoin: Clone + Debug + PartialEq + ToLispy;
    /// What an authored truth PROBE said: how the relation was addressed
    /// and the dequalifying access that IS its correlation.
    ///
    /// Resolution SPENDS it — the probe's relation is resolved and the
    /// correlation synthesized onto it — so a phase past resolution holds
    /// none. That is what makes ONE existence and ONE relational-membership
    /// carrier serve every phase: the field changes what it holds, and no
    /// resolved twin exists to drift from its authored partner.
    type ProbeAddressing: Clone + Debug + PartialEq + ToLispy;

    /// A CORRELATED RESTRICTION — the condition the enclosing join evaluates
    /// and the interior occurrences it reads, as the ONE value the relation
    /// authority's correlation act minted.
    ///
    /// Inhabited in the resolved phase alone: the resolver's act mints it
    /// and the refiner's classification spends it at the interior boundary,
    /// so the authored phase cannot hold one (nothing is correlated before
    /// names resolve) and the refined phase cannot either (one still
    /// standing there was never hoisted). The doors below are how a walk
    /// crosses it, and a phase that admits none refuses rather than
    /// dropping what it was handed.
    type Correlated: Clone + Debug + PartialEq + ToLispy;

    /// A CORRELATED INTERIOR — a join-position body that reads the
    /// enclosing row, still standing on its source population.
    ///
    /// Admitted in the resolved phase alone: nothing is correlated before
    /// names resolve, and the refiner's classification refuses one before
    /// the refined phase.
    type CorrelatedInterior: Clone + Debug + PartialEq + ToLispy;

    /// The DECLARATION a field select picked from, once resolution has read
    /// the catalog.
    ///
    /// A mode-compressed pick names an output of a functional dependency the
    /// CALLEE declared, and that declaration lives in the catalog — so the
    /// authored phase holds nothing rather than a fabricated proof, and a
    /// bound phase holds the witness resolution answered with: the entity,
    /// the resolved mode, and the POSITION the selected output occupies. The
    /// position is what carries the pick to lowering, so no phase past
    /// resolution addresses the field by characters.
    type FunctionalDependency: Clone + Debug + PartialEq + ToLispy;

    /// The BODY a sigma application observes, once resolution has fetched it.
    ///
    /// Uninhabited before resolution: an authored application names a call,
    /// and the rule's body lives in the catalog. A bin predicate keeps its
    /// call in both phases, so this slot is inhabited only where a DQL truth
    /// rule was expanded — and the polarity stays on the application either
    /// way, which is what carries it to the lowering that spells the
    /// observation.
    type SigmaBody: Clone + Debug + PartialEq + ToLispy;

    /// The query-scoped CFE definitions a query still carries: the authored
    /// definitions before resolution, and NOTHING after — resolution spends
    /// each one at its call sites, so a bound phase holds no slot for them,
    /// not an empty list of them, and no consumer needs an arm for one.
    type CfeBindings: Clone + Debug + PartialEq + ToLispy;

    /// The query-scoped CHOE definitions a query still carries, under the
    /// same law as `CfeBindings`: authored before resolution, spent at the
    /// call sites, and no slot at all afterwards.
    type HoBindings: Clone + Debug + PartialEq + ToLispy;

    /// The query-scoped sigma families a query still carries: authored
    /// before resolution and spent at their observation sites, just like
    /// the other local definition families.
    type SigmaBindings: Clone + Debug + PartialEq + ToLispy;

    /// The construction-owned query-local name fact, present only while the
    /// authored definitions it judges are present.
    type QueryLocalNames: Clone + Debug + PartialEq + ToLispy;

    /// What a ground read's mention SAYS: how the author addressed the
    /// relation, and the marks written on the mention itself.
    ///
    /// Resolution SPENDS it. The read's occurrence is minted answering to
    /// the spelling, the `!!` evidence is recorded on that occurrence, and
    /// the passthrough decision is taken where the backend table is looked
    /// up. A phase past resolution therefore has no mention — not an empty
    /// one, none — so no lowering can address a relation by characters and
    /// no vestigial spelling can drift from the scope that answers.
    type Mention: Clone + Debug + PartialEq + ToLispy;

    /// The name a pipe stage's output was written with.
    ///
    /// Resolution SPENDS it: the stage's scope is minted answering to that
    /// spelling and its columns carry it, and from then on the scope is the
    /// only thing that knows. A phase past resolution therefore has no
    /// authored name — not an absent one, none — so a lowering has nothing
    /// to look at and no vestigial second carrier can drift from the scope.
    type StageName: Clone + Debug + PartialEq + ToLispy + Default;

    /// The correlation a bag step carries, read as the tree node it is.
    ///
    /// A payload that CONTAINS tree nodes needs this pair, because a walk
    /// cannot descend into an associated type it knows nothing about. The
    /// two are inverses where the phase admits a correlation at all, and
    /// `admit_correlation` is the door a phase that admits none closes: it
    /// refuses rather than dropping what it was handed.
    fn correlation(carried: &Self::Corr) -> Option<&super::BagCorrelation<Self>>;

    /// The same, by value.
    fn into_correlation(carried: Self::Corr) -> Option<super::BagCorrelation<Self>>;

    /// Put a correlation into this phase's slot.
    fn admit_correlation(
        correlation: Option<super::BagCorrelation<Self>>,
    ) -> crate::error::Result<Self::Corr>;

    /// The member correlation this phase's slot is holding, if any.
    fn member_correlation(carried: &Self::MemberCorr) -> Option<&super::MemberCorrelation<Self>>;

    /// The same, by value.
    fn into_member_correlation(carried: Self::MemberCorr)
        -> Option<super::MemberCorrelation<Self>>;

    /// Put a member correlation into this phase's slot. A decided phase
    /// REFUSES `None`: after resolution the relationship is total, and a
    /// walker with nothing to put there built a member no resolution
    /// decided.
    fn admit_member_correlation(
        correlation: Option<super::MemberCorrelation<Self>>,
    ) -> crate::error::Result<Self::MemberCorr>;

    /// Put the Cartesian decision witness into this phase's slot. The
    /// authored phase refuses: nothing is decided before resolution.
    fn admit_decided() -> crate::error::Result<Self::Decided>;

    /// The correspondence this phase's slot is holding.
    fn correspondence(carried: &Self::Correspondence) -> &super::Correspondence;

    /// The same, by value.
    fn into_correspondence(carried: Self::Correspondence) -> super::Correspondence;

    /// Put a correspondence into this phase's slot. The authored phase
    /// REFUSES rather than dropping it: a fold handing one there built a
    /// correspondence before the access that directs it was resolved.
    fn admit_correspondence(
        correspondence: super::Correspondence,
    ) -> crate::error::Result<Self::Correspondence>;

    /// A column reference read as the caller-pattern slot it classifies to.
    ///
    /// A bare written name offers a binder; anything else constrains its
    /// position with a term. Whether a column CAN be a bare written name
    /// is what the phase selects, so the phase answers once here instead of
    /// each consumer re-deciding from whatever fields it can still see.
    fn classify_column(column: Self::Col) -> super::Slot<Self>;

    /// A binder read back as the column reference it was classified from.
    /// Classification and reconstruction are inverses where the phase still
    /// holds characters; after resolution a term no longer says whether its
    /// slot bound, so the pair is a widening, not a round trip.
    fn binder_column(binder: Self::Binder) -> Self::Col;

    /// An open leaf standing in slot position. Authored, `_` is the
    /// anonymous slot and `@` a value constraint; a bound phase has no leaf
    /// to classify — the payload is uninhabited, and this cannot be called.
    fn classify_open_slot(leaf: Self::OpenLeaf) -> super::Slot<Self>;

    /// A cover's callable read back where the phase still holds one:
    /// `None` after resolution has applied and spent it.
    fn cover_callable(callable: &Self::CoverCallable) -> Option<&super::Callable<Self>>;

    /// The anonymous slot read back as the term it was classified from.
    /// `None` where the phase no longer holds a leaf to spell it with.
    fn anon_slot_term() -> Option<super::DomainExpression<Self>>;

    /// The column a binder bound, when the phase has one. `None` before
    /// resolution — a written name is not yet an identity, and there is no
    /// answer to give.
    fn bound_binder(binder: &Self::Binder) -> Option<crate::relation::PortId>;

    /// The edge a continuation carries, read as the tree node it is.
    ///
    /// Same reason the correlation trio exists: a walk cannot descend into an
    /// associated type it knows nothing about. A phase that admits no edge has
    /// no value to hand back, so these two are only reachable through one.
    fn er_join(carried: &Self::ErJoin) -> &super::ErJoinStep<Self>;

    /// The same, by value.
    fn into_er_join(carried: Self::ErJoin) -> super::ErJoinStep<Self>;

    /// Put an edge into this phase's slot. A phase that admits none refuses
    /// rather than dropping what it was handed.
    fn admit_er_join(step: super::ErJoinStep<Self>) -> crate::error::Result<Self::ErJoin>;

    /// The slot's value for a pipe nobody named. Total, because "unnamed"
    /// is the ordinary case in every phase — only a name that EXISTS has to
    /// ask whether the phase still holds one.
    fn no_stage_name() -> Self::StageName;

    /// The stage name this phase is holding, by value.
    fn into_stage_name(carried: Self::StageName) -> Option<delightql_types::SqlIdentifier>;

    /// Put an authored stage name into this phase's slot. A phase that
    /// admits none REFUSES rather than dropping it: the spelling is spent
    /// where the scope is minted, so a fold still holding one walked past
    /// the place that spends it, and dropping it there would leave the
    /// stage unreachable by the name its author wrote.
    fn admit_stage_name(
        name: Option<delightql_types::SqlIdentifier>,
    ) -> crate::error::Result<Self::StageName>;

    /// THE RELATION A NODE PUBLISHES, read out of this phase's slot by
    /// value: none before resolution, the occurrence after.
    fn into_scope(scope: Self::Scope) -> Option<crate::relation::SemanticRelation>;

    /// Put what a node published in another phase into this phase's slot.
    ///
    /// THE ONE DOOR a node's identity crosses through, and it is the
    /// phases' — no walk is asked. A bound phase REFUSES an absence: the
    /// relation is minted where the node is resolved, so a fold arriving
    /// with none walked past a node nobody resolved. The authored phase
    /// refuses a presence: nothing is resolved there, and a resolved
    /// identity standing in an authored tree would claim a resolution
    /// nobody performed.
    fn admit_scope(
        scope: Option<crate::relation::SemanticRelation>,
    ) -> crate::error::Result<Self::Scope>;

    /// WHICH ARM a whole-heading correlation names, read out of this
    /// phase's slot: the spelling the author wrote beside the glob, or the
    /// relation resolution answered that spelling with.
    fn into_correlation_arm(arm: Self::CorrelationArm) -> CorrelationArm;

    /// Put a named arm into this phase's slot. The same door law as
    /// [`Phase::admit_scope`]: a bound phase refuses a spelling, because an
    /// arm is answered where the correlation is resolved and a fold still
    /// holding the spelling walked past that place; the authored phase
    /// refuses an answered relation.
    fn admit_correlation_arm(arm: CorrelationArm) -> crate::error::Result<Self::CorrelationArm>;

    /// A member's join, read out of this phase's slot.
    fn into_member_join(join: Self::MemberJoin) -> MemberJoinPayload;

    /// Put a member's join into this phase's slot. THE ONE DOOR a member's
    /// join crosses through: the phases before refinement admit roles and
    /// refuse a decided type; the refined phase admits a decided type and
    /// refuses roles, because a join type is decided over a whole RUN by the
    /// rebuild, never by a walk crossing one member at a time.
    fn admit_member_join(join: MemberJoinPayload) -> crate::error::Result<Self::MemberJoin>;

    /// The correlated restriction this phase is holding, read as the value
    /// it is.
    fn correlated(carried: &Self::Correlated) -> &crate::relation::Correlated<Self>;

    /// The same, by value.
    fn into_correlated(carried: Self::Correlated) -> crate::relation::Correlated<Self>;

    /// Put a correlated restriction into this phase's slot. A phase that
    /// admits none refuses: the authored phase has nothing resolved to
    /// correlate, and a correlation reaching the refined phase walked past
    /// the classification that spends it.
    fn admit_correlated(
        correlated: crate::relation::Correlated<Self>,
    ) -> crate::error::Result<Self::Correlated>;

    /// JUDGE A CORRELATED RESTRICTION'S CONDITION: every comparison leaf's
    /// equality role, against the join the restriction makes between the
    /// `interior` occurrences its act derived and the enclosing row. A
    /// correlated value is made only with a condition that came back from
    /// here — at the correlation act and at every rewrite of the condition —
    /// so no value holds an unjudged one. A phase that holds no correlated
    /// restriction refuses, as its admission does.
    fn judge_correlated_condition(
        condition: super::expressions::truth::TruthExpression<Self>,
        interior: &[crate::relation::PortId],
    ) -> crate::error::Result<super::expressions::truth::TruthExpression<Self>>;

    /// Whether this correlated restriction's OWNER is the relation a step
    /// publishes. Every step assembly asks, so a walk cannot pair one
    /// step's correlation with another step's result: the owner travels in
    /// the value, and a result that is not it is refused where the step is
    /// built.
    fn correlated_stands_on(payload: &Self::Correlated, result: &Self::Scope) -> bool;

    /// The correlated interior this phase is holding, read as the value it is.
    fn correlated_interior(
        carried: &Self::CorrelatedInterior,
    ) -> &super::expressions::relational::CorrelatedInterior<Self>;

    /// The same, by value.
    fn into_correlated_interior(
        carried: Self::CorrelatedInterior,
    ) -> super::expressions::relational::CorrelatedInterior<Self>;

    /// Put a correlated interior into this phase's slot. A phase that admits
    /// none refuses: the authored phase has nothing resolved to correlate,
    /// and one reaching the refined phase walked past the classification
    /// that refuses it.
    fn admit_correlated_interior(
        interior: super::expressions::relational::CorrelatedInterior<Self>,
    ) -> crate::error::Result<Self::CorrelatedInterior>;

    /// THE OCCURRENCES A CONDITION READS in this phase, at its own level —
    /// what a correlated restriction's crossing re-reads to prove a rewrite
    /// kept every occurrence its act derived support for. The authored
    /// phase names spellings and holds no occurrence.
    fn occurrences_read(
        condition: &super::expressions::truth::TruthExpression<Self>,
    ) -> Vec<crate::relation::PortId>;

    /// The probe addressing this phase is holding, by value.
    fn into_probe_addressing(
        carried: Self::ProbeAddressing,
    ) -> Option<super::expressions::truth::ProbeAddressing>;

    /// Put an authored probe addressing into this phase's slot. A phase that
    /// has spent it refuses rather than dropping it: the resolver spends the
    /// addressing where it resolves the probe, so a fold still carrying one
    /// walked past that place.
    fn admit_probe_addressing(
        addressing: Option<super::expressions::truth::ProbeAddressing>,
    ) -> crate::error::Result<Self::ProbeAddressing>;

    /// The observed body this phase is holding, read as the tree node it is.
    fn sigma_body(carried: &Self::SigmaBody) -> &super::expressions::truth::TruthExpression<Self>;

    /// The same, writable in place.
    fn sigma_body_mut(
        carried: &mut Self::SigmaBody,
    ) -> &mut super::expressions::truth::TruthExpression<Self>;

    /// The same, by value.
    fn into_sigma_body(
        carried: Self::SigmaBody,
    ) -> super::expressions::truth::TruthExpression<Self>;

    /// Put an observed body into this phase's slot. The authored phase
    /// refuses: a rule's body is fetched where its name is resolved, and an
    /// authored application observes a call.
    fn admit_sigma_body(
        body: super::expressions::truth::TruthExpression<Self>,
    ) -> crate::error::Result<Self::SigmaBody>;

    /// The mode witness this phase is holding, read as the tree node it is.
    /// A payload that CONTAINS tree nodes needs this pair, because a walk
    /// cannot descend into an associated type it knows nothing about.
    fn mode_witness(
        carried: &Self::FunctionalDependency,
    ) -> Option<&super::expressions::functions::ModeWitness<Self>>;

    /// The same, by value.
    fn into_mode_witness(
        carried: Self::FunctionalDependency,
    ) -> Option<super::expressions::functions::ModeWitness<Self>>;

    /// Put a mode witness into this phase's slot. The authored phase REFUSES
    /// rather than dropping it: the declaration is what licenses the pick,
    /// and an authored tree that carried one would be claiming a catalog
    /// reading nobody took.
    fn admit_mode_witness(
        witness: Option<super::expressions::functions::ModeWitness<Self>>,
    ) -> crate::error::Result<Self::FunctionalDependency>;

    /// The mention this phase is holding, by value.
    fn into_mention(carried: Self::Mention) -> Option<super::expressions::GroundMention>;

    /// Put an authored enumeration into this phase's slot. A phase that has
    /// spent it refuses rather than dropping it: a fold still carrying a
    /// spread walked past the container that expands it, and dropping it
    /// there would silently publish nothing where several columns were
    /// addressed.
    fn admit_enumeration() -> crate::error::Result<Self::Enumeration>;

    /// Admit the generated zero-width record into this phase. The authored
    /// phase refuses because only resolution can prove the expansion empty.
    fn admit_empty_record() -> crate::error::Result<Self::EmptyRecord>;

    /// Put an authored mention into this phase's slot.
    ///
    /// Both directions refuse. A phase that has spent the mention refuses a
    /// `Some`, for the same reason `admit_stage_name` does. The authored
    /// phase refuses a `None`, because there is no such thing as a ground
    /// read nobody addressed: a fold arriving there without one lost the
    /// only statement of which relation is being read.
    fn admit_mention(
        mention: Option<super::expressions::GroundMention>,
    ) -> crate::error::Result<Self::Mention>;

    /// The slot's value for a query that binds no CFEs. Total, because "no
    /// definitions" is the ordinary case in every phase.
    fn no_cfe_bindings() -> Self::CfeBindings;

    /// The definitions this phase's slot is holding — empty where the
    /// phase holds none.
    fn cfe_bindings(carried: &Self::CfeBindings) -> &[super::queries::CfeDefinition];

    /// The same, by value.
    fn into_cfe_bindings(carried: Self::CfeBindings) -> Vec<super::queries::CfeDefinition>;

    /// Put query-scoped definitions into this phase's slot. A phase that
    /// has spent them REFUSES rather than dropping them: a fold handing
    /// definitions across the resolution boundary walked past the resolver
    /// that spends them, and dropping them there would silently unbind
    /// every call site they were written for.
    fn admit_cfe_bindings(
        cfes: Vec<super::queries::CfeDefinition>,
    ) -> crate::error::Result<Self::CfeBindings>;

    /// The slot's value for a query that binds no CHOEs.
    fn no_ho_bindings() -> Self::HoBindings;

    /// The CHOE definitions this phase's slot is holding — empty where the
    /// phase holds none.
    fn ho_bindings(carried: &Self::HoBindings) -> &[super::queries::HoDefinition];

    /// The same, by value.
    fn into_ho_bindings(carried: Self::HoBindings) -> Vec<super::queries::HoDefinition>;

    /// Put query-scoped CHOE definitions into this phase's slot; a phase
    /// that has spent them REFUSES, for the reason `admit_cfe_bindings`
    /// does.
    fn admit_ho_bindings(
        hos: Vec<super::queries::HoDefinition>,
    ) -> crate::error::Result<Self::HoBindings>;

    fn no_sigma_bindings() -> Self::SigmaBindings;

    fn sigma_bindings(carried: &Self::SigmaBindings) -> &[super::queries::SigmaDefinition];

    fn into_sigma_bindings(carried: Self::SigmaBindings) -> Vec<super::queries::SigmaDefinition>;

    fn admit_sigma_bindings(
        sigmas: Vec<super::queries::SigmaDefinition>,
    ) -> crate::error::Result<Self::SigmaBindings>;

    fn no_query_local_names() -> Self::QueryLocalNames;

    fn query_local_names_is_empty(carried: &Self::QueryLocalNames) -> bool;

    fn into_query_local_names(
        carried: Self::QueryLocalNames,
    ) -> Option<super::queries::QueryLocalNames>;

    fn admit_query_local_names(
        names: Option<super::queries::QueryLocalNames>,
    ) -> crate::error::Result<Self::QueryLocalNames>;
}

pub fn carry_query_local_names<P: Phase, Q: Phase>(
    carried: P::QueryLocalNames,
) -> crate::error::Result<Q::QueryLocalNames> {
    Q::admit_query_local_names(P::into_query_local_names(carried))
}

/// Carry a query's CFE bindings across a phase change. One door, so
/// whether the definitions survive is decided by the phases involved
/// rather than by whichever walker happened to be written.
pub fn carry_cfe_bindings<P: Phase, Q: Phase>(
    carried: P::CfeBindings,
) -> crate::error::Result<Q::CfeBindings> {
    Q::admit_cfe_bindings(P::into_cfe_bindings(carried))
}

/// Carry a query's CHOE bindings across a phase change — the same one door.
pub fn carry_ho_bindings<P: Phase, Q: Phase>(
    carried: P::HoBindings,
) -> crate::error::Result<Q::HoBindings> {
    Q::admit_ho_bindings(P::into_ho_bindings(carried))
}

/// Carry query-local sigma families across a phase change through the phase
/// boundary, so spent definitions cannot survive as an untracked payload.
pub fn carry_sigma_bindings<P: Phase, Q: Phase>(
    carried: P::SigmaBindings,
) -> crate::error::Result<Q::SigmaBindings> {
    Q::admit_sigma_bindings(P::into_sigma_bindings(carried))
}

/// Carry a pipe stage's name across a phase change.
///
/// One door for every cross-phase fold, so that "the name survives" and
/// "the name is spent" are both decided by the phases involved rather than
/// by whichever walker happened to be written.
pub fn carry_stage_name<P: Phase, Q: Phase>(
    carried: P::StageName,
) -> crate::error::Result<Q::StageName> {
    Q::admit_stage_name(P::into_stage_name(carried))
}

/// Which arm a whole-heading correlation names, as a phase holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum CorrelationArm {
    /// The stage name the author wrote beside the glob.
    Spelled(delightql_types::SqlIdentifier),
    /// The relation resolution answered that spelling with.
    Answered(crate::relation::SemanticRelation),
}

/// CARRY WHAT A NODE PUBLISHES across a phase change, or through a
/// same-phase rewrite.
///
/// THE PHASES' LAW, NOT THE WALK'S. A node's identity is the answer the
/// relation authority gave when the node was derived, and a rewrite is not
/// a place a node acquires another: the door hands the destination phase
/// exactly what the source node published, and whether that phase may hold
/// it is the phases' answer. There is no hook a walk implements for this —
/// a walk that could answer with a relation could put another relation's
/// identity under a step derived over this one, and the admission judging
/// a correlated step against its prefix would be judging against the
/// walk's answer. A crossing into a bound phase from the authored one
/// refuses: the relation is minted where the node is resolved.
pub fn carry_scope<P: Phase, Q: Phase>(scope: P::Scope) -> crate::error::Result<Q::Scope> {
    Q::admit_scope(P::into_scope(scope))
}

/// Carry which arm a whole-heading correlation names. The same law as
/// [`carry_scope`], for the one other relation identity the tree holds:
/// the arm resolution answered a spelling with crosses as itself, and no
/// walk is asked for one.
pub fn carry_correlation_arm<P: Phase, Q: Phase>(
    arm: P::CorrelationArm,
) -> crate::error::Result<Q::CorrelationArm> {
    Q::admit_correlation_arm(P::into_correlation_arm(arm))
}

/// Carry a correlated restriction across a phase change. One door: the walk
/// rewrites the condition inside the value the act minted — never a
/// condition beside a separately held occurrence list — the value judges the
/// rewrite before holding it, and whether the destination phase may hold the
/// result is the phases' answer, not the walker's.
pub fn carry_correlated<P: Phase, Q: Phase>(
    carried: P::Correlated,
    condition: impl FnOnce(
        super::expressions::truth::TruthExpression<P>,
    ) -> crate::error::Result<super::expressions::truth::TruthExpression<Q>>,
) -> crate::error::Result<Q::Correlated> {
    Q::admit_correlated(P::into_correlated(carried).crossing(condition)?)
}

fn authored_phase_holds_no_correlation() -> crate::error::DelightQLError {
    Internal::invariant(
        "correlated",
        "a correlated restriction is minted where the relation it stands on is resolved; the \
         authored phase holds none",
    )
}

/// Carry a truth probe's authored addressing across a phase change. One
/// door, so whether the addressing survives is decided by the phases rather
/// than by whichever walker happened to be written.
pub fn carry_probe_addressing<P: Phase, Q: Phase>(
    carried: P::ProbeAddressing,
) -> crate::error::Result<Q::ProbeAddressing> {
    Q::admit_probe_addressing(P::into_probe_addressing(carried))
}

/// Carry a member's correspondence across a phase change. One door, so
/// whether a phase may hold one is decided by the phases rather than by
/// whichever walker happened to be written.
pub fn carry_correspondence<P: Phase, Q: Phase>(
    carried: P::Correspondence,
) -> crate::error::Result<Q::Correspondence> {
    Q::admit_correspondence(P::into_correspondence(carried))
}

/// Carry a member's Cartesian decision witness across a phase change.
pub fn carry_decided<P: Phase, Q: Phase>(_: P::Decided) -> crate::error::Result<Q::Decided> {
    Q::admit_decided()
}

/// Carry a ground read's mention across a phase change.
///
/// The same one door as `carry_stage_name`, for the same reason: whether the
/// authored addressing survives is decided by the phases involved, not by
/// whichever walker happened to be written.
pub fn carry_mention<P: Phase, Q: Phase>(carried: P::Mention) -> crate::error::Result<Q::Mention> {
    Q::admit_mention(P::into_mention(carried))
}

/// The authored phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unresolved;

impl Phase for Unresolved {
    type ColumnOrdinal = super::ColumnOrdinal;
    type FormalSelector = super::definitions::FormalSelector;
    fn into_argument(selector: Self::FormalSelector) -> super::definitions::FormalSelector {
        selector
    }
    fn admit_argument(
        selector: super::definitions::FormalSelector,
    ) -> crate::error::Result<Self::FormalSelector> {
        Ok(selector)
    }
    type PhysicalColumn = crate::pipeline::asts::vocabulary::Never;
    fn into_physical(column: Self::PhysicalColumn) -> crate::error::Result<crate::names::ColId> {
        match column {}
    }
    fn cte_binding_of_authored(
        binding: crate::pipeline::bindings::AuthoredBinding<Self>,
    ) -> crate::error::Result<crate::pipeline::bindings::CteBinding<Self>> {
        Ok(binding.into_binding())
    }
    fn admit_physical(_: crate::names::ColId) -> crate::error::Result<Self::PhysicalColumn> {
        Err(Internal::invariant(
            "reference",
            "a physical SQL slot cannot enter an authored tree",
        ))
    }
    type ColumnRange = super::ColumnRange;
    type Enumeration = ();
    type EmptyRecord = crate::pipeline::asts::vocabulary::Never;
    // A consulted view is the RESOLVER's own product — a name it looked up
    // and expanded. The authored tree never holds one, so the slot is
    // uninhabited and the two consulted-view forms cannot be built here.
    // `()` said "nothing yet", which is a different claim: it left an
    // authored consulted view constructible by anyone who wanted one.
    // No decision has been taken about recursion here — not a default one,
    // none. The clauses of one definition have not even met: an authored
    // binding is ONE clause, and the resolver groups them where a body's
    // reference is known to be its own binding's scope.
    type CteBody = super::expressions::chain::Chain<Unresolved>;
    // The subject as constructed: an authored spelling with its effect
    // declaration, or a compiler-built carrier scope. An authored variant
    // has no room for a bound scope, so an unresolved user binding cannot
    // claim resolution it never had.
    // The head and the provenance judgments still await resolution, which
    // spends them whole.
    type CteAuthority = super::queries::CteAuthority;
    type CteBindingState = crate::pipeline::bindings::UnresolvedBindingState;
    // Which output an expression publishes is the resolver's answer, and a
    // relation that has not been resolved publishes nothing anyone can name.
    type Output = ();
    // Nothing has been resolved, so there is no occurrence a scalarized
    // relation could publish.
    type ScalarOutput = ();
    type Drill = super::operators::AuthoredDrill;
    // Nothing has been resolved, so there is no relation to name. A phantom
    // schema standing here would be a fabricated answer to a question no one
    // can yet ask.
    type Scope = ();
    // A correlation is never written as a field. What an author writes is a
    // predicate standing over a bag run; which pair it constrains — if any —
    // is settled downstream, so the authored phase carries no such field to
    // set.
    type Corr = ();
    type CorrelationArm = delightql_types::SqlIdentifier;
    // A correspondence is read off the ACCESS at resolution. The authored
    // phase holds the access, not its consequence, so there is nothing here
    // to build and `Correspond` has no inhabitant before resolution.
    type Correspondence = crate::pipeline::asts::vocabulary::Never;
    type MemberCorr = Option<super::MemberCorrelation<Self>>;
    type MemberJoin = super::operators::JoinRoles;
    type Decided = crate::pipeline::asts::vocabulary::Never;
    type Entity = crate::pipeline::asts::vocabulary::Ref;
    type Col = super::columns::AuthoredColumn;
    type Binder = super::columns::WrittenBinder;
    // A pattern member is written here; the occurrence it publishes is
    // minted where its destructure or narrowing binds.
    type PatternBinder = super::columns::WrittenBinder;
    type ReachBinder = Option<delightql_types::SqlIdentifier>;
    type RenameTarget = super::specs::NameTarget;
    type OpenLeaf = super::expressions::DomainHole;
    type CoverCallable = super::expressions::Callable<Self>;
    type Placeholder = super::columns::AtSign;
    type ContextMarker = super::columns::ContextMarker;
    type ErJoin = super::ErJoinStep<Self>;
    type StageName = Option<delightql_types::SqlIdentifier>;
    type Mention = super::expressions::GroundMention;
    type ProbeAddressing = super::expressions::truth::ProbeAddressing;
    /// Nothing is correlated before names resolve.
    type Correlated = crate::pipeline::asts::vocabulary::Never;
    type CorrelatedInterior = crate::pipeline::asts::vocabulary::Never;
    // A DQL truth rule's body is fetched where its NAME is resolved, so an
    // authored sigma application observes a call and nothing else.
    type SigmaBody = crate::pipeline::asts::vocabulary::Never;
    // The authored definitions, in authored order, still unspent.
    type CfeBindings = Vec<super::queries::CfeDefinition>;
    type HoBindings = Vec<super::queries::HoDefinition>;
    type SigmaBindings = Vec<super::queries::SigmaDefinition>;
    type QueryLocalNames = super::queries::QueryLocalNames;
    // The declaration a pick names lives in the catalog, and the authored
    // phase has not read it. Nothing here — not an absent witness, none.
    type FunctionalDependency = ();

    fn admit_empty_record() -> crate::error::Result<Self::EmptyRecord> {
        Err(Internal::invariant(
            "empty_record",
            "a generated empty record cannot stand in the authored phase",
        ))
    }

    fn sigma_body(carried: &Self::SigmaBody) -> &super::expressions::truth::TruthExpression<Self> {
        match *carried {}
    }

    fn sigma_body_mut(
        carried: &mut Self::SigmaBody,
    ) -> &mut super::expressions::truth::TruthExpression<Self> {
        match *carried {}
    }

    fn into_sigma_body(
        carried: Self::SigmaBody,
    ) -> super::expressions::truth::TruthExpression<Self> {
        match carried {}
    }

    fn admit_sigma_body(
        _: super::expressions::truth::TruthExpression<Self>,
    ) -> crate::error::Result<Self::SigmaBody> {
        Err(Internal::invariant(
            "sigma_body",
            "a sigma application reached the authored phase observing a BODY: an authored \
             application names a call, and a rule's body is fetched where that name is \
             resolved",
        ))
    }

    fn mode_witness(
        _: &Self::FunctionalDependency,
    ) -> Option<&super::expressions::functions::ModeWitness<Self>> {
        None
    }

    fn into_mode_witness(
        _: Self::FunctionalDependency,
    ) -> Option<super::expressions::functions::ModeWitness<Self>> {
        None
    }

    fn admit_mode_witness(
        witness: Option<super::expressions::functions::ModeWitness<Self>>,
    ) -> crate::error::Result<Self::FunctionalDependency> {
        match witness {
            None => Ok(()),
            Some(_) => Err(Internal::invariant(
                "functional_dependency",
                "a field select reached the authored phase carrying a declaration: the \
                 mode lives in the catalog, and an authored pick names an output without \
                 having read one",
            )),
        }
    }

    fn into_probe_addressing(
        carried: Self::ProbeAddressing,
    ) -> Option<super::expressions::truth::ProbeAddressing> {
        Some(carried)
    }

    fn correlated(carried: &Self::Correlated) -> &crate::relation::Correlated<Self> {
        match *carried {}
    }

    fn into_correlated(carried: Self::Correlated) -> crate::relation::Correlated<Self> {
        match carried {}
    }

    fn admit_correlated(
        _: crate::relation::Correlated<Self>,
    ) -> crate::error::Result<Self::Correlated> {
        Err(authored_phase_holds_no_correlation())
    }

    fn judge_correlated_condition(
        _: super::expressions::truth::TruthExpression<Self>,
        _: &[crate::relation::PortId],
    ) -> crate::error::Result<super::expressions::truth::TruthExpression<Self>> {
        Err(authored_phase_holds_no_correlation())
    }

    fn correlated_stands_on(payload: &Self::Correlated, _: &Self::Scope) -> bool {
        match *payload {}
    }
    fn correlated_interior(
        carried: &Self::CorrelatedInterior,
    ) -> &super::expressions::relational::CorrelatedInterior<Self> {
        match *carried {}
    }
    fn into_correlated_interior(
        carried: Self::CorrelatedInterior,
    ) -> super::expressions::relational::CorrelatedInterior<Self> {
        match carried {}
    }
    fn admit_correlated_interior(
        _: super::expressions::relational::CorrelatedInterior<Self>,
    ) -> crate::error::Result<Self::CorrelatedInterior> {
        Err(Internal::invariant(
            "correlated interior",
            "a correlated interior reached a phase that holds none: its classification in \
             the resolved phase refuses it before anything crosses",
        ))
    }

    fn occurrences_read(
        _: &super::expressions::truth::TruthExpression<Self>,
    ) -> Vec<crate::relation::PortId> {
        Vec::new()
    }

    fn admit_probe_addressing(
        addressing: Option<super::expressions::truth::ProbeAddressing>,
    ) -> crate::error::Result<Self::ProbeAddressing> {
        addressing.ok_or_else(|| {
            Internal::invariant(
                "probe_addressing",
                "a truth probe reached the authored phase with no addressing: there is no \
                 probe of a relation nobody addressed",
            )
        })
    }

    fn into_mention(carried: Self::Mention) -> Option<super::expressions::GroundMention> {
        Some(carried)
    }

    fn admit_mention(
        mention: Option<super::expressions::GroundMention>,
    ) -> crate::error::Result<Self::Mention> {
        mention.ok_or_else(|| {
            Internal::invariant(
                "mention",
                "a ground read reached the authored phase with no mention: there is no \
                 read of a relation nobody addressed",
            )
        })
    }

    fn admit_enumeration() -> crate::error::Result<Self::Enumeration> {
        Ok(())
    }

    fn no_cfe_bindings() -> Self::CfeBindings {
        Vec::new()
    }

    fn cfe_bindings(carried: &Self::CfeBindings) -> &[super::queries::CfeDefinition] {
        carried
    }

    fn into_cfe_bindings(carried: Self::CfeBindings) -> Vec<super::queries::CfeDefinition> {
        carried
    }

    fn admit_cfe_bindings(
        cfes: Vec<super::queries::CfeDefinition>,
    ) -> crate::error::Result<Self::CfeBindings> {
        Ok(cfes)
    }

    fn no_ho_bindings() -> Self::HoBindings {
        Vec::new()
    }

    fn ho_bindings(carried: &Self::HoBindings) -> &[super::queries::HoDefinition] {
        carried
    }

    fn into_ho_bindings(carried: Self::HoBindings) -> Vec<super::queries::HoDefinition> {
        carried
    }

    fn admit_ho_bindings(
        hos: Vec<super::queries::HoDefinition>,
    ) -> crate::error::Result<Self::HoBindings> {
        Ok(hos)
    }

    fn no_sigma_bindings() -> Self::SigmaBindings {
        Vec::new()
    }

    fn sigma_bindings(carried: &Self::SigmaBindings) -> &[super::queries::SigmaDefinition] {
        carried
    }

    fn into_sigma_bindings(carried: Self::SigmaBindings) -> Vec<super::queries::SigmaDefinition> {
        carried
    }

    fn admit_sigma_bindings(
        sigmas: Vec<super::queries::SigmaDefinition>,
    ) -> crate::error::Result<Self::SigmaBindings> {
        Ok(sigmas)
    }

    fn no_query_local_names() -> Self::QueryLocalNames {
        super::queries::QueryLocalNames::default()
    }

    fn query_local_names_is_empty(carried: &Self::QueryLocalNames) -> bool {
        carried.is_empty()
    }

    fn into_query_local_names(
        carried: Self::QueryLocalNames,
    ) -> Option<super::queries::QueryLocalNames> {
        Some(carried)
    }

    fn admit_query_local_names(
        names: Option<super::queries::QueryLocalNames>,
    ) -> crate::error::Result<Self::QueryLocalNames> {
        names.ok_or_else(|| {
            Internal::invariant(
                "query_local_names",
                "an authored query cannot be rebuilt without its query-local name fact",
            )
        })
    }

    fn no_stage_name() -> Self::StageName {
        None
    }

    fn into_stage_name(carried: Self::StageName) -> Option<delightql_types::SqlIdentifier> {
        carried
    }

    fn admit_stage_name(
        name: Option<delightql_types::SqlIdentifier>,
    ) -> crate::error::Result<Self::StageName> {
        Ok(name)
    }

    fn into_scope(_: Self::Scope) -> Option<crate::relation::SemanticRelation> {
        None
    }

    fn admit_scope(
        scope: Option<crate::relation::SemanticRelation>,
    ) -> crate::error::Result<Self::Scope> {
        match scope {
            None => Ok(()),
            Some(_) => Err(Internal::invariant(
                "result",
                "a resolved relation reached the authored phase: nothing is resolved there, \
                 and an authored node publishing one would claim a resolution nobody performed",
            )),
        }
    }

    fn into_correlation_arm(arm: Self::CorrelationArm) -> CorrelationArm {
        CorrelationArm::Spelled(arm)
    }

    fn into_member_join(join: Self::MemberJoin) -> MemberJoinPayload {
        MemberJoinPayload::Roles(join)
    }

    fn admit_member_join(join: MemberJoinPayload) -> crate::error::Result<Self::MemberJoin> {
        join.roles()
    }

    fn admit_correlation_arm(arm: CorrelationArm) -> crate::error::Result<Self::CorrelationArm> {
        match arm {
            CorrelationArm::Spelled(spelling) => Ok(spelling),
            CorrelationArm::Answered(_) => Err(Internal::invariant(
                "correlation_arm",
                "a whole-heading correlation's answered arm reached the authored phase: an \
                 authored correlation names its arms by spelling",
            )),
        }
    }

    fn er_join(carried: &Self::ErJoin) -> &super::ErJoinStep<Self> {
        carried
    }

    fn into_er_join(carried: Self::ErJoin) -> super::ErJoinStep<Self> {
        carried
    }

    fn admit_er_join(step: super::ErJoinStep<Self>) -> crate::error::Result<Self::ErJoin> {
        Ok(step)
    }

    fn classify_column(column: Self::Col) -> super::Slot<Self> {
        match column {
            // Only a name standing alone binds: a qualifier makes the term
            // a reference to somebody else's column, which constrains the
            // position instead of offering a name for it.
            super::columns::AuthoredColumn {
                name,
                qualifier: None,
                namespace_path,
            } => super::Slot::Bind(super::columns::WrittenBinder {
                name,
                namespace_path,
            }),
            // A QUALIFIED name reuses the enclosing logical value; it
            // addresses a column rather than offering a fresh one.
            qualified => super::Slot::Reuse(super::expressions::NamedReference(qualified)),
        }
    }

    fn binder_column(binder: Self::Binder) -> Self::Col {
        super::columns::AuthoredColumn {
            name: binder.name,
            qualifier: None,
            namespace_path: binder.namespace_path,
        }
    }

    fn cover_callable(callable: &Self::CoverCallable) -> Option<&super::Callable<Self>> {
        Some(callable)
    }

    fn classify_open_slot(leaf: Self::OpenLeaf) -> super::Slot<Self> {
        match leaf {
            super::expressions::DomainHole::Disregarded => super::Slot::Anon,
            // `@` in slot position constrains the position with the value
            // that flows in — the same reading any non-name term takes.
            hole @ super::expressions::DomainHole::CompositionInput => {
                super::Slot::Constraint(Box::new(super::DomainExpression::Application(
                    super::FunctionApplication::Open(hole),
                )))
            }
        }
    }

    fn anon_slot_term() -> Option<super::DomainExpression<Self>> {
        Some(super::DomainExpression::Application(
            super::FunctionApplication::Open(super::expressions::DomainHole::Disregarded),
        ))
    }

    fn bound_binder(binder: &Self::Binder) -> Option<crate::relation::PortId> {
        let _ = binder;
        None
    }

    fn correlation(_: &()) -> Option<&super::BagCorrelation<Self>> {
        None
    }

    fn into_correlation(_: ()) -> Option<super::BagCorrelation<Self>> {
        None
    }

    fn admit_correlation(
        correlation: Option<super::BagCorrelation<Self>>,
    ) -> crate::error::Result<()> {
        match correlation {
            None => Ok(()),
            Some(_) => Err(Internal::invariant(
                "bag_op",
                "a correlation cannot be written on an authored bag step: what is \
                 written is a predicate standing over the run",
            )),
        }
    }

    fn correspondence(carried: &Self::Correspondence) -> &super::Correspondence {
        match *carried {}
    }

    fn into_correspondence(carried: Self::Correspondence) -> super::Correspondence {
        match carried {}
    }

    fn member_correlation(carried: &Self::MemberCorr) -> Option<&super::MemberCorrelation<Self>> {
        carried.as_ref()
    }

    fn into_member_correlation(
        carried: Self::MemberCorr,
    ) -> Option<super::MemberCorrelation<Self>> {
        carried
    }

    fn admit_member_correlation(
        correlation: Option<super::MemberCorrelation<Self>>,
    ) -> crate::error::Result<Self::MemberCorr> {
        Ok(correlation)
    }

    fn admit_decided() -> crate::error::Result<Self::Decided> {
        Err(Internal::invariant(
            "member",
            "a member's Cartesian decision cannot stand on an authored tree: the \
             live bare interface has not been enumerated before resolution",
        ))
    }

    fn admit_correspondence(
        _: super::Correspondence,
    ) -> crate::error::Result<Self::Correspondence> {
        Err(Internal::invariant(
            "member",
            "a correspondence cannot stand on an authored member: it is read off \
             the access that directs it, and that access has not been resolved",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::expressions::GroundMention;
    use super::super::metadata::NamespacePath;
    use super::super::QualifiedName;
    use super::*;

    fn mention(name: &str) -> GroundMention {
        GroundMention::named(QualifiedName {
            namespace_path: NamespacePath::empty(),
            name: name.into(),
        })
    }

    /// The declaration is the CATALOG's. An authored tree carrying one is
    /// claiming a reading nobody took; a bound tree lacking one has lost the
    /// only thing that says which output the pick selects.
    #[test]
    fn the_mode_witness_crosses_in_one_direction_only() {
        use super::super::expressions::functions::{
            FactFunctionArm, FactFunctionMode, ModeWitness,
        };
        use super::super::expressions::FunctionApplication;
        use super::super::DomainExpression;
        use crate::pipeline::asts::vocabulary::Vec1;

        let witness = ModeWitness::<Unresolved> {
            entity: QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: "shipping".into(),
            },
            mode: FactFunctionMode {
                inputs: Vec1::new("zone".into()),
                outputs: Vec1::new("carrier".into()),
                arms: Vec1::new(FactFunctionArm {
                    inputs: Vec1::new(crate::pipeline::asts::core::LiteralValue::Null),
                    outputs: Vec1::new(DomainExpression::Application(FunctionApplication::Ground(
                        crate::pipeline::asts::core::LiteralValue::Null,
                    ))),
                }),
                default: None,
            },
            inputs: Vec::new(),
            selected: 0,
        };
        assert!(Unresolved::admit_mode_witness(Some(witness)).is_err());
        assert!(Unresolved::admit_mode_witness(None).is_ok());
    }

    /// The carries the pipeline actually makes.
    #[test]
    fn the_two_live_carries_pass() {
        assert_eq!(
            carry_mention::<Unresolved, Unresolved>(mention("users")).expect("authored"),
            mention("users")
        );
    }
}

/// A member's join as it crosses between phases: the roles its syntax fixed,
/// or the type a rebuild decided over its run.
pub enum MemberJoinPayload {
    Roles(super::operators::JoinRoles),
    Decided(super::operators::JoinType),
}

impl MemberJoinPayload {
    fn roles(self) -> crate::error::Result<super::operators::JoinRoles> {
        match self {
            MemberJoinPayload::Roles(roles) => Ok(roles),
            MemberJoinPayload::Decided(_) => Err(Internal::invariant(
                "member join",
                "a decided join type reached a phase that holds member roles: roles are never \
                 recovered from a join type",
            )),
        }
    }
}
