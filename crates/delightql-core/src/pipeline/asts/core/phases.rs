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
    /// The decided-once recursion fact, taken where a self-reference binds.
    /// A `ScopeId` is not a decision, which is why this is its own slot.
    type Recursion: Clone + Debug + PartialEq + ToLispy;
    /// What a CTE binding stands on: the authored subject — spelling with
    /// its strop bit and effect declaration, or a compiler-built carrier
    /// scope — before resolution, and the exact bound scope after.
    ///
    /// Resolution SPENDS the subject where it mints the binding's scope, so
    /// no bound phase can hold an authored-only name, an optional binding,
    /// or an unjudged effect mark — not spent copies of them, none.
    type CteSubject: Clone + Debug + PartialEq + ToLispy;
    /// The head and provenance judgments resolution spends from an authored
    /// CTE binding (`CteAuthority`): present before resolution, and DELETED
    /// by the phase system after — a bound phase cannot carry a spent copy.
    type CteAuthority: Clone + Debug + PartialEq + ToLispy;
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
    /// The columns a destructuring pattern produces, decided where the
    /// pattern binds.
    type Destructure: Clone + Debug + PartialEq + ToLispy;
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
    /// A consulted view's already-bound output boundary.
    type Consulted: Clone + Debug + PartialEq + ToLispy;
    /// A positional column reference — `|2|`, `|-1|` — and its authored
    /// qualification.
    type ColumnOrdinal: Clone + Debug + PartialEq + ToLispy;
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
    /// Which formal of a higher-order group a piped relation landed at,
    /// while the landing is still the resolver's to judge (R8: the first
    /// formal or the one authored `@`; never search, never displace).
    /// Spent by that judgment — a resolved group carries no landing.
    type HoLanding: Clone + Debug + PartialEq + Default + ToLispy;
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

    /// The column a CROSSED caller-pattern slot unifies with.
    ///
    /// Nothing before resolution — which column a slot constrains is its
    /// POSITION in the pattern, and no heading has been read — and the
    /// occurrence itself after. The pair is what lets the unification be
    /// built at lowering, null-safely, instead of at resolution as a
    /// comparison whose value operand would have to be able to contain a
    /// truth.
    type ConstrainedColumn: Clone + Debug + PartialEq + ToLispy;

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
    fn bound_binder(binder: &Self::Binder) -> Option<crate::names::ColId>;

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

    /// The column a crossed slot constrains, when the phase has bound one.
    fn into_constrained_column(carried: Self::ConstrainedColumn) -> Option<crate::names::ColId>;

    /// The observed body this phase is holding, read as the tree node it is.
    fn sigma_body(carried: &Self::SigmaBody) -> &super::expressions::truth::TruthExpression<Self>;

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

    /// Put a crossed slot's column into this phase's slot. Both directions
    /// refuse: an authored slot has no column to hold, and a bound phase
    /// cannot hold a crossing whose column nobody resolved — the unification
    /// is built from that column, so arriving without one would silently
    /// drop the constraint.
    fn admit_constrained_column(
        column: Option<crate::names::ColId>,
    ) -> crate::error::Result<Self::ConstrainedColumn>;

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
}

/// Carry a query's CFE bindings across a phase change. One door, so
/// whether the definitions survive is decided by the phases involved
/// rather than by whichever walker happened to be written.
pub fn carry_cfe_bindings<P: Phase, Q: Phase>(
    carried: P::CfeBindings,
) -> crate::error::Result<Q::CfeBindings> {
    Q::admit_cfe_bindings(P::into_cfe_bindings(carried))
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

/// Carry a truth probe's authored addressing across a phase change. One
/// door, so whether the addressing survives is decided by the phases rather
/// than by whichever walker happened to be written.
pub fn carry_probe_addressing<P: Phase, Q: Phase>(
    carried: P::ProbeAddressing,
) -> crate::error::Result<Q::ProbeAddressing> {
    Q::admit_probe_addressing(P::into_probe_addressing(carried))
}

/// Carry a crossed slot's column across a phase change. One door, so whether
/// a phase may hold one is decided by the phases rather than by whichever
/// walker happened to be written.
pub fn carry_constrained_column<P: Phase, Q: Phase>(
    carried: P::ConstrainedColumn,
) -> crate::error::Result<Q::ConstrainedColumn> {
    Q::admit_constrained_column(P::into_constrained_column(carried))
}

/// Carry a member's correspondence across a phase change. One door, so
/// whether a phase may hold one is decided by the phases rather than by
/// whichever walker happened to be written.
pub fn carry_correspondence<P: Phase, Q: Phase>(
    carried: P::Correspondence,
) -> crate::error::Result<Q::Correspondence> {
    Q::admit_correspondence(P::into_correspondence(carried))
}

/// Carry a ground read's mention across a phase change.
///
/// The same one door as `carry_stage_name`, for the same reason: whether the
/// authored addressing survives is decided by the phases involved, not by
/// whichever walker happened to be written.
pub fn carry_mention<P: Phase, Q: Phase>(carried: P::Mention) -> crate::error::Result<Q::Mention> {
    Q::admit_mention(P::into_mention(carried))
}

/// Past resolution the slot is uninhabited by an authored spelling: `()`
/// holds nothing to read, so no lowering can reach one and no widening of
/// these types can happen without this stopping compiling.
const _: () = {
    fn spent<P: Phase<StageName = (), Mention = ()>>() {}
    let _ = spent::<Resolved>;
    let _ = spent::<Refined>;
};

/// A query-scoped CFE definition is a BINDING consumed during resolution:
/// the authored phase carries the definitions and a bound phase carries no
/// slot for them — `()` holds nothing to read — so no resolved or refined
/// query can hold a definition, and no consumer needs an arm for one.
const _: () = {
    fn authored<P: Phase<CfeBindings = Vec<super::queries::CfeDefinition>>>() {}
    let _ = authored::<Unresolved>;
    fn spent<P: Phase<CfeBindings = ()>>() {}
    let _ = spent::<Resolved>;
    let _ = spent::<Refined>;
};

/// A CTE binding's subject is spent at resolution: the authored phase holds
/// the typed authored subject — a spelling with its effect declaration, or
/// a structural carrier scope — and a bound phase holds the exact `ScopeId`
/// and nothing else. A resolved binding therefore cannot carry an
/// authored-only name, an optional binding, or an effect boolean, and an
/// authored user binding cannot carry a resolved scope.
const _: () = {
    fn authored<P: Phase<CteSubject = super::queries::CteSubject>>() {}
    let _ = authored::<Unresolved>;
    fn bound<P: Phase<CteSubject = crate::names::ScopeId>>() {}
    let _ = bound::<Resolved>;
    let _ = bound::<Refined>;
};

/// THE DECLARATION IS THE CATALOG'S, AND A BOUND PHASE HAS IT.
///
/// `()` before resolution: the authored pick names an output without having
/// read anything, so there is no proof to hold and none to fabricate. A
/// witness after, and not an optional one: a field select that survived
/// resolution HAS the declaration that licensed it, so no consumer needs an
/// arm for one that does not and no lowering can be reached without it.
const _: () = {
    fn authored<P: Phase<FunctionalDependency = ()>>() {}
    let _ = authored::<Unresolved>;
    fn bound<
        P: Phase<
            FunctionalDependency = Box<super::expressions::functions::ModeWitness<P>>,
            Col = super::columns::ColumnOccurrence,
        >,
    >() {
    }
    let _ = bound::<Resolved>;
    let _ = bound::<Refined>;
};

/// A zero-width record is likewise a resolver product, never authored
/// syntax. It remains inhabited through the phases that consume resolution.
const _: () = {
    fn authored<P: Phase<EmptyRecord = crate::pipeline::asts::vocabulary::Never>>() {}
    let _ = authored::<Unresolved>;
};

/// And the enumeration is spent by being UNINHABITED: `Spread` has three
/// arms and every one of their payloads is `Never` after resolution, so a
/// resolved or refined tree cannot hold a spread anywhere — not in a
/// publication item, a selector, a rename source, a record member, or an
/// argument row.
const _: () = {
    fn expanded<P: Phase<Enumeration = crate::pipeline::asts::vocabulary::Never>>() {}
    let _ = expanded::<Resolved>;
    let _ = expanded::<Refined>;
};

/// The authored phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unresolved;
/// After name resolution: handles, not spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved;
/// After refinement: correlations settled, strategies chosen — the shape a
/// lowering plan consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refined;

// After resolution a column IS an occurrence, and no slot offers a name:
// binding is the resolver's act and it finishes there. `Never` is not "no
// binder was found" — it is the absence of the form, so a slot that offers
// a name cannot be built and no consumer needs an arm for one.
macro_rules! bound_columns {
    () => {
        type Col = super::columns::ColumnOccurrence;
        type Binder = crate::names::ColId;
        // A resolved CTE binding IS its scope. The authored spelling and
        // the effect declaration were spent where that scope was minted, so
        // a bound phase holds the identity and nothing beside it — no name
        // to disagree with the scope, no "maybe bound" state to match on.
        type CteSubject = crate::names::ScopeId;
        // The head was grouped and spent, and the provenance judgments were
        // taken, where the binding's scope was minted. Nothing survives —
        // not a constant glob, not a copied provenance tag.
        type CteAuthority = ();
        // A position and a range of positions are SPELLINGS. Resolution
        // answers them against a heading and what comes back is the
        // occurrence, so after resolution there is no ordinal left to carry
        // — not an ordinal that failed to resolve, none.
        type ColumnOrdinal = crate::pipeline::asts::vocabulary::Never;
        type ColumnRange = crate::pipeline::asts::vocabulary::Never;
        // A spread is expanded where its container resolves it. After that
        // there is no enumeration left to carry — not an expanded one,
        // none — so no arm of `Spread` can be built.
        type Enumeration = crate::pipeline::asts::vocabulary::Never;
        type EmptyRecord = ();
        // Resolution expands a rename target against the matched column;
        // the spelling it minted is what a bound phase carries.
        type RenameTarget = crate::names::Spelling;
        // The applying position spent the leaf at resolution. A closed
        // phase cannot carry one — not an unapplied hole, none.
        type OpenLeaf = crate::pipeline::asts::vocabulary::Never;
        // The cover applied its callable per cell at resolution: the
        // callable is SPENT, and what a bound phase carries is the applied
        // cells beside this absence.
        type CoverCallable = ();
        // A landing is consumed where the invocation that reads it is built.
        // What survives is the argument role, not the mark.
        type Placeholder = crate::pipeline::asts::vocabulary::Never;
        // The context marker is consumed where the call instantiates. A
        // resolved argument row carries the captured context as ordinary
        // arguments, never the mark.
        type ContextMarker = crate::pipeline::asts::vocabulary::Never;
        // The landing was judged against the formals and spent: the source
        // stands as an ordinary member or carrier, and nothing downstream
        // can tell a piped call from a direct one.
        type HoLanding = ();
        // The resolver expands `&`/`&&` into ordinary members. A resolved
        // chain standing on an edge was a runtime panic in six walks; it is
        // now a shape nobody can build.
        type ErJoin = crate::pipeline::asts::vocabulary::Never;
        // Spent at resolution, where the stage's scope was minted answering
        // to it. What a later phase would do with the spelling is nothing —
        // it addresses scopes — so it is not carried as an absence either.
        type StageName = ();
        // Likewise the mention: which relation a ground read names was
        // answered at resolution and recorded on the occurrence. A resolved
        // ground is addressed by scope and by nothing else.
        type Mention = ();
        // A query-scoped definition is spent at its call sites during
        // resolution. A bound query has no slot for one — not an empty
        // list, none.
        type CfeBindings = ();
        // The probe's addressing is spent where the probe is resolved.
        type ProbeAddressing = ();
        // A crossed slot's column is ANSWERED at resolution: the pattern
        // reads its relation's heading and the slot's position names one
        // occurrence. From here the pair travels to the lowering that
        // spells the unification.
        type ConstrainedColumn = crate::names::ColId;
        type SigmaBody = Box<super::expressions::truth::TruthExpression<Self>>;
        // The declaration ANSWERED: which entity declared the mode, the mode
        // itself resolved, and the position the picked output occupies.
        type FunctionalDependency = Box<super::expressions::functions::ModeWitness<Self>>;

        fn admit_empty_record() -> crate::error::Result<Self::EmptyRecord> {
            Ok(())
        }

        fn mode_witness(
            carried: &Self::FunctionalDependency,
        ) -> Option<&super::expressions::functions::ModeWitness<Self>> {
            Some(carried)
        }

        fn into_mode_witness(
            carried: Self::FunctionalDependency,
        ) -> Option<super::expressions::functions::ModeWitness<Self>> {
            Some(*carried)
        }

        fn admit_mode_witness(
            witness: Option<super::expressions::functions::ModeWitness<Self>>,
        ) -> crate::error::Result<Self::FunctionalDependency> {
            witness.map(Box::new).ok_or_else(|| {
                crate::error::DelightQLError::transformation_error(
                    "a field select reached a bound phase with no declaration to pick from: \
                     the declared mode is what licenses the pick, and carrying the pick \
                     without it would leave lowering nothing to select by",
                    "functional_dependency",
                )
            })
        }

        fn sigma_body(
            carried: &Self::SigmaBody,
        ) -> &super::expressions::truth::TruthExpression<Self> {
            carried
        }

        fn into_sigma_body(
            carried: Self::SigmaBody,
        ) -> super::expressions::truth::TruthExpression<Self> {
            *carried
        }

        fn admit_sigma_body(
            body: super::expressions::truth::TruthExpression<Self>,
        ) -> crate::error::Result<Self::SigmaBody> {
            Ok(Box::new(body))
        }

        fn into_constrained_column(carried: Self::ConstrainedColumn) -> Option<crate::names::ColId> {
            Some(carried)
        }

        fn admit_constrained_column(
            column: Option<crate::names::ColId>,
        ) -> crate::error::Result<Self::ConstrainedColumn> {
            column.ok_or_else(|| {
                crate::error::DelightQLError::transformation_error(
                    "a crossed slot reached a bound phase with no column to unify with: the \
                     unification is built from that column, and carrying the crossing without \
                     one would drop the constraint the author wrote",
                    "constrained_column",
                )
            })
        }

        fn into_probe_addressing(
            _: Self::ProbeAddressing,
        ) -> Option<super::expressions::truth::ProbeAddressing> {
            None
        }

        fn admit_probe_addressing(
            addressing: Option<super::expressions::truth::ProbeAddressing>,
        ) -> crate::error::Result<Self::ProbeAddressing> {
            match addressing {
                None => Ok(()),
                Some(_) => Err(crate::error::DelightQLError::transformation_error(
                    "a truth probe's authored addressing reached a phase that has already \
                     spent it: the probe's relation is resolved and its correlation \
                     synthesized, and a second carrier beside those is free to disagree",
                    "probe_addressing",
                )),
            }
        }

        fn into_mention(_: Self::Mention) -> Option<super::expressions::GroundMention> {
            None
        }

        fn admit_mention(
            mention: Option<super::expressions::GroundMention>,
        ) -> crate::error::Result<Self::Mention> {
            match mention {
                None => Ok(()),
                Some(_) => Err(crate::error::DelightQLError::transformation_error(
                    "a ground read's authored mention reached a phase that has already \
                     spent it: the read's scope answers for the relation, and a second \
                     carrier beside that scope is free to disagree with it",
                    "mention",
                )),
            }
        }

        fn admit_enumeration() -> crate::error::Result<Self::Enumeration> {
            Err(crate::error::DelightQLError::transformation_error(
                "a spread is expanded where its container resolves it, and this \
                 fold walked past a container still holding one",
                "enumeration",
            ))
        }

        fn no_cfe_bindings() -> Self::CfeBindings {}

        fn cfe_bindings(carried: &Self::CfeBindings) -> &[super::queries::CfeDefinition] {
            let () = carried;
            &[]
        }

        fn into_cfe_bindings(_: Self::CfeBindings) -> Vec<super::queries::CfeDefinition> {
            Vec::new()
        }

        fn admit_cfe_bindings(
            cfes: Vec<super::queries::CfeDefinition>,
        ) -> crate::error::Result<Self::CfeBindings> {
            if cfes.is_empty() {
                Ok(())
            } else {
                Err(crate::error::DelightQLError::transformation_error(
                    "a query-scoped definition reached a phase that has already spent it: \
                     the resolver spends each definition at its call sites, and a fold \
                     still carrying one walked past the place that spends it",
                    "cfe_bindings",
                ))
            }
        }

        fn no_stage_name() -> Self::StageName {}

        fn into_stage_name(_: Self::StageName) -> Option<delightql_types::SqlIdentifier> {
            None
        }

        fn admit_stage_name(
            name: Option<delightql_types::SqlIdentifier>,
        ) -> crate::error::Result<Self::StageName> {
            match name {
                None => Ok(()),
                Some(name) => Err(crate::error::DelightQLError::transformation_error(
                    format!(
                        "a pipe's authored name '{name}' reached a phase that has already \
                         spent it: the stage's scope answers to it, and a second carrier \
                         beside that scope is free to disagree with it"
                    ),
                    "stage_name",
                )),
            }
        }

        fn er_join(carried: &Self::ErJoin) -> &super::ErJoinStep<Self> {
            match *carried {}
        }

        fn into_er_join(carried: Self::ErJoin) -> super::ErJoinStep<Self> {
            match carried {}
        }

        fn admit_er_join(_: super::ErJoinStep<Self>) -> crate::error::Result<Self::ErJoin> {
            Err(crate::error::DelightQLError::transformation_error(
                "an ER edge is expanded where the relation is resolved, and this \
                 fold walked past a chain still standing on one",
                "er_join",
            ))
        }

        // A resolved column reference read as a slot is a TERM. Whether the
        // slot it came from bound a name is not a property of the occurrence
        // — the site that resolved the pattern knows, and builds `Bind`
        // itself. Reading it back off the column would be a guess.
        fn classify_column(column: Self::Col) -> super::Slot<Self> {
            super::Slot::Reuse(super::expressions::NamedReference(column))
        }

        fn classify_open_slot(leaf: Self::OpenLeaf) -> super::Slot<Self> {
            match leaf {}
        }

        fn cover_callable(callable: &Self::CoverCallable) -> Option<&super::Callable<Self>> {
            let () = callable;
            None
        }

        fn anon_slot_term() -> Option<super::DomainExpression<Self>> {
            None
        }

        fn binder_column(binder: Self::Binder) -> Self::Col {
            super::columns::ColumnOccurrence {
                column: binder,
                explicit_qualifier: false,
            }
        }

        fn bound_binder(binder: &Self::Binder) -> Option<crate::names::ColId> {
            Some(*binder)
        }
    };
}

impl Phase for Unresolved {
    type ColumnOrdinal = super::ColumnOrdinal;
    type ColumnRange = super::ColumnRange;
    type Enumeration = ();
    type EmptyRecord = crate::pipeline::asts::vocabulary::Never;
    // A consulted view is the RESOLVER's own product — a name it looked up
    // and expanded. The authored tree never holds one, so the slot is
    // uninhabited and the two consulted-view forms cannot be built here.
    // `()` said "nothing yet", which is a different claim: it left an
    // authored consulted view constructible by anyone who wanted one.
    type Consulted = crate::pipeline::asts::vocabulary::Never;
    // No decision has been taken about recursion here — not a default one,
    // none. The resolver takes it where a body's reference is known to be
    // its own binding's scope.
    type Recursion = ();
    // The subject as constructed: an authored spelling with its effect
    // declaration, or a compiler-built carrier scope. An authored variant
    // has no room for a bound scope, so an unresolved user binding cannot
    // claim resolution it never had.
    type CteSubject = super::queries::CteSubject;
    // The head and the provenance judgments still await resolution, which
    // spends them whole.
    type CteAuthority = super::queries::CteAuthority;
    // Which output an expression publishes is the resolver's answer, and a
    // relation that has not been resolved publishes nothing anyone can name.
    type Output = ();
    // Nothing has been resolved, so there is no occurrence a scalarized
    // relation could publish.
    type ScalarOutput = ();
    // Likewise the columns a destructuring pattern produces: the pattern is
    // written here, its columns are minted where it binds.
    type Destructure = ();
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
    type Entity = crate::pipeline::asts::vocabulary::Ref;
    type Col = super::columns::AuthoredColumn;
    type Binder = super::columns::WrittenBinder;
    type RenameTarget = super::specs::NameTarget;
    type OpenLeaf = super::expressions::DomainHole;
    type CoverCallable = super::expressions::Callable<Self>;
    type Placeholder = super::columns::AtSign;
    type ContextMarker = super::columns::ContextMarker;
    type HoLanding = Option<usize>;
    type ErJoin = super::ErJoinStep<Self>;
    type StageName = Option<delightql_types::SqlIdentifier>;
    type Mention = super::expressions::GroundMention;
    type ProbeAddressing = super::expressions::truth::ProbeAddressing;
    // Which column a crossed slot constrains is the slot's POSITION in the
    // pattern, and no heading has been read. There is nothing here to hold
    // — not an absent column, none.
    type ConstrainedColumn = ();
    // A DQL truth rule's body is fetched where its NAME is resolved, so an
    // authored sigma application observes a call and nothing else.
    type SigmaBody = crate::pipeline::asts::vocabulary::Never;
    // The authored definitions, in authored order, still unspent.
    type CfeBindings = Vec<super::queries::CfeDefinition>;
    // The declaration a pick names lives in the catalog, and the authored
    // phase has not read it. Nothing here — not an absent witness, none.
    type FunctionalDependency = ();

    fn admit_empty_record() -> crate::error::Result<Self::EmptyRecord> {
        Err(crate::error::DelightQLError::transformation_error(
            "a generated empty record cannot stand in the authored phase",
            "empty_record",
        ))
    }

    fn sigma_body(carried: &Self::SigmaBody) -> &super::expressions::truth::TruthExpression<Self> {
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
        Err(crate::error::DelightQLError::transformation_error(
            "a sigma application reached the authored phase observing a BODY: an authored \
             application names a call, and a rule's body is fetched where that name is \
             resolved",
            "sigma_body",
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
            Some(_) => Err(crate::error::DelightQLError::transformation_error(
                "a field select reached the authored phase carrying a declaration: the \
                 mode lives in the catalog, and an authored pick names an output without \
                 having read one",
                "functional_dependency",
            )),
        }
    }

    fn into_constrained_column(_: Self::ConstrainedColumn) -> Option<crate::names::ColId> {
        None
    }

    fn admit_constrained_column(
        column: Option<crate::names::ColId>,
    ) -> crate::error::Result<Self::ConstrainedColumn> {
        match column {
            None => Ok(()),
            Some(_) => Err(crate::error::DelightQLError::transformation_error(
                "a crossed slot's resolved column reached the authored phase: the slot's \
                 position is what names the column, and a second carrier beside that \
                 position is free to disagree with it",
                "constrained_column",
            )),
        }
    }

    fn into_probe_addressing(
        carried: Self::ProbeAddressing,
    ) -> Option<super::expressions::truth::ProbeAddressing> {
        Some(carried)
    }

    fn admit_probe_addressing(
        addressing: Option<super::expressions::truth::ProbeAddressing>,
    ) -> crate::error::Result<Self::ProbeAddressing> {
        addressing.ok_or_else(|| {
            crate::error::DelightQLError::transformation_error(
                "a truth probe reached the authored phase with no addressing: there is no \
                 probe of a relation nobody addressed",
                "probe_addressing",
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
            crate::error::DelightQLError::transformation_error(
                "a ground read reached the authored phase with no mention: there is no \
                 read of a relation nobody addressed",
                "mention",
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
                super::Slot::Constraint(super::SlotConstraint::Value(Box::new(
                    super::DomainExpression::Application(super::FunctionApplication::Open(hole)),
                )))
            }
        }
    }

    fn anon_slot_term() -> Option<super::DomainExpression<Self>> {
        Some(super::DomainExpression::Application(
            super::FunctionApplication::Open(super::expressions::DomainHole::Disregarded),
        ))
    }

    fn bound_binder(binder: &Self::Binder) -> Option<crate::names::ColId> {
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
            Some(_) => Err(crate::error::DelightQLError::transformation_error(
                "a correlation cannot be written on an authored bag step: what is \
                 written is a predicate standing over the run",
                "bag_op",
            )),
        }
    }

    fn correspondence(carried: &Self::Correspondence) -> &super::Correspondence {
        match *carried {}
    }

    fn into_correspondence(carried: Self::Correspondence) -> super::Correspondence {
        match carried {}
    }

    fn admit_correspondence(
        _: super::Correspondence,
    ) -> crate::error::Result<Self::Correspondence> {
        Err(crate::error::DelightQLError::transformation_error(
            "a correspondence cannot stand on an authored member: it is read off \
             the access that directs it, and that access has not been resolved",
            "member",
        ))
    }
}

impl Phase for Resolved {
    bound_columns!();
    type Consulted = crate::names::ScopeId;
    type Scope = crate::names::ScopeId;
    type Recursion = crate::pipeline::asts::vocabulary::RecursionState;
    type Output = Option<crate::names::ColId>;
    // The sole column the degree judgment answered with, taken once at
    // the value admission. Not optional: a scalarized relation that
    // published none did not cross that boundary.
    type ScalarOutput = crate::names::ColId;
    type Destructure = Vec<super::expressions::pipes::DestructureMapping>;
    type Drill = super::operators::BoundDrill;
    type Corr = Option<super::expressions::chain::BagCorrelation<Self>>;
    type CorrelationArm = crate::names::ScopeId;
    type Correspondence = super::Correspondence;
    type Entity = crate::names::CallableId;

    fn correlation(carried: &Self::Corr) -> Option<&super::BagCorrelation<Self>> {
        carried.as_ref()
    }

    fn into_correlation(carried: Self::Corr) -> Option<super::BagCorrelation<Self>> {
        carried
    }

    fn admit_correlation(
        correlation: Option<super::BagCorrelation<Self>>,
    ) -> crate::error::Result<Self::Corr> {
        Ok(correlation)
    }

    fn correspondence(carried: &Self::Correspondence) -> &super::Correspondence {
        carried
    }

    fn into_correspondence(carried: Self::Correspondence) -> super::Correspondence {
        carried
    }

    fn admit_correspondence(
        correspondence: super::Correspondence,
    ) -> crate::error::Result<Self::Correspondence> {
        Ok(correspondence)
    }
}

impl Phase for Refined {
    bound_columns!();
    type Consulted = crate::names::ScopeId;
    type Scope = crate::names::ScopeId;
    type Recursion = crate::pipeline::asts::vocabulary::RecursionState;
    type Output = Option<crate::names::ColId>;
    // The sole column the degree judgment answered with, taken once at
    // the value admission. Not optional: a scalarized relation that
    // published none did not cross that boundary.
    type ScalarOutput = crate::names::ColId;
    type Destructure = Vec<super::expressions::pipes::DestructureMapping>;
    type Drill = super::operators::BoundDrill;
    type Corr = Option<super::expressions::chain::BagCorrelation<Self>>;
    type CorrelationArm = crate::names::ScopeId;
    type Correspondence = super::Correspondence;
    type Entity = crate::names::CallableId;

    fn correlation(carried: &Self::Corr) -> Option<&super::BagCorrelation<Self>> {
        carried.as_ref()
    }

    fn into_correlation(carried: Self::Corr) -> Option<super::BagCorrelation<Self>> {
        carried
    }

    fn admit_correlation(
        correlation: Option<super::BagCorrelation<Self>>,
    ) -> crate::error::Result<Self::Corr> {
        Ok(correlation)
    }

    fn correspondence(carried: &Self::Correspondence) -> &super::Correspondence {
        carried
    }

    fn into_correspondence(carried: Self::Correspondence) -> super::Correspondence {
        carried
    }

    fn admit_correspondence(
        correspondence: super::Correspondence,
    ) -> crate::error::Result<Self::Correspondence> {
        Ok(correspondence)
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

    /// The types say a resolved ground read holds `()`. They cannot say what
    /// happens when a fold that still HAS a spelling tries to cross into a
    /// phase that has spent one — the door has to refuse rather than drop it,
    /// because dropping it silently would let a walk written past the
    /// resolver look like it had resolved something.
    #[test]
    fn a_phase_that_spent_the_mention_refuses_to_receive_one() {
        assert!(carry_mention::<Unresolved, Resolved>(mention("users")).is_err());
        assert!(carry_mention::<Unresolved, Refined>(mention("users")).is_err());
    }

    /// And the other direction: there is no ground read nobody addressed, so
    /// arriving in the authored phase with nothing to say is a lost mention,
    /// not an anonymous relation.
    #[test]
    fn the_authored_phase_refuses_a_mention_it_was_not_given() {
        assert!(carry_mention::<Resolved, Unresolved>(()).is_err());
        assert!(carry_mention::<Refined, Unresolved>(()).is_err());
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
        assert!(Resolved::admit_mode_witness(None).is_err());
        assert!(Refined::admit_mode_witness(None).is_err());
    }

    /// A query-scoped definition is a binding consumed during resolution.
    /// The types make a resolved query's slot uninhabited by definitions;
    /// the DOOR is what refuses a fold that walked past the resolver still
    /// carrying some — dropping them there would silently unbind every call
    /// site they were written for.
    #[test]
    fn a_phase_that_spent_the_definitions_refuses_to_receive_them() {
        let definition = super::super::queries::CfeDefinition {
            name: delightql_types::SqlIdentifier::new("f"),
            formals: super::super::queries::CfeFormals::from_role_groups(
                [],
                [delightql_types::SqlIdentifier::new("x")],
            ),
            context_mode: super::super::queries::ContextMode::None,
            body: super::super::expressions::OutValue::Domain(
                super::super::DomainExpression::Application(
                    super::super::FunctionApplication::Ground(
                        crate::pipeline::asts::core::LiteralValue::Null,
                    ),
                ),
            ),
            source_namespace: None,
        };
        assert!(carry_cfe_bindings::<Unresolved, Resolved>(vec![definition.clone()]).is_err());
        assert!(carry_cfe_bindings::<Unresolved, Refined>(vec![definition]).is_err());
        // No definitions cross freely, in every direction the pipeline takes.
        assert!(carry_cfe_bindings::<Unresolved, Resolved>(Vec::new()).is_ok());
        assert!(carry_cfe_bindings::<Resolved, Refined>(()).is_ok());
    }

    /// The carries the pipeline actually makes.
    #[test]
    fn the_two_live_carries_pass() {
        assert_eq!(carry_mention::<Resolved, Refined>(()).expect("spent"), ());
        assert_eq!(
            carry_mention::<Unresolved, Unresolved>(mention("users")).expect("authored"),
            mention("users")
        );
    }
}
