// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The exact semantic forms.
//!
//! Every row-producing operation the language has is one member here. The
//! vocabulary is closed and has no generic member: an operation whose row
//! meaning nobody can state is a design question, not an implementation
//! exception, and there is no arm for it to hide in.
//!
//! These are ARGUMENTS, not storage. The stored tree keeps its own
//! syntax-shaped variants; a caller assembles one of these to say what it
//! is asking for, hands it to the authority, and receives the result. No
//! node holds a `RelForm`, so this is not a second tree beside the first.
//!
//! What a spec may carry is the OPERATION. What it may not carry is a
//! choice the operation already determines: an owner, a destination scope,
//! a birth, a republication class, a boundary kind, or an addressing
//! policy. Those are derived here, once, from the form.

use super::carrier::SemanticRelation;
use super::port::PortId;
use crate::names::{EntityId, Spelling};

/// One row-producing semantic operation.
#[derive(Debug)]
pub enum RelForm<'a> {
    /// A relation read from somewhere outside this query: the catalog, a
    /// table-valued function, a fact body, a compiler-supplied source.
    Source(SourceSpec<'a>),
    /// `_(…)`, an anonymous table literal, an argument row — rows written
    /// here rather than read from anywhere.
    Anonymous(AnonymousSpec<'a>),
    /// A relation whose identity exists and whose dimensions are not
    /// enumerable. Not a heading of width zero.
    Opaque,

    /// The relation standing to the left CONTINUES. No occurrence is
    /// created and the interface is the input's, exactly.
    Order(SemanticRelation),
    /// A new occurrence publishing the input's whole heading, one-to-one,
    /// in order.
    Export(ExportSpec),

    /// `(*)`, `(a, b)`, `.(a, b)`, `.*` — the dimensions a read asks for.
    Access(AccessSpec<'a>),
    /// `|> ( … )` — the heading is what the items publish, and nothing
    /// else.
    Project(ProjectSpec<'a>),
    /// The boundary of an ER edge: schema(A) + schema(B), each position
    /// answering to its endpoint.
    ErBoundary(ErBoundarySpec<'a>),
    /// `|> +( … )` — the operand's whole heading, then the added items.
    Embed(ProjectSpec<'a>),
    /// `|> *( … )` — the heading's positions are untouched; the names
    /// change.
    Rename(RenameSpec<'a>),
    /// `*[c as n]` — the heading's names are untouched; the positions
    /// change.
    Reposition(RepositionSpec<'a>),
    /// `|> -( … )` — the heading minus the addressed positions.
    ProjectOut(ProjectOutSpec<'a>),
    /// `$(…)`, `$$(…)`, `+$(…)` — a callable applied per covered cell.
    Cover(CoverSpec<'a>),

    /// `%(keys)` or `%(keys ~> reductions)`.
    Group(GroupSpec<'a>),

    /// The comma's relation case: both operands' headings, concatenated in
    /// operand order.
    Join(JoinSpec<'a>),
    /// `||`, `;`, `|;|` and their correlated forms — one law, one total
    /// arm matrix.
    Set(SetSpec<'a>),
    /// `-` — left export plus exact anti-match.
    Minus(MinusSpec),

    /// `+` / `\+` — existence reified as the one-row, one-column result.
    Witness(WitnessSpec),
    /// `+-` — the input's heading with `met` appended last.
    SignedWitness(SignedWitnessSpec),
    /// One USE of a definition: a fact, a rule, a query-scoped or consulted
    /// callable, a CTE, a view. Fresh relation and fresh ports per use.
    Instantiate(InstanceSpec),
    /// One authored read of compiler-owned plan storage. The plan object is
    /// the template; the authored read is a fresh relation occurrence.
    PlanRead(PlanReadSpec),
    /// `col ~= {…}` — read fields out of a document, or iterate and
    /// explode rows.
    Destructure(DestructureSpec<'a>),
    /// `.col(…)` — explode an interior relation column into rows, carrying
    /// context forward.
    Drill(DrillSpec<'a>),
    /// `|> .nest{…}` — iterate the array a nest carries. Payload only; no
    /// context rides through.
    Narrow(NarrowSpec<'a>),
    /// The interior relation a tree-group column owns.
    Interior(InteriorSpec),
    /// `^` — the relation's schema as data, with the fixed heading.
    Meta(MetaSpec),

    /// An effect plan's scratch object entering the ordinary relation
    /// road: a snapshot, a result, a tee, an insert staging, a barrier.
    Scratch(ScratchSpec<'a>),
}

// ---------------------------------------------------------------- sources

/// Where a source's rows come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceOrigin {
    /// `users(*)` — a catalog entity.
    Catalog { entity: EntityId },
    /// A table-valued function's result.
    TableValued { entity: EntityId },
}

/// One dimension a source publishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSlot {
    /// The catalog ordinal, which is what makes a source's heading
    /// reproducible without re-reading characters.
    pub position: u32,
    /// The name the catalog gives it, if any.
    pub named: Option<Spelling>,
    /// The catalog's type spelling. SQL type syntax, not an identifier, so
    /// it travels as value data and is never interned as a name.
    pub declared_type: Option<String>,
}

#[derive(Debug)]
pub struct SourceSpec<'a> {
    pub origin: SourceOrigin,
    pub slots: &'a [SourceSlot],
    /// The spelling this read answers to from outside. An alias is the one
    /// spelling that replaces it, and an alias is its own form.
    pub answers_to: Option<Spelling>,
}

/// What an anonymous relation's rows are written as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnonymousShape {
    /// `_(1, 2; 3, 4)` — positional rows under a declared or inferred
    /// width.
    Tabular,
    /// A call site's argument row.
    ArgumentRow,
}

/// One dimension an anonymous relation contributes.
///
/// The alternatives are semantic, not a bag of independently selectable
/// flags. In particular, an unnamed declared position is latent, an inferred
/// or literal position is output-only, and a computed header constraint is
/// hygienic; no caller can combine those dispositions with another origin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnonymousSlot {
    /// A header lvar introduces a named position.
    Binder {
        position: u32,
        named: Spelling,
        declared_type: Option<String>,
        shape: crate::names::ValueShape,
    },
    /// A ground header cell is an unnamed output position.
    Literal {
        position: u32,
        declared_type: Option<String>,
        shape: crate::names::ValueShape,
    },
    /// A computed header probes the enclosing row and is not published.
    Constraint {
        position: u32,
        declared_type: Option<String>,
        shape: crate::names::ValueShape,
    },
    /// A headerless grid contributes one unnamed position per inferred
    /// column.
    Inferred {
        position: u32,
        declared_type: Option<String>,
        shape: crate::names::ValueShape,
    },
    /// A caller pattern states a target's otherwise unknown heading.
    /// `None` is latent: counted and emitted, but addressable by no name.
    Declared {
        position: u32,
        named: Option<Spelling>,
    },
}

#[derive(Debug)]
pub struct AnonymousSpec<'a> {
    pub shape: AnonymousShape,
    pub slots: &'a [AnonymousSlot],
    pub answers_to: Option<Spelling>,
}

// ----------------------------------------------------- transparent/export

/// Why a new occurrence publishes its input's whole heading.
///
/// Closed, and each member determines the ownership disposition rather than
/// stating it. A caller names the road; it has no spelling for what the
/// road does to owners.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportWhy {
    /// `… as p` — an authored alias. The alias replaces the input's
    /// answering name.
    Alias { answer: Spelling },
    /// A slot row's own publication: the binders, bare. It answers to the
    /// authored owner when one was written and to NOTHING otherwise — an
    /// unaliased argumentative access activates no name, and the scope it
    /// publishes records none.
    Bound { answer: Option<Spelling> },
    /// `|>` — a pipe stage's republication. It names nothing: the owner
    /// an author writes with `as` is stamped on the produced relation by
    /// the crossing, and the route it opens is the lexical frontier's.
    Stage,
    /// A `WITH` binding.
    Cte { role: CteWhy, label: CteLabelWhy },
    /// One hop of an entity-relationship chain.
    ErHop { hop: u16 },
    /// The SQL-hygiene re-alias of a join operand. An alias by ownership,
    /// answering to nothing.
    EmissionAlias,
}

/// What a `WITH` binding is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CteWhy {
    TreeGroup,
    GroupCarrier,
    Recursive,
    Reachability,
    Materialize,
}

/// How a `WITH` binding is labelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CteLabelWhy {
    /// An authored spelling the binding answers to.
    Answering(Spelling),
    /// Rendered under a prefix; answers to nothing.
    Prefixed(&'static str),
}

#[derive(Debug)]
pub struct ExportSpec {
    pub input: SemanticRelation,
    pub why: ExportWhy,
}

// ---------------------------------------------------- ordered publication

/// What a published position is called.
///
/// The caller supplies what the AUTHOR wrote, or that the author wrote
/// nothing. It does not supply an emitted spelling: nothing is named until
/// baptism, and a name that existed earlier would be addressable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Naming {
    /// `as n` — the author named this position.
    Authored(Spelling),
    /// `users(user_id, _, …)` — a caller's own argumentative binding. It
    /// publishes the written name and answers to nothing else: the name is
    /// the author's for this call, not a name the relation has, so a
    /// qualified reference reaches the source through the carry chain and
    /// never through the binder.
    Bound(Spelling),
    /// The position keeps whatever its source published.
    Inherited,
    /// The author wrote no name and the position has no source name to
    /// keep. Output only.
    Anonymous,
    /// The compiler's own position. Never addressable, pruned from the
    /// visible view.
    Hygienic,
}

/// One position of a projection's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectSlot {
    /// The value at `source` continues into a new position.
    Carried { source: PortId, naming: Naming },
    /// A new value.
    Computed {
        naming: Naming,
        shape: crate::names::ValueShape,
    },
}

/// Which operation a projection-family form is. Closed, and the member
/// determines the output boundary rather than stating an address role: a
/// caller names the operation and has no spelling for addressability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectWhy {
    /// An authored pipe stage. Every PIPE FORM is SCOPE-DEQUALIFYING, so
    /// every position the stage publishes crosses the one boundary act as
    /// a bare live lvar.
    Stage,
    /// A compiler republication of positions an operand already publishes.
    /// Not a pipe: each position keeps the publication it proposed.
    Restate,
}

#[derive(Debug)]
pub struct ProjectSpec<'a> {
    pub input: SemanticRelation,
    pub why: ProjectWhy,
    pub slots: &'a [ProjectSlot],
    /// Input positions the result does NOT publish and a later operation
    /// still reads — a hoisted correlation's carrier. Lowering emits them
    /// as physical support beside the published list; nothing addresses
    /// them by name.
    pub dependencies: &'a [PortId],
}

/// One endpoint position an ER edge boundary exports.
///
/// An edge publishes schema(A) + schema(B) and NOTHING ELSE, and each
/// exported position keeps answering to the endpoint it came from. That
/// answering channel is the pairing key a composed path joins on and the
/// qualifier `A.x` reaches through, which is why the endpoint travels with
/// the position rather than being recovered from a name afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErExport {
    pub source: PortId,
    pub endpoint: crate::names::Sym,
}

#[derive(Debug)]
pub struct ErBoundarySpec<'a> {
    pub input: SemanticRelation,
    pub exports: &'a [ErExport],
}

/// How a read addresses the dimensions it asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessShape {
    /// `(*)` / `.*` — every published dimension, in order.
    Whole,
    /// `(a, b)` — the named dimensions, in the order written.
    Named,
    /// `()` — no dimension. A read with a heading of width zero, not an
    /// opaque one.
    Empty,
}

#[derive(Debug)]
pub struct AccessSpec<'a> {
    pub input: SemanticRelation,
    pub shape: AccessShape,
    pub slots: &'a [ProjectSlot],
    /// Input positions consumed by filters or unification but absent from
    /// the published interface. Lowering may carry them as physical support
    /// slots until the access predicate is spent.
    pub dependencies: &'a [PortId],
}

/// One renaming: the position keeps its place and takes a new name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenameSlot {
    pub source: PortId,
    pub to: Spelling,
}

#[derive(Debug)]
pub struct RenameSpec<'a> {
    pub input: SemanticRelation,
    /// Which operation this rename is: the authored `|> *( … )` stage, or
    /// a compiler restatement of positions an interior already publishes.
    pub why: ProjectWhy,
    pub renames: &'a [RenameSlot],
}

/// One move: the position keeps its name and takes a new place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepositionSlot {
    pub source: PortId,
    pub to: u32,
}

#[derive(Debug)]
pub struct RepositionSpec<'a> {
    pub input: SemanticRelation,
    pub moves: &'a [RepositionSlot],
}

#[derive(Debug)]
pub struct ProjectOutSpec<'a> {
    pub input: SemanticRelation,
    /// The positions the operand publishes that the result does not.
    pub removed: &'a [PortId],
}

/// Which cover was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverKind {
    /// `$(f)(…)` — the covered positions keep their identities and take
    /// the applied value.
    Map,
    /// `$$( … )` — many-to-many redefinition; every item names the slot it
    /// writes.
    Transform,
    /// `+$(f)(…)` — the covered positions stay and the applied values are
    /// added after them.
    EmbedMap,
}

/// One covered cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoverCell {
    pub covered: PortId,
    pub naming: Naming,
    /// Whether this cover puts a DIFFERENT value in the slot.
    ///
    /// A cover that hands the slot back its own column writes nothing — the
    /// same value under the same name — and an update whose only cover is
    /// that one has nothing to change. Whether the two are the same column is
    /// a question about the authored expression, so the form states it; the
    /// fact then travels with the value from the one act that made it.
    pub writes: bool,
}

#[derive(Debug)]
pub struct CoverSpec<'a> {
    pub input: SemanticRelation,
    pub kind: CoverKind,
    pub cells: &'a [CoverCell],
}

// --------------------------------------------------------------- grouping

/// What each equivalence class publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupKind {
    /// `%(keys)` — each class publishes its keys, once.
    Distinct,
    /// `%(keys ~> reductions)` — keys, then one position per reduction.
    /// Zero keys is the singleton reduction.
    Reduce,
}

/// One position a reduction publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReductionSlot {
    /// One reduced value.
    Value { slot: ProjectSlot },
    /// A metadata group: the class's rows become an interior relation.
    Group { naming: Naming },
    /// A delegate's payload position, pulled from a representative row.
    Delegate { slot: ProjectSlot },
    /// One column per value the pivot's authored membership predicate
    /// named, in the order written. The heading is the predicate's, so a
    /// pivot publishes several positions and no single one.
    PivotValue { naming: Naming },
}

#[derive(Debug)]
pub struct GroupSpec<'a> {
    pub input: SemanticRelation,
    pub kind: GroupKind,
    /// The key positions, in authored order. Empty is the singleton class.
    pub keys: &'a [ProjectSlot],
    pub reductions: &'a [ReductionSlot],
}

// ----------------------------------------------------------- multi-input

/// Which rows a join keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

/// A merged position: one output standing for a port of each operand.
///
/// Recorded at the join rather than inferred from a shared name, because a
/// name cannot tell one operand's `id` from the other's and stops working
/// entirely once the name is one the compiler drew.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergedKey {
    pub left: PortId,
    pub right: PortId,
}

impl crate::lispy::ToLispy for MergedKey {
    fn to_lispy(&self) -> String {
        format!("({:?} {:?})", self.left, self.right)
    }
}

#[derive(Debug)]
pub struct JoinSpec<'a> {
    pub left: SemanticRelation,
    pub right: SemanticRelation,
    pub kind: JoinKind,
    /// The positions the two operands share by construction. Empty for an
    /// ordinary condition join.
    pub merged: &'a [MergedKey],
}

/// How a set's arms line up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetAlignment {
    /// `||` — by ordinal, widths must agree.
    Positional,
    /// `;` — by stable published name; a missing name pads.
    Corresponding,
    /// `|;|` — exact stable-name agreement, any order.
    Smart,
}

/// One arm of a set, with the correlation that decided which of its rows
/// contribute.
///
/// Correlated and uncorrelated arms produce the same matrix: correlation
/// decides rows, alignment decides columns, and neither reads the other's
/// answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetArm {
    pub relation: SemanticRelation,
    pub correlated: bool,
}

#[derive(Debug)]
pub struct SetSpec<'a> {
    pub alignment: SetAlignment,
    /// Two or more. A one-armed set is not a set.
    pub arms: &'a [SetArm],
}

#[derive(Debug)]
pub struct MinusSpec {
    pub left: SemanticRelation,
    /// Probed, never published. Its ports reach the anti-match predicate
    /// and nothing else.
    pub right: SemanticRelation,
}

// -------------------------------------------------------------- existence

/// Which answer the witness reifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WitnessPolarity {
    /// `+` — met when the input has rows.
    Positive,
    /// `\+` — met when it does not.
    Negative,
}

#[derive(Debug)]
pub struct WitnessSpec {
    pub input: SemanticRelation,
    pub polarity: WitnessPolarity,
}

#[derive(Debug)]
pub struct SignedWitnessSpec {
    pub input: SemanticRelation,
}

// ------------------------------------------------------------ definitions

/// What kind of definition is being used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefinitionKind {
    /// A stored fact.
    Fact,
    /// A higher-order application's carrier, in the role it plays for the
    /// application: the argument it stands for, the piped source, a scalar
    /// input, or a proffered binding.
    HigherOrder(HoPart),
    /// A `WITH` binding read at a use site.
    Cte,
    /// A consulted view, inlined.
    View,
}

/// What a higher-order carrier stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HoPart {
    Argument,
    PipeSource,
    ScalarInput,
    Proffer,
}

/// A definition's identity, distinct from any use of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DefinitionId(pub(super) u32);

/// One physical storage object several uses may share.
///
/// A shared CTE keeps one storage identity while each read of it is a
/// distinct semantic occurrence. Keeping the two apart is what makes
/// "instantiate" mean a fresh relation without meaning a second CTE.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageId(pub(super) u32);

#[derive(Debug)]
pub struct InstanceSpec {
    pub kind: DefinitionKind,
    /// The definition occurrence whose interface this use instantiates.
    /// Definition and storage identities are allocated from this relation
    /// inside the authority; callers cannot nominate either one.
    pub template: SemanticRelation,
    pub answers_to: Option<Spelling>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanReadKind {
    Scratch,
    HigherOrder,
}

#[derive(Debug)]
pub struct PlanReadSpec {
    pub kind: PlanReadKind,
    pub template: SemanticRelation,
    pub answers_to: Spelling,
}

// ------------------------------------------------------------------ trees

/// How a destructure consumes its source document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestructureMode {
    /// Read fields out of one document; the row count is unchanged.
    Scalar,
    /// Iterate and explode rows.
    Aggregate,
}

#[derive(Debug)]
pub struct DestructureSpec<'a> {
    pub input: SemanticRelation,
    pub mode: DestructureMode,
    /// The columns the expansion ADDS. The operand's heading rides in
    /// front of them.
    pub bound: &'a [ProjectSlot],
}

#[derive(Debug)]
pub struct DrillSpec<'a> {
    pub input: SemanticRelation,
    /// The interior column being exploded.
    pub interior_of: PortId,
    /// The interior positions selected. A glob is spent at binding; what
    /// stands here is what it selected.
    pub selected: &'a [PortId],
    /// How the pattern selected them.
    pub selection: DrillSelection,
}

/// HOW A DRILL'S PATTERN SELECTED the interior positions it publishes.
/// Stated where the pattern is read, because nothing about a selected
/// port says which: a binder GIVES its position a name and publishes it
/// bare, an argumentative binding like any other; a glob takes the
/// interior whole, and what it publishes stays under the nest's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrillSelection {
    /// `.items(a, _, b)` — one authored name per interior position.
    Bound,
    /// `.items(*)` — the interior's own heading, whole.
    Whole,
}

impl crate::lispy::ToLispy for DrillSelection {
    fn to_lispy(&self) -> String {
        match self {
            DrillSelection::Bound => "bound".to_string(),
            DrillSelection::Whole => "whole".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct NarrowSpec<'a> {
    pub input: SemanticRelation,
    pub nest: PortId,
    /// Payload only — no context rides through, which is the whole
    /// difference from a drill.
    pub bound: &'a [ProjectSlot],
}

#[derive(Debug)]
pub struct InteriorSpec {
    /// The column that owns this interior. Atomic with the back-link: a
    /// column owns exactly one interior.
    pub owner: PortId,
    pub body: SemanticRelation,
}

#[derive(Debug)]
pub struct MetaSpec {
    /// The interface being reported. Meta-ize reports the exact owner,
    /// published name, and ordinal of what it receives — never a
    /// re-derived heading.
    pub subject: SemanticRelation,
}

// ---------------------------------------------------------------- effects

/// What an effect-plan scratch object holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScratchWhy {
    Snapshot,
    /// The staged source rows a DML statement reads one row per target row
    /// from. A snapshot by lifetime, its own position by what it stages.
    DmlSource,
    Result,
    Tee,
    Insert,
    Barrier,
}

#[derive(Debug)]
pub struct ScratchSpec<'a> {
    pub why: ScratchWhy,
    /// A plan-lifetime physical base several statements must agree on.
    /// Never an answering name: the plan reaches it by identity.
    pub base: Option<Spelling>,
    /// What the scratch publishes. Private, so the two shapes are the only
    /// two: a caller states one or the other and cannot state both, neither,
    /// or some combination.
    interface: ScratchInterface<'a>,
}

/// WHAT A SCRATCH PUBLISHES, and there is no third way to acquire it.
///
/// A scratch's heading travels INTO its derivation. One that acquired its
/// positions afterwards would record an interface the registry heading then
/// diverged from, and every reader of the record would answer with the
/// heading nobody grew.
#[derive(Debug)]
pub(super) enum ScratchInterface<'a> {
    /// The scratch HOLDS a statement's emitted output list. A
    /// `CREATE TABLE ... AS SELECT` makes its columns from exactly these,
    /// so the scratch republishes them — one position each, carrying the
    /// spelling and the addressing the statement gave them.
    Holds(&'a crate::relation::SemanticRelation),
    /// The scratch STATES its own positions, for a shell the plan writes
    /// into rather than selects into. An empty statement is a scratch that
    /// publishes nothing, which is a fact rather than an omission.
    States(&'a [ScratchSlot]),
}

/// One position a plan-owned shell states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScratchSlot {
    pub position: u32,
    pub named: Spelling,
}

impl<'a> ScratchSpec<'a> {
    /// A scratch that holds what a compiled statement emits.
    pub fn holding(
        why: ScratchWhy,
        base: Option<Spelling>,
        emitted: &'a crate::relation::SemanticRelation,
    ) -> Self {
        ScratchSpec {
            why,
            base,
            interface: ScratchInterface::Holds(emitted),
        }
    }

    /// A scratch that states the positions it publishes.
    pub fn stating(why: ScratchWhy, base: Option<Spelling>, slots: &'a [ScratchSlot]) -> Self {
        ScratchSpec {
            why,
            base,
            interface: ScratchInterface::States(slots),
        }
    }

    pub(super) fn interface(&self) -> &ScratchInterface<'a> {
        &self.interface
    }
}
