// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The core's nodes. Every fact a node carries is a private field written
//! by the node's constructor (the `impl Builder` blocks in the child
//! modules) and read through a getter; nothing sets a field after birth.

pub(crate) mod declaration;
pub(crate) mod effect;
pub(crate) mod expr;
pub(crate) mod rel;
pub(crate) mod provenance;
pub(crate) mod run;
pub(crate) mod truth;
pub(crate) mod walk;

use super::heading::{Heading, Interior, Name};
use super::ids::{BinderId, ExprId, InstanceId, MergeId, PassengerId, RelId, TruthId};
use crate::pipeline::middle::facade::{BinOp, CmpOp, LiteralValue};
use std::collections::BTreeSet;

/// Free binders of a node: the enclosing rows it reads (W5 #3).
pub(crate) type BinderSet = BTreeSet<BinderId>;

/// A statement's effect class (W5 #14): one returning a relation that holds
/// an act is effectful.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EffectClass {
    Pure,
    Effect,
}

// ---------------------------------------------------------------------------
// Relations
// ---------------------------------------------------------------------------

pub(crate) struct RelNode {
    kind: RelKind,
    heading: Heading,
    fv: BinderSet,
}

impl RelNode {
    pub(crate) fn kind(&self) -> &RelKind {
        &self.kind
    }
    pub(crate) fn heading(&self) -> &Heading {
        &self.heading
    }
    pub(crate) fn fv(&self) -> &BinderSet {
        &self.fv
    }
}

pub(crate) enum RelKind {
    Read {
        source: ReadSource,
        access: ReadAccess,
    },
    Lit {
        header: Vec<HeaderSlot>,
        rows: Vec<Vec<ExprId>>,
        /// Whether an authored alias names the table: its positions are
        /// then reached through that qualifier.
        aliased: bool,
    },
    /// An act's receipt: at most one row of the descriptor's columns, which
    /// exists exactly when the act's verdict is YES. Its existence is the
    /// act's outcome at run time; each cell is a constant of the act or a
    /// relation the receipt carries as an interior value.
    Receipt {
        act: Box<effect::EffectAct>,
        header: Vec<Name>,
        cells: Vec<effect::ReceiptCell>,
    },
    Run(Run),
    Pipe {
        input: RelId,
        op: PipeOp,
    },
    Order {
        input: RelId,
        keys: Vec<OrderKey>,
        bound: Option<Bound>,
    },
    /// A union step: both arms' rows, aligned as `alignment` says; with a
    /// correlation, each arm keeps only its rows the correlation matches in
    /// the other arm (CORRELATION IS PAIR-SCOPED).
    SetOp {
        left: RelId,
        right: RelId,
        op: SetOpKind,
        alignment: Vec<(Option<usize>, Option<usize>)>,
        correlation: Option<Correlation>,
    },
    /// THE MINUS LAW: the left arm's rows, each kept with its full
    /// multiplicity exactly when no row of the right arm matches it on every
    /// pair (`pairs`: a left position and the right position publishing its
    /// name), each match in the decided `class`, a pair's sides compared as
    /// its decided `membership` says.
    Minus {
        left: RelId,
        right: RelId,
        pairs: Vec<(usize, usize)>,
        membership: Vec<crate::pipeline::middle::core::decide::document::Membership>,
        class: EqClass,
    },
    Meta {
        input: RelId,
    },
    /// THE SIGNED WITNESS (receipt-algebra THE TOTAL LEDGER) over an operand
    /// holding at most one row by construction: its row widened with
    /// `met = 1`, or, when it has none, one proxy row with `met = 0` whose
    /// positions are NULL except those listed in `empty`, relation-valued
    /// positions that hold the empty relation.
    Witnessed {
        input: RelId,
        empty: Vec<usize>,
    },
    /// The rows of a structured value, by one decided expansion (THE
    /// EXPANSION FAMILY): a member standing after the row it reads. The
    /// drill, the narrows and the destructure are each one expansion; a
    /// receipt's payload is released by the drill (RELEASE: the ordinary
    /// interior-relation machinery).
    Unnest {
        value: ExprId,
        expansion: Expansion,
    },
    Family {
        clauses: Vec<FamilyClause>,
        /// The family's name where every clause is a fact: a position no
        /// clause names publishes the canonical fact name under it.
        facts: Option<String>,
    },
    Fix(Fix),
    Apply {
        instance: InstanceId,
    },
}

/// One decided expansion: the levels by which a structured value's rows
/// are reached, and what each level binds. Its heading is its published
/// binds in level order.
#[derive(Clone, Debug)]
pub(crate) struct Expansion {
    pub(crate) levels: Vec<Level>,
    /// What directs the expansion: its form decides what it may expand and
    /// whether it consumes the drilled position (the drill keeps its
    /// context less that position).
    pub(crate) form: rel::ExpansionForm,
}

impl Expansion {
    pub(crate) fn consumes(&self) -> bool {
        self.form == rel::ExpansionForm::Drill
    }
}

/// One level of an expansion.
#[derive(Clone, Debug)]
pub(crate) struct Level {
    /// The node the level expands: the value itself (`None`), or an earlier
    /// level's element, or the node a path reaches from it.
    pub(crate) from: Option<(usize, Option<crate::pipeline::middle::facade::Path>)>,
    pub(crate) reach: Reach,
    pub(crate) binds: Vec<Bind>,
}

/// How a level's rows are reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reach {
    /// One row: the node itself.
    Node,
    /// A known interior's rows: a collection's elements, or a carried
    /// relation's rows.
    Known,
    /// The elements of a sequence; a node that is not an array contributes
    /// zero rows (the sequence guard).
    Sequence,
    /// The members of an object, each key bound; a node that is not an
    /// object contributes zero rows.
    Keys,
}

/// One value a level binds.
#[derive(Clone, Debug)]
pub(crate) struct Bind {
    pub(crate) at: BindAt,
    pub(crate) role: BindRole,
}

/// Where a bound value is read from the level's element.
#[derive(Clone, Debug)]
pub(crate) enum BindAt {
    /// A position of the known interior heading.
    Position(u16),
    /// What a path reaches.
    Path(crate::pipeline::middle::facade::Path),
    /// The element itself.
    Element,
    /// The element's key, for a level over an object's members.
    Key,
}

/// What a bound value is for.
#[derive(Clone, Debug)]
pub(crate) enum BindRole {
    /// A published position of the expansion's heading, under its name; a
    /// drill of every known position publishes the interior's own names.
    Publish(Option<Name>),
    /// A constraint: the value equals the term with the decided equality
    /// class, and is consumed.
    Constrain { value: ExprId, class: EqClass },
}

/// A stored column as the catalog declares it: its name, its declared
/// type's spelling, and the class the target's type vocabulary gives that
/// declaration (numeric, date or boolean; none for any other).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CatalogColumn {
    pub(crate) name: Name,
    pub(crate) declared: Option<String>,
    pub(crate) class: Option<crate::pipeline::middle::facade::DeclaredClass>,
    /// Whether the table's storage computes the column's value (a generated
    /// column), as its serving backend answers: read like any column and
    /// never written. `None`: the backend gives no answer.
    pub(crate) computed: Option<bool>,
    /// Whether the catalog records the column as holding a nested relation
    /// a materialization stored.
    pub(crate) stored: bool,
}

/// What a read reads.
pub(crate) enum ReadSource {
    /// A database table or view the catalog serves: its columns in catalog
    /// order, each with its declared type. A read marked as a mutation's
    /// source carries its row locator, one passenger per part (empty
    /// otherwise).
    Catalog {
        name: Name,
        namespace: String,
        entity: Option<i64>,
        columns: Vec<CatalogColumn>,
        locator: Vec<PassengerId>,
        physical: Physical,
        /// Whether the relation is typed: a stored table whose serving
        /// backend answers that its storage guarantees its declared column
        /// types, a backend serving the target's own dialect. A view, a
        /// virtual table, a relation no introspection answers for and one
        /// served in another dialect are not.
        typed: bool,
    },
    /// A query-local or clause-local binding's body, or a relation formal's
    /// actual.
    Local(RelId),
    /// A relation's rows as the act consuming it staged them, once: a
    /// construction-owned staging, read as a pure relation (THE EFFECT
    /// FENCE: the road from an effect's result into an enclosed position is
    /// materialization). It holds no act and reads no enclosing row.
    Staged(RelId),
    /// The frontier of the fixpoint being defined.
    Frontier(BinderId),
    /// The object one of the statement's own creations makes (its act's
    /// receipt `receipt`), read after that act: its columns are the
    /// creation's source's, its storage where the creation placed it.
    Created {
        receipt: RelId,
        name: Name,
        columns: Vec<CatalogColumn>,
        physical: Physical,
    },
    /// A table function the statement target's engine serves, its heading
    /// the engine's own: the columns its access selected, in written order,
    /// and its arguments, values of the rows it stands beside or constants.
    /// Its rows exist per row of what its arguments read.
    Function {
        name: Name,
        args: Vec<ExprId>,
        columns: Vec<CatalogColumn>,
        physical: Physical,
    },
}

/// Where the catalog says a served relation's rows are read: the
/// connection serving them and the backend schema qualifying the read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Physical {
    pub(crate) connection: i64,
    pub(crate) schema: Option<String>,
}

/// What the parens on a read ask for.
pub(crate) enum ReadAccess {
    All,
    /// Inchoate: the dimensions stay latent.
    Unasked,
    Slots(Vec<Slot>),
}

pub(crate) enum Slot {
    Bind(Name),
    Anon,
    /// A repeated name: an equality with the earlier slot at `first`,
    /// within one row of the read, with its decided equality class.
    Reuse { first: usize, class: EqClass },
    /// A ground term: an equality of the slot's column with the value, with
    /// its decided equality class.
    Constraint { value: ExprId, class: EqClass },
}

/// One position of an anonymous table's header.
pub(crate) enum HeaderSlot {
    Bind(Name),
    Anon,
    /// `_` in a written header: the cell is written and publishes nothing.
    Disregard,
    /// A repeated binder: its cell equals the cell of the earlier slot at
    /// `first` in every row, with its decided equality class; it publishes
    /// nothing.
    Reuse { first: usize, class: EqClass },
    /// A ground term or a qualified reference: the cell equals the value,
    /// with its decided equality class, and publishes nothing.
    Constraint { value: ExprId, class: EqClass },
}

/// One comma run: members, guards and bounds in authored order, each member
/// and guard holding the facts decided when the run closes (W5 #9), and the
/// merged keys the run made.
pub(crate) struct Run {
    quals: Vec<Qual>,
    merges: Vec<(MergeId, MergeRule, Placement)>,
    outputs: Vec<Cell>,
}

impl Run {
    pub(in crate::pipeline::middle::core) fn of(
        quals: Vec<Qual>,
        merges: Vec<(MergeId, MergeRule, Placement)>,
        outputs: Vec<Cell>,
    ) -> Self {
        Run { quals, merges, outputs }
    }
    pub(crate) fn quals(&self) -> &[Qual] {
        &self.quals
    }
    /// The members, in authored order.
    pub(crate) fn members(&self) -> impl Iterator<Item = &Member> {
        self.quals.iter().filter_map(|q| match q {
            Qual::Member(m) => Some(m),
            Qual::Guard(_) | Qual::Bound(_) => None,
        })
    }
    /// The guards, in authored order.
    pub(crate) fn guards(&self) -> impl Iterator<Item = &Guard> {
        self.quals.iter().filter_map(|q| match q {
            Qual::Guard(g) => Some(g),
            Qual::Member(_) | Qual::Bound(_) => None,
        })
    }
    /// Each merged key, in the order the run made them, with the rule its
    /// value follows and the join whose condition it is.
    pub(crate) fn merges(&self) -> &[(MergeId, MergeRule, Placement)] {
        &self.merges
    }
    /// What each output position holds, in heading order.
    pub(crate) fn outputs(&self) -> &[Cell] {
        &self.outputs
    }
}

pub(crate) enum Qual {
    Member(Member),
    Guard(Guard),
    Bound(Bound),
}

/// A condition of a run and where it applies, decided when the run closes.
pub(crate) struct Guard {
    truth: TruthId,
    facts: GuardFacts,
}

/// What a closing run decides for one guard: the join whose condition it is
/// or a filter, and, for a filter, whether it is a boundary: applied where
/// it is written, before every later join, because it does not commute with
/// one (predicate-placement-law: population evaluation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GuardFacts {
    pub(crate) placement: Placement,
    pub(crate) boundary: bool,
}

impl Guard {
    pub(in crate::pipeline::middle::core) fn of(truth: TruthId, facts: GuardFacts) -> Self {
        Guard { truth, facts }
    }
    pub(crate) fn truth(&self) -> TruthId {
        self.truth
    }
    pub(crate) fn placement(&self) -> Placement {
        self.facts.placement
    }
    /// Whether the guard is a boundary of its run's population.
    pub(crate) fn boundary(&self) -> bool {
        self.facts.boundary
    }
    pub(crate) fn facts(&self) -> GuardFacts {
        self.facts
    }
}

/// A comma member: a relation bound to a fresh binder, as written, with the
/// facts its run decided when it closed: its mark, its join role, how it
/// reads the members before it, and the acts its gate opens.
pub(crate) struct Member {
    binder: BinderId,
    rel: RelId,
    route: Route,
    scope: Option<Name>,
    names_scope: bool,
    requalifies: bool,
    publishes: bool,
    mark: Mark,
    role: JoinRole,
    dependence: Option<(Dependence, Placement)>,
    opens: BTreeSet<RelId>,
}

/// What a closing run decides for one member.
pub(crate) struct MemberFacts {
    pub(crate) mark: Mark,
    pub(crate) role: JoinRole,
    pub(crate) dependence: Option<(Dependence, Placement)>,
    pub(crate) opens: BTreeSet<RelId>,
}

/// A member's written parts.
pub(crate) struct WrittenParts {
    pub(crate) binder: BinderId,
    pub(crate) rel: RelId,
    pub(crate) route: Route,
    pub(crate) scope: Option<Name>,
    pub(crate) names_scope: bool,
    pub(crate) requalifies: bool,
    pub(crate) publishes: bool,
}

impl Member {
    pub(in crate::pipeline::middle::core) fn of(written: WrittenParts, facts: MemberFacts) -> Self {
        Member {
            binder: written.binder,
            rel: written.rel,
            route: written.route,
            scope: written.scope,
            names_scope: written.names_scope,
            requalifies: written.requalifies,
            publishes: written.publishes,
            mark: facts.mark,
            role: facts.role,
            dependence: facts.dependence,
            opens: facts.opens,
        }
    }
    /// Whether the member's scope name qualifies its positions.
    pub(crate) fn requalifies(&self) -> bool {
        self.requalifies
    }
    /// Whether the member's positions reach the run's output: a walk's
    /// interior term publishes none of them.
    pub(crate) fn publishes(&self) -> bool {
        self.publishes
    }
    pub(crate) fn binder(&self) -> BinderId {
        self.binder
    }
    pub(crate) fn rel(&self) -> RelId {
        self.rel
    }
    pub(crate) fn route(&self) -> Route {
        self.route
    }
    pub(crate) fn scope(&self) -> Option<&Name> {
        self.scope.as_ref()
    }
    /// Whether the scope name is a live qualifier no other member of the
    /// run may share.
    pub(crate) fn names_scope(&self) -> bool {
        self.names_scope
    }
    /// The member's `?`, as its run decided it.
    pub(crate) fn mark(&self) -> Mark {
        self.mark
    }
    pub(crate) fn role(&self) -> &JoinRole {
        &self.role
    }
    /// How the member's relation reads earlier members of its run, and the
    /// join whose condition that reading is; `None` when it reads none.
    pub(crate) fn dependence(&self) -> Option<(Dependence, Placement)> {
        self.dependence
    }
    /// The acts the member's gate opens: those it holds that no earlier
    /// member holds, each run only when the members before it have a row
    /// (SET-AT-A-TIME). Empty for an ungated member.
    pub(crate) fn opens(&self) -> &BTreeSet<RelId> {
        &self.opens
    }
    /// The enclosing rows the member's relation reads (W5 #21): its free
    /// binders, read from the relation, never stored beside it.
    pub(crate) fn dependent_on<'a>(
        &self,
        arena: &'a impl super::graph::Arena,
    ) -> &'a BinderSet {
        arena.rel(self.rel).fv()
    }
}

/// A member's `?`, as its run decided it: the lead's `?` is written on the
/// member after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mark {
    /// No `?`: part of the required core.
    Unmarked,
    /// Optional: padded where its join matches nothing.
    Optional,
    /// Written optional, but read by an unmarked member whose rows exist per
    /// its rows: it is never padded, so it is required.
    Spent,
}

impl Mark {
    /// Whether the member was written optional (a spent mark included).
    pub(crate) fn written(self) -> bool {
        !matches!(self, Mark::Unmarked)
    }
}

/// How a member was written: its route is read only by the admission
/// fences.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Plain,
    Inline,
    Named,
}

/// A member's join role. A comma run associates left to right; where the
/// written-order fold and the markedness wording agree (two members, or
/// every member marked or none), the role is stored by markedness, and
/// otherwise as the member's join in the fold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JoinRole {
    /// An unmarked member: part of the required core, inner-joined.
    Required,
    /// A marked member, attached by a left join onto the tree once the
    /// members its relations read are present (`onto`, member indexes;
    /// empty: onto the required core as a whole). Its relations are the
    /// conditions, merges and dependences placed on its join. Marked members
    /// that read each other attach in that reference order.
    Attached { onto: Vec<usize> },
    /// Every member is marked: nobody is required, and the run is a full
    /// outer left fold in written order.
    Full,
    /// A member of a run of three or more members, some marked and some
    /// not: the run is a left-deep fold in written order, and the member
    /// joins the members before it by this join.
    Folded(FoldJoin),
}

impl JoinRole {
    /// Whether the member's join keeps the member's rows that match nothing
    /// before it (a full join, or a right join of the fold): no row-wise
    /// filter of the rows before it commutes with such a join.
    pub(crate) fn keeps_member(&self) -> bool {
        matches!(self, JoinRole::Full | JoinRole::Folded(FoldJoin::Full | FoldJoin::Right))
    }
}

/// How a member's relation reads earlier members of its run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dependence {
    /// Its rows exist only per row of the members it reads (a correlated
    /// interior, an application of their values, a drill): it is never a
    /// preserved operand, and the reading is its own join's condition.
    Population,
    /// Only its read's slot constraints read them: a condition of the join,
    /// placed like any other.
    Constraint,
}

/// A member's join onto the fold of the members before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FoldJoin {
    /// The first member: the fold begins with it.
    Start,
    /// The member is unmarked and the fold so far is not the marked lead
    /// alone, or the member is dependent and unmarked.
    Inner,
    /// The member is marked and the fold so far holds an unmarked member,
    /// or the member is dependent: the fold so far is kept.
    Left,
    /// The member after a marked lead, itself unmarked: it is kept, the
    /// lead padded (top-grammar FN.41's right orientation).
    Right,
    /// The member and every member before it are marked: the prefix is a
    /// full outer fold, as an all-marked run is.
    Full,
}

/// Which operand supplies a merged key's value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeRule {
    /// Both operands are required: they agree wherever a row exists.
    Agreed,
    /// The merge's left operand (the right one attached onto it).
    Left,
    /// The merge's right operand (the left one is a marked member attached
    /// onto it).
    Right,
    /// Both operands are optional in a full outer fold: the first present.
    Coalesced,
}

/// Where a guard applies: a filter on the run so far, or the match
/// condition of one member's join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placement {
    Filter,
    On { member: usize },
}

/// A run output position's value: a member's column (a carried passenger
/// included), or a merged key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cell {
    Col(BinderId, u16),
    Merged(MergeId),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Bound {
    pub(crate) count: Option<i64>,
    pub(crate) offset: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
pub(crate) struct OrderKey {
    pub(crate) expr: ExprId,
    pub(crate) direction: Direction,
}

pub(crate) enum PipeOp {
    Project(Vec<Item>),
    Embed(Vec<Item>),
    /// The positions the selectors read are removed.
    ProjectOut(Selection<()>),
    /// Each target's position takes the paired value.
    Cover(Selection<ExprId>),
    Group { keys: Vec<Item>, reductions: Vec<Item> },
    Distinct(Vec<Item>),
    /// Append each passenger as a hidden position: the birth of a
    /// configured value over its construction rows (W5 #13).
    Carry(Vec<PassengerId>),
}

/// References to output positions of one stage input, each with what it
/// pairs: resolved against that input, whose identity the selection keeps,
/// so a stage over another input refuses it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selection<T> {
    input: RelId,
    items: Vec<(Selected, T)>,
}

impl<T> Selection<T> {
    pub(in crate::pipeline::middle::core) fn of(input: RelId, items: Vec<(Selected, T)>) -> Self {
        Selection { input, items }
    }
    /// The input the references were resolved against.
    pub(crate) fn input(&self) -> RelId {
        self.input
    }
    pub(crate) fn items(&self) -> &[(Selected, T)] {
        &self.items
    }
}

/// A reference to one output position of a stage's input, with that
/// position: resolved by the stage's constructor against that input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Selected {
    reference: ExprId,
    position: u16,
}

impl Selected {
    pub(in crate::pipeline::middle::core) fn at(reference: ExprId, position: u16) -> Self {
        Selected { reference, position }
    }
    pub(crate) fn reference(self) -> ExprId {
        self.reference
    }
    pub(crate) fn position(self) -> usize {
        usize::from(self.position)
    }
}

/// One stage item: its value and how the author named it. The name the
/// position publishes is the heading's, formed from this.
#[derive(Clone, Debug)]
pub(crate) struct Item {
    pub(crate) expr: ExprId,
    pub(crate) naming: Naming,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Naming {
    /// `as name`.
    As(Name),
    /// A written reference: the referenced position's name state travels.
    Reference,
    /// One position of an unqualified glob: the position of the stage's
    /// input it covers, carried as that input publishes it.
    Glob,
    /// One position of a qualified glob `q.*`: the member's own position.
    QualifiedGlob,
    /// An unnamed computed value.
    Computed,
    /// A head's unlabeled ground term: it supplies its value and abstains
    /// from naming the position (heads-law: HEAD `as` IS RENAME AND
    /// BAPTISM).
    Abstain,
}

/// A collected record's members; a nested level is one more collect.
#[derive(Clone, Debug)]
pub(crate) enum CollectMember {
    /// One value of the record, named as the author named it.
    Item(Item),
    /// A nested level under one key: a collection of records, or of tuples
    /// whose members are addressed by position.
    Nested(Name, crate::pipeline::middle::core::heading::Layout, Vec<CollectMember>),
    /// A metadata group under one key (THE METADATA TARGET IS A COLLECTOR):
    /// an object keyed by the values of its level's key, each key holding
    /// the rows of its partition as the target collects them.
    Metadata(Name, MetaLevel),
}

/// One level of a metadata group: the value whose values key it, and what
/// each key holds.
#[derive(Clone, Debug)]
pub(crate) struct MetaLevel {
    pub(crate) key: ExprId,
    pub(crate) target: MetaTarget,
}

/// What each key of a metadata level holds.
#[derive(Clone, Debug)]
pub(crate) enum MetaTarget {
    /// The rows of the key's partition, collected.
    Collect(crate::pipeline::middle::core::heading::Layout, Vec<CollectMember>),
    /// A further level, keyed within the key's partition.
    Group(Box<MetaLevel>),
}

impl MetaLevel {
    /// The chain's keys, outermost first, and its collected target.
    pub(crate) fn chain(&self) -> (Vec<ExprId>, crate::pipeline::middle::core::heading::Layout, &[CollectMember]) {
        let mut keys = vec![self.key];
        let mut target = &self.target;
        loop {
            match target {
                MetaTarget::Collect(layout, members) => return (keys, *layout, members),
                MetaTarget::Group(inner) => {
                    keys.push(inner.key);
                    target = &inner.target;
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    Positional,
    Corresponding,
    Smart,
}

/// A union step's correlation under the acknowledged `min_multiplicity`
/// gate, decided by its constructor: the matched position pairs (a
/// left-arm position, a right-arm position), each pair's sides as set
/// identity compares them, and the class of the match. Each tuple stands as
/// many times as the fewer of its copies in the two arms, the left arm's
/// copies. Every other correlation filters the arms it names and is no
/// step's (elaborate::bag).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Correlation {
    pairs: Vec<(usize, usize)>,
    membership: Vec<crate::pipeline::middle::core::decide::document::Membership>,
    class: EqClass,
    written: Vec<setop::Atom>,
}

impl Correlation {
    pub(in crate::pipeline::middle::core) fn of(
        pairs: Vec<(usize, usize)>,
        membership: Vec<crate::pipeline::middle::core::decide::document::Membership>,
        class: EqClass,
        written: Vec<setop::Atom>,
    ) -> Self {
        Correlation {
            pairs,
            membership,
            class,
            written,
        }
    }
    pub(crate) fn pairs(&self) -> &[(usize, usize)] {
        &self.pairs
    }
    pub(crate) fn membership(&self) -> &[crate::pipeline::middle::core::decide::document::Membership] {
        &self.membership
    }
    pub(crate) fn class(&self) -> EqClass {
        self.class
    }
    pub(in crate::pipeline::middle::core) fn written(&self) -> &[setop::Atom] {
        &self.written
    }
}

pub(crate) mod setop {
    use super::super::heading::Name;

    /// One side of a correlation atom, as written against its arm.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) enum Side {
        /// `x.col`.
        Name(Name),
        /// `x|i|`, counted from one.
        Ordinal(usize),
        /// `x.*`.
        AllNames,
        /// `x|*|`.
        AllOrdinals,
        /// Anything else: a computed or nested value over the arm.
        Other,
    }

    /// A correlation conjunct as written, oriented to its step: `left`
    /// addresses the step's left arm, `right` its right arm.
    #[derive(Clone, Debug, PartialEq)]
    pub(crate) struct Atom {
        pub(crate) equality: bool,
        pub(crate) left: Side,
        pub(crate) right: Side,
    }
}

pub(crate) struct FamilyClause {
    pub(crate) guard: Option<TruthId>,
    pub(crate) body: RelId,
}

pub(crate) struct Fix {
    frontier: BinderId,
    deduplicating: bool,
    anchors: Vec<RelId>,
    steps: Vec<RelId>,
    cap: Option<DemandCap>,
}

/// THE DEMAND CAP of a fixpoint: each `#<N` a recursive clause writes on
/// its own chain is a total-row cap on the unfold, so the fixpoint holds at
/// most the least of them. A cap's bounds bound nothing else: a target that
/// spells the cap spells none of them where it is written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DemandCap {
    count: i64,
    runs: Vec<RelId>,
}

impl DemandCap {
    /// The most rows the unfold holds.
    pub(crate) fn count(&self) -> i64 {
        self.count
    }
    /// The runs whose bounds are the cap.
    pub(crate) fn runs(&self) -> &[RelId] {
        &self.runs
    }
}

impl Fix {
    /// The fixpoint's demand cap, when its recursive clauses write one.
    pub(crate) fn cap(&self) -> Option<&DemandCap> {
        self.cap.as_ref()
    }
    pub(crate) fn frontier(&self) -> BinderId {
        self.frontier
    }
    pub(crate) fn deduplicating(&self) -> bool {
        self.deduplicating
    }
    pub(crate) fn anchors(&self) -> &[RelId] {
        &self.anchors
    }
    pub(crate) fn steps(&self) -> &[RelId] {
        &self.steps
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

pub(crate) struct ExprNode {
    kind: ExprKind,
    fv: BinderSet,
    occurrences: BTreeSet<Occurrence>,
}

impl ExprNode {
    pub(crate) fn kind(&self) -> &ExprKind {
        &self.kind
    }
    pub(crate) fn fv(&self) -> &BinderSet {
        &self.fv
    }
    /// The distinct row occurrences the value's own references read: a
    /// binder, or a merged key counted once.
    pub(crate) fn occurrences(&self) -> &BTreeSet<Occurrence> {
        &self.occurrences
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Occurrence {
    Binder(BinderId),
    Merge(MergeId),
}

pub(crate) enum ExprKind {
    Col(BinderId, u16),
    Merged(MergeId),
    Const(LiteralValue),
    Call {
        callee: Callee,
        args: Vec<Arg>,
        grade: Grade,
    },
    Window {
        callee: Callee,
        args: Vec<Arg>,
        partition: Vec<ExprId>,
        order: Vec<OrderKey>,
        frame: Option<Frame>,
    },
    Infix(BinOp, ExprId, ExprId),
    Case {
        anchor: Option<ExprId>,
        arms: Vec<(CaseTest, ExprId)>,
        default: Option<ExprId>,
    },
    Crossed(TruthId),
    Scalar {
        rel: RelId,
        cardinality: Cardinality,
    },
    Passenger(PassengerId),
    /// A collection of records or tuples over a reduction's group (a
    /// reduction item); a tuple's members are unnamed.
    Collect {
        layout: super::heading::Layout,
        members: Vec<CollectMember>,
    },
    /// A metadata collector standing as a reduction item (THE METADATA
    /// TARGET IS A COLLECTOR): its group's object keyed by the level's key,
    /// each key holding the rows of its partition as the target collects
    /// them.
    Metadata {
        level: MetaLevel,
    },
    /// One record or tuple made in value position: its members in written
    /// order, a record's each named by its key.
    Construct {
        layout: super::heading::Layout,
        members: Vec<Item>,
    },
    /// A group delegate's payload value: `value` as the one row of the
    /// group its `rank` puts first holds it. It is that row's value — its
    /// structure and its column's affinity are `value`'s — and it stands
    /// for the group.
    Pick { value: ExprId, rank: ExprId },
    /// The part of a value a path addresses.
    Path {
        source: ExprId,
        path: crate::pipeline::middle::facade::Path,
    },
    /// A relation definition's value formal, read in its body where the
    /// definition's read boundary changes what is known of its actual: the
    /// actual's value, with the boundary's judgment of its structure.
    Across(ExprId),
    /// A value definition's argument: the caller's value, evaluated once
    /// where the call stands, at the grade that position gives the call,
    /// before the body runs. Every read of the formal in the body is this
    /// one node. A value evaluated over a population (an aggregate, a window,
    /// a collection) reads the rows the call stands in, `population`.
    /// `structure` is what is known of the value's structure where the call
    /// stands.
    Argument {
        value: ExprId,
        population: Vec<BinderId>,
        structure: Box<super::heading::Interior>,
    },
}

pub(crate) enum CaseTest {
    /// A match arm: the anchor compared with the literal, with the decided
    /// equality class of that comparison.
    Literal { value: LiteralValue, class: EqClass },
    Truth(TruthId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Callee {
    pub(crate) name: String,
}

pub(crate) enum Arg {
    Value { expr: ExprId, distinct: bool },
    Star,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Grade {
    /// Row-wise, by what the language or the target knows of the call, or
    /// by the author's assertion where nothing is known.
    Scalar(Basis),
    Aggregate,
    /// The call's own grade contradicts the grade its position asks for.
    /// A call so graded is refused before the graph is finished: where it
    /// stands (inside an interior or not) decides under which identity.
    Contradicted(super::decide::grade::Contradiction),
}

/// What a row-wise grade rests on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Basis {
    /// The language or the target knows the call.
    Known,
    /// Nothing is known of the call: its row-wise position asserts it.
    Asserted,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Frame {
    pub(crate) rows: bool,
    pub(crate) start: FrameEdge,
    pub(crate) end: FrameEdge,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FrameEdge {
    Unbounded,
    CurrentRow,
    Preceding(i64),
    Following(i64),
}

/// Scalar cardinality (W5 #17).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cardinality {
    /// At most one row by construction.
    Bounded,
    /// Maps directly; engine semantics apply to more than one row.
    Direct,
}

// ---------------------------------------------------------------------------
// Truths
// ---------------------------------------------------------------------------

pub(crate) struct TruthNode {
    kind: TruthKind,
    fv: BinderSet,
    occurrences: BTreeSet<Occurrence>,
}

impl TruthNode {
    pub(crate) fn kind(&self) -> &TruthKind {
        &self.kind
    }
    pub(crate) fn fv(&self) -> &BinderSet {
        &self.fv
    }
    pub(crate) fn occurrences(&self) -> &BTreeSet<Occurrence> {
        &self.occurrences
    }
}

pub(crate) enum TruthKind {
    Cmp {
        op: CmpOp,
        left: ExprId,
        right: ExprId,
        consumer: Consumer,
        /// Whether the comparison reads a row of the relation it stands in,
        /// rather than only the enclosing row: an input fact of the class.
        own_row: bool,
        class: EqClass,
    },
    And(Vec<TruthId>),
    Or(Vec<TruthId>),
    Not(TruthId),
    Exists {
        positive: bool,
        rel: RelId,
    },
    Sigma {
        proof: SigmaProof,
        args: Vec<ExprId>,
        positive: bool,
    },
}

/// What proves a sigma citation, as its selection found it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SigmaProof {
    /// A predicate the engine serves, by its catalog identity.
    Served { namespace: String, name: Name },
    /// An authored truth rule instantiated at the citation: its clauses'
    /// truths over the citation's arguments, as alternatives.
    Rule { definition: String, expansion: TruthId },
}

/// Which equality a comparison means (W5 #10). Both equality classes
/// compare values exactly, never under a collation a column declares
/// (`decide::equality`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EqClass {
    /// Not an equality comparison.
    Ordering,
    /// A per-row verdict: NULL equals NULL.
    NullSafe,
    /// A match between distinct row occurrences: NULL matches nothing.
    Correspondence,
}

/// Where a comparison's truth is consumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Consumer {
    Filter,
    Value,
    /// A set correlation's condition: it filters, and its equality is set
    /// identity (NULL meets NULL).
    Correlation,
}

// ---------------------------------------------------------------------------
// Binders, merges, instances, passengers
// ---------------------------------------------------------------------------

/// The positions a binder's occurrence carries, as its member contributes
/// them to the run, and the relation the member binds (none for a
/// fixpoint's frontier).
pub(crate) struct BinderSite {
    heading: Heading,
    rel: Option<RelId>,
}

impl BinderSite {
    pub(crate) fn heading(&self) -> &Heading {
        &self.heading
    }
    pub(crate) fn rel(&self) -> Option<RelId> {
        self.rel
    }
}

/// One merged key: the left operand's value, the right member's position
/// it unified with, and the structure of the merged value, both operands'
/// facts met (`Interior::meet`), decided when the merge is made.
pub(crate) struct MergeSite {
    left: Cell,
    right: (BinderId, u16),
    name: Name,
    structure: Interior,
}

impl MergeSite {
    pub(crate) fn left(&self) -> Cell {
        self.left
    }
    pub(crate) fn right(&self) -> (BinderId, u16) {
        self.right
    }
    pub(crate) fn name(&self) -> &Name {
        &self.name
    }
    pub(crate) fn structure(&self) -> &Interior {
        &self.structure
    }
}

/// One definition instance: the definition, by the identity of its
/// declaration, each formal's actual (W5 #5), and the body elaborated with
/// them (W5 #16).
pub(crate) struct Instance {
    definition: String,
    formals: Vec<super::instance::Actual>,
    body: RelId,
}

impl Instance {
    pub(in crate::pipeline::middle::core) fn of(
        definition: String,
        formals: Vec<super::instance::Actual>,
        body: RelId,
    ) -> Self {
        Instance {
            definition,
            formals,
            body,
        }
    }
    pub(crate) fn definition(&self) -> &str {
        &self.definition
    }
    pub(crate) fn formals(&self) -> &[super::instance::Actual] {
        &self.formals
    }
    pub(crate) fn body(&self) -> RelId {
        self.body
    }
}

/// A value carried beside the published positions (W5 #13).
pub(crate) enum Passenger {
    /// A configured value's captured term over its construction row.
    Configured {
        value: ExprId,
        construction: BinderId,
    },
    /// The row locator of the marked read that holds it, as the read's
    /// table is reached: the table's row identity, decided once at the
    /// marked read.
    RowLocator(RowLocator),
    /// The node of an extraction a stage publishes: the extracted value
    /// with its kind (record, tuple, scalar or text), carried beside the
    /// published value so a structured consumer after the stage reads the
    /// kind the row observed, never the bytes alone.
    Node { extraction: ExprId },
}

/// How a statement reaches one stored row of a marked read's table
/// (`decide::mutation::row_locator`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RowLocator {
    /// The target's pseudo-column, under the one of its names no stored
    /// column takes.
    Pseudo(Name),
    /// A stored column that is the locator itself, or one part of it (a
    /// key column): its catalog position.
    Column(u16),
}
