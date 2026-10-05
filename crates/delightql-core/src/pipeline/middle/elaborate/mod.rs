// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The elaborator: one statement's unresolved AST, read once, into the core.
//! It keeps a lexical environment (the scopes of the runs being written,
//! the query-local blocks around them, and the formals of the definition
//! instances being elaborated); every fact it establishes is a node a core
//! constructor decided.

mod bag;
mod chain;
mod cover;
mod declaration;
mod delegate;
mod edge;
pub(crate) use declaration::declaration;
mod effect;
pub(crate) use effect::judge_declared_family;
mod env;
mod instances;
pub(crate) use instances::declared_columns;
mod integer;
mod calls;
mod locals;
mod pattern;
mod target;
mod pivot;
mod value;

use crate::pipeline::middle::core::graph::{Builder, Finished, Graph};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId};
use crate::pipeline::middle::core::node::run::{MergeRequest, OpenRun};
use crate::pipeline::middle::core::node::walk::Child;
use crate::pipeline::middle::core::node::Route;
use crate::pipeline::middle::core::refuse::Refusal;
use crate::pipeline::middle::core::switches::Switches;
use crate::pipeline::middle::facade::{Input, MarkedScopeId, Query};
use crate::pipeline::middle::select::{Referent, Referred, Standpoint, World};

/// One chain being written: the run open at the top, and the relation
/// waiting to become its first member.
struct Scope {
    run: OpenRun,
    pending: Option<Term>,
    /// Opened after a set operation: the qualifiers its arms answered to.
    /// A condition that addresses one of them is a set-operation
    /// correlation (set-operations-law: CORRELATION IS PAIR-SCOPED).
    arms: Vec<Name>,
    /// Whether the run holds an edge traversed against its declared
    /// orientation: its column order is not decided, so nothing may read
    /// it (a position, a whole-heading read, a stage publishing the run's
    /// heading).
    reversed: bool,
    /// Whether the scope answers qualified names only: a set region's arms
    /// while it judges which arms a condition reads (a bare name is the
    /// step's result's, never an arm's).
    qualified_only: bool,
}

/// A relation standing where a member will bind it.
struct Term {
    rel: RelId,
    scope: Option<Name>,
    route: Route,
    merge: MergeRequest,
    /// Whether the member's scope name is a live qualifier two members of
    /// one run may not share: an inchoate access introduces no names, and
    /// an unaliased positional access introduces bare binders and no
    /// qualifier.
    names_scope: bool,
    /// Whether the scope name qualifies the member's positions. A pipe
    /// form (a stage, a piped call) is scope-dequalifying: a stage name is
    /// an addressing route beside positions that stay bare binders.
    requalifies: bool,
    /// What bore the relation, as the naming laws read it.
    born: crate::pipeline::middle::core::node::run::Born,
}

/// An object one of the statement's creations makes, as a read after its
/// act sees it.
struct Created {
    name: Name,
    /// The namespace its exact catalog path names.
    namespace: String,
    /// The durable namespace it belongs to or overlays, and its shape: a
    /// session object holds its connection's temp name for that owner.
    owner: String,
    session: Option<crate::pipeline::middle::facade::ObjectShape>,
    /// The creating act's receipt, once it is built.
    receipt: Option<RelId>,
    /// The columns of the creation's source; `None` when a position of it
    /// answers to no name.
    columns: Option<Vec<crate::pipeline::middle::core::node::CatalogColumn>>,
    physical: crate::pipeline::middle::core::node::Physical,
    /// Whether a read of it has been written.
    read: bool,
}

/// What a definition under construction defines: a relation (whose
/// self-reference reads its fixpoint's frontier), a value, or a truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    Relation,
    Value,
    Truth,
}

/// One definition under construction.
struct Building {
    definition: String,
    display: String,
    /// Declared in a query's block rather than a catalog namespace: only
    /// the refusals' wording differs.
    local: bool,
    form: Form,
    actuals: Vec<crate::pipeline::middle::core::instance::Actual>,
    /// Whether the actuals are stand-ins where the definition is declared,
    /// for what no caller supplies (A BODY IS JUDGED WHERE IT IS DECLARED).
    stands_in: bool,
    frontier: Option<BinderId>,
}

/// What `@`, the column self (ddl-grammar FN.2), stands for in the text
/// being elaborated: nothing outside a companion cell, the column a cell is
/// written for, or no column in a table-level cell.
#[derive(Clone, Copy)]
enum ColumnSelf {
    Outside,
    Column(ExprId),
    Table,
}

/// The formals of one definition instance being elaborated: the clause
/// reading's own marked scope, the scalar actuals by position, the relation
/// actuals by formal name, and a value or truth definition's formals by
/// bare name.
#[derive(Clone, Default)]
struct Frame {
    marked: Option<MarkedScopeId>,
    values: Vec<Option<ExprId>>,
    relations: Vec<(Name, RelId)>,
    /// A value or truth definition's formals, reached by bare name.
    named: Vec<(Name, ExprId)>,
    /// Rule formals: the configured value each is bound to.
    rules: Vec<(Name, crate::pipeline::middle::core::instance::RuleValue)>,
    /// A value definition's callable formals: the callable each designates.
    callables: Vec<(Name, calls::Callable)>,
    /// A context function's capture: the caller's environment its body's
    /// free names read (an index of `Elaborator::suspended`).
    context: Option<usize>,
    /// Whether the frame is a context function's: a `..` call anywhere in
    /// its body refuses.
    captures: bool,
}

/// The environment a definition's body suspended where it was entered: what
/// a callable written there, or a capture of its row, reads.
struct Suspended {
    scopes: Vec<Scope>,
    interiors: usize,
    stages: Vec<usize>,
    /// Each block's visibility and horizon.
    blocks: Vec<(bool, LexicalHorizonState)>,
    /// The frames the body hides, after the first `frames_kept`.
    frames_kept: usize,
    hidden_frames: Vec<Frame>,
    column_self: ColumnSelf,
    flowing: Option<ExprId>,
    at: Standpoint,
}

type LexicalHorizonState = crate::pipeline::middle::facade::LexicalHorizon;

pub(crate) struct Elaborator<'e, 's> {
    b: Builder,
    input: &'e Input<'s>,
    switches: Switches,
    /// The catalog state the statement reads, where every mention is judged.
    world: World<'s>,
    scopes: Vec<Scope>,
    blocks: Vec<locals::Block>,
    frames: Vec<Frame>,
    /// Where the text being elaborated stands.
    at: Standpoint,
    /// The interiors open in the current definition environment. A call
    /// whose grade contradicts its position inside one is judged where the
    /// interior's member is admitted, which knows whether it stands over a
    /// dependent population.
    interiors: usize,
    /// The scopes whose runs a pipe stage or an ordering being elaborated
    /// reads, innermost last: each stage's input is one formed row.
    stages: Vec<usize>,
    /// The definitions being elaborated, innermost last: the definition, its
    /// actuals, and its frontier (`None` while the anchor is elaborated).
    building: Vec<Building>,
    next_block: usize,
    /// The body standpoints of catalog definitions, by index.
    sites: Vec<Standpoint>,
    /// The definitions configured rule values designate, by key.
    designated: Vec<instances::Definition>,
    /// The instance frames each closed rule value captured where it was
    /// designated (`RuleValue::closure`).
    closures: Vec<Vec<Frame>>,
    /// The objects the statement's creations make, in the order they are
    /// written: a later read of one reads the object, never the catalog the
    /// statement was compiled against.
    created: Vec<Created>,
    /// How many query-local relation families are being elaborated where
    /// their block opens while the block's effect bindings, elaborated at
    /// each demand, are still to come: a name such a body cannot select may
    /// be one the statement's acts create.
    before_acts: usize,
    /// The relations built for query-local relation families where their
    /// blocks open (indexes into the builder's relations): one body read
    /// wherever the family is demanded, before or after the statement's
    /// acts.
    block_bodies: Vec<std::ops::Range<usize>>,
    /// How many definition bodies the elaboration stands inside: a session
    /// directive is legal only in the statement's own text.
    nested: usize,
    /// Whether the statement's whole body is a run, the only place an
    /// execution directive stands at the prompt.
    whole_run: bool,
    column_self: ColumnSelf,
    /// What a callable's composition input `@` stands for while the
    /// callable is applied: a covered cell, or a value definition's argument.
    flowing: Option<ExprId>,
    /// The environments definition bodies suspended, innermost last.
    suspended: Vec<Suspended>,
    /// The danger gates the statement acknowledges.
    gates: Gates,
    /// Whether a step's pairing has spent the `min_multiplicity` gate.
    gate_spent: bool,
    /// While one pivot column's value is elaborated: the condition admitting
    /// that column's rows, which each of its reductions reads under.
    column: Option<crate::pipeline::middle::core::ids::TruthId>,
    /// While an edge's conditions are elaborated: its two endpoints, each
    /// under the name its body reads it by; nothing else is in scope.
    edge_scope: Option<[(Name, BinderId); 2]>,
    /// The anonymous tables written as a call's relation arguments: a
    /// literal the text states before any data, which may witness a pivot
    /// through the formal it binds.
    call_literals: Vec<RelId>,
    /// The passengers a column-bound scalar actual rides (A SCALAR ACTUAL
    /// KEEPS ITS OCCURRENCE): read only where the relation carrying them is.
    column_bound: std::collections::BTreeSet<crate::pipeline::middle::core::ids::PassengerId>,
}

/// The danger gates a statement acknowledges, as the elaborator reads them.
/// A statement whose relation feeds a served call is compiled in parts: the
/// fed relation (`fed`) leaves the judgment of an unspent gate to the
/// statement, which counts a gate a fed relation spent (`spent_apart`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Gates {
    pub(crate) min_multiplicity: bool,
    pub(crate) fed: bool,
    pub(crate) spent_apart: bool,
}

/// Whether a frozen graph's steps spent the `min_multiplicity` gate: a
/// correlated union step pairing by minimum multiplicity.
pub(crate) fn spends_gate(graph: &Graph) -> bool {
    use crate::pipeline::middle::core::graph::Arena;
    use crate::pipeline::middle::core::node::RelKind;
    let reach = crate::pipeline::middle::core::node::walk::reachable(graph, &graph.statement().roots());
    reach.rels.iter().any(|r| {
        matches!(
            graph.rel(*r).kind(),
            RelKind::SetOp { correlation: Some(_), .. }
        )
    })
}

/// Elaborate one statement standing at `at` in `world` into a frozen graph,
/// under the unruled switches the statement is compiled with.
pub(crate) fn statement<'s>(
    input: &Input<'s>,
    query: &Query,
    world: World<'s>,
    at: Standpoint,
    switches: Switches,
    gates: Gates,
) -> Result<Graph, Refusal> {
    let mut e = Elaborator::new(input, world, at, switches);
    e.gates = gates;
    e.whole_run = crate::pipeline::middle::facade::whole_run(query);
    e.declared_effect_rules(query)?;
    e.open_block(query)?;
    let body = e.chain(&query.body)?;
    e.blocks.pop();
    // A call whose grade contradicts its position inside an interior is
    // judged where the interior's member is admitted; one standing in an
    // interior that no member binds (an existence's, a scalar's) is judged
    // here, before the graph is finished.
    if let Some((callee, contradiction)) =
        crate::pipeline::middle::core::node::run::contradicted_call(&e.b, &[Child::Rel(body)])
    {
        return Err(crate::pipeline::middle::core::decide::admission::grade_refusal(
            &callee,
            contradiction,
        ));
    }
    // Every marked read the statement evaluates is the occurrence one
    // mutation reaches.
    let consumed = crate::pipeline::middle::core::decide::effect::consumed_locators(&e.b, body);
    let born = crate::pipeline::middle::core::decide::effect::born_locators(&e.b, body);
    if born.iter().any(|p| !consumed.contains(p)) {
        return Err(crate::pipeline::middle::core::refuse::marker_forbidden("this statement"));
    }
    // An acknowledged gate no correlated union spent is a mistake: the gate
    // changes nothing here.
    if e.gates.min_multiplicity && !e.gates.fed && !e.gates.spent_apart && !e.gate_spent {
        return Err(crate::pipeline::middle::core::refuse::min_multiplicity_unspent());
    }
    e.b.finish(Finished::Relation(body), switches)
}

/// THE PROJECTION LAW where a consultation declares relational families:
/// each family `names` holds, as the namespace `at` stands in selects it,
/// its heads judged against its bodies, and a family holding facts its
/// clauses' agreement (`Elaborator::declared_heads`). A name that selects
/// no relational family of this load is judged at its uses.
pub(crate) fn declared_heads<'s>(
    input: &Input<'s>,
    world: World<'s>,
    at: Standpoint,
    names: &[Name],
) -> Result<(), Refusal> {
    let mut e = Elaborator::new(input, world, at, Switches::default());
    for name in names {
        let Ok(Some(Referent::Family(family))) = e.refer(name, None) else {
            continue;
        };
        let Ok(definition) = e.catalog_definition(
            &family,
            &[
                crate::pipeline::middle::facade::DefKind::View,
                crate::pipeline::middle::facade::DefKind::HoView,
                crate::pipeline::middle::facade::DefKind::Fact,
            ],
        ) else {
            continue;
        };
        e.declared_heads(definition)?;
    }
    Ok(())
}

impl<'e, 's> Elaborator<'e, 's> {
    fn new(input: &'e Input<'s>, world: World<'s>, at: Standpoint, switches: Switches) -> Self {
        Elaborator {
            b: Builder::new(),
            input,
            switches,
            world,
            scopes: Vec::new(),
            blocks: Vec::new(),
            frames: Vec::new(),
            at,
            interiors: 0,
            stages: Vec::new(),
            building: Vec::new(),
            next_block: 0,
            sites: Vec::new(),
            designated: Vec::new(),
            closures: Vec::new(),
            created: Vec::new(),
            before_acts: 0,
            block_bodies: Vec::new(),
            nested: 0,
            whole_run: false,
            column_self: ColumnSelf::Outside,
            flowing: None,
            suspended: Vec::new(),
            gates: Gates::default(),
            gate_spent: false,
            column: None,
            edge_scope: None,
            call_literals: Vec::new(),
            column_bound: std::collections::BTreeSet::new(),
        }
    }

    /// Whether the `min_multiplicity` gate stands where the text being
    /// elaborated stands: closed when the statement does not acknowledge it;
    /// in a consulted body (where the statement's gate reaching it is not
    /// ruled); otherwise open (a query-scoped body is the query's own text).
    fn gate(&self) -> crate::pipeline::middle::core::decide::setop::Gate {
        use crate::pipeline::middle::core::decide::setop::Gate;
        match (self.gates.min_multiplicity, self.building.iter().any(|b| !b.local)) {
            (false, _) => Gate::Closed,
            (true, true) => Gate::Consulted,
            (true, false) => Gate::Open,
        }
    }
}

impl Elaborator<'_, '_> {
    /// A query: its block of query-local definitions, then its body.
    fn query(&mut self, query: &Query) -> Result<RelId, Refusal> {
        self.open_block(query)?;
        let body = self.chain(&query.body);
        self.blocks.pop();
        body
    }

    /// Run `f` in a declaration environment: no scope open, no interior
    /// open, only the first `blocks` query-local blocks and the first
    /// `frames` instance frames visible. A definition's head and body
    /// resolve where the definition was declared (static scope), never in
    /// the caller's open runs, nested blocks or formals. The blocks stay
    /// open, out of reach, so a query-local definition spent inside a body
    /// declared elsewhere (a rule value crossing into a consulted consumer)
    /// enters its own declaration environment again.
    fn apart<T>(
        &mut self,
        blocks: usize,
        frames: usize,
        f: impl FnOnce(&mut Self) -> Result<T, Refusal>,
    ) -> Result<T, Refusal> {
        let kept = frames.min(self.frames.len());
        let suspended = Suspended {
            scopes: std::mem::take(&mut self.scopes),
            interiors: std::mem::take(&mut self.interiors),
            stages: std::mem::take(&mut self.stages),
            blocks: self.blocks.iter().map(|b| (b.hidden, b.horizon)).collect(),
            frames_kept: kept,
            hidden_frames: self.frames.split_off(kept),
            column_self: std::mem::replace(&mut self.column_self, ColumnSelf::Outside),
            flowing: self.flowing.take(),
            at: self.at.clone(),
        };
        for (i, block) in self.blocks.iter_mut().enumerate() {
            block.hidden = i >= blocks;
        }
        self.suspended.push(suspended);
        self.nested += 1;
        let edge_scope = self.edge_scope.take();
        let out = f(self);
        self.edge_scope = edge_scope;
        self.nested -= 1;
        let suspended = self.suspended.pop().expect("the environment this body suspended");
        self.flowing = suspended.flowing;
        self.column_self = suspended.column_self;
        self.frames.extend(suspended.hidden_frames);
        for (block, (hidden, _)) in self.blocks.iter_mut().zip(suspended.blocks) {
            block.hidden = hidden;
        }
        self.interiors = suspended.interiors;
        self.stages = suspended.stages;
        self.scopes = suspended.scopes;
        out
    }
}

impl Elaborator<'_, '_> {
    /// What `name`, under its written qualifier, refers to where the text
    /// being elaborated stands: the one judgment of the mention.
    fn refer(
        &self,
        name: &crate::pipeline::middle::facade::SqlIdentifier,
        qualifier: Option<&crate::pipeline::middle::facade::Qualifier>,
    ) -> Result<Option<Referent>, Refusal> {
        self.world.refer(&self.at, name, qualifier)
    }

    /// [`Self::refer`], with the road of the law that decided it.
    fn judge(
        &self,
        name: &crate::pipeline::middle::facade::SqlIdentifier,
        qualifier: Option<&crate::pipeline::middle::facade::Qualifier>,
    ) -> Result<Referred, Refusal> {
        self.world.judge(&self.at, name, qualifier)
    }

    /// Where a served relation's rows are read, as the catalog places them.
    fn physical(
        &self,
        served: &crate::pipeline::middle::select::Served,
    ) -> Result<crate::pipeline::middle::core::node::Physical, Refusal> {
        let (connection, schema) = self.input.physical(served)?;
        Ok(crate::pipeline::middle::core::node::Physical { connection, schema })
    }

    /// Whether a served relation is typed: a stored table whose storage its
    /// serving backend answers guarantees its declared types, in the
    /// target's own dialect.
    fn typed(
        &self,
        served: &crate::pipeline::middle::select::Served,
        physical: &crate::pipeline::middle::core::node::Physical,
    ) -> bool {
        effect::stored_table(served.kind())
            && self.input.declared_types_guaranteed(served, physical.connection, physical.schema.as_deref())
    }
}
