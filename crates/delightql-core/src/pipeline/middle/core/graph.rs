// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The graph's arenas. A [`Builder`] is appended to by node constructors and
//! read by the elaborator; [`Builder::finish`] consumes it and answers the
//! frozen [`Graph`], the only thing an analysis can take.

use super::ids::{BinderId, ExprId, InstanceId, MergeId, PassengerId, RelId, TruthId};
use super::node::{
    BinderSite, EffectClass, ExprNode, Instance, MergeSite, Passenger, RelNode, TruthNode,
};

pub(crate) struct Builder {
    rels: Vec<RelNode>,
    exprs: Vec<ExprNode>,
    truths: Vec<TruthNode>,
    binders: Vec<BinderSite>,
    merges: Vec<MergeSite>,
    instances: Vec<Instance>,
    passengers: Vec<Passenger>,
}

/// The completed representation of one statement.
pub(crate) struct Graph {
    rels: Vec<RelNode>,
    exprs: Vec<ExprNode>,
    truths: Vec<TruthNode>,
    binders: Vec<BinderSite>,
    merges: Vec<MergeSite>,
    instances: Vec<Instance>,
    passengers: Vec<Passenger>,
    statement: Statement,
    switches: super::switches::Switches,
    settled: Settled,
}

/// What the core decides of a frozen graph's values once, at the freeze,
/// for every reader after it: each value's structure (or the refusal that
/// judging it answers) and node road, whether a value evaluates a window
/// itself, whether a value or a truth depends on the rows around its own
/// row, and whether each expansion bind offers its node beside its value.
struct Settled {
    structures: Vec<Result<super::heading::Interior, super::refuse::Refusal>>,
    roads: Vec<Option<super::node::rel::NodeRoad>>,
    windows: Vec<bool>,
    sensitive: Vec<bool>,
    truths_sensitive: Vec<bool>,
    offers: std::collections::BTreeMap<RelId, Vec<Vec<bool>>>,
}

impl Settled {
    fn of(b: &Builder) -> Settled {
        use super::node::{expr, rel, RelKind};
        let values = (0..b.exprs.len()).map(ExprId::at);
        let mut offers = std::collections::BTreeMap::new();
        for (i, node) in b.rels.iter().enumerate() {
            if let RelKind::Unnest { value, expansion } = node.kind() {
                let levels = expansion
                    .levels
                    .iter()
                    .map(|level| level.binds.iter().map(|bind| rel::bind_offers_node(b, *value, level, bind)).collect())
                    .collect();
                offers.insert(RelId::at(i), levels);
            }
        }
        Settled {
            structures: values.clone().map(|e| rel::structure_of(b, None, e)).collect(),
            roads: values.clone().map(|e| rel::node_road(b, e)).collect(),
            windows: values.clone().map(|e| expr::evaluates_window(b, e)).collect(),
            sensitive: values.map(|e| expr::population_sensitive(b, e)).collect(),
            truths_sensitive: (0..b.truths.len()).map(|t| expr::truth_population_sensitive(b, TruthId::at(t))).collect(),
            offers,
        }
    }
}

/// What an elaborated statement returns: a relation, or a declared table.
pub(crate) enum Finished {
    Relation(RelId),
    Declaration(super::node::declaration::Declaration),
}

/// What the statement is: a query over one relation, a program, the
/// relation it returns holding the acts the statement performs (each act is
/// held by its receipt), or a declared table, which returns nothing.
pub(crate) enum Statement {
    Query(RelId),
    Program(super::decide::effect::Program),
    Declaration(super::node::declaration::Declaration),
}

impl Statement {
    /// The statement's effect class (W5 #14).
    pub(crate) fn effect(&self) -> EffectClass {
        match self {
            Statement::Query(_) | Statement::Declaration(_) => EffectClass::Pure,
            Statement::Program(_) => EffectClass::Effect,
        }
    }

    /// The nodes the statement holds, from which every other is reached.
    pub(crate) fn roots(&self) -> Vec<super::node::walk::Child> {
        match self {
            Statement::Query(rel) => vec![super::node::walk::Child::Rel(*rel)],
            Statement::Program(program) => vec![super::node::walk::Child::Rel(program.result())],
            Statement::Declaration(declaration) => declaration.roots(),
        }
    }
}

/// Read access shared by the builder and the frozen graph.
pub(crate) trait Arena {
    fn rel(&self, id: RelId) -> &RelNode;
    fn expr(&self, id: ExprId) -> &ExprNode;
    fn truth(&self, id: TruthId) -> &TruthNode;
    fn binder(&self, id: BinderId) -> &BinderSite;
    fn merge(&self, id: MergeId) -> &MergeSite;
    fn instance(&self, id: InstanceId) -> &Instance;
    fn passenger(&self, id: PassengerId) -> &Passenger;
    /// The passenger carrying an extraction's node, where a stage carries
    /// one.
    fn node_carrier(&self, extraction: ExprId) -> Option<PassengerId>;
}

macro_rules! arena_reads {
    ($ty:ty) => {
        impl Arena for $ty {
            fn rel(&self, id: RelId) -> &RelNode {
                &self.rels[id.index()]
            }
            fn expr(&self, id: ExprId) -> &ExprNode {
                &self.exprs[id.index()]
            }
            fn truth(&self, id: TruthId) -> &TruthNode {
                &self.truths[id.index()]
            }
            fn binder(&self, id: BinderId) -> &BinderSite {
                &self.binders[id.index()]
            }
            fn merge(&self, id: MergeId) -> &MergeSite {
                &self.merges[id.index()]
            }
            fn instance(&self, id: InstanceId) -> &Instance {
                &self.instances[id.index()]
            }
            fn passenger(&self, id: PassengerId) -> &Passenger {
                &self.passengers[id.index()]
            }
            fn node_carrier(&self, extraction: ExprId) -> Option<PassengerId> {
                self.passengers
                    .iter()
                    .position(|p| matches!(p, Passenger::Node { extraction: e } if *e == extraction))
                    .map(PassengerId::at)
            }
        }
    };
}

arena_reads!(Builder);
arena_reads!(Graph);

mod sealed {
    pub(crate) trait Sealed {}
}

/// An arena a decider may judge over: the builder, while the graph is being
/// made, and the core's own recheck of a frozen graph. The frozen [`Graph`]
/// realization holds is not one: a fact the core decides is read from the
/// node that stores it, never decided again downstream.
pub(crate) trait Judging: Arena + sealed::Sealed {}

impl sealed::Sealed for Builder {}
impl Judging for Builder {}

/// The core's recheck view of a frozen graph: only the core makes one.
pub(crate) struct Recheck<'g>(&'g Graph);

impl<'g> Recheck<'g> {
    pub(in crate::pipeline::middle::core) fn of(graph: &'g Graph) -> Self {
        Recheck(graph)
    }
}

impl sealed::Sealed for Recheck<'_> {}
impl Judging for Recheck<'_> {}

impl Arena for Recheck<'_> {
    fn rel(&self, id: RelId) -> &RelNode {
        self.0.rel(id)
    }
    fn expr(&self, id: ExprId) -> &ExprNode {
        self.0.expr(id)
    }
    fn truth(&self, id: TruthId) -> &TruthNode {
        self.0.truth(id)
    }
    fn binder(&self, id: BinderId) -> &BinderSite {
        self.0.binder(id)
    }
    fn merge(&self, id: MergeId) -> &MergeSite {
        self.0.merge(id)
    }
    fn instance(&self, id: InstanceId) -> &Instance {
        self.0.instance(id)
    }
    fn passenger(&self, id: PassengerId) -> &Passenger {
        self.0.passenger(id)
    }
    fn node_carrier(&self, extraction: ExprId) -> Option<PassengerId> {
        self.0.node_carrier(extraction)
    }
}

impl Builder {
    pub(crate) fn new() -> Self {
        Builder {
            rels: Vec::new(),
            exprs: Vec::new(),
            truths: Vec::new(),
            binders: Vec::new(),
            merges: Vec::new(),
            instances: Vec::new(),
            passengers: Vec::new(),
        }
    }

    /// Every relation built so far.
    pub(crate) fn built(&self) -> impl Iterator<Item = RelId> {
        (0..self.rels.len()).map(RelId::at)
    }

    pub(super) fn push_rel(&mut self, node: RelNode) -> RelId {
        self.rels.push(node);
        RelId::at(self.rels.len() - 1)
    }

    pub(super) fn push_expr(&mut self, node: ExprNode) -> ExprId {
        self.exprs.push(node);
        ExprId::at(self.exprs.len() - 1)
    }

    pub(super) fn push_truth(&mut self, node: TruthNode) -> TruthId {
        self.truths.push(node);
        TruthId::at(self.truths.len() - 1)
    }

    /// Mint a fresh binder: the one act that begins an occurrence (W5 #1).
    pub(super) fn fresh_binder(&mut self, site: BinderSite) -> BinderId {
        self.binders.push(site);
        BinderId::at(self.binders.len() - 1)
    }

    pub(super) fn push_merge(&mut self, site: MergeSite) -> MergeId {
        self.merges.push(site);
        MergeId::at(self.merges.len() - 1)
    }

    pub(super) fn push_instance(&mut self, instance: Instance) -> InstanceId {
        self.instances.push(instance);
        InstanceId::at(self.instances.len() - 1)
    }

    pub(super) fn push_passenger(&mut self, passenger: Passenger) -> PassengerId {
        self.passengers.push(passenger);
        PassengerId::at(self.passengers.len() - 1)
    }

    /// Freeze the representation, the statement's kind worked out from the
    /// relation it returns: a program when it holds an act
    /// (`decide::effect::acts`, the one answer to that question), decided
    /// as one (`decide::effect::program`), else a query; and the facts of
    /// its values settled. Under `debug_assertions` every derived fact is
    /// recomputed by its decider over the frozen children and compared with
    /// the stored one.
    pub(crate) fn finish(self, finished: Finished, switches: super::switches::Switches) -> Result<Graph, super::refuse::Refusal> {
        let statement = match finished {
            Finished::Relation(rel) if super::decide::effect::acts(&self, rel).is_empty() => Statement::Query(rel),
            Finished::Relation(rel) => Statement::Program(super::decide::effect::program(&self, rel)?),
            Finished::Declaration(declaration) => Statement::Declaration(declaration),
        };
        let settled = Settled::of(&self);
        let graph = Graph {
            rels: self.rels,
            exprs: self.exprs,
            truths: self.truths,
            binders: self.binders,
            merges: self.merges,
            instances: self.instances,
            passengers: self.passengers,
            statement,
            switches,
            settled,
        };
        #[cfg(debug_assertions)]
        super::recompute::check(&graph);
        Ok(graph)
    }
}

impl Graph {
    pub(crate) fn statement(&self) -> &Statement {
        &self.statement
    }

    /// A value's structure (`node::rel::structure_of`), settled at the
    /// freeze, or the refusal judging it answered.
    pub(crate) fn structure(&self, e: ExprId) -> Result<&super::heading::Interior, super::refuse::Refusal> {
        self.settled.structures[e.index()].as_ref().map_err(Clone::clone)
    }

    /// Whether a value evaluates a window itself
    /// (`node::expr::evaluates_window`), settled at the freeze.
    pub(crate) fn evaluates_window(&self, e: ExprId) -> bool {
        self.settled.windows[e.index()]
    }

    /// An extracted value's node road (`node::rel::node_road`), settled at
    /// the freeze.
    pub(crate) fn node_road(&self, e: ExprId) -> Option<super::node::rel::NodeRoad> {
        self.settled.roads[e.index()]
    }

    /// Whether a value depends on the rows around its own row
    /// (`node::expr::population_sensitive`), settled at the freeze.
    pub(crate) fn population_sensitive(&self, e: ExprId) -> bool {
        self.settled.sensitive[e.index()]
    }

    /// Whether a truth reads a population-sensitive value, settled at the
    /// freeze.
    pub(crate) fn truth_population_sensitive(&self, t: TruthId) -> bool {
        self.settled.truths_sensitive[t.index()]
    }

    /// Whether bind `bind` of level `level` of the expansion `unnest` offers
    /// its node beside its value (`node::rel::bind_offers_node`), settled at
    /// the freeze. A bind the freeze settled nothing for is a broken
    /// contract.
    pub(crate) fn bind_offers_node(&self, unnest: RelId, level: usize, bind: usize) -> Result<bool, super::refuse::Refusal> {
        self.settled
            .offers
            .get(&unnest)
            .and_then(|levels| levels.get(level))
            .and_then(|binds| binds.get(bind))
            .copied()
            .ok_or_else(|| super::refuse::contract("a bind of an expansion the freeze settled nothing for"))
    }

    pub(crate) fn switches(&self) -> &super::switches::Switches {
        &self.switches
    }

    pub(crate) fn rel_count(&self) -> usize {
        self.rels.len()
    }

    pub(crate) fn expr_count(&self) -> usize {
        self.exprs.len()
    }

    pub(crate) fn truth_count(&self) -> usize {
        self.truths.len()
    }

    pub(crate) fn instance_count(&self) -> usize {
        self.instances.len()
    }

    pub(crate) fn passenger_count(&self) -> usize {
        self.passengers.len()
    }
}
