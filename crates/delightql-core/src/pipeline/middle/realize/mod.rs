// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Realization: SQL for one target, chosen from the frozen graph by general
//! rules, and lowered through the retained doors. Realization reads the
//! graph and its analyses; it decides no semantic fact and writes nothing
//! into the core. Its records (the environment, tables, frames, the kind
//! table) are built after the graph is finished and die with the
//! statement's realization.

mod collect;
mod declaration;
mod dependent;
mod document;
mod effect;
mod fix;
mod frame;
mod kinds;
mod rel;
mod run;
mod target;
mod value;

use super::api::{Route, Window};
use super::core::graph::{Arena, Graph, Statement};
use super::core::ids::RelId;
use super::core::node::walk::{self, Child};
use super::core::node::{ReadSource, RelKind};
use super::facade::{self, ColId, Cte, Output, ScopeId};
use super::facts::activation::Activation;
use frame::Enclosing;
use std::collections::BTreeMap;

pub(super) type Result<T> = std::result::Result<T, delightql_types::DelightQLError>;

/// A body read more than once, realized once as a statement-level binding.
struct Shared {
    scope: ScopeId,
    cols: Vec<ColId>,
}

/// A table a user receives (`Realizer::received`).
#[derive(Clone, Copy)]
pub(super) enum Received {
    Result,
    Created,
    Inserted,
}

/// One statement's realization for one target.
pub(super) struct Realizer<'r, 's> {
    graph: &'r Graph,
    activation: Activation,
    out: Output<'s>,
    kinds: kinds::KindTable,
    /// Statement-level bindings, each after the ones it reads.
    ctes: Vec<Cte>,
    shared: BTreeMap<RelId, Shared>,
    /// How many reads each built relation has.
    reads: BTreeMap<RelId, usize>,
    /// The relations a program staged once, which every later read reads.
    stagings: BTreeMap<RelId, effect::Staging>,
    /// Each act's staged receipt row, by its receipt.
    receipts: BTreeMap<RelId, effect::Staging>,
    /// Each written table as the program's statements name it, by its
    /// target read: one scope every statement writing or probing it reads.
    targets: BTreeMap<(String, Option<String>), effect::Target>,
    /// Each fixpoint's working table, for the reads of its frontier.
    frontiers: BTreeMap<super::core::ids::BinderId, fix::Frontier>,
    /// The runs whose bound is the total cap of the recursive step being
    /// written, which the step's member writes as its own row clause.
    caps: std::collections::BTreeSet<RelId>,
    /// The anchors of the anchored cases the statement holds.
    anchors: std::collections::BTreeSet<super::core::ids::ExprId>,
    /// Every interior heading, by the collection that formed it.
    interior_headings: BTreeMap<super::core::ids::ExprId, super::core::heading::Heading>,
    /// The columns naming each interior heading's positions, by the
    /// collection that formed it and the heading.
    interior_scopes: Vec<(super::core::heading::Origin, super::core::heading::Heading, Vec<ColId>)>,
    /// Each read a session object the statement creates would shadow, with
    /// the schema that reaches the durable relation past the session pool,
    /// as the creation records it.
    past_session: BTreeMap<RelId, String>,
    /// For each fixpoint's working-table scope, the positions whose parts
    /// are carried without their affinity (`Realizer::affinity_free`).
    free_positions: BTreeMap<crate::pipeline::middle::facade::ScopeId, Vec<bool>>,
    /// The relation the statement presents: the one whose ending ordering
    /// presentation consumes.
    presented: Option<RelId>,
}

impl Window<'_, '_> {
    /// Realize the statement: its SQL for the window's target, as the
    /// relay runs it.
    pub(super) fn realize(&self) -> Result<Route> {
        Realizer::new(self.graph, self.input.output(self.names)).statement()
    }
}

impl<'r, 's> Realizer<'r, 's> {
    fn new(graph: &'r Graph, out: Output<'s>) -> Self {
        let activation = super::facts::activation::of(graph);
        let mut reads: BTreeMap<RelId, usize> = BTreeMap::new();
        let roots = statement_roots(graph);
        let reach = walk::reachable(graph, &roots);
        let kinds = kinds::KindTable::of(graph, reach.rels.iter().copied(), &out);
        let interior_headings = collect::interior_headings(graph, reach.rels.iter().copied(), reach.exprs.iter().copied());
        let anchors = reach
            .exprs
            .iter()
            .filter_map(|e| match graph.expr(*e).kind() {
                super::core::node::ExprKind::Case { anchor, .. } => *anchor,
                _ => None,
            })
            .collect();
        // Each read a creation's session object would shadow is spelled
        // past the session pool wherever it is realized, as elaboration
        // recorded on the creation.
        let mut past_session = BTreeMap::new();
        for r in &reach.rels {
            match graph.rel(*r).kind() {
                RelKind::Receipt { act, .. } => {
                    if let super::core::node::effect::Write::Object(created) = act.write() {
                        past_session.extend(created.past_session.iter().cloned());
                    }
                }
                RelKind::Read {
                    source: ReadSource::Local(body),
                    ..
                } => *reads.entry(*body).or_insert(0) += 1,
                _ => {}
            }
        }
        Realizer {
            graph,
            activation,
            out,
            kinds,
            ctes: Vec::new(),
            shared: BTreeMap::new(),
            reads,
            stagings: BTreeMap::new(),
            receipts: BTreeMap::new(),
            targets: BTreeMap::new(),
            frontiers: BTreeMap::new(),
            caps: std::collections::BTreeSet::new(),
            anchors,
            interior_headings,
            interior_scopes: Vec::new(),
            past_session,
            free_positions: BTreeMap::new(),
            presented: None,
        }
    }

    fn statement(&mut self) -> Result<Route> {
        match self.graph.statement() {
            Statement::Query(rel) => {
                let rel = *rel;
                let connection = self.one_connection(&self.read_connections())?;
                let query = self.result(rel)?;
                let with = std::mem::take(&mut self.ctes);
                let statement = facade::SqlStatement::Query {
                    with_clause: (!with.is_empty()).then_some(with),
                    query,
                };
                let rendered = self.out.render(vec![statement])?;
                let (Some(sql), Some(naming)) = (rendered.sql.into_iter().next(), rendered.naming.into_iter().next()) else {
                    return Err(self.contract("a rendering that wrote no statement"));
                };
                Ok(Route::Query(self.out.compiled_query(sql, Some(connection), naming)))
            }
            Statement::Program(program) => Ok(Route::Program(
                self.program(program)?,
                crate::pipeline::middle::facade::Trailing::after(Vec::new()),
            )),
            Statement::Declaration(_) => Err(self.contract("a declared table realized as a statement")),
        }
    }

    /// B13: the statement's result: every displayed position under its
    /// display name (a lost or minted name under a drawn one), in the final
    /// ordering when the result relation ends in one.
    fn result(&mut self, rel: RelId) -> Result<facade::QueryExpression> {
        self.presented = Some(rel);
        let table = self.table(rel, &Enclosing::none())?;
        let heading = self.graph.rel(rel).heading().clone();
        let at = self.out.scope(None);
        let mut items = Vec::new();
        for (i, p) in heading.displayed() {
            let col = self.out.column(at, p.answering_name());
            items.push(facade::SelectItem::expression_with_alias(
                facade::SqlExpr::Column(table.cols[i]),
                col,
            ));
        }
        self.received(&items, Received::Result)?;
        let order_by = table
            .order
            .iter()
            .map(|(c, d)| facade::OrderTerm::new(facade::SqlExpr::Column(*c), Some(d.clone())))
            .collect();
        self.out.select(facade::Select {
            at,
            distinct: false,
            items,
            from: vec![table.from],
            filter: None,
            group_by: None,
            having: None,
            order_by,
            limit: None,
        })
    }

    /// ZERO WIDTH IS LAWFUL (FN.14): a table a user receives — the
    /// statement's result, a created view or table, a table rows are
    /// inserted into — holds the values its relation supplies and nothing
    /// else, so a placeholder never becomes one of them. A zero-width result
    /// or created object exists only on a target whose SQL holds a
    /// zero-column table, and the SQL written here has no empty select list;
    /// a row inserted supplying no column is no row a SELECT here writes.
    fn received(&self, items: &[facade::SelectItem], into: Received) -> Result<()> {
        if !items.is_empty() {
            return Ok(());
        }
        let what = match into {
            Received::Result => "the statement's result",
            Received::Created => "a created view or table",
            Received::Inserted => return Err(self.uncovered("an insert whose rows supply no column")),
        };
        let dialect = self.out.dialect();
        if !dialect.returns_zero_column_table() {
            return Err(super::core::refuse::zero_width_table(what, &format!("{dialect:?}")));
        }
        Err(self.uncovered("a table of zero columns a user receives: the SQL written here has no empty select list"))
    }

    /// The connections the statement's catalog reads are served by.
    fn read_connections(&self) -> Vec<i64> {
        let reach = walk::reachable(self.graph, &statement_roots(self.graph));
        let mut connections: Vec<i64> = reach
            .rels
            .iter()
            .filter_map(|r| match self.graph.rel(*r).kind() {
                RelKind::Read {
                    source:
                        ReadSource::Catalog { physical, .. }
                        | ReadSource::Function { physical, .. }
                        | ReadSource::Created { physical, .. },
                    ..
                } => Some(physical.connection),
                _ => None,
            })
            .collect();
        connections.sort_unstable();
        connections.dedup();
        connections
    }

    /// The one connection of a statement's connections; the user
    /// connection when there is none.
    fn one_connection(&self, connections: &[i64]) -> Result<i64> {
        match connections {
            [] => Ok(2),
            [one] => Ok(*one),
            _ => Err(self.uncovered("a statement reading relations of several connections")),
        }
    }

    /// A refusal naming what realization does not cover.
    fn uncovered(&self, what: &str) -> delightql_types::DelightQLError {
        super::core::refuse::outside(&format!("realizing {what} for {:?}", self.out.dialect()))
    }

    /// A contract of the realization broken: a defect it caught.
    fn contract(&self, what: &str) -> delightql_types::DelightQLError {
        super::core::refuse::contract(what)
    }

    /// A value only another road stages, read on a road that does not:
    /// a form realization does not cover.
    fn unreached(&self, what: &str) -> delightql_types::DelightQLError {
        self.uncovered(&format!("{what} where the stage reading it does not carry it"))
    }

    /// A value the road reading it must itself have staged, missing: a
    /// defect, never a boundary of what realization covers.
    fn unsupplied(&self, what: &str) -> delightql_types::DelightQLError {
        self.contract(&format!("{what} missing from the stage reading it, which realization supplies"))
    }
}

/// The relations a statement holds, from its result: an act's relations
/// are its receipt's children.
fn statement_roots(graph: &Graph) -> Vec<Child> {
    graph.statement().roots()
}
