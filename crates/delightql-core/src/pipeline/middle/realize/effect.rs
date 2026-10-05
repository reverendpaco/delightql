// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! B14: a program. Its acts run in the order the core built them (the
//! receipts the statement's result reaches, by construction order), each in
//! the run its place in the statement's top-level union gives it. Per act:
//! the relation it consumes is staged once, and every later read of that
//! relation (its witness, its payload, its receipt's interiors, the
//! write) reads the staging; its receipt row is staged when its verdict is
//! YES, judged before the write; then the check its obligations ask for,
//! and the write, the print or the assertion's verdict. The statement's
//! result reads the stagings. Every staging is dropped after the last run.

use super::frame::{Enclosing, Env, Table};
use super::value::{integer, At};
use super::{Realizer, Result};
use crate::pipeline::middle::core::decide::effect as decide_effect;
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::decide::mutation::Arrival;
use crate::pipeline::middle::core::heading::{Heading, Known, Origin};
use crate::pipeline::middle::core::ids::RelId;
use crate::pipeline::middle::core::node::effect::{EffectAct, ReceiptCell, RowWrite, Terminal, Verdict, Write};
use crate::pipeline::middle::core::node::walk::{self, Child};
use crate::pipeline::middle::core::node::{ReadSource, RelKind};
use crate::pipeline::middle::facade::{
    self, BinaryOperator, ColId, EntityId, Intrinsic, LiteralValue, PlanStatement, ProgramStep, RelationTarget,
    ObjectResidence, ObjectShape, ScopeId, SelectItem, SqlExpr, SqlStatement, StepAction, TableExpression,
};

/// A relation staged once: its scratch table and one column per heading
/// position, hidden positions included.
#[derive(Clone)]
pub(super) struct Staging {
    pub(super) scope: ScopeId,
    pub(super) cols: Vec<ColId>,
}

/// A written table as the program's statements name it: its entity, its
/// scope, one column per stored column, and, once a write reaches rows by
/// it, the column holding each row's locator.
#[derive(Clone)]
pub(super) struct Target {
    entity: EntityId,
    scope: ScopeId,
    cols: Vec<ColId>,
    locator: Option<Vec<ColId>>,
}

/// One statement of a step before rendering: SQL the generator writes, or
/// a creation's text around the SELECT it wraps.
#[derive(Clone)]
enum Part {
    Sql(SqlStatement),
    Wrapped(String, SqlStatement),
    Text(String),
}

impl Part {
    fn statement(&self) -> Option<&SqlStatement> {
        match self {
            Part::Sql(s) | Part::Wrapped(_, s) => Some(s),
            Part::Text(_) => None,
        }
    }
}

/// The statements one act's steps hold, before rendering: each step's
/// action shape and its statements, in order.
enum Pending {
    Stage(Vec<Part>),
    Check(Part, delightql_types::DelightQLError),
    Dml(Vec<Part>),
    Ddl(Vec<Part>),
    Host(Vec<Part>, Part),
    /// An abort's probe and label, and whether an author wrote it (else an
    /// assertion's verdict reaches it).
    Abort(Vec<Part>, Part, String, bool),
    /// A reached `exit!`'s latch.
    Exit(Vec<Part>),
    /// A session act: the directive, the statement reading its arguments'
    /// rows, and the statement writing each row it reports (with the
    /// report's width).
    Session(String, Part, Option<(Part, usize)>),
}

impl Pending {
    /// Every statement, in order, with whether it ships a result.
    fn parts(&self) -> Vec<(&Part, bool)> {
        match self {
            Pending::Stage(ps) | Pending::Dml(ps) | Pending::Ddl(ps) | Pending::Exit(ps) => {
                ps.iter().map(|p| (p, false)).collect()
            }
            Pending::Check(p, _) => vec![(p, false)],
            Pending::Host(ps, ship) => ps.iter().map(|p| (p, false)).chain(std::iter::once((ship, true))).collect(),
            Pending::Abort(ps, probe, _, _) => {
                ps.iter().map(|p| (p, false)).chain(std::iter::once((probe, false))).collect()
            }
            Pending::Session(_, arguments, report) => std::iter::once((arguments, false))
                .chain(report.iter().map(|(fill, _)| (fill, false)))
                .collect(),
        }
    }

    /// The step, its statements written in order.
    fn action(self, written: &mut impl Iterator<Item = PlanStatement>) -> Option<StepAction> {
        let many = |n: usize, written: &mut dyn Iterator<Item = PlanStatement>| -> Option<Vec<PlanStatement>> {
            (0..n).map(|_| written.next()).collect()
        };
        Some(match self {
            Pending::Stage(ps) => StepAction::Stage(many(ps.len(), written)?),
            Pending::Dml(ps) => StepAction::Dml(many(ps.len(), written)?),
            Pending::Ddl(ps) => StepAction::Ddl(many(ps.len(), written)?),
            Pending::Check(_, refusal) => StepAction::Check {
                statement: written.next()?,
                refusal,
            },
            Pending::Host(ps, _) => StepAction::Host {
                statements: many(ps.len(), written)?,
                ship: written.next()?,
            },
            Pending::Abort(ps, _, label, authored) => StepAction::Abort {
                statements: many(ps.len(), written)?,
                probe: written.next()?,
                label,
                authored,
            },
            Pending::Exit(ps) => StepAction::Exit(many(ps.len(), written)?),
            Pending::Session(directive, _, report) => StepAction::Session {
                directive,
                arguments: written.next()?,
                report: match report {
                    Some((_, width)) => Some((written.next()?, width)),
                    None => None,
                },
            },
        })
    }
}

impl Realizer<'_, '_> {
    /// The program of a statement whose result holds acts.
    pub(super) fn program(&mut self, program: &decide_effect::Program) -> Result<facade::CompiledPlan> {
        let graph = self.graph;
        let result = program.result();
        let acts: Vec<RelId> = program.acts().iter().map(|a| a.receipt()).collect();
        let connection = self.program_connection(&acts)?;
        let run_count = program.runs().len();
        let mut pending: Vec<Vec<(RelId, String, Pending)>> = (0..run_count).map(|_| Vec::new()).collect();
        let mut created: Vec<Vec<facade::CreatedObject>> = (0..run_count).map(|_| Vec::new()).collect();
        let mut setup: Vec<Part> = Vec::new();
        let mut latch: Option<Staging> = None;
        // The acts a gate opens: a carried relation such an act stages has an
        // empty shell made before every run, so a release of its payload
        // reads no rows where the act's steps did not run.
        let mut opened: std::collections::BTreeSet<RelId> = std::collections::BTreeSet::new();
        for r in walk::reachable(graph, &[Child::Rel(result)]).rels {
            if let RelKind::Run(run) = graph.rel(r).kind() {
                for m in run.members() {
                    opened.extend(m.opens().iter().copied());
                }
            }
        }
        for facts in program.acts() {
            let (r, run) = (&facts.receipt(), &facts.run());
            let RelKind::Receipt { act, cells, .. } = graph.rel(*r).kind() else {
                return Err(self.contract("an act that no receipt holds"));
            };
            let mut staged = Vec::new();
            // An invocation's arguments are staged once, before the first
            // act that reads them: every clause reads the one relation the
            // call bound its formal to.
            let unstaged: Vec<RelId> = facts.reads().iter().copied().filter(|i| !self.stagings.contains_key(i)).collect();
            for input in unstaged {
                let (_, statements) = self.stage_once(input)?;
                staged.extend(statements.into_iter().map(Part::Sql));
            }
            if !staged.is_empty() {
                pending[*run].push((*r, act.operation().to_string(), Pending::Stage(staged)));
            }
            let made = latch.is_none();
            let (steps, object, mut shells) = self.act(facts, act, cells, opened.contains(r), &mut latch)?;
            // The exit latch's empty shell is made before every run.
            if made {
                if let Some(staging) = &latch {
                    shells.extend(self.staging_shell(staging)?.into_iter().map(Part::Sql));
                }
            }
            pending[*run].extend(steps.into_iter().map(|p| (*r, act.operation().to_string(), p)));
            created[*run].extend(object);
            setup.extend(shells);
        }
        // Each gated member's gate: a probe of the members before it,
        // sampled by every step of the acts it opens.
        let mut gates: Vec<Part> = Vec::new();
        let mut gates_of: std::collections::BTreeMap<RelId, Vec<usize>> = std::collections::BTreeMap::new();
        for r in walk::reachable(graph, &[Child::Rel(result)]).rels {
            let RelKind::Run(run) = graph.rel(r).kind() else {
                continue;
            };
            for (j, m) in run.members().enumerate() {
                if m.opens().is_empty() {
                    continue;
                }
                let probe = self.gate_probe(r, j)?;
                let id = gates.len();
                gates.push(Part::Sql(probe));
                for act in m.opens() {
                    gates_of.entry(*act).or_default().push(id);
                }
            }
        }
        let result_query = self.result(result)?;
        let with = self.statement_bindings();
        let ship = Part::Sql(SqlStatement::Query {
            with_clause: (!with.is_empty()).then_some(with),
            query: result_query,
        });
        let cleanup: Vec<Part> = self
            .stagings
            .values()
            .chain(self.receipts.values())
            .chain(latch.iter())
            .map(|s| Part::Sql(SqlStatement::DropTempTable { table: s.scope }))
            .collect();
        // Every statement in program order, with whether it ships: the
        // setup, the runs' steps, the result, the cleanup, the gates.
        let mut flat: Vec<(Part, bool)> = setup.iter().map(|p| (p.clone(), false)).collect();
        for run in &pending {
            for (_, _, p) in run {
                flat.extend(p.parts().into_iter().map(|(part, ships)| (part.clone(), ships)));
            }
        }
        flat.push((ship, true));
        flat.extend(cleanup.iter().map(|p| (p.clone(), false)));
        flat.extend(gates.iter().map(|p| (p.clone(), false)));
        let latch_probe = match &latch {
            Some(staging) => Some(Part::Sql(self.latch_probe(staging)?)),
            None => None,
        };
        flat.extend(latch_probe.iter().map(|p| (p.clone(), false)));
        // Render every statement through one baptism. A write names its
        // table by the table's own name, so the writes stand first: each
        // written table's one scope is baptised before any read of it.
        let rendered_at: Vec<usize> = (0..flat.len()).filter(|i| flat[*i].0.statement().is_some()).collect();
        let writes: Vec<usize> = rendered_at
            .iter()
            .copied()
            .filter(|i| flat[*i].0.statement().is_some_and(is_write))
            .collect();
        let order: Vec<usize> =
            writes.iter().copied().chain(rendered_at.iter().copied().filter(|i| !writes.contains(i))).collect();
        let rendered = self.out.render(order.iter().filter_map(|i| flat[*i].0.statement().cloned()).collect())?;
        let mut written: Vec<Option<PlanStatement>> = flat
            .iter()
            .map(|(part, _)| match part {
                Part::Text(text) => Some(PlanStatement {
                    sql: text.clone(),
                    connection_id: Some(connection),
                    comment: None,
                    naming: None,
                }),
                Part::Sql(_) | Part::Wrapped(..) => None,
            })
            .collect();
        if rendered.sql.len() != order.len() || rendered.naming.len() != order.len() {
            return Err(self.contract("a rendering that does not write one statement per statement asked"));
        }
        for ((sql, naming), i) in rendered.sql.into_iter().zip(rendered.naming).zip(&order) {
            let (part, ships) = &flat[*i];
            let sql = match part {
                Part::Sql(_) | Part::Text(_) => sql,
                Part::Wrapped(prefix, _) => format!("{prefix}{sql}"),
            };
            written[*i] = Some(PlanStatement {
                sql,
                connection_id: Some(connection),
                comment: None,
                naming: if *ships { naming } else { None },
            });
        }
        let written: Vec<PlanStatement> = written
            .into_iter()
            .collect::<Option<_>>()
            .ok_or_else(|| self.contract("a program statement that no rendering wrote"))?;
        let short = || self.contract("a program whose steps hold more statements than it wrote");
        let mut written = written.into_iter();
        let setup = (0..setup.len()).map(|_| written.next()).collect::<Option<Vec<_>>>().ok_or_else(short)?;
        let mut program_runs = Vec::with_capacity(pending.len());
        // Every step after a reached `exit!` runs only while its latch is
        // unset: later effects, later runs' included, and the result.
        let mut after_exit = false;
        for run in pending {
            let mut steps = Vec::new();
            for (act, operation, p) in run {
                let exits = matches!(p, Pending::Exit(_));
                steps.push(ProgramStep {
                    operation,
                    gates: gates_of.get(&act).cloned().unwrap_or_default(),
                    after_exit,
                    action: p.action(&mut written).ok_or_else(short)?,
                });
                after_exit |= exits;
            }
            program_runs.push(steps);
        }
        let ship = written.next().ok_or_else(short)?;
        if let Some(last) = program_runs.last_mut() {
            last.push(ProgramStep {
                operation: "return!".to_string(),
                gates: Vec::new(),
                after_exit,
                action: StepAction::Return(ship),
            });
        }
        let cleanup = (0..cleanup.len()).map(|_| written.next()).collect::<Option<Vec<_>>>().ok_or_else(short)?;
        let gates = (0..gates.len()).map(|_| written.next().map(|s| s.sql)).collect::<Option<Vec<_>>>().ok_or_else(short)?;
        let exit = match latch_probe {
            Some(_) => Some(written.next().map(|s| s.sql).ok_or_else(short)?),
            None => None,
        };
        if written.next().is_some() {
            return Err(self.contract("a program that wrote more statements than its steps hold"));
        }
        Ok(self.out.program(facade::Program {
            connection,
            setup,
            runs: program_runs,
            bracketed: program.runs().iter().map(|r| r.bracketed()).collect(),
            cleanup,
            gates,
            exit,
            created,
        }))
    }

    /// One act's steps, and the object it creates, by the facts the core
    /// decided of it in its program.
    fn act(
        &mut self,
        facts: &decide_effect::ProgramAct,
        act: &EffectAct,
        cells: &[ReceiptCell],
        gated: bool,
        latch: &mut Option<Staging>,
    ) -> Result<(Vec<Pending>, Option<facade::CreatedObject>, Vec<Part>)> {
        let (receipt, ordinal) = (facts.receipt(), facts.ordinal());
        let mut out = Vec::new();
        let mut stage: Vec<Part> = Vec::new();
        let creates = matches!(act.write(), Write::Object(_));
        // The consumed relation, staged once: a creation reads it whole in
        // its own statement instead.
        // Every staging a gated act makes has an empty shell made before
        // every run: a later read of it (a release of the act's payload)
        // reads no rows where the act's steps did not run.
        let mut carried_shells = Vec::new();
        let source = match act.source() {
            Some(source) if !creates => {
                let (staging, statements) = self.stage_once(source)?;
                if gated && !statements.is_empty() {
                    carried_shells.extend(self.staging_shell(&staging)?);
                }
                stage.extend(statements.into_iter().map(Part::Sql));
                Some(staging)
            }
            Some(_) | None => None,
        };
        // The witness, staged once over the staged input, and every other
        // relation the receipt carries: each holds its value at the act.
        if let Verdict::Witness { witness, .. } = act.verdict() {
            let (staging, statements) = self.stage_once(*witness)?;
            if gated && !statements.is_empty() {
                carried_shells.extend(self.staging_shell(&staging)?);
            }
            stage.extend(statements.into_iter().map(Part::Sql));
        }
        // The relation the act reports into: an empty shell before the act,
        // filled by the act's report, read by the receipt after it.
        let report = match act.write() {
            Write::Session { report: Some(rel), .. } => Some(*rel),
            _ => None,
        };
        let mut fill = None;
        if let Some(rel) = report {
            let scope = self.out.scratch();
            let width = self.graph.rel(rel).heading().displayed().count();
            let cols: Vec<ColId> = (0..width).map(|_| self.out.column(scope, None)).collect();
            let staging = Staging { scope, cols };
            stage.extend(self.staging_shell(&staging)?.into_iter().map(Part::Sql));
            if gated {
                carried_shells.extend(self.staging_shell(&staging)?);
            }
            fill = Some((Part::Sql(self.report_fill(&staging)?), width));
            self.stagings.insert(rel, staging);
        }
        for cell in cells {
            if let ReceiptCell::Interior(rel) = cell {
                if Some(*rel) == report {
                    continue;
                }
                let (staging, statements) = self.stage_once(*rel)?;
                if gated && !statements.is_empty() {
                    carried_shells.extend(self.staging_shell(&staging)?);
                }
                stage.extend(statements.into_iter().map(Part::Sql));
            }
        }
        // The receipt row, judged before the write.
        let verdict = self.verdict(act, source.as_ref())?;
        let (shell, row) = self.receipt_row(receipt, cells, verdict)?;
        let setup: Vec<Part> = shell.into_iter().chain(carried_shells).map(Part::Sql).collect();
        let receipt_row = vec![Part::Sql(row)];
        let mut object = None;
        match act.write() {
            Write::None => {
                stage.extend(receipt_row);
                out.push(Pending::Stage(stage));
                if act.prints() {
                    let staging = source.ok_or_else(|| self.contract("a print with no input"))?;
                    let ship = self.read_all(&staging)?;
                    out.push(Pending::Host(Vec::new(), Part::Sql(ship)));
                }
                if let Verdict::Witness { witness, label } = act.verdict() {
                    let staging = self.stagings[witness].clone();
                    let at = self.out.scope(None);
                    let inner = self.exists_in(&staging)?;
                    let probe = self.select_at(at, Vec::new(), Vec::new(), vec![SqlExpr::not_exists(inner)], None, Vec::new(), None, false)?;
                    out.push(Pending::Abort(
                        Vec::new(),
                        Part::Sql(SqlStatement::Query {
                            with_clause: None,
                            query: probe,
                        }),
                        label.clone().unwrap_or_else(|| format!("{}#{ordinal}", act.operation())),
                        false,
                    ));
                }
            }
            Write::Terminal(terminal) => {
                // With no input the disposition is reached whenever its step
                // runs: its condition is empty.
                let reached = match &source {
                    Some(staging) => vec![SqlExpr::exists(self.exists_in(staging)?)],
                    None => Vec::new(),
                };
                stage.extend(receipt_row);
                out.push(Pending::Stage(stage));
                match terminal {
                    Terminal::Abort { label } => {
                        let at = self.out.scope(None);
                        let probe = self.select_at(at, Vec::new(), Vec::new(), reached, None, Vec::new(), None, false)?;
                        out.push(Pending::Abort(
                            Vec::new(),
                            Part::Sql(SqlStatement::Query {
                                with_clause: None,
                                query: probe,
                            }),
                            label.clone(),
                            true,
                        ));
                    }
                    Terminal::Exit => {
                        let latch = self.exit_latch(latch)?;
                        let at = self.out.scope(None);
                        let set = self.select_at(
                            at,
                            vec![SelectItem::expression_with_alias(SqlExpr::Literal(integer(1)), self.out.column(at, None))],
                            Vec::new(),
                            reached,
                            None,
                            Vec::new(),
                            None,
                            false,
                        )?;
                        out.push(Pending::Exit(vec![Part::Sql(SqlStatement::Insert {
                            target: RelationTarget::Scope(latch.scope),
                            target_scope: latch.scope,
                            columns: latch.cols.clone(),
                            with_clause: None,
                            source: set,
                        })]));
                    }
                }
            }
            Write::Session { directive, .. } => {
                let staging = source.ok_or_else(|| self.contract("a session act with no arguments"))?;
                // A receipt carrying the act's report reads it after the act.
                let after = fill.is_some();
                if !after {
                    stage.extend(receipt_row.clone());
                }
                out.push(Pending::Stage(stage));
                let arguments = self.read_all(&staging)?;
                out.push(Pending::Session(directive.clone(), Part::Sql(arguments), fill));
                if after {
                    out.push(Pending::Stage(receipt_row));
                }
            }
            Write::Rows { target, rows } => {
                let staging = source.ok_or_else(|| self.contract("a row write with no source"))?;
                out.push(Pending::Stage(stage));
                // DECISION(mutation): the source staged once, its verdict and
                // check judged on the stage, then the write.
                if let RowWrite::Replace { arrival, .. } = rows {
                    out.push(self.one_row_per_locator(&staging, arrival)?);
                }
                let write = self.row_write(*target, rows, &staging)?;
                let mut statements = receipt_row;
                statements.push(Part::Sql(write));
                out.push(Pending::Dml(statements));
            }
            Write::Object(created) => {
                let source = act.source().ok_or_else(|| self.contract("a creation with no source"))?;
                if created.shape() == ObjectShape::View && facts.source_holds_acts() {
                    return Err(self.uncovered("a tracking view over an effect's receipt"));
                }
                let mut statements = stage;
                statements.extend(receipt_row);
                if let Some(holder) = created.replaces {
                    statements.push(Part::Text(self.out.drop_text(&created.placement, holder == ObjectShape::View)?));
                }
                // A session creation replaces an object of its own kind and
                // name in the temp schema, recorded or not.
                if created.residence() == ObjectResidence::SessionShadow && created.replaces != Some(created.shape()) {
                    statements.push(Part::Text(self.out.drop_text(&created.placement, created.shape() == ObjectShape::View)?));
                }
                let select = self.select_whole(source)?;
                let prefix = self.out.create_text(&created.placement, "");
                statements.push(Part::Wrapped(prefix, select));
                let interior = self
                    .graph
                    .rel(source)
                    .heading()
                    .displayed()
                    // A stored nested relation is a position holding rows;
                    // a made record or tuple is stored as its document.
                    .filter(|(_, p)| matches!(p.interior.known, Known::Shape(..) | Known::PerArm(_)))
                    .map(|(i, _)| i)
                    .collect();
                object = Some(self.out.created_object(&created.placement, interior));
                out.push(Pending::Ddl(statements));
            }
        }
        Ok((out, object, setup))
    }

    /// The program's exit latch: one scratch table, set by a reached
    /// `exit!`.
    fn exit_latch(&mut self, latch: &mut Option<Staging>) -> Result<Staging> {
        if let Some(staging) = latch {
            return Ok(staging.clone());
        }
        let scope = self.out.scratch();
        let staging = Staging {
            scope,
            cols: vec![self.out.column(scope, None)],
        };
        *latch = Some(staging.clone());
        Ok(staging)
    }

    /// The exit latch's probe: the number of its rows.
    fn latch_probe(&mut self, latch: &Staging) -> Result<SqlStatement> {
        let at = self.out.scope(None);
        let count = SelectItem::expression_with_alias(self.out.function("count", vec![SqlExpr::Star]), self.out.column(at, None));
        let query = self.select_at(at, vec![count], vec![TableExpression::Scope(latch.scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(SqlStatement::Query {
            with_clause: None,
            query,
        })
    }

    /// A gate's probe: the number of rows of the members of a run before
    /// its gated member, under the qualifiers written before it.
    fn gate_probe(&mut self, run_rel: RelId, j: usize) -> Result<SqlStatement> {
        use super::run::{Indexed, Placed, Quals, Rows};
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(run_rel).kind() else {
            return Err(self.contract("a gate of no run"));
        };
        let outer = Enclosing::none();
        let q = Quals::of(run);
        let mut placed = Placed::of(self, &q)?;
        let items = q.indexed();
        let boundaries = self.boundaries(&q, &items);
        let at = items
            .iter()
            .position(|i| matches!(i, Indexed::Member(m) if *m == j))
            .ok_or_else(|| self.contract("a gated member the run does not hold"))?;
        let segments = self.run_population(run, &items, &boundaries, |i| i < at, &[])?;
        let mut ordering = Vec::new();
        let (rows, filters) = self.realize_population(
            Rows::of(super::frame::Frame::unit()),
            &q,
            &mut placed,
            segments,
            &mut ordering,
            &outer,
        )?;
        let rows = self.filtered(rows, &filters, &outer)?;
        let frame = self.stage(rows, &[], &outer)?;
        let count_at = self.out.scope(None);
        let count = SelectItem::expression_with_alias(self.out.function("count", vec![SqlExpr::Star]), self.out.column(count_at, None));
        let query = self.select_at(count_at, vec![count], frame.from.into_iter().collect(), Vec::new(), None, Vec::new(), None, false)?;
        let with = self.statement_bindings();
        Ok(SqlStatement::Query {
            with_clause: (!with.is_empty()).then_some(with),
            query,
        })
    }

    /// The whole of a relation as one SELECT: its displayed positions under
    /// their names, in heading order.
    fn select_whole(&mut self, rel: RelId) -> Result<SqlStatement> {
        let graph = self.graph;
        let table = self.table(rel, &Enclosing::none())?;
        let with = self.statement_bindings();
        let heading = graph.rel(rel).heading().clone();
        let at = self.out.scope(None);
        let mut items = Vec::new();
        for (i, p) in heading.displayed() {
            let name = p
                .answering_name()
                .ok_or_else(|| self.uncovered("a created object whose position answers to no name"))?;
            let col = self.out.column(at, Some(name));
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(table.cols[i]), col));
        }
        self.received(&items, super::Received::Created)?;
        let query = self.select_at(at, items, vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
        Ok(SqlStatement::Query {
            with_clause: (!with.is_empty()).then_some(with),
            query,
        })
    }

    /// The statement-level bindings realized so far, for the statement
    /// being written: a binding is the statement's own, so a later
    /// statement that reads the same body realizes it again.
    fn statement_bindings(&mut self) -> Vec<facade::Cte> {
        self.shared.clear();
        std::mem::take(&mut self.ctes)
    }

    /// Stage a relation once: its scratch table, and the statements that
    /// make it. Every later read of the relation reads the staging.
    fn stage_once(&mut self, rel: RelId) -> Result<(Staging, Vec<SqlStatement>)> {
        if let Some(staging) = self.stagings.get(&rel) {
            return Ok((staging.clone(), Vec::new()));
        }
        let table = self.table(rel, &Enclosing::none())?;
        let with = self.statement_bindings();
        let scope = self.out.scratch();
        let cols: Vec<ColId> = table.cols.iter().map(|_| self.out.column(scope, None)).collect();
        let items = table
            .cols
            .iter()
            .zip(&cols)
            .map(|(c, sc)| SelectItem::expression_with_alias(SqlExpr::Column(*c), *sc))
            .collect();
        let query = self.select_at(scope, items, vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
        let staging = Staging { scope, cols };
        self.stagings.insert(rel, staging.clone());
        Ok((
            staging,
            vec![
                SqlStatement::DropTempTable { table: scope },
                SqlStatement::CreateTempTable {
                    table: scope,
                    with_clause: (!with.is_empty()).then_some(with),
                    query,
                },
            ],
        ))
    }

    /// A staging's empty shell: its table with no row, made before every
    /// run.
    fn staging_shell(&mut self, staging: &Staging) -> Result<Vec<SqlStatement>> {
        let items = staging
            .cols
            .iter()
            .map(|c| SelectItem::expression_with_alias(SqlExpr::Literal(LiteralValue::Null), *c))
            .collect();
        let never = SqlExpr::Binary {
            left: Box::new(SqlExpr::Literal(integer(1))),
            op: BinaryOperator::Equal,
            right: Box::new(SqlExpr::Literal(integer(0))),
        };
        let shell = self.select_at(staging.scope, items, Vec::new(), vec![never], None, Vec::new(), None, false)?;
        Ok(vec![
            SqlStatement::DropTempTable { table: staging.scope },
            SqlStatement::CreateTempTable {
                table: staging.scope,
                with_clause: None,
                query: shell,
            },
        ])
    }

    /// The statement writing one row an act reports into its staging: each
    /// position the report marker the runtime substitutes.
    fn report_fill(&mut self, staging: &Staging) -> Result<SqlStatement> {
        let at = self.out.scope(None);
        let items = staging
            .cols
            .iter()
            .enumerate()
            .map(|(k, _)| {
                SelectItem::expression_with_alias(
                    SqlExpr::Literal(LiteralValue::String(facade::report_marker(k))),
                    self.out.column(at, None),
                )
            })
            .collect();
        let source = self.select_at(at, items, Vec::new(), Vec::new(), None, Vec::new(), None, false)?;
        Ok(SqlStatement::Insert {
            target: RelationTarget::Scope(staging.scope),
            target_scope: staging.scope,
            columns: staging.cols.clone(),
            with_clause: None,
            source,
        })
    }

    /// The relation's staging when it was staged: read as a FROM item.
    pub(super) fn staged_read(&mut self, rel: RelId) -> Result<Option<Table>> {
        let Some(staging) = self.stagings.get(&rel).cloned() else {
            return Ok(None);
        };
        self.scratch_table(&staging).map(Some)
    }

    /// A scratch table read under a fresh scope.
    fn scratch_table(&mut self, staging: &Staging) -> Result<Table> {
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut cols = Vec::new();
        for c in &staging.cols {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
            cols.push(col);
        }
        let query = self.select_at(at, items, vec![TableExpression::Scope(staging.scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// `SELECT 1 FROM staging`, for an existence test.
    fn exists_in(&mut self, staging: &Staging) -> Result<facade::QueryExpression> {
        let at = self.out.scope(None);
        self.select_at(at, Vec::new(), vec![TableExpression::Scope(staging.scope)], Vec::new(), None, Vec::new(), None, false)
    }

    /// Every column of a staging, as the statement a print ships: its
    /// displayed positions under their names.
    fn read_all(&mut self, staging: &Staging) -> Result<SqlStatement> {
        let graph = self.graph;
        let rel = self
            .stagings
            .iter()
            .find(|(_, s)| s.scope == staging.scope)
            .map(|(r, _)| *r)
            .ok_or_else(|| self.contract("a print of no staging"))?;
        let heading = graph.rel(rel).heading().clone();
        let at = self.out.scope(None);
        let mut items = Vec::new();
        for (i, p) in heading.displayed() {
            let col = self.out.column(at, p.answering_name());
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(staging.cols[i]), col));
        }
        let query = self.select_at(at, items, vec![TableExpression::Scope(staging.scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(SqlStatement::Query {
            with_clause: None,
            query,
        })
    }

    /// The condition under which the act's receipt holds its row, judged
    /// before the write: a row write reaches a stored row; a witness or a
    /// ledger has a row; anything else when reached.
    fn verdict(&mut self, act: &EffectAct, source: Option<&Staging>) -> Result<Option<SqlExpr>> {
        Ok(match (act.verdict(), act.write()) {
            (Verdict::Affected, Write::Rows { target, rows }) => {
                let staging = source.ok_or_else(|| self.contract("a row write with no source"))?;
                Some(match rows {
                    RowWrite::Insert { .. } => SqlExpr::exists(self.exists_in(staging)?),
                    RowWrite::Replace { arrival, .. } | RowWrite::Remove { arrival } => {
                        let at = self.out.scope(None);
                        let (entity, scope, _) = self.target_read(*target)?;
                        let target_locator = self.target_locator(*target, arrival)?;
                        let staged_locator = self.arrived(staging, arrival)?;
                        let inner = self.matched(staging, &target_locator, &staged_locator)?;
                        let reached = self.select_at(
                            at,
                            Vec::new(),
                            vec![TableExpression::Entity {
                                entity,
                                alias: Some(scope),
                            }],
                            vec![SqlExpr::exists(inner)],
                            None,
                            Vec::new(),
                            None,
                            false,
                        )?;
                        SqlExpr::exists(reached)
                    }
                })
            }
            (Verdict::Affected, Write::None | Write::Object(_) | Write::Session { .. } | Write::Terminal(_)) => {
                return Err(self.contract("an affected-row verdict with no row write"))
            }
            (Verdict::Reached, _) => None,
            (Verdict::Witness { witness, .. }, _) => {
                let staging = self.stagings[witness].clone();
                Some(SqlExpr::exists(self.exists_in(&staging)?))
            }
            (Verdict::Any(ledger), _) => {
                let table = self.table(*ledger, &Enclosing::none())?;
                let at = self.out.scope(None);
                let inner = self.select_at(at, Vec::new(), vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
                Some(SqlExpr::exists(inner))
            }
        })
    }

    /// Stage the receipt's row: its constants and interiors when the
    /// verdict holds.
    /// The receipt's staging: its empty shell, made before every run so a
    /// gated act's receipt reads NO where its step did not run, and the
    /// statement that stages its row.
    fn receipt_row(&mut self, receipt: RelId, cells: &[ReceiptCell], verdict: Option<SqlExpr>) -> Result<(Vec<SqlStatement>, SqlStatement)> {
        let local = Env::fresh();
        let outer = Enclosing::none();
        let scope = self.out.scratch();
        let at = self.out.scope(None);
        let mut shell_items = Vec::new();
        let mut items = Vec::new();
        let mut cols = Vec::new();
        let heading = self.graph.rel(receipt).heading().clone();
        for (cell, position) in cells.iter().zip(heading.positions()) {
            let (shell, value) = match (cell, &position.interior.known) {
                (ReceiptCell::Const(e), _) => {
                    let value = self.value(*e, At { local: &local, outer: &outer })?;
                    (value.clone(), value)
                }
                (ReceiptCell::Interior(rel), Known::Shape(interior, origin)) => {
                    (SqlExpr::Literal(LiteralValue::Null), self.interior_document(*rel, *origin, interior)?)
                }
                (ReceiptCell::Interior(_), _) => {
                    return Err(self.contract("a receipt's carried relation with no interior heading"))
                }
            };
            let col = self.out.column(scope, None);
            shell_items.push(SelectItem::expression_with_alias(shell, col));
            items.push(SelectItem::expression_with_alias(value, self.out.column(at, None)));
            cols.push(col);
        }
        let never = SqlExpr::Binary {
            left: Box::new(SqlExpr::Literal(integer(1))),
            op: BinaryOperator::Equal,
            right: Box::new(SqlExpr::Literal(integer(0))),
        };
        let shell = self.select_at(scope, shell_items, Vec::new(), vec![never], None, Vec::new(), None, false)?;
        let row = self.select_at(at, items, Vec::new(), verdict.into_iter().collect(), None, Vec::new(), None, false)?;
        let with = self.statement_bindings();
        self.receipts.insert(receipt, Staging { scope, cols: cols.clone() });
        Ok((
            vec![
                SqlStatement::DropTempTable { table: scope },
                SqlStatement::CreateTempTable {
                    table: scope,
                    with_clause: None,
                    query: shell,
                },
            ],
            SqlStatement::Insert {
                target: RelationTarget::Scope(scope),
                target_scope: scope,
                columns: cols,
                with_clause: (!with.is_empty()).then_some(with),
                source: row,
            },
        ))
    }

    /// An interior relation as a value: the document of its displayed rows,
    /// one object per row keyed by the interior heading's columns, the keys
    /// a drill of the document reads.
    fn interior_document(&mut self, rel: RelId, origin: Origin, heading: &Heading) -> Result<SqlExpr> {
        let graph = self.graph;
        let table = self.table(rel, &Enclosing::none())?;
        let mut args = Vec::new();
        for (i, _) in heading.displayed() {
            let key = self.interior_col(origin, heading, i)?;
            args.push(SqlExpr::PublishedNameLiteral(key));
            let document = self.kinds.position(graph, rel, i as u16) == super::kinds::Kind::Json;
            let admit = if document { Intrinsic::JsonSplice } else { Intrinsic::JsonScalar };
            args.push(self.out.intrinsic(admit, vec![SqlExpr::Column(table.cols[i])]));
        }
        let object = self.out.function("JSON_OBJECT", args);
        let concat = self.out.function("GROUP_CONCAT", vec![object, SqlExpr::Literal(LiteralValue::String(",".to_string()))]);
        let document = self.out.function(
            "JSON",
            vec![SqlExpr::Binary {
                left: Box::new(SqlExpr::Binary {
                    left: Box::new(SqlExpr::Literal(LiteralValue::String("[".to_string()))),
                    op: BinaryOperator::Concatenate,
                    right: Box::new(concat),
                }),
                op: BinaryOperator::Concatenate,
                right: Box::new(SqlExpr::Literal(LiteralValue::String("]".to_string()))),
            }],
        );
        let empty = self.out.function("JSON", vec![SqlExpr::Literal(LiteralValue::String("[]".to_string()))]);
        let value = self.out.function("COALESCE", vec![document, empty]);
        let at = self.out.scope(None);
        let col = self.out.column(at, None);
        let query = self.select_at(at, vec![SelectItem::expression_with_alias(value, col)], vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
        Ok(SqlExpr::Subquery(Box::new(query)))
    }

    /// The staged source's columns holding the marked occurrence's row
    /// locator, part by part: the positions its arrival names.
    fn arrived(&self, staging: &Staging, arrival: &Arrival) -> Result<Vec<ColId>> {
        arrival
            .positions()
            .into_iter()
            .map(|at| {
                staging
                    .cols
                    .get(at)
                    .copied()
                    .ok_or_else(|| self.contract("an arrival past its source's staged columns"))
            })
            .collect()
    }

    /// A stored row matched to a staged row: each part of the target's
    /// locator equal to the staged one.
    fn locates(&self, target_locator: &[ColId], staged_locator: &[ColId]) -> Vec<SqlExpr> {
        target_locator
            .iter()
            .zip(staged_locator)
            .map(|(t, s)| SqlExpr::Binary {
                left: Box::new(SqlExpr::Column(*t)),
                op: BinaryOperator::Equal,
                right: Box::new(SqlExpr::Column(*s)),
            })
            .collect()
    }

    /// `SELECT 1 FROM staging WHERE target_locator = staged_locator`.
    fn matched(&mut self, staging: &Staging, target_locator: &[ColId], staged_locator: &[ColId]) -> Result<facade::QueryExpression> {
        let at = self.out.scope(None);
        let conditions = self.locates(target_locator, staged_locator);
        self.select_at(at, Vec::new(), vec![TableExpression::Scope(staging.scope)], conditions, None, Vec::new(), None, false)
    }

    /// The check a replacement asks for: no stored row is reached by two
    /// source rows.
    fn one_row_per_locator(&mut self, staging: &Staging, arrival: &Arrival) -> Result<Pending> {
        let staged_locator = self.arrived(staging, arrival)?;
        let grouped_at = self.out.scope(None);
        let grouped = self.out.select(facade::Select {
            at: grouped_at,
            distinct: false,
            items: vec![SelectItem::scaffolding_value(SqlExpr::Literal(integer(1)), self.out.scaffolding())],
            from: vec![TableExpression::Scope(staging.scope)],
            filter: None,
            group_by: Some(staged_locator.into_iter().map(SqlExpr::Column).collect()),
            having: Some(SqlExpr::Binary {
                left: Box::new(self.out.function("count", vec![SqlExpr::Star])),
                op: BinaryOperator::GreaterThan,
                right: Box::new(SqlExpr::Literal(integer(1))),
            }),
            order_by: Vec::new(),
            limit: None,
        })?;
        let verdict_at = self.out.scope(None);
        let verdict = self.select_at(verdict_at, Vec::new(), Vec::new(), vec![SqlExpr::not_exists(grouped)], None, Vec::new(), None, false)?;
        let refusal: delightql_types::DelightQLError = delightql_types::diagnostic::DmlShape::UpdateAmbiguousSource {
            message: "update! refuses: the source offers more than one row for a row of the relation being \
                      updated, so the statement does not say what that row becomes."
                .to_string(),
        }
        .into();
        Ok(Pending::Check(
            Part::Sql(SqlStatement::Query {
                with_clause: None,
                query: verdict,
            }),
            refusal,
        ))
    }

    /// The write of a stored table's rows from the staged source.
    fn row_write(&mut self, target: RelId, rows: &RowWrite, staging: &Staging) -> Result<SqlStatement> {
        let (entity, target_scope, target_cols) = self.target_read(target)?;
        match rows {
            RowWrite::Insert { columns } => {
                let at = self.out.scope(None);
                let mut items = Vec::new();
                let mut written = Vec::new();
                for (t, s) in columns {
                    let col = self.out.column(at, None);
                    items.push(SelectItem::expression_with_alias(SqlExpr::Column(staging.cols[*s as usize]), col));
                    written.push(target_cols[*t as usize]);
                }
                self.received(&items, super::Received::Inserted)?;
                let source = self.select_at(at, items, vec![TableExpression::Scope(staging.scope)], Vec::new(), None, Vec::new(), None, false)?;
                Ok(SqlStatement::Insert {
                    target: RelationTarget::Entity(entity),
                    target_scope,
                    columns: written,
                    with_clause: None,
                    source,
                })
            }
            RowWrite::Replace { arrival, columns } => {
                let target_locator = self.target_locator(target, arrival)?;
                let staged_locator = self.arrived(staging, arrival)?;
                let mut set_clause = Vec::new();
                for (t, s) in columns {
                    let at = self.out.scope(None);
                    let item = SelectItem::scaffolding_value(SqlExpr::Column(staging.cols[*s as usize]), self.out.scaffolding());
                    let conditions = self.locates(&target_locator, &staged_locator);
                    let read = self.select_at(at, vec![item], vec![TableExpression::Scope(staging.scope)], conditions, None, Vec::new(), None, false)?;
                    set_clause.push((target_cols[*t as usize], SqlExpr::Subquery(Box::new(read))));
                }
                let reached = self.matched(staging, &target_locator, &staged_locator)?;
                Ok(SqlStatement::Update {
                    target: RelationTarget::Entity(entity),
                    target_scope,
                    with_clause: None,
                    set_clause,
                    where_clause: Some(SqlExpr::exists(reached)),
                })
            }
            RowWrite::Remove { arrival } => {
                let target_locator = self.target_locator(target, arrival)?;
                let staged_locator = self.arrived(staging, arrival)?;
                let reached = self.matched(staging, &target_locator, &staged_locator)?;
                Ok(SqlStatement::Delete {
                    target: RelationTarget::Entity(entity),
                    target_scope,
                    with_clause: None,
                    where_clause: Some(SqlExpr::exists(reached)),
                })
            }
        }
    }

    /// The target table as a statement reads it: its entity, its scope, and
    /// one column per stored column.
    fn target_read(&mut self, target: RelId) -> Result<(EntityId, ScopeId, Vec<ColId>)> {
        let key = self.target_key(target)?;
        if let Some(t) = self.targets.get(&key) {
            return Ok((t.entity, t.scope, t.cols.clone()));
        }
        let RelKind::Read {
            source: ReadSource::Catalog {
                name, columns, physical, ..
            },
            ..
        } = self.graph.rel(target).kind()
        else {
            return Err(self.contract("a row write whose target is no stored table"));
        };
        let entity = self.out.entity(name, physical.schema.as_deref());
        let scope = self.out.table_scope(entity, name);
        let cols: Vec<ColId> = columns.iter().map(|c| self.out.column(scope, Some(&c.name))).collect();
        self.targets.insert(
            key,
            Target {
                entity,
                scope,
                cols: cols.clone(),
                locator: None,
            },
        );
        Ok((entity, scope, cols))
    }

    /// The target's columns holding each row's locator, part by part, as
    /// the marked read whose locator `arrival` carries decided the table is
    /// reached.
    fn target_locator(&mut self, target: RelId, arrival: &Arrival) -> Result<Vec<ColId>> {
        let (_, scope, cols) = self.target_read(target)?;
        let key = self.target_key(target)?;
        if let Some(found) = self.targets.get(&key).and_then(|t| t.locator.clone()) {
            return Ok(found);
        }
        let found = arrival
            .locator()
            .into_iter()
            .map(|part| self.located(part, scope, &cols))
            .collect::<Result<Vec<ColId>>>()?;
        if let Some(t) = self.targets.get_mut(&key) {
            t.locator = Some(found.clone());
        }
        Ok(found)
    }

    /// A written table's key among the program's targets: its name and the
    /// schema qualifying it.
    fn target_key(&self, target: RelId) -> Result<(String, Option<String>)> {
        match self.graph.rel(target).kind() {
            RelKind::Read {
                source: ReadSource::Catalog { name, physical, .. },
                ..
            } => Ok((name.to_string(), physical.schema.clone())),
            _ => Err(self.contract("a row write whose target is no stored table")),
        }
    }

    /// A receipt as a FROM item: its staged row.
    pub(super) fn receipt(&mut self, receipt: RelId) -> Result<Table> {
        let staging = self
            .receipts
            .get(&receipt)
            .cloned()
            .ok_or_else(|| self.contract("a receipt read before its act's step"))?;
        self.scratch_table(&staging)
    }

    /// The one connection the program runs on: the connection every
    /// catalog read of the statement is served by, and every object it
    /// creates is placed on; the user connection when it names none. A
    /// connection whose transport cannot surface a statement's failure runs
    /// no program.
    fn program_connection(&self, acts: &[RelId]) -> Result<i64> {
        let mut connections = self.read_connections();
        for r in acts {
            if let RelKind::Receipt { act, .. } = self.graph.rel(*r).kind() {
                if let Write::Object(created) = act.write() {
                    connections.push(created.placement.connection());
                }
            }
        }
        connections.sort_unstable();
        connections.dedup();
        let connection = self.one_connection(&connections)?;
        self.out.program_runs_on(connection)?;
        Ok(connection)
    }
}


/// Whether a statement writes a stored table's rows.
fn is_write(statement: &SqlStatement) -> bool {
    matches!(
        statement,
        SqlStatement::Insert {
            target: RelationTarget::Entity(_),
            ..
        } | SqlStatement::Update { .. }
            | SqlStatement::Delete { .. }
    )
}
