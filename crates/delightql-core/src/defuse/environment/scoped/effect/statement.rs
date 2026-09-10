// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! STATEMENT COMPILATION: a pure statement or value of this walk is
//! RESOLVED here, in the world the walk stands in, and the planner is
//! handed the resolved query to refine, lower and record. Guards are this
//! walk's syntax too, so their SQL is derived here and only interned there.

use super::walk::value_contains_witness;
use super::{EffectWalk, GuardSource, WalkCtx};
use crate::error::Result;
use crate::pipeline::ast_unresolved::{Chain, Continuation, GroundMention, Query, Relation};
use crate::pipeline::asts::core::Access;
use crate::pipeline::compiled_query;
use crate::pipeline::effect_transformer::{
    select_one_from, unsupported, CompiledStmt, CompiledText, DeferredSql, MarkedStepKind,
    PendingPlanStatement, ReceiptGate, ReceiptShape, ValueQe,
};
use crate::pipeline::resolver;
use crate::pipeline::sql_ast::{DomainExpression as SqlExpr, QueryExpression, SqlStatement};

impl<'p, 'a> EffectWalk<'p, 'a> {
    /// Phases 2–4 over one statement, resolved in the CURRENT lexical
    /// world — the innermost open rule body, or the plan's session world —
    /// with the plan's own creations registered as that world's
    /// materialized relations → refine → address → transformer.
    /// Definitions are spent at their call sites during resolution.
    pub(super) fn compile_statement(
        &mut self,
        ctx: &WalkCtx<'_>,
        query: Query,
    ) -> Result<CompiledStmt> {
        self.compile_statement_with(ctx, query, false, None)
    }

    pub(super) fn compile_rule_application(
        &mut self,
        ctx: &WalkCtx<'_>,
        query: Query,
        formal: delightql_types::SqlIdentifier,
        value: crate::defuse::ho::RuleValueId,
    ) -> Result<CompiledStmt> {
        self.compile_statement_with(ctx, query, false, Some((formal, value)))
    }

    /// `serve_bootstrap`: compile as a MATERIALIZATION SOURCE
    /// (materialization-law §2) — bootstrap reads are served as literal
    /// snapshots during resolution, so connection 1 never enters the
    /// attribution set and the zero/one/many judgment below IS the ruled
    /// attribution: zero → primary, one → that connection, more → the
    /// ordinary federation refusal.
    pub(super) fn compile_statement_with(
        &mut self,
        ctx: &WalkCtx<'_>,
        query: Query,
        serve_bootstrap: bool,
        rule_value: Option<(
            delightql_types::SqlIdentifier,
            crate::defuse::ho::RuleValueId,
        )>,
    ) -> Result<CompiledStmt> {
        let world = ctx.world;
        if !self.plan.discovering() {
            return self.plan.replay_statement(serve_bootstrap);
        }
        // Every statement here is a REPLAY — an instantiated body or a
        // compiler-built query — so the authored-environment judgments stay
        // with the submission that authored them.
        let mut config = resolver::ResolutionConfig {
            authored_environment: false,
            ..self.plan.config().clone()
        };
        if serve_bootstrap && !cfg!(target_arch = "wasm32") {
            config.serve_bootstrap_reads = true;
        }
        // THE PLAN'S OWN CREATIONS are program state: they register into a
        // PROGRAM world and never into a consulted body — a rule body reads
        // a plan creation only through an explicit actual.
        for (name, note) in self.plan.notes() {
            world.register_materialized(delightql_types::SqlIdentifier::new(name.clone()), *note);
        }
        // THE SYNTAX IS RESOLVED HERE, in the world this walk stands in.
        // What the planner receives is the resolved statement and its
        // connection attribution — never the syntax, never the world.
        let (resolved, connection_id) = {
            let mut registry = self.plan.resolver_core()?;
            let resolved = match rule_value {
                Some((formal, value)) => world.resolve_query_with_rule_value(
                    &mut registry,
                    config,
                    query,
                    formal,
                    value,
                )?,
                None => world.resolve_query(&mut registry, config, query)?,
            }
            .into_query();
            (resolved, registry.validate_single_connection()?)
        };
        self.plan
            .plan_resolved(resolved, serve_bootstrap, connection_id)
    }

    /// Compile a PURE (already-walked) value expression to SQL text.
    /// Handles the signed witness compositionally; everything
    /// else takes the ordinary chain.
    pub(super) fn compile_value_text(
        &mut self,
        expr: &Chain,
        ctx: &WalkCtx,
    ) -> Result<CompiledText> {
        if value_contains_witness(expr) {
            let value = self.compile_value_qe(expr, ctx)?;
            let stmt = SqlStatement::Query {
                with_clause: None,
                query: value.query,
            };
            let sql = self.plan.finish_statement(&stmt)?;
            return Ok(CompiledText {
                sql,
                columns: value.columns,
                ports: value.ports,
                relation: value.relation,
                connection_id: value.connection_id,
            });
        }
        let compiled = self.compile_statement(ctx, ctx.pure_query(expr.clone()))?;
        match &compiled.stmt {
            SqlStatement::Query { .. } => {}
            _ => {
                return Err(unsupported(
                    "an effect body's value position compiled to a non-SELECT".to_string(),
                ))
            }
        }
        let sql = self.plan.finish_statement(&compiled.stmt)?;
        Ok(CompiledText {
            sql,
            columns: compiled.columns,
            ports: compiled.ports,
            relation: compiled.relation,
            connection_id: compiled.connection_id,
        })
    }

    /// Compiled COMPOSITIONALLY over the AST as parsed:
    /// `V +-` is the LEFT JOIN preserved from the one-row unit (DEE) over
    /// V's compiled SELECT; a union of values aligns by corresponding
    /// columns (SQLite UNION ALL is positional; the compiler knows every
    /// schema). Stacked witnesses each carry a `met` column, and those are
    /// republished through the registry like any other — so the ambiguity
    /// they make is arbitrated where every other one is, and this function
    /// spells nothing.
    ///
    /// BINDING: a trailing postfix operator binds the ACCUMULATED union — the
    /// language's one uniform rule; per-arm scoping is spelled interior
    /// (`s!(+-)`). This function lowers whatever shape the
    /// parser hands it, which is now correct by construction: the
    /// interior spelling produces per-arm witnesses (pinned by the
    /// torture capstone's per-arm ledger assertions), the exterior
    /// spelling produces the stacked union witness both docs now
    /// describe.
    #[stacksafe::stacksafe]
    pub(super) fn compile_value_qe(&mut self, expr: &Chain, ctx: &WalkCtx) -> Result<ValueQe> {
        match expr
            .split_last()
            .map(|(step, prefix)| (step.form(), prefix))
        {
            Some((
                Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                    form: crate::pipeline::asts::core::StructuralForm::SignedWitness,
                    ..
                }),
                prefix,
            )) => {
                let inner = self.compile_value_qe(&prefix.to_chain(), ctx)?;
                self.plan.witness_wrap(inner)
            }
            Some((Continuation::BagOp { arm, .. }, prefix)) if value_contains_witness(expr) => {
                let arms: Vec<ValueQe> = vec![
                    self.compile_value_qe(&prefix.to_chain(), ctx)?,
                    self.compile_value_qe(arm, ctx)?,
                ];
                self.plan.union_corresponding_qes(arms)
            }
            _ => {
                let other = expr;
                let compiled = self.compile_statement(ctx, ctx.pure_query(other.clone()))?;
                let query = match compiled.stmt {
                    SqlStatement::Query { with_clause, query } => match with_clause {
                        Some(ctes) => QueryExpression::WithCte {
                            ctes,
                            query: Box::new(query),
                        },
                        None => query,
                    },
                    _ => {
                        return Err(unsupported(
                            "a value position compiled to a non-SELECT".to_string(),
                        ))
                    }
                };
                Ok(ValueQe {
                    query,
                    columns: compiled.columns,
                    ports: compiled.ports,
                    relation: compiled.relation,
                    connection_id: compiled.connection_id,
                })
            }
        }
    }

    pub(super) fn guard_from_value(&self, expr: &Chain) -> GuardSource {
        if let (
            Some(Relation::Ground {
                mention: GroundMention::Scratch { row },
                outer: false,
                ..
            }),
            Some(Access::All),
        ) = (expr.as_read_relation(), expr.head_access())
        {
            return GuardSource::Table(row.relation());
        }
        GuardSource::Expr {
            body: Box::new(expr.clone()),
        }
    }

    pub(super) fn guard_to_sql(
        &mut self,
        ctx: &WalkCtx<'_>,
        guard: &GuardSource,
    ) -> Result<SqlExpr> {
        match guard {
            GuardSource::Table(t) => Ok(SqlExpr::exists(select_one_from(*t, &self.plan.names())?)),
            GuardSource::Expr { body } => {
                let compiled = self.compile_statement(ctx, ctx.pure_query((**body).clone()))?;
                match compiled.stmt {
                    SqlStatement::Query { with_clause, query } => {
                        let qe = match with_clause {
                            Some(ctes) => QueryExpression::WithCte {
                                ctes,
                                query: Box::new(query),
                            },
                            None => query,
                        };
                        Ok(SqlExpr::exists(qe))
                    }
                    _ => Err(unsupported(
                        "a guard conjunct compiled to a non-SELECT".to_string(),
                    )),
                }
            }
        }
    }

    /// The gates a data statement carries: the context's EXISTS guards
    /// and — when armed — the exit guard.
    /// `include_exit` is false for positions that handle exit separately.
    pub(super) fn gate_exprs(&mut self, ctx: &WalkCtx, include_exit: bool) -> Result<Vec<SqlExpr>> {
        let guards = ctx.guards.clone();
        let mut out = Vec::with_capacity(guards.len() + 1);
        for g in &guards {
            out.push(self.guard_to_sql(ctx, g)?);
        }
        if include_exit && self.plan.exit_armed() {
            out.push(self.plan.exit_gate());
        }
        Ok(out)
    }

    /// The receipt table an emission writes: the rule's shared sink
    /// when present, else a fresh per-directive table named after the
    /// enclosing arm label.
    pub(super) fn receipt_table_for(
        &mut self,
        ctx: &WalkCtx,
        shape: &ReceiptShape,
    ) -> Result<crate::relation::ScratchRow> {
        if let Some(sink) = &ctx.sink {
            return Ok(sink.table);
        }
        self.plan
            .alloc_receipt_shell_named(&shape.columns(), &shape.scratch_name)
    }

    /// THE REQUIREMENT EDGES A STEP CARRIES: one PRESENT edge per
    /// enclosing guard, and an ABSENT edge on the exit latch when exit was
    /// armed before the step. A guard is this walk's own syntax standing in
    /// its world, so it is lowered here; the planner interns only the SQL.
    pub(super) fn step_requirements(
        &mut self,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<Vec<compiled_query::Requirement>> {
        use compiled_query::{GuardPolarity, Requirement};
        let mut requirements = Vec::new();
        if let Some(ctx) = ctx {
            let sources = ctx.guards.clone();
            for g in &sources {
                let expr = self.guard_to_sql(ctx, g)?;
                let sql = self.plan.render_guard_select(expr)?;
                let guard_id = self.plan.guard_def_id(sql);
                // Two comma conjuncts can intern to one guard definition.
                // Deduplicate their requirement edges.
                if !requirements
                    .iter()
                    .any(|r: &Requirement| r.guard_id == guard_id)
                {
                    requirements.push(Requirement {
                        guard_id,
                        polarity: GuardPolarity::Present,
                        reason: "comma",
                    });
                }
            }
        }
        if exit_armed_before {
            requirements.push(self.plan.exit_requirement()?);
        }
        Ok(requirements)
    }

    /// Close a non-terminal step over the entries its handler emitted. The
    /// requirements are derived here, from this walk's guards; the planner
    /// owns the entries and the occurrence.
    pub(super) fn mark_step(
        &mut self,
        kind: MarkedStepKind,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        if !self.plan.has_pending_entries() {
            return Ok(());
        }
        let requirements = self.step_requirements(ctx, exit_armed_before)?;
        let path = self.rule_stack.join("::");
        self.plan.mark_step(kind, bare, &path, requirements)
    }

    /// Construct graceful completion as a terminal value before assembly.
    pub(super) fn mark_exit_step(
        &mut self,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        let requirements = self.step_requirements(ctx, exit_armed_before)?;
        let path = self.rule_stack.join("::");
        self.plan.mark_exit_step(&path, requirements)
    }

    /// Construct erroneous completion in one act.
    pub(super) fn mark_abort_step(
        &mut self,
        probe: PendingPlanStatement,
        provenance: compiled_query::AbortProvenance,
        bare: &str,
        ctx: Option<&WalkCtx>,
        exit_armed_before: bool,
    ) -> Result<()> {
        let requirements = self.step_requirements(ctx, exit_armed_before)?;
        let path = self.rule_stack.join("::");
        self.plan
            .mark_abort_step(probe, provenance, bare, &path, requirements)
    }

    /// A receipt insert carries this walk's gates: the context's EXISTS
    /// guards and, when armed, the exit guard — lowered here, emitted there.
    pub(super) fn emit_receipt_insert(
        &mut self,
        table: crate::relation::SemanticRelation,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let gates = self.gate_exprs(ctx, true)?;
        self.plan
            .emit_receipt_insert(table, shape, gate, gates, ctx.sink.is_some())
    }

    pub(super) fn build_receipt_insert_sql(
        &mut self,
        table: crate::relation::SemanticRelation,
        shape: &ReceiptShape,
        gate: ReceiptGate,
        ctx: &WalkCtx,
    ) -> Result<DeferredSql> {
        let gates = self.gate_exprs(ctx, true)?;
        self.plan
            .build_receipt_insert_sql(table, shape, gate, gates, ctx.sink.is_some())
    }
}
