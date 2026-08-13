// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Transformer V4: Unified Builder Architecture
//!
//! Clean-slate transformer built around a unified `Builder<P>` that carries
//! both SQL structure and scope. The phantom type `P` (`Unprojected` or
//! `Projected`) enforces at compile time that `to_sql()` is only callable
//! after a projection has been set.
//!
//! # Module layout
//!
//! - `builder/` — `Builder<P>` (the single type), state machine, name generator
//! - `descend` — recursive descent dispatcher (`descend`, `descend_as_final`)
//! - `dml` — DML terminal operators (DELETE, UPDATE, INSERT)
//! - `relational` — `r_lower_*` handlers (relational algebra → builder ops)
//! - `scalar` — `s_lower_*` handlers (AST scalar expressions → SQL expressions)

mod anchors;
pub mod builder;
mod descend;
mod dml;
mod plan;
mod relational;
mod scalar;
mod tree_group;

pub use plan::Mutation;


use crate::error::Result;
use crate::pipeline::asts::refined as ast_refined;
use crate::pipeline::sql_ast::{QueryExpression, SqlStatement};

use builder::NameGenerator;

// ---------------------------------------------------------------------------
// Transform context
// ---------------------------------------------------------------------------

/// Context available to all lowering functions.
///
/// Carries query-scoped information needed by handlers (CFE definitions,
/// name generator, backend dialect, option flags, etc.). Immutable — state
/// passed DOWN. The builder carries state passed UP.
///
/// Only holds entities declared *in the query text* (CFEs). DDL entities
/// arrive pre-resolved in the AST: views as `ConsultedView` nodes with
/// the body inlined, functions pre-expanded by the resolver.
pub(crate) struct TransformCtx {
    /// The compilation's append-only scope and column identity arena.
    pub(super) identities: std::rc::Rc<crate::names::Registry>,
    /// Shared name generator. Interior-mutable (Arc<AtomicUsize>), so
    /// cloning is cheap and all paths share the same counter. Scalar
    /// lowering uses this for subquery descent (InnerExists, a scalarized relation).
    pub(super) names: NameGenerator,
    /// Outer scope columns for correlated subqueries.
    ///
    /// When entering a scalar subquery, the enclosing scope's columns are
    /// snapshotted here. `s_lower_lvar` checks this on inner-scope miss,
    /// so correlated references (e.g., `users.id` inside an orders subquery)
    /// resolve through the same qualify logic as everything else — no
    /// caller-side passthrough needed.
    pub(super) outer_columns: Vec<crate::pipeline::asts::core::ColumnMetadata>,
    /// Danger gates for this query (controls opt-in behaviors like min_multiplicity).
    pub(crate) danger_gates: crate::pipeline::danger_gates::DangerGateMap,
}

impl TransformCtx {
    /// Create a child context for a correlated subquery.
    ///
    /// The outer scope's columns are captured so that `s_lower_lvar` can
    /// resolve correlated references without a caller-side passthrough.
    pub(super) fn with_outer_scope(
        &self,
        columns: Vec<crate::pipeline::asts::core::ColumnMetadata>,
    ) -> TransformCtx {
        TransformCtx {
            identities: std::rc::Rc::clone(&self.identities),
            names: self.names.fork(),
            outer_columns: columns,
            danger_gates: self.danger_gates.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// DML detection
// ---------------------------------------------------------------------------

/// Whether a call is a mutation.
///
/// The registry descriptor answers, because after resolution there is no
/// string to match: a callable carries the category it was minted with, and
/// a name comparison here would be a second authority that can disagree with
/// the one the enforcement road already used.
pub(super) fn is_mutation_call(call: &ast_refined::SealedCall, ctx: &TransformCtx) -> bool {
    matches!(
        ctx.identities.callable_category(call.call().callee),
        Some(crate::names::CallableCategory::Dml(_))
    )
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// One statement and what it may not run without.
///
/// An obligation is a read that decides whether the statement is allowed to
/// happen — evaluated before it, refusing the run when it does not hold. It
/// is a statement in its own right rather than a clause folded into the one
/// it guards, because a clause could only make the mutation match no rows,
/// and "nothing happened" is not the same answer as "this was refused".
pub(crate) struct Lowered {
    pub(crate) statement: SqlStatement,
    pub(crate) obligations: Vec<Obligation>,
    /// Statements that must run, in order, before the obligations and the
    /// statement.
    ///
    /// A mutation stages the relation it reads before anyone reads it, so
    /// the check and the mutation see the SAME rows. Two evaluations of one
    /// source are two relations whenever the source is volatile, external,
    /// or concurrently written — and then the check proves something about
    /// a relation the mutation never consumed.
    pub(crate) prepare: Vec<SqlStatement>,
    /// The temporary relations `prepare` creates, for the road to remove
    /// when it is done with them.
    pub(crate) staged: Vec<crate::names::ScopeId>,
}

/// A read the statement may not run without, and what its failure means.
///
/// The refusal travels with the check because they are one decision: the
/// road that runs the check is the road that must report it, and a message
/// written where the check is consumed would be a second authority on what
/// the check is for.
pub(crate) struct Obligation {
    pub(crate) statement: SqlStatement,
    pub(crate) refusal: crate::pipeline::compiled_query::Refusal,
}

impl Lowered {
    /// The statement, for a road that executes statements alone.
    ///
    /// A caller with nowhere to run an obligation may not have one: dropping
    /// it is the exact failure the obligation exists to prevent, so the road
    /// refuses instead of quietly proceeding. Every such caller today lowers
    /// something that cannot be a mutation, and this is what keeps that true
    /// as the pipeline changes.
    pub(crate) fn without_obligations(self) -> Result<SqlStatement> {
        if self.obligations.is_empty() && self.prepare.is_empty() {
            return Ok(self.statement);
        }
        Err(crate::error::DelightQLError::validation_error_categorized(
            "dml/plan/unrunnable_obligation",
            "this statement may not run without a check that only the effect \
             plan can perform, and this road executes statements alone",
            "run the mutation as a query rather than compiling it to SQL",
        ))
    }
}

impl From<SqlStatement> for Lowered {
    fn from(statement: SqlStatement) -> Self {
        Lowered {
            statement,
            obligations: Vec::new(),
            prepare: Vec::new(),
            staged: Vec::new(),
        }
    }
}

/// Entry point: lower a refined `Query` into a statement and its
/// obligations.
///
/// Detects DML terminals and routes to the DML path; otherwise produces
/// `SqlStatement::Query` via the normal transform path.
pub(crate) fn transform(query: ast_refined::Query, ctx: &TransformCtx) -> Result<Lowered> {
    match dml::transform_dml(query, ctx) {
        Ok(lowered) => lowered,
        Err(query) => {
            let qe = transform_with_names(query, &ctx.names, ctx)?;
            Ok(SqlStatement::Query {
                with_clause: None,
                query: qe,
            }
            .into())
        }
    }
}

/// Inner transform that accepts an existing `NameGenerator`.
///
/// Used by `r_lower_consulted_view` so that view body lowering shares the
/// caller's name counter (no alias collisions between view internals and
/// the outer query).
pub(super) fn transform_with_names(
    query: ast_refined::Query,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<QueryExpression> {
    let ast_refined::Query { cfes: (), ctes, body } = query;
    let sql_ctes: Vec<crate::pipeline::sql_ast::Cte> = ctes
        .into_iter()
        .map(|binding| relational::lower_cte_binding(binding, names, ctx))
        .collect::<Result<_>>()?;

    // Lower the body
    let main_query = descend::descend_as_final(body, names, ctx)?.to_sql()?;

    // Wrap in WITH clause, merging if the body already has CTEs
    if sql_ctes.is_empty() {
        Ok(main_query)
    } else {
        match main_query {
            QueryExpression::WithCte {
                ctes: inner_ctes,
                query: inner_query,
            } => {
                let mut merged = sql_ctes;
                merged.extend(inner_ctes);
                Ok(QueryExpression::WithCte {
                    ctes: merged,
                    query: inner_query,
                })
            }
            other => Ok(QueryExpression::WithCte {
                ctes: sql_ctes,
                query: Box::new(other),
            }),
        }
    }
}
