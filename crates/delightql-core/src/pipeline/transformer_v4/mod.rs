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
//!
//! See TRANSFORMER-V4-SKETCH.md for the full design and
//! NODE-TRANSFORMATION-PRINCIPLES.md for the principles.

pub mod builder;
mod descend;
mod dml;
mod relational;
mod scalar;
mod tree_group;

use crate::error::Result;
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::asts::core::expressions::relational::RelationalExpression;
use crate::pipeline::asts::core::operators::UnaryRelationalOperator;
use crate::pipeline::sql_ast_v3::{QueryExpression, SqlStatement};

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
    /// CFE definitions from `WithPrecompiledCfes` query wrappers.
    /// Looked up by name during scalar function lowering.
    pub(super) cfes: Vec<ast_addressed::PrecompiledCfeDefinition>,
    /// Shared name generator. Interior-mutable (Arc<AtomicUsize>), so
    /// cloning is cheap and all paths share the same counter. Scalar
    /// lowering uses this for subquery descent (InnerExists, ScalarSubquery).
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
    /// Look up a CFE by name. Used by `try_expand_cfe` in scalar.rs.
    pub(super) fn lookup_function(
        &self,
        name: &str,
    ) -> Option<&ast_addressed::PrecompiledCfeDefinition> {
        self.cfes.iter().find(|def| def.name == name)
    }

    /// Create a child context for a correlated subquery.
    ///
    /// The outer scope's columns are captured so that `s_lower_lvar` can
    /// resolve correlated references without a caller-side passthrough.
    pub(super) fn with_outer_scope(
        &self,
        columns: Vec<crate::pipeline::asts::core::ColumnMetadata>,
    ) -> TransformCtx {
        TransformCtx {
            cfes: self.cfes.clone(),
            names: self.names.fork(),
            outer_columns: columns,
            danger_gates: self.danger_gates.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// DML detection
// ---------------------------------------------------------------------------

/// Check whether a query's innermost relational expression ends with DmlTerminal.
#[stacksafe::stacksafe]
fn has_dml_terminal(query: &ast_addressed::Query) -> bool {
    let expr = match query {
        ast_addressed::Query::Relational(expr) => expr,
        ast_addressed::Query::WithCtes { query: expr, .. } => expr,
        ast_addressed::Query::WithPrecompiledCfes { query, .. } => return has_dml_terminal(query),
        ast_addressed::Query::ReplTempTable { query, .. } => return has_dml_terminal(query),
        ast_addressed::Query::ReplTempView { query, .. } => return has_dml_terminal(query),
        _ => return false,
    };
    // The outermost Pipe node holds the last operator in the chain.
    matches!(
        expr,
        RelationalExpression::Pipe(pipe) if matches!(pipe.operator, UnaryRelationalOperator::DmlTerminal { .. })
    )
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Entry point: lower an addressed `Query` into a `SqlStatement`.
///
/// Detects DML terminals and routes to the DML path; otherwise produces
/// `SqlStatement::Query` via the normal transform path.
pub(crate) fn transform(query: ast_addressed::Query, ctx: &TransformCtx) -> Result<SqlStatement> {
    if has_dml_terminal(&query) {
        dml::transform_dml(query, ctx)
    } else {
        let qe = transform_with_names(query, &ctx.names, ctx)?;
        Ok(SqlStatement::Query {
            with_clause: None,
            query: qe,
        })
    }
}

/// Inner transform that accepts an existing `NameGenerator`.
///
/// Used by `r_lower_consulted_view` so that view body lowering shares the
/// caller's name counter (no alias collisions between view internals and
/// the outer query).
pub(super) fn transform_with_names(
    query: ast_addressed::Query,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<QueryExpression> {
    match query {
        ast_addressed::Query::Relational(expr) => {
            descend::descend_as_final(expr, names, ctx)?.to_sql()
        }

        ast_addressed::Query::WithCtes { ctes, query: expr } => {
            let sql_ctes: Vec<crate::pipeline::sql_ast_v3::Cte> = ctes
                .into_iter()
                .map(|binding| relational::lower_cte_binding(binding, names, ctx))
                .collect::<Result<_>>()?;

            // Lower the main query
            let main_query = descend::descend_as_final(expr, names, ctx)?.to_sql()?;

            // Wrap in WITH clause, merging if main_query already has CTEs
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

        ast_addressed::Query::WithPrecompiledCfes { cfes, query } => {
            // Extend CFE registry for the inner query scope.
            let mut all_cfes = ctx.cfes.clone();
            all_cfes.extend(cfes);
            let ctx_with_cfes = TransformCtx {
                cfes: all_cfes,
                names: names.fork(),
                outer_columns: vec![],
                danger_gates: ctx.danger_gates.clone(),
            };
            transform_with_names(*query, names, &ctx_with_cfes)
        }

        ast_addressed::Query::ReplTempTable { query, table_name } => {
            // REPL temp table: lower the inner query, the REPL layer handles
            // the CREATE TEMP TABLE wrapper.
            let _ = table_name;
            transform_with_names(*query, names, ctx)
        }

        ast_addressed::Query::ReplTempView { query, .. } => {
            transform_with_names(*query, names, ctx)
        }

        // WithCfes and WithErContext are unresolved-only — unreachable.
        _ => unreachable!("Unresolved-only Query variant reached transformer v4"),
    }
}
