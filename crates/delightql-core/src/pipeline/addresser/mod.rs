// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::Result;
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::phase_box::PhaseBoxable;
use crate::pipeline::asts::core::Refined;
use crate::pipeline::{ast_addressed, ast_refined};
use delightql_types::SqlIdentifier;

pub fn address_query(query: ast_refined::Query) -> Result<ast_addressed::Query> {
    address_query_inner(query)
}

fn address_query_inner(query: ast_refined::Query) -> Result<ast_addressed::Query> {
    match query {
        ast_refined::Query::WithCtes { ctes, query } => {
            let addressed_ctes = ctes
                .into_iter()
                .map(|cte| {
                    let is_recursive = expression_references_name(&cte.expression, &cte.name);
                    let addressed_expr: ast_addressed::RelationalExpression = cte.expression.into();
                    ast_addressed::CteBinding {
                        expression: addressed_expr,
                        name: cte.name,
                        effect_label: cte.effect_label,
                        is_recursive: is_recursive.new(),
                    }
                })
                .collect();
            Ok(ast_addressed::Query::WithCtes {
                ctes: addressed_ctes,
                query: query.into(),
            })
        }
        ast_refined::Query::WithPrecompiledCfes { cfes, query } => {
            Ok(ast_addressed::Query::WithPrecompiledCfes {
                cfes,
                query: Box::new(address_query_inner(*query)?),
            })
        }
        ast_refined::Query::ReplTempTable { query, table_name } => {
            Ok(ast_addressed::Query::ReplTempTable {
                query: Box::new(address_query_inner(*query)?),
                table_name,
            })
        }
        ast_refined::Query::ReplTempView { query, view_name } => {
            Ok(ast_addressed::Query::ReplTempView {
                query: Box::new(address_query_inner(*query)?),
                view_name,
            })
        }
        // Plain relational query — no CTEs, just convert phase.
        ast_refined::Query::Relational(expr) => Ok(ast_addressed::Query::Relational(expr.into())),
        // These are consumed before the refined phase and should never reach the addresser.
        ast_refined::Query::WithCfes { .. } | ast_refined::Query::WithErContext { .. } => {
            unreachable!("WithCfes/WithErContext should be consumed before addressing")
        }
    }
}

// ---------------------------------------------------------------------------
// Recursive-CTE detection: does a CTE body reference its own name via a Ground
// relation ANYWHERE — including inside a predicate subquery (`Filter.condition`,
// IN/EXISTS/scalar), a pipe-operator argument, a consulted-view body, or a
// nested CTE — not only along the source spine?
//
// Rides the shared whole-tree closure `AstVisit<Refined>` (INDUCTIVE-TRAVERSAL-
// PLAN R-I1/R-I3), with an early `Break` on the first hit. The former
// hand-rolled walker matched `Filter { source, .. }` and dropped the recursive
// `condition` field (INDUCTIVE-INVENTORY §2a W7); the default `walk_visit_*`
// descent names every recursive edge once, so a self-reference in a subquery
// can no longer be silently ignored.
//
// The `is_recursive` flag produced here is ADVISORY: the SQL rewriter's
// `mark_recursive_ctes` (sql_rewriter/recursive_cte.rs) is the authoritative
// detector — it re-marks recursion structurally at the SQL level (descending
// into subqueries) and only ever ADDS the keyword, never removes it, then
// `validate_recursive_members` refuses a subquery-buried self-reference with
// N4 (`semantic/recursion/self_subquery`). So closing this hole cannot change
// the outcome of any legal query: a subquery-only self-reference is N4-illegal
// regardless, and the SQL-level marker already detects everything this walk now
// detects. Pinned by the recursion_contract ball staying 16/16
// (epic1/REPORT-INDUCTIVE-D-RISK-CANDIDATES.md, W7).
fn expression_references_name(expr: &ast_refined::RelationalExpression, name: &str) -> bool {
    let mut finder = NameReferenceFinder {
        name: SqlIdentifier::new(name),
        found: false,
    };
    walk_visit_relational(&mut finder, expr)
        .expect("recursive-CTE self-reference detection is infallible (hooks never return Err)");
    finder.found
}

/// Finds any `Ground` relation whose identifier equals `name`, anywhere in the
/// tree. The `AstVisit` default walk supplies the complete structural descent
/// (consulted-view bodies, inner-relation subqueries, predicate subqueries,
/// operator arguments); this only inspects the Ground leaf and stops early.
struct NameReferenceFinder {
    name: SqlIdentifier,
    found: bool,
}

impl AstVisit<Refined> for NameReferenceFinder {
    fn enter_relation(&mut self, rel: &ast_refined::Relation) -> Result<Descent> {
        if let ast_refined::Relation::Ground { identifier, .. } = rel {
            if identifier.name == self.name {
                self.found = true;
                return Ok(Descent::Break);
            }
        }
        Ok(Descent::Continue)
    }
}

// ---------------------------------------------------------------------------
// NOTE: the `_tg_N` tree-group CTE-naming walk (formerly `walk_*_for_tree_groups`
// here, INVENTORY §2a W8) was DELETED in Phase E. Its sole output,
// `CteRequirements.cte_name`, had no reader anywhere in transformer_v4 /
// generator_v3 (the transformer derives the tree-group column name from the
// resolver's `cpr_schema`, `transformer_v4/tree_group.rs`), and `_tg_` never
// appeared in a generated-SQL baseline — the assignment was vestigial. Deleting
// it is behavior-preserving (proven by the corpus being outcome-identical).
// Details: epic1/REPORT-INDUCTIVE-D-RISK-CANDIDATES.md (W8) and the Phase E
// report. NB: `system.rs::walk_relational_for_tree_groups` shares only the NAME
// — it registers `interior_entity` schemas (a live consumer) and is unrelated.
// ---------------------------------------------------------------------------
