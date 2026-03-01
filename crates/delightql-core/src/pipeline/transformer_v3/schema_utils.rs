// Schema Utilities Module
// Provides helper functions for extracting schemas from expressions

use crate::pipeline::ast_addressed;

/// Extract schema from a RelationalExpression
pub fn get_relational_schema(expr: &ast_addressed::RelationalExpression) -> ast_addressed::CprSchema {
    match expr {
        ast_addressed::RelationalExpression::Relation(rel) => match rel {
            ast_addressed::Relation::Ground { cpr_schema, .. }
            | ast_addressed::Relation::Anonymous { cpr_schema, .. }
            | ast_addressed::Relation::TVF { cpr_schema, .. }
            | ast_addressed::Relation::InnerRelation { cpr_schema, .. } => cpr_schema.get().clone(),
            ast_addressed::Relation::ConsultedView { scoped, .. } => scoped.get().schema().clone(),

            ast_addressed::Relation::PseudoPredicate { .. } => {
                panic!(
                    "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                     Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                )
            }
        },
        ast_addressed::RelationalExpression::Filter { cpr_schema, .. }
        | ast_addressed::RelationalExpression::Join { cpr_schema, .. }
        | ast_addressed::RelationalExpression::SetOperation { cpr_schema, .. } => {
            cpr_schema.get().clone()
        }
        ast_addressed::RelationalExpression::Pipe(pipe) => pipe.cpr_schema.get().clone(),
        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    }
}
