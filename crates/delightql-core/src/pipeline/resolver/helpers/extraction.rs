// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::relation::SemanticRelation;
use delightql_types::SqlIdentifier;

pub(in super::super) fn extract_inline_using_columns(
    expr: &ast_resolved::Chain,
) -> Option<Vec<SqlIdentifier>> {
    // Only a read nothing has consumed carries an inline USING: the
    // dequalifying access is the mention's own.
    match expr.as_read_relation()? {
        ast_resolved::Relation::Ground { .. } => match expr.head_access()? {
            // Dequalify: table(*.(col1, col2)) — has USING columns.
            ast_resolved::Access::Dequalify(cols) => Some(cols.clone()),
            // Glob: table(*), Positional: table(a, b), Bare: natural join marker.
            // None of these carry USING columns.
            ast_resolved::Access::All
            | ast_resolved::Access::DequalifyAll
            | ast_resolved::Access::Slots(_)
            | ast_resolved::Access::Unasked => None,
        },
        // Relations without an access spec carrying USING syntax.
        ast_resolved::Relation::FunctorCall { call: _, .. }
        | ast_resolved::Relation::InnerRelation { .. }
        | ast_resolved::Relation::ConsultedView { .. } => None,
    }
}

/// Transform a schema's table names to use a new table name
/// This is used for CTEs to ensure their columns reference the CTE name, not the original table
/// Also pushes CteRegistration identity onto each column's identity stack.
/// `origin` is the binding's TYPED construction provenance — never inferred
/// from the name (a user may legally write `_ho_*` identifiers).
pub(in crate::pipeline) fn transform_schema_table_names(
    input: &SemanticRelation,
    new_table_name: &SqlIdentifier,
    origin: ast_resolved::CteOrigin,
    role: crate::names::CteRole,
    identities: &crate::relation::Planning,
) -> Result<SemanticRelation> {
    use crate::relation::form::{CteLabelWhy, CteWhy, ExportSpec, ExportWhy};
    let spelling = identities.intern(new_table_name.as_str(), new_table_name.is_stropped());
    let label = match origin {
        ast_resolved::CteOrigin::UserDefined => CteLabelWhy::Answering(spelling),
        ast_resolved::CteOrigin::CompilerGenerated => CteLabelWhy::Prefixed("cte"),
    };
    let role = match role {
        crate::names::CteRole::TreeGroup => CteWhy::TreeGroup,
        crate::names::CteRole::GroupCarrier => CteWhy::GroupCarrier,
        crate::names::CteRole::Recursive => CteWhy::Recursive,
        crate::names::CteRole::Reachability => CteWhy::Reachability,
        crate::names::CteRole::Materialize => CteWhy::Materialize,
    };
    identities
        .authority()
        .derive(crate::relation::RelForm::Export(ExportSpec {
            input: input.clone(),
            why: ExportWhy::Cte { role, label },
        }))
}
