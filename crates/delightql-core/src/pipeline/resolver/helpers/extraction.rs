// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::Result;
use crate::names::ScopeId;
use crate::pipeline::ast_resolved;
use delightql_types::SqlIdentifier;

/// The scope a chain publishes: the last continuation's, or the head's
/// when nothing has consumed it.
pub(in crate::pipeline) fn extract_cpr_schema(expr: &ast_resolved::Chain) -> ScopeId {
    if let Some(continuation) = expr.continuations.last() {
        return *continuation
            .cpr_schema()
            .expect("ER-join consumed by resolver");
    }
    extract_head_cpr_schema(&expr.head)
}

/// The scope a chain HEAD publishes.
pub(in super::super) fn extract_head_cpr_schema(head: &ast_resolved::Grelex) -> ScopeId {
    match head {
        ast_resolved::Grelex::Literal(anon) => anon.table.cpr_schema,
        ast_resolved::Grelex::Reference(rel) => match rel {
            ast_resolved::Relation::Ground { cpr_schema, .. }
            | ast_resolved::Relation::FunctorCall { cpr_schema, .. }
            | ast_resolved::Relation::InnerRelation { cpr_schema, .. } => *cpr_schema,
            ast_resolved::Relation::ConsultedView { scoped, .. } => *scoped,
        },
    }
}

/// The scope a resolved Query publishes.
/// Dispatches to `extract_cpr_schema` on the main relational expression.
pub(in super::super) fn extract_cpr_schema_from_query(
    query: &ast_resolved::Query,
) -> Result<ScopeId> {
    Ok(extract_cpr_schema(&query.body))
}

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
pub(in super::super) fn transform_schema_table_names(
    input: ScopeId,
    new_table_name: &SqlIdentifier,
    origin: ast_resolved::CteOrigin,
    role: crate::names::CteRole,
    identities: &crate::names::Registry,
) -> ScopeId {
    let spelling = identities.intern(new_table_name.as_str(), new_table_name.is_stropped());
    let hint = match origin {
        ast_resolved::CteOrigin::UserDefined => crate::names::Hint::User(spelling),
        ast_resolved::CteOrigin::CompilerGenerated => crate::names::Hint::Prefix("cte"),
    };
    let scope = identities.mint_derived_scope(crate::names::ScopeOrigin::Cte { input, role }, hint);
    identities.republish_heading(input, scope, crate::names::Republish::BoundaryExport);
    scope
}
