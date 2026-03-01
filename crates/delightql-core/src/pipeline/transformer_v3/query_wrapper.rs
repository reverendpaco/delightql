use crate::pipeline::sql_ast_v3::QueryExpression;

/// Identity pass-through — previously walked the query tree to update
/// a `provenance` field on `DomainExpression::Column` that has been removed.
/// Retained as an identity function so call sites don't need to change.
pub fn update_query_provenance(query: QueryExpression, _alias: &str) -> QueryExpression {
    query
}
