use std::collections::HashMap;

use super::segment_handler::SegmentSource;
use crate::pipeline::ast_addressed;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, OrderTerm, QueryExpression, SelectBuilder, TableExpression,
};

/// Represents the state of query building within a pipe segment
pub enum QueryBuildState {
    /// Just a table reference, not yet a SELECT statement
    Table(TableExpression),
    /// Building up a SELECT statement within a pipe segment
    Builder(SelectBuilder),
    /// Builder with hygienic column injections that need wrapping during finalization
    BuilderWithHygienic {
        builder: SelectBuilder,
        hygienic_injections: Vec<(String, String)>,
    },
    /// Complete query expression (after pipe)
    Expression(QueryExpression),
    /// Accumulating operations within a CPR segment
    /// This variant allows us to accumulate joins flatly without creating subqueries
    Segment {
        source: SegmentSource,
        filters: Vec<DomainExpression>,
        order_by: Vec<OrderTerm>,
        limit_offset: Option<(i64, i64)>,
        cpr_schema: ast_addressed::CprSchema,
        dialect: SqlDialect,
        remappings: HashMap<String, String>,
    },
    /// Anonymous table that needs subquery wrapping when joined
    /// This ensures anonymous tables get proper aliases for CPR replacement
    AnonymousTable(TableExpression),
    /// Melt table - needs special CTE + json_each handling
    /// EPOCH 7: Signals that this anonymous table should generate premelt CTE
    MeltTable {
        melt_packet_sql: String, // json_array(json_array(...), ...) SQL string
        headers: Vec<String>,    // Column names for json_extract mapping
        alias: String,           // Alias for the melt table
    },
    /// A completed DML statement (DELETE, UPDATE, INSERT)
    DmlStatement(crate::pipeline::sql_ast_v3::SqlStatement),
}
