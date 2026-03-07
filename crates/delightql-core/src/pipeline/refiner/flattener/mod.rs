// flattener.rs - Phase 1 of FAR cycle: Flatten AST segments
//
// The flattener transforms tree-form resolved AST into flat structures
// for analysis. It preserves ALL information needed for Laws.

mod context;
mod expression;
mod inner_relation;
mod predicates;
mod rewrite;
mod types;

// Re-export public types
pub use types::{
    AnonymousTableData, CorrelationRef, FlatOperator, FlatOperatorKind, FlatPredicate, FlatSegment,
    FlatTable, OperationContext, TvfData,
};

// Re-export alias heuristic for use by correlation_analyzer
pub(super) use rewrite::could_be_inner_alias;

use crate::error::Result;
use crate::pipeline::asts::resolved;
use context::FlattenContext;
use std::collections::{HashMap, HashSet};

/// Main entry point - flatten a resolved expression
pub fn flatten(expr: resolved::RelationalExpression) -> Result<FlatSegment> {
    flatten_with_scope(expr, HashMap::new())
}

/// Flatten with inherited scope aliases from parent inner-relation depths.
/// Each depth pushes its own aliases before recursing, so the map grows
/// inductively: depth N knows about all ancestors 0..N-1.
pub(super) fn flatten_with_scope(
    expr: resolved::RelationalExpression,
    scope_aliases: HashMap<String, String>,
) -> Result<FlatSegment> {
    let mut segment = FlatSegment {
        tables: Vec::new(),
        predicates: Vec::new(),
        operators: Vec::new(),
    };

    let mut context = FlattenContext {
        position: 0,
        scope_id: 0,
        tables_in_scope: HashSet::new(),
        anon_counter: 0,
        scope_aliases,
    };

    expression::flatten_expression(expr, &mut segment, &mut context)?;

    Ok(segment)
}
