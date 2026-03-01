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

// Re-export public scope type

use crate::error::Result;
use crate::pipeline::asts::resolved;
use context::FlattenContext;
use std::collections::HashSet;

/// Main entry point - flatten a resolved expression
pub fn flatten(expr: resolved::RelationalExpression) -> Result<FlatSegment> {
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
    };

    expression::flatten_expression(expr, &mut segment, &mut context)?;

    // Analyzer will determine segment type

    Ok(segment)
}
