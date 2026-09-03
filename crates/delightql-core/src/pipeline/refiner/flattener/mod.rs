// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
pub use predicates::extract_value_references;
pub use types::{
    AnonymousTableData, FlatOperator, FlatOperatorKind, FlatPredicate, FlatSegment, FlatTable,
    TvfData,
};

use crate::error::Result;
use crate::pipeline::asts::resolved;
use context::FlattenContext;
use std::collections::HashSet;

/// Main entry point - flatten a resolved expression
pub fn flatten(
    expr: resolved::Chain,
    operand: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<FlatSegment> {
    let mut segment = FlatSegment {
        operand,
        tables: Vec::new(),
        predicates: Vec::new(),
        operators: Vec::new(),
    };

    let mut context = FlattenContext {
        identities,
        position: 0,
        scope_id: 0,
        tables_in_scope: HashSet::new(),
    };

    expression::flatten_expression(expr, &mut segment, &mut context)?;

    Ok(segment)
}
