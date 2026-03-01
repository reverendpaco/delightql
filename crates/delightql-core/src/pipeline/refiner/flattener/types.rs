// types.rs - Core data structures for flattening

use crate::pipeline::asts::refined::QualifiedName;
use crate::pipeline::asts::resolved::{self, InnerRelationPattern, Resolved};
use delightql_types::SqlIdentifier;
use std::collections::HashSet;

/// Operation context - where did this table come from?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationContext {
    /// Table appears directly (not from flattening)
    Direct,

    /// Table came from flattening a SetOperation
    FromSetOp,
}

/// Flattened representation of a segment (between pipes)
#[derive(Debug, Clone)]
pub struct FlatSegment {
    /// All tables in the segment
    pub tables: Vec<FlatTable>,

    /// All predicates (unanalyzed)
    pub predicates: Vec<FlatPredicate>,

    /// Operator chain (preserves nesting!)
    pub operators: Vec<FlatOperator>,
}

/// A table in flattened form
#[derive(Debug, Clone)]
pub struct FlatTable {
    pub identifier: QualifiedName,
    pub canonical_name: Option<SqlIdentifier>,
    pub alias: Option<String>,
    pub position: usize,
    pub _scope_id: usize,                  // Which operator introduces it
    pub domain_spec: resolved::DomainSpec, // Full domain specification
    pub operation_context: OperationContext,
    pub schema: resolved::CprSchema,
    pub outer: bool, // Has ? prefix for outer joins
    // For anonymous tables - preserve the data
    pub anonymous_data: Option<AnonymousTableData>,
    // EPOCH 7: Column references in anonymous table data that need correlation
    pub correlation_refs: Vec<CorrelationRef>,
    // For INNER-RELATION - preserve the pattern for rebuilder (resolved phase)
    // NOTE: In Phase 3+, this will transition to storing only metadata, not the full subquery
    pub inner_relation_pattern: Option<InnerRelationPattern<Resolved>>,
    // For INNER-RELATION - the FLATTENED subquery (Phase 2+)
    // This replaces recursive processing of the AST in inner_relation_pattern
    pub subquery_segment: Option<Box<FlatSegment>>,
    // For pipes - preserve the entire expression for later refinement
    pub pipe_expr: Option<Box<resolved::RelationalExpression>>,
    // For CONSULTED-VIEW - preserve the resolved Query for independent refinement by rebuilder
    pub consulted_view_query: Option<Box<resolved::Query>>,
    // Filters that should be applied directly to this table (e.g., PositionalLiteral)
    pub _table_filters: Vec<(resolved::BooleanExpression, resolved::FilterOrigin)>,
    // For TVFs - preserve function name and arguments
    pub tvf_data: Option<TvfData>,
}

/// Data for table-valued functions
#[derive(Debug, Clone)]
pub struct TvfData {
    pub function: String,
    pub arguments: Vec<String>,
    pub domain_spec: resolved::DomainSpec,
    pub namespace: Option<crate::pipeline::asts::core::metadata::NamespacePath>,
    pub grounding: Option<crate::pipeline::asts::core::metadata::GroundedPath>,
}

/// Data for anonymous tables
#[derive(Debug, Clone)]
pub struct AnonymousTableData {
    pub column_headers: Option<Vec<resolved::DomainExpression>>,
    pub rows: Vec<resolved::Row>,
    pub exists_mode: bool, // EPOCH 3: true = +_() (EXISTS/filtering), false = _() (cartesian/join)
}

/// A column reference in anonymous table data that needs correlation with outer table
/// EPOCH 7: Used for inverted IN pattern detection
#[derive(Debug, Clone)]
pub struct CorrelationRef {
    #[allow(dead_code)]
    pub column_name: String,
    #[allow(dead_code)]
    pub outer_table: Option<String>, // Which outer table this correlates to (if resolved)
}

/// A predicate in flattened form (unanalyzed)
#[derive(Debug, Clone)]
pub struct FlatPredicate {
    pub expr: resolved::BooleanExpression,
    #[allow(dead_code)]
    pub position: usize,
    pub qualified_refs: HashSet<String>,   // a.id, b.name
    pub unqualified_refs: HashSet<String>, // id, name
    pub _scope_id: usize,
    pub origin: resolved::FilterOrigin, // Track where this predicate came from
}

/// An operator in flattened form
#[derive(Debug, Clone)]
pub struct FlatOperator {
    pub position: usize,
    pub kind: FlatOperatorKind,
    pub left_tables: Vec<String>,  // Tables from left operand
    pub right_tables: Vec<String>, // Tables from right operand
}

#[derive(Debug, Clone)]
pub enum FlatOperatorKind {
    Join { using_columns: Option<Vec<String>> },
    SetOp { operator: resolved::SetOperator },
}
