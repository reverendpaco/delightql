// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// types.rs - Core data structures for flattening

use crate::pipeline::asts::resolved::{self, InnerRelationPattern, Resolved};
use std::collections::HashSet;

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
    pub identity: crate::names::ScopeId,
    pub position: usize,
    pub _scope_id: usize,         // Which operator introduces it
    pub access: resolved::Access, // Full access
    pub schema: crate::names::ScopeId,
    pub outer: bool, // Has ? prefix for outer joins
    // For anonymous tables - preserve the data
    pub anonymous_data: Option<AnonymousTableData>,
    // For INNER-RELATION - preserve the pattern for rebuilder (resolved phase)
    // NOTE: this is owed a transition to storing only metadata, not the full subquery
    pub inner_relation_pattern: Option<InnerRelationPattern<Resolved>>,
    pub preminted_scope: Option<crate::names::ScopeId>,
    // For INNER-RELATION - the FLATTENED subquery
    // This replaces recursive processing of the AST in inner_relation_pattern
    pub subquery_segment: Option<Box<FlatSegment>>,
    // For pipes - preserve the entire expression for later refinement
    pub pipe_expr: Option<Box<resolved::Chain>>,
    // For CONSULTED-VIEW - preserve the resolved Query for independent refinement by rebuilder
    pub consulted_view_query: Option<Box<resolved::Query>>,
    // Filters that should be applied directly to this table (e.g., PositionalLiteral)
    pub _table_filters: Vec<(resolved::TruthExpression, resolved::FilterOrigin)>,
    // For TVFs - preserve function name and arguments
    pub tvf_data: Option<TvfData>,
}

/// Data for table-valued functions
#[derive(Debug, Clone)]
pub struct TvfData {
    pub function: crate::names::FnId,
    /// `None` is a position with no scalar value — a skip, a spread, a
    /// star — carried as the absence it is rather than a placeholder
    /// expression a rebuild could mistake for a value.
    pub arguments: Vec<Option<resolved::DomainExpression>>,
    pub access: resolved::Access,
}

/// Data for anonymous tables
#[derive(Debug, Clone)]
pub struct AnonymousTableData {
    pub body: resolved::TabularBody<resolved::HeaderItem, resolved::Datum>,
}

/// A predicate in flattened form (unanalyzed)
#[derive(Debug, Clone)]
pub struct FlatPredicate {
    pub expr: resolved::TruthExpression,
    #[allow(dead_code)]
    pub position: usize,
    pub references: HashSet<crate::names::ColId>,
    pub _scope_id: usize,
    pub origin: resolved::FilterOrigin, // Track where this predicate came from
}

/// An operator in flattened form
#[derive(Debug, Clone)]
pub struct FlatOperator {
    pub position: usize,
    pub kind: FlatOperatorKind,
    pub left_tables: Vec<crate::names::ScopeId>,
    pub right_tables: Vec<crate::names::ScopeId>,
}

/// A segment's only operator is the join: a bag step is opaque here, so
/// its arms and its correlation never become a table pool and a predicate
/// class.
#[derive(Debug, Clone)]
pub enum FlatOperatorKind {
    Join {
        correspondence: Option<resolved::Correspondence>,
    },
}
