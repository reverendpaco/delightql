// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// types.rs - Core data structures for flattening

use crate::pipeline::asts::resolved::{self, InnerRelationPattern, Resolved};
use std::collections::HashSet;

/// Flattened representation of a segment (between pipes)
#[derive(Debug, Clone)]
pub struct FlatSegment {
    /// THE RELATION THE SEGMENT WAS FLATTENED OUT OF.
    ///
    /// A rebuild stands over the operand's own SOURCES rather than over the
    /// operand, so what it publishes is that operand's sibling. This is the
    /// provenance that says which node is being rebuilt: without it the
    /// rebuild would have to be recognized afterwards from what it happens
    /// to resemble.
    pub operand: crate::relation::SemanticRelation,

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
    /// WHAT THIS TABLE IS, and what it publishes: ONE relation, the one the
    /// resolved node carries. An identity beside a schema was two names for
    /// the same construction result, kept in step by an assignment.
    pub relation: crate::relation::SemanticRelation,
    pub position: usize,
    pub _scope_id: usize,         // Which operator introduces it
    pub access: resolved::Access, // Full access
    pub outer: bool,              // Has ? prefix for outer joins
    // For anonymous tables - preserve the data
    pub anonymous_data: Option<AnonymousTableData>,
    /// THE HEAD THIS TABLE WAS FLATTENED OUT OF, where it had one.
    ///
    /// A node, not its parts. The rebuilder CROSSES it into the refined
    /// phase — rewriting the form and keeping what the head publishes —
    /// rather than assembling a fresh head out of a pattern and a relation
    /// stored side by side, which is a pairing nothing could check.
    /// `None` where the table stands for a whole chain rather than a read.
    pub head: Option<resolved::Grelex>,

    // For INNER-RELATION - the FLATTENED subquery
    // This replaces recursive processing of the AST in inner_relation_pattern
    pub subquery_segment: Option<Box<FlatSegment>>,
    // For pipes - preserve the entire expression for later refinement
    pub pipe_expr: Option<Box<resolved::Chain>>,

    // Filters that should be applied directly to this table (e.g., PositionalLiteral)
    pub _table_filters: Vec<(resolved::TruthExpression, resolved::FilterOrigin)>,
    // For TVFs - preserve function name and arguments
    pub tvf_data: Option<TvfData>,
}

impl FlatTable {
    /// WHAT THE NODE THIS TABLE HOLDS ACTUALLY PUBLISHES.
    ///
    /// A table standing for a whole chain holds that chain, and the chain
    /// publishes its own relation — which is not the one beside it when the
    /// flattener recorded the wrapper rather than the body. A rebuild stands
    /// over what the node publishes, so this is what it stood over.
    pub fn stood_over(&self) -> crate::relation::SemanticRelation {
        self.pipe_expr
            .as_ref()
            .map_or(self.relation, |pipe| pipe.semantic_relation())
    }

    /// THE HEAD, WHERE IT IS A CONSULTED EXPANSION.
    ///
    /// The rebuilder CROSSES this node rather than assembling a fresh head
    /// out of a body and a relation stored side by side, so `None` here is
    /// "this table is not that", never "the relation is somewhere else".
    pub fn consulted_head(&self) -> Option<&resolved::Grelex> {
        self.head.as_ref().filter(|head| {
            matches!(
                head.form(),
                resolved::GroundForm::Reference(resolved::Relation::ConsultedView { .. })
            )
        })
    }

    /// THE HEAD, WHERE IT IS A DERIVED TABLE, with the pattern it wraps.
    pub fn inner_head(&self) -> Option<(&resolved::Grelex, &InnerRelationPattern<Resolved>)> {
        let head = self.head.as_ref()?;
        match head.form() {
            resolved::GroundForm::Reference(resolved::Relation::InnerRelation {
                pattern, ..
            }) => Some((head, pattern)),
            _ => None,
        }
    }
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
    pub references: HashSet<crate::relation::PortId>,
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
        /// THE MEMBER'S OWN CORRELATION, exactly as the construction stated
        /// it: TOTAL — a correspondence merges the heading, a condition is
        /// the truth the pair must satisfy, a decided Cartesian is the
        /// deliberate cross. It rides the OPERATOR and is never pooled with
        /// the segment's ambient predicates: a reader that recovered it from
        /// a predicate's references would answer a different question than
        /// the act that built the join, and answers "local filter" for a
        /// correlation whose ports the join's own tables no longer publish.
        correlation: resolved::MemberCorrelation,
    },
}
