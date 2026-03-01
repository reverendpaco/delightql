// types.rs - Core types for the principled refiner
//
// Based on PRINCIPLED-RELOOK-AT-REFINER.md
// These types enforce the classification system and laws

use crate::pipeline::asts::resolved;
use std::collections::HashSet;

/// Core predicate classification from the principled document
/// Every predicate MUST be classified into one of these categories
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateClass {
    /// FJC - Join condition between two tables
    FJC { left: String, right: String },

    /// FIC - Intersect/correlation condition for set operations
    FIC { left: String, right: String },

    /// F - Regular filter on a single table
    F { table: String },

    /// Fx - Non-participating filter (1=1, #<2, etc)
    Fx,

    /// F! - Semantically valid but forbidden by laws
    Forbidden { reason: ForbiddenReason },
}

/// Reasons why a predicate is forbidden
#[derive(Debug, Clone, PartialEq)]
pub enum ForbiddenReason {
    /// Law 1: Cannot join into UL fragment
    /// Example: (a UL b) J c FJC(c,a) is FORBIDDEN
    Law1UlFragmentJoin,

    /// Law 3: Intersection needs proper qualification
    /// Example: users_2022(*) ; users_2023(*), email = email (no qualification)
    Law3ImproperQualification,

    /// Law 6: PLF with ambiguous Lvar
    /// Example: foo(a,b,c) || bar(a,y,z), a<10 (both have 'a')
    Law6PlfRestriction,
}

/// An analyzed predicate with its classification
#[derive(Debug, Clone)]
pub struct AnalyzedPredicate {
    /// The classification of this predicate
    pub class: PredicateClass,

    /// The original expression
    pub expr: resolved::BooleanExpression,

    /// Which operator this predicate modifies
    pub operator_ref: OperatorRef,

    /// The origin of this predicate (e.g., PositionalLiteral)
    pub origin: resolved::FilterOrigin,
}

/// Where in the expression tree a predicate attaches
#[derive(Debug, Clone, PartialEq)]
pub struct ScopePoint {
    /// Position in the flattened segment
    pub position: usize,

    /// Tables in scope at this point
    pub tables_in_scope: HashSet<String>,
}

/// Reference to an operator in the flattened segment
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorRef {
    /// References a join at position
    Join { position: usize },

    /// References a set operation
    SetOp {
        position: usize,
        operator: resolved::SetOperator,
    },

    /// Top-level (Fx predicates)
    TopLevel,
}

/// Lvar binding for positional unification
#[derive(Debug, Clone)]
pub struct LvarBinding {
    /// Which table it comes from
    pub table: String,
    /// What kind of operation introduced this table (join vs setop)
    pub operation_context: crate::pipeline::refiner::flattener::OperationContext,
}

/// Segment type determines rebuild semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentType {
    /// Tables should be joined with conditions as ON clauses
    Join,

    /// Tables should be unioned with correlations as Filter+InnerExists
    SetOperation,

    /// Mixed segment containing both joins and set operations
    Mixed,
}
