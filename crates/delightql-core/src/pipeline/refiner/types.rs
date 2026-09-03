// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// types.rs - Core types for the principled refiner
//
// These types enforce the classification system and laws.

pub use super::settled::Settled;
use crate::names::ScopeId;
use crate::pipeline::asts::resolved;
use std::collections::HashSet;

/// Core predicate classification from the principled document
/// Every predicate MUST be classified into one of these categories
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateClass {
    /// FJC - Join condition between two tables
    FJC { left: ScopeId, right: ScopeId },

    /// F - Regular filter on a single table
    F { table: ScopeId },

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

    /// The classifier cannot assign a predicate spanning three or more
    /// independent scopes to a single lawful owner.
    TooManyReferencedTables { count: usize },
}

/// An analyzed predicate with its classification
#[derive(Debug, Clone)]
pub struct AnalyzedPredicate {
    /// The classification of this predicate
    pub class: PredicateClass,

    /// The expression, with every comparison leaf's equality class settled
    pub expr: Settled,

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
    pub tables_in_scope: HashSet<ScopeId>,
}

/// Reference to an operator in the flattened segment
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorRef {
    /// References a join at position
    Join { position: usize },

    /// Top-level (Fx predicates)
    TopLevel,
}
