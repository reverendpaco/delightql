// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Smart constructors for AST expression types
// Provides fluent builder APIs to reduce boilerplate in AST construction

use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{
    AuthoredColumn, DomainExpression, LiteralValue, Phase, TruthExpression, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use delightql_types::SqlIdentifier;

// ============================================================================
// DomainExpression Builders
// ============================================================================

impl DomainExpression<Unresolved> {
    /// A written column reference. Only the authored phase has one to write:
    /// after resolution a column is an identity, and there is no door here
    /// that turns characters back into one.
    pub fn lvar_builder(name: impl Into<SqlIdentifier>) -> LvarBuilder {
        LvarBuilder {
            name: name.into(),
            qualifier: None,
            namespace_path: vec![],
        }
    }
}

impl<P: Phase> DomainExpression<P> {
    pub fn literal_builder(value: LiteralValue) -> LiteralBuilder {
        LiteralBuilder { value }
    }
}

// ============================================================================
// Builder Structs
// ============================================================================

pub struct LvarBuilder {
    name: SqlIdentifier,
    qualifier: Option<SqlIdentifier>,
    namespace_path: Vec<String>,
}

impl LvarBuilder {
    pub fn build(self) -> DomainExpression<Unresolved> {
        use crate::pipeline::asts::unresolved::NamespacePath;
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name: self.name,
            qualifier: self.qualifier,
            namespace_path: NamespacePath::from_parts(self.namespace_path)
                .expect("Invalid namespace path"),
        })))
    }
}

pub struct LiteralBuilder {
    value: LiteralValue,
}

impl LiteralBuilder {
    pub fn build(self) -> DomainExpression {
        DomainExpression::Application(super::expressions::FunctionApplication::Ground(self.value))
    }
}

// ============================================================================
// TruthExpression Builders
// ============================================================================

impl TruthExpression {
    pub fn comparison(
        op: crate::pipeline::asts::vocabulary::CmpOp,
        left: DomainExpression,
        right: DomainExpression,
    ) -> Self {
        TruthExpression::Comparison(Comparison {
            operator: op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}

// THE CANONICAL CONSTRUCTORS ARE `all` AND `any`, and they are the only ones.
// A binary `and`/`or` pair stood here building a two-member `Vec2` directly,
// which rebuilt the same-operator nesting the n-ary carrier exists to make
// impossible: handed a conjunction, it produced a conjunction of one. They
// had no caller; splicing is not an option a second door may decline.

// ============================================================================
// FunctionApplication Builders
// ============================================================================

// ============================================================================
// Mini-Kingdom: Binary Predicate Composition
// ============================================================================
// REMOVED - The old and/or methods were incorrectly wrapping predicates
// Now we use the proper TruthExpression::And and TruthExpression::Or variants
// defined above in the main TruthExpression impl block

// ============================================================================
// Kingdom 2: Chain builders
// ============================================================================
