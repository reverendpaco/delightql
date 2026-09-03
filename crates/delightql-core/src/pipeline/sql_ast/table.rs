// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::expressions::DomainExpression;
use super::query::QueryExpression;

#[derive(Debug, Clone, PartialEq)]
pub enum TableExpression {
    /// A base table, CTE, or scratch relation occurrence.
    Scope(crate::names::ScopeId),

    /// A scope placed in a dialect-selected physical schema.
    QualifiedScope {
        schema: String,
        scope: crate::names::ScopeId,
    },

    /// A catalog entity, optionally exposed under an occurrence scope.
    Entity {
        entity: crate::names::EntityId,
        alias: Option<crate::names::ScopeId>,
    },

    /// Subquery: (SELECT ...) AS alias
    /// QueryExpression is wrapped in StackSafe to break drop recursion
    /// through deeply nested subquery chains (e.g. 1000-pipe queries).
    Subquery {
        query: Box<stacksafe::StackSafe<QueryExpression>>,
        alias: crate::names::ScopeId,
    },

    /// JOIN expression
    Join {
        left: Box<TableExpression>,
        right: Box<TableExpression>,
        join_type: JoinType,
        join_condition: JoinCondition,
    },

    /// Table-Valued Function: json_each(...), pragma_table_info(...)
    TVF {
        function: crate::names::FnId,
        arguments: Vec<TvfArgument>,
        alias: crate::names::ScopeId,
    },
}

/// Structured TVF argument — replaces raw strings for proper qualifier resolution.
#[derive(Debug, Clone, PartialEq)]
pub enum TvfArgument {
    Literal(crate::pipeline::asts::core::LiteralValue),
    /// A resolved column occurrence.
    Column(crate::names::ColId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On(DomainExpression),
    /// A correspondence: pairs of exact physical slots that are ONE value.
    /// The condition is their equality, spelled from each slot's own
    /// identity (`ON l.a = r.b`), and the published heading keeps one slot
    /// of each pair. Never a `USING`: that spelling joins by NAME, and the
    /// two slots of a pair need not share one — a poisoned mint on one
    /// side beside an authored name on the other is one value still.
    Merge(Vec<MergedSlots>),
    /// A DELIBERATE cross: the semantic join carried an explicit Cartesian
    /// judgment. Renders with no ON clause where the dialect accepts that
    /// spelling; `bare_join` legalizes it to CROSS JOIN / ON TRUE
    /// elsewhere. Never a default — no lowering arm produces this from a
    /// missing correlation.
    Cartesian,
}

/// The two exact physical slots one correspondence position merges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergedSlots {
    pub left: crate::names::ColId,
    pub right: crate::names::ColId,
}

// Smart constructors for TableExpression
impl TableExpression {
    pub fn subquery(query: QueryExpression, alias: crate::names::ScopeId) -> Self {
        TableExpression::Subquery {
            query: Box::new(stacksafe::StackSafe::new(query)),
            alias,
        }
    }
}
