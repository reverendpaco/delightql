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
    Using(Vec<crate::names::ColId>),
    Natural,
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
