// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::expressions::DomainExpression;
use super::query::QueryExpression;

#[derive(Debug, Clone, PartialEq)]
pub enum TableExpression {
    /// A base table, CTE, or scratch relation occurrence.
    Scope(crate::names::ScopeId),


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

impl TableExpression {
    /// The occurrence behind each column this FROM entry carries, when the
    /// statement holds its heading. A relation named from outside the
    /// statement and a table function answer with the engine's own heading,
    /// which the compiler never saw.
    #[stacksafe::stacksafe]
    pub fn heading(&self) -> Option<Vec<Option<crate::names::ColId>>> {
        match self {
            TableExpression::Subquery { query, .. } => query.heading(),
            TableExpression::Join { left, right, .. } => {
                let mut heading = left.heading()?;
                heading.extend(right.heading()?);
                Some(heading)
            }
            TableExpression::Scope(_)
            | TableExpression::Entity { .. }
            | TableExpression::TVF { .. } => None,
        }
    }
}

/// Structured TVF argument — replaces raw strings for proper qualifier resolution.
#[derive(Debug, Clone, PartialEq)]
pub enum TvfArgument {
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
    /// A DELIBERATE cross: the semantic join carried an explicit Cartesian
    /// judgment. Renders with no ON clause where the dialect accepts that
    /// spelling; `bare_join` legalizes it to CROSS JOIN / ON TRUE
    /// elsewhere. Never a default — no lowering arm produces this from a
    /// missing correlation.
    Cartesian,
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
