// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::query::QueryExpression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationTarget {
    Entity(crate::names::EntityId),
    Scope(crate::names::ScopeId),
    QualifiedScope {
        schema: String,
        scope: crate::names::ScopeId,
    },
}

/// A complete SQL statement - the root of our SQL AST
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStatement {
    /// A regular query with optional CTEs
    Query {
        /// Optional WITH clause containing CTEs
        with_clause: Option<Vec<Cte>>,
        /// The main query
        query: QueryExpression,
    },
    /// CREATE TEMPORARY TABLE statement (REPL-only)
    CreateTempTable {
        /// Name of the temporary table
        table: crate::names::ScopeId,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// Query to populate the table
        query: QueryExpression,
    },
    /// CREATE TEMPORARY VIEW statement (REPL-only)
    CreateTempView {
        /// Name of the temporary view
        view: crate::names::ScopeId,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// Query definition for the view
        query: QueryExpression,
    },
    /// DELETE FROM statement
    Delete {
        target: RelationTarget,
        target_scope: crate::names::ScopeId,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// WHERE clause expression
        where_clause: Option<super::DomainExpression>,
    },
    /// UPDATE statement
    Update {
        target: RelationTarget,
        target_scope: crate::names::ScopeId,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// SET clause: (column_name, value_expression)
        set_clause: Vec<(crate::names::ColId, super::DomainExpression)>,
        /// WHERE clause expression
        where_clause: Option<super::DomainExpression>,
    },
    /// DROP TABLE IF EXISTS, for a temporary relation the compiler created.
    ///
    /// Emitted before the CREATE that makes one, never on its own: a run
    /// that ended early left its scratch behind, and the next run of the
    /// same statement asks for the same name. Dropping first is what makes
    /// a refused run repeatable.
    DropTempTable {
        /// The temporary relation to remove.
        table: crate::names::ScopeId,
    },
    /// INSERT INTO ... SELECT statement
    Insert {
        target: RelationTarget,
        target_scope: crate::names::ScopeId,
        /// Column names for the INSERT
        columns: Vec<crate::names::ColId>,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// Source query for the INSERT
        source: QueryExpression,
    },
}

impl SqlStatement {
    pub fn with_ctes(with_clause: Option<Vec<Cte>>, query: QueryExpression) -> Self {
        Self::Query { with_clause, query }
    }
}

/// WHAT A CTE'S BODY IS.
///
/// A FIXPOINT KEEPS ITS PARTS. Its anchor and its recursive members are
/// structure here, not a set-operation spine that every later pass
/// rediscovers by walking and re-deciding what the spine meant.
///
/// `Fixpoint`'s payload is unwritable outside the binding authority and its
/// ONE producer is `CteBinding::into_sql`, which carries a decided body's
/// variant across unchanged and names its scope from the binding's own
/// subject. So an ordinary body cannot be made recursive here, and one
/// fixpoint can be given neither another's parts nor another's scope.
#[derive(Debug, Clone, PartialEq)]
pub enum CteBody {
    Ordinary(QueryExpression),
    Fixpoint(crate::pipeline::bindings::SqlFixpoint),
}

impl CteBody {
    /// Every query this body holds, anchor first. The order is the order it
    /// emits in, so a pass that walks for reading sees what SQL will.
    pub fn parts(&self) -> Vec<&QueryExpression> {
        match self {
            CteBody::Ordinary(query) => vec![query],
            CteBody::Fixpoint(fixpoint) => fixpoint.parts(),
        }
    }

    /// The same, to rewrite in place. A pass that transforms a fixpoint
    /// transforms its parts and KEEPS the variant: there is no field here
    /// through which a rewrite could reach the accumulation, and no road
    /// from one variant to the other.
    pub fn parts_mut(&mut self) -> Vec<&mut QueryExpression> {
        match self {
            CteBody::Ordinary(query) => vec![query],
            CteBody::Fixpoint(fixpoint) => fixpoint.parts_mut(),
        }
    }
}

/// Common Table Expression (CTE) - lives at statement level
#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    /// The scope bound by this CTE.
    scope: crate::names::ScopeId,
    /// What the CTE is: an ordinary query, or a fixpoint that keeps its
    /// anchor and members apart.
    body: CteBody,
    /// This reusable binding must be evaluated into storage once before any
    /// reader spends it. The producer is semantic construction, not a
    /// volatility-name list in SQL generation.
    materialized_once: bool,
}

impl Cte {
    /// An ordinary, non-recursive binding.
    pub fn ordinary(scope: crate::names::ScopeId, query: QueryExpression) -> Self {
        Cte {
            scope,
            body: CteBody::Ordinary(query),
            materialized_once: false,
        }
    }

    /// A RECURSIVE binding, from a body that already names its own scope.
    ///
    /// There is no scope argument: the body was decided for one binding and
    /// carries which, so a fixpoint cannot be bound anywhere else — and the
    /// body itself is unwritable outside the authority that decided it.
    pub(in crate::pipeline) fn fixpoint(fixpoint: crate::pipeline::bindings::SqlFixpoint) -> Self {
        Cte {
            scope: fixpoint.scope(),
            body: CteBody::Fixpoint(fixpoint),
            materialized_once: false,
        }
    }

    pub fn scope(&self) -> crate::names::ScopeId {
        self.scope
    }

    pub fn body(&self) -> &CteBody {
        &self.body
    }

    pub fn is_recursive(&self) -> bool {
        matches!(self.body, CteBody::Fixpoint(_))
    }

    pub(in crate::pipeline) fn requiring_materialization(mut self) -> Self {
        debug_assert!(!self.is_recursive());
        self.materialized_once = true;
        self
    }

    pub fn materialized_once(&self) -> bool {
        self.materialized_once
    }

    /// Every part of this binding's body, to rewrite in place. The variant
    /// is not reachable from here: a rewrite transforms what a fixpoint
    /// accumulates, never WHETHER it is one and never with what.
    pub fn parts_mut(&mut self) -> Vec<&mut QueryExpression> {
        self.body.parts_mut()
    }

    /// Rewrite every part of this binding's body, KEEPING the variant.
    ///
    /// Rebuilding through a constructor would silently answer a question
    /// the rewrite was not asked, because the recursion decision was taken
    /// long before this pass ran.
    pub fn rewrite_parts<E>(
        mut self,
        mut rewrite: impl FnMut(QueryExpression) -> std::result::Result<QueryExpression, E>,
    ) -> std::result::Result<Self, E> {
        for part in self.body.parts_mut() {
            let taken = std::mem::replace(part, QueryExpression::Values { rows: Vec::new() });
            *part = rewrite(taken)?;
        }
        Ok(self)
    }
}
