use serde::{Deserialize, Serialize};

use super::expressions::DomainExpression;
use super::ordering::{Limit, OrderTerm};
use super::select_items::SelectItem;
use super::table::TableExpression;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryExpression {
    /// A SELECT statement
    Select(Box<SelectStatement>),

    /// UNION/UNION ALL/INTERSECT/EXCEPT
    SetOperation {
        op: SetOperator,
        left: Box<QueryExpression>,
        right: Box<QueryExpression>,
    },

    /// VALUES clause (for inline data)
    Values { rows: Vec<Vec<DomainExpression>> },

    /// Nested WITH clause (for CTEs within CTEs)
    /// Generates: WITH cte1 AS (...), cte2 AS (...) SELECT ...
    /// This allows tree groups (which generate intermediate CTEs) to be bound as CTEs themselves
    WithCte {
        ctes: Vec<super::Cte>,
        query: Box<QueryExpression>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    /// DISTINCT flag
    pub(super) distinct: bool,

    /// What to select (columns, expressions, *)
    pub(super) select_list: Vec<SelectItem>,

    /// FROM clause - tables, subqueries, joins
    pub(super) from: Option<Vec<TableExpression>>,

    /// WHERE clause
    pub(super) where_clause: Option<DomainExpression>,

    /// GROUP BY clause
    pub(super) group_by: Option<Vec<DomainExpression>>,

    /// HAVING clause (only valid with GROUP BY)
    pub(super) having: Option<DomainExpression>,

    /// ORDER BY clause
    pub(super) order_by: Option<Vec<OrderTerm>>,

    /// LIMIT clause with optional OFFSET
    pub(super) limit: Option<Limit>,
}

impl SelectStatement {
    pub fn builder() -> super::builders::SelectBuilder {
        super::builders::SelectBuilder::new()
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }

    pub fn select_list(&self) -> &[SelectItem] {
        &self.select_list
    }

    pub fn from(&self) -> Option<&[TableExpression]> {
        self.from.as_deref()
    }

    pub fn where_clause(&self) -> Option<&DomainExpression> {
        self.where_clause.as_ref()
    }

    pub fn group_by(&self) -> Option<&[DomainExpression]> {
        self.group_by.as_deref()
    }

    pub fn having(&self) -> Option<&DomainExpression> {
        self.having.as_ref()
    }

    pub fn order_by(&self) -> Option<&[OrderTerm]> {
        self.order_by.as_deref()
    }

    pub fn limit(&self) -> Option<&Limit> {
        self.limit.as_ref()
    }
}
