use serde::{Deserialize, Serialize};

use super::expressions::DomainExpression;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderTerm {
    expr: DomainExpression,
    direction: Option<OrderDirection>,
}

impl OrderTerm {
    pub fn new(expr: DomainExpression, direction: Option<OrderDirection>) -> Self {
        OrderTerm { expr, direction }
    }

    pub fn expr(&self) -> &DomainExpression {
        &self.expr
    }

    pub fn direction(&self) -> Option<&OrderDirection> {
        self.direction.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Limit {
    count: i64,
    offset: Option<i64>,
}

impl Limit {
    pub fn new(count: i64) -> Self {
        Limit {
            count,
            offset: None,
        }
    }

    pub fn with_offset(count: i64, offset: i64) -> Self {
        Limit {
            count,
            offset: Some(offset),
        }
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    pub fn offset(&self) -> Option<i64> {
        self.offset
    }
}
