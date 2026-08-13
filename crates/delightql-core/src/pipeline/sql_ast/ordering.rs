// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use serde::{Deserialize, Serialize};

use super::expressions::DomainExpression;

#[derive(Debug, Clone, PartialEq)]
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

    pub fn expr_mut(&mut self) -> &mut DomainExpression {
        &mut self.expr
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

/// A ROW CLAUSE: how many rows, and where the count starts.
///
/// The count is OPTIONAL because the language admits a bound that names
/// none: `#>n` skips rows and selects no maximum. Every target can express
/// that and none of them spells it the same way, so the AST carries the
/// absence and the generator writes the target's form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Limit {
    count: Option<i64>,
    offset: Option<i64>,
}

impl Limit {
    pub fn new(count: i64) -> Self {
        Limit {
            count: Some(count),
            offset: None,
        }
    }

    pub fn with_offset(count: i64, offset: i64) -> Self {
        Limit {
            count: Some(count),
            offset: Some(offset),
        }
    }

    /// A skip with no maximum.
    pub fn offset_only(offset: i64) -> Self {
        Limit {
            count: None,
            offset: Some(offset),
        }
    }

    /// The same clause with a maximum supplied. The offset it already
    /// carries says where that maximum starts counting.
    pub fn capped_at(&self, count: i64) -> Self {
        Limit {
            count: Some(count),
            offset: self.offset,
        }
    }

    pub fn count(&self) -> Option<i64> {
        self.count
    }

    pub fn offset(&self) -> Option<i64> {
        self.offset
    }
}
