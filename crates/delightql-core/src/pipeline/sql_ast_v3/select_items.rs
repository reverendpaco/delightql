// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use serde::{Deserialize, Serialize};

use super::expressions::{ColumnQualifier, DomainExpression};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItem {
    Star,

    QualifiedStar {
        qualifier: ColumnQualifier,
    },

    Expression {
        expr: DomainExpression,
        alias: Option<String>,
    },
}

// Smart constructors for SelectItem
impl SelectItem {
    pub fn star() -> Self {
        SelectItem::Star
    }

    pub fn expression(expr: DomainExpression) -> Self {
        SelectItem::Expression { expr, alias: None }
    }

    pub fn expression_with_alias(expr: DomainExpression, alias: impl Into<String>) -> Self {
        SelectItem::Expression {
            expr,
            alias: Some(alias.into()),
        }
    }
}
