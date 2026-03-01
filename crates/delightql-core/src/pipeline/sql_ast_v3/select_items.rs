use serde::{Deserialize, Serialize};

use super::expressions::DomainExpression;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItem {
    Star,

    QualifiedStar {
        qualifier: String,
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

    pub fn qualified_star(qualifier: impl Into<String>) -> Self {
        SelectItem::QualifiedStar {
            qualifier: qualifier.into(),
        }
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
