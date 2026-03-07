use serde::{Deserialize, Serialize};

use super::expressions::DomainExpression;
use super::query::QueryExpression;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableExpression {
    /// Simple table reference: [schema.]table [AS alias]
    Table {
        schema: Option<String>,
        name: String,
        alias: Option<String>,
    },

    /// Subquery: (SELECT ...) AS alias
    /// QueryExpression is wrapped in StackSafe to break drop recursion
    /// through deeply nested subquery chains (e.g. 1000-pipe queries).
    Subquery {
        query: Box<stacksafe::StackSafe<QueryExpression>>,
        alias: String, // Required in SQL!
    },

    /// JOIN expression
    Join {
        left: Box<TableExpression>,
        right: Box<TableExpression>,
        join_type: JoinType,
        join_condition: JoinCondition,
    },

    /// VALUES clause: VALUES (row1), (row2), ... AS alias
    /// Use this when no column headers are specified
    Values {
        rows: Vec<Vec<DomainExpression>>,
        alias: String, // Required in FROM clause
    },

    /// UNION ALL for anonymous tables with headers
    /// First SELECT has column aliases, rest are UNION ALL
    UnionTable {
        selects: Vec<QueryExpression>,
        alias: String, // Required to reference the result
    },

    /// Table-Valued Function: json_each(...), pragma_table_info(...)
    TVF {
        schema: Option<String>,
        function: String,
        arguments: Vec<TvfArgument>,
        alias: Option<String>,
    },
}

/// Structured TVF argument — replaces raw strings for proper qualifier resolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TvfArgument {
    /// String literal: "orders" or """json""" (includes DQL quotes, converted to SQL on emit)
    StringLiteral(String),
    /// Numeric literal: 42, 3.14, -1
    NumberLiteral(String),
    /// Bare identifier: users, my_table
    Identifier(String),
    /// Qualified column reference: table.column
    QualifiedRef { qualifier: String, column: String },
}

impl TvfArgument {
    /// Parse a raw string argument into a structured TvfArgument.
    /// Grammar constrains TVF arguments to: string_literal | number_literal |
    /// identifier | qualified_column.
    pub fn parse(raw: &str) -> Self {
        if raw.starts_with("\"\"\"") || raw.starts_with('"') {
            return TvfArgument::StringLiteral(raw.to_string());
        }

        // Check for numeric literal (including negative numbers)
        let numeric_start = if raw.starts_with('-') || raw.starts_with('+') {
            &raw[1..]
        } else {
            raw
        };
        if !numeric_start.is_empty()
            && numeric_start
                .chars()
                .all(|c| c.is_ascii_digit() || c == '.')
        {
            return TvfArgument::NumberLiteral(raw.to_string());
        }

        // Check for qualified reference: identifier chars, then '.', then identifier chars
        if let Some(dot_pos) = raw.find('.') {
            let qualifier = &raw[..dot_pos];
            let column = &raw[dot_pos + 1..];
            if !qualifier.is_empty()
                && !column.is_empty()
                && qualifier.chars().all(|c| c.is_alphanumeric() || c == '_')
                && column
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
            {
                return TvfArgument::QualifiedRef {
                    qualifier: qualifier.to_string(),
                    column: column.to_string(),
                };
            }
        }

        TvfArgument::Identifier(raw.to_string())
    }

    /// Resolve qualifier through scope stack. If the resolver returns a remapping
    /// for the qualifier, return a new QualifiedRef with the remapped qualifier.
    /// All other variants pass through unchanged.
    pub fn resolve_qualifier(&self, resolver: impl Fn(&str) -> Option<String>) -> Self {
        match self {
            TvfArgument::QualifiedRef { qualifier, column } => {
                if let Some(new_qualifier) = resolver(qualifier) {
                    TvfArgument::QualifiedRef {
                        qualifier: new_qualifier,
                        column: column.clone(),
                    }
                } else {
                    self.clone()
                }
            }
            TvfArgument::StringLiteral(_)
            | TvfArgument::NumberLiteral(_)
            | TvfArgument::Identifier(_) => self.clone(),
        }
    }

    /// Emit SQL text. StringLiteral converts DQL quotes to SQL single quotes.
    /// QualifiedRef emits as `qualifier.column` (unquoted — SQLite TVFs expect this).
    pub fn to_sql(&self) -> String {
        match self {
            TvfArgument::StringLiteral(s) => {
                // Convert DQL string delimiters to SQL single quotes
                let inner = if s.starts_with("\"\"\"") && s.ends_with("\"\"\"") {
                    &s[3..s.len() - 3]
                } else if s.starts_with('"') && s.ends_with('"') {
                    &s[1..s.len() - 1]
                } else {
                    s
                };
                // Escape single quotes in the content for SQL
                let escaped = inner.replace('\'', "''");
                format!("'{}'", escaped)
            }
            TvfArgument::NumberLiteral(n) => n.clone(),
            TvfArgument::Identifier(id) => id.clone(),
            TvfArgument::QualifiedRef { qualifier, column } => {
                format!("\"{}\".\"{}\"", qualifier, column)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinCondition {
    On(DomainExpression),
    Using(Vec<String>),
    Natural,
}

// Smart constructors for TableExpression
impl TableExpression {
    pub fn table(name: impl Into<String>) -> Self {
        TableExpression::Table {
            schema: None,
            name: name.into(),
            alias: None,
        }
    }

    pub fn table_with_alias(name: impl Into<String>, alias: impl Into<String>) -> Self {
        TableExpression::Table {
            schema: None,
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    pub fn subquery(query: QueryExpression, alias: impl Into<String>) -> Self {
        TableExpression::Subquery {
            query: Box::new(stacksafe::StackSafe::new(query)),
            alias: alias.into(),
        }
    }

    pub fn inner_join(
        left: TableExpression,
        right: TableExpression,
        on: Option<DomainExpression>,
    ) -> Self {
        TableExpression::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            join_condition: on.map(JoinCondition::On).unwrap_or(JoinCondition::Natural),
        }
    }

    pub fn left_join(left: TableExpression, right: TableExpression, on: DomainExpression) -> Self {
        TableExpression::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Left,
            join_condition: JoinCondition::On(on),
        }
    }
}
