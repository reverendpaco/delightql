use serde::{Deserialize, Serialize};

use super::query::QueryExpression;

/// A complete SQL statement - the root of our SQL AST
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        table_name: String,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// Query to populate the table
        query: QueryExpression,
    },
    /// CREATE TEMPORARY VIEW statement (REPL-only)
    CreateTempView {
        /// Name of the temporary view
        view_name: String,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// Query definition for the view
        query: QueryExpression,
    },
    /// DELETE FROM statement
    Delete {
        /// Target table name (will be quoted by generator)
        target_table: String,
        /// Optional namespace prefix (e.g., "hr" in hr.employee)
        target_namespace: Option<String>,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// WHERE clause expression
        where_clause: Option<super::DomainExpression>,
    },
    /// UPDATE statement
    Update {
        /// Target table name (will be quoted by generator)
        target_table: String,
        /// Optional namespace prefix
        target_namespace: Option<String>,
        /// Optional WITH clause for CTEs
        with_clause: Option<Vec<Cte>>,
        /// SET clause: (column_name, value_expression)
        set_clause: Vec<(String, super::DomainExpression)>,
        /// WHERE clause expression
        where_clause: Option<super::DomainExpression>,
    },
    /// INSERT INTO ... SELECT statement
    Insert {
        /// Target table name (will be quoted by generator)
        target_table: String,
        /// Optional namespace prefix
        target_namespace: Option<String>,
        /// Column names for the INSERT
        columns: Vec<String>,
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

    pub fn create_temp_table(
        table_name: String,
        with_clause: Option<Vec<Cte>>,
        query: QueryExpression,
    ) -> Self {
        Self::CreateTempTable {
            table_name,
            with_clause,
            query,
        }
    }

    pub fn create_temp_view(
        view_name: String,
        with_clause: Option<Vec<Cte>>,
        query: QueryExpression,
    ) -> Self {
        Self::CreateTempView {
            view_name,
            with_clause,
            query,
        }
    }

    pub fn simple(query: QueryExpression) -> Self {
        SqlStatement::Query {
            with_clause: None,
            query,
        }
    }
}

/// Common Table Expression (CTE) - lives at statement level
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    /// CTE name (e.g., "_cpr_1")
    name: String,
    /// The query that defines this CTE
    query: QueryExpression,
    /// Whether this CTE is recursive (references itself)
    is_recursive: bool,
}

impl Cte {
    pub fn new(name: impl Into<String>, query: QueryExpression) -> Self {
        Cte {
            name: name.into(),
            query,
            is_recursive: false,
        }
    }

    pub fn new_recursive(name: impl Into<String>, query: QueryExpression) -> Self {
        Cte {
            name: name.into(),
            query,
            is_recursive: true,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn query(&self) -> &QueryExpression {
        &self.query
    }

    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }
}
