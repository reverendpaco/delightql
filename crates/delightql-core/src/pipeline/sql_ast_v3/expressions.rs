use crate::pipeline::ast_refined::LiteralValue;
use serde::{Deserialize, Serialize};

use super::operators::{BinaryOperator, UnaryOperator};
use super::query::QueryExpression;

/// Column qualifier — table, schema.table, or database.schema.table.
///
/// Cannot be constructed via enum literals. Use factory methods:
/// `ColumnQualifier::table()`, `ColumnQualifier::schema_table()`, etc.
/// Read with `.parts()` or `.table_name()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnQualifier(ColumnQualifierKind);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum ColumnQualifierKind {
    /// Just table/alias: t.col
    Table(String),

    /// Schema + table: schema.table.col
    SchemaTable { schema: String, table: String },

    /// Database + schema + table: db.schema.table.col
    DatabaseSchemaTable {
        database: String,
        schema: String,
        table: String,
    },
}

/// Read-only view of qualifier parts. Can be matched on but cannot
/// construct a ColumnQualifier (holds borrowed refs, no From impl).
#[derive(Debug)]
pub enum QualifierParts<'a> {
    Table(&'a str),
    SchemaTable {
        schema: &'a str,
        table: &'a str,
    },
    DatabaseSchemaTable {
        database: &'a str,
        schema: &'a str,
        table: &'a str,
    },
}

impl ColumnQualifier {
    /// Construct a table-qualified ColumnQualifier.
    pub(in crate::pipeline) fn table(name: impl Into<String>) -> Self {
        ColumnQualifier(ColumnQualifierKind::Table(name.into()))
    }

    pub(in crate::pipeline) fn schema_table(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        ColumnQualifier(ColumnQualifierKind::SchemaTable {
            schema: schema.into(),
            table: table.into(),
        })
    }

    /// Destructure for pattern matching (read-only).
    pub fn parts(&self) -> QualifierParts<'_> {
        match &self.0 {
            ColumnQualifierKind::Table(t) => QualifierParts::Table(t),
            ColumnQualifierKind::SchemaTable { schema, table } => {
                QualifierParts::SchemaTable { schema, table }
            }
            ColumnQualifierKind::DatabaseSchemaTable {
                database,
                schema,
                table,
            } => QualifierParts::DatabaseSchemaTable {
                database,
                schema,
                table,
            },
        }
    }

    /// The table/alias name (present in all variants).
    pub fn table_name(&self) -> &str {
        match &self.0 {
            ColumnQualifierKind::Table(t) => t,
            ColumnQualifierKind::SchemaTable { table, .. } => table,
            ColumnQualifierKind::DatabaseSchemaTable { table, .. } => table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DomainExpression {
    /// Column reference with optional qualification
    Column {
        name: String,
        qualifier: Option<ColumnQualifier>,
    },

    /// Literal value
    Literal(LiteralValue),

    /// Binary operation: left op right
    Binary {
        left: Box<DomainExpression>,
        op: BinaryOperator,
        right: Box<DomainExpression>,
    },

    /// Unary operation: op expr
    Unary {
        op: UnaryOperator,
        expr: Box<DomainExpression>,
    },

    /// Function call: func(args)
    Function {
        name: String,
        args: Vec<DomainExpression>,
        distinct: bool, // For COUNT(DISTINCT ...)
    },

    /// Star for COUNT(*)
    Star,

    /// Parenthesized expression
    Parens(Box<DomainExpression>),

    /// CASE expression
    Case {
        expr: Option<Box<DomainExpression>>, // Optional expression after CASE
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<DomainExpression>>,
    },

    /// IN/NOT IN
    InList {
        expr: Box<DomainExpression>,
        not: bool,
        values: Vec<DomainExpression>,
    },

    /// IN/NOT IN subquery
    InSubquery {
        expr: Box<DomainExpression>,
        not: bool,
        query: Box<QueryExpression>,
    },

    /// EXISTS/NOT EXISTS
    Exists {
        not: bool,
        query: Box<QueryExpression>,
    },

    /// Scalar subquery - returns a single value
    Subquery(Box<QueryExpression>),

    /// Window function: func() OVER (PARTITION BY ... ORDER BY ... frame_spec)
    WindowFunction {
        name: String,
        args: Vec<DomainExpression>,
        partition_by: Vec<DomainExpression>,
        order_by: Vec<(DomainExpression, super::ordering::OrderDirection)>,
        frame: Option<SqlWindowFrame>,
    },

    /// Raw SQL expression - for cases where we need to inject literal SQL
    /// EPOCH 7: Used for melt json_array packets with column references
    RawSql(String),

    /// Row value constructor: (expr1, expr2, ...)
    /// Used for multi-column IN expressions: (a, b) IN (VALUES ...)
    Tuple(Vec<DomainExpression>),

    /// Predicate-position rewrite call (sigma predicates like +like, +between).
    /// The generator consults the bin_registry to render this.
    PredicateRewrite {
        name: String,
        args: Vec<DomainExpression>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    when: DomainExpression,
    then: DomainExpression,
}

impl WhenClause {
    pub fn new(when: DomainExpression, then: DomainExpression) -> Self {
        WhenClause { when, then }
    }

    pub fn when(&self) -> &DomainExpression {
        &self.when
    }

    pub fn then(&self) -> &DomainExpression {
        &self.then
    }
}

/// SQL window frame specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlWindowFrame {
    pub mode: SqlFrameMode,
    pub start: SqlFrameBound,
    pub end: SqlFrameBound,
}

/// SQL frame mode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlFrameMode {
    Groups,
    Rows,
    Range,
}

/// SQL frame bound
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlFrameBound {
    Unbounded,
    CurrentRow,
    Preceding(Box<DomainExpression>),
    Following(Box<DomainExpression>),
}

/// A predicate in boolean position (WHERE, ON, HAVING).
///
/// Either a plain `DomainExpression` or a rewrite call that the generator
/// resolves via the bin_registry (sigma predicates like +like, +between).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlPredicate {
    /// A domain expression used as a predicate.
    Expr(DomainExpression),
    /// A rewrite-rule predicate (sigma predicates).
    /// The generator consults the bin_registry to render this.
    RewriteCall {
        name: String,
        args: Vec<DomainExpression>,
        negated: bool,
    },
}

impl SqlPredicate {
    /// Wrap a `DomainExpression` as a predicate.
    pub(crate) fn new(expr: DomainExpression) -> Self {
        Self::Expr(expr)
    }

    /// Wrap a rewrite-rule predicate.
    pub(crate) fn rewrite_call(
        name: impl Into<String>,
        args: Vec<DomainExpression>,
        negated: bool,
    ) -> Self {
        Self::RewriteCall {
            name: name.into(),
            args,
            negated,
        }
    }

    /// Access the inner expression (read-only).
    /// Panics on RewriteCall — use pattern matching when both variants are possible.
    pub fn as_expr(&self) -> &DomainExpression {
        match self {
            Self::Expr(e) => e,
            Self::RewriteCall { .. } => panic!("as_expr called on RewriteCall"),
        }
    }

    /// Unwrap into a `DomainExpression`.
    /// RewriteCall converts to `DomainExpression::PredicateRewrite`.
    pub fn into_expr(self) -> DomainExpression {
        match self {
            Self::Expr(e) => e,
            Self::RewriteCall {
                name,
                args,
                negated,
            } => DomainExpression::PredicateRewrite {
                name,
                args,
                negated,
            },
        }
    }

    /// Combine two predicates with AND.
    pub fn and(self, other: SqlPredicate) -> Self {
        Self::Expr(DomainExpression::and(vec![
            self.into_expr(),
            other.into_expr(),
        ]))
    }

    /// Combine two predicates with OR.
    pub fn or(self, other: SqlPredicate) -> Self {
        Self::Expr(DomainExpression::or(vec![
            self.into_expr(),
            other.into_expr(),
        ]))
    }

    /// Negate this predicate.
    pub fn not(self) -> Self {
        Self::Expr(DomainExpression::Unary {
            op: super::operators::UnaryOperator::Not,
            expr: Box::new(self.into_expr()),
        })
    }
}

// Smart constructors for DomainExpression
impl DomainExpression {
    pub fn column(name: impl Into<String>) -> Self {
        DomainExpression::Column {
            name: name.into(),
            qualifier: None,
        }
    }

    /// Construct a qualified column from a pre-built ColumnQualifier.
    /// No mint parameter — the qualifier was already minted at construction.
    /// All string→qualifier conversion happens in QualifierScope::structural().
    pub fn with_qualifier(qualifier: ColumnQualifier, name: impl Into<String>) -> Self {
        DomainExpression::Column {
            name: name.into(),
            qualifier: Some(qualifier),
        }
    }

    pub fn literal(value: LiteralValue) -> Self {
        DomainExpression::Literal(value)
    }

    pub fn star() -> Self {
        DomainExpression::Star
    }

    pub fn function(name: impl Into<String>, args: Vec<DomainExpression>) -> Self {
        DomainExpression::Function {
            name: name.into(),
            args,
            distinct: false,
        }
    }

    pub fn add(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        }
    }

    pub fn subtract(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Subtract,
            right: Box::new(right),
        }
    }

    pub fn multiply(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Multiply,
            right: Box::new(right),
        }
    }

    pub fn divide(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Divide,
            right: Box::new(right),
        }
    }

    pub fn modulo(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Modulo,
            right: Box::new(right),
        }
    }

    pub fn concat(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Concatenate,
            right: Box::new(right),
        }
    }

    /// Logical AND
    pub fn and(exprs: Vec<DomainExpression>) -> Self {
        if exprs.is_empty() {
            return DomainExpression::Literal(LiteralValue::Boolean(true));
        }
        if exprs.len() == 1 {
            return exprs.into_iter().next().expect("Checked len==1 above");
        }

        // Build left-associative AND chain
        let mut iter = exprs.into_iter();
        let mut result = iter.next().expect("Checked non-empty above");
        for expr in iter {
            result = DomainExpression::Binary {
                left: Box::new(result),
                op: BinaryOperator::And,
                right: Box::new(expr),
            };
        }
        result
    }

    /// Logical OR
    pub fn or(exprs: Vec<DomainExpression>) -> Self {
        if exprs.is_empty() {
            return DomainExpression::Literal(LiteralValue::Boolean(false));
        }
        if exprs.len() == 1 {
            return exprs.into_iter().next().expect("Checked len==1 above");
        }

        // Build left-associative OR chain
        let mut iter = exprs.into_iter();
        let mut result = iter.next().expect("Checked non-empty above");
        for expr in iter {
            result = DomainExpression::Binary {
                left: Box::new(result),
                op: BinaryOperator::Or,
                right: Box::new(expr),
            };
        }
        result
    }

    pub fn eq(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Equal,
            right: Box::new(right),
        }
    }

    pub fn gt(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::GreaterThan,
            right: Box::new(other),
        }
    }

    pub fn lt(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::LessThan,
            right: Box::new(other),
        }
    }

    pub fn gte(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::GreaterThanOrEqual,
            right: Box::new(other),
        }
    }

    pub fn ge(self, other: DomainExpression) -> Self {
        self.gte(other)
    }

    pub fn lte(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::LessThanOrEqual,
            right: Box::new(other),
        }
    }

    pub fn le(self, other: DomainExpression) -> Self {
        self.lte(other)
    }

    pub fn ne(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::NotEqual,
            right: Box::new(other),
        }
    }

    /// IS NOT DISTINCT FROM (NULL-safe equality)
    pub fn is_not_distinct_from(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::IsNotDistinctFrom,
            right: Box::new(other),
        }
    }

    /// IS DISTINCT FROM (NULL-safe inequality)
    pub fn is_distinct_from(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::IsDistinctFrom,
            right: Box::new(other),
        }
    }

    pub fn exists(query: QueryExpression) -> Self {
        DomainExpression::Exists {
            not: false,
            query: Box::new(query),
        }
    }

    pub fn not_exists(query: QueryExpression) -> Self {
        DomainExpression::Exists {
            not: true,
            query: Box::new(query),
        }
    }

    pub fn subquery(query: QueryExpression) -> Self {
        DomainExpression::Subquery(Box::new(query))
    }

    /// Drop any table qualifier, keeping just the column name.
    /// Non-column expressions pass through unchanged.
    pub fn unqualified(self) -> Self {
        match self {
            DomainExpression::Column { name, .. } => DomainExpression::Column {
                name,
                qualifier: None,
            },
            other => other,
        }
    }
}
