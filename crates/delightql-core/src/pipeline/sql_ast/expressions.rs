// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::operators::{BinaryOperator, UnaryOperator};
use super::query::QueryExpression;
use crate::pipeline::ast_refined::LiteralValue;

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionName {
    User(String),
    Intrinsic(crate::names::Intrinsic),
}

impl FunctionName {
    pub fn user(&self) -> Option<&str> {
        match self {
            FunctionName::User(name) => Some(name),
            FunctionName::Intrinsic(_) => None,
        }
    }
}

impl From<String> for FunctionName {
    fn from(name: String) -> Self {
        FunctionName::User(name)
    }
}

impl From<&str> for FunctionName {
    fn from(name: &str) -> Self {
        FunctionName::User(name.to_string())
    }
}

impl From<crate::names::Intrinsic> for FunctionName {
    fn from(intrinsic: crate::names::Intrinsic) -> Self {
        FunctionName::Intrinsic(intrinsic)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DomainExpression {
    /// A resolved column occurrence. Qualification follows from its owner
    /// scope and the select that emits the reference.
    Column(crate::names::ColId),

    /// Literal value
    Literal(LiteralValue),

    /// The published name of a resolved column, used as data rather than as
    /// an SQL identifier. Baptism supplies the characters at generation.
    PublishedNameLiteral(crate::names::ColId),

    /// A JSON member path derived from a resolved column's published name.
    /// Baptism supplies and escapes the member spelling at generation.
    PublishedJsonPathLiteral(crate::names::ColId),

    /// A typed reach, rendered as the target's JSON path.
    ///
    /// THE PATH IS RENDERED, NOT INTERPOLATED: a key is written quoted and
    /// escaped and an index is written as a subscript, so a key carrying a
    /// quote, a dot or a backslash reaches the value it names instead of
    /// re-parsing as more path.
    JsonPathLiteral(crate::pipeline::asts::core::Path),

    /// The emitted name of a scope used as scalar data.
    ScopeNameLiteral(crate::names::ScopeId),

    /// Type cast: CAST(expr AS type). `type_name` is the DQL-canonical
    /// type word (integer|real|text|numeric|boolean); the generator spells
    /// it per target via `dialect_render` `type.*` rows (canonical =
    /// uppercased name). Semantics are the TARGET's cast — invalid-input
    /// behavior is deliberately target-dependent (see the book's cast page).
    Cast {
        expr: Box<DomainExpression>,
        type_name: String,
    },

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
        name: FunctionName,
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
        distinct: bool,
        partition_by: Vec<DomainExpression>,
        order_by: Vec<(DomainExpression, super::ordering::OrderDirection)>,
        frame: Option<SqlWindowFrame>,
    },

    /// Predicate-position rewrite call (sigma predicates like +like, +between).
    /// The generator consults the bin_registry to render this.
    PredicateRewrite {
        name: String,
        args: Vec<DomainExpression>,
        negated: bool,
    },

    /// THE POLARITY OBSERVATION — `IS TRUE` / `IS NOT TRUE`.
    ///
    /// A collapse, not an operator: it turns UNKNOWN into a definite answer,
    /// which is why the two polarities equipartition their input. Nothing in
    /// truth position can express one, so the mark lives here, at the seam
    /// where the target's own spelling is chosen.
    Observation {
        expr: Box<DomainExpression>,
        positive: bool,
    },
}

impl DomainExpression {
    /// Structurally map every column reference in this expression through
    /// `f`, recursing into composite variants. A subquery INTERIOR keeps
    /// its own qualification road and is not entered — but the expression
    /// standing beside one belongs to this layer: `x IN (SELECT …)` reads
    /// `x` here, and leaving it behind would hold an occurrence this
    /// layer no longer publishes.
    pub fn map_columns(self, f: &impl Fn(crate::names::ColId) -> crate::names::ColId) -> Self {
        use DomainExpression as E;
        let re = |e: Box<E>| Box::new(e.map_columns(f));
        let re_vec = |es: Vec<E>| es.into_iter().map(|e| e.map_columns(f)).collect();
        match self {
            E::Column(column) => E::Column(f(column)),
            E::Cast { expr, type_name } => E::Cast {
                expr: re(expr),
                type_name,
            },
            E::Binary { left, op, right } => E::Binary {
                left: re(left),
                op,
                right: re(right),
            },
            E::Unary { op, expr } => E::Unary { op, expr: re(expr) },
            E::Function {
                name,
                args,
                distinct,
            } => E::Function {
                name,
                args: re_vec(args),
                distinct,
            },
            E::Parens(expr) => E::Parens(re(expr)),
            E::Case {
                expr,
                when_clauses,
                else_clause,
            } => E::Case {
                expr: expr.map(re),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|clause| {
                        WhenClause::new(clause.when.map_columns(f), clause.then.map_columns(f))
                    })
                    .collect(),
                else_clause: else_clause.map(re),
            },
            E::WindowFunction {
                name,
                args,
                distinct,
                partition_by,
                order_by,
                frame,
            } => E::WindowFunction {
                name,
                args: re_vec(args),
                distinct,
                partition_by: re_vec(partition_by),
                order_by: order_by
                    .into_iter()
                    .map(|(e, direction)| (e.map_columns(f), direction))
                    .collect(),
                frame,
            },
            E::PredicateRewrite {
                name,
                args,
                negated,
            } => E::PredicateRewrite {
                name,
                args: re_vec(args),
                negated,
            },
            E::Observation { expr, positive } => E::Observation {
                expr: re(expr),
                positive,
            },
            other @ (E::Literal(_)
            | E::PublishedNameLiteral(_)
            | E::PublishedJsonPathLiteral(_)
            | E::JsonPathLiteral(_)
            | E::ScopeNameLiteral(_)
            | E::Star
            | E::Exists { .. }
            | E::Subquery(_)) => other,
        }
    }
}

#[cfg(test)]
mod map_columns_tests {
    //! Where the structural re-anchor walk STOPS.
    //!
    //! A subquery interior qualifies against scopes established inside it,
    //! and re-anchoring one of those against a scope out here moves a
    //! reference that is already correct. The expression standing beside
    //! the subquery is the opposite case: it reads at this layer.

    use super::DomainExpression as E;
    use crate::names::{Addressing, ColId, ColumnOrigin, Computation, Hint, Registry, ScopeOrigin};

    fn two_columns() -> (ColId, ColId) {
        let registry = Registry::new(&[]);
        let scope = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let mint = || {
            registry.mint_column(
                scope,
                ColumnOrigin::Computed {
                    via: Computation::Operator,
                },
                None,
                Addressing::Published,
                Default::default(),
            )
        };
        (mint(), mint())
    }

    #[test]
    fn a_reference_nested_in_composites_is_reached() {
        let (from, to) = two_columns();
        let buried = E::Function {
            name: super::FunctionName::from("upper"),
            args: vec![E::Case {
                expr: None,
                when_clauses: vec![super::WhenClause::new(E::Star, E::Column(from))],
                else_clause: Some(Box::new(E::Parens(Box::new(E::Column(from))))),
            }],
            distinct: false,
        };
        let E::Function { args, .. } = buried.map_columns(&|_| to) else {
            panic!("shape preserved");
        };
        let [E::Case {
            when_clauses,
            else_clause,
            ..
        }] = args.as_slice()
        else {
            panic!("shape preserved");
        };
        assert_eq!(*when_clauses[0].then(), E::Column(to));
        assert_eq!(
            **else_clause.as_ref().unwrap(),
            E::Parens(Box::new(E::Column(to)))
        );
    }
}

#[derive(Debug, Clone, PartialEq)]
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

    pub fn when_mut(&mut self) -> &mut DomainExpression {
        &mut self.when
    }

    pub fn then_mut(&mut self) -> &mut DomainExpression {
        &mut self.then
    }
}

/// SQL window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct SqlWindowFrame {
    pub mode: SqlFrameMode,
    pub start: SqlFrameBound,
    pub end: SqlFrameBound,
}

/// SQL frame mode
#[derive(Debug, Clone, PartialEq)]
pub enum SqlFrameMode {
    Groups,
    Rows,
    Range,
}

/// SQL frame bound
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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

    /// OBSERVE this predicate: `IS TRUE` positively, `IS NOT TRUE`
    /// negatively. The two are complementary over every input row, the
    /// UNKNOWN-answering ones included, which Kleene `NOT` is not.
    pub fn observed(self, positive: bool) -> Self {
        Self::Expr(DomainExpression::Observation {
            expr: Box::new(self.into_expr()),
            positive,
        })
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
    pub fn literal(value: LiteralValue) -> Self {
        DomainExpression::Literal(value)
    }

    pub fn star() -> Self {
        DomainExpression::Star
    }

    pub fn function(name: impl Into<String>, args: Vec<DomainExpression>) -> Self {
        // Code chooses the form: sqlite's scalar max/min and 2-arg round
        // are arity-distinguished overloads of the aggregate/1-arg forms,
        // and a name-keyed render row cannot split arities — so the node
        // carries the form as an intrinsic identity. The overload itself is
        // `Intrinsic::scalar_overload`, which resolution consults too: one
        // answer, so the window judgment and the render form cannot disagree
        // about which function a call names.
        let name = name.into();
        let name = match crate::names::Intrinsic::scalar_overload(&name, args.len()) {
            Some(intrinsic) => FunctionName::Intrinsic(intrinsic),
            None => FunctionName::User(name),
        };
        DomainExpression::Function {
            name,
            args,
            distinct: false,
        }
    }

    pub fn intrinsic(intrinsic: crate::names::Intrinsic, args: Vec<DomainExpression>) -> Self {
        DomainExpression::Function {
            name: FunctionName::Intrinsic(intrinsic),
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

    /// IS NOT DISTINCT FROM (NULL-safe equality)
    pub fn is_not_distinct_from(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::IsNotDistinctFrom,
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

    /// CAST(expr AS type) — `type_name` is the DQL-canonical type word.
    pub fn cast(expr: DomainExpression, type_name: impl Into<String>) -> Self {
        DomainExpression::Cast {
            expr: Box::new(expr),
            type_name: type_name.into(),
        }
    }
}
