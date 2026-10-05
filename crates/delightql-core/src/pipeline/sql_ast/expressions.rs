// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::operators::{BinaryOperator, UnaryOperator};
use super::query::QueryExpression;
use crate::diagnostic::Internal;
use crate::pipeline::asts::core::LiteralValue;

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

    /// Predicate-position rewrite call (sigma predicates like +like, +between,
    /// +sql_eq). The generator consults the bin_registry to render this. The
    /// SELECTED entity is the complete namespace/name identity carried by the
    /// resolver, whether the author qualified it or not, so every SQL-AST
    /// pass moves the call whole and none reads it as a comparison.
    PredicateRewrite {
        name: String,
        namespace: Vec<String>,
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
                namespace,
                args,
                negated,
            } => E::PredicateRewrite {
                name,
                namespace,
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
    use crate::names::{Addressing, ColId, Registry};

    fn two_columns() -> (ColId, ColId) {
        let registry = Registry::new(&[]);
        let scope = registry.anonymous_scope(None);
        let mint = || registry.sql_column(scope, None, Addressing::Published);
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

// Smart constructors for DomainExpression
impl DomainExpression {
    pub fn literal(value: LiteralValue) -> Self {
        DomainExpression::Literal(value)
    }

    /// A constant written as a token, signed or parenthesized or not. It
    /// reads no column and computes nothing, so as an ORDER BY or GROUP BY
    /// key it distinguishes no rows. Anything that computes, a cast
    /// included, is read as its value.
    pub fn is_literal(&self) -> bool {
        match self {
            DomainExpression::Literal(_)
            | DomainExpression::PublishedNameLiteral(_)
            | DomainExpression::PublishedJsonPathLiteral(_)
            | DomainExpression::JsonPathLiteral(_)
            | DomainExpression::ScopeNameLiteral(_) => true,
            DomainExpression::Parens(inner)
            | DomainExpression::Unary {
                op: super::operators::UnaryOperator::Minus | super::operators::UnaryOperator::Plus,
                expr: inner,
            } => inner.is_literal(),
            _ => false,
        }
    }

    /// The literal keys SQL reads by POSITION as an ORDER BY or GROUP BY
    /// key: an integer, or a boolean (SQLite, MySQL and T-SQL spell it `1`
    /// or `0`), signed or parenthesized or not. `ORDER BY 2` names the second output
    /// column, and SQLite reads `(2)` and `+2` the same way. Written there, the
    /// value would be read with another meaning.
    pub fn reads_as_position(&self) -> bool {
        match self {
            DomainExpression::Literal(LiteralValue::Number(number)) => number.is_integer(),
            DomainExpression::Literal(LiteralValue::Boolean(_)) => true,
            DomainExpression::Parens(inner)
            | DomainExpression::Unary {
                op: super::operators::UnaryOperator::Minus | super::operators::UnaryOperator::Plus,
                expr: inner,
            } => inner.reads_as_position(),
            _ => false,
        }
    }

    #[cfg(test)]
    pub fn add(left: DomainExpression, right: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        }
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

    pub fn gt(self, other: DomainExpression) -> Self {
        DomainExpression::Binary {
            left: Box::new(self),
            op: BinaryOperator::GreaterThan,
            right: Box::new(other),
        }
    }

    /// AN ANCHORED MATCH, in the shape the target can actually express.
    ///
    /// THE ANCHOR IS ONE VALUE, so it is evaluated once. A match arm is a
    /// null-safe question — `WHEN anchor IS NOT DISTINCT FROM term` — and
    /// asking it per arm means writing the anchor per arm, which for a
    /// volatile anchor asks about a DIFFERENT value each time and can reach
    /// an arm no single value could.
    ///
    /// Where no term is null, THE EQUALITY LAW makes the target's own simple
    /// `CASE anchor WHEN term` equivalent: a null anchor answers UNKNOWN to
    /// `=` and FALSE to `IS NOT DISTINCT FROM`, and neither fires, so both
    /// fall to the same default. That form names the anchor once and is what
    /// this emits.
    ///
    /// Where a term IS null the two disagree — SQL's simple CASE makes a null
    /// arm dead code and the language's does not — so the null-safe spelling
    /// stays, and with it the per-arm occurrence. A computed anchor cannot be
    /// repeated, so it must be published by the row that owns the case; one
    /// standing where no row publishes it REFUSES rather than being asked
    /// twice.
    ///
    /// Each term is compared as `compared` spells it: the caller's equality
    /// decides how a value and a term are told apart.
    ///
    /// ONE LOWERING FOR ONE LAW: the query road and the DDL road ask the same
    /// question of the same arms, and a second copy would answer it once.
    pub fn anchored_case(
        anchor: DomainExpression,
        arms: Vec<(LiteralValue, DomainExpression)>,
        default: Option<DomainExpression>,
        compared: impl Fn(DomainExpression) -> DomainExpression,
    ) -> crate::Result<Self> {
        if !arms
            .iter()
            .any(|(term, _)| matches!(term, LiteralValue::Null))
        {
            return Ok(DomainExpression::Case {
                expr: Some(Box::new(anchor)),
                when_clauses: arms
                    .into_iter()
                    .map(|(term, then)| WhenClause::new(compared(DomainExpression::Literal(term)), then))
                    .collect(),
                else_clause: default.map(Box::new),
            });
        }
        let anchor = anchor.bound_where_it_stands()?;
        Ok(DomainExpression::Case {
            expr: None,
            when_clauses: arms
                .into_iter()
                .map(|(term, then)| {
                    WhenClause::new(
                        anchor
                            .clone()
                            .is_not_distinct_from(compared(DomainExpression::Literal(term))),
                        then,
                    )
                })
                .collect(),
            else_clause: default.map(Box::new),
        })
    }

    /// THE VALUE WHERE IT STANDS, when no row published it.
    ///
    /// A column reference IS one occurrence and so is a literal: naming
    /// either twice names the same value. Anything else must have been
    /// published by the row that owns it, and a position with no row to
    /// publish it — a predicate, an ordering, a context that never reached a
    /// projection — has nowhere to put it. Repeating it there would ask a
    /// volatile value twice and answer for neither, so this refuses instead.
    pub fn bound_where_it_stands(self) -> crate::Result<Self> {
        if matches!(
            self,
            DomainExpression::Column(_) | DomainExpression::Literal(_)
        ) {
            return Ok(self);
        }
        Err(Internal::invariant(
            "a match arm spelling `null` asks its question of the anchor itself, \
             so a computed anchor must be published by the row that owns the \
             case; this one stands where no row publishes it",
            "case/anchor_needs_a_row",
        ))
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

    /// CAST(expr AS type) — `type_name` is the DQL-canonical type word.
    pub fn cast(expr: DomainExpression, type_name: impl Into<String>) -> Self {
        DomainExpression::Cast {
            expr: Box::new(expr),
            type_name: type_name.into(),
        }
    }
}
