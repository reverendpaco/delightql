// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! B11: values and truths. Every equality class, grade and sigma identity
//! is the core's; this file only spells them.

use super::frame::{Enclosing, Env, Key, Private};
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::ids::{ExprId, TruthId};
use crate::pipeline::middle::core::node::{
    Arg, CaseTest, EqClass, ExprKind, FrameEdge, SigmaProof, TruthKind,
};
use crate::pipeline::middle::facade::{
    BinOp, BinaryOperator, CmpOp, LiteralValue, SqlDirection, SqlExpr, SqlFrameBound, SqlFrameMode,
    SqlWindowFrame, UnaryOperator, WhenClause,
};

/// Who consumes a truth: a filtering position keeps TRUE rows; anything
/// else reads the truth's value.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Consumer {
    Filter,
    Value,
}

/// Where a value is read: the stage being written, and what the relation
/// being realized reads from outside itself. A reference is read from the
/// one of the two the core's free binders name: outside when its binder is
/// free in the relation, in the stage otherwise.
#[derive(Clone, Copy)]
pub(super) struct At<'a> {
    pub(super) local: &'a Env,
    pub(super) outer: &'a Enclosing,
}

impl At<'_> {
    pub(super) fn get(&self, key: Key) -> Option<SqlExpr> {
        if self.outer.offers(key) {
            self.outer.get(key).cloned()
        } else {
            self.local.get(key).cloned()
        }
    }
}

impl Realizer<'_, '_> {
    /// A value, as SQL reading the stage `at` describes.
    #[stacksafe::stacksafe]
    pub(super) fn value(&mut self, e: ExprId, at: At<'_>) -> Result<SqlExpr> {
        if let Some(hoisted) = at.get(Key::Value(e)) {
            return Ok(hoisted);
        }
        let graph = self.graph;
        match graph.expr(e).kind() {
            ExprKind::Col(b, i) => at
                .get(Key::Col(*b, *i))
                .ok_or_else(|| self.unsupplied(&format!("a column of {b:?}"))),
            ExprKind::Merged(m) => at
                .get(Key::Merged(*m))
                .ok_or_else(|| self.unsupplied(&format!("the merged key {m:?}"))),
            // A configured value is read as itself only by a ground formal's
            // dispatch guard (a `$.` reference reads its carrier's column).
            // The core records no reach for a passenger, so no stage is
            // bound to carry it where the guard stands.
            ExprKind::Passenger(p) => at.get(Key::Passenger(*p)).ok_or_else(|| {
                self.uncovered(&format!(
                    "a clause's dispatch guard reading the configured value {p:?}, which its stage does not carry"
                ))
            }),
            ExprKind::Const(v) => Ok(SqlExpr::Literal(v.clone())),
            ExprKind::Call { callee, args, grade } => {
                let (args, distinct) = self.arguments(args, at)?;
                let mut args = rows_counted(&callee.name, args);
                if let (Some(flag), crate::pipeline::middle::core::node::Grade::Aggregate) =
                    (keep_flag(at.local), *grade)
                {
                    // An aggregate over a KEEP stage reads real rows only:
                    // its arguments are NULL on carriers, which an
                    // aggregate that ignores NULL inputs does not count.
                    // Any other aggregate over kept carriers refuses.
                    if !crate::pipeline::middle::core::decide::grade::ignores_null(&callee.name) {
                        return Err(self.uncovered("an aggregate that reads NULL inputs over kept carriers"));
                    }
                    args = args
                        .into_iter()
                        .map(|a| {
                            let real = SqlExpr::Binary {
                                left: Box::new(flag.clone()),
                                op: BinaryOperator::Equal,
                                right: Box::new(SqlExpr::Literal(integer(1))),
                            };
                            let value = match a {
                                SqlExpr::Star => SqlExpr::Literal(integer(1)),
                                other => other,
                            };
                            SqlExpr::Case {
                                expr: None,
                                when_clauses: vec![WhenClause::new(real, value)],
                                else_clause: None,
                            }
                        })
                        .collect();
                }
                if distinct {
                    args = args.into_iter().map(super::rel::set_key).collect();
                }
                if callee.name == "cast" {
                    if let [value, SqlExpr::Literal(LiteralValue::Symbol(type_name))] = args.as_slice() {
                        return Ok(SqlExpr::cast(value.clone(), type_name.clone()));
                    }
                    return Err(self.uncovered("a cast whose type is not a type symbol"));
                }
                let call = self.out.function(&callee.name, args);
                Ok(match call {
                    SqlExpr::Function { name, args, .. } => SqlExpr::Function {
                        name,
                        args,
                        distinct,
                    },
                    other => other,
                })
            }
            ExprKind::Window {
                callee,
                args,
                partition,
                order,
                frame,
            } => {
                let (args, distinct) = self.arguments(args, at)?;
                let args = if distinct { args.into_iter().map(super::rel::set_key).collect() } else { args };
                let mut partition_by = self.population(at.local)?;
                for p in partition {
                    partition_by.push(self.value(*p, at)?);
                }
                let partition_by = partition_by.into_iter().map(super::rel::set_key).collect();
                let mut order_by = Vec::with_capacity(order.len());
                for k in order {
                    order_by.push((self.value(k.expr, at)?, direction(k.direction)));
                }
                Ok(SqlExpr::WindowFunction {
                    name: callee.name.clone(),
                    args: rows_counted(&callee.name, args),
                    distinct,
                    partition_by,
                    order_by,
                    frame: frame.as_ref().map(window_frame),
                })
            }
            ExprKind::Infix(op, l, r) => {
                let left = self.value(*l, at)?;
                let right = self.value(*r, at)?;
                Ok(SqlExpr::Binary {
                    left: Box::new(grouped(left)),
                    op: infix(*op),
                    right: Box::new(grouped(right)),
                })
            }
            ExprKind::Case {
                anchor,
                arms,
                default,
            } => self.case(*anchor, arms, *default, at),
            ExprKind::Crossed(t) => {
                // DECISION(spelling): a truth read as a value is written as
                // the truth itself; its UNKNOWN reads as NULL.
                self.truth(*t, Consumer::Value, at)
            }
            ExprKind::Scalar { rel, .. } => {
                // DECISION(dependent): a scalar position is a correlated
                // subquery.
                let query = self.scalar_query(*rel, at)?;
                Ok(SqlExpr::Subquery(Box::new(query)))
            }
            ExprKind::Collect { .. } => self.collect(e, at),
            ExprKind::Metadata { .. } => self.metadata_object(e, at),
            ExprKind::Construct { layout, members } => self.construct(e, *layout, members, at),
            ExprKind::Pick { value, rank } => self.pick(*value, *rank, at),
            ExprKind::Path { source, path } => self.path(*source, path, at),
            ExprKind::Across(actual) | ExprKind::Argument { value: actual, .. } => self.value(*actual, at),
        }
    }

    /// The partition a population-sensitive value of the stage `env`
    /// describes is computed within: none for rows no dependent member
    /// owns; otherwise the innermost owner's witness, and its KEEP flag
    /// when it keeps carriers. An owner with no witness in the stage is a
    /// step the classifier did not count as population-sensitive, and the
    /// value refuses rather than range over every occurrence.
    pub(super) fn population(&self, env: &Env) -> Result<Vec<SqlExpr>> {
        let Some(owner) = env.owner() else {
            return Ok(Vec::new());
        };
        let witness = env
            .get(Key::Private(owner, Private::Witness))
            .cloned()
            .ok_or_else(|| self.contract("a window or reduction over a population whose member has no witness"))?;
        Ok(std::iter::once(witness).chain(keep_flag(env)).collect())
    }

    fn arguments(&mut self, args: &[Arg], at: At<'_>) -> Result<(Vec<SqlExpr>, bool)> {
        let mut out = Vec::with_capacity(args.len());
        let mut distinct = false;
        for a in args {
            match a {
                Arg::Star => out.push(SqlExpr::Star),
                Arg::Value { expr, distinct: d } => {
                    distinct |= *d;
                    out.push(self.value(*expr, at)?);
                }
            }
        }
        Ok((out, distinct))
    }

    fn case(
        &mut self,
        anchor: Option<ExprId>,
        arms: &[(CaseTest, ExprId)],
        default: Option<ExprId>,
        at: At<'_>,
    ) -> Result<SqlExpr> {
        self.case_of(anchor, arms, default, at, |this, result, at| this.value(result, at))
    }

    /// A case whose results are written by `result`: its own values, or
    /// their nodes.
    pub(super) fn case_of(
        &mut self,
        anchor: Option<ExprId>,
        arms: &[(CaseTest, ExprId)],
        default: Option<ExprId>,
        at: At<'_>,
        mut result: impl FnMut(&mut Self, ExprId, At<'_>) -> Result<SqlExpr>,
    ) -> Result<SqlExpr> {
        let default = match default {
            Some(d) => Some(result(self, d, at)?),
            None => None,
        };
        match anchor {
            Some(anchor) => {
                let anchor = self.value(anchor, at)?;
                let mut literal_arms = Vec::with_capacity(arms.len());
                for (test, result_expr) in arms {
                    let CaseTest::Literal { value, class } = test else {
                        return Err(self.uncovered("an anchored case with a truth arm"));
                    };
                    if *class != EqClass::NullSafe {
                        return Err(self.uncovered("a match arm that is not null-safe"));
                    }
                    literal_arms.push((value.clone(), result(self, *result_expr, at)?));
                }
                SqlExpr::anchored_case(anchor, literal_arms, default, |term| {
                    super::rel::compared(term, EqClass::NullSafe)
                })
            }
            None => {
                let mut when_clauses = Vec::with_capacity(arms.len());
                for (test, result_expr) in arms {
                    let CaseTest::Truth(t) = test else {
                        return Err(self.uncovered("an unanchored case with a literal arm"));
                    };
                    let when = self.truth(*t, Consumer::Filter, at)?;
                    when_clauses.push(WhenClause::new(when, result(self, *result_expr, at)?));
                }
                Ok(SqlExpr::Case {
                    expr: None,
                    when_clauses,
                    else_clause: default.map(Box::new),
                })
            }
        }
    }

    /// A truth, as SQL. A positive sigma application is its proof, UNKNOWN
    /// included; a negative one is `IS NOT TRUE` of it, so an ordinary
    /// filter partitions its input between the two polarities.
    #[stacksafe::stacksafe]
    pub(super) fn truth(&mut self, t: TruthId, _consumer: Consumer, at: At<'_>) -> Result<SqlExpr> {
        let graph = self.graph;
        match graph.truth(t).kind() {
            TruthKind::Cmp {
                op,
                left,
                right,
                class,
                ..
            } => {
                let l = self.value(*left, at)?;
                let r = self.value(*right, at)?;
                let operator = comparison(*op, *class);
                let compare = |l: SqlExpr, r: SqlExpr| SqlExpr::Binary {
                    left: Box::new(grouped(l)),
                    op: operator.clone(),
                    right: Box::new(super::rel::compared(grouped(r), *class)),
                };
                Ok(self.compared_reaffined((self.unaffined(*left), l), (self.unaffined(*right), r), compare))
            }
            TruthKind::And(parts) => {
                let mut out = Vec::with_capacity(parts.len());
                for p in parts {
                    out.push(grouped(self.truth(*p, Consumer::Value, at)?));
                }
                Ok(SqlExpr::and(out))
            }
            TruthKind::Or(parts) => {
                let mut out = Vec::with_capacity(parts.len());
                for p in parts {
                    out.push(grouped(self.truth(*p, Consumer::Value, at)?));
                }
                Ok(SqlExpr::Parens(Box::new(SqlExpr::or(out))))
            }
            TruthKind::Not(p) => Ok(SqlExpr::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(SqlExpr::Parens(Box::new(self.truth(*p, Consumer::Value, at)?))),
            }),
            TruthKind::Exists { positive, rel } => {
                // DECISION(dependent): an existence is a correlated subquery.
                let query = self.exists_query(*rel, at)?;
                Ok(SqlExpr::Exists {
                    not: !positive,
                    query: Box::new(query),
                })
            }
            TruthKind::Sigma {
                proof,
                args,
                positive,
            } => {
                let proof = match proof {
                    SigmaProof::Served { namespace, name } => {
                        let mut values = Vec::with_capacity(args.len());
                        for a in args {
                            values.push(self.value(*a, at)?);
                        }
                        SqlExpr::PredicateRewrite {
                            name: name.to_string(),
                            namespace: namespace.split("::").map(str::to_string).collect(),
                            args: values,
                            negated: false,
                        }
                    }
                    SigmaProof::Rule { expansion, .. } => {
                        SqlExpr::Parens(Box::new(self.truth(*expansion, Consumer::Value, at)?))
                    }
                };
                // A positive application carries its proof, UNKNOWN
                // included: a filtering consumer admits only TRUE, a value
                // crossing keeps UNKNOWN as NULL, and `!` over it stays
                // Kleene. A negative one is the two-valued "not proven TRUE".
                Ok(if *positive {
                    proof
                } else {
                    SqlExpr::Observation {
                        expr: Box::new(proof),
                        positive: false,
                    }
                })
            }
        }
    }
}

impl Realizer<'_, '_> {
    /// A pick, spelled as the reduction of the one value its rank chooses:
    /// one row of each group (of each KEEP partition) has rank 1, so the
    /// maximum over that row's value is the value, NULL included; a KEEP
    /// stage's carriers stand outside the choice.
    fn pick(&mut self, value: ExprId, rank: ExprId, at: At<'_>) -> Result<SqlExpr> {
        let first = |left: SqlExpr| SqlExpr::Binary {
            left: Box::new(grouped(left)),
            op: BinaryOperator::Equal,
            right: Box::new(SqlExpr::Literal(integer(1))),
        };
        let mut chosen = vec![first(self.value(rank, at)?)];
        if let Some(flag) = keep_flag(at.local) {
            chosen.push(first(flag));
        }
        let read = SqlExpr::Case {
            expr: None,
            when_clauses: vec![WhenClause::new(SqlExpr::and(chosen), self.value(value, at)?)],
            else_clause: None,
        };
        Ok(self.out.function("max", vec![read]))
    }

    /// `compare` over an operand's SQL, the affinity its value lost taken
    /// back (`unaffined`): a value of the affinity's storage class is
    /// compared through the cast to the column's type (which leaves it
    /// unchanged), so the target converts the other operand as the ordinary
    /// road does; any other value is compared as it is, as the ordinary
    /// road compares it.
    pub(super) fn reaffined(&self, operand: ExprId, sql: SqlExpr, compare: impl Fn(SqlExpr) -> SqlExpr) -> SqlExpr {
        self.reaffined_by(self.unaffined(operand), sql, compare)
    }

    /// THE ONE ROAD A LOST AFFINITY IS TAKEN BACK BY, for every comparison
    /// (a condition, a correspondence by name or USING, a membership's
    /// candidates, a slot, a lifted slot): `compare` over two operands'
    /// SQL, each operand that lost its column's affinity re-asserted, so the
    /// target applies the operands' affinities to each other as it would to
    /// the two columns read plainly (a numeric affinity converts a TEXT
    /// operand on either side).
    pub(super) fn compared_reaffined(
        &self,
        left: (Option<Lost>, SqlExpr),
        right: (Option<Lost>, SqlExpr),
        compare: impl Fn(SqlExpr, SqlExpr) -> SqlExpr,
    ) -> SqlExpr {
        let ((left_lost, l), (right_lost, r)) = (left, right);
        self.reaffined_by(left_lost, l, |l| self.reaffined_by(right_lost, r.clone(), |r| compare(l.clone(), r)))
    }

    fn reaffined_by(&self, lost: Option<Lost>, sql: SqlExpr, compare: impl Fn(SqlExpr) -> SqlExpr) -> SqlExpr {
        let Some((type_word, classes)) = lost else {
            return compare(sql);
        };
        let storage = self.out.function("typeof", vec![sql.clone()]);
        let fits = SqlExpr::or(
            classes
                .iter()
                .map(|class| SqlExpr::Binary {
                    left: Box::new(storage.clone()),
                    op: BinaryOperator::Equal,
                    right: Box::new(SqlExpr::Literal(LiteralValue::String(class.to_string()))),
                })
                .collect(),
        );
        let affined = compare(SqlExpr::cast(sql.clone(), type_word));
        SqlExpr::Case {
            expr: None,
            when_clauses: vec![WhenClause::new(fits, affined)],
            else_clause: Some(Box::new(compare(sql))),
        }
    }

    /// The affinity a value lost, when it reads a member's column
    /// ([`Self::unaffined_at`]).
    pub(super) fn unaffined(&self, e: ExprId) -> Option<Lost> {
        let ExprKind::Col(b, i) = self.graph.expr(e).kind() else {
            return None;
        };
        self.unaffined_at(self.graph.binder(*b).rel()?, *i)
    }

    /// [`Self::unaffined`] for a run cell: a member's column, or a merged
    /// key's left operand.
    pub(super) fn unaffined_cell(&self, cell: crate::pipeline::middle::core::node::Cell) -> Option<Lost> {
        match cell {
            crate::pipeline::middle::core::node::Cell::Col(b, i) => self.unaffined_at(self.graph.binder(b).rel()?, i),
            crate::pipeline::middle::core::node::Cell::Merged(m) => self.unaffined_cell(self.graph.merge(m).left()),
        }
    }

    /// The affinity position `i` of a relation lost, on SQLite: a value of
    /// a declared column read through an expression of its own — a
    /// delegate's pick (an aggregate over the chosen row), or a member
    /// realized over its carrier through a clause family (whose positions
    /// it reads through a tag's case or a document's element); its cast's
    /// type word and the storage classes the cast leaves unchanged.
    pub(super) fn unaffined_at(&self, rel: crate::pipeline::middle::core::ids::RelId, i: u16) -> Option<Lost> {
        use super::kinds::Affinity;
        use crate::pipeline::middle::core::node::walk::{reachable, Child};
        use crate::pipeline::middle::core::node::RelKind;
        if self.out.dialect() != crate::pipeline::middle::facade::SqlDialect::SQLite {
            return None;
        }
        let lost = match self.kinds.lost_affinity(self.graph, rel, i) {
            Some(affinity) => affinity,
            None => {
                if self.graph.rel(rel).fv().is_empty()
                    || !reachable(self.graph, &[Child::Rel(rel)])
                        .rels
                        .iter()
                        .any(|r| matches!(self.graph.rel(*r).kind(), RelKind::Family { .. }))
                {
                    return None;
                }
                self.kinds.declared_affinity(self.graph, rel, i)?
            }
        };
        Some(match lost {
            Affinity::Text => ("text", &["text"]),
            Affinity::Integer | Affinity::Real | Affinity::Numeric => ("numeric", &["integer", "real"]),
        })
    }
}

/// What an operand lost of its column's affinity: the type word its cast
/// takes the affinity back with, and the storage classes that cast leaves
/// unchanged.
pub(super) type Lost = (&'static str, &'static [&'static str]);

/// The KEEP flag of the member whose population a stage's rows are: an
/// aggregate over the stage counts that member's real rows only.
pub(super) fn keep_flag(env: &Env) -> Option<SqlExpr> {
    env.owner().and_then(|owner| env.get(Key::Private(owner, Private::Flag)).cloned())
}

/// The SQL operator of a comparison in its decided equality class.
fn comparison(op: CmpOp, class: EqClass) -> BinaryOperator {
    match (op, class) {
        (CmpOp::NullSafeEqual, EqClass::NullSafe) => BinaryOperator::IsNotDistinctFrom,
        (CmpOp::NullSafeNotEqual, EqClass::NullSafe) => BinaryOperator::IsDistinctFrom,
        (CmpOp::NullSafeEqual | CmpOp::Equal, EqClass::Correspondence | EqClass::Ordering) => {
            BinaryOperator::Equal
        }
        (CmpOp::NullSafeNotEqual | CmpOp::NotEqual, EqClass::Correspondence | EqClass::Ordering) => {
            BinaryOperator::NotEqual
        }
        (CmpOp::Equal, EqClass::NullSafe) => BinaryOperator::Equal,
        (CmpOp::NotEqual, EqClass::NullSafe) => BinaryOperator::NotEqual,
        (CmpOp::LessThan, _) => BinaryOperator::LessThan,
        (CmpOp::GreaterThan, _) => BinaryOperator::GreaterThan,
        (CmpOp::LessThanOrEqual, _) => BinaryOperator::LessThanOrEqual,
        (CmpOp::GreaterThanOrEqual, _) => BinaryOperator::GreaterThanOrEqual,
    }
}

fn infix(op: BinOp) -> BinaryOperator {
    match op {
        BinOp::Add => BinaryOperator::Add,
        BinOp::Sub => BinaryOperator::Subtract,
        BinOp::Mul => BinaryOperator::Multiply,
        BinOp::Div => BinaryOperator::Divide,
        BinOp::Mod => BinaryOperator::Modulo,
        BinOp::Concat => BinaryOperator::Concatenate,
    }
}

pub(super) fn direction(d: crate::pipeline::middle::core::node::Direction) -> SqlDirection {
    match d {
        crate::pipeline::middle::core::node::Direction::Ascending => SqlDirection::Asc,
        crate::pipeline::middle::core::node::Direction::Descending => SqlDirection::Desc,
    }
}

fn window_frame(frame: &crate::pipeline::middle::core::node::Frame) -> SqlWindowFrame {
    let edge = |e: &FrameEdge| match e {
        FrameEdge::Unbounded => SqlFrameBound::Unbounded,
        FrameEdge::CurrentRow => SqlFrameBound::CurrentRow,
        FrameEdge::Preceding(n) => SqlFrameBound::Preceding(Box::new(SqlExpr::Literal(integer(*n)))),
        FrameEdge::Following(n) => SqlFrameBound::Following(Box::new(SqlExpr::Literal(integer(*n)))),
    };
    SqlWindowFrame {
        mode: if frame.rows { SqlFrameMode::Rows } else { SqlFrameMode::Range },
        start: edge(&frame.start),
        end: edge(&frame.end),
    }
}

/// An integer literal.
pub(super) fn integer(n: i64) -> LiteralValue {
    LiteralValue::integer(n)
}

/// An operand parenthesized when it is itself an operation.
pub(super) fn grouped(e: SqlExpr) -> SqlExpr {
    match e {
        SqlExpr::Binary { .. } | SqlExpr::Unary { .. } | SqlExpr::Observation { .. } => {
            SqlExpr::Parens(Box::new(e))
        }
        other => other,
    }
}

/// A count of no argument counts rows; it is written `count(*)`, which
/// PostgreSQL requires and every target reads as the same count.
fn rows_counted(name: &str, args: Vec<SqlExpr>) -> Vec<SqlExpr> {
    if args.is_empty() && name.eq_ignore_ascii_case("count") {
        vec![SqlExpr::Star]
    } else {
        args
    }
}
