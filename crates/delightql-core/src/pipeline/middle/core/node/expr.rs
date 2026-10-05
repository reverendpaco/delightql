// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Value constructors. Each computes the value's free binders and the row
//! occurrences its own references read, then pushes the node.

use super::{
    Arg, BinderSet, Callee, Cardinality, CaseTest, ExprKind, ExprNode, Frame, Occurrence, OrderKey,
    RelKind,
};
use crate::pipeline::middle::core::decide::{self, grade::CallPosition, grade::Known};
use crate::pipeline::middle::core::graph::{Arena, Builder, Judging};
use crate::pipeline::middle::core::ids::{
    BinderId, ExprId, MergeId, PassengerId, RelId, TruthId,
};
use crate::pipeline::middle::core::refuse::Refusal;
use crate::pipeline::middle::facade::{BinOp, LiteralValue};
use std::collections::BTreeSet;

impl Builder {
    fn push_value(&mut self, kind: ExprKind) -> ExprId {
        let (fv, occurrences) = value_facts(self, &kind);
        self.push_expr(ExprNode {
            kind,
            fv,
            occurrences,
        })
    }

    pub(crate) fn col(&mut self, binder: BinderId, position: u16) -> ExprId {
        self.push_value(ExprKind::Col(binder, position))
    }

    pub(crate) fn merged(&mut self, merge: MergeId) -> ExprId {
        self.push_value(ExprKind::Merged(merge))
    }

    pub(crate) fn constant(&mut self, value: LiteralValue) -> ExprId {
        self.push_value(ExprKind::Const(value))
    }

    /// The values a consumer reads the bytes of, each judged by
    /// `decide::document::observed`.
    pub(in crate::pipeline::middle::core::node) fn observe(
        &self,
        values: impl IntoIterator<Item = ExprId>,
        consumer: &str,
    ) -> Result<(), Refusal> {
        for value in values {
            decide::document::observed(&super::rel::structure_of(self, None, value)?, consumer)?;
        }
        Ok(())
    }

    /// A call, graded where it stands (W5 #11). A grade that contradicts
    /// the position is stored as such; the call is refused where its
    /// standing is known (see `decide::grade`). A target function reads its
    /// arguments' bytes.
    pub(crate) fn call(
        &mut self,
        callee: Callee,
        args: Vec<Arg>,
        position: CallPosition,
        known: Known,
        target_reduces: bool,
    ) -> Result<ExprId, Refusal> {
        self.observe(values(&args), "a call")?;
        let grade = decide::grade::of(position, known, target_reduces);
        Ok(self.push_value(ExprKind::Call { callee, args, grade }))
    }

    /// A call standing in one column of a pivot: graded as any call; a
    /// reduction reads only the rows `column` admits, each argument the
    /// value on those rows and NULL on the others, which the aggregates
    /// that ignore NULL inputs pass over. Any other aggregate refuses.
    pub(crate) fn call_within(
        &mut self,
        callee: Callee,
        args: Vec<Arg>,
        position: CallPosition,
        known: Known,
        target_reduces: bool,
        column: TruthId,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<ExprId, Refusal> {
        if decide::grade::of(position, known, target_reduces) != super::Grade::Aggregate {
            return self.call(callee, args, position, known, target_reduces);
        }
        if !decide::grade::ignores_null(&callee.name) {
            return Err(crate::pipeline::middle::core::refuse::outside(
                "a pivot whose value reduces with an aggregate that reads NULL inputs",
            ));
        }
        let admitted = self.admitted_args(args, column, switches)?;
        self.call(callee, admitted, position, known, target_reduces)
    }

    /// Each argument read only where `admitted` holds: a value is NULL
    /// elsewhere and `*` is 1 where admitted and NULL elsewhere, so an
    /// aggregate that ignores NULL inputs reads only the admitted rows.
    fn admitted_args(
        &mut self,
        args: Vec<Arg>,
        admitted: TruthId,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<Vec<Arg>, Refusal> {
        let mut read = Vec::with_capacity(args.len());
        for arg in args {
            read.push(match arg {
                Arg::Value { expr, distinct } => Arg::Value {
                    expr: self.case(None, vec![(CaseArm::Truth(admitted), expr)], None, switches)?,
                    distinct,
                },
                Arg::Star => {
                    let one = self.constant(LiteralValue::integer(1));
                    Arg::Value {
                        expr: self.case(None, vec![(CaseArm::Truth(admitted), one)], None, switches)?,
                        distinct: false,
                    }
                }
            });
        }
        Ok(read)
    }

    /// A guarded call (`f:(args | guard)`, domain-expressions FN.45). On an
    /// aggregate the guard filters contributions: for one that ignores NULL
    /// inputs, filtering the rows and nulling the arguments are one answer,
    /// given as `call_within` gives a pivot column's; one that keeps NULL
    /// inputs needs FILTER, which is not built. On a row-wise call the
    /// value exists only where the guard holds: `CASE WHEN guard THEN
    /// f(args) END`. A call whose grade contradicts its position is built
    /// as one, for its position to refuse.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn guarded_call(
        &mut self,
        callee: Callee,
        args: Vec<Arg>,
        position: CallPosition,
        known: Known,
        target_reduces: bool,
        admitted: TruthId,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<ExprId, Refusal> {
        match decide::grade::of(position, known, target_reduces) {
            super::Grade::Aggregate if decide::grade::ignores_null(&callee.name) => {
                self.call_within(callee, args, position, known, target_reduces, admitted, switches)
            }
            super::Grade::Aggregate => Err(crate::pipeline::middle::core::refuse::outside(
                "a guard on an aggregate that keeps NULL inputs (its contributions filtered by FILTER)",
            )),
            super::Grade::Scalar(_) => {
                let value = self.call(callee, args, position, known, target_reduces)?;
                self.case(None, vec![(CaseArm::Truth(admitted), value)], None, switches)
            }
            super::Grade::Contradicted(_) => self.call(callee, args, position, known, target_reduces),
        }
    }

    /// A guarded aggregate computed as a window (domain-expressions FN.45):
    /// the guard filters the frame's contributions, given for an aggregate
    /// that ignores NULL inputs by its admitted arguments, as on a reduction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn guarded_window(
        &mut self,
        callee: Callee,
        args: Vec<Arg>,
        admitted: TruthId,
        partition: Vec<ExprId>,
        order: Vec<OrderKey>,
        frame: Option<Frame>,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<ExprId, Refusal> {
        if !decide::grade::ignores_null(&callee.name) {
            return Err(crate::pipeline::middle::core::refuse::outside(
                "a guard on a windowed aggregate that keeps NULL inputs (its contributions filtered by FILTER)",
            ));
        }
        let args = self.admitted_args(args, admitted, switches)?;
        self.window(callee, args, partition, order, frame)
    }

    /// GROUP DELEGATES (pipe-operators FN.11): DISTINCT ON's ordered
    /// consumption of a group's rows. One delegate is one pick, the row of
    /// its group ranked first under the delegate's ordering (`keys` the
    /// group's key values, `order` the ordering; absent ordering, any row);
    /// each payload value is that one row's value, so every payload of a
    /// delegate reads the one rank. With no keys the group is the whole
    /// input: the reduction publishes one row even over an empty input, its
    /// payload NULL there (pipe-operators FN.9).
    pub(crate) fn pick(&mut self, keys: &[ExprId], order: Vec<OrderKey>, payload: &[ExprId]) -> Result<Vec<ExprId>, Refusal> {
        let rank = self.window(
            Callee {
                name: "row_number".to_string(),
            },
            Vec::new(),
            keys.to_vec(),
            order,
            None,
        )?;
        Ok(payload
            .iter()
            .map(|value| self.push_value(ExprKind::Pick { value: *value, rank }))
            .collect())
    }

    /// One column of a pivot (pipe-operators FN.10): the value over the
    /// group's rows `column` admits (its reductions already read only
    /// them, `call_within`), NULL where the group holds no such row (the
    /// absence law). A value read per row stands for its group only where
    /// the input is proven to hold at most one row per group and key
    /// (`single`); the one row's value is then its group's.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pivot_column(
        &mut self,
        column: TruthId,
        value: ExprId,
        keys: &[super::Cell],
        own: &BinderSet,
        single: bool,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<ExprId, Refusal> {
        let aggregate = |b: &mut Builder, name: &str, arg: ExprId| {
            b.call(
                Callee { name: name.to_string() },
                vec![Arg::Value { expr: arg, distinct: false }],
                CallPosition::Reduction,
                Known::Aggregate,
                false,
            )
        };
        let cell = if per_row_value(self, value, keys, own) {
            if !single {
                return Err(crate::pipeline::middle::core::refuse::unruled(
                    "which row a pivot publishes under a key a group may hold more than once (DOCKET: a pivot under a \
                     duplicated key)",
                ));
            }
            let admitted = self.case(None, vec![(CaseArm::Truth(column), value)], None, switches)?;
            aggregate(self, "max", admitted)?
        } else {
            value
        };
        let one = self.constant(LiteralValue::integer(1));
        let marked = self.case(None, vec![(CaseArm::Truth(column), one)], None, switches)?;
        let present = aggregate(self, "count", marked)?;
        let none = self.constant(LiteralValue::integer(0));
        let held = self.cmp(
            crate::pipeline::middle::facade::CmpOp::GreaterThan,
            present,
            none,
            super::Consumer::Filter,
            false,
            switches,
        )?;
        self.case(None, vec![(CaseArm::Truth(held), cell)], None, switches)
    }

    /// A window call: it reads the bytes of its arguments, its partition
    /// and its ordering.
    pub(crate) fn window(
        &mut self,
        callee: Callee,
        args: Vec<Arg>,
        partition: Vec<ExprId>,
        order: Vec<OrderKey>,
        frame: Option<Frame>,
    ) -> Result<ExprId, Refusal> {
        self.observe(
            values(&args).chain(partition.iter().copied()).chain(order.iter().map(|k| k.expr)),
            "a window",
        )?;
        Ok(self.push_value(ExprKind::Window {
            callee,
            args,
            partition,
            order,
            frame,
        }))
    }

    /// An operator: it reads its operands' bytes.
    pub(crate) fn infix(&mut self, op: BinOp, left: ExprId, right: ExprId) -> Result<ExprId, Refusal> {
        self.observe([left, right], "an operator")?;
        Ok(self.push_value(ExprKind::Infix(op, left, right)))
    }

    /// A case. An anchored case's literal arm compares the anchor with the
    /// literal as a match, consumed as a value; its equality class is
    /// decided here.
    pub(crate) fn case(
        &mut self,
        anchor: Option<ExprId>,
        arms: Vec<(CaseArm, ExprId)>,
        default: Option<ExprId>,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        use crate::pipeline::middle::core::heading::Interior;
        if let Some(anchor) = anchor {
            let structure = super::rel::structure_of(self, None, anchor)?;
            for (arm, _) in &arms {
                if let CaseArm::Literal(value) = arm {
                    let nulls = match value {
                        LiteralValue::Null => decide::document::Nulls::Null,
                        _ => decide::document::Nulls::Never,
                    };
                    decide::document::comparable(
                        compared(self, &structure, anchor),
                        decide::document::Compared {
                            structure: &Interior::flat(),
                            nulls,
                        },
                        crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
                        super::Consumer::Value,
                    )?;
                }
            }
        }
        let occurrences = anchor.map(|a| self.expr(a).occurrences().len()).unwrap_or(0);
        let arms = arms
            .into_iter()
            .map(|(arm, result)| {
                let test = match arm {
                    CaseArm::Literal(value) => CaseTest::Literal {
                        value,
                        class: decide::equality::class(
                            crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
                            occurrences,
                            true,
                            super::Consumer::Value,
                            switches,
                        ),
                    },
                    CaseArm::Truth(t) => CaseTest::Truth(t),
                };
                (test, result)
            })
            .collect();
        Ok(self.push_value(ExprKind::Case {
            anchor,
            arms,
            default,
        }))
    }

    pub(crate) fn crossed(&mut self, truth: TruthId) -> ExprId {
        self.push_value(ExprKind::Crossed(truth))
    }

    /// A scalar position over a relation, with its cardinality (W5 #17).
    pub(crate) fn scalar(&mut self, rel: RelId) -> ExprId {
        let cardinality = cardinality(self, rel);
        self.push_value(ExprKind::Scalar { rel, cardinality })
    }

    pub(crate) fn passenger_value(&mut self, passenger: PassengerId) -> ExprId {
        self.push_value(ExprKind::Passenger(passenger))
    }

    /// A collection of records or tuples, standing as a reduction item. A
    /// collected record's members publish as the collection's interior
    /// heading forms them, a member no name answers to under a minted key.
    /// Each member is judged as a value-position constructor's member is.
    pub(crate) fn collect(
        &mut self,
        layout: crate::pipeline::middle::core::heading::Layout,
        members: Vec<super::CollectMember>,
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        use super::rel::structure_of;
        fn items(members: &[super::CollectMember], out: &mut Vec<super::Item>, keys: &mut Vec<ExprId>) {
            for member in members {
                match member {
                    super::CollectMember::Item(item) => out.push(item.clone()),
                    super::CollectMember::Nested(_, _, inner) => items(inner, out, keys),
                    super::CollectMember::Metadata(_, level) => {
                        let (chain, _, inner) = level.chain();
                        keys.extend(chain);
                        items(inner, out, keys);
                    }
                }
            }
        }
        let mut flat = Vec::new();
        let mut keys = Vec::new();
        items(&members, &mut flat, &mut keys);
        for item in &flat {
            let structure = structure_of(self, None, item.expr)?;
            decide::document::member(&structure)?;
            super::rel::node_reached(self, item.expr, &structure)?;
        }
        // A metadata key partitions its rows by value, as a grouping key does.
        for key in keys {
            decide::document::set_member(&[&structure_of(self, None, key)?], "a metadata key")?;
        }
        Ok(self.push_value(ExprKind::Collect { layout, members }))
    }

    /// A metadata collector standing as a reduction item: its target's
    /// members judged as a collection's are, and its keys as grouping keys.
    pub(crate) fn metadata(
        &mut self,
        level: super::MetaLevel,
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        use super::rel::structure_of;
        let (keys, _, members) = level.chain();
        let mut flat = Vec::new();
        visit_collected(members, &mut |e| flat.push(e));
        for e in flat {
            let structure = structure_of(self, None, e)?;
            decide::document::member(&structure)?;
            super::rel::node_reached(self, e, &structure)?;
        }
        for key in keys {
            decide::document::set_member(&[&structure_of(self, None, key)?], "a metadata key")?;
        }
        Ok(self.push_value(ExprKind::Metadata { level }))
    }

    /// One record or tuple made in value position. A record's key is its
    /// member's naming, or the name of the position a reference reads, or
    /// else minted; the same occurrence under one key is one member (the
    /// first stands), and one key two different values supply refuses
    /// (engines disagree on duplicate keys, so it never reaches lowering).
    pub(crate) fn construct(
        &mut self,
        layout: crate::pipeline::middle::core::heading::Layout,
        members: Vec<super::Item>,
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        use super::rel::structure_of;
        use crate::pipeline::middle::core::heading::Layout;
        for item in &members {
            decide::document::member(&structure_of(self, None, item.expr)?)?;
        }
        let kept = match layout {
            Layout::Record => record_keys(self, members, &[])?,
            Layout::Tuple => members,
        };
        for item in &kept {
            super::rel::node_reached(self, item.expr, &structure_of(self, None, item.expr)?)?;
        }
        Ok(self.push_value(ExprKind::Construct { layout, members: kept }))
    }

    /// The part of `source` a path addresses (FN.20: one path, a scalar
    /// reach). What it addresses is decided by `decide::document::reach`,
    /// which refuses a declared non-document column and an extracted value
    /// read across a read boundary.
    pub(crate) fn path(
        &mut self,
        source: ExprId,
        path: crate::pipeline::middle::facade::Path,
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        let structure = super::rel::structure_of(self, None, source)?;
        decide::document::reach(&structure, &path)?;
        super::rel::node_reached(self, source, &structure)?;
        Ok(self.push_value(ExprKind::Path { source, path }))
    }

    /// A relation definition's value formal read in its body: its actual,
    /// read across the definition's boundary. Where the boundary leaves
    /// what is known of the actual unchanged, the read is the actual.
    pub(crate) fn formal_read(&mut self, actual: ExprId) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        let structure = super::rel::structure_of(self, None, actual)?;
        if decide::document::read(&structure) == structure {
            return Ok(actual);
        }
        Ok(self.push_value(ExprKind::Across(actual)))
    }

    /// A value definition's argument as the body receives it: a value read
    /// where the call stands is one occurrence already (a column, a merged
    /// key, a constant, a passenger, an argument or formal received from an
    /// enclosing call) and is the argument itself; any other is evaluated
    /// once, where the call stands.
    pub(crate) fn argument(
        &mut self,
        value: ExprId,
        standing: &[BinderId],
    ) -> Result<ExprId, crate::pipeline::middle::core::refuse::Refusal> {
        Ok(match self.expr(value).kind() {
            ExprKind::Col(..)
            | ExprKind::Merged(_)
            | ExprKind::Const(_)
            | ExprKind::Passenger(_)
            | ExprKind::Argument { .. }
            | ExprKind::Across(_) => value,
            _ => {
                let population = if population_sensitive(self, value) { standing.to_vec() } else { Vec::new() };
                let structure = Box::new(super::rel::structure_of(self, None, value)?);
                self.push_value(ExprKind::Argument {
                    value,
                    population,
                    structure,
                })
            }
        })
    }
}

/// A record member's key: its naming, or the name of the position a
/// reference reads.
pub(crate) fn record_key(arena: &impl Judging, item: &super::Item) -> Option<crate::pipeline::middle::core::heading::Name> {
    use super::Naming;
    match &item.naming {
        Naming::As(key) => Some(key.clone()),
        Naming::Reference | Naming::Glob | Naming::QualifiedGlob => {
            super::rel::referenced_position(arena, item.expr).and_then(|p| p.answering_name().cloned())
        }
        Naming::Computed | Naming::Abstain => None,
    }
}

/// THE RECORD KEY LAW for a record made in value position: a member's key
/// is its naming, or the name of the position a reference reads; the same
/// occurrence under one key is one member (the first stands); one key two
/// different values supply refuses (engines disagree on duplicate keys). A
/// member no key names keeps its minted key. `levels` are keys taken before
/// the members.
pub(crate) fn record_keys(
    arena: &impl Judging,
    members: Vec<super::Item>,
    levels: &[crate::pipeline::middle::core::heading::Name],
) -> Result<Vec<super::Item>, crate::pipeline::middle::core::refuse::Refusal> {
    use super::rel::referenced_cell;
    use crate::pipeline::middle::core::refuse;
    let mut kept = Vec::with_capacity(members.len());
    let mut keys: Vec<(crate::pipeline::middle::core::heading::Name, Option<super::Cell>)> =
        levels.iter().map(|n| (n.clone(), None)).collect();
    for item in members {
        let Some(key) = record_key(arena, &item) else {
            kept.push(item);
            continue;
        };
        let cell = referenced_cell(arena, item.expr);
        if let Some((_, earlier)) = keys.iter().find(|(k, _)| *k == key) {
            if cell.is_some() && *earlier == cell {
                continue;
            }
            return Err(refuse::key_collision(&key));
        }
        keys.push((key, cell));
        kept.push(item);
    }
    Ok(kept)
}

/// The values an argument list carries.
fn values(args: &[Arg]) -> impl Iterator<Item = ExprId> + '_ {
    args.iter().filter_map(|a| match a {
        Arg::Value { expr, .. } => Some(*expr),
        Arg::Star => None,
    })
}

/// What a compared value states of the NULLs inside it: a literal its
/// own, a structure made in place its members', anything else nothing.
pub(crate) fn nulls(arena: &impl Judging, e: ExprId) -> decide::document::Nulls {
    use decide::document::Nulls;
    match arena.expr(e).kind() {
        ExprKind::Const(LiteralValue::Null) => Nulls::Null,
        ExprKind::Const(_) => Nulls::Never,
        ExprKind::Construct { members, .. } => Nulls::Members(members.iter().map(|m| nulls(arena, m.expr)).collect()),
        _ => Nulls::Unknown,
    }
}

/// One operand of a comparison, as `decide::document::comparable` reads it.
pub(crate) fn compared<'a>(
    arena: &impl Judging,
    structure: &'a crate::pipeline::middle::core::heading::Interior,
    e: ExprId,
) -> decide::document::Compared<'a> {
    decide::document::Compared {
        structure,
        nulls: nulls(arena, e),
    }
}

/// A written case arm's test.
pub(crate) enum CaseArm {
    Literal(LiteralValue),
    Truth(TruthId),
}

/// Every value a collection's members read, a metadata level's keys among
/// them, at every depth.
pub(crate) fn visit_collected(members: &[super::CollectMember], visit: &mut dyn FnMut(ExprId)) {
    for member in members {
        match member {
            super::CollectMember::Item(item) => visit(item.expr),
            super::CollectMember::Nested(_, _, inner) => visit_collected(inner, visit),
            super::CollectMember::Metadata(_, level) => {
                let (keys, _, inner) = level.chain();
                keys.into_iter().for_each(&mut *visit);
                visit_collected(inner, visit);
            }
        }
    }
}

/// A value's free binders and read occurrences, from its children.
pub(crate) fn value_facts(arena: &impl Judging, kind: &ExprKind) -> (BinderSet, BTreeSet<Occurrence>) {
    let mut fv = BinderSet::new();
    let mut occ = BTreeSet::new();
    let child = |id: ExprId, fv: &mut BinderSet, occ: &mut BTreeSet<Occurrence>| {
        let node = arena.expr(id);
        fv.extend(node.fv().iter().copied());
        occ.extend(node.occurrences().iter().copied());
    };
    match kind {
        ExprKind::Col(binder, _) => {
            fv.insert(*binder);
            occ.insert(Occurrence::Binder(*binder));
        }
        ExprKind::Merged(merge) => {
            fv.extend(merge_binders(arena, *merge));
            occ.insert(Occurrence::Merge(*merge));
        }
        ExprKind::Const(_) => {}
        ExprKind::Call { args, .. } => {
            for arg in args {
                if let Arg::Value { expr, .. } = arg {
                    child(*expr, &mut fv, &mut occ);
                }
            }
        }
        ExprKind::Window {
            args,
            partition,
            order,
            ..
        } => {
            for arg in args {
                if let Arg::Value { expr, .. } = arg {
                    child(*expr, &mut fv, &mut occ);
                }
            }
            for e in partition {
                child(*e, &mut fv, &mut occ);
            }
            for key in order {
                child(key.expr, &mut fv, &mut occ);
            }
        }
        ExprKind::Infix(_, l, r) => {
            child(*l, &mut fv, &mut occ);
            child(*r, &mut fv, &mut occ);
        }
        ExprKind::Case {
            anchor,
            arms,
            default,
        } => {
            if let Some(a) = anchor {
                child(*a, &mut fv, &mut occ);
            }
            for (test, result) in arms {
                if let CaseTest::Truth(t) = test {
                    let node = arena.truth(*t);
                    fv.extend(node.fv().iter().copied());
                    occ.extend(node.occurrences().iter().copied());
                }
                child(*result, &mut fv, &mut occ);
            }
            if let Some(d) = default {
                child(*d, &mut fv, &mut occ);
            }
        }
        ExprKind::Crossed(t) => {
            let node = arena.truth(*t);
            fv.extend(node.fv().iter().copied());
            occ.extend(node.occurrences().iter().copied());
        }
        ExprKind::Scalar { rel, .. } => {
            let node = arena.rel(*rel);
            fv.extend(node.fv().iter().copied());
            occ.extend(node.fv().iter().map(|b| Occurrence::Binder(*b)));
        }
        ExprKind::Passenger(_) => {}
        ExprKind::Collect { members, .. } => {
            visit_collected(members, &mut |e| child(e, &mut fv, &mut occ));
        }
        ExprKind::Metadata { level } => {
            let (keys, _, members) = level.chain();
            for key in keys {
                child(key, &mut fv, &mut occ);
            }
            visit_collected(members, &mut |e| child(e, &mut fv, &mut occ));
        }
        ExprKind::Construct { members, .. } => {
            for m in members {
                child(m.expr, &mut fv, &mut occ);
            }
        }
        ExprKind::Path { source, .. } | ExprKind::Across(source) => child(*source, &mut fv, &mut occ),
        ExprKind::Argument { value, population, .. } => {
            child(*value, &mut fv, &mut occ);
            fv.extend(population.iter().copied());
        }
        ExprKind::Pick { value, rank } => {
            child(*value, &mut fv, &mut occ);
            child(*rank, &mut fv, &mut occ);
        }
    }
    (fv, occ)
}

/// Whether a value in a reduction item has one answer per row standing
/// outside every reduction: a column that is not one of the group's keys
/// (`keys`, the cells the grouping keys read), reached through row-wise
/// forms. A reducing call, a collection and a constant stand for the group.
pub(crate) fn per_row_value(arena: &impl Judging, e: ExprId, keys: &[super::Cell], own: &BinderSet) -> bool {
    use crate::pipeline::middle::core::node::rel::referenced_cell;
    match arena.expr(e).kind() {
        ExprKind::Col(..) | ExprKind::Merged(_) => referenced_cell(arena, e).is_none_or(|cell| !keys.contains(&cell)),
        ExprKind::Const(_)
        | ExprKind::Passenger(_)
        | ExprKind::Collect { .. }
        | ExprKind::Metadata { .. }
        | ExprKind::Pick { .. } => false,
        ExprKind::Call { grade, args, .. } => match grade {
            super::Grade::Aggregate => false,
            super::Grade::Scalar(_) | super::Grade::Contradicted(_) => args.iter().any(|a| match a {
                Arg::Value { expr, .. } => per_row_value(arena, *expr, keys, own),
                Arg::Star => false,
            }),
        },
        ExprKind::Window { .. } => true,
        ExprKind::Infix(_, l, r) => per_row_value(arena, *l, keys, own) || per_row_value(arena, *r, keys, own),
        ExprKind::Case { anchor, arms, default } => {
            anchor.is_some_and(|a| per_row_value(arena, a, keys, own))
                || arms.iter().any(|(test, result)| {
                    let tested = match test {
                        CaseTest::Truth(t) => per_row_truth(arena, *t, keys, own),
                        CaseTest::Literal { .. } => false,
                    };
                    tested || per_row_value(arena, *result, keys, own)
                })
                || default.is_some_and(|d| per_row_value(arena, d, keys, own))
        }
        ExprKind::Crossed(t) => per_row_truth(arena, *t, keys, own),
        ExprKind::Scalar { rel, .. } => per_row_read(arena, *rel, keys, own),
        ExprKind::Construct { members, .. } => members.iter().any(|m| per_row_value(arena, m.expr, keys, own)),
        ExprKind::Path { source, .. } | ExprKind::Across(source) => per_row_value(arena, *source, keys, own),
        // An argument evaluated where a call outside the group stands is
        // one value for the whole group; one of the group's own calls is its
        // value's.
        ExprKind::Argument { value, .. } => {
            let fv = arena.expr(e).fv();
            (fv.is_empty() || fv.iter().any(|b| own.contains(b))) && per_row_value(arena, *value, keys, own)
        }
    }
}

fn per_row_truth(arena: &impl Judging, t: TruthId, keys: &[super::Cell], own: &BinderSet) -> bool {
    use super::TruthKind;
    match arena.truth(t).kind() {
        TruthKind::Cmp { left, right, .. } => per_row_value(arena, *left, keys, own) || per_row_value(arena, *right, keys, own),
        TruthKind::And(parts) | TruthKind::Or(parts) => parts.iter().any(|p| per_row_truth(arena, *p, keys, own)),
        TruthKind::Not(p) => per_row_truth(arena, *p, keys, own),
        TruthKind::Exists { rel, .. } => per_row_read(arena, *rel, keys, own),
        TruthKind::Sigma { args, .. } => args.iter().any(|a| per_row_value(arena, *a, keys, own)),
    }
}

/// Whether a subquery standing in a reduction item reads a column of the
/// group's input that is not a key.
fn per_row_read(arena: &impl Judging, rel: RelId, keys: &[super::Cell], own: &BinderSet) -> bool {
    use crate::pipeline::middle::core::node::rel::referenced_cell;
    use super::walk::Child;
    let free = arena.rel(rel).fv();
    let mut seen = super::walk::Reach::default();
    let mut pending = vec![Child::Rel(rel)];
    while let Some(c) = pending.pop() {
        let new = match c {
            Child::Rel(r) => seen.rels.insert(r),
            Child::Expr(e) => seen.exprs.insert(e),
            Child::Truth(t) => seen.truths.insert(t),
        };
        if !new {
            continue;
        }
        if let Child::Expr(x) = c {
            match arena.expr(x).kind() {
                ExprKind::Col(..) | ExprKind::Merged(_)
                    if arena.expr(x).fv().iter().any(|b| free.contains(b))
                        && referenced_cell(arena, x).is_some_and(|cell| !keys.contains(&cell)) =>
                {
                    return true;
                }
                // An argument the relation receives from outside is a value
                // of its call, judged where the call stands.
                ExprKind::Argument { .. } if arena.expr(x).fv().iter().all(|b| free.contains(b)) => {
                    if per_row_value(arena, x, keys, own) {
                        return true;
                    }
                    continue;
                }
                _ => {}
            }
        }
        pending.extend(super::walk::of(arena, c));
    }
    false
}

/// The binders a merged key reads: every operand, through earlier merges.
/// Whether a value depends on the rows around its own row: a window call,
/// an aggregate call or a collection standing in it outside any nested
/// relation (whose rows are its own). A step evaluating such a value over a
/// population is population-sensitive.
pub(crate) fn population_sensitive(arena: &impl Judging, e: ExprId) -> bool {
    use super::walk::{of_expr, Child};
    match arena.expr(e).kind() {
        ExprKind::Window { .. } | ExprKind::Collect { .. } | ExprKind::Metadata { .. } | ExprKind::Pick { .. } => true,
        ExprKind::Call { grade, .. } if *grade == super::Grade::Aggregate => true,
        ExprKind::Scalar { .. } => false,
        kind => of_expr(kind).into_iter().any(|c| match c {
            Child::Expr(c) => population_sensitive(arena, c),
            Child::Truth(t) => truth_population_sensitive(arena, t),
            Child::Rel(_) => false,
        }),
    }
}

/// Whether a truth reads a population-sensitive value.
pub(crate) fn truth_population_sensitive(arena: &impl Judging, t: TruthId) -> bool {
    use super::walk::{of_truth, Child};
    of_truth(arena.truth(t).kind()).into_iter().any(|c| match c {
        Child::Expr(e) => population_sensitive(arena, e),
        Child::Truth(t) => truth_population_sensitive(arena, t),
        Child::Rel(_) => false,
    })
}

/// Whether some relation's rows supply a value even where it names no
/// column: it evaluates a window, an aggregate or a collection over a
/// population, or reads a relation (a scalar subquery, an existence).
pub(crate) fn relation_supplied(arena: &impl Judging, e: ExprId) -> bool {
    use super::walk::{of_expr, Child};
    match arena.expr(e).kind() {
        ExprKind::Window { .. }
        | ExprKind::Collect { .. }
        | ExprKind::Metadata { .. }
        | ExprKind::Pick { .. }
        | ExprKind::Scalar { .. } => true,
        ExprKind::Call { grade, .. } if *grade == super::Grade::Aggregate => true,
        kind => of_expr(kind).into_iter().any(|c| match c {
            Child::Expr(c) => relation_supplied(arena, c),
            Child::Truth(t) => truth_relation_supplied(arena, t),
            Child::Rel(_) => true,
        }),
    }
}

fn truth_relation_supplied(arena: &impl Judging, t: TruthId) -> bool {
    use super::walk::{of_truth, Child};
    of_truth(arena.truth(t).kind()).into_iter().any(|c| match c {
        Child::Expr(e) => relation_supplied(arena, e),
        Child::Truth(t) => truth_relation_supplied(arena, t),
        Child::Rel(_) => true,
    })
}

/// Whether a value evaluates a window itself: a window call reached without
/// crossing a nested relation or a scalar subquery, each of which keeps its
/// own evaluation rules. A column a window computed earlier is a column.
pub(crate) fn evaluates_window(arena: &impl Judging, e: ExprId) -> bool {
    use super::walk::{of_expr, Child};
    match arena.expr(e).kind() {
        ExprKind::Window { .. } => true,
        ExprKind::Scalar { .. } => false,
        kind => of_expr(kind).into_iter().any(|c| match c {
            Child::Expr(c) => evaluates_window(arena, c),
            Child::Truth(t) => truth_evaluates_window(arena, t),
            Child::Rel(_) => false,
        }),
    }
}

/// Whether a truth evaluates a window itself (see [`evaluates_window`]).
pub(crate) fn truth_evaluates_window(arena: &impl Judging, t: TruthId) -> bool {
    use super::walk::{of_truth, Child};
    of_truth(arena.truth(t).kind()).into_iter().any(|c| match c {
        Child::Expr(e) => evaluates_window(arena, e),
        Child::Truth(t) => truth_evaluates_window(arena, t),
        Child::Rel(_) => false,
    })
}

pub(crate) fn merge_binders(arena: &impl Arena, merge: MergeId) -> BinderSet {
    let site = arena.merge(merge);
    let mut out = BinderSet::new();
    out.insert(site.right().0);
    match site.left() {
        super::Cell::Col(b, _) => {
            out.insert(b);
        }
        super::Cell::Merged(m) => out.extend(merge_binders(arena, m)),
    }
    out
}

/// A scalar position is bounded when its relation ends in a reduction with
/// no keys or in a bound of at most one row; otherwise it maps directly
/// and the engine's semantics apply to more than one row.
pub(crate) fn cardinality(arena: &impl Judging, rel: RelId) -> Cardinality {
    match arena.rel(rel).kind() {
        RelKind::Pipe {
            op: super::PipeOp::Group { keys, .. },
            ..
        } if keys.is_empty() => Cardinality::Bounded,
        RelKind::Order {
            bound: Some(bound), ..
        } if bound.count.is_some_and(|n| n <= 1) => Cardinality::Bounded,
        RelKind::Run(run) => match run.quals() {
            [super::Qual::Member(only)] => cardinality(arena, only.rel()),
            [] | [_, ..] => Cardinality::Direct,
        },
        // An act's receipt holds at most one row (receipt-algebra: a YES
        // is one row, a NO none), and so does its witness.
        RelKind::Receipt { .. } | RelKind::Witnessed { .. } => Cardinality::Bounded,
        RelKind::Pipe { .. }
        | RelKind::Order { .. }
        | RelKind::Read { .. }
        | RelKind::Lit { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Fix(_)
        | RelKind::Apply { .. } => Cardinality::Direct,
    }
}
