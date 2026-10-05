// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The children of every node kind, and the nodes reachable from a set of
//! roots: the one enumeration the analyses and the judgments over a
//! subtree share.

use super::{Arg, CaseTest, CollectMember, ExprKind, PipeOp, Qual, ReadAccess, ReadSource, RelKind, Slot, SigmaProof, TruthKind};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::{Known, Origin};
use crate::pipeline::middle::core::ids::{ExprId, RelId, TruthId};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Child {
    Rel(RelId),
    Expr(ExprId),
    Truth(TruthId),
}

/// A relation's children, in construction order.
pub(crate) fn of_rel(arena: &impl Arena, kind: &RelKind) -> Vec<Child> {
    let mut out = Vec::new();
    match kind {
        RelKind::Read { source, access } => {
            match source {
                ReadSource::Local(r) => out.push(Child::Rel(*r)),
                ReadSource::Function { args, .. } => out.extend(args.iter().map(|a| Child::Expr(*a))),
                ReadSource::Catalog { .. }
                | ReadSource::Staged(_)
                | ReadSource::Frontier(_)
                | ReadSource::Created { .. } => {}
            }
            if let ReadAccess::Slots(slots) = access {
                for slot in slots {
                    if let Slot::Constraint { value, .. } = slot {
                        out.push(Child::Expr(*value));
                    }
                }
            }
        }
        RelKind::Lit { header, rows, .. } => {
            out.extend(rows.iter().flatten().map(|e| Child::Expr(*e)));
            for slot in header {
                if let super::HeaderSlot::Constraint { value, .. } = slot {
                    out.push(Child::Expr(*value));
                }
            }
        }
        RelKind::Receipt { act, cells, .. } => {
            out.extend(act.reads().into_iter().map(Child::Rel));
            for cell in cells {
                match cell {
                    super::effect::ReceiptCell::Const(e) => out.push(Child::Expr(*e)),
                    super::effect::ReceiptCell::Interior(r) => out.push(Child::Rel(*r)),
                }
            }
        }
        RelKind::Run(run) => {
            for qual in run.quals() {
                match qual {
                    Qual::Member(m) => out.push(Child::Rel(m.rel())),
                    Qual::Guard(g) => out.push(Child::Truth(g.truth())),
                    Qual::Bound(_) => {}
                }
            }
        }
        RelKind::Pipe { input, op } => {
            out.push(Child::Rel(*input));
            out.extend(super::run::pipe_exprs(op).into_iter().map(Child::Expr));
            if let PipeOp::Carry(passengers) = op {
                for p in passengers {
                    if let super::Passenger::Configured { value, .. } = arena.passenger(*p) {
                        out.push(Child::Expr(*value));
                    }
                }
            }
        }
        RelKind::Order { input, keys, .. } => {
            out.push(Child::Rel(*input));
            out.extend(keys.iter().map(|k| Child::Expr(k.expr)));
        }
        RelKind::SetOp { left, right, .. } | RelKind::Minus { left, right, .. } => {
            out.push(Child::Rel(*left));
            out.push(Child::Rel(*right));
        }
        RelKind::Meta { input } | RelKind::Witnessed { input, .. } => out.push(Child::Rel(*input)),
        RelKind::Unnest { value, expansion } => {
            out.push(Child::Expr(*value));
            out.extend(carried(arena, *value).map(Child::Rel));
            for level in &expansion.levels {
                for bind in &level.binds {
                    if let super::BindRole::Constrain { value, .. } = bind.role {
                        out.push(Child::Expr(value));
                    }
                }
            }
        }
        RelKind::Family { clauses, .. } => {
            for c in clauses {
                out.push(Child::Rel(c.body));
                if let Some(g) = c.guard {
                    out.push(Child::Truth(g));
                }
            }
        }
        RelKind::Fix(fix) => out.extend(fix.anchors().iter().chain(fix.steps()).map(|r| Child::Rel(*r))),
        RelKind::Apply { instance } => out.push(Child::Rel(arena.instance(*instance).body())),
    }
    out
}

/// The relation whose rows a drilled value holds, when a receipt carries
/// it there: the drill's rows are that relation's rows.
pub(crate) fn carried(arena: &impl Arena, value: ExprId) -> Option<RelId> {
    let ExprKind::Col(binder, at) = arena.expr(value).kind() else {
        return None;
    };
    match arena.binder(*binder).heading().positions().get(*at as usize).map(|p| &p.interior.known) {
        Some(Known::Shape(_, Origin::Carried(rel))) => Some(*rel),
        _ => None,
    }
}

/// A value's children.
pub(crate) fn of_expr(kind: &ExprKind) -> Vec<Child> {
    let mut out = Vec::new();
    let args = |args: &[Arg], out: &mut Vec<Child>| {
        for a in args {
            if let Arg::Value { expr, .. } = a {
                out.push(Child::Expr(*expr));
            }
        }
    };
    match kind {
        ExprKind::Col(..) | ExprKind::Merged(_) | ExprKind::Const(_) | ExprKind::Passenger(_) => {}
        ExprKind::Call { args: a, .. } => args(a, &mut out),
        ExprKind::Window {
            args: a,
            partition,
            order,
            ..
        } => {
            args(a, &mut out);
            out.extend(partition.iter().map(|e| Child::Expr(*e)));
            out.extend(order.iter().map(|k| Child::Expr(k.expr)));
        }
        ExprKind::Infix(_, l, r) => {
            out.push(Child::Expr(*l));
            out.push(Child::Expr(*r));
        }
        ExprKind::Case { anchor, arms, default } => {
            if let Some(a) = anchor {
                out.push(Child::Expr(*a));
            }
            for (test, result) in arms {
                if let CaseTest::Truth(t) = test {
                    out.push(Child::Truth(*t));
                }
                out.push(Child::Expr(*result));
            }
            if let Some(d) = default {
                out.push(Child::Expr(*d));
            }
        }
        ExprKind::Crossed(t) => out.push(Child::Truth(*t)),
        ExprKind::Scalar { rel, .. } => out.push(Child::Rel(*rel)),
        ExprKind::Collect { members, .. } => {
            fn walk(members: &[CollectMember], out: &mut Vec<Child>) {
                for m in members {
                    match m {
                        CollectMember::Item(item) => out.push(Child::Expr(item.expr)),
                        CollectMember::Nested(_, _, inner) => walk(inner, out),
                        CollectMember::Metadata(_, level) => {
                            let (keys, _, inner) = level.chain();
                            out.extend(keys.into_iter().map(Child::Expr));
                            walk(inner, out);
                        }
                    }
                }
            }
            walk(members, &mut out);
        }
        ExprKind::Metadata { level } => {
            let (keys, _, members) = level.chain();
            out.extend(keys.into_iter().map(Child::Expr));
            super::expr::visit_collected(members, &mut |e| out.push(Child::Expr(e)));
        }
        ExprKind::Construct { members, .. } => out.extend(members.iter().map(|m| Child::Expr(m.expr))),
        ExprKind::Path { source, .. } | ExprKind::Across(source) | ExprKind::Argument { value: source, .. } => {
            out.push(Child::Expr(*source))
        }
        ExprKind::Pick { value, rank } => {
            out.push(Child::Expr(*value));
            out.push(Child::Expr(*rank));
        }
    }
    out
}

/// A truth's children.
pub(crate) fn of_truth(kind: &TruthKind) -> Vec<Child> {
    match kind {
        TruthKind::Cmp { left, right, .. } => vec![Child::Expr(*left), Child::Expr(*right)],
        TruthKind::And(parts) | TruthKind::Or(parts) => parts.iter().map(|t| Child::Truth(*t)).collect(),
        TruthKind::Not(p) => vec![Child::Truth(*p)],
        TruthKind::Exists { rel, .. } => vec![Child::Rel(*rel)],
        TruthKind::Sigma { proof, args, .. } => {
            let mut out: Vec<Child> = args.iter().map(|e| Child::Expr(*e)).collect();
            if let SigmaProof::Rule { expansion, .. } = proof {
                out.push(Child::Truth(*expansion));
            }
            out
        }
    }
}

/// The children of any node.
pub(crate) fn of(arena: &impl Arena, child: Child) -> Vec<Child> {
    match child {
        Child::Rel(r) => of_rel(arena, arena.rel(r).kind()),
        Child::Expr(e) => of_expr(arena.expr(e).kind()),
        Child::Truth(t) => of_truth(arena.truth(t).kind()),
    }
}

/// Every node reachable from `roots`, each once.
#[derive(Default)]
pub(crate) struct Reach {
    pub(crate) rels: BTreeSet<RelId>,
    pub(crate) exprs: BTreeSet<ExprId>,
    pub(crate) truths: BTreeSet<TruthId>,
}

pub(crate) fn reachable(arena: &impl Arena, roots: &[Child]) -> Reach {
    let mut reach = Reach::default();
    let mut pending: Vec<Child> = roots.to_vec();
    while let Some(c) = pending.pop() {
        let new = match c {
            Child::Rel(r) => reach.rels.insert(r),
            Child::Expr(e) => reach.exprs.insert(e),
            Child::Truth(t) => reach.truths.insert(t),
        };
        if new {
            pending.extend(of(arena, c));
        }
    }
    reach
}
