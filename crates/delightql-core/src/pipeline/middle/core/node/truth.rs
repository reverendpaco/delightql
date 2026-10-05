// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Truth constructors. A comparison's equality class is decided here, from
//! its operands and where its truth is consumed (W5 #10).

use super::{BinderSet, Consumer, Occurrence, SigmaProof, TruthKind, TruthNode};
use crate::pipeline::middle::core::decide;
use crate::pipeline::middle::core::graph::{Arena, Builder, Judging};
use crate::pipeline::middle::core::ids::{ExprId, RelId, TruthId};
use crate::pipeline::middle::core::switches::Switches;
use crate::pipeline::middle::facade::CmpOp;
use std::collections::BTreeSet;

impl Builder {
    fn push_truth_kind(&mut self, kind: TruthKind) -> TruthId {
        let (fv, occurrences) = truth_facts(self, &kind);
        self.push_truth(TruthNode {
            kind,
            fv,
            occurrences,
        })
    }

    /// A comparison. `own_row` says whether it reads a row of the relation
    /// it stands in, rather than only the enclosing row. A comparison of
    /// structures is judged here (`decide::document::comparable`).
    pub(crate) fn cmp(
        &mut self,
        op: CmpOp,
        left: ExprId,
        right: ExprId,
        consumer: Consumer,
        own_row: bool,
        switches: &Switches,
    ) -> Result<TruthId, crate::pipeline::middle::core::refuse::Refusal> {
        use super::expr::compared;
        use super::rel::structure_of;
        let (ls, rs) = (structure_of(self, None, left)?, structure_of(self, None, right)?);
        decide::document::comparable(compared(self, &ls, left), compared(self, &rs, right), op, consumer)?;
        let mut occ = self.expr(left).occurrences().clone();
        occ.extend(self.expr(right).occurrences().iter().copied());
        let class = decide::equality::class(op, occ.len(), own_row, consumer, switches);
        Ok(self.push_truth_kind(TruthKind::Cmp {
            op,
            left,
            right,
            consumer,
            own_row,
            class,
        }))
    }

    pub(crate) fn and(&mut self, parts: Vec<TruthId>) -> TruthId {
        self.push_truth_kind(TruthKind::And(parts))
    }

    pub(crate) fn or(&mut self, parts: Vec<TruthId>) -> TruthId {
        self.push_truth_kind(TruthKind::Or(parts))
    }

    pub(crate) fn not(&mut self, part: TruthId) -> TruthId {
        self.push_truth_kind(TruthKind::Not(part))
    }

    pub(crate) fn exists(&mut self, positive: bool, rel: RelId) -> TruthId {
        self.push_truth_kind(TruthKind::Exists { positive, rel })
    }

    /// A predicate cited over values. One the engine serves reads its
    /// arguments' bytes; an authored rule's expansion judges its own.
    pub(crate) fn sigma(
        &mut self,
        proof: SigmaProof,
        args: Vec<ExprId>,
        positive: bool,
    ) -> Result<TruthId, crate::pipeline::middle::core::refuse::Refusal> {
        if let SigmaProof::Served { .. } = &proof {
            for arg in &args {
                decide::document::observed(&super::rel::structure_of(self, None, *arg)?, "a predicate")?;
            }
        }
        Ok(self.push_truth_kind(TruthKind::Sigma {
            proof,
            args,
            positive,
        }))
    }
}

/// A truth's free binders and read occurrences, from its children.
pub(crate) fn truth_facts(arena: &impl Judging, kind: &TruthKind) -> (BinderSet, BTreeSet<Occurrence>) {
    let mut fv = BinderSet::new();
    let mut occ = BTreeSet::new();
    let expr = |id: ExprId, fv: &mut BinderSet, occ: &mut BTreeSet<Occurrence>| {
        fv.extend(arena.expr(id).fv().iter().copied());
        occ.extend(arena.expr(id).occurrences().iter().copied());
    };
    match kind {
        TruthKind::Cmp { left, right, .. } => {
            expr(*left, &mut fv, &mut occ);
            expr(*right, &mut fv, &mut occ);
        }
        TruthKind::And(parts) | TruthKind::Or(parts) => {
            for p in parts {
                fv.extend(arena.truth(*p).fv().iter().copied());
                occ.extend(arena.truth(*p).occurrences().iter().copied());
            }
        }
        TruthKind::Not(p) => {
            fv.extend(arena.truth(*p).fv().iter().copied());
            occ.extend(arena.truth(*p).occurrences().iter().copied());
        }
        TruthKind::Exists { rel, .. } => {
            fv.extend(arena.rel(*rel).fv().iter().copied());
            occ.extend(arena.rel(*rel).fv().iter().map(|b| Occurrence::Binder(*b)));
        }
        // An authored predicate is judged as its expanded truth: it reads
        // what the expansion reads, and an actual its body never reads
        // supplies no dependency. A served predicate reads its actuals.
        TruthKind::Sigma { proof, args, .. } => match proof {
            SigmaProof::Rule { expansion, .. } => {
                fv.extend(arena.truth(*expansion).fv().iter().copied());
                occ.extend(arena.truth(*expansion).occurrences().iter().copied());
            }
            SigmaProof::Served { .. } => {
                for a in args {
                    expr(*a, &mut fv, &mut occ);
                }
            }
        },
    }
    (fv, occ)
}
