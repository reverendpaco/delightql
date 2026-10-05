// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The debug recompute: every derived fact stored in the frozen graph is
//! computed again by its decider over the stored children and compared,
//! and every verdict a constructor passed is judged again. A mismatch is a
//! second decider or a post-birth write.

use super::decide;
use super::graph::{Arena, Graph};
use super::heading::form::{self, Formed, ReadForm, RunMember};
use super::heading::Heading;
use super::ids::{ExprId, RelId, TruthId};
use super::node::{
    effect, expr, rel, run, truth, CaseTest, EqClass, ExprKind, Grade, HeaderSlot, Qual, ReadAccess,
    ReadSource, RelKind, Slot, TruthKind,
};

pub(super) fn check(graph: &Graph) {
    let judged = &super::graph::Recheck::of(graph);
    if let super::graph::Statement::Program(program) = graph.statement() {
        assert!(
            decide::effect::program(judged, program.result()).ok().as_ref() == Some(program),
            "recompute: the program"
        );
    }
    let within_row = decide::equality::class(
        crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
        1,
        true,
        super::node::Consumer::Filter,
        graph.switches(),
    );
    for i in 0..graph.expr_count() {
        let id = ExprId::at(i);
        let node = graph.expr(id);
        let (fv, occ) = expr::value_facts(judged, node.kind());
        assert!(&fv == node.fv() && &occ == node.occurrences(), "recompute: value {id:?}");
        match node.kind() {
            ExprKind::Call { grade, .. } => {
                assert!(
                    !matches!(grade, Grade::Contradicted(_)),
                    "recompute: {id:?} is a call whose grade contradicts its position"
                );
            }
            ExprKind::Case { anchor, arms, .. } => {
                let occurrences = anchor.map(|a| graph.expr(a).occurrences().len()).unwrap_or(0);
                for (test, _) in arms {
                    if let CaseTest::Literal { class, .. } = test {
                        let again = decide::equality::class(
                            crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
                            occurrences,
                            true,
                            super::node::Consumer::Value,
                            graph.switches(),
                        );
                        assert!(again == *class, "recompute: match arm class of {id:?}");
                    }
                }
            }
            ExprKind::Scalar { rel, cardinality } => {
                assert!(expr::cardinality(judged, *rel) == *cardinality, "recompute: cardinality of {id:?}");
            }
            _ => {}
        }
    }
    for i in 0..graph.truth_count() {
        let id = TruthId::at(i);
        let node = graph.truth(id);
        let (fv, occ) = truth::truth_facts(judged, node.kind());
        assert!(&fv == node.fv() && &occ == node.occurrences(), "recompute: truth {id:?}");
        if let TruthKind::Cmp {
            op,
            left,
            right,
            consumer,
            own_row,
            class,
        } = node.kind()
        {
            let mut occ = graph.expr(*left).occurrences().clone();
            occ.extend(graph.expr(*right).occurrences().iter().copied());
            let again = decide::equality::class(*op, occ.len(), *own_row, *consumer, graph.switches());
            assert!(again == *class, "recompute: equality class of {id:?}");
        }
    }
    for i in 0..graph.rel_count() {
        let id = RelId::at(i);
        let node = graph.rel(id);
        assert!(&rel::rel_fv(judged, node.kind()) == node.fv(), "recompute: fv of {id:?}");
        if let RelKind::Run(r) = node.kind() {
            check_run(graph, id, r);
            continue;
        }
        let again = heading_of(graph, node.kind(), within_row);
        assert!(again.as_ref() == Some(node.heading()), "recompute: heading of {id:?}");
        match node.kind() {
            RelKind::Read {
                source: ReadSource::Frontier(_),
                access,
            } => {
                assert!(matches!(access, ReadAccess::All), "recompute: {id:?} rebinds a frontier");
            }
            RelKind::Pipe { input, op } => {
                assert!(
                    rel::per_row_item(judged, *input, op, node.heading()).is_none(),
                    "recompute: a reduction item of {id:?} stands for no group"
                );
                let stored: Vec<(super::node::Selected, ())> = match op {
                    super::node::PipeOp::ProjectOut(selection) => selection.items().to_vec(),
                    super::node::PipeOp::Cover(selection) => selection.items().iter().map(|(t, _)| (*t, ())).collect(),
                    _ => Vec::new(),
                };
                let references: Vec<(ExprId, ())> = stored.iter().map(|(s, _)| (s.reference(), ())).collect();
                assert!(
                    stored.is_empty()
                        || rel::select(judged, *input, references).is_some_and(|again| again.items() == stored.as_slice()),
                    "recompute: selected positions of {id:?}"
                );
            }
            RelKind::SetOp { left, right, op, alignment, correlation } => {
                let again = rel::align(graph.rel(*left).heading(), graph.rel(*right).heading(), *op);
                assert!(again.as_ref().ok() == Some(alignment), "recompute: alignment of {id:?}");
                if let Some(c) = correlation {
                    let pairs = decide::setop::correlation(
                        match op {
                            super::node::SetOpKind::Positional => decide::setop::Mode::Ordinals,
                            _ => decide::setop::Mode::Names,
                        },
                        graph.rel(*left).heading(),
                        graph.rel(*right).heading(),
                        c.written(),
                    );
                    assert!(pairs.as_ref().ok().map(Vec::as_slice) == Some(c.pairs()), "recompute: correlation of {id:?}");
                    let judged_again = rel::pair_membership(judged, *left, *right, c.pairs(), "a set-operation correlation");
                    assert!(judged_again.as_ref().ok().map(Vec::as_slice) == Some(c.membership()), "recompute: correlation membership of {id:?}");
                    assert!(c.class() == decide::equality::set_identity(), "recompute: correlation class of {id:?}");
                }
            }
            RelKind::Minus { left, right, pairs, membership, class } => {
                let again = rel::minus_pairs(graph.rel(*left).heading(), graph.rel(*right).heading());
                assert!(again.as_ref().ok() == Some(pairs), "recompute: minus alignment of {id:?}");
                let judged_again = rel::pair_membership(judged, *left, *right, pairs, "a minus step");
                assert!(judged_again.as_ref().ok() == Some(membership), "recompute: minus membership of {id:?}");
                assert!(*class == crate::pipeline::middle::core::decide::equality::set_identity(), "recompute: minus class of {id:?}");
            }
            RelKind::Fix(fix) => {
                assert!(rel::demand_cap(judged, fix.steps()).as_ref() == fix.cap(), "recompute: demand cap of {id:?}");
                let facts = rel::step_facts(judged, fix.steps(), fix.frontier());
                assert!(decide::recursion::judge(&facts).is_ok(), "recompute: recursion verdict of {id:?}");
                assert!(
                    fix.steps().iter().all(|s| graph.rel(*s).fv().contains(&fix.frontier()))
                        && fix.anchors().iter().all(|a| !graph.rel(*a).fv().contains(&fix.frontier())),
                    "recompute: recursive clauses of {id:?}"
                );
            }
            _ => {}
        }
    }
    for i in 0..graph.instance_count() {
        let instance = graph.instance(super::ids::InstanceId::at(i));
        assert!(
            decide::capture::relation_actuals(judged, instance.formals()).is_ok(),
            "recompute: a relation actual of instance {i} reads the calling row"
        );
    }
    for i in 0..graph.rel_count() {
        if let RelKind::Receipt { act, .. } = graph.rel(RelId::at(i)).kind() {
            assert!(super::node::effect::recheck(judged, act), "recompute: the act of r{i}");
        }
    }
}

/// The heading of a node of any kind but a run, from its decider over its
/// stored children; `None` where the decider refuses. The equality class
/// of every implicit comparison the node stores is rechecked here too.
fn heading_of(graph: &Graph, kind: &RelKind, within_row: EqClass) -> Option<Heading> {
    let judged = &super::graph::Recheck::of(graph);
    let read = |source: &Heading, access: &ReadAccess| -> Option<Heading> {
        match access {
            ReadAccess::All => form::form(Formed::Read { source, access: ReadForm::All }).ok(),
            ReadAccess::Unasked => form::form(Formed::Read { source, access: ReadForm::Unasked }).ok(),
            ReadAccess::Slots(slots) => {
                let classes_hold = slots.iter().all(|s| match s {
                    Slot::Reuse { class, .. } => *class == within_row,
                    Slot::Constraint { value, class } => *class == rel::constraint_class(judged, *value, graph.switches()),
                    Slot::Bind(_) | Slot::Anon => true,
                });
                let names = rel::slot_names(slots);
                classes_hold
                    .then(|| form::form(Formed::Read { source, access: ReadForm::Slots(&names) }).ok())
                    .flatten()
            }
        }
    };
    match kind {
        RelKind::Read { source, access } => match source {
            ReadSource::Catalog { columns, locator, .. } => {
                let names: Vec<_> = columns.iter().map(|c| c.name.clone()).collect();
                let structures: Vec<_> = columns
                    .iter()
                    .map(|c| crate::pipeline::middle::core::decide::document::declared(c.class, c.stored))
                    .collect();
                let catalog = form::form(Formed::Catalog {
                    columns: &names,
                    structures: &structures,
                })
                .ok()?;
                let heading = read(&catalog, access)?;
                if locator.is_empty() {
                    Some(heading)
                } else {
                    form::form(Formed::Marked { read: &heading, locator }).ok()
                }
            }
            ReadSource::Local(r) => read(graph.rel(*r).heading(), access),
            ReadSource::Staged(r) => form::form(Formed::Staged { input: graph.rel(*r).heading() }).ok(),
            ReadSource::Frontier(b) => read(graph.binder(*b).heading(), access),
            ReadSource::Function { columns, .. } | ReadSource::Created { columns, .. } => {
                let names: Vec<_> = columns.iter().map(|c| c.name.clone()).collect();
                let structures: Vec<_> = columns
                    .iter()
                    .map(|c| crate::pipeline::middle::core::decide::document::declared(c.class, c.stored))
                    .collect();
                let function = form::form(Formed::Catalog {
                    columns: &names,
                    structures: &structures,
                })
                .ok()?;
                read(&function, access)
            }
        },
        RelKind::Lit { header, aliased, rows } => {
            let structures = rel::lit_structures(judged, header.len(), rows).ok()?;
            let classes_hold = header.iter().all(|s| match s {
                HeaderSlot::Reuse { class, .. } => *class == within_row,
                HeaderSlot::Constraint { value, class } => *class == rel::constraint_class(judged, *value, graph.switches()),
                HeaderSlot::Bind(_) | HeaderSlot::Anon | HeaderSlot::Disregard => true,
            });
            classes_hold
                .then(|| {
                    form::form(Formed::Lit {
                        header,
                        aliased: *aliased,
                        structures: &structures,
                    })
                    .ok()
                })
                .flatten()
        }
        RelKind::Receipt { header, cells, .. } => {
            let columns = effect::receipt_columns(judged, header, cells);
            form::form(Formed::Receipt { columns: &columns }).ok()
        }
        RelKind::Run(_) => None,
        RelKind::Pipe { input, op } => rel::pipe_heading(judged, *input, op).ok(),
        RelKind::Order { input, .. } => form::form(Formed::Same { input: graph.rel(*input).heading() }).ok(),
        RelKind::SetOp { left, right, alignment, .. } => form::form(Formed::SetOp {
            left: graph.rel(*left).heading(),
            right: graph.rel(*right).heading(),
            alignment,
        })
        .ok(),
        RelKind::Minus { left, .. } => rel::minus_heading(graph.rel(*left).heading()).ok(),
        RelKind::Meta { .. } => form::form(Formed::Meta).ok(),
        RelKind::Witnessed { input, .. } => form::form(Formed::Witnessed {
            input: graph.rel(*input).heading(),
        })
        .ok(),
        RelKind::Unnest { value, expansion } => rel::expansion_heading(judged, *value, expansion, "").ok(),
        RelKind::Family { clauses, facts } => {
            let headings: Vec<&Heading> = clauses.iter().map(|c| graph.rel(c.body).heading()).collect();
            let heading = form::form(Formed::Family { name: "", clauses: &headings, closed: false }).ok()?;
            Some(match facts {
                Some(family) => form::fact_named(&heading, family),
                None => heading,
            })
        }
        RelKind::Fix(fix) => {
            let headings: Vec<&Heading> = fix
                .anchors()
                .iter()
                .chain(fix.steps())
                .map(|r| graph.rel(*r).heading())
                .collect();
            form::form(Formed::Family { name: "", clauses: &headings, closed: false }).ok()
        }
        RelKind::Apply { instance } => form::form(Formed::Instance {
            body: graph.rel(graph.instance(*instance).body()).heading(),
        })
        .ok(),
    }
}

/// A run's member headings and admissions, heading, and every fact its
/// close decided (marks, join roles, dependences, gates, placements, merge
/// rules), each from its decider over the stored members. The written marks
/// are not stored; the stored marks are the judgment's input, so this
/// checks every fact but the lead's mark.
fn check_run(graph: &Graph, id: RelId, r: &super::node::Run) {
    let judged = &super::graph::Recheck::of(graph);
    let members: Vec<&super::node::Member> = r.members().collect();
    for m in &members {
        let binding = run::member_binding(judged, m.rel(), m.scope().is_some() && m.requalifies());
        let again = form::form(Formed::Member {
            rel: graph.rel(m.rel()).heading(),
            binding,
        });
        assert!(
            again.as_ref().ok() == Some(graph.binder(m.binder()).heading()),
            "recompute: member heading of {:?} in {id:?}",
            m.binder()
        );
        assert!(run::admit(judged, m.rel(), m.route()).is_ok(), "recompute: admission of {:?} in {id:?}", m.binder());
    }
    let named: Vec<_> = members.iter().filter(|m| m.names_scope()).filter_map(|m| m.scope()).collect();
    for (i, name) in named.iter().enumerate() {
        assert!(!named[i + 1..].contains(name), "recompute: two live scopes named {name} in {id:?}");
    }
    let pairs: Vec<(super::ids::BinderId, RelId)> = members.iter().map(|m| (m.binder(), m.rel())).collect();
    let drilled = run::drilled(judged, &pairs);
    let away: Vec<Vec<usize>> = members
        .iter()
        .map(|m| {
            let merged: Vec<usize> = r
                .merges()
                .iter()
                .map(|(merge, _, _)| graph.merge(*merge).right())
                .filter(|(b, _)| *b == m.binder())
                .map(|(_, i)| i as usize)
                .collect();
            match m.publishes() {
                true => run::away_of(m.binder(), &merged, &drilled),
                false => (0..graph.binder(m.binder()).heading().len()).collect(),
            }
        })
        .collect();
    for (merge, _, _) in r.merges() {
        let site = graph.merge(*merge);
        let (rb, ri) = site.right();
        let again = rel::cell_position(judged, site.left()).and_then(|left| {
            graph
                .binder(rb)
                .heading()
                .positions()
                .get(ri as usize)
                .map(|right| super::heading::Interior::meet(&left.interior, &right.interior))
        });
        assert!(again.as_ref() == Some(site.structure()), "recompute: merged structure of {merge:?} in {id:?}");
    }
    let met = run::met(judged, &pairs, r.outputs());
    let run_members: Vec<RunMember<'_>> = members
        .iter()
        .zip(&away)
        .zip(&met)
        .map(|((m, a), met)| RunMember {
            heading: graph.binder(m.binder()).heading(),
            merged_away: a,
            met,
        })
        .collect();
    let again = form::form(Formed::Run {
        members: &run_members,
    });
    assert!(again.as_ref().ok() == Some(graph.rel(id).heading()), "recompute: run heading of {id:?}");
    let mut seen: Vec<super::ids::BinderId> = Vec::new();
    let mut guard_reads: Vec<(TruthId, run::GuardReads)> = Vec::new();
    for q in r.quals() {
        match q {
            Qual::Member(m) => seen.push(m.binder()),
            Qual::Guard(g) => guard_reads.push((
                g.truth(),
                run::guard_reads(judged, seen.iter().copied(), graph.truth(g.truth()).occurrences()),
            )),
            Qual::Bound(_) => {}
        }
    }
    let guards: Vec<(TruthId, &run::GuardReads)> = guard_reads.iter().map(|(t, read)| (*t, read)).collect();
    let written: Vec<bool> = members.iter().map(|m| m.mark().written()).collect();
    let order = run::written_order(r.quals().iter().map(|q| match q {
        Qual::Member(_) => run::Order::Member(0),
        Qual::Guard(_) => run::Order::Guard(0),
        Qual::Bound(_) => run::Order::Bound,
    }));
    let facts = run::judge(judged, &pairs, &written, &guards, &order, r.outputs())
        .unwrap_or_else(|_| panic!("recompute: {id:?} is an all-marked run with an unrelated full join"));
    for (m, f) in members.iter().zip(&facts.members) {
        assert!(f.mark == m.mark(), "recompute: mark of {:?} in {id:?}", m.binder());
        assert!(&f.role == m.role(), "recompute: join role of {:?} in {id:?}", m.binder());
        assert!(f.dependence == m.dependence(), "recompute: dependence of {:?} in {id:?}", m.binder());
        assert!(&f.opens == m.opens(), "recompute: gate of {:?} in {id:?}", m.binder());
    }
    let stored: Vec<super::node::GuardFacts> = r.guards().map(|g| g.facts()).collect();
    assert!(facts.guards == stored, "recompute: placements and boundaries of {id:?}");
    assert!(facts.merges.as_slice() == r.merges(), "recompute: merge rules of {id:?}");
}
