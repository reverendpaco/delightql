// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The comma run. An [`OpenRun`] is the run being written: each member
//! pushed mints its binder and states which of its positions merge with
//! the run so far; [`OpenRun::close`] decides what needs every member —
//! join roles, merged keys' value rules, placements — and constructs the
//! node.

use super::{
    Basis, BinderSet, BinderSite, Bound, Cell, ExprKind, Grade, Guard, GuardFacts, JoinRole, Mark, Member, MemberFacts,
    MergeRule, MergeSite, Occurrence, PipeOp, Qual, RelKind, Route, Run, TruthKind, WrittenParts,
};
use crate::pipeline::middle::core::decide::admission::{NonEquality, PopulationSteps};
use crate::pipeline::middle::core::decide::{self, run::Marks};
use crate::pipeline::middle::core::graph::{Arena, Builder, Judging};
use crate::pipeline::middle::core::heading::correspondence::answers_to;
use crate::pipeline::middle::core::heading::form::{self, Formed, MemberBinding, RunMember};
use crate::pipeline::middle::core::heading::{Binding, Heading, Interior, Name, Position, Visibility};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, MergeId, RelId, TruthId};
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// How a member merges with the run so far, as written.
pub(crate) enum MergeRequest {
    None,
    /// `.(a, b)`: merge the named positions.
    Using(Vec<Name>),
    /// `.*`: merge every shared name.
    Natural,
}

pub(crate) struct MemberSpec {
    pub(crate) marked: bool,
    pub(crate) completes_marked_lead: bool,
    pub(crate) route: Route,
    pub(crate) scope: Option<Name>,
    pub(crate) names_scope: bool,
    /// Whether the scope name qualifies the member's positions.
    pub(crate) requalifies: bool,
    /// What bore the member's relation, as far as naming reads it.
    pub(crate) born: Born,
    pub(crate) merge: MergeRequest,
}

/// What bore a member's relation, as the naming laws read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Born {
    /// A written relation, or a stage output an `as` named.
    Written,
    /// A pipe stage's output no `as` named: what the deictic `_` reaches.
    UnnamedStage,
    /// A walk's interior term (A CHAIN READS EACH TERM ONCE): read and
    /// joined, its positions read by its edges' conditions, publishing
    /// nothing.
    Interior,
}

/// A member as written, before its run closes: its marks are the input of
/// the run's judgment, and exist nowhere after it.
pub(crate) struct WrittenMember {
    binder: BinderId,
    rel: RelId,
    marked: bool,
    completes_marked_lead: bool,
    route: Route,
    scope: Option<Name>,
    names_scope: bool,
    requalifies: bool,
    born: Born,
}

impl WrittenMember {
    pub(crate) fn binder(&self) -> BinderId {
        self.binder
    }
    pub(crate) fn rel(&self) -> RelId {
        self.rel
    }
    pub(crate) fn scope(&self) -> Option<&Name> {
        self.scope.as_ref()
    }
    /// Whether the member is the output of a pipe stage no `as` named: what
    /// the deictic `_` reaches (lvars-law `_` IS DEIXIS).
    pub(crate) fn deictic(&self) -> bool {
        self.born == Born::UnnamedStage
    }
}

/// A qualifier as written.
enum OpenQual {
    Member(WrittenMember),
    Guard(TruthId),
    Bound(Bound),
}

/// A run being written. Elaboration state: its members are values, and
/// the run's facts exist only in the node `close` constructs.
pub(crate) struct OpenRun {
    quals: Vec<OpenQual>,
    guard_reads: Vec<GuardReads>,
    merged_away: Vec<Vec<usize>>,
    outputs: Vec<(Position, Cell)>,
    heading: Heading,
}

impl OpenRun {
    pub(crate) fn new() -> Self {
        OpenRun {
            quals: Vec::new(),
            guard_reads: Vec::new(),
            merged_away: Vec::new(),
            outputs: Vec::new(),
            heading: Heading::default(),
        }
    }

    /// The members pushed so far.
    pub(crate) fn members(&self) -> impl Iterator<Item = &WrittenMember> {
        self.quals.iter().filter_map(|q| match q {
            OpenQual::Member(m) => Some(m),
            OpenQual::Guard(_) | OpenQual::Bound(_) => None,
        })
    }

    /// The last member answering to the scope name `q`: what a qualified
    /// address, a qualified glob or a header's reuse through `q` reaches.
    pub(crate) fn answering(&self, q: &Name) -> Result<Option<&WrittenMember>, Refusal> {
        Ok(self.members().filter(|m| m.scope.as_ref() == Some(q)).last())
    }

    /// The run so far: its output heading, and what each position holds.
    pub(crate) fn heading(&self) -> &Heading {
        &self.heading
    }

    pub(crate) fn outputs(&self) -> &[(Position, Cell)] {
        &self.outputs
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.quals.is_empty()
    }

    /// The conditions pushed so far, in written order.
    pub(crate) fn guards(&self) -> impl Iterator<Item = TruthId> + '_ {
        self.quals.iter().filter_map(|q| match q {
            OpenQual::Guard(t) => Some(*t),
            OpenQual::Member(_) | OpenQual::Bound(_) => None,
        })
    }

    /// The members pushed so far, each as its binder and relation.
    pub(crate) fn bound(&self) -> Vec<(BinderId, RelId)> {
        self.members().map(|m| (m.binder, m.rel)).collect()
    }

    /// Push a member: judge its admission and the grades standing in it,
    /// mint its binder, pair its merges with the run so far (W5 #1, #4,
    /// #11, #21).
    pub(crate) fn push_member(
        &mut self,
        b: &mut Builder,
        rel: RelId,
        spec: MemberSpec,
        switches: &crate::pipeline::middle::core::switches::Switches,
    ) -> Result<BinderId, Refusal> {
        if spec.names_scope {
            if let Some(name) = &spec.scope {
                if self.members().any(|m| m.names_scope && m.scope.as_ref() == Some(name)) {
                    return Err(refuse::scope_duplicate(name));
                }
            }
        }
        let binding = member_binding(b, rel, spec.scope.is_some() && spec.requalifies);
        let contributed = form::form(Formed::Member {
            rel: b.rel(rel).heading(),
            binding,
        })?;
        // Two operands carrying one configured value's construction rows
        // (C4). A row locator reached twice is a statement's affair, judged
        // at its mutation terminal.
        for p in contributed.positions() {
            if let Visibility::Hidden(passenger) = p.visibility {
                let configured = matches!(Arena::passenger(b, passenger), super::Passenger::Configured { .. });
                if configured
                    && self.outputs.iter().any(|(q, _)| q.visibility == Visibility::Hidden(passenger))
                {
                    return match switches.completion_pairing {
                        crate::pipeline::middle::core::switches::CompletionPairing::Refuse => {
                            Err(refuse::completion_pairing())
                        }
                    };
                }
            }
        }
        admit(b, rel, spec.route)?;
        let binder = b.fresh_binder(BinderSite {
            heading: contributed.clone(),
            rel: Some(rel),
        });
        let pairs = self.pairs(&contributed, &spec.merge)?;
        // A merge compares its two positions' values as a correspondence
        // filter: a comparison of structures is judged as any other.
        for (left_index, right_index) in &pairs {
            use crate::pipeline::middle::core::decide::document::{comparable, Compared, Nulls};
            comparable(
                Compared {
                    structure: &self.outputs[*left_index].0.interior,
                    nulls: Nulls::Unknown,
                },
                Compared {
                    structure: &contributed.positions()[*right_index].interior,
                    nulls: Nulls::Unknown,
                },
                crate::pipeline::middle::facade::CmpOp::Equal,
                super::Consumer::Filter,
            )?;
        }
        let mut away = Vec::new();
        for (left_index, right_index) in pairs {
            let name = contributed.positions()[right_index]
                .answering_name()
                .cloned()
                .ok_or_else(|| refuse::column("?", "a merge needs a named position"))?;
            let structure = Interior::meet(
                &self.outputs[left_index].0.interior,
                &contributed.positions()[right_index].interior,
            );
            let m = b.push_merge(MergeSite {
                left: self.outputs[left_index].1,
                right: (binder, right_index as u16),
                name,
                structure,
            });
            away.push(right_index);
            self.outputs[left_index].1 = Cell::Merged(m);
        }
        if spec.born == Born::Interior {
            away = (0..contributed.len()).collect();
        }
        self.merged_away.push(away);
        self.quals.push(OpenQual::Member(WrittenMember {
            binder,
            rel,
            marked: spec.marked,
            completes_marked_lead: spec.completes_marked_lead,
            route: spec.route,
            scope: spec.scope,
            names_scope: spec.names_scope,
            requalifies: spec.requalifies,
            born: spec.born,
        }));
        self.reform(b)?;
        Ok(binder)
    }

    /// Push a condition.
    pub(crate) fn push_guard(&mut self, b: &Builder, truth: TruthId) {
        let reads = guard_reads(b, self.members().map(|m| m.binder), b.truth(truth).occurrences());
        self.guard_reads.push(reads);
        self.quals.push(OpenQual::Guard(truth));
    }

    /// Push a bound standing beside no ordering.
    pub(crate) fn push_bound(&mut self, bound: Bound) {
        self.quals.push(OpenQual::Bound(bound));
    }

    /// Close the run: decide each member's mark, join role, dependence and
    /// gate, each guard's placement and each merged key's value rule (W5
    /// #9), and construct the node with the heading the run formed as its
    /// members were pushed: every decided fact is built into the member or
    /// guard it describes.
    pub(crate) fn close(self, b: &mut Builder) -> Result<RelId, Refusal> {
        let cells: Vec<Cell> = self.outputs.iter().map(|(_, c)| *c).collect();
        let members: Vec<(BinderId, RelId)> = self.members().map(|m| (m.binder, m.rel)).collect();
        let marks: Vec<Marks> = self
            .members()
            .map(|m| Marks {
                marked: m.marked,
                completes_marked_lead: m.completes_marked_lead,
            })
            .collect();
        let written = decide::run::marked(&marks);
        let guards: Vec<(TruthId, &GuardReads)> = self
            .quals
            .iter()
            .filter_map(|q| match q {
                OpenQual::Guard(t) => Some(*t),
                OpenQual::Member(_) | OpenQual::Bound(_) => None,
            })
            .zip(&self.guard_reads)
            .collect();
        let order = written_order(self.quals.iter().map(|q| match q {
            OpenQual::Member(_) => Order::Member(0),
            OpenQual::Guard(_) => Order::Guard(0),
            OpenQual::Bound(_) => Order::Bound,
        }));
        let facts = judge(b, &members, &written, &guards, &order, &cells)?;
        let heading = self.heading.clone();
        let mut member_facts = facts.members.into_iter();
        let mut placements = facts.guards.into_iter();
        let mut quals = Vec::with_capacity(self.quals.len());
        for q in self.quals {
            quals.push(match q {
                OpenQual::Member(m) => {
                    let facts = member_facts
                        .next()
                        .ok_or_else(|| refuse::elaboration_contract("a run member its judgment did not decide"))?;
                    Qual::Member(Member::of(
                        WrittenParts {
                            binder: m.binder,
                            rel: m.rel,
                            route: m.route,
                            scope: m.scope,
                            names_scope: m.names_scope,
                            requalifies: m.requalifies,
                            publishes: m.born != Born::Interior,
                        },
                        facts,
                    ))
                }
                OpenQual::Guard(t) => Qual::Guard(Guard::of(
                    t,
                    placements
                        .next()
                        .ok_or_else(|| refuse::elaboration_contract("a run guard its judgment did not place"))?,
                )),
                OpenQual::Bound(bound) => Qual::Bound(bound),
            });
        }
        let kind = RelKind::Run(Run::of(quals, facts.merges, cells));
        let fv = super::rel::rel_fv(b, &kind);
        Ok(b.push_rel(super::RelNode { kind, heading, fv }))
    }

    /// Which output positions of the run so far merge with which of the
    /// new member's positions: the USING or natural names written, and
    /// every bare binder the member shares by name with a bare binder of
    /// the run.
    fn pairs(
        &self,
        contributed: &Heading,
        request: &MergeRequest,
    ) -> Result<Vec<(usize, usize)>, Refusal> {
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        let left_of = |name: &Name| -> Option<usize> {
            self.outputs
                .iter()
                .position(|(p, _)| p.answering_name() == Some(name))
        };
        match request {
            MergeRequest::None => {}
            MergeRequest::Using(names) => {
                for name in names {
                    let lost_left = !crate::pipeline::middle::core::heading::correspondence::lost(&self.heading, name)
                        .is_empty();
                    let left = match left_of(name) {
                        Some(left) => left,
                        None if lost_left => return Err(refuse::correspondence_not_exact(name.as_str())),
                        None => {
                            return Err(refuse::column(name.as_str(), "no column to the left publishes this name"))
                        }
                    };
                    let right = *answers_to(contributed, name).first().ok_or_else(|| {
                        refuse::column(name.as_str(), "the merged relation does not publish it")
                    })?;
                    pairs.push((left, right));
                }
            }
            MergeRequest::Natural => {
                for (right, p) in contributed.positions().iter().enumerate() {
                    if let Some(left) = p.answering_name().and_then(left_of) {
                        pairs.push((left, right));
                    }
                }
            }
        }
        // A bare binder unifies with the run's bare binder of the same
        // name even where a collision took that name from the published
        // heading: the pairing is of the two slots, not of the published
        // name.
        for (right, p) in contributed.positions().iter().enumerate() {
            if p.binding != Binding::Bare || pairs.iter().any(|(_, r)| *r == right) {
                continue;
            }
            if let Some(name) = binder_name(p) {
                let left = self.outputs.iter().position(|(q, _)| {
                    q.binding == Binding::Bare && binder_name(q) == Some(name)
                });
                if let Some(left) = left {
                    pairs.push((left, right));
                }
            }
        }
        Ok(pairs)
    }

    /// Re-form the run's output heading after a push. Earlier positions
    /// keep their cells (a merge replaced the left operand's); the new
    /// member's unmerged positions follow.
    fn reform(&mut self, b: &Builder) -> Result<(), Refusal> {
        let members: Vec<(BinderId, RelId)> = self.members().map(|m| (m.binder, m.rel)).collect();
        let headings: Vec<&Heading> = members.iter().map(|(binder, _)| b.binder(*binder).heading()).collect();
        let drilled = drilled(b, &members);
        let away: Vec<Vec<usize>> = members
            .iter()
            .zip(&self.merged_away)
            .map(|((binder, _), merged)| away_of(*binder, merged, &drilled))
            .collect();
        let mut cells: Vec<Cell> = self.outputs.iter().map(|(_, c)| *c).collect();
        let met = met(b, &members, &cells);
        let run_members: Vec<RunMember<'_>> = headings
            .iter()
            .zip(&away)
            .zip(&met)
            .map(|((h, away), met)| RunMember {
                heading: h,
                merged_away: away,
                met,
            })
            .collect();
        let heading = form::form(Formed::Run {
            members: &run_members,
        })?;
        if let (Some((last, _)), Some(away)) = (members.last(), self.merged_away.last()) {
            for i in 0..b.binder(*last).heading().len() {
                if !away.contains(&i) {
                    cells.push(Cell::Col(*last, i as u16));
                }
            }
        }
        cells.retain(|c| !matches!(c, Cell::Col(binder, i) if drilled.contains(&(*binder, *i as usize))));
        self.outputs = heading.positions().iter().cloned().zip(cells).collect();
        self.heading = heading;
        Ok(())
    }
}

/// The name a bare binder was written with, whether or not a collision
/// took it from the published heading.
fn binder_name(p: &Position) -> Option<&Name> {
    use crate::pipeline::middle::core::heading::NameState;
    match &p.name {
        NameState::Authored(n) | NameState::Catalog(n) | NameState::Lost(n) => Some(n),
        NameState::Minted(_) => None,
    }
}

/// The positions a run's drills consume: a member (its binder and relation)
/// that drills a structured position of an earlier member of the run
/// replaces that position by its rows, so the position leaves the run's
/// output.
pub(crate) fn drilled(arena: &impl Judging, members: &[(BinderId, RelId)]) -> Vec<(BinderId, usize)> {
    let binders: Vec<BinderId> = members.iter().map(|(b, _)| *b).collect();
    members
        .iter()
        .filter_map(|(_, rel)| match arena.rel(*rel).kind() {
            RelKind::Unnest { value, expansion } if expansion.consumes() => match arena.expr(*value).kind() {
                ExprKind::Col(b, i) if binders.contains(b) => Some((*b, *i as usize)),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// The member positions a merged key stands at (its leftmost operand's),
/// each with the structure its last merge decided, by member.
pub(crate) fn met(arena: &impl Judging, members: &[(BinderId, RelId)], outputs: &[Cell]) -> Vec<Vec<(usize, Interior)>> {
    fn leftmost(arena: &impl Judging, cell: Cell) -> (BinderId, u16) {
        match cell {
            Cell::Col(b, i) => (b, i),
            Cell::Merged(m) => leftmost(arena, arena.merge(m).left()),
        }
    }
    let mut out = vec![Vec::new(); members.len()];
    for cell in outputs {
        if let Cell::Merged(m) = cell {
            let (b, i) = leftmost(arena, *cell);
            if let Some(at) = members.iter().position(|(x, _)| *x == b) {
                out[at].push((i as usize, arena.merge(*m).structure().clone()));
            }
        }
    }
    out
}

/// A member's positions that leave the run's output: those a merge
/// absorbed and those a drill consumed.
pub(crate) fn away_of(binder: BinderId, merged: &[usize], drilled: &[(BinderId, usize)]) -> Vec<usize> {
    let mut away = merged.to_vec();
    away.extend(drilled.iter().filter(|(b, _)| *b == binder).map(|(_, i)| *i));
    away
}

/// What a condition's values read of the run it stands in: the indexes of
/// the members whose columns they read, and the merged keys they read,
/// whose members only the merge rules decide ([`value_members`], in
/// [`judge`]). An enclosing value (an interior's, a body's or a correlated
/// subquery's caller) is no local joining side and is not counted
/// (predicate-placement-law: enclosing values are not local joining sides).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuardReads {
    columns: Vec<usize>,
    merges: Vec<MergeId>,
}

impl GuardReads {
    /// The members the condition reads: its columns' members, and each
    /// merged key's members as [`members_through`] reads it.
    fn members(&self, arena: &impl Judging, members: &[BinderId], rules: &[(MergeId, MergeRule)]) -> Vec<usize> {
        let mut out = self.columns.clone();
        for m in &self.merges {
            let read = members_through(arena, members, rules, Cell::Merged(*m), Some(&out));
            out.extend(read);
        }
        out.sort_unstable();
        out.dedup();
        out
    }
}

pub(crate) fn guard_reads(
    arena: &impl Judging,
    members: impl Iterator<Item = BinderId>,
    occurrences: &std::collections::BTreeSet<Occurrence>,
) -> GuardReads {
    let members: Vec<BinderId> = members.collect();
    let mut columns: Vec<usize> = Vec::new();
    let mut merges: Vec<MergeId> = Vec::new();
    for occurrence in occurrences {
        match occurrence {
            Occurrence::Binder(binder) => {
                if let Some(i) = members.iter().position(|m| m == binder) {
                    if !columns.contains(&i) {
                        columns.push(i);
                    }
                }
            }
            Occurrence::Merge(merge) => {
                if members.contains(&arena.merge(*merge).right().0) && !merges.contains(merge) {
                    merges.push(*merge);
                }
            }
        }
    }
    GuardReads { columns, merges }
}

/// A run's facts decided at its close (W5 #9): per member, per guard, per
/// merged key.
pub(crate) struct RunFacts {
    pub(crate) members: Vec<MemberFacts>,
    pub(crate) guards: Vec<GuardFacts>,
    pub(crate) merges: Vec<(MergeId, MergeRule, super::Placement)>,
}

/// A run's qualifiers in authored order: each member and guard by its index
/// among its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Order {
    Member(usize),
    Guard(usize),
    Bound,
}

/// Number each member and guard of an authored sequence of qualifiers among
/// its kind.
pub(crate) fn written_order(kinds: impl Iterator<Item = Order>) -> Vec<Order> {
    let (mut members, mut guards) = (0, 0);
    kinds
        .map(|k| match k {
            Order::Member(_) => {
                members += 1;
                Order::Member(members - 1)
            }
            Order::Guard(_) => {
                guards += 1;
                Order::Guard(guards - 1)
            }
            Order::Bound => Order::Bound,
        })
        .collect()
}

/// Which guards are boundaries (predicate-placement-law: population
/// evaluation): a filter evaluating a window, an aggregate or a collection
/// never commutes with a later join; a row-wise filter commutes with a later
/// inner join and with a left join that keeps the rows so far, not with a
/// later join that keeps the later member's rows (a full join, or a right
/// join of the fold), and only the members before the next boundary (a bound
/// is always one) are later joins it could move past. Then THE CROSSING: a
/// condition placed on the join of a member written before a boundary,
/// itself written after it, refuses, since whether the boundary's population
/// includes it is unruled.
fn boundaries(
    arena: &impl Judging,
    order: &[Order],
    guards: &[TruthId],
    placements: &[super::Placement],
    roles: &[JoinRole],
) -> Result<Vec<bool>, Refusal> {
    let mut at_boundary = vec![false; order.len()];
    let mut out = vec![false; guards.len()];
    for at in (0..order.len()).rev() {
        at_boundary[at] = match order[at] {
            Order::Member(_) => false,
            Order::Bound => true,
            Order::Guard(g) => {
                let boundary = match placements[g] {
                    super::Placement::On { .. } => false,
                    super::Placement::Filter if super::expr::truth_population_sensitive(arena, guards[g]) => true,
                    super::Placement::Filter => order[at + 1..]
                        .iter()
                        .zip(&at_boundary[at + 1..])
                        .take_while(|(_, boundary)| !**boundary)
                        .any(|(item, _)| matches!(item, Order::Member(j) if roles[*j].keeps_member())),
                };
                out[g] = boundary;
                boundary
            }
        };
    }
    let member_at = |m: usize| order.iter().position(|i| *i == Order::Member(m));
    for (at, item) in order.iter().enumerate() {
        let Order::Guard(g) = item else {
            continue;
        };
        let super::Placement::On { member } = placements[*g] else {
            continue;
        };
        if let Some(from) = member_at(member) {
            if from < at && at_boundary[from + 1..at].iter().any(|b| *b) {
                return Err(refuse::condition_after_population_step());
            }
        }
    }
    Ok(out)
}

/// A run's facts, from its members (each binder and relation, in authored
/// order) with their written marks (`written`: the lead's `?` already moved
/// onto it), its guards with what each reads, and its output cells: a
/// condition reads the members its values read, a merged key as its rule
/// takes it (the same answer a merge's own reading gives); a merge reads its right
/// member and the members whose value its left operand holds (by the rules
/// of the merges made before it); a member whose relation reads earlier
/// members depends on them. Every relation is placed by one rule, so a join
/// spelled as a merge and as a condition stores one role. Each member's
/// mark is its written mark, spent where an unmarked dependent member reads
/// it; its role reads the marks that are not spent.
pub(crate) fn judge(
    arena: &impl Judging,
    members: &[(BinderId, RelId)],
    written: &[bool],
    guards: &[(TruthId, &GuardReads)],
    order: &[Order],
    outputs: &[Cell],
) -> Result<RunFacts, Refusal> {
    let association = decide::run::association(written);
    let binders: Vec<BinderId> = members.iter().map(|(b, _)| *b).collect();
    let index = |b: BinderId| binders.iter().position(|x| *x == b);
    // What each member's relation reads of the members before it: through
    // its rows (a population that exists per enclosing row), or only
    // through its read's slot constraints (a condition).
    let mut dependence_reads: Vec<(usize, super::Dependence, Vec<usize>)> = Vec::new();
    for (j, (_, rel)) in members.iter().enumerate() {
        let reads = |set: &BinderSet| -> Vec<usize> { set.iter().filter_map(|b| index(*b)).filter(|i| *i != j).collect() };
        let through_rows = reads(&population_fv(arena, *rel));
        let mut read = reads(arena.rel(*rel).fv());
        if read.is_empty() {
            continue;
        }
        let kind = if through_rows.is_empty() {
            super::Dependence::Constraint
        } else {
            super::Dependence::Population
        };
        read.push(j);
        read.sort_unstable();
        read.dedup();
        dependence_reads.push((j, kind, read));
    }
    let mut dependent = vec![false; members.len()];
    let mut pinned = vec![false; members.len()];
    for (j, kind, read) in &dependence_reads {
        if *kind == super::Dependence::Population {
            dependent[*j] = true;
            if !written[*j] {
                for i in read.iter().filter(|i| **i != *j) {
                    pinned[*i] = true;
                }
            }
        }
    }
    // Every judgment below but the effect fence reads the effective marks.
    let marked = decide::run::effective(written, association, &pinned);
    let kinds = decide::run::kinds(&marked, association, &dependent);
    let mut found = merges_of(arena, outputs);
    found.sort();
    let mut rules: Vec<(MergeId, MergeRule)> = Vec::new();
    let mut merge_reads: Vec<Vec<usize>> = Vec::new();
    for m in &found {
        let site = arena.merge(*m);
        let right = index(site.right().0);
        let right_kind = right.map(|i| kinds[i].clone()).unwrap_or(JoinRole::Required);
        let left_present = present(arena, &binders, &kinds, site.left());
        rules.push((*m, decide::run::merge_rule(&right_kind, left_present)));
        let mut read = value_members(arena, &binders, &rules, site.left());
        read.extend(right);
        read.sort_unstable();
        read.dedup();
        merge_reads.push(read);
    }
    // Which members each condition reads, answered once with the merge
    // rules in hand: a merged key is read as its rule takes it.
    let guard_members: Vec<Vec<usize>> = guards.iter().map(|(_, read)| read.members(arena, &binders, &rules)).collect();
    let placements: Vec<super::Placement> =
        guard_members.iter().map(|read| decide::run::placement(&marked, read, association)).collect();
    let truths: Vec<TruthId> = guards.iter().map(|(t, _)| *t).collect();
    judge_join_conditions(arena, &truths, members, &placements)?;
    judge_merged_key_conditions(arena, &truths, &binders, &rules, &placements)?;
    let merge_placements: Vec<super::Placement> =
        merge_reads.iter().map(|read| decide::run::placement(&marked, read, association)).collect();
    // A population's reading of earlier members is its own join's
    // condition: it is never a preserved operand.
    let dependences: Vec<(usize, super::Dependence, super::Placement)> = dependence_reads
        .iter()
        .map(|(j, kind, read)| {
            let placement = match kind {
                super::Dependence::Population => super::Placement::On { member: *j },
                super::Dependence::Constraint => decide::run::placement(&marked, read, association),
            };
            (*j, *kind, placement)
        })
        .collect();
    let mut relations: Vec<(super::Placement, Vec<usize>)> = Vec::new();
    relations.extend(placements.iter().copied().zip(guard_members.iter().cloned()));
    relations.extend(merge_placements.iter().copied().zip(merge_reads.iter().cloned()));
    relations.extend(dependences.iter().map(|(_, _, p)| *p).zip(dependence_reads.iter().map(|(_, _, r)| r.clone())));
    let roles = decide::run::roles(&marked, &relations, association, &dependent);
    if let Some(member) = decide::run::unrelated_full_join(&roles, &relations) {
        return Err(refuse::full_outer_unrelated(member));
    }
    let boundary = boundaries(arena, order, &truths, &placements, &roles)?;
    let rels: Vec<RelId> = members.iter().map(|(_, r)| *r).collect();
    let opens = decide::effect::judge_run(arena, &rels, written)?;
    let member_facts = roles
        .into_iter()
        .zip(opens)
        .enumerate()
        .map(|(j, (role, opens))| MemberFacts {
            mark: match (written[j], marked[j]) {
                (false, _) => Mark::Unmarked,
                (true, true) => Mark::Optional,
                (true, false) => Mark::Spent,
            },
            role,
            dependence: dependences.iter().find(|(k, _, _)| *k == j).map(|(_, d, p)| (*d, *p)),
            opens,
        })
        .collect();
    Ok(RunFacts {
        members: member_facts,
        guards: placements
            .into_iter()
            .zip(boundary)
            .map(|(placement, boundary)| GuardFacts { placement, boundary })
            .collect(),
        merges: rules
            .into_iter()
            .zip(merge_placements)
            .map(|((m, rule), placement)| (m, rule, placement))
            .collect(),
    })
}

/// A condition reading a merged key an outer join publishes is settled only
/// as the match condition of a member joined after the merge's right member:
/// every reading of what the merged key depends on places it there. Placed
/// as a filter or on the merge's own join, the readings part (one operand's
/// value, or both operands), and it refuses.
fn judge_merged_key_conditions(
    arena: &impl Judging,
    guards: &[TruthId],
    binders: &[BinderId],
    rules: &[(MergeId, MergeRule)],
    placements: &[super::Placement],
) -> Result<(), Refusal> {
    for (t, placement) in guards.iter().copied().zip(placements) {
        for occurrence in arena.truth(t).occurrences() {
            let Occurrence::Merge(m) = occurrence else {
                continue;
            };
            let Some(rule) = rules.iter().find(|(x, _)| x == m).map(|(_, rule)| *rule) else {
                continue;
            };
            if rule == MergeRule::Agreed {
                continue;
            }
            let right = binders.iter().position(|b| *b == arena.merge(*m).right().0);
            let later = matches!((placement, right), (super::Placement::On { member }, Some(r)) if *member > r);
            // A preserved operand's key is that operand's own value, so a
            // condition reading it reads one member: a filter over the
            // joined rows (continuation-and-access-law: the preserved left
            // operand supplies a LEFT OUTER key, the right a RIGHT OUTER
            // key). A coalesced key reads both operands.
            let filter = *placement == super::Placement::Filter && matches!(rule, MergeRule::Left | MergeRule::Right);
            if !later && !filter {
                return Err(refuse::merged_key_condition());
            }
        }
    }
    Ok(())
}

/// A WINDOW IS NEVER EVALUATED BY A JOIN'S CONDITION (predicate-placement-law:
/// population evaluation and condition placement): a join evaluates its
/// condition per pair of rows, where a value evaluated over a population has
/// none. The conditions a join evaluates are a condition placed on a member's
/// join, a member read's slot constraint and a clause family's dispatch
/// guard; each is judged once, here, on its expanded truth, so a sigma or
/// scalar wrapper hides nothing, and a nested relation or scalar subquery
/// keeps its own rules. A window refuses by the law. An aggregate or a
/// collection reaches a join's condition only as a value evaluated over
/// another population, which the fragment does not cover.
fn judge_join_conditions(
    arena: &impl Judging,
    guards: &[TruthId],
    members: &[(BinderId, RelId)],
    placements: &[super::Placement],
) -> Result<(), Refusal> {
    for (t, placement) in guards.iter().copied().zip(placements) {
        if matches!(placement, super::Placement::On { .. }) {
            judge_join_condition(arena, JoinCondition::Truth(t), "a condition relating two members of a join")?;
        }
    }
    for (_, rel) in members {
        for (condition, what) in member_join_conditions(arena, *rel) {
            judge_join_condition(arena, condition, what)?;
        }
    }
    Ok(())
}

/// One condition a join evaluates.
#[derive(Clone, Copy)]
enum JoinCondition {
    Truth(TruthId),
    Value(ExprId),
}

fn judge_join_condition(arena: &impl Judging, condition: JoinCondition, what: &str) -> Result<(), Refusal> {
    let (window, sensitive) = match condition {
        JoinCondition::Truth(t) => (
            super::expr::truth_evaluates_window(arena, t),
            super::expr::truth_population_sensitive(arena, t),
        ),
        JoinCondition::Value(e) => (super::expr::evaluates_window(arena, e), super::expr::population_sensitive(arena, e)),
    };
    if window {
        return Err(refuse::window_on_join(what));
    }
    if sensitive {
        return Err(refuse::outside(&format!(
            "{what} reading an aggregate or a collection evaluated over another population"
        )));
    }
    Ok(())
}

/// The conditions of a member's own join: its read's slot constraints, and
/// the dispatch guards of the clause family it reads.
fn member_join_conditions(arena: &impl Judging, rel: RelId) -> Vec<(JoinCondition, &'static str)> {
    let family_guards = |body: RelId| -> Vec<(JoinCondition, &'static str)> {
        match arena.rel(body).kind() {
            RelKind::Family { clauses, .. } => clauses
                .iter()
                .filter_map(|c| c.guard)
                .map(|g| (JoinCondition::Truth(g), "a clause's dispatch guard"))
                .collect(),
            _ => Vec::new(),
        }
    };
    match arena.rel(rel).kind() {
        RelKind::Read { source, access } => {
            let mut out: Vec<(JoinCondition, &'static str)> = match access {
                super::ReadAccess::Slots(slots) => slots
                    .iter()
                    .filter_map(|s| match s {
                        super::Slot::Constraint { value, .. } => {
                            Some((JoinCondition::Value(*value), "a member's slot constraint"))
                        }
                        super::Slot::Bind(_) | super::Slot::Anon | super::Slot::Reuse { .. } => None,
                    })
                    .collect(),
                super::ReadAccess::All | super::ReadAccess::Unasked => Vec::new(),
            };
            if let super::ReadSource::Local(body) = source {
                out.extend(family_guards(*body));
            }
            out
        }
        RelKind::Apply { instance } => family_guards(arena.instance(*instance).body()),
        RelKind::Family { .. } => family_guards(rel),
        _ => Vec::new(),
    }
}

/// The enclosing rows a relation's rows exist per: its free binders, less
/// those only a read's slot constraints read (a slot constraint is a
/// condition on rows the read has regardless).
fn population_fv(arena: &impl Judging, rel: RelId) -> BinderSet {
    match arena.rel(rel).kind() {
        RelKind::Read { source, .. } => match source {
            super::ReadSource::Local(inner) => arena.rel(*inner).fv().clone(),
            super::ReadSource::Frontier(frontier) => std::iter::once(*frontier).collect(),
            super::ReadSource::Catalog { .. } | super::ReadSource::Staged(_) | super::ReadSource::Created { .. } => {
                BinderSet::new()
            }
            super::ReadSource::Function { args, .. } => {
                args.iter().flat_map(|a| arena.expr(*a).fv().iter().copied()).collect()
            }
        },
        // An anonymous table's header constraints read earlier members as a
        // condition; only its cells make rows per enclosing row.
        RelKind::Lit { rows, .. } => rows.iter().flatten().flat_map(|e| arena.expr(*e).fv().iter().copied()).collect(),
        _ => arena.rel(rel).fv().clone(),
    }
}

/// Whether a cell's value is present in every row of the tree: a required
/// member's column, or a merged key one of whose operands is.
fn present(arena: &impl Judging, members: &[BinderId], kinds: &[JoinRole], cell: Cell) -> bool {
    match cell {
        Cell::Col(b, _) => members
            .iter()
            .position(|m| *m == b)
            .is_some_and(|i| kinds[i] == JoinRole::Required),
        Cell::Merged(m) => {
            let right = arena.merge(m).right().0;
            present(arena, members, kinds, arena.merge(m).left())
                || members
                    .iter()
                    .position(|x| *x == right)
                    .is_some_and(|i| kinds[i] == JoinRole::Required)
        }
    }
}

/// The members whose value a cell holds: a member's column, or a merged
/// key's operands as its rule takes them (the left's, the right's, or
/// either).
fn value_members(arena: &impl Judging, members: &[BinderId], rules: &[(MergeId, MergeRule)], cell: Cell) -> Vec<usize> {
    members_through(arena, members, rules, cell, None)
}

/// The members a reading of `cell` reaches. With no `reading`, every member
/// whose value the cell holds: a merge matches its right member against
/// each of them. With the members a condition already reads, the members
/// the condition reads through the cell: a preserved operand's key is that
/// operand's value, a right-supplied key the right member's, a coalesced
/// key both operands'; an agreed key (an inner match) is one value every
/// holder has on every row (continuation-and-access-law THE MERGE IS OF THE
/// HEADING: an inner equality match agrees on the key), so reading it
/// relates no two members: it is read through a holder the condition
/// already reads, or else through its left operand, whose lvar the merge
/// keeps.
fn members_through(
    arena: &impl Judging,
    members: &[BinderId],
    rules: &[(MergeId, MergeRule)],
    cell: Cell,
    reading: Option<&[usize]>,
) -> Vec<usize> {
    let m = match cell {
        Cell::Col(b, _) => return members.iter().position(|x| *x == b).into_iter().collect(),
        Cell::Merged(m) => m,
    };
    let site = arena.merge(m);
    let right: Vec<usize> = members.iter().position(|x| *x == site.right().0).into_iter().collect();
    let rule = rules.iter().find(|(x, _)| *x == m).map(|(_, r)| *r).unwrap_or(MergeRule::Agreed);
    let both = |reading: Option<&[usize]>| {
        let mut out = members_through(arena, members, rules, site.left(), reading);
        out.extend(right.iter().copied());
        out
    };
    match (rule, reading) {
        (MergeRule::Left, _) => members_through(arena, members, rules, site.left(), reading),
        (MergeRule::Right, _) => right,
        (MergeRule::Coalesced, _) | (MergeRule::Agreed, None) => both(reading),
        (MergeRule::Agreed, Some(already)) => {
            if both(None).iter().any(|i| already.contains(i)) {
                Vec::new()
            } else {
                members_through(arena, members, rules, site.left(), reading)
            }
        }
    }
}

/// Every merge a run's output cells hold: a merge replaces its left
/// operand's cell, and a later merge onto that cell chains on it.
fn merges_of(arena: &impl Judging, outputs: &[Cell]) -> Vec<MergeId> {
    let mut found: Vec<MergeId> = Vec::new();
    fn chain(arena: &impl Judging, cell: Cell, out: &mut Vec<MergeId>) {
        if let Cell::Merged(m) = cell {
            chain(arena, arena.merge(m).left(), out);
            if !out.contains(&m) {
                out.push(m);
            }
        }
    }
    for cell in outputs {
        chain(arena, *cell, &mut found);
    }
    found
}

/// A member's binding: a read or an anonymous table keeps its own; a
/// relation named by a scope is reached through it; an unnamed stage's
/// output is a set of bare binders.
pub(crate) fn member_binding(b: &impl Judging, rel: RelId, scoped: bool) -> MemberBinding {
    match b.rel(rel).kind() {
        RelKind::Read { .. } | RelKind::Lit { .. } => MemberBinding::Own,
        RelKind::Run(_)
        | RelKind::Pipe { .. }
        | RelKind::Order { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Fix(_)
        | RelKind::Apply { .. }
        | RelKind::Receipt { .. } => {
            if scoped {
                MemberBinding::Qualified
            } else {
                MemberBinding::Bare
            }
        }
    }
}

/// A member's admission (W5 #4): the fences an inline dependent member
/// must pass, then the grades standing in it. A call whose grade
/// contradicts its position is refused here when it stands in an
/// interior: over a dependent population the fence names it; otherwise the
/// grade law does.
pub(crate) fn admit(arena: &impl Judging, rel: RelId, route: Route) -> Result<(), Refusal> {
    let dependent_on = arena.rel(rel).fv().clone();
    let steps = population_steps(arena, rel, &dependent_on);
    decide::admission::judge(route, !dependent_on.is_empty(), &steps)?;
    if let Some((callee, contradiction)) = contradicted_call(arena, &[super::walk::Child::Rel(rel)]) {
        return Err(decide::admission::grade_refusal(&callee, contradiction));
    }
    Ok(())
}

/// The first call under `roots` whose grade contradicts its position.
pub(crate) fn contradicted_call(
    arena: &impl Judging,
    roots: &[super::walk::Child],
) -> Option<(String, crate::pipeline::middle::core::decide::grade::Contradiction)> {
    let reach = super::walk::reachable(arena, roots);
    reach.exprs.iter().find_map(|e| match arena.expr(*e).kind() {
        ExprKind::Call {
            callee,
            grade: Grade::Contradicted(c),
            ..
        } => Some((callee.name.clone(), *c)),
        _ => None,
    })
}

/// What a member's own chain does to the population it selects, read in
/// authored order through every step of the chain: the steps the admission
/// fences read. The population is what the correlating conditions select;
/// a step written before every correlating condition acts on the source,
/// not on the population, and no fence reads it.
pub(crate) fn population_steps(arena: &impl Judging, rel: RelId, dep: &BinderSet) -> PopulationSteps {
    let mut walk = Population {
        dep,
        steps: PopulationSteps::default(),
        correlating: Vec::new(),
    };
    if !dep.is_empty() {
        walk.rel(arena, rel);
    }
    walk.steps
}

struct Population<'d> {
    dep: &'d BinderSet,
    steps: PopulationSteps,
    /// The correlating conditions met so far, in authored order.
    correlating: Vec<TruthId>,
}

impl Population<'_> {
    fn reads_dependency(&self, arena: &impl Judging, r: RelId) -> bool {
        arena.rel(r).fv().iter().any(|x| self.dep.contains(x))
    }

    /// The chain's steps in authored order: a stage's input before the
    /// stage, a run's first member before its other qualifiers.
    fn rel(&mut self, arena: &impl Judging, rel: RelId) {
        match arena.rel(rel).kind() {
            RelKind::Pipe { input, op } => {
                self.rel(arena, *input);
                let over = self.reads_dependency(arena, *input);
                if over {
                    if let PipeOp::Group { keys, .. } = op {
                        if keys.is_empty() {
                            self.steps.keyless_reduction = true;
                        }
                    }
                    for e in pipe_exprs(op) {
                        self.bare(arena, e, false);
                    }
                }
            }
            RelKind::Order { input, keys, bound } => {
                self.rel(arena, *input);
                if self.reads_dependency(arena, *input) {
                    for k in keys {
                        self.bare(arena, k.expr, false);
                    }
                }
                if bound.is_some() {
                    self.bound(arena);
                }
            }
            RelKind::Run(run) => {
                let mut quals = run.quals().iter();
                if let Some(Qual::Member(first)) = run.quals().first() {
                    self.rel(arena, first.rel());
                    quals.next();
                }
                for qual in quals {
                    match qual {
                        Qual::Member(_) => {}
                        // A correlating condition selects the population; it
                        // is not a step over it, so only the conditions after
                        // it are read for what stands bare over the population.
                        Qual::Guard(g) => {
                            let t = g.truth();
                            if arena.truth(t).fv().iter().any(|x| self.dep.contains(x)) {
                                self.correlating.push(t);
                            } else if !self.correlating.is_empty() {
                                self.bare_truth(arena, t, false);
                            }
                        }
                        Qual::Bound(_) => self.bound(arena),
                    }
                }
            }
            RelKind::Apply { instance } => {
                let through = arena.instance(*instance).formals().iter().any(|actual| {
                    matches!(actual, crate::pipeline::middle::core::instance::Actual::Relation { rel, .. }
                        if self.reads_dependency(arena, *rel))
                });
                if through {
                    self.steps.through_instance = true;
                }
            }
            RelKind::Read { .. }
            | RelKind::Lit { .. }
            | RelKind::Receipt { .. }
            | RelKind::SetOp { .. }
            | RelKind::Minus { .. }
            | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
            | RelKind::Unnest { .. }
            | RelKind::Family { .. }
            | RelKind::Fix(_) => {}
        }
    }

    /// A bound ranks the population the correlating conditions before it
    /// select; when one of them is not a conjunction of equalities, no
    /// equality names that population.
    fn bound(&mut self, arena: &impl Judging) {
        if self.steps.noneq_bound.is_none() {
            self.steps.noneq_bound = self.correlating.iter().find_map(|t| first_nonequality(arena, *t));
        }
    }

    /// A call standing bare over the population: one whose grade reduces
    /// in a row-wise position, or one the compiler holds no record of that
    /// stands row-wise by the author's assertion where no reduction encloses
    /// it (`reduced`): read there as possibly a reduction.
    fn bare(&mut self, arena: &impl Judging, e: ExprId, reduced: bool) {
        if self.steps.bare_reduction.is_some() {
            return;
        }
        match arena.expr(e).kind() {
            ExprKind::Call { callee, args, grade } => {
                let bare = match grade {
                    Grade::Contradicted(c) => matches!(
                        c,
                        decide::grade::Contradiction::ReducingInRowWise
                            | decide::grade::Contradiction::ReducingInReduced
                    ),
                    Grade::Scalar(Basis::Asserted) => !reduced,
                    Grade::Scalar(Basis::Known) | Grade::Aggregate => false,
                };
                if bare {
                    self.steps.bare_reduction = Some(callee.name.clone());
                    return;
                }
                let within = reduced || *grade == Grade::Aggregate;
                for a in args {
                    if let super::Arg::Value { expr, .. } = a {
                        self.bare(arena, *expr, within);
                    }
                }
            }
            ExprKind::Infix(_, l, r) => {
                self.bare(arena, *l, reduced);
                self.bare(arena, *r, reduced);
            }
            ExprKind::Case { anchor, arms, default } => {
                if let Some(a) = anchor {
                    self.bare(arena, *a, reduced);
                }
                for (test, result) in arms {
                    if let super::CaseTest::Truth(t) = test {
                        self.bare_truth(arena, *t, reduced);
                    }
                    self.bare(arena, *result, reduced);
                }
                if let Some(d) = default {
                    self.bare(arena, *d, reduced);
                }
            }
            ExprKind::Crossed(t) => self.bare_truth(arena, *t, reduced),
            ExprKind::Construct { members, .. } => {
                for m in members {
                    self.bare(arena, m.expr, reduced);
                }
            }
            ExprKind::Path { source, .. } | ExprKind::Across(source) | ExprKind::Argument { value: source, .. } => {
                self.bare(arena, *source, reduced)
            }
            // A pick's value is read within its group, as an aggregate's
            // argument is.
            ExprKind::Pick { value, .. } => self.bare(arena, *value, true),
            ExprKind::Col(..)
            | ExprKind::Merged(_)
            | ExprKind::Const(_)
            | ExprKind::Window { .. }
            | ExprKind::Scalar { .. }
            | ExprKind::Passenger(_)
            | ExprKind::Collect { .. }
            | ExprKind::Metadata { .. } => {}
        }
    }

    fn bare_truth(&mut self, arena: &impl Judging, t: TruthId, reduced: bool) {
        match arena.truth(t).kind() {
            TruthKind::Cmp { left, right, .. } => {
                self.bare(arena, *left, reduced);
                self.bare(arena, *right, reduced);
            }
            TruthKind::And(parts) | TruthKind::Or(parts) => {
                for p in parts {
                    self.bare_truth(arena, *p, reduced);
                }
            }
            TruthKind::Not(p) => self.bare_truth(arena, *p, reduced),
            TruthKind::Sigma { args, .. } => {
                for a in args {
                    self.bare(arena, *a, reduced);
                }
            }
            TruthKind::Exists { .. } => {}
        }
    }
}

/// The expressions a stage holds.
pub(crate) fn pipe_exprs(op: &PipeOp) -> Vec<ExprId> {
    match op {
        PipeOp::Project(items) | PipeOp::Embed(items) | PipeOp::Distinct(items) => {
            items.iter().map(|i| i.expr).collect()
        }
        PipeOp::Group { keys, reductions } => keys.iter().chain(reductions).map(|i| i.expr).collect(),
        PipeOp::ProjectOut(selection) => selection.items().iter().map(|(s, _)| s.reference()).collect(),
        PipeOp::Cover(selection) => selection.items().iter().flat_map(|(t, v)| [t.reference(), *v]).collect(),
        PipeOp::Carry(_) => Vec::new(),
    }
}

/// The first part of a correlating condition that is not a conjunction of
/// equalities, spelled; `None` when every part is one.
fn first_nonequality(arena: &impl Judging, t: TruthId) -> Option<NonEquality> {
    use crate::pipeline::middle::facade::CmpOp;
    match arena.truth(t).kind() {
        TruthKind::And(parts) => parts.iter().find_map(|p| first_nonequality(arena, *p)),
        TruthKind::Cmp { op, .. } => match op {
            CmpOp::NullSafeEqual | CmpOp::Equal => None,
            CmpOp::NullSafeNotEqual | CmpOp::NotEqual => Some(NonEquality::Comparison("!=")),
            CmpOp::LessThan => Some(NonEquality::Comparison("<")),
            CmpOp::GreaterThan => Some(NonEquality::Comparison(">")),
            CmpOp::LessThanOrEqual => Some(NonEquality::Comparison("<=")),
            CmpOp::GreaterThanOrEqual => Some(NonEquality::Comparison(">=")),
        },
        TruthKind::Or(_) => Some(NonEquality::Or),
        TruthKind::Not(_) => Some(NonEquality::Not),
        TruthKind::Exists { .. } | TruthKind::Sigma { .. } => Some(NonEquality::Unprovable),
    }
}
