// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A comma run's join roles, its relations' placements and its merged
//! keys' value rules (W5 #9), decided once every member is known: an early
//! member's role depends on the members after it.
//!
//! A comma run associates left to right: each member joins the members
//! before it, in written order, and a relation mentioning a later member is
//! placed at that member's join. A run of two members, or one whose members
//! are all marked or all unmarked, reads the same under the markedness
//! wording (top-grammar FN.41), which is how its roles are stored: the
//! unmarked members are required, inner-joined; a marked member attaches
//! onto the members its relations read; when every member is marked the run
//! is a full outer left fold in written order. A run of three or more
//! members, some marked and some not, stores its written-order fold. In a
//! run whose members are all marked, a relation reading two or more members
//! is the condition of the full join of the last one it reads, one reading
//! a single member is a filter after the run, and every full join needs a
//! relation of its own. A member whose rows exist only per row of the
//! members it reads is never a preserved operand.

use crate::pipeline::middle::core::node::{FoldJoin, JoinRole, MergeRule, Placement};

/// One member's markedness as written: its own `?`, and whether it
/// completes a marked lead.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Marks {
    pub(crate) marked: bool,
    pub(crate) completes_marked_lead: bool,
}

/// Each member's markedness. The lead member's `?` is written on the
/// member after it.
pub(crate) fn marked(members: &[Marks]) -> Vec<bool> {
    members
        .iter()
        .enumerate()
        .map(|(i, m)| {
            if i == 0 {
                members.get(1).is_some_and(|next| next.completes_marked_lead)
            } else {
                m.marked
            }
        })
        .collect()
}

/// Each member's effective mark: the mark every judgment of the run reads
/// (its role, its merges' value rules and the placement of every relation).
/// Stored by markedness, a marked member is the padded side of its join
/// with the members it attaches onto; a member an unmarked dependent member
/// reads (`pinned`) is never padded, since the dependent member has no row
/// without it, so its mark is spent on nothing and it is required. Under
/// the written-order fold a dependent member joins the relation accumulated
/// before it, which it never pads, and the fold's own rule places it.
pub(crate) fn effective(marked: &[bool], association: Association, pinned: &[bool]) -> Vec<bool> {
    marked
        .iter()
        .enumerate()
        .map(|(i, m)| match association {
            Association::Markedness => *m && !pinned.get(i).copied().unwrap_or(false),
            Association::WrittenOrderFold => *m,
        })
        .collect()
}

/// How a run's roles are stored: by markedness where the written-order
/// fold and the markedness wording agree (two members, or every member
/// marked or none), as the fold otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Association {
    Markedness,
    WrittenOrderFold,
}

pub(crate) fn association(marked: &[bool]) -> Association {
    let mixed = marked.iter().any(|m| *m) && marked.iter().any(|m| !*m);
    if marked.len() >= 3 && mixed {
        Association::WrittenOrderFold
    } else {
        Association::Markedness
    }
}

/// Where a relation between members applies: a condition, a merge, or a
/// member's dependence on the members its relation reads. `members` is the
/// set of the run's member indexes the relation reads. A relation reading
/// at most one member is a filter on the run so far. One reading two or
/// more matches across a join: stored by markedness, it is the attaching
/// condition of the last marked member it reads, or, when it reads only
/// required members (or the run is a full fold), the condition of the join
/// of the last member it reads; stored as the fold, it is the condition of
/// the join of the last member it reads.
pub(crate) fn placement(marked: &[bool], members: &[usize], association: Association) -> Placement {
    if members.len() < 2 {
        return Placement::Filter;
    }
    across(marked, members, association)
}

/// The join a relation reading two or more relation occurrences matches
/// across.
fn across(marked: &[bool], members: &[usize], association: Association) -> Placement {
    let last = members.iter().copied().max().unwrap_or(0);
    let full = marked.iter().all(|m| *m);
    let last_marked = members.iter().copied().filter(|i| marked.get(*i).copied().unwrap_or(false)).max();
    match (association, full, last_marked) {
        (Association::Markedness, false, Some(member)) => Placement::On { member },
        (Association::Markedness, true, _)
        | (Association::Markedness, false, None)
        | (Association::WrittenOrderFold, _, _) => Placement::On { member: last },
    }
}

/// Each member's role before its attachment is known: required, attached,
/// full when every member is marked, or, under the written-order fold, its
/// join onto the relation accumulated before it. A member whose rows exist
/// only per row of the members it reads (`dependent`) is never a preserved
/// operand: marked it attaches, unmarked it is inner, and in an all-marked
/// run it attaches onto the full fold of the others.
///
/// The fold consumes the relation to its left, so each member joins by the
/// accumulated relation's optionality against its own. The accumulated
/// relation is optional while every member in it is marked. A marked member
/// joins an optional accumulation FULL (consecutive marked members at the
/// run's head are one full outer fold, as an all-marked run is) and any
/// other LEFT. An unmarked member joins the marked lead alone RIGHT (the
/// lead's mark is spent on its peer, top-grammar FN.41) and any other
/// accumulation INNER: the full fold of two or more marked members is a
/// relation like any other.
pub(crate) fn kinds(marked: &[bool], association: Association, dependent: &[bool]) -> Vec<JoinRole> {
    let full = !marked.is_empty() && marked.iter().all(|m| *m);
    marked
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let dependent = dependent.get(i).copied().unwrap_or(false);
            match (association, full, *m) {
                (Association::Markedness, true, _) if dependent => JoinRole::Attached { onto: Vec::new() },
                (Association::Markedness, true, _) => JoinRole::Full,
                (Association::Markedness, false, true) => JoinRole::Attached { onto: Vec::new() },
                (Association::Markedness, false, false) => JoinRole::Required,
                (Association::WrittenOrderFold, _, marked_here) => {
                    let optional = i > 0 && marked[..i].iter().all(|m| *m);
                    JoinRole::Folded(match (i, marked_here, optional && !dependent) {
                        (0, _, _) => FoldJoin::Start,
                        (_, true, true) => FoldJoin::Full,
                        (_, true, false) => FoldJoin::Left,
                        (1, false, true) => FoldJoin::Right,
                        (_, false, _) => FoldJoin::Inner,
                    })
                }
            }
        })
        .collect()
}

/// Each member's role, from the effective marks. `relations` pairs every
/// relation's placement with the member indexes it reads: stored by
/// markedness, a marked member attaches onto every other member a relation
/// placed on its join reads.
pub(crate) fn roles(
    marked: &[bool],
    relations: &[(Placement, Vec<usize>)],
    association: Association,
    dependent: &[bool],
) -> Vec<JoinRole> {
    kinds(marked, association, dependent)
        .into_iter()
        .enumerate()
        .map(|(i, kind)| match kind {
            JoinRole::Attached { .. } => {
                let mut onto: Vec<usize> = relations
                    .iter()
                    .filter(|(placement, _)| *placement == Placement::On { member: i })
                    .flat_map(|(_, read)| read.iter().copied().filter(|r| *r != i))
                    .collect();
                onto.sort_unstable();
                onto.dedup();
                JoinRole::Attached { onto }
            }
            JoinRole::Required | JoinRole::Full | JoinRole::Folded(_) => kind,
        })
        .collect()
}

/// A merged key's value rule, from its right member's kind and whether its
/// left operand is present in every row of the tree.
pub(crate) fn merge_rule(right: &JoinRole, left_present: bool) -> MergeRule {
    match right {
        JoinRole::Full | JoinRole::Folded(FoldJoin::Full) => MergeRule::Coalesced,
        JoinRole::Attached { .. } | JoinRole::Folded(FoldJoin::Left) => MergeRule::Left,
        JoinRole::Required if left_present => MergeRule::Agreed,
        JoinRole::Required | JoinRole::Folded(FoldJoin::Right) => MergeRule::Right,
        JoinRole::Folded(FoldJoin::Start | FoldJoin::Inner) => MergeRule::Agreed,
    }
}

/// The first member after the run's first whose full join no relation is
/// placed on: a full join needs a condition saying how its two sides align.
/// `None` when every one has one.
pub(crate) fn unrelated_full_join(roles: &[JoinRole], relations: &[(Placement, Vec<usize>)]) -> Option<usize> {
    (1..roles.len())
        .filter(|i| matches!(roles[*i], JoinRole::Full | JoinRole::Folded(FoldJoin::Full)))
        .find(|i| !relations.iter().any(|(placement, _)| *placement == Placement::On { member: *i }))
}
