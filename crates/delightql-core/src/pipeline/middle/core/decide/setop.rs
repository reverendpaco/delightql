// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What a correlated union step compares under the acknowledged
//! `min_multiplicity` gate, which pairs copies of whole rows
//! (set-operations-law, the gate's note): one equality of a name (or one
//! ordinal) both arms publish, every such name (`x.* = y.*`) under the name
//! modes, or every position (`x|*| = y|*|`) under the positional mode. Its
//! match is set identity: NULL meets NULL. Every other correlation filters
//! the arms it names (elaborate::bag).

use crate::pipeline::middle::core::heading::correspondence::answers_to;
use crate::pipeline::middle::core::heading::{Heading, Name};
use crate::pipeline::middle::core::node::setop::{Atom, Side};
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// How a step aligns its arms, and so how a correlation addresses them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Names,
    Ordinals,
}

/// Whether the statement acknowledges the `min_multiplicity` gate where a
/// step stands: not at all; in the statement's own text (its query-local
/// bodies included: a query-scoped body is the query's own text); or where
/// the step stands in a consulted body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    Closed,
    Open,
    Consulted,
}

/// The matched position pairs (a left-arm position, a right-arm position)
/// of a gated correlation written as `atoms` over a step of `mode`: its
/// conjuncts are one key (ONE PAIR, ONE CORRELATION), every conjunct's
/// pairs, each once, in written order. Whether the key is whole is
/// `gated`'s judgment over all of them.
pub(crate) fn correlation(mode: Mode, left: &Heading, right: &Heading, atoms: &[Atom]) -> Result<Vec<(usize, usize)>, Refusal> {
    let mut pairs = Vec::new();
    for atom in atoms {
        for pair in atom_pairs(mode, left, right, atom)? {
            if !pairs.contains(&pair) {
                pairs.push(pair);
            }
        }
    }
    Ok(pairs)
}

/// The position pairs one conjunct of a gated correlation compares.
fn atom_pairs(mode: Mode, left: &Heading, right: &Heading, atom: &Atom) -> Result<Vec<(usize, usize)>, Refusal> {
    if !atom.equality {
        return Err(refuse::min_multiplicity_operator());
    }
    match (mode, &atom.left, &atom.right) {
        (_, Side::Other, _) | (_, _, Side::Other) => Err(refuse::min_multiplicity_partial()),
        (Mode::Names, Side::Name(a), Side::Name(b)) => {
            if a != b {
                return Err(refuse::min_multiplicity_partial());
            }
            if answers_to(left, a).is_empty() && answers_to(right, b).is_empty() {
                return Err(refuse::column(a.as_str(), "neither arm of the correlation publishes this name"));
            }
            Ok(vec![(named(left, a)?, named(right, b)?)])
        }
        (Mode::Names, Side::AllNames, Side::AllNames) => {
            let pairs: Vec<(usize, usize)> = left
                .displayed()
                .filter_map(|(l, p)| {
                    let name = p.answering_name()?;
                    match answers_to(right, name).as_slice() {
                        [r] => Some((l, *r)),
                        _ => None,
                    }
                })
                .collect();
            if pairs.is_empty() {
                return Err(refuse::whole_correlation_empty(&"the first arm", &"the second arm"));
            }
            Ok(pairs)
        }
        (Mode::Ordinals, Side::Ordinal(i), Side::Ordinal(j)) => {
            if i != j {
                return Err(refuse::min_multiplicity_partial());
            }
            Ok(vec![(ordinal(left, *i)?, ordinal(right, *j)?)])
        }
        (Mode::Ordinals, Side::AllOrdinals, Side::AllOrdinals) => {
            Ok(left.displayed().map(|(l, _)| l).zip(right.displayed().map(|(r, _)| r)).collect())
        }
        _ => Err(refuse::min_multiplicity_partial()),
    }
}

/// Whether the acknowledged gate pairs a correlated union step: the gate
/// written in the statement's own text, over a correlation comparing every
/// aligned position of both arms.
pub(crate) fn gated(gate: Gate, alignment: &[(Option<usize>, Option<usize>)], pairs: &[(usize, usize)]) -> Result<(), Refusal> {
    match gate {
        Gate::Closed => Err(refuse::contract("a correlation stored on a step no gate acknowledges")),
        Gate::Consulted => Err(refuse::unruled(
            "whether a statement's min_multiplicity gate reaches a correlated union in a consulted body",
        )),
        Gate::Open => {
            let whole = alignment
                .iter()
                .all(|(l, r)| matches!((l, r), (Some(l), Some(r)) if pairs.contains(&(*l, *r))));
            if whole {
                Ok(())
            } else {
                Err(refuse::min_multiplicity_partial())
            }
        }
    }
}

/// The one displayed position of an arm answering to a name a correlation
/// writes; a correlation may name only what its arm publishes.
fn named(heading: &Heading, name: &Name) -> Result<usize, Refusal> {
    match answers_to(heading, name).as_slice() {
        [one] => Ok(*one),
        [] => Err(refuse::column(name.as_str(), "the correlated arm does not publish this name")),
        [_, _, ..] => Err(refuse::ambiguous_column(name)),
    }
}

/// The displayed position an ordinal (counted from one) addresses.
fn ordinal(heading: &Heading, at: usize) -> Result<usize, Refusal> {
    let displayed: Vec<usize> = heading.displayed().map(|(i, _)| i).collect();
    at.checked_sub(1)
        .and_then(|k| displayed.get(k).copied())
        .ok_or_else(|| refuse::column(&format!("|{at}|"), "past the arm's displayed heading"))
}
