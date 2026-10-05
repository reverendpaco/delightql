// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A rule value (top-grammar FN.48): the boundary a designator crosses into
//! a rule-valued position, and what the relation a spend completes it over
//! is to the rows its configured values were born over.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::heading::{Name, Visibility};
use crate::pipeline::middle::core::ids::{PassengerId, RelId};
use crate::pipeline::middle::core::node::walk::{self, Child};
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// One position of a parameter row, as a residual contract reads it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Mode {
    Scalar,
    /// A relation: open (`T(*)`), or declared with these names.
    Relation(Option<Vec<Name>>),
    /// A rule value. No contract's remaining position is one (there is no
    /// order above three).
    Rule,
}

/// A parameter row and the heading its completion publishes: a family's
/// declared signature, or a residual contract's remaining positions and
/// final group. `None` is an open heading (`(*)`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Signature {
    pub(crate) modes: Vec<Mode>,
    pub(crate) output: Option<Vec<Name>>,
}

/// THE BOUNDARY (FN.48): designating a family whose declared signature is
/// `family`, its first `configured` positions sealed, into a position whose
/// contract is `contract`. A completed application leaves no residual; the
/// positions that remain must be the contract's, in order and kind, and a
/// declared relation or heading must state the contract's names. An open
/// contract position (`T(*)`, `(*)`) means any heading, so it admits a
/// family that declares one; a declared contract position is exact, so a
/// family that leaves it open refuses (FN.48).
pub(crate) fn construct(rule: &str, family: &Signature, configured: usize, contract: &Signature) -> Result<(), Refusal> {
    if configured >= family.modes.len() {
        return Err(refuse::residual_none(rule, configured, family.modes.len()));
    }
    let remaining = &family.modes[configured..];
    if remaining.len() != contract.modes.len() {
        return Err(refuse::residual_contract_mismatch(rule));
    }
    for (have, want) in remaining.iter().zip(&contract.modes) {
        match (have, want) {
            (Mode::Scalar, Mode::Scalar) | (Mode::Relation(_), Mode::Relation(None)) => {}
            (Mode::Relation(Some(have)), Mode::Relation(Some(want))) if have == want => {}
            (Mode::Relation(None), Mode::Relation(Some(_))) => return Err(refuse::residual_contract_open(rule)),
            (Mode::Scalar | Mode::Relation(_) | Mode::Rule, _) => return Err(refuse::residual_contract_mismatch(rule)),
        }
    }
    match (&family.output, &contract.output) {
        (_, None) => Ok(()),
        (Some(have), Some(want)) if have == want => Ok(()),
        (Some(_), Some(_)) => Err(refuse::residual_contract_mismatch(rule)),
        (None, Some(_)) => Err(refuse::residual_contract_open(rule)),
    }
}

/// What the relation a spend completes a rule value over is to the rows
/// its configured values were born over (FN.48).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Completion {
    /// Its rows carry every configured value: each is one of the
    /// construction rows, or derived from one row for row, and completes
    /// with that row's value alone.
    Carried,
    /// It reads no construction row: a distinct relation, which joins the
    /// value's capture and meets every construction row's value.
    Distinct,
}

/// The completion of a rule value whose configured values ride
/// `passengers`, born over the rows `capture` carries, over `supplied`.
/// A relation that reads the construction rows but no longer carries their
/// values was made by combining them (a reduction): what it completes with
/// is not decided, and it refuses.
pub(crate) fn completion(
    arena: &impl Judging,
    supplied: RelId,
    passengers: &[PassengerId],
    capture: RelId,
) -> Result<Completion, Refusal> {
    let heading = arena.rel(supplied).heading();
    let carried = |p: &PassengerId| heading.positions().iter().any(|q| q.visibility == Visibility::Hidden(*p));
    if passengers.iter().all(carried) {
        return Ok(Completion::Carried);
    }
    if passengers.iter().any(carried) {
        return Err(refuse::outside("a completion carrying some of a rule value's configured values and not others"));
    }
    if walk::reachable(arena, &[Child::Rel(supplied)]).rels.contains(&capture) {
        return Err(refuse::residual_completion());
    }
    Ok(Completion::Distinct)
}
