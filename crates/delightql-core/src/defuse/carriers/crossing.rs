// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CROSSING CARRIERS A CLOSED VALUE PUBLISHES THROUGH A PROJECTION.
//!
//! A residual's construction-row token and its captured configured values
//! cross a definition use as hygienic positions of the carrier relation. A
//! projection the body writes states its own heading and would drop them;
//! this rebuilds that projection to publish them beside it, as semantic
//! positions a later join can spend. It serves moded application only. A
//! correlation's support is a different thing — an obligation the relation
//! authority carries by construction from the correlation act to the
//! interior boundary — and no projection is rebuilt for it anywhere.

use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;

/// THE LAST PROJECTION OF A CHAIN'S TRANSPARENT RUN, where a carrier its
/// operand still has would be published beside the items.
///
/// WHICH STEP DROPPED IT IS A CHAIN QUESTION, NOT A LAST-STEP ONE. Only a
/// projection can strip a carrier, but the steps a projection is followed
/// by — an ordering, a bound, a restriction — publish the NARROWED heading,
/// so a projection three steps back has dropped the carrier just as
/// completely as a trailing one. The carrier is published at the projection
/// that dropped it, because that is the last level standing on the operand
/// that still has it; every transparent step above carries it onward.
///
/// A STEP THAT REPLACES THE RELATION CANNOT CARRY WHAT IT DOES NOT KNOW
/// ABOUT. Publishing under a grouping or a set would leave the carrier owed
/// by a relation nothing above publishes it from; the transparent run is
/// exactly the steps whose result IS the projection's. `None` when the
/// chain has no projection, or its last one is not the final relation's.
struct LastProjection {
    at: usize,
    tail: Vec<resolved::Step>,
    pipe_relation: crate::relation::SemanticRelation,
    items: Vec<resolved::OutItem>,
    stored: crate::relation::pending::Publishes,
}

fn last_transparent_projection(subquery: &resolved::Chain) -> Option<LastProjection> {
    let at = subquery.continuations().iter().rposition(|step| {
        matches!(
            step.form(),
            resolved::Continuation::Pipe {
                operator: resolved::PipeOp::Project(_) | resolved::PipeOp::Embed(_),
                ..
            }
        )
    })?;
    let final_relation = subquery.semantic_relation();
    let tail: Vec<_> = subquery.continuations()[at + 1..].to_vec();
    if tail.iter().any(|step| *step.result() != final_relation) {
        return None;
    }
    let projection = subquery.continuations().get(at).cloned()?;
    let pipe_relation = *projection.result();
    let resolved::Continuation::Pipe {
        operator: pipe_operator,
        named: (),
    } = projection.into_form()
    else {
        return None;
    };
    let stored = match &pipe_operator {
        resolved::PipeOp::Embed(_) => crate::relation::pending::Publishes::Edited,
        _ => crate::relation::pending::Publishes::Anew,
    };
    let (resolved::PipeOp::Project(items) | resolved::PipeOp::Embed(items)) = pipe_operator else {
        return None;
    };
    Some(LastProjection {
        at,
        tail,
        pipe_relation,
        items: items.into_vec(),
        stored,
    })
}

/// REPLACE THE PROJECTION with the derivation `staged` and continue every
/// tail step over it: a transparent step is RESTATED there, and a stage
/// republication (the ordering) is re-derived over the rebuilt operand with
/// its landing recorded — so references that resolved against the old
/// stage bind through the record.
fn continue_tail_over(
    rebuilt: resolved::Chain,
    staged: resolved::Step,
    projection: LastProjection,
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    let authority = identities.authority();
    let mut chain = authority.reland(rebuilt, staged)?;
    let mut stood = projection.pipe_relation;
    for step in projection.tail {
        let next = *step.result();
        chain = authority.continue_over(chain, step, stood)?;
        stood = next;
    }
    Ok(chain)
}

/// THE SUPPORT ONE FIXPOINT CLAUSE PUBLISHES, sealed: each pair names the
/// position of the clause's projection operand that continues a caller
/// actual and the exact actual it stands for. Constructible only here,
/// from a clause resolved under its own [`DefinitionFrontier`] — no safe
/// signature outside this module pairs a relation with actual ports and
/// asks the relation authority to mark them.
pub(crate) struct FrontierSupport {
    carriers: Vec<(crate::relation::PortId, crate::relation::PortId)>,
}

impl FrontierSupport {
    /// The `(landed, actual)` pairs, for the one relation-authority act that
    /// consumes this value whole.
    pub(crate) fn carriers(&self) -> &[(crate::relation::PortId, crate::relation::PortId)] {
        &self.carriers
    }
}

pub(in crate::defuse) fn carry_frontier_actuals(
    subquery: resolved::Chain,
    frontier: &crate::defuse::instance::DefinitionFrontier,
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    let actuals = frontier.carried_actuals();
    if actuals.is_empty() {
        return Ok(subquery);
    }
    let authority = identities.authority();
    let stands_for = |port: crate::relation::PortId, actual: crate::relation::PortId| {
        authority.frontier_actual(port) == Some(actual)
            || identities.continues_occurrence(port, actual)
    };
    let heading = crate::relation::published_ports(identities, &subquery.semantic_relation())?;
    let missing: Vec<_> = actuals
        .iter()
        .copied()
        .filter(|actual| !heading.iter().any(|port| stands_for(*port, *actual)))
        .collect();
    if missing.is_empty() {
        return Ok(subquery);
    }
    let uncarried = || {
        Internal::invariant(
            "recursive caller isolation",
            "a parameterized fixpoint clause does not carry the caller's actual to its \
             frontier: the actual is discarded before the clause's last projection",
        )
    };
    let Some(projection) = last_transparent_projection(&subquery) else {
        return Err(uncarried());
    };
    let rebuilt = subquery.clone().truncated(projection.at);
    let operand_ports = crate::relation::published_ports(identities, &rebuilt.semantic_relation())?;
    // THE POSITION THAT CARRIES THE ACTUAL IS THE ONE THAT CONTINUES ITS
    // OCCURRENCE, by the continuation edge construction wrote; a value
    // republished at a second position is a copy, not the occurrence, and
    // a clause that keeps only copies has discarded the caller.
    let mut carriers = Vec::with_capacity(missing.len());
    for actual in missing {
        let landed: Vec<_> = operand_ports
            .iter()
            .copied()
            .filter(|port| stands_for(*port, actual))
            .collect();
        let landed = match landed.as_slice() {
            [landed] => *landed,
            [] => return Err(uncarried()),
            _ => {
                return Err(Internal::invariant(
                    "recursive caller isolation",
                    "a caller actual continues at more than one position of a fixpoint \
                     clause's projection operand",
                ))
            }
        };
        carriers.push((landed, actual));
    }
    let items = projection.items.clone();
    let stored = projection.stored;
    let (staged, _) =
        authority.bind(crate::relation::pending::Pending::FrontierActualInjection {
            replaces: projection.pipe_relation,
            support: FrontierSupport { carriers },
            items,
            stored,
        })?;
    continue_tail_over(rebuilt, staged, projection, identities)
}

/// Publish the crossing carriers through the projection that would strip
/// them, where one stands in the chain's shaping run.
pub(crate) fn inject_crossing_carriers(
    subquery: resolved::Chain,
    carriers: &[crate::relation::PortId],
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    if carriers.is_empty() {
        return Ok(subquery);
    }

    // No projection anywhere, or a relation-replacing tail: every column the
    // operand published is still published, or nothing above could publish
    // the carrier — either way there is nothing to carry here.
    let Some(projection) = last_transparent_projection(&subquery) else {
        return Ok(subquery);
    };
    let pipe_relation = projection.pipe_relation;
    let items = &projection.items;

    // Which carriers are missing from the projection. The occurrence an
    // item READS is what containment asks about, not the one it publishes.
    fn projected_column(item: &resolved::OutItem) -> Option<crate::relation::PortId> {
        match item.value() {
            Some(resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            )))) => Some(*column),
            _ => None,
        }
    }
    let projected_columns: std::collections::HashSet<crate::relation::PortId> =
        items.iter().filter_map(projected_column).collect();

    let new_items = items.clone();
    let rebuilt = subquery.clone().truncated(projection.at);
    let operand = rebuilt.semantic_relation();
    let operand_ports = crate::relation::published_ports(identities, &operand)?;
    let authority = identities.authority();
    let mut landed_carriers = Vec::new();

    for source in carriers {
        // Already-projected is a CHAIN question, not a ColId one: the
        // projection references a downstream occurrence of the carrier, and
        // publishing beside it mints a second carrier of the same value —
        // which later makes a by-value re-anchor genuinely ambiguous.
        let source_token = authority.residual_row_token(*source);
        let token_already_projected = source_token.is_some_and(|token| {
            projected_columns
                .iter()
                .any(|projected| authority.residual_row_token(*projected) == Some(token))
        });
        if projected_columns.contains(source) || token_already_projected {
            continue;
        }
        // WHICH position of the operand the carrier is comes from the
        // construction record: a boundary between the capture and this
        // projection republished it, and the carrier is the position
        // standing here, not the one the capture was written against.
        if let Some(value) = authority.residual_capture_value(*source) {
            let matches: Vec<_> = operand_ports
                .iter()
                .copied()
                .filter(|port| authority.residual_capture_value(*port) == Some(value))
                .collect();
            match matches.as_slice() {
                [landed] => {
                    landed_carriers.push(*landed);
                    continue;
                }
                // A value this world never carried crosses nothing here.
                [] => continue,
                [_, _, ..] => {
                    return Err(Internal::invariant(
                        "crossing injection",
                        "a residual configured value does not land exactly once in the \
                         projection operand",
                    ));
                }
            }
        }
        if let Some(token) = authority.residual_row_token(*source) {
            let matches: Vec<_> = operand_ports
                .iter()
                .copied()
                .filter(|port| authority.residual_row_token(*port) == Some(token))
                .collect();
            match matches.as_slice() {
                [landed] => {
                    landed_carriers.push(*landed);
                    continue;
                }
                [] => continue,
                [_, _, ..] => {
                    return Err(Internal::invariant(
                        "crossing injection",
                        "a residual row token does not land exactly once in the projection \
                         operand",
                    ));
                }
            }
        }
        let Some(landed) = crate::relation::landed_in(identities, &operand_ports, *source)? else {
            return Err(Internal::invariant(
                "crossing injection",
                "a crossing carrier is not a position of the projection operand",
            ));
        };
        landed_carriers.push(landed);
    }

    if landed_carriers.is_empty() {
        return Ok(subquery);
    }

    // ONE ACT: the rebuilt interface, the items that stand at it, and the
    // map from the projection this REPLACES are all written by the same
    // derivation. Nothing here asks afterwards whether two finished
    // relations are related — the replacement says where its operand's
    // positions went while it is putting them there.
    let stored = projection.stored;
    let (staged, _) = authority.bind(
        crate::relation::pending::Pending::CrossingCarrierInjection {
            replaces: pipe_relation,
            carriers: landed_carriers,
            items: new_items,
            stored,
        },
    )?;
    continue_tail_over(rebuilt, staged, projection, identities)
}
