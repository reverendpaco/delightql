// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The bag road — where a set operation's correlation is settled.
//!
//! A bag step is a carrier, not a segment. Its arms are relations in their
//! own right, so the one question refinement has to answer is which PAIR of
//! arms a predicate standing on the run relates. That answer is decided
//! HERE, once, and written onto the step that owns the pair as an
//! [`ArmIx`]. Nothing downstream re-derives it from the predicate's column
//! owners — which is what made a three-arm correlation unrepresentable and
//! a bare name silently pick the first arm that published it.

use crate::pipeline::asts::core::ColumnOccurrence;
use std::rc::Rc;

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::vocabulary::CmpOp;

/// The arms one bag run combines: arm 0 is the chain the run stands on and
/// arm `k` is the `k`th step's arm.
pub(super) struct RunArms {
    scopes: Vec<ScopeId>,
}

impl RunArms {
    /// Read the run's arms off the chain it ends in.
    pub(super) fn of(expr: &resolved::Chain) -> Result<Self> {
        let run = expr
            .trailing_bag_run()
            .ok_or_else(|| DelightQLError::parse_error("a bag run was expected here"))?;
        let mut scopes = Vec::with_capacity(run.arms());
        scopes.push(arm_scope(&resolved::Chain {
            head: expr.head.clone(),
            continuations: expr.continuations[..run.base].to_vec(),
        }));
        for step in 0..run.steps {
            let resolved::Continuation::BagOp { arm, .. } = &expr.continuations[run.base + step]
            else {
                unreachable!("the run's steps are bag steps")
            };
            scopes.push(arm_scope(arm));
        }
        Ok(RunArms { scopes })
    }

    /// The one arm a resolved column reads, when exactly one does.
    ///
    /// Both directions of the republication chain are asked: a reference
    /// bound at the run's OUTPUT heading stands above the arm that carries
    /// its value, and a reference bound inside an arm stands below the
    /// heading its own operand published.
    fn of_column(&self, column: ColId, identities: &Registry) -> Option<usize> {
        if let Some(arm) = self.of_scope(identities.scope_of(column), identities) {
            return Some(arm);
        }
        // "Exactly one arm owns this" is a claim about every arm. An arm
        // whose dimensions the target never published cannot be shown to
        // own the column OR to be free of it, so no arm can be named the
        // sole owner while one is in the run.
        let mut owners: Vec<usize> = Vec::new();
        for (arm, scope) in self.scopes.iter().enumerate() {
            match identities.heading(*scope) {
                crate::names::HeadingKnowledge::Opaque => return None,
                crate::names::HeadingKnowledge::Known(heading) => {
                    if heading.iter().any(|export| {
                        identities.republishes(*export, column)
                            || identities.republishes(column, *export)
                    }) {
                        owners.push(arm);
                    }
                }
            }
        }
        match owners.as_slice() {
            [arm] => Some(*arm),
            _ => None,
        }
    }

    /// The one arm a scope belongs to, when exactly one does.
    fn of_scope(&self, scope: ScopeId, identities: &Registry) -> Option<usize> {
        let owners: Vec<usize> = self
            .scopes
            .iter()
            .enumerate()
            .filter(|(_, arm)| **arm == scope || identities.contains_scope(**arm, scope))
            .map(|(arm, _)| arm)
            .collect();
        match owners.as_slice() {
            [arm] => Some(*arm),
            _ => None,
        }
    }
}

fn arm_scope(chain: &resolved::Chain) -> ScopeId {
    crate::pipeline::resolver::helpers::extraction::extract_cpr_schema(chain)
}

/// The scope a chain publishes.
pub(super) fn published_scope(chain: &resolved::Chain) -> ScopeId {
    arm_scope(chain)
}

/// What one conjunct standing on the run turns out to be.
pub(super) enum Related {
    /// It constrains exactly this pair of arms, earlier first.
    Pair(usize, usize),
    /// It stands over the one relation the run publishes: it names a single
    /// arm, none at all, or nothing this walk reads as an arm reference.
    /// After a set operation there is one relation, and asking which arm a
    /// row came from has no answer.
    Whole,
    /// It relates three or more arms at once, so no step owns it.
    Spanning(usize),
}

/// What a conjunct standing on the run relates.
///
/// The caller splits a conjunction first: a correlation is stated one PAIR
/// at a time, and `x.a = y.a and y.a = z.a` is two correlations written with
/// one `and`, exactly as `x.a = y.a, y.a = z.a` is two written with a comma.
/// Reading the whole conjunction at once sees three arms and can name no
/// pair — which is how the two spellings came to disagree.
pub(super) fn related(
    conjunct: &resolved::TruthExpression,
    arms: &RunArms,
    identities: &Registry,
) -> Related {
    let mut named: Vec<usize> = Vec::new();
    if !collect_arms(conjunct, arms, identities, &mut named) {
        return Related::Whole;
    }
    pair(named)
}

/// What a whole-heading correlation relates.
///
/// The spelling `x.* = y.*` NAMES the pair: both sides are scopes, so there
/// is no column walk and no ambiguity to resolve. An arm the run does not
/// carry leaves the correlation owned by nothing, which the caller refuses
/// rather than silently filtering the finished relation.
pub(super) fn related_whole(
    whole: &resolved::WholeHeading,
    arms: &RunArms,
    identities: &Registry,
) -> Related {
    let (left, right) = whole.arms();
    let mut named: Vec<usize> = Vec::new();
    for scope in [left, right] {
        match arms.of_scope(*scope, identities) {
            Some(arm) => {
                if !named.contains(&arm) {
                    named.push(arm);
                }
            }
            None => return Related::Whole,
        }
    }
    pair(named)
}

fn pair(named: Vec<usize>) -> Related {
    match named.as_slice() {
        [left, right] => Related::Pair(*left.min(right), *left.max(right)),
        [..] if named.len() > 2 => Related::Spanning(named.len()),
        _ => Related::Whole,
    }
}

/// Split a predicate into the conjuncts a correlation is stated in.
///
/// Only `and` splits: it is the one connective under which each part must
/// hold on its own, so each part can be owned by the step whose pair it
/// names. A disjunction is one condition over the finished relation.
pub(super) fn conjuncts(
    predicate: resolved::TruthExpression,
    out: &mut Vec<resolved::TruthExpression>,
) {
    predicate.into_conjuncts(out);
}

/// Rejoin conjuncts into the one predicate they came from.
pub(super) fn conjoin(parts: Vec<resolved::TruthExpression>) -> Option<resolved::TruthExpression> {
    resolved::TruthExpression::all(parts)
}

/// A conjunct that relates three or more arms has no owning step.
///
/// Splitting it across steps would invent an association the writer did not
/// write, and keeping it above the run would silently make it a filter over
/// a heading its references do not stand in. Refuse and say how to write it.
pub(super) fn refuse_spanning_conjunct(arms: usize) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "resolution/setop/correlation_owner",
        format!(
            "a set-operation correlation relates two operands, and this condition \
             names {arms} at once"
        ),
        "write one condition per pair: `x(*) as a ; y(*) as b ; z(*) as c, \
         a.k = b.k, b.k = c.k`",
    )
}

/// A whole-heading correlation that names no pair of the run's arms.
///
/// Both sides name a stage, so this is a stage the run does not carry — a
/// correlation with nothing to correlate. It refuses rather than becoming a
/// filter over the finished relation, which is a different query.
pub(super) fn refuse_unowned_whole_heading() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "resolution/setop/correlation_owner",
        "a whole-heading correlation names two operands of this set operation, \
         and one of these names no operand of it",
        "name the two arms: `x(*) as a ; y(*) as b, a.* = b.*`",
    )
}

/// Collect the arms a predicate names, answering whether the whole
/// predicate was read. A shape this walk cannot read answers `false`: an
/// unread reference must not pass for one that is not there.
fn collect_arms(
    predicate: &resolved::TruthExpression,
    arms: &RunArms,
    identities: &Registry,
    out: &mut Vec<usize>,
) -> bool {
    use resolved::TruthExpression as Boolean;
    match predicate {
        Boolean::Comparison(Comparison { left, right, .. }) => {
            collect_domain_arms(left, arms, identities, out)
                && collect_domain_arms(right, arms, identities, out)
        }
        Boolean::Conjunction(parts) => parts
            .iter()
            .all(|part| collect_arms(part, arms, identities, out)),
        // Everything else is a filter over the finished relation, not a
        // correlation: disjunction, negation, membership, and the subquery
        // predicates each stand on the one heading the run publishes.
        Boolean::Disjunction(_)
        | Boolean::Not { .. }
        | Boolean::Membership { .. }
        | Boolean::Existence { .. }
        | Boolean::RelationalMembership { .. }
        | Boolean::Sigma { .. } => false,
    }
}

/// Collect the arms one side of a comparison names.
///
/// A correlation side may be COMPUTED — `upper:(x.v)`, `(y.id + 5)` — and
/// the arm it reads is the arm its references read. Every shape whose
/// references this walk can enumerate answers `true`; anything else answers
/// `false`, because an unread reference must not pass for one that is not
/// there.
fn collect_domain_arms(
    expr: &resolved::DomainExpression,
    arms: &RunArms,
    identities: &Registry,
    out: &mut Vec<usize>,
) -> bool {
    use resolved::DomainExpression as Domain;
    match expr {
        Domain::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column, ..
        }))) => match arms.of_column(*column, identities) {
            Some(arm) => {
                if !out.contains(&arm) {
                    out.push(arm);
                }
                true
            }
            None => false,
        },
        Domain::Application(function) => match function {
            resolved::FunctionApplication::Ground(_) => true,
            resolved::FunctionApplication::Standard(application) => {
                ({
                    let arguments = &application.call().arguments;
                    // A relational argument brings its own scope; a
                    // correlation reading one is not a pair of arms. A
                    // crossed argument brings its own truth.
                    arguments.relations().next().is_none()
                        && arguments
                            .value_domains()
                            .all(|argument| collect_domain_arms(argument, arms, identities, out))
                }) && application.guard.is_none()
            }
            resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => tuple
                .elements
                .iter()
                .all(|element| collect_domain_arms(element, arms, identities, out)),
            resolved::FunctionApplication::Infix(infix) => {
                collect_domain_arms(&infix.left, arms, identities, out)
                    && collect_domain_arms(&infix.right, arms, identities, out)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Refuse a bare correlation reference more than one arm publishes.
///
/// A correlation reference binds ONE operand's heading. A name both arms
/// carry gives it two candidates, and the merge chain would answer with
/// whichever arm minted the merged column — so the other arm's rows compare
/// against a value they never contributed and drop without a word.
pub(super) fn refuse_ambiguous_bare_reference(
    predicate: &resolved::TruthExpression,
    arms: &RunArms,
    identities: &Registry,
) -> Result<()> {
    let mut ambiguous = false;
    walk_bare_references(predicate, &mut |column| {
        let Some(name) = identities.published_sym(column) else {
            return;
        };
        // Likewise "only one arm carries this name": an arm nobody
        // enumerated might carry it too, so the reference is not shown to
        // be unambiguous and is refused as ambiguous rather than let by.
        let mut carriers = 0usize;
        for scope in &arms.scopes {
            match identities.heading(*scope) {
                crate::names::HeadingKnowledge::Opaque => {
                    ambiguous = true;
                    return;
                }
                crate::names::HeadingKnowledge::Known(heading) => {
                    if heading
                        .iter()
                        .any(|candidate| identities.published_sym(*candidate) == Some(name))
                    {
                        carriers += 1;
                    }
                }
            }
        }
        if carriers > 1 {
            ambiguous = true;
        }
    });
    if ambiguous {
        return Err(DelightQLError::validation_error_categorized(
            "resolution/setop/correlation_owner",
            "a bare set-operation correlation name is carried by more than one operand, \
             so it names no single column",
            "qualify the reference by the operand it reads",
        ));
    }
    Ok(())
}

fn walk_bare_references(predicate: &resolved::TruthExpression, note: &mut dyn FnMut(ColId)) {
    use resolved::TruthExpression as Boolean;
    match predicate {
        Boolean::Comparison(Comparison { left, right, .. }) => {
            walk_bare_domain(left, note);
            walk_bare_domain(right, note);
        }
        Boolean::Conjunction(parts) | Boolean::Disjunction(parts) => {
            for part in parts.iter() {
                walk_bare_references(part, note);
            }
        }
        Boolean::Not { expr } => walk_bare_references(expr, note),
        // A whole-heading correlation names its operands by scope, so it has
        // no bare reference to be ambiguous; the rest never reach a claim.
        _ => {}
    }
}

fn walk_bare_domain(expr: &resolved::DomainExpression, note: &mut dyn FnMut(ColId)) {
    use resolved::DomainExpression as Domain;
    match expr {
        Domain::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            explicit_qualifier,
            ..
        }))) => {
            if !explicit_qualifier {
                note(*column);
            }
        }
        Domain::Application(function) => match function {
            resolved::FunctionApplication::Standard(application) => {
                for argument in application.call().arguments.value_domains() {
                    walk_bare_domain(argument, note);
                }
            }
            resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => {
                for element in tuple.elements.iter() {
                    walk_bare_domain(element, note);
                }
            }
            resolved::FunctionApplication::Infix(infix) => {
                walk_bare_domain(&infix.left, note);
                walk_bare_domain(&infix.right, note);
            }
            _ => {}
        },
        _ => {}
    }
}

/// Refuse a correlation that names no operand on either side.
///
/// `users_2022(*) ; users_2023(*), email = email` reads as an equality
/// between one heading's column and itself: whichever arm published the
/// name first answers for both sides, and the other arm's rows compare
/// against nothing. Qualify the reference by the operand it reads.
pub(super) fn refuse_unqualified_correlation(predicate: &resolved::TruthExpression) -> Result<()> {
    let resolved::TruthExpression::Comparison(Comparison {
        left,
        right,
        operator,
    }) = predicate
    else {
        return Ok(());
    };
    if !matches!(operator, CmpOp::Equal | CmpOp::NullSafeEqual) {
        return Ok(());
    }
    if is_unqualified(left) && is_unqualified(right) {
        return Err(DelightQLError::validation_error_categorized(
            "resolution/setop/correlation_owner",
            "a set-operation correlation must say which operand each side reads",
            "alias the operands and qualify both sides: `a(*) as x ; b(*) as y, x.col = y.col`",
        ));
    }
    Ok(())
}

fn is_unqualified(expr: &resolved::DomainExpression) -> bool {
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence {
                explicit_qualifier, ..
            },
        ))) => !explicit_qualifier,
        _ => false,
    }
}

/// The whole-tuple correlation a BARE minus stands on.
///
/// Minus is minus: the rows of the left operand with no corresponding row
/// in the right, duplicates preserved and nulls matching nulls. That is the
/// anti-semijoin over every column, so a bare minus is a CORRELATED minus
/// whose predicate this fills in — bare and correlated never became two
/// roads.
pub(super) fn whole_tuple_correlation(
    left: ScopeId,
    arm: ScopeId,
    identities: &Rc<Registry>,
) -> Result<resolved::TruthExpression> {
    let left_columns = identities.known_heading(left)?.to_vec();
    let arm_columns = identities.known_heading(arm)?.to_vec();
    let matched = identities.corresponding_slots(&left_columns, &arm_columns)?;
    let mut comparisons = Vec::with_capacity(left_columns.len());
    for (left_column, arm_column) in left_columns.iter().zip(matched) {
        let Some(arm_column) = arm_column else {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/setop/minus_heading",
                "minus aligns by name and the right operand does not publish every name the left does",
                "rename the right operand's columns to match the left: `left(*) - right(|> *(other as name))`",
            ));
        };
        comparisons.push(resolved::TruthExpression::Comparison(Comparison {
            // Nulls match nulls here: the probe asks whether a matching row
            // is PRESENT, and a null-blind `=` would keep every null-bearing
            // row the right operand plainly contains.
            operator: CmpOp::NullSafeEqual,
            left: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column: *left_column,
                    explicit_qualifier: true,
                }),
            ))),
            right: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column: arm_column,
                    explicit_qualifier: true,
                }),
            ))),
        }));
    }
    resolved::TruthExpression::all(comparisons).ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            "resolution/setop/minus_heading",
            "minus has no columns to compare",
            "both operands must publish at least one column",
        )
    })
}

#[cfg(test)]
mod tests {
    //! What the correlation-owner law admits and what it refuses.

    use super::*;
    use crate::names::{Addressing, ColumnOrigin, Hint, ScopeOrigin, ValueFacts};

    fn lvar(column: ColId, explicit_qualifier: bool) -> resolved::DomainExpression {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            explicit_qualifier,
        })))
    }

    fn column_of(registry: &Registry, scope_name: &str, column_name: &str) -> ColId {
        let scope = registry.mint_scope(
            ScopeOrigin::AnonRelation,
            Hint::User(registry.intern(scope_name, false)),
            None,
        );
        registry.mint_column(
            scope,
            ColumnOrigin::Bound { position: 0 },
            Some(registry.intern(column_name, false)),
            Addressing::Published,
            ValueFacts::default(),
        )
    }

    fn call_of(name: &str, argument: resolved::DomainExpression) -> resolved::DomainExpression {
        let registry = Registry::new(&[]);
        resolved::DomainExpression::Application(resolved::FunctionApplication::Standard(
            crate::pipeline::asts::core::StandardApplication::plain(
                crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall {
                        callee: registry.mint_function(registry.intern(name, false), Vec::new()),
                        arguments: crate::pipeline::asts::core::operators::CallArguments::Scalar(
                            vec![crate::pipeline::asts::core::operators::ScalarArgument::plain(
                                argument,
                            )],
                        ),
                        marks: Default::default(),
                    },
                ),
            ),
        ))
    }

    fn equality(
        left: resolved::DomainExpression,
        right: resolved::DomainExpression,
    ) -> resolved::TruthExpression {
        resolved::TruthExpression::Comparison(Comparison {
            operator: CmpOp::Equal,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    /// A correlation with nothing but bare names on both sides has no way to
    /// say which operand each side reads.
    #[test]
    fn both_sides_bare_refuses() {
        let registry = Registry::new(&[]);
        let column = column_of(&registry, "x", "v");
        assert!(refuse_unqualified_correlation(&equality(
            lvar(column, false),
            lvar(column, false)
        ))
        .is_err());
    }

    /// A COMPUTED side is not a bare name: `upper:(x.v) = y.v` says which
    /// operand each side reads, and refusing it would refuse a legal
    /// per-column intersection.
    #[test]
    fn a_computed_side_is_not_bare() {
        let registry = Registry::new(&[]);
        let left = column_of(&registry, "x", "v");
        let right = column_of(&registry, "y", "v");
        assert!(refuse_unqualified_correlation(&equality(
            call_of("upper", lvar(left, true)),
            lvar(right, true)
        ))
        .is_ok());
        // Even wrapping a BARE reference: a function is not a bare column.
        assert!(refuse_unqualified_correlation(&equality(
            call_of("upper", lvar(left, false)),
            lvar(right, false)
        ))
        .is_ok());
    }

    /// One qualified side is enough: the pair is stated.
    #[test]
    fn one_qualified_side_is_enough() {
        let registry = Registry::new(&[]);
        let left = column_of(&registry, "x", "v");
        let right = column_of(&registry, "y", "v");
        assert!(
            refuse_unqualified_correlation(&equality(lvar(left, false), lvar(right, true))).is_ok()
        );
    }

    /// An arm whose dimensions the target never published could own the
    /// column too, so no other arm may be named its sole owner.
    #[test]
    fn an_opaque_arm_stops_a_unique_owner_conclusion() {
        let registry = Registry::new(&[]);
        // A column bound outside the run, republished by one arm. Ownership
        // is decided by reading the arms' headings, which is the road an
        // opaque arm breaks.
        let source = column_of(&registry, "source", "v");
        let known = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        registry.republish_column(
            source,
            known,
            crate::names::Republish::Passthrough,
            registry.published(source),
            Addressing::Published,
            |_| {},
        );
        let other = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        let opaque = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        registry.mark_heading_opaque(opaque);

        // With both arms enumerable, the arm that republishes it owns it.
        let settled = RunArms {
            scopes: vec![known, other],
        };
        assert_eq!(settled.of_column(source, &registry), Some(0));

        // With one arm opaque, no arm is shown to be the only owner.
        let unsettled = RunArms {
            scopes: vec![known, opaque],
        };
        assert_eq!(unsettled.of_column(source, &registry), None);
    }

    /// Likewise "only one arm carries this name": an arm nobody enumerated
    /// might carry it too, so the reference is refused rather than let by.
    #[test]
    fn an_opaque_arm_stops_a_non_ambiguity_conclusion() {
        let registry = Registry::new(&[]);
        let carried = column_of(&registry, "known", "v");
        let known = registry.scope_of(carried);
        let opaque = registry.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None);
        registry.mark_heading_opaque(opaque);
        let predicate = equality(lvar(carried, false), lvar(carried, false));

        let settled = RunArms {
            scopes: vec![known],
        };
        assert!(refuse_ambiguous_bare_reference(&predicate, &settled, &registry).is_ok());

        let unsettled = RunArms {
            scopes: vec![known, opaque],
        };
        assert!(refuse_ambiguous_bare_reference(&predicate, &unsettled, &registry).is_err());
    }
}
