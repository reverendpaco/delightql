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

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::vocabulary::CmpOp;

/// The arms one bag run combines: arm 0 is the chain the run stands on and
/// arm `k` is the `k`th step's arm.
pub(super) struct RunArms {
    relations: Vec<crate::relation::SemanticRelation>,
    /// The set relations the run's steps publish, innermost first. A
    /// reference bound at a run's OUTPUT heading stands above the arm
    /// whose contribution cell carries its value, and the matrix the set's
    /// construction wrote is what says which.
    results: Vec<crate::relation::SemanticRelation>,
}

impl RunArms {
    /// Read the run's arms off the chain it ends in.
    pub(super) fn of(expr: &resolved::Chain) -> Result<Self> {
        let run = expr
            .trailing_bag_run()
            .ok_or_else(|| DelightQLError::parse_error("a bag run was expected here"))?;
        let mut relations = Vec::with_capacity(run.arms());
        let mut results = Vec::with_capacity(run.steps);
        relations.push(arm_relation(&expr.prefix(run.base)));
        for step in 0..run.steps {
            let at = &expr.continuations()[run.base + step];
            let resolved::Continuation::BagOp { arm, .. } = at.form() else {
                unreachable!("the run's steps are bag steps")
            };
            relations.push(arm_relation(arm));
            results.push(*at.result());
        }
        Ok(RunArms { relations, results })
    }

    /// Follow a reference bound at a run's OUTPUT heading down to the one
    /// arm whose cell contributes its value. The walk reads the
    /// contribution matrices the set construction wrote — never position,
    /// spelling, or a pad. A value more than one operand contributes is an
    /// ambiguity, not an owner.
    fn carried_output(
        &self,
        port: crate::relation::PortId,
        identities: &crate::relation::Planning,
    ) -> Result<Carried> {
        let mut port = port;
        let mut walked = false;
        'walk: loop {
            for result in &self.results {
                let Some(matrix) = crate::relation::contributions(identities, result)? else {
                    continue;
                };
                let Some(output) = matrix.outputs().iter().find(|slot| slot.result() == port)
                else {
                    continue;
                };
                let contributing: Vec<crate::relation::PortId> = output
                    .by_arm()
                    .iter()
                    .filter_map(|cell| match cell {
                        crate::relation::set::Contribution::Port(p) => Some(*p),
                        crate::relation::set::Contribution::Padding(_) => None,
                    })
                    .collect();
                match contributing.as_slice() {
                    [one] => {
                        port = *one;
                        walked = true;
                        continue 'walk;
                    }
                    [] => return Ok(Carried::No),
                    _ => return Ok(Carried::Several),
                }
            }
            return Ok(match walked {
                true => Carried::One(port),
                false => Carried::No,
            });
        }
    }

    /// The one arm a resolved column reads, when exactly one does.
    ///
    /// Both directions of the republication chain are asked: a reference
    /// bound at the run's OUTPUT heading stands above the arm that carries
    /// its value, and a reference bound inside an arm stands below the
    /// heading its own operand published.
    fn of_port(
        &self,
        port: crate::relation::PortId,
        identities: &crate::relation::Planning,
    ) -> Option<usize> {
        let authority = identities.authority();
        let owners: Vec<usize> = self
            .relations
            .iter()
            .enumerate()
            .filter_map(|(arm, relation)| {
                authority
                    .interface(relation)
                    .ok()
                    .filter(|interface| !interface.is_opaque() && interface.ports().contains(&port))
                    .map(|_| arm)
            })
            .collect();
        match owners.as_slice() {
            [arm] => Some(*arm),
            _ => None,
        }
    }

    /// The arm a reference reads, through either direction of the record:
    /// an arm's own port directly, or a run-output port through the
    /// contribution walk.
    fn arm_and_port(
        &self,
        port: crate::relation::PortId,
        identities: &crate::relation::Planning,
    ) -> Result<Attributed> {
        if let Some(arm) = self.of_port(port, identities) {
            return Ok(Attributed::Arm(arm));
        }
        Ok(match self.carried_output(port, identities)? {
            Carried::One(carried) => match self.of_port(carried, identities) {
                Some(arm) => Attributed::Arm(arm),
                None => Attributed::No,
            },
            Carried::Several => Attributed::Shared,
            Carried::No => Attributed::No,
        })
    }

    /// The one arm a scope belongs to, when exactly one does.
    fn of_relation(&self, relation: crate::relation::SemanticRelation) -> Option<usize> {
        let owners: Vec<usize> = self
            .relations
            .iter()
            .enumerate()
            .filter(|(_, arm)| **arm == relation)
            .map(|(arm, _)| arm)
            .collect();
        match owners.as_slice() {
            [arm] => Some(*arm),
            _ => None,
        }
    }
}

fn arm_relation(chain: &resolved::Chain) -> crate::relation::SemanticRelation {
    chain.semantic_relation()
}

/// Where one reference's value comes from, read off the record.
enum Carried {
    One(crate::relation::PortId),
    /// More than one operand contributes the value.
    Several,
    No,
}

/// What one reference of a conjunct attributes to.
enum Attributed {
    Arm(usize),
    /// The value is carried by more than one operand: attributable to no
    /// single arm, and an ambiguity the moment the conjunct is otherwise a
    /// correlation.
    Shared,
    No,
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
    identities: &crate::relation::Planning,
) -> Result<Related> {
    let mut named: Vec<usize> = Vec::new();
    let mut shared = false;
    let all = collect_arms(conjunct, arms, identities, &mut named, &mut shared)?;
    // A conjunct that names an arm AND reads a value more than one operand
    // contributes is a correlation with no stated pairing: the shared value
    // would silently bind whichever arm minted the merged column. A
    // conjunct whose every reference is shared names no arm at all and
    // stays what it always was — one condition over the finished relation.
    if shared && !named.is_empty() {
        return Err(DelightQLError::validation_error_categorized(
            "resolution/setop/correlation/shared",
            "a set-operation correlation reads a value that is carried by \
             more than one operand, so which arm it correlates is unstated",
            "qualify the reference with the arm it means: \
             `x(*) as a ; y(*) as b, a.k = b.k`",
        ));
    }
    if !all {
        return Ok(Related::Whole);
    }
    Ok(pair(named))
}

/// Restate a claimed correlation over the arms' OWN headings: every
/// reference bound at the run's output moves to the port the contributing
/// arm published, following the contribution record. The lowering binds a
/// correlation against the two arm sites, so an output-bound occurrence has
/// no site there and a pad is never addressable.
pub(super) fn rebind_to_arms(
    predicate: &mut resolved::TruthExpression,
    arms: &RunArms,
    identities: &crate::relation::Planning,
) -> Result<()> {
    use resolved::TruthExpression as Boolean;
    match predicate {
        Boolean::Comparison(Comparison { left, right, .. }) => {
            rebind_domain(left, arms, identities)?;
            rebind_domain(right, arms, identities)?;
        }
        Boolean::Conjunction(parts) => {
            for part in parts.iter_mut() {
                rebind_to_arms(part, arms, identities)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn rebind_domain(
    expr: &mut resolved::DomainExpression,
    arms: &RunArms,
    identities: &crate::relation::Planning,
) -> Result<()> {
    use resolved::DomainExpression as Domain;
    match expr {
        Domain::Reference(Reference::Named(NamedReference(occurrence))) => {
            if arms.of_port(occurrence.column, identities).is_none() {
                if let Carried::One(carried) = arms.carried_output(occurrence.column, identities)? {
                    occurrence.column = carried;
                }
            }
        }
        Domain::Application(function) => match function {
            resolved::FunctionApplication::Standard(application) => {
                for argument in application.call_mut().arguments.scalar_members_mut() {
                    if let Some(domain) = argument.scalar_domain_mut() {
                        rebind_domain(domain, arms, identities)?;
                    }
                }
            }
            resolved::FunctionApplication::Infix(infix) => {
                rebind_domain(&mut infix.left, arms, identities)?;
                rebind_domain(&mut infix.right, arms, identities)?;
            }
            resolved::FunctionApplication::Crossed(crossing) => {
                for operand in crossing.scalar_operands_mut() {
                    rebind_domain(operand, arms, identities)?;
                }
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
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
    _identities: &crate::relation::Planning,
) -> Related {
    let (left, right) = whole.arms();
    let mut named: Vec<usize> = Vec::new();
    for scope in [left, right] {
        match arms.of_relation(*scope) {
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
    identities: &crate::relation::Planning,
    out: &mut Vec<usize>,
    shared: &mut bool,
) -> Result<bool> {
    use resolved::TruthExpression as Boolean;
    Ok(match predicate {
        Boolean::Comparison(Comparison { left, right, .. }) => {
            // Both sides are read even when one already failed to
            // attribute: a shared value beside a named arm is the refusal,
            // and short-circuiting would hide the arm that names it.
            let left = collect_domain_arms(left, arms, identities, out, shared)?;
            let right = collect_domain_arms(right, arms, identities, out, shared)?;
            left && right
        }
        Boolean::Conjunction(parts) => {
            let mut all = true;
            for part in parts.iter() {
                all = collect_arms(part, arms, identities, out, shared)? && all;
            }
            all
        }
        // Everything else is a filter over the finished relation, not a
        // correlation: disjunction, negation, membership, and the subquery
        // predicates each stand on the one heading the run publishes.
        Boolean::Disjunction(_)
        | Boolean::Not { .. }
        | Boolean::Membership { .. }
        | Boolean::Existence { .. }
        | Boolean::RelationalMembership { .. }
        | Boolean::Sigma { .. } => false,
    })
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
    identities: &crate::relation::Planning,
    out: &mut Vec<usize>,
    shared: &mut bool,
) -> Result<bool> {
    use resolved::DomainExpression as Domain;
    // A relation beneath the value brings its own scope; its references are
    // not this walk's to enumerate, so the side is not attributable.
    if expr.nests_relation() {
        return Ok(false);
    }
    Ok(match expr {
        Domain::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column, ..
        }))) => match arms.arm_and_port(*column, identities)? {
            Attributed::Arm(arm) => {
                if !out.contains(&arm) {
                    out.push(arm);
                }
                true
            }
            Attributed::Shared => {
                *shared = true;
                false
            }
            Attributed::No => false,
        },
        Domain::Application(function) => match function {
            resolved::FunctionApplication::Ground(_) => true,
            resolved::FunctionApplication::Standard(application) => {
                ({
                    let arguments = &application.call().arguments;
                    let mut all = true;
                    for argument in arguments.value_domains() {
                        all = collect_domain_arms(argument, arms, identities, out, shared)? && all;
                    }
                    all
                }) && application.guard.is_none()
            }
            resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => {
                let mut all = true;
                for element in tuple.elements.iter() {
                    all =
                        collect_domain_arms(element.value(), arms, identities, out, shared)? && all;
                }
                all
            }
            resolved::FunctionApplication::Infix(infix) => {
                let left = collect_domain_arms(&infix.left, arms, identities, out, shared)?;
                let right = collect_domain_arms(&infix.right, arms, identities, out, shared)?;
                left && right
            }
            // A crossed truth's reads are its truth's reads.
            resolved::FunctionApplication::Crossed(crossing) => {
                let mut all = true;
                for operand in crossing.truth().scalar_operands() {
                    all = collect_domain_arms(operand, arms, identities, out, shared)? && all;
                }
                all
            }
            _ => false,
        },
        _ => false,
    })
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
    identities: &crate::relation::Planning,
) -> Result<()> {
    let mut ambiguous = false;
    walk_bare_references(predicate, &mut |port| {
        let Some(name) = identities.published_sym(port.column()) else {
            return;
        };
        // Likewise "only one arm carries this name": an arm nobody
        // enumerated might carry it too, so the reference is not shown to
        // be unambiguous and is refused as ambiguous rather than let by.
        let mut carriers = 0usize;
        for relation in &arms.relations {
            match identities.authority().interface(relation) {
                Ok(interface) if interface.is_opaque() => {
                    ambiguous = true;
                    return;
                }
                Ok(interface) => {
                    if interface
                        .ports()
                        .iter()
                        .any(|candidate| identities.published_sym(candidate.column()) == Some(name))
                    {
                        carriers += 1;
                    }
                }
                Err(_) => {
                    ambiguous = true;
                    return;
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

fn walk_bare_references(
    predicate: &resolved::TruthExpression,
    note: &mut dyn FnMut(crate::relation::PortId),
) {
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

fn walk_bare_domain(
    expr: &resolved::DomainExpression,
    note: &mut dyn FnMut(crate::relation::PortId),
) {
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
                    walk_bare_domain(element.value(), note);
                }
            }
            resolved::FunctionApplication::Infix(infix) => {
                walk_bare_domain(&infix.left, note);
                walk_bare_domain(&infix.right, note);
            }
            resolved::FunctionApplication::Crossed(crossing) => {
                for operand in crossing.truth().scalar_operands() {
                    walk_bare_domain(operand, note);
                }
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

/// The anti-match predicate a bare minus carries.
///
/// Reads the ONE pairing the authority proved when it built the minus: which
/// left dimension answers which right one. It is not recomputed here — the
/// pairing decides what the result publishes and what this predicate
/// compares, and two roads deriving it separately are two authorities that
/// can disagree about a heading they both call exact.
pub(super) fn whole_tuple_correlation(
    result: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<resolved::TruthExpression> {
    let anti_match = crate::relation::anti_match(identities, &result)?.ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::RESOLUTION_SETOP_MINUS_HEADING,
            "a minus was refined without the exact pairing its construction proved",
            "this is a compiler fault: report the query",
        )
    })?;
    let comparisons = anti_match
        .pairs()
        .iter()
        .map(|pair| {
            resolved::TruthExpression::Comparison(Comparison {
                // Nulls match nulls here: the probe asks whether a matching
                // row is PRESENT, and a null-blind `=` would keep every
                // null-bearing row the right operand plainly contains.
                operator: CmpOp::NullSafeEqual,
                left: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine_qualified(pair.left())),
                ))),
                right: Box::new(resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence::engine_qualified(pair.right())),
                ))),
            })
        })
        .collect();
    resolved::TruthExpression::all(comparisons).ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            crate::uri_registry::subcat::RESOLUTION_SETOP_MINUS_HEADING,
            "minus has no columns to compare",
            "both operands must publish at least one column",
        )
    })
}

#[cfg(test)]
mod tests {
    //! What the correlation-owner law admits and what it refuses.

    use super::*;
    use crate::relation::PortId;

    fn lvar(column: PortId, explicit_qualifier: bool) -> resolved::DomainExpression {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            if explicit_qualifier {
                ColumnOccurrence::engine_qualified(column)
            } else {
                ColumnOccurrence::engine(column)
            },
        )))
    }

    fn column_of(
        registry: &crate::relation::Planning,
        scope_name: &str,
        column_name: &str,
    ) -> PortId {
        let answer = registry.intern(scope_name, false);
        let named = registry.intern(column_name, false);
        let relation = registry
            .authority()
            .derive(crate::relation::RelForm::Anonymous(
                crate::relation::form::AnonymousSpec {
                    shape: crate::relation::form::AnonymousShape::Tabular,
                    slots: &[crate::relation::form::AnonymousSlot::Binder {
                        position: 0,
                        named,
                        declared_type: None,
                        shape: crate::names::ValueShape::Unknown,
                    }],
                    answers_to: Some(answer),
                },
            ))
            .unwrap();
        crate::relation::published_ports(registry, &relation).unwrap()[0]
    }

    fn call_of(name: &str, argument: resolved::DomainExpression) -> resolved::DomainExpression {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        resolved::DomainExpression::Application(resolved::FunctionApplication::Standard(
            crate::pipeline::asts::core::StandardApplication::plain(
                crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall {
                        callee: registry.mint_function(registry.intern(name, false), Vec::new()),
                        arguments: crate::pipeline::asts::core::operators::CallArguments::Scalar(
                            vec![
                                crate::pipeline::asts::core::operators::ScalarArgument::plain(
                                    argument,
                                ),
                            ],
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
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
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
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
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
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let left = column_of(&registry, "x", "v");
        let right = column_of(&registry, "y", "v");
        assert!(
            refuse_unqualified_correlation(&equality(lvar(left, false), lvar(right, true))).is_ok()
        );
    }
}
