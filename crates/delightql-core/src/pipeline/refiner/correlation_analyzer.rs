// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Correlation Filter Detection
//
// Shared module for detecting correlation filters in subqueries.
// Used by both pattern_classifier (for initial classification) and
// flattener (for re-detection of misclassified patterns).
//
// A correlation filter is a predicate that references columns from BOTH:
// 1. Inner scope (the table being derived)
// 2. Outer scope (tables outside the SNEAKY-PARENTHESES)

use crate::pipeline::asts::core::ColumnOccurrence;
use std::collections::HashSet;

use crate::error::Result;
use crate::pipeline::asts::core::{
    Comparison, Existence, Membership, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;

/// Detect (but don't remove!) correlation filters in the subquery.
/// Returns them for metadata purposes only - they stay in the AST.
///
/// SCOPE-LOCAL (INVENTORY L4, the canonical scope-local proof): finds correlation
/// filters in the CURRENT scope only. It deliberately does NOT recurse into
/// nested INNER-RELATIONs — their correlation filters belong to their own level
/// and "would be hoisted to the wrong level" (see the `Relation` arm). The
/// `_in_scope` name marks that load-bearing stop boundary.
#[stacksafe::stacksafe]
pub fn detect_correlation_filters_in_scope(
    expr: &resolved::Chain,
    identities: &crate::names::Registry,
) -> Result<Vec<resolved::TruthExpression>> {
    let mut filters = Vec::new();

    for continuation in &expr.continuations {
        match continuation {
            resolved::Continuation::Restrict { condition, .. } => {
                // Clone for metadata, but the filter stays in the chain.
                if is_correlation_predicate(condition, identities) {
                    filters.push(condition.clone());
                }
            }
            resolved::Continuation::Member { rhs, .. } => {
                filters.extend(detect_correlation_filters_in_scope(rhs, identities)?);
            }
            resolved::Continuation::BagOp { arm, .. } => {
                filters.extend(detect_correlation_filters_in_scope(arm, identities)?);
            }
            resolved::Continuation::Access { .. }
            | resolved::Continuation::Bound { .. }
            | resolved::Continuation::Correlate { .. }
            | resolved::Continuation::Destructure { .. }
            | resolved::Continuation::Pipe { .. }
            | resolved::Continuation::Structural(_) => {}
            resolved::Continuation::ErJoin(_) => {
                unreachable!("ER chains consumed before correlation analysis")
            }
        }
    }
    // The head is NOT descended: a nested INNER-RELATION's correlation
    // filters belong to its own level, and hoisting them here would bind
    // them to the wrong one.

    Ok(filters)
}

/// Check if a predicate references both inner and outer scopes (correlation)
///
/// A correlation predicate references columns from TWO or more DISTINCT qualifiers.
/// `o.user_id = u.id` has two qualifiers (`o`, `u`) → correlation.
/// `o.status = "completed"` has one qualifier (`o`) → internal filter, NOT correlation.
/// `user_id = u.id` has one qualifier (`u`) + an unqualified lvar → correlation.
pub fn is_correlation_predicate(
    pred: &resolved::TruthExpression,
    identities: &crate::names::Registry,
) -> bool {
    let mut scopes = HashSet::new();
    let mut has_unqualified_lvar = false;
    collect_qualifiers_and_unqualified(pred, &mut scopes, &mut has_unqualified_lvar, identities);
    scopes.len() >= 2
}

/// The top-N partition-proof contract: convert ONE proved correlation
/// equality into the partition key it contributes, or refuse. Sound
/// pre-ranking requires each conjunct to pin a whole partition group
/// per outer row, which holds exactly when one side is a plain interior
/// column (the key) and the other side provably references only the
/// outer scope (constant per outer row). A wrapped interior key has no
/// directly representable partition column; an interior reference on
/// the non-key side narrows the candidate set within a group after
/// ranking. Both refuse, default-deny — the alternative is a plausible
/// ranking over the wrong population (an unpartitioned or
/// mispartitioned row_number()).
pub fn prove_partition_key(
    filter: &resolved::TruthExpression,
    inner_scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Result<crate::names::ColId> {
    use crate::error::DelightQLError;

    let resolved::TruthExpression::Comparison(Comparison { left, right, .. }) = filter else {
        unreachable!("prove_equality_conjunction flattens correlation filters to comparisons")
    };

    let topn_hint = "join normally and rank explicitly: ... |> (..., row_number:(<~ %(outer identity), #(ordering)) as rnk), rnk <= N";

    let (key, flank) = match (
        interior_key_column(left, inner_scope, identities),
        interior_key_column(right, inner_scope, identities),
    ) {
        (Some(key), None) => (key, right),
        (None, Some(key)) => (key, left),
        (Some(_), Some(_)) => {
            return Err(DelightQLError::validation_error_categorized(
                "interior/topn/unprovable_partition",
                "interior top-N requires the non-key side of each correlation equality to reference only the outer scope: both sides of this equality read the interior relation".to_string(),
                topn_hint,
            ))
        }
        (None, None) => {
            return Err(DelightQLError::validation_error_categorized(
                "interior/topn/unprovable_partition",
                "interior top-N requires each correlation equality to name a plain interior column on one side: here the interior key is wrapped in an expression, so no partition key is directly representable and the pre-ranked lowering would rank the wrong population".to_string(),
                topn_hint,
            ))
        }
    };

    if !provably_outer_only(flank, inner_scope, identities) {
        return Err(DelightQLError::validation_error_categorized(
            "interior/topn/unprovable_partition",
            "interior top-N requires the non-key side of each correlation equality to reference only the outer scope: an interior reference there narrows the candidate set within a partition group, and the pre-ranked lowering would rank rows the join predicate then discards".to_string(),
            topn_hint,
        ));
    }

    Ok(key)
}

/// A directly representable interior partition key: a bare Lvar whose
/// qualifier names the interior relation, or an unqualified Lvar (the
/// correlation model reads unqualified lvars as interior — the same
/// reading `is_correlation_predicate` classifies by). Anything wrapped
/// (functions, parentheses) is not directly representable: None.
fn interior_key_column(
    expr: &resolved::DomainExpression,
    inner_scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Option<crate::names::ColId> {
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) if identities.contains_scope(inner_scope, identities.scope_of(*column)) => {
            Some(*column)
        }
        _ => None,
    }
}

/// Default-deny purity check for the non-key side of a correlation
/// equality: true only for shapes PROVABLY constant per outer row —
/// outer-qualified lvars, literals, and plain function/parenthesis
/// composition over those. Unqualified lvars read the interior; any
/// shape this match does not affirmatively admit (case expressions,
/// windows, subqueries, ...) is unproven and answers false.
fn provably_outer_only(
    expr: &resolved::DomainExpression,
    inner_scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> bool {
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => !identities.contains_scope(inner_scope, identities.scope_of(*column)),
        resolved::DomainExpression::Application(resolved::FunctionApplication::Ground(_)) => true,
        resolved::DomainExpression::Application(func) => match func {
            resolved::FunctionApplication::Standard(application) => {
                let arguments = &application.call().arguments;
                arguments.relations().next().is_none()
                    && arguments
                        .value_domains()
                        .all(|expr| provably_outer_only(expr, inner_scope, identities))
                    && arguments
                        .scalar_members()
                        .iter()
                        .all(|member| member.scalar_domain().is_some())
                    && arguments
                        .ho_members()
                        .all(|argument| argument.scalar_domain().is_some())
            }
            resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => tuple
                .elements
                .iter()
                .all(|element| provably_outer_only(element, inner_scope, identities)),
            resolved::FunctionApplication::Infix(infix) => {
                provably_outer_only(&infix.left, inner_scope, identities)
                    && provably_outer_only(&infix.right, inner_scope, identities)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Extract correlation column names from correlation filters
pub fn extract_correlation_columns(
    filters: &[resolved::TruthExpression],
    inner_scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Vec<crate::names::ColId> {
    let mut columns = vec![];

    for filter in filters {
        if let resolved::TruthExpression::Comparison(Comparison { left, right, .. }) = filter {
            if let Some(col) = interior_key_column(left, inner_scope, identities) {
                columns.push(col);
            } else if let Some(col) = interior_key_column(right, inner_scope, identities) {
                columns.push(col);
            }
        }
    }

    columns
}

// ============================================================================
// Private Helper Functions
// ============================================================================

/// Collect all distinct qualifier names from a boolean expression
fn collect_qualifiers(
    expr: &resolved::TruthExpression,
    out: &mut HashSet<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) {
    let mut _unused = false;
    collect_qualifiers_and_unqualified(expr, out, &mut _unused, identities);
}

/// Like collect_qualifiers but also tracks whether any unqualified lvars exist
fn collect_qualifiers_and_unqualified(
    expr: &resolved::TruthExpression,
    out: &mut HashSet<crate::names::ScopeId>,
    has_unqualified: &mut bool,
    identities: &crate::names::Registry,
) {
    match expr {
        resolved::TruthExpression::Comparison(Comparison { left, right, .. }) => {
            collect_domain_qualifiers_and_unqualified(left, out, has_unqualified, identities);
            collect_domain_qualifiers_and_unqualified(right, out, has_unqualified, identities);
        }
        resolved::TruthExpression::Conjunction(parts)
        | resolved::TruthExpression::Disjunction(parts) => {
            for part in parts.iter() {
                collect_qualifiers_and_unqualified(part, out, has_unqualified, identities);
            }
        }
        resolved::TruthExpression::Not { expr } => {
            collect_qualifiers_and_unqualified(expr, out, has_unqualified, identities);
        }
        resolved::TruthExpression::Membership(Membership { probe, rows, .. }) => {
            for value in probe.values() {
                collect_domain_qualifiers_and_unqualified(value, out, has_unqualified, identities);
            }
            for row in rows {
                for value in &row.0 {
                    collect_domain_qualifiers_and_unqualified(
                        value,
                        out,
                        has_unqualified,
                        identities,
                    );
                }
            }
        }
        // Scope-local STOP, spelled per R-I3 (was a bare `_ => {}`): the
        // subquery-bearing variants (InnerExists.subquery, InRelational.subquery,
        // Sigma.condition) are self-contained nested scopes this correlation-
        // qualifier collector DELIBERATELY does not descend (the load-bearing
        // stop-at-subquery boundary, L4); the rest carry no qualifiers to collect
        // (Using / BooleanLiteral / Glob correlations). Kept no-op (byte-identical);
        // a newly-added boolean variant now forces a decision here.
        resolved::TruthExpression::Existence(Existence { .. })
        | resolved::TruthExpression::RelationalMembership(RelationalMembership { .. })
        | resolved::TruthExpression::Sigma(SigmaApplication { .. }) => {}
    }
}

/// Collect qualifier names from a domain expression
fn collect_domain_qualifiers(
    expr: &resolved::DomainExpression,
    out: &mut HashSet<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) {
    let mut _unused = false;
    collect_domain_qualifiers_and_unqualified(expr, out, &mut _unused, identities);
}

/// Like collect_domain_qualifiers but also tracks unqualified lvars
fn collect_domain_qualifiers_and_unqualified(
    expr: &resolved::DomainExpression,
    out: &mut HashSet<crate::names::ScopeId>,
    has_unqualified: &mut bool,
    identities: &crate::names::Registry,
) {
    match expr {
        resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence {
                column,
                explicit_qualifier,
                ..
            },
        ))) => {
            out.insert(identities.scope_of(*column));
            if !explicit_qualifier {
                *has_unqualified = true;
            }
        }
        resolved::DomainExpression::Application(func) => match func {
            resolved::FunctionApplication::Ground(_) | resolved::FunctionApplication::Open(_) => {}
            resolved::FunctionApplication::Standard(application) => {
                // A crossed argument carries its own truth, which this
                // correlation walk does not descend.
                for argument in application.call().arguments.value_domains() {
                    collect_domain_qualifiers_and_unqualified(
                        argument,
                        out,
                        has_unqualified,
                        identities,
                    );
                }
            }
            // The pick's arguments are the correlated values; the arms are
            // the CALLEE's constants and correlate with nothing here.
            resolved::FunctionApplication::FieldSelect(select) => {
                for argument in select.application.call().arguments.value_domains() {
                    {
                        collect_domain_qualifiers_and_unqualified(
                            argument,
                            out,
                            has_unqualified,
                            identities,
                        );
                    }
                }
            }
            resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => {
                for arg in tuple.elements.iter() {
                    collect_domain_qualifiers_and_unqualified(
                        arg,
                        out,
                        has_unqualified,
                        identities,
                    );
                }
            }
            resolved::FunctionApplication::Infix(infix) => {
                collect_domain_qualifiers_and_unqualified(
                    &infix.left,
                    out,
                    has_unqualified,
                    identities,
                );
                collect_domain_qualifiers_and_unqualified(
                    &infix.right,
                    out,
                    has_unqualified,
                    identities,
                );
            }
            resolved::FunctionApplication::ClauseSelection(selection) => {
                for arm in &selection.arms {
                    if let Some(guard) = &arm.guard {
                        collect_qualifiers(guard, out, identities);
                    }
                    collect_result_qualifiers(&arm.result, out, identities);
                }
            }
            resolved::FunctionApplication::Case(case) => {
                collect_case_qualifiers(case, out, identities)
            }
            // A drill reads its source at THIS level: `u.meta:{.k}` carries
            // the correlation qualifier `u`, and skipping it fails OPEN — a
            // correlated interior classified as an ordinary filter. The
            // path side is a member spelling, never a reference.
            resolved::FunctionApplication::JsonAccess(access) => {
                collect_domain_qualifiers_and_unqualified(
                    &access.source,
                    out,
                    has_unqualified,
                    identities,
                );
            }
            // Spelled per R-I3 (was a bare `_ => {}`): these function variants DO
            // carry recursive domain expressions (HigherOrder/Window args,
            // Lambda.body, record members, metadata levels),
            // but this correlation-qualifier collector deliberately does not descend
            // them (kept no-op, byte-identical). A new function variant now forces a
            // decision here.
            // A scalarized relation is a self-contained nested scope: this
            // collector stops at the boundary, exactly as it did when the
            // compression was the chain's last step.
            resolved::FunctionApplication::Scalarized(_)
            | resolved::FunctionApplication::Template(_)
            | resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Record(_),
            )
            | resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::EmptyRecord(_),
            ) => {}
        },
        // Spelled per R-I3 (was a bare `_ => {}`). The recursive-field-bearing
        // variants here (Tuple.elements, ScalarSubquery.subquery) are
        // DELIBERATELY not descended by this correlation-qualifier collector —
        // ScalarSubquery is a self-contained nested scope (scope-local stop),
        // the others carry no correlation qualifiers at this level. Kept
        // no-op (byte-identical); the true leaf variants carry nothing. A
        // newly-added domain variant now forces a decision here.
        // Uninhabited after resolution, and still written: a match on a
        // REFERENCE cannot omit an uninhabited variant's arm.
        resolved::DomainExpression::Reference(Reference::Ordinal(_)) => {}
    }
}

/// Collect qualifier names from a CASE arm
/// What an arm computes: a value, or the licensed crossing.
fn collect_result_qualifiers(
    result: &resolved::OutValue,
    out: &mut HashSet<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) {
    match result {
        resolved::OutValue::Domain(domain) => collect_domain_qualifiers(domain, out, identities),
        resolved::OutValue::Truth(crossing) => {
            collect_qualifiers(crossing.truth(), out, identities)
        }
    }
}

fn collect_case_qualifiers(
    case: &resolved::CaseExpression,
    out: &mut HashSet<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) {
    let default = match case {
        resolved::CaseExpression::Anchored {
            anchor,
            arms,
            default,
        } => {
            collect_domain_qualifiers(anchor, out, identities);
            for arm in arms.iter() {
                collect_domain_qualifiers(&arm.result, out, identities);
            }
            default
        }
        resolved::CaseExpression::Searched { arms, default } => {
            for arm in arms.iter() {
                collect_qualifiers(&arm.condition, out, identities);
                collect_domain_qualifiers(&arm.result, out, identities);
            }
            default
        }
    };
    if let Some(result) = default {
        collect_domain_qualifiers(result, out, identities);
    }
}
