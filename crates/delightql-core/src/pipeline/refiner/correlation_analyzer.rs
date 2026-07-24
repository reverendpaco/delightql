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

use std::collections::HashSet;

use crate::error::Result;
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
    expr: &resolved::RelationalExpression,
) -> Result<Vec<resolved::BooleanExpression>> {
    let mut filters = Vec::new();

    match expr {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin: _,
            cpr_schema: _,
        } => {
            // Check if this is a correlation predicate
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                if is_correlation_predicate(pred) {
                    // Clone for metadata, but filter stays in AST
                    filters.push(pred.clone());
                }
            }
            // Recursively check source
            filters.extend(detect_correlation_filters_in_scope(source)?);
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Check source
            filters.extend(detect_correlation_filters_in_scope(&pipe_expr.source)?);
        }
        resolved::RelationalExpression::Join { left, right, .. } => {
            // Check both sides of the join
            filters.extend(detect_correlation_filters_in_scope(left)?);
            filters.extend(detect_correlation_filters_in_scope(right)?);
        }
        resolved::RelationalExpression::Relation(_rel) => {
            // DO NOT recursively check nested INNER-RELATIONs!
            // Nested INNER-RELATIONs will be processed at their own level during flattening.
            // If we include their correlation filters here, they would be hoisted to the wrong level.
            //
            // For example:
            //   orders(, o.user_id = u.id, order_items(, oi.order_id = order_id))
            //
            // When detecting correlation filters for `orders`, we should only find `o.user_id = u.id`,
            // NOT `oi.order_id = order_id` (which belongs to the nested `order_items`).
            //
            // Note: We still need to check other relation types (Ground, Anonymous, etc.)
            // in case they contain Filter nodes wrapping them.
        }
        resolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                filters.extend(detect_correlation_filters_in_scope(operand)?);
            }
        }
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before correlation analysis")
        }
        resolved::RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }

    Ok(filters)
}

/// Check if a predicate references both inner and outer scopes (correlation)
///
/// A correlation predicate references columns from TWO or more DISTINCT qualifiers.
/// `o.user_id = u.id` has two qualifiers (`o`, `u`) → correlation.
/// `o.status = "completed"` has one qualifier (`o`) → internal filter, NOT correlation.
/// `user_id = u.id` has one qualifier (`u`) + an unqualified lvar → correlation.
pub fn is_correlation_predicate(pred: &resolved::BooleanExpression) -> bool {
    let mut qualifiers = HashSet::new();
    let mut has_unqualified_lvar = false;
    collect_qualifiers_and_unqualified(pred, &mut qualifiers, &mut has_unqualified_lvar);
    // Two+ distinct qualifiers (e.g., o.x = u.y), OR
    // one qualifier + unqualified lvar (e.g., x = u.y — inner x, outer u.y)
    qualifiers.len() >= 2 || (qualifiers.len() == 1 && has_unqualified_lvar)
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
    filter: &resolved::BooleanExpression,
    table_identifier: &resolved::QualifiedName,
) -> Result<String> {
    use crate::error::DelightQLError;

    let resolved::BooleanExpression::Comparison { left, right, .. } = filter else {
        unreachable!("prove_equality_conjunction flattens correlation filters to comparisons")
    };

    let topn_hint = "join normally and rank explicitly: ... |> (..., row_number:(<~ %(outer identity), #(ordering)) as rnk), rnk <= N";

    let (key, flank) = match (
        interior_key_column(left, table_identifier),
        interior_key_column(right, table_identifier),
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

    if !provably_outer_only(flank, table_identifier) {
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
    table_identifier: &resolved::QualifiedName,
) -> Option<String> {
    if let resolved::DomainExpression::Lvar {
        name, qualifier, ..
    } = expr
    {
        match qualifier {
            Some(q) if super::flattener::could_be_inner_alias(q, &table_identifier.name) => {
                Some(name.to_string())
            }
            None => Some(name.to_string()),
            Some(_) => None,
        }
    } else {
        None
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
    table_identifier: &resolved::QualifiedName,
) -> bool {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => match qualifier {
            Some(q) => !super::flattener::could_be_inner_alias(q, &table_identifier.name),
            None => false,
        },
        resolved::DomainExpression::Literal { .. } => true,
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            provably_outer_only(inner, table_identifier)
        }
        resolved::DomainExpression::Function(func) => match func {
            resolved::FunctionExpression::Regular { arguments, .. }
            | resolved::FunctionExpression::Curried { arguments, .. }
            | resolved::FunctionExpression::Bracket { arguments, .. } => arguments
                .iter()
                .all(|arg| provably_outer_only(arg, table_identifier)),
            resolved::FunctionExpression::Infix { left, right, .. } => {
                provably_outer_only(left, table_identifier)
                    && provably_outer_only(right, table_identifier)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Extract correlation column names from correlation filters
pub fn extract_correlation_column_names(
    filters: &[resolved::BooleanExpression],
    table_identifier: &resolved::QualifiedName,
) -> Vec<String> {
    let mut columns = vec![];

    for filter in filters {
        if let resolved::BooleanExpression::Comparison { left, right, .. } = filter {
            if let Some(col) = interior_key_column(left, table_identifier) {
                columns.push(col);
            } else if let Some(col) = interior_key_column(right, table_identifier) {
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
fn collect_qualifiers(expr: &resolved::BooleanExpression, out: &mut HashSet<String>) {
    let mut _unused = false;
    collect_qualifiers_and_unqualified(expr, out, &mut _unused);
}

/// Like collect_qualifiers but also tracks whether any unqualified lvars exist
fn collect_qualifiers_and_unqualified(
    expr: &resolved::BooleanExpression,
    out: &mut HashSet<String>,
    has_unqualified: &mut bool,
) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            collect_domain_qualifiers_and_unqualified(left, out, has_unqualified);
            collect_domain_qualifiers_and_unqualified(right, out, has_unqualified);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            collect_qualifiers_and_unqualified(left, out, has_unqualified);
            collect_qualifiers_and_unqualified(right, out, has_unqualified);
        }
        resolved::BooleanExpression::Not { expr } => {
            collect_qualifiers_and_unqualified(expr, out, has_unqualified);
        }
        resolved::BooleanExpression::In { value, set, .. } => {
            collect_domain_qualifiers_and_unqualified(value, out, has_unqualified);
            for elem in set {
                collect_domain_qualifiers_and_unqualified(elem, out, has_unqualified);
            }
        }
        // Scope-local STOP, spelled per R-I3 (was a bare `_ => {}`): the
        // subquery-bearing variants (InnerExists.subquery, InRelational.subquery,
        // Sigma.condition) are self-contained nested scopes this correlation-
        // qualifier collector DELIBERATELY does not descend (the load-bearing
        // stop-at-subquery boundary, L4); the rest carry no qualifiers to collect
        // (Using / BooleanLiteral / Glob correlations). Kept no-op (byte-identical);
        // a newly-added boolean variant now forces a decision here.
        resolved::BooleanExpression::InnerExists { .. }
        | resolved::BooleanExpression::InRelational { .. }
        | resolved::BooleanExpression::Sigma { .. }
        | resolved::BooleanExpression::Using { .. }
        | resolved::BooleanExpression::BooleanLiteral { .. }
        | resolved::BooleanExpression::GlobCorrelation { .. }
        | resolved::BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}

/// Collect qualifier names from a domain expression
fn collect_domain_qualifiers(expr: &resolved::DomainExpression, out: &mut HashSet<String>) {
    let mut _unused = false;
    collect_domain_qualifiers_and_unqualified(expr, out, &mut _unused);
}

/// Like collect_domain_qualifiers but also tracks unqualified lvars
fn collect_domain_qualifiers_and_unqualified(
    expr: &resolved::DomainExpression,
    out: &mut HashSet<String>,
    has_unqualified: &mut bool,
) {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => {
            if let Some(q) = qualifier {
                out.insert(q.to_string());
            } else {
                *has_unqualified = true;
            }
        }
        resolved::DomainExpression::Function(func) => match func {
            resolved::FunctionExpression::Regular { arguments, .. }
            | resolved::FunctionExpression::Curried { arguments, .. }
            | resolved::FunctionExpression::Bracket { arguments, .. } => {
                for arg in arguments {
                    collect_domain_qualifiers_and_unqualified(arg, out, has_unqualified);
                }
            }
            resolved::FunctionExpression::Infix { left, right, .. } => {
                collect_domain_qualifiers_and_unqualified(left, out, has_unqualified);
                collect_domain_qualifiers_and_unqualified(right, out, has_unqualified);
            }
            resolved::FunctionExpression::CaseExpression { arms, .. } => {
                for arm in arms {
                    collect_case_arm_qualifiers(arm, out);
                }
            }
            // Spelled per R-I3 (was a bare `_ => {}`): these function variants DO
            // carry recursive domain expressions (HigherOrder/Window args,
            // Lambda.body, JsonPath.source, Curly/Array/MetadataTreeGroup members),
            // but this correlation-qualifier collector deliberately does not descend
            // them (kept no-op, byte-identical). A new function variant now forces a
            // decision here.
            resolved::FunctionExpression::HigherOrder { .. }
            | resolved::FunctionExpression::Lambda { .. }
            | resolved::FunctionExpression::StringTemplate { .. }
            | resolved::FunctionExpression::Window { .. }
            | resolved::FunctionExpression::Curly { .. }
            | resolved::FunctionExpression::Array { .. }
            | resolved::FunctionExpression::MetadataTreeGroup { .. }
            | resolved::FunctionExpression::JsonPath { .. } => {}
        },
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            collect_domain_qualifiers_and_unqualified(inner, out, has_unqualified);
        }
        resolved::DomainExpression::Predicate { expr, .. } => {
            collect_qualifiers_and_unqualified(expr, out, has_unqualified);
        }
        // Spelled per R-I3 (was a bare `_ => {}`). The recursive-field-bearing
        // variants here (PipedExpression.value/transforms, Tuple.elements,
        // PivotOf.value_column/pivot_key, ScalarSubquery.subquery) are DELIBERATELY
        // not descended by this correlation-qualifier collector — ScalarSubquery is
        // a self-contained nested scope (scope-local stop), the others carry no
        // correlation qualifiers at this level. Kept no-op (byte-identical); the
        // true leaf variants (Literal/Projection/Substitution/…) carry nothing. A
        // newly-added domain variant now forces a decision here.
        resolved::DomainExpression::PipedExpression { .. }
        | resolved::DomainExpression::Tuple { .. }
        | resolved::DomainExpression::PivotOf { .. }
        | resolved::DomainExpression::ScalarSubquery { .. }
        | resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(_) => {}
    }
}

/// Collect qualifier names from a CASE arm
fn collect_case_arm_qualifiers(arm: &resolved::CaseArm, out: &mut HashSet<String>) {
    match arm {
        resolved::CaseArm::Simple {
            test_expr, result, ..
        } => {
            collect_domain_qualifiers(test_expr, out);
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::CurriedSimple { result, .. } => {
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::Searched { condition, result } => {
            collect_qualifiers(condition, out);
            collect_domain_qualifiers(result, out);
        }
        resolved::CaseArm::Default { result } => {
            collect_domain_qualifiers(result, out);
        }
    }
}

