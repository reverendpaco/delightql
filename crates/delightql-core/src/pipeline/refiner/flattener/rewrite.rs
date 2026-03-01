// rewrite.rs - Qualifier and hygienic name rewriting

use crate::pipeline::asts::resolved;

/// Rewrite qualifiers in a correlation filter
/// Maps inner table references to the derived table's actual alias
///
/// Example: In `orders(, o.user_id = u.id)`, the filter has `o.user_id`
/// where `o` is a self-reference to `orders`. We need to rewrite it to
/// `orders.user_id` (or the explicit alias if provided).
pub(super) fn rewrite_inner_qualifiers(
    expr: resolved::BooleanExpression,
    inner_table_name: &str,
    derived_table_alias: &str,
) -> resolved::BooleanExpression {
    match expr {
        resolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => resolved::BooleanExpression::Comparison {
            operator,
            left: Box::new(rewrite_domain_qualifiers(
                *left,
                inner_table_name,
                derived_table_alias,
            )),
            right: Box::new(rewrite_domain_qualifiers(
                *right,
                inner_table_name,
                derived_table_alias,
            )),
        },
        resolved::BooleanExpression::And { left, right } => resolved::BooleanExpression::And {
            left: Box::new(rewrite_inner_qualifiers(
                *left,
                inner_table_name,
                derived_table_alias,
            )),
            right: Box::new(rewrite_inner_qualifiers(
                *right,
                inner_table_name,
                derived_table_alias,
            )),
        },
        resolved::BooleanExpression::Or { left, right } => resolved::BooleanExpression::Or {
            left: Box::new(rewrite_inner_qualifiers(
                *left,
                inner_table_name,
                derived_table_alias,
            )),
            right: Box::new(rewrite_inner_qualifiers(
                *right,
                inner_table_name,
                derived_table_alias,
            )),
        },
        resolved::BooleanExpression::Not { expr: inner } => resolved::BooleanExpression::Not {
            expr: Box::new(rewrite_inner_qualifiers(
                *inner,
                inner_table_name,
                derived_table_alias,
            )),
        },
        // Using: rewrite column names (qualifiers in USING are implicit)
        resolved::BooleanExpression::Using { columns } => {
            resolved::BooleanExpression::Using { columns }
        }
        // In: rewrite value and set elements
        resolved::BooleanExpression::In {
            value,
            set,
            negated,
        } => resolved::BooleanExpression::In {
            value: Box::new(rewrite_domain_qualifiers(
                *value,
                inner_table_name,
                derived_table_alias,
            )),
            set: set
                .into_iter()
                .map(|e| rewrite_domain_qualifiers(e, inner_table_name, derived_table_alias))
                .collect(),
            negated,
        },
        // BooleanLiteral, Sigma, GlobCorrelation, OrdinalGlobCorrelation:
        // no domain expressions with qualifiers to rewrite — pass through
        other @ resolved::BooleanExpression::BooleanLiteral { .. }
        | other @ resolved::BooleanExpression::Sigma { .. }
        | other @ resolved::BooleanExpression::GlobCorrelation { .. }
        | other @ resolved::BooleanExpression::OrdinalGlobCorrelation { .. } => other,
        // InnerExists, InRelational: pass through (subquery scope is separate)
        other @ resolved::BooleanExpression::InnerExists { .. }
        | other @ resolved::BooleanExpression::InRelational { .. } => other,
    }
}

pub(super) fn rewrite_domain_qualifiers(
    expr: resolved::DomainExpression,
    inner_table_name: &str,
    derived_table_alias: &str,
) -> resolved::DomainExpression {
    match expr {
        resolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => {
            // If this references the inner table, rewrite to derived table alias
            let new_qualifier = match qualifier.as_deref() {
                Some(q) if q == inner_table_name || could_be_inner_alias(q, inner_table_name) => {
                    Some(derived_table_alias.to_string())
                }
                other => other.map(|s| s.to_string()),
            };
            resolved::DomainExpression::Lvar {
                name,
                qualifier: new_qualifier.map(|s| s.into()),
                namespace_path,
                alias,
                provenance: resolved::PhaseBox::phantom(),
            }
        }
        // Function expressions: recurse into arguments
        resolved::DomainExpression::Function(func) => {
            // For simplicity, pass through — correlation filters are typically simple comparisons
            resolved::DomainExpression::Function(func)
        }
        // Parenthesized: recurse
        resolved::DomainExpression::Parenthesized { inner, alias } => {
            resolved::DomainExpression::Parenthesized {
                inner: Box::new(rewrite_domain_qualifiers(
                    *inner,
                    inner_table_name,
                    derived_table_alias,
                )),
                alias,
            }
        }
        // Predicate: recurse into boolean expression
        resolved::DomainExpression::Predicate { expr, alias } => {
            resolved::DomainExpression::Predicate {
                expr: Box::new(rewrite_inner_qualifiers(
                    *expr,
                    inner_table_name,
                    derived_table_alias,
                )),
                alias,
            }
        }
        // All other domain expressions: pass through unchanged
        other => other,
    }
}

/// Rewrite column names to use hygienic aliases in correlation filters
///
/// For example, if `user_id` was injected as `__dql_corr_0`, then rewrite:
///   `orders.user_id = u.id` → `orders.__dql_corr_0 = u.id`
pub(super) fn rewrite_with_hygienic_names(
    expr: resolved::BooleanExpression,
    derived_table_alias: &str,
    hygienic_injections: &[(String, String)],
) -> resolved::BooleanExpression {
    match expr {
        resolved::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => resolved::BooleanExpression::Comparison {
            operator,
            left: Box::new(rewrite_domain_with_hygienic_names(
                *left,
                derived_table_alias,
                hygienic_injections,
            )),
            right: Box::new(rewrite_domain_with_hygienic_names(
                *right,
                derived_table_alias,
                hygienic_injections,
            )),
        },
        resolved::BooleanExpression::And { left, right } => resolved::BooleanExpression::And {
            left: Box::new(rewrite_with_hygienic_names(
                *left,
                derived_table_alias,
                hygienic_injections,
            )),
            right: Box::new(rewrite_with_hygienic_names(
                *right,
                derived_table_alias,
                hygienic_injections,
            )),
        },
        resolved::BooleanExpression::Or { left, right } => resolved::BooleanExpression::Or {
            left: Box::new(rewrite_with_hygienic_names(
                *left,
                derived_table_alias,
                hygienic_injections,
            )),
            right: Box::new(rewrite_with_hygienic_names(
                *right,
                derived_table_alias,
                hygienic_injections,
            )),
        },
        other => panic!(
            "catch-all hit in flattener/rewrite.rs rewrite_with_hygienic_names: {:?}",
            other
        ),
    }
}

pub(super) fn rewrite_domain_with_hygienic_names(
    expr: resolved::DomainExpression,
    derived_table_alias: &str,
    hygienic_injections: &[(String, String)],
) -> resolved::DomainExpression {
    match expr {
        resolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => {
            // Check if this column references the derived table AND was hygienically injected
            if let Some(q) = &qualifier {
                if q == derived_table_alias {
                    // Look up the hygienic name for this column
                    for (original_name, hygienic_name) in hygienic_injections {
                        if &name == original_name {
                            // Rewrite to use hygienic name
                            return resolved::DomainExpression::Lvar {
                                name: hygienic_name.clone().into(),
                                qualifier: Some(derived_table_alias.into()),
                                namespace_path: namespace_path.clone(),
                                alias,
                                provenance: resolved::PhaseBox::phantom(),
                            };
                        }
                    }
                }
            }
            // No match - return as-is
            resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: resolved::PhaseBox::phantom(),
            }
        }
        other => panic!(
            "catch-all hit in flattener/rewrite.rs rewrite_domain_with_hygienic_names: {:?}",
            other
        ),
    }
}

/// Check if a qualifier could be a self-reference alias inside SNEAKY-PARENTHESES
/// Heuristic: single-letter names like 'o', 'p', etc. are likely aliases
fn could_be_inner_alias(qualifier: &str, table_name: &str) -> bool {
    // Check if qualifier is a common abbreviation of table_name
    // Examples: o -> orders, oi -> order_items, u -> users
    //
    // IMPORTANT: This must be conservative - only match when we're CERTAIN
    // it's a self-reference, not an outer table reference!

    if qualifier == table_name {
        return true;
    }

    // Single letter matching first letter - BUT ONLY if table name doesn't have underscore
    // This prevents "o" matching "order_items" (should be "oi")
    if qualifier.len() == 1 && table_name.starts_with(qualifier) && !table_name.contains('_') {
        return true;
    }

    // Two-letter abbreviation from underscore-separated words
    // Examples: oi (order_items), ui (user_info)
    if qualifier.len() == 2 && table_name.contains('_') {
        let parts: Vec<&str> = table_name.split('_').collect();
        if parts.len() == 2 {
            let abbrev = format!(
                "{}{}",
                parts[0].chars().next().unwrap_or('_'),
                parts[1].chars().next().unwrap_or('_')
            );
            if qualifier == abbrev {
                return true;
            }
        }
    }

    false
}
