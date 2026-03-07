// rewrite.rs - Qualifier and hygienic name rewriting
//
// Uses AstTransform<Resolved, Resolved> folds for structural descent.
// Only the Lvar interception logic differs between rewriting rules —
// the walk handles all other node types automatically.

use crate::error::Result;
use crate::pipeline::ast_transform::{self, AstTransform};
use crate::pipeline::asts::resolved;
use std::collections::HashMap;

// =============================================================================
// LvarRule — parameterized Lvar qualifier rewriting (Group A)
// =============================================================================
//
// All correlation-filter rewriters walk the same boolean/domain structure,
// intercept Lvars, and apply one of three rewriting rules. This enum
// captures the rule; LvarRewriteFold applies it via AstTransform.
//
// Group A rewriters STOP at InnerExists/InRelational boundaries because
// subquery scope is separate from the correlation filter's scope.

enum LvarRule<'a> {
    /// q == derived_alias && name in injections → hygienic_name
    HygienicNames {
        derived_table_alias: &'a str,
        injections: &'a [(String, String)],
    },
    /// q matches inner → derived_alias, else q in scope → scope[q]
    WithScope {
        inner_table_name: &'a str,
        derived_table_alias: &'a str,
        scope_aliases: &'a HashMap<String, String>,
    },
}

impl LvarRule<'_> {
    /// Apply the rewriting rule to an Lvar's (name, qualifier).
    /// Returns (new_name, new_qualifier). new_name is None when unchanged.
    fn apply(&self, name: &str, qualifier: Option<&str>) -> (Option<String>, Option<String>) {
        match self {
            LvarRule::HygienicNames {
                derived_table_alias,
                injections,
            } => {
                if let Some(q) = qualifier {
                    if q == *derived_table_alias {
                        for (original_name, hygienic_name) in *injections {
                            if name == original_name.as_str() {
                                return (
                                    Some(hygienic_name.clone()),
                                    Some(derived_table_alias.to_string()),
                                );
                            }
                        }
                    }
                }
                (None, qualifier.map(|s| s.to_string()))
            }
            LvarRule::WithScope {
                inner_table_name,
                derived_table_alias,
                scope_aliases,
            } => {
                let new_q = match qualifier {
                    Some(q)
                        if q == *inner_table_name
                            || could_be_inner_alias(q, inner_table_name) =>
                    {
                        Some(derived_table_alias.to_string())
                    }
                    Some(q) if scope_aliases.contains_key(q) => {
                        Some(scope_aliases[q].to_string())
                    }
                    None => {
                        // Unqualified lvar in a correlation filter — qualify with
                        // the derived table alias so the hoisted ON clause is unambiguous.
                        Some(derived_table_alias.to_string())
                    }
                    other => other.map(|s| s.to_string()),
                };
                (None, new_q)
            }
        }
    }
}

struct LvarRewriteFold<'a> {
    rule: LvarRule<'a>,
}

impl AstTransform<resolved::Resolved, resolved::Resolved> for LvarRewriteFold<'_> {
    fn transform_domain(
        &mut self,
        expr: resolved::DomainExpression,
    ) -> Result<resolved::DomainExpression> {
        match expr {
            resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: _,
            } => {
                let (new_name, new_q) = self.rule.apply(&name, qualifier.as_deref());
                Ok(resolved::DomainExpression::Lvar {
                    name: new_name.map(|n| n.into()).unwrap_or(name),
                    qualifier: new_q.map(|q| q.into()),
                    namespace_path,
                    alias,
                    provenance: resolved::PhaseBox::phantom(),
                })
            }
            other => ast_transform::walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: resolved::BooleanExpression,
    ) -> Result<resolved::BooleanExpression> {
        match expr {
            // InnerExists/InRelational: subquery scope is separate — don't recurse
            other @ resolved::BooleanExpression::InnerExists { .. }
            | other @ resolved::BooleanExpression::InRelational { .. } => Ok(other),
            other => ast_transform::walk_transform_boolean(self, other),
        }
    }
}

/// Rewrite column names to use hygienic aliases in correlation filters.
pub(super) fn rewrite_with_hygienic_names(
    expr: resolved::BooleanExpression,
    derived_table_alias: &str,
    hygienic_injections: &[(String, String)],
) -> resolved::BooleanExpression {
    let mut fold = LvarRewriteFold {
        rule: LvarRule::HygienicNames {
            derived_table_alias,
            injections: hygienic_injections,
        },
    };
    fold.transform_boolean(expr)
        .expect("hygienic name rewriting is infallible")
}

/// Scope-aware correlation filter rewriting.
///
/// For each qualifier in the filter:
/// 1. If it matches the inner table (self-reference) → rewrite to derived_table_alias
/// 2. If it's found in scope_aliases → rewrite to the canonical table name from the map
/// 3. Otherwise → leave unchanged (sibling table, already valid)
pub(super) fn rewrite_correlation_filter_with_scope(
    expr: resolved::BooleanExpression,
    inner_table_name: &str,
    derived_table_alias: &str,
    scope_aliases: &HashMap<String, String>,
) -> resolved::BooleanExpression {
    let mut fold = LvarRewriteFold {
        rule: LvarRule::WithScope {
            inner_table_name,
            derived_table_alias,
            scope_aliases,
        },
    };
    fold.transform_boolean(expr)
        .expect("scope-aware rewriting is infallible")
}

// =============================================================================
// SelfReferenceFold — cascading self-reference rewriting (Group B)
// =============================================================================
//
// Rewrites self-reference qualifiers throughout a subquery. Unlike Group A,
// this fold DOES recurse into InnerExists/InRelational with cascading:
// at each boundary, it rewrites BOTH the parent's self-refs AND the child's
// own self-refs (two sequential fold passes). This is inductive: each depth
// propagates all ancestor rewrites downward.

struct SelfReferenceFold<'a> {
    inner_table_name: &'a str,
}

impl AstTransform<resolved::Resolved, resolved::Resolved> for SelfReferenceFold<'_> {
    fn transform_domain(
        &mut self,
        expr: resolved::DomainExpression,
    ) -> Result<resolved::DomainExpression> {
        match expr {
            resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: _,
            } => {
                let new_qualifier = match qualifier.as_deref() {
                    Some(q)
                        if q == self.inner_table_name
                            || could_be_inner_alias(q, self.inner_table_name) =>
                    {
                        Some(self.inner_table_name.to_string().into())
                    }
                    _ => qualifier,
                };
                Ok(resolved::DomainExpression::Lvar {
                    name,
                    qualifier: new_qualifier,
                    namespace_path,
                    alias,
                    provenance: resolved::PhaseBox::phantom(),
                })
            }
            other => ast_transform::walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: resolved::BooleanExpression,
    ) -> Result<resolved::BooleanExpression> {
        match expr {
            resolved::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery,
                alias,
                using_columns,
            } => {
                // 1. Rewrite parent's self-refs (inner_table_name) in the subquery
                let rewritten = self.transform_relational_action(*subquery)?.into_inner();
                // 2. Rewrite the InnerExists's own self-refs (its identifier)
                let rewritten = {
                    let mut child =
                        SelfReferenceFold { inner_table_name: identifier.name.as_str() };
                    child.transform_relational_action(rewritten)?.into_inner()
                };
                Ok(resolved::BooleanExpression::InnerExists {
                    exists,
                    identifier,
                    subquery: Box::new(rewritten),
                    alias,
                    using_columns,
                })
            }
            resolved::BooleanExpression::InRelational {
                value,
                subquery,
                identifier,
                negated,
            } => {
                // Rewrite parent's self-refs in the value expression
                let value = self.transform_domain(*value)?;
                // 1. Rewrite parent's self-refs in the subquery
                let rewritten = self.transform_relational_action(*subquery)?.into_inner();
                // 2. Rewrite child's own self-refs
                let rewritten = {
                    let mut child =
                        SelfReferenceFold { inner_table_name: identifier.name.as_str() };
                    child.transform_relational_action(rewritten)?.into_inner()
                };
                Ok(resolved::BooleanExpression::InRelational {
                    value: Box::new(value),
                    subquery: Box::new(rewritten),
                    identifier,
                    negated,
                })
            }
            other => ast_transform::walk_transform_boolean(self, other),
        }
    }
}

/// Rewrite self-reference qualifiers throughout a subquery's conditions.
///
/// When a non-correlation filter like `o.status = "completed"` stays inside the subquery,
/// the self-reference alias `o` needs to be rewritten to the actual table name `orders`
/// so it resolves correctly inside the subquery scope.
///
/// Also cascades into InnerExists/InRelational subqueries so that nested
/// inner relations at deeper levels have their ancestor aliases resolved.
pub(super) fn rewrite_subquery_self_references(
    expr: resolved::RelationalExpression,
    inner_table_name: &str,
) -> resolved::RelationalExpression {
    let mut fold = SelfReferenceFold { inner_table_name };
    fold.transform_relational_action(expr)
        .map(|a| a.into_inner())
        .expect("self-reference rewriting is infallible")
}

// =============================================================================
// could_be_inner_alias — shared heuristic
// =============================================================================

/// Check if a qualifier could be a self-reference alias inside SNEAKY-PARENTHESES.
/// Heuristic: single-letter names like 'o', 'p', etc. are likely aliases.
pub(in crate::pipeline::refiner) fn could_be_inner_alias(
    qualifier: &str,
    table_name: &str,
) -> bool {
    if qualifier == table_name {
        return true;
    }

    // Single letter matching first letter - BUT ONLY if table name doesn't have underscore
    if qualifier.len() == 1 && table_name.starts_with(qualifier) && !table_name.contains('_') {
        return true;
    }

    // Two-letter abbreviation from underscore-separated words
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

// =============================================================================
// ScopeAliasFold — AstTransform<Resolved, Resolved>
// =============================================================================
//
// Rewrites Lvar qualifiers found in scope_aliases to their canonical table
// names. Uses the walk infrastructure for structural descent — only overrides
// transform_domain to intercept Lvars. All other node types (Function,
// Boolean, Operator, InnerRelation, ScalarSubquery, etc.) are handled by the
// walk automatically.

struct ScopeAliasFold<'a> {
    scope_aliases: &'a HashMap<String, String>,
}

impl AstTransform<resolved::Resolved, resolved::Resolved> for ScopeAliasFold<'_> {
    fn transform_domain(
        &mut self,
        expr: resolved::DomainExpression,
    ) -> Result<resolved::DomainExpression> {
        match expr {
            resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance,
            } => {
                let new_qualifier = match qualifier.as_deref() {
                    Some(q) if self.scope_aliases.contains_key(q) => {
                        Some(self.scope_aliases[q].clone().into())
                    }
                    _ => qualifier,
                };
                Ok(resolved::DomainExpression::Lvar {
                    name,
                    qualifier: new_qualifier,
                    namespace_path,
                    alias,
                    provenance,
                })
            }
            other => ast_transform::walk_transform_domain(self, other),
        }
    }
}

pub(super) fn apply_scope_aliases_to_expr(
    expr: resolved::RelationalExpression,
    scope_aliases: &HashMap<String, String>,
) -> resolved::RelationalExpression {
    if scope_aliases.is_empty() {
        return expr;
    }
    let mut fold = ScopeAliasFold { scope_aliases };
    fold.transform_relational_action(expr)
        .map(|a| a.into_inner())
        .expect("scope alias rewriting is infallible")
}

// =============================================================================
// collect_filter_qualifiers — qualifier collection (not a rewriter)
// =============================================================================

/// Collect all qualifier strings from a boolean expression (returned as Vec for caller flexibility)
pub(super) fn collect_filter_qualifiers(expr: &resolved::BooleanExpression) -> Vec<String> {
    let mut out = Vec::new();
    collect_filter_qualifiers_inner(expr, &mut out);
    out
}

fn collect_filter_qualifiers_inner(expr: &resolved::BooleanExpression, out: &mut Vec<String>) {
    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            collect_domain_filter_qualifiers(left, out);
            collect_domain_filter_qualifiers(right, out);
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            collect_filter_qualifiers_inner(left, out);
            collect_filter_qualifiers_inner(right, out);
        }
        resolved::BooleanExpression::Not { expr } => collect_filter_qualifiers_inner(expr, out),
        resolved::BooleanExpression::In { value, set, .. } => {
            collect_domain_filter_qualifiers(value, out);
            for elem in set {
                collect_domain_filter_qualifiers(elem, out);
            }
        }
        _ => {}
    }
}

fn collect_domain_filter_qualifiers(expr: &resolved::DomainExpression, out: &mut Vec<String>) {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => {
            if let Some(q) = qualifier {
                out.push(q.to_string());
            }
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            collect_domain_filter_qualifiers(inner, out);
        }
        resolved::DomainExpression::Predicate { expr, .. } => {
            collect_filter_qualifiers_inner(expr, out);
        }
        _ => {}
    }
}
