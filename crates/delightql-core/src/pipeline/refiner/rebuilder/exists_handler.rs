// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::Existence;
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::analyzer;
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Nest interdependent EXISTS predicates
pub(super) fn nest_interdependent_exists(
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    exists_deps: &analyzer::ExistsDependencies,
    identities: &crate::relation::Planning,
) -> Result<()> {
    log::debug!(
        "nest_interdependent_exists: roots={:?}, deps={:?}",
        exists_deps.roots,
        exists_deps.dependencies
    );

    // Only process top-level predicates (where EXISTS typically appear)
    if let Some(top_preds) = op_predicates.get_mut(&OperatorRef::TopLevel) {
        // Separate EXISTS from other predicates
        let mut exists_preds = Vec::new();
        let mut other_preds = Vec::new();

        for pred in top_preds.drain(..) {
            if matches!(
                pred.expr.truth(),
                resolved::TruthExpression::Existence(Existence { .. })
            ) {
                exists_preds.push(pred);
            } else {
                other_preds.push(pred);
            }
        }

        // If we have interdependent EXISTS, nest them
        if !exists_deps.dependencies.is_empty() {
            let mut exists_map: HashMap<crate::names::ScopeId, AnalyzedPredicate> = HashMap::new();
            for pred in exists_preds {
                if let resolved::TruthExpression::Existence(Existence {
                    relation: subquery, ..
                }) = pred.expr.truth()
                {
                    let scope = subquery.semantic_relation().scope();
                    exists_map.insert(scope, pred);
                }
            }

            // Process root EXISTS and nest dependent ones
            let mut nested_exists = Vec::new();
            for root_scope in &exists_deps.roots {
                log::debug!("Processing root EXISTS: {:?}", root_scope);
                if let Some(mut root_pred) = exists_map.remove(root_scope) {
                    // Nest any dependent EXISTS into this root
                    log::debug!("Nesting dependents into root: {:?}", root_scope);
                    nest_exists_recursive(
                        &mut root_pred,
                        *root_scope,
                        &exists_deps.dependencies,
                        &mut exists_map,
                        identities,
                    )?;
                    nested_exists.push(root_pred);
                }
            }

            // Add any remaining EXISTS that weren't nested (shouldn't happen if deps are correct)
            for (_, pred) in exists_map {
                nested_exists.push(pred);
            }

            // Put back the predicates
            other_preds.extend(nested_exists);
        } else {
            // No interdependencies, put EXISTS back as-is
            other_preds.extend(exists_preds);
        }

        *top_preds = other_preds;
    }

    Ok(())
}

/// Recursively nest EXISTS predicates
pub(super) fn nest_exists_recursive(
    parent_pred: &mut AnalyzedPredicate,
    parent_scope: crate::names::ScopeId,
    dependencies: &HashMap<crate::names::ScopeId, std::collections::HashSet<crate::names::ScopeId>>,
    exists_map: &mut HashMap<crate::names::ScopeId, AnalyzedPredicate>,
    identities: &crate::relation::Planning,
) -> Result<()> {
    // Find EXISTS that depend on this parent
    let mut dependents = Vec::new();
    for (dependent_scope, referenced_scopes) in dependencies {
        if referenced_scopes.contains(&parent_scope) {
            log::debug!("Found {:?} depends on {:?}", dependent_scope, parent_scope);
            if let Some(dep_pred) = exists_map.remove(dependent_scope) {
                dependents.push((*dependent_scope, dep_pred));
            }
        }
    }

    // If we have dependents, inject them into the parent's subquery. THE
    // INTERIOR IS A RELATION: the settled truth hands out its existence's
    // interior and nothing else, and a parent that is not an existence never
    // runs the rebuild — exactly as the match this replaces did nothing there.
    if !dependents.is_empty() {
        parent_pred.expr.rebuild_existence_interior(|subquery| {
            // For each dependent, recursively nest its dependents first
            let mut nested_dependents = Vec::new();
            for (dependent_scope, mut dep_pred) in dependents {
                nest_exists_recursive(
                    &mut dep_pred,
                    dependent_scope,
                    dependencies,
                    exists_map,
                    identities,
                )?;
                nested_dependents.push(dep_pred);
            }

            inject_exists_into_subquery(subquery, nested_dependents, identities)
        })?;
    }

    Ok(())
}

/// Inject EXISTS predicates into a subquery as AND conditions
pub(super) fn inject_exists_into_subquery(
    subquery: resolved::Chain,
    exists_predicates: Vec<AnalyzedPredicate>,
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    // Find the filter in the subquery or create one. The outermost step
    // travels WITH its operand, so nothing here holds a step beside a chain
    // it did not come off.
    let subquery = match subquery.peel() {
        Err(bare) => bare,
        Ok(peeled) => match peeled.last().form() {
            resolved::Continuation::Restrict { .. } => {
                let Ok((source, resolved::Transparent::Restrict { condition, origin })) =
                    peeled.transparent()
                else {
                    unreachable!("just matched a restriction")
                };
                let exists_exprs = exists_predicates
                    .into_iter()
                    .map(|p| p.expr.into_truth())
                    .collect();
                let combined_pred = combine_resolved_predicates_opt(Some(condition), exists_exprs);
                // A wider predicate over the same rows publishes the same
                // relation, so the result is RESTATED from the operand
                // rather than carried over from the step that came off.
                return Ok(source.transparently(resolved::Transparent::Restrict {
                    condition: combined_pred.unwrap_or_else(create_resolved_true_literal),
                    origin,
                }));
            }
            // A bound selects by position, so the EXISTS belongs UNDER it:
            // added above, it would filter rows the bound had already chosen.
            resolved::Continuation::Bound { .. } => {
                let Ok((source, resolved::Transparent::Bound { bound })) = peeled.transparent()
                else {
                    unreachable!("just matched a bound")
                };
                let source = inject_exists_into_subquery(source, exists_predicates, identities)?;
                return Ok(source.transparently(resolved::Transparent::Bound { bound }));
            }
            resolved::Continuation::Destructure { .. } => {
                return Err(DelightQLError::validation_error_categorized(
                    "refiner/exists/injection_condition",
                    format!(
                        "A dependent EXISTS cannot be placed through this parent condition: \
                         {:?}",
                        peeled.last().form()
                    ),
                    "dependent EXISTS placement",
                ))
            }
            _ => peeled.rejoin(),
        },
    };

    // No filter yet, create one with the EXISTS predicates
    let exists_exprs: Vec<_> = exists_predicates
        .into_iter()
        .map(|p| p.expr.into_truth())
        .collect();
    let combined_pred = if !exists_exprs.is_empty() {
        Some(combine_resolved_predicates_with_and(exists_exprs))
    } else {
        None
    };

    let _ = identities;
    Ok(match combined_pred {
        // A filter publishes what it filters.
        Some(pred) => subquery.transparently(resolved::Transparent::Restrict {
            condition: pred,
            origin: resolved::FilterOrigin::Generated,
        }),
        None => subquery,
    })
}

/// Create a resolved "1 = 1" true literal expression
pub(super) fn create_resolved_true_literal() -> resolved::TruthExpression {
    resolved::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(resolved::DomainExpression::Application(
            resolved::FunctionApplication::Ground(resolved::LiteralValue::Number("1".to_string())),
        )),
        right: Box::new(resolved::DomainExpression::Application(
            resolved::FunctionApplication::Ground(resolved::LiteralValue::Number("1".to_string())),
        )),
    })
}

/// Combine resolved predicates with AND
pub(super) fn combine_resolved_predicates_with_and(
    predicates: Vec<resolved::TruthExpression>,
) -> resolved::TruthExpression {
    resolved::TruthExpression::all(predicates).unwrap_or_else(create_resolved_true_literal)
}

/// Combine optional existing predicate with new predicates
pub(super) fn combine_resolved_predicates_opt(
    existing: Option<resolved::TruthExpression>,
    new_predicates: Vec<resolved::TruthExpression>,
) -> Option<resolved::TruthExpression> {
    let mut all_preds = Vec::new();
    if let Some(ex) = existing {
        all_preds.push(ex);
    }
    all_preds.extend(new_predicates);

    if all_preds.is_empty() {
        None
    } else {
        Some(combine_resolved_predicates_with_and(all_preds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comparison(value: &str) -> resolved::TruthExpression {
        resolved::TruthExpression::Comparison(Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(resolved::DomainExpression::Application(
                resolved::FunctionApplication::Ground(resolved::LiteralValue::Number(
                    value.to_string(),
                )),
            )),
            right: Box::new(resolved::DomainExpression::Application(
                resolved::FunctionApplication::Ground(resolved::LiteralValue::Number(
                    value.to_string(),
                )),
            )),
        })
    }

    fn schema(identities: &crate::relation::Planning) -> crate::relation::SemanticRelation {
        crate::relation::any_relation(identities)
    }

    fn relation(identities: &crate::relation::Planning) -> resolved::Chain {
        let table = resolved::AnonTable::from_values(
            None,
            vec![vec![resolved::DomainExpression::Application(
                resolved::FunctionApplication::Ground(resolved::LiteralValue::Number("1".into())),
            )]],
        )
        .unwrap();
        resolved::Chain::ground(
            identities
                .authority()
                .reading(crate::relation::builder::ReadHead::Anonymous {
                    relation: resolved::AnonRelation::plain(table),
                    published: schema(identities),
                })
                .expect("an anonymous head"),
        )
    }

    #[test]
    fn dependent_exists_enters_below_a_parent_tuple_limit() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let existing = comparison("1");
        let dependent = comparison("2");
        let source = relation(&identities).transparently(resolved::Transparent::Restrict {
            condition: existing.clone(),
            origin: resolved::FilterOrigin::Generated,
        });
        let limit = resolved::TupleOrdinalClause {
            operator: resolved::TupleOrdinalOperator::LessThan,
            value: 5,
            offset: None,
        };
        let limited = source.transparently(resolved::Transparent::Bound {
            bound: limit.clone(),
        });
        let predicates = vec![AnalyzedPredicate {
            class: PredicateClass::Fx,
            expr: crate::pipeline::refiner::settled::fixtures::settled_over_nothing(
                dependent.clone(),
                &identities,
            ),
            operator_ref: OperatorRef::TopLevel,
            origin: resolved::FilterOrigin::Generated,
        }];

        let mut injected = inject_exists_into_subquery(limited, predicates, &identities).unwrap();
        let Some(resolved::Continuation::Bound { bound, .. }) =
            injected.pop_continuation().map(|step| step.into_form())
        else {
            panic!("the parent tuple limit disappeared");
        };
        assert_eq!(bound, limit);

        let Some(resolved::Continuation::Restrict { condition, .. }) =
            injected.pop_continuation().map(|step| step.into_form())
        else {
            panic!("the dependent predicate was not injected below the tuple limit");
        };
        assert_eq!(
            condition,
            resolved::TruthExpression::all(vec![existing, dependent]).expect("two conjuncts")
        );
    }
}
