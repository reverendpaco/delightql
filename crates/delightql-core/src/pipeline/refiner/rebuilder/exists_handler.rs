use crate::error::Result;
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::analyzer;
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Nest interdependent EXISTS predicates
pub(super) fn nest_interdependent_exists(
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    exists_deps: &analyzer::ExistsDependencies,
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
            if matches!(pred.expr, resolved::BooleanExpression::InnerExists { .. }) {
                exists_preds.push(pred);
            } else {
                other_preds.push(pred);
            }
        }

        // If we have interdependent EXISTS, nest them
        if !exists_deps.dependencies.is_empty() {
            // Build a map of EXISTS predicates by table name
            let mut exists_map: HashMap<String, AnalyzedPredicate> = HashMap::new();
            for pred in exists_preds {
                if let resolved::BooleanExpression::InnerExists { ref identifier, .. } = pred.expr {
                    exists_map.insert(identifier.name.to_string(), pred);
                }
            }

            // Process root EXISTS and nest dependent ones
            let mut nested_exists = Vec::new();
            for root_name in &exists_deps.roots {
                log::debug!("Processing root EXISTS: {}", root_name);
                if let Some(mut root_pred) = exists_map.remove(root_name) {
                    // Nest any dependent EXISTS into this root
                    log::debug!("Nesting dependents into root: {}", root_name);
                    nest_exists_recursive(
                        &mut root_pred,
                        root_name,
                        &exists_deps.dependencies,
                        &mut exists_map,
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
    parent_name: &str,
    dependencies: &HashMap<String, std::collections::HashSet<String>>,
    exists_map: &mut HashMap<String, AnalyzedPredicate>,
) -> Result<()> {
    // Find EXISTS that depend on this parent
    let mut dependents = Vec::new();
    for (dep_name, dep_refs) in dependencies {
        if dep_refs.contains(parent_name) {
            log::debug!("Found {} depends on {}", dep_name, parent_name);
            if let Some(dep_pred) = exists_map.remove(dep_name) {
                dependents.push((dep_name.clone(), dep_pred));
            }
        }
    }

    // If we have dependents, inject them into the parent's subquery
    if !dependents.is_empty() {
        if let resolved::BooleanExpression::InnerExists {
            exists: _,
            identifier: _,
            subquery,
            alias: _,
            using_columns: _,
        } = &mut parent_pred.expr
        {
            // For each dependent, recursively nest its dependents first
            let mut nested_dependents = Vec::new();
            for (dep_name, mut dep_pred) in dependents {
                nest_exists_recursive(&mut dep_pred, &dep_name, dependencies, exists_map)?;
                nested_dependents.push(dep_pred);
            }

            // Now inject the dependents into the parent's subquery
            *subquery = Box::new(inject_exists_into_subquery(
                *subquery.clone(),
                nested_dependents,
            )?);
        }
    }

    Ok(())
}

/// Inject EXISTS predicates into a subquery as AND conditions
pub(super) fn inject_exists_into_subquery(
    subquery: resolved::RelationalExpression,
    exists_predicates: Vec<AnalyzedPredicate>,
) -> Result<resolved::RelationalExpression> {
    // Find the filter in the subquery or create one
    match subquery {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            // Extract the existing predicate
            let existing_pred = match condition {
                resolved::SigmaCondition::Predicate(pred) => Some(pred),
                other => panic!("catch-all hit in rebuilder/exists_handler.rs inject_exists_into_subquery (SigmaCondition): {:?}", other),
            };

            // Build the combined condition
            let exists_exprs: Vec<_> = exists_predicates.into_iter().map(|p| p.expr).collect();
            let combined_pred = combine_resolved_predicates_opt(existing_pred, exists_exprs);

            // Return the modified filter
            Ok(resolved::RelationalExpression::Filter {
                source,
                condition: resolved::SigmaCondition::Predicate(
                    combined_pred.unwrap_or_else(create_resolved_true_literal),
                ),
                origin,
                cpr_schema,
            })
        }
        _ => {
            // No filter yet, create one with the EXISTS predicates
            let exists_exprs: Vec<_> = exists_predicates.into_iter().map(|p| p.expr).collect();
            let combined_pred = if !exists_exprs.is_empty() {
                Some(combine_resolved_predicates_with_and(exists_exprs))
            } else {
                None
            };

            if let Some(pred) = combined_pred {
                Ok(resolved::RelationalExpression::Filter {
                    source: Box::new(subquery),
                    condition: resolved::SigmaCondition::Predicate(pred),
                    origin: resolved::FilterOrigin::Generated,
                    cpr_schema: resolved::PhaseBox::new(resolved::CprSchema::Unknown),
                })
            } else {
                Ok(subquery)
            }
        }
    }
}

/// Create a resolved "1 = 1" true literal expression
pub(super) fn create_resolved_true_literal() -> resolved::BooleanExpression {
    resolved::BooleanExpression::Comparison {
        operator: "traditional_eq".to_string(),
        left: Box::new(resolved::DomainExpression::Literal {
            value: resolved::LiteralValue::Number("1".to_string()),
            alias: None,
        }),
        right: Box::new(resolved::DomainExpression::Literal {
            value: resolved::LiteralValue::Number("1".to_string()),
            alias: None,
        }),
    }
}

/// Combine resolved predicates with AND
pub(super) fn combine_resolved_predicates_with_and(
    predicates: Vec<resolved::BooleanExpression>,
) -> resolved::BooleanExpression {
    if predicates.is_empty() {
        create_resolved_true_literal()
    } else if predicates.len() == 1 {
        predicates.into_iter().next().unwrap()
    } else {
        predicates
            .into_iter()
            .reduce(|acc, pred| resolved::BooleanExpression::And {
                left: Box::new(acc),
                right: Box::new(pred),
            })
            .unwrap()
    }
}

/// Combine optional existing predicate with new predicates
pub(super) fn combine_resolved_predicates_opt(
    existing: Option<resolved::BooleanExpression>,
    new_predicates: Vec<resolved::BooleanExpression>,
) -> Option<resolved::BooleanExpression> {
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
