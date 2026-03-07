use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use std::collections::{HashMap, HashSet};

/// Validates grouped CTE definitions (after merging duplicates)
/// This operates on the logical structure that will actually be compiled
pub fn validate_grouped_cte_dependencies(
    cte_groups: &HashMap<String, Vec<ast_unresolved::CteBinding>>,
    cte_order: &[String], // Order of first appearance for each unique name
) -> Result<()> {
    check_forward_references_grouped(cte_groups, cte_order)?;
    check_for_cycles_grouped(cte_groups, cte_order)?;
    Ok(())
}

/// Check that grouped CTEs don't reference CTEs defined later
fn check_forward_references_grouped(
    cte_groups: &HashMap<String, Vec<ast_unresolved::CteBinding>>,
    cte_order: &[String],
) -> Result<()> {
    let mut defined = HashSet::new();

    for cte_name in cte_order {
        let group = &cte_groups[cte_name];

        // Extract all table references from all expressions in this group
        let mut all_refs = HashSet::new();
        for cte in group {
            let refs = extract_table_references(&cte.expression);
            all_refs.extend(refs);
        }

        // Check if any reference is to a CTE not yet defined
        for table_ref in all_refs {
            // Check if this reference is to another CTE that comes later
            if let Some(ref_position) = cte_order.iter().position(|name| name == &table_ref) {
                let current_position = cte_order.iter().position(|name| name == cte_name).unwrap();

                if ref_position > current_position {
                    return Err(DelightQLError::ParseError {
                        message: format!(
                            "CTE '{}' references '{}' which is defined later. CTEs must reference previously defined CTEs.",
                            cte_name, table_ref
                        ),
                        source: None,
                        subcategory: None,
                    });
                }
            }
            // If not found in cte_order, it's either a database table or doesn't exist
            // The resolver will handle those errors
        }

        defined.insert(cte_name);
    }
    Ok(())
}

/// Check for circular dependencies between grouped CTEs
fn check_for_cycles_grouped(
    cte_groups: &HashMap<String, Vec<ast_unresolved::CteBinding>>,
    cte_order: &[String],
) -> Result<()> {
    // Build dependency graph from grouped CTEs
    let mut graph: HashMap<String, Vec<String>> = HashMap::new();

    for cte_name in cte_order {
        let group = &cte_groups[cte_name];
        let mut all_deps = HashSet::new();

        // Collect all dependencies from all expressions in this group
        for cte in group {
            let refs = extract_table_references(&cte.expression);
            for ref_name in refs {
                // Only include references to other CTEs
                // ALLOW self-references (recursion), but track other CTE references
                if cte_order.contains(&ref_name) && ref_name != *cte_name {
                    all_deps.insert(ref_name);
                }
            }
        }

        graph.insert(cte_name.clone(), all_deps.into_iter().collect());
    }

    // Check for cycles using DFS
    let mut visited = HashSet::new();
    let mut recursion_stack = HashSet::new();

    for cte_name in cte_order {
        if !visited.contains(cte_name)
            && has_cycle_dfs(&graph, cte_name, &mut visited, &mut recursion_stack)?
        {
            return Err(DelightQLError::ParseError {
                message: format!("Circular CTE dependency detected involving '{}'", cte_name),
                source: None,
                subcategory: None,
            });
        }
    }

    Ok(())
}

/// DFS cycle detection
fn has_cycle_dfs(
    graph: &HashMap<String, Vec<String>>,
    node: &str,
    visited: &mut HashSet<String>,
    recursion_stack: &mut HashSet<String>,
) -> Result<bool> {
    visited.insert(node.to_string());
    recursion_stack.insert(node.to_string());

    if let Some(dependencies) = graph.get(node) {
        for dep in dependencies {
            if !visited.contains(dep) {
                if has_cycle_dfs(graph, dep, visited, recursion_stack)? {
                    return Ok(true);
                }
            } else if recursion_stack.contains(dep) {
                // Found a back edge - indicates cycle
                return Ok(true);
            }
        }
    }

    recursion_stack.remove(node);
    Ok(false)
}

/// Extract table references from a relational expression
/// This is a simple implementation that extracts Ground relation identifiers
fn extract_table_references(expr: &ast_unresolved::RelationalExpression) -> Vec<String> {
    let mut refs = Vec::new();
    extract_table_references_recursive(expr, &mut refs);
    refs
}

#[stacksafe::stacksafe]
fn extract_table_references_recursive(
    expr: &ast_unresolved::RelationalExpression,
    refs: &mut Vec<String>,
) {
    match expr {
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
            identifier,
            ..
        }) => {
            refs.push(identifier.name.to_string());
        }
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Anonymous {
            ..
        }) => {
            // Anonymous relations don't reference tables
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            extract_table_references_recursive(left, refs);
            extract_table_references_recursive(right, refs);
        }
        ast_unresolved::RelationalExpression::Filter { source, .. } => {
            extract_table_references_recursive(source, refs);
        }
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            extract_table_references_recursive(&pipe.source, refs);
        }
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            // SetOperation can appear from |;| syntax or will be created during CTE merging
            for operand in operands {
                extract_table_references_recursive(operand, refs);
            }
        }
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::TVF {
            ..
        }) => {
            // TVFs don't reference tables, skip
        }
        ast_unresolved::RelationalExpression::Relation(
            ast_unresolved::Relation::InnerRelation { pattern, .. },
        ) => {
            // InnerRelation references both the base table and any tables in the subquery
            match pattern {
                ast_unresolved::InnerRelationPattern::Indeterminate {
                    identifier,
                    subquery,
                    ..
                }
                | ast_unresolved::InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier,
                    subquery,
                    ..
                }
                | ast_unresolved::InnerRelationPattern::CorrelatedScalarJoin {
                    identifier,
                    subquery,
                    ..
                }
                | ast_unresolved::InnerRelationPattern::CorrelatedGroupJoin {
                    identifier,
                    subquery,
                    ..
                }
                | ast_unresolved::InnerRelationPattern::CorrelatedWindowJoin {
                    identifier,
                    subquery,
                    ..
                } => {
                    refs.push(identifier.name.to_string());
                    extract_table_references_recursive(subquery, refs);
                }
            }
        }

        ast_unresolved::RelationalExpression::Relation(
            ast_unresolved::Relation::ConsultedView { body, .. },
        ) => {
            // Recursively validate CTE references in the consulted view body
            match body.as_ref() {
                ast_unresolved::Query::Relational(expr) => {
                    extract_table_references_recursive(expr, refs);
                }
                ast_unresolved::Query::WithCtes { ctes, query: main } => {
                    for cte in ctes {
                        extract_table_references_recursive(&cte.expression, refs);
                    }
                    extract_table_references_recursive(main, refs);
                }
                other => panic!("catch-all hit in cte_validation.rs extract_table_references_recursive (Query variant): {:?}", other),
            }
        }

        ast_unresolved::RelationalExpression::Relation(
            ast_unresolved::Relation::PseudoPredicate { .. },
        ) => {
            panic!(
                "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                 Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
            )
        }
        ast_unresolved::RelationalExpression::ErJoinChain { relations } => {
            for rel in relations {
                if let ast_unresolved::Relation::Ground { identifier, .. } = rel {
                    refs.push(identifier.name.to_string());
                }
            }
        }
        ast_unresolved::RelationalExpression::ErTransitiveJoin { left, right } => {
            extract_table_references_recursive(left, refs);
            extract_table_references_recursive(right, refs);
        }
    }
}
