// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::{Relation, Unresolved};
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

/// Extract table references from a relational expression — every Ground table
/// name and InnerRelation base name reachable ANYWHERE in the body, including
/// inside predicate subqueries (`Filter.condition`), pipe-operator arguments,
/// consulted-view bodies, and nested CTE bodies.
///
/// Rides the shared whole-tree closure `AstVisit<Unresolved>` (INDUCTIVE-
/// TRAVERSAL-PLAN R-I1/R-I3): the former hand-rolled walker matched
/// `Filter { source, .. }` and dropped the recursive `condition` field (a
/// third instance of the P1/P2 pattern, INDUCTIVE-INVENTORY §2a W6). The
/// default `walk_visit_*` descent names every recursive edge once, so a table
/// reference hidden in a predicate subquery can no longer be silently ignored.
///
/// Behavior-preserving in expectation: this graph is validation-only (forward-
/// reference + cycle checks; it never reorders emission). Collecting a
/// reference to an EARLIER CTE is harmless (backward references trigger
/// neither check); a reference to a LATER CTE is illegal under left-to-right
/// scoping and is rejected regardless — closing the hole can only sharpen the
/// diagnostic on an already-rejected query, never flip accept/reject.
fn extract_table_references(expr: &ast_unresolved::RelationalExpression) -> Vec<String> {
    let mut collector = TableRefCollector { refs: Vec::new() };
    walk_visit_relational(&mut collector, expr)
        .expect("CTE table-reference collection is infallible (hooks never return Err)");
    collector.refs
}

/// Collects Ground table names and InnerRelation base names into `refs`. The
/// `AstVisit` default walk supplies the complete structural descent; this only
/// names the leaf positions that contribute a reference.
struct TableRefCollector {
    refs: Vec<String>,
}

impl AstVisit<Unresolved> for TableRefCollector {
    fn enter_relation(&mut self, rel: &Relation<Unresolved>) -> Result<Descent> {
        match rel {
            Relation::Ground { identifier, .. } => {
                self.refs.push(identifier.name.to_string());
            }
            // A pseudo-predicate must have been executed and replaced during
            // Phase 1.X before resolution reaches CTE validation; its presence
            // here is an internal invariant violation (loud, as before).
            Relation::PseudoPredicate { .. } => panic!(
                "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                 Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
            ),
            // Anonymous / TVF / ConsultedView contribute no ref of their own;
            // their recursive children (TVF table args, consulted-view bodies)
            // are reached by the default walk.
            Relation::Anonymous { .. }
            | Relation::TVF { .. }
            | Relation::ConsultedView { .. } => {}
            // InnerRelation's base identifier is contributed in enter_inner_relation.
            Relation::InnerRelation { .. } => {}
        }
        Ok(Descent::Continue)
    }

    fn enter_inner_relation(&mut self, pattern: &InnerRelationPattern<Unresolved>) -> Result<Descent> {
        // The InnerRelation's base table name is itself a reference (matching
        // the former walker); its subquery is descended by the default walk.
        match pattern {
            InnerRelationPattern::Indeterminate { identifier, .. }
            | InnerRelationPattern::UncorrelatedDerivedTable { identifier, .. }
            | InnerRelationPattern::CorrelatedScalarJoin { identifier, .. }
            | InnerRelationPattern::CorrelatedGroupJoin { identifier, .. } => {
                self.refs.push(identifier.name.to_string());
            }
        }
        Ok(Descent::Continue)
    }
}
