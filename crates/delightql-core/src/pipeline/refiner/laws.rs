// laws.rs - Implementation of Laws 1-6 from PRINCIPLED-RELOOK-AT-REFINER.md
//
// These laws govern predicate classification and association

use super::types::*;
use crate::pipeline::asts::resolved;
use std::collections::HashSet;

/// Context needed for law checking
pub struct LawContext {
    /// Which tables came from set operations
    pub setop_tables: HashSet<String>,

    /// Which tables have positional patterns (PLF)
    pub plf_tables: HashSet<String>,

    /// Lvar mappings for positional unification
    pub lvar_map: std::collections::HashMap<String, Vec<LvarBinding>>,
}

/// Law 1: Forbidden UL Fragment Join
/// A join condition modifying a J (join) to a previous UL must not use
/// any table-qualified columns of tables that participated in the UL
///
/// Example: (a UL b) J c FJC(c,a) is FORBIDDEN
pub fn check_law1(
    pred: &resolved::BooleanExpression,
    left_table: &str,
    right_table: &str,
    context: &LawContext,
) -> Option<ForbiddenReason> {
    // Check if left_table came from a UL (union-like) operation
    if context.setop_tables.contains(left_table) {
        // Check if predicate references any table from the UL fragment
        if references_table(pred, left_table) {
            return Some(ForbiddenReason::Law1UlFragmentJoin);
        }
    }

    // Check the reverse case
    if context.setop_tables.contains(right_table) && references_table(pred, right_table) {
        return Some(ForbiddenReason::Law1UlFragmentJoin);
    }

    None
}

/// Law 3: Intersection Qualification Requirements
/// Correlation predicates need proper qualification
///
/// Examples:
/// - users_2022(*) ; users_2023(*), email = email -- No qualification, FORBIDDEN
/// - users_2022(*) as u22; users_2023(*) as u23, u22.email = u23.email -- OK
pub fn check_law3(
    pred: &resolved::BooleanExpression,
    _left_table: &str,
    _right_table: &str,
) -> Option<ForbiddenReason> {
    // Check if predicate is an equality with unqualified columns on both sides
    if let resolved::BooleanExpression::Comparison {
        left,
        right,
        operator,
    } = pred
    {
        if operator == "=" || operator == "null_safe_eq" {
            // Both sides unqualified = forbidden
            if is_unqualified(left) && is_unqualified(right) {
                return Some(ForbiddenReason::Law3ImproperQualification);
            }
        }
    }
    None
}

/// Law 4: Non-Intersection Filters
/// Filters on columns that don't exist in all operands become regular F filters
/// This is handled during classification, not as a forbidding law

/// Law 5: Scope Eagerness
/// Predicates eagerly attach to the earliest operation where all their
/// referenced symbols are in scope
///
/// This returns the earliest valid scope point for a predicate
pub fn find_earliest_scope(
    pred: &resolved::BooleanExpression,
    tables_sequence: &[(usize, HashSet<String>)],
) -> ScopePoint {
    let referenced = extract_referenced_tables(pred);

    // Find the earliest point where all referenced tables are in scope
    for (position, tables_in_scope) in tables_sequence {
        if referenced.is_subset(tables_in_scope) {
            return ScopePoint {
                position: *position,
                tables_in_scope: tables_in_scope.clone(),
            };
        }
    }

    // If no valid scope found, attach at the end
    let last = tables_sequence.last().unwrap();
    ScopePoint {
        position: last.0,
        tables_in_scope: last.1.clone(),
    }
}

/// Law 6: PLF Set Operation Restriction
/// When PLFs are used with set operations:
/// - Correlation predicates allowed only with distinct Lvars
/// - Regular filters allowed only when Lvar appears in exactly ONE set-operation operand
///
/// Law6 only applies across set-operation boundaries. Lvars shared across
/// join boundaries are resolved by USING clauses and are not ambiguous.
pub fn check_law6(
    pred: &resolved::BooleanExpression,
    context: &LawContext,
) -> Option<ForbiddenReason> {
    use crate::pipeline::refiner::flattener::OperationContext;

    // Extract Lvars referenced in the predicate
    let referenced_lvars = extract_lvars(pred);

    for lvar in referenced_lvars {
        // Count how many distinct set-operation operands have this Lvar.
        // Only bindings from setop contexts count — join-context bindings
        // are handled by USING and are not ambiguous.
        let setop_operand_count = context
            .lvar_map
            .get(&lvar)
            .map(|bindings| {
                bindings
                    .iter()
                    .filter(|b| b.operation_context == OperationContext::FromSetOp)
                    .map(|b| &b.table)
                    .collect::<HashSet<_>>()
                    .len()
            })
            .unwrap_or(0);

        // If Lvar appears in multiple set-operation operands, it's ambiguous
        if setop_operand_count > 1 {
            return Some(ForbiddenReason::Law6PlfRestriction);
        }
    }

    None
}

// Helper functions

fn references_table(pred: &resolved::BooleanExpression, table: &str) -> bool {
    match pred {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            references_table_in_domain(left, table) || references_table_in_domain(right, table)
        }
        resolved::BooleanExpression::InnerExists { subquery, .. } => {
            // Check if the subquery references the table
            references_table_in_relational(subquery, table)
        }
        resolved::BooleanExpression::And { left, right }
        | resolved::BooleanExpression::Or { left, right } => {
            references_table(left, table) || references_table(right, table)
        }
        other => panic!("catch-all hit in laws.rs references_table: {:?}", other),
    }
}

fn references_table_in_domain(expr: &resolved::DomainExpression, table: &str) -> bool {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => {
            qualifier.as_ref().is_some_and(|q| q == table)
        }
        other => panic!(
            "catch-all hit in laws.rs references_table_in_domain: {:?}",
            other
        ),
    }
}

fn is_unqualified(expr: &resolved::DomainExpression) -> bool {
    match expr {
        resolved::DomainExpression::Lvar { qualifier, .. } => qualifier.is_none(),
        other => panic!("catch-all hit in laws.rs is_unqualified: {:?}", other),
    }
}

fn extract_referenced_tables(pred: &resolved::BooleanExpression) -> HashSet<String> {
    let mut tables = HashSet::new();
    if let resolved::BooleanExpression::Comparison { left, right, .. } = pred {
        extract_tables_from_domain(left, &mut tables);
        extract_tables_from_domain(right, &mut tables);
    }
    tables
}

fn extract_tables_from_domain(expr: &resolved::DomainExpression, tables: &mut HashSet<String>) {
    if let resolved::DomainExpression::Lvar { qualifier, .. } = expr {
        if let Some(q) = qualifier {
            tables.insert(q.to_string());
        }
    }
}

fn extract_lvars(pred: &resolved::BooleanExpression) -> HashSet<String> {
    let mut lvars = HashSet::new();
    if let resolved::BooleanExpression::Comparison { left, right, .. } = pred {
        extract_lvars_from_domain(left, &mut lvars);
        extract_lvars_from_domain(right, &mut lvars);
    }
    lvars
}

fn extract_lvars_from_domain(expr: &resolved::DomainExpression, lvars: &mut HashSet<String>) {
    if let resolved::DomainExpression::Lvar {
        name, qualifier, ..
    } = expr
    {
        // Only check unqualified Lvars for ambiguity - qualified Lvars are unambiguous
        if qualifier.is_none() {
            lvars.insert(name.to_string());
        }
    }
}

fn references_table_in_relational(expr: &resolved::RelationalExpression, table: &str) -> bool {
    match expr {
        resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            // Check filter condition
            let cond_refs = if let resolved::SigmaCondition::Predicate(pred) = condition {
                references_table(pred, table)
            } else {
                false
            };
            cond_refs || references_table_in_relational(source, table)
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            // Check join condition
            let cond_refs = if let Some(cond) = join_condition {
                references_table(cond, table)
            } else {
                false
            };
            cond_refs
                || references_table_in_relational(left, table)
                || references_table_in_relational(right, table)
        }
        other => panic!(
            "catch-all hit in laws.rs references_table_in_relational: {:?}",
            other
        ),
    }
}
