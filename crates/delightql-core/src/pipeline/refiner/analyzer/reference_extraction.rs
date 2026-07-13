// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// reference_extraction.rs - Extract table references from expressions
//
// This module handles extraction of table references from various AST node types

use crate::error::Result;
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::{FlatPredicate, FlatSegment};
use std::collections::HashSet;

/// Extract table references from a DomainExpression (recursively).
///
/// SCOPE-LOCAL (INVENTORY L6): collects table refs from a predicate within the
/// current scope; the boolean sub-walker
/// (`extract_table_references_from_boolean`) does NOT descend nested subquery
/// scopes. The `_in_scope` name marks that stop boundary (and disambiguates it
/// from the whole-tree `cte_validation::extract_table_references`).
pub(super) fn extract_table_references_in_scope(expr: &resolved::DomainExpression) -> Vec<String> {
    let mut tables = Vec::new();

    match expr {
        resolved::DomainExpression::Lvar {
            qualifier: Some(qual),
            ..
        } => {
            tables.push(qual.to_string());
        }
        resolved::DomainExpression::Lvar {
            qualifier: None, ..
        } => {
            // Unqualified column: no table reference to extract
        }
        resolved::DomainExpression::Function(func) => {
            // Recursively extract from function arguments
            match func {
                resolved::FunctionExpression::Regular { arguments, .. }
                | resolved::FunctionExpression::Curried { arguments, .. }
                | resolved::FunctionExpression::Bracket { arguments, .. } => {
                    for arg in arguments {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                }
                resolved::FunctionExpression::HigherOrder {
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    for arg in curried_arguments {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                    for arg in regular_arguments {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                }
                resolved::FunctionExpression::Infix { left, right, .. } => {
                    tables.extend(extract_table_references_in_scope(left));
                    tables.extend(extract_table_references_in_scope(right));
                }
                resolved::FunctionExpression::Lambda { body, .. } => {
                    tables.extend(extract_table_references_in_scope(body));
                }
                resolved::FunctionExpression::StringTemplate { .. } => {
                    // StringTemplate should have been expanded to concat by resolver
                    // No table references to extract from unexpanded templates
                }
                resolved::FunctionExpression::CaseExpression { arms, .. } => {
                    // Extract table references from all CASE arms
                    for arm in arms {
                        match arm {
                            resolved::CaseArm::Simple {
                                test_expr, result, ..
                            } => {
                                tables.extend(extract_table_references_in_scope(test_expr));
                                tables.extend(extract_table_references_in_scope(result));
                            }
                            resolved::CaseArm::CurriedSimple { result, .. } => {
                                // Curried simple has no test_expr (it uses @)
                                tables.extend(extract_table_references_in_scope(result));
                            }
                            resolved::CaseArm::Searched { condition, result } => {
                                tables.extend(extract_table_references_from_boolean(condition));
                                tables.extend(extract_table_references_in_scope(result));
                            }
                            resolved::CaseArm::Default { result } => {
                                tables.extend(extract_table_references_in_scope(result));
                            }
                        }
                    }
                }
                resolved::FunctionExpression::Curly { .. } => {
                    // Tree groups don't contain table references (Epoch 1)
                }
                resolved::FunctionExpression::MetadataTreeGroup { .. } => {
                    // Tree groups don't contain table references (Epoch 1)
                }
                resolved::FunctionExpression::Window {
                    arguments,
                    partition_by,
                    order_by,
                    ..
                } => {
                    // Extract from window function arguments, partition, and order clauses
                    for arg in arguments {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                    for arg in partition_by {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                    for spec in order_by {
                        tables.extend(extract_table_references_in_scope(&spec.column));
                    }
                }
                // Spelled per R-I3 (was a bare `_ =>`): JsonPath.source/path and
                // Array.members carry recursive domain expressions this scope-local
                // walker does not yet extract from. Kept as the SAME `unimplemented!`
                // (byte-identical) so a newly-added function variant forces a
                // decision here instead of being swallowed.
                resolved::FunctionExpression::JsonPath { .. }
                | resolved::FunctionExpression::Array { .. } => {
                    unimplemented!("JsonPath not yet implemented in this phase")
                }
            }
        }
        resolved::DomainExpression::Predicate { expr, .. } => {
            // Extract from boolean expression
            tables.extend(extract_table_references_from_boolean(expr));
        }
        resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            tables.extend(extract_table_references_in_scope(value));
            for (_, transform) in transforms {
                if let resolved::FunctionExpression::Regular { arguments, .. } = transform {
                    for arg in arguments {
                        tables.extend(extract_table_references_in_scope(arg));
                    }
                }
            }
        }
        resolved::DomainExpression::Parenthesized { inner, .. } => {
            tables.extend(extract_table_references_in_scope(inner));
        }
        // Tuple: recurse into elements
        resolved::DomainExpression::Tuple { elements, .. } => {
            for elem in elements {
                tables.extend(extract_table_references_in_scope(elem));
            }
        }
        // ScalarSubquery: the subquery relation references tables, but that's handled
        // at the relational level, not here
        resolved::DomainExpression::ScalarSubquery { .. } => {}
        // PivotOf: recurse
        resolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => {
            tables.extend(extract_table_references_in_scope(value_column));
            tables.extend(extract_table_references_in_scope(pivot_key));
        }
        // Leaf types: no table references
        resolved::DomainExpression::Literal { .. }
        | resolved::DomainExpression::Projection(_)
        | resolved::DomainExpression::NonUnifiyingUnderscore
        | resolved::DomainExpression::ValuePlaceholder { .. }
        | resolved::DomainExpression::Substitution(_)
        | resolved::DomainExpression::ColumnOrdinal(_) => {}
    }

    tables
}

/// Helper to extract table references from boolean expressions
pub(super) fn extract_table_references_from_boolean(
    expr: &resolved::BooleanExpression,
) -> Vec<String> {
    let mut tables = Vec::new();

    match expr {
        resolved::BooleanExpression::Comparison { left, right, .. } => {
            tables.extend(extract_table_references_in_scope(left));
            tables.extend(extract_table_references_in_scope(right));
        }
        resolved::BooleanExpression::And { left, right } => {
            tables.extend(extract_table_references_from_boolean(left));
            tables.extend(extract_table_references_from_boolean(right));
        }
        resolved::BooleanExpression::Or { left, right } => {
            tables.extend(extract_table_references_from_boolean(left));
            tables.extend(extract_table_references_from_boolean(right));
        }
        resolved::BooleanExpression::Not { expr } => {
            tables.extend(extract_table_references_from_boolean(expr));
        }
        resolved::BooleanExpression::GlobCorrelation { left, right } => {
            tables.push(left.to_string());
            tables.push(right.to_string());
        }
        resolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            tables.push(left.to_string());
            tables.push(right.to_string());
        }
        // SCOPE-LOCAL contract, spelled per R-I3 (was a bare `other =>`). This
        // predicate-reference walker runs over already-flattened current-scope
        // conjunctions of comparisons; the subquery-bearing variants
        // (InnerExists.subquery, InRelational.value/subquery, In.value/set,
        // Sigma.condition) do NOT reach it in practice — they are handled at the
        // relational level, before/around flattening — so encountering one is an
        // internal invariant violation, kept as the SAME LOUD panic (byte-identical
        // behavior). Spelling the variants means a newly-added boolean variant now
        // forces a decision here instead of being silently swallowed.
        other @ (resolved::BooleanExpression::InnerExists { .. }
        | resolved::BooleanExpression::InRelational { .. }
        | resolved::BooleanExpression::In { .. }
        | resolved::BooleanExpression::Sigma { .. }
        | resolved::BooleanExpression::Using { .. }
        | resolved::BooleanExpression::BooleanLiteral { .. }) => panic!("catch-all hit in analyzer/reference_extraction.rs extract_table_references_from_boolean: {:?}", other),
    }

    tables
}

/// Check if a table's schema contains a specific column
pub(super) fn table_has_column(schema: &resolved::CprSchema, column_name: &str) -> bool {
    if let resolved::CprSchema::Resolved(columns) = schema {
        columns
            .iter()
            .any(|col| delightql_types::SqlIdentifier::str_eq(col.name(), column_name))
    } else {
        false
    }
}

/// Extract which tables are referenced by this predicate
pub(super) fn extract_referenced_tables(
    pred: &FlatPredicate,
    flat: &FlatSegment,
) -> Result<HashSet<String>> {
    let mut tables = HashSet::new();

    // Add qualified references
    for qual_ref in &pred.qualified_refs {
        tables.insert(qual_ref.clone());
    }

    // For unqualified refs, use CPR schema to determine which table they belong to
    for unqual_ref in &pred.unqualified_refs {
        // Search through all tables' schemas to find the column
        for table in &flat.tables {
            if table_has_column(&table.schema, unqual_ref) {
                let table_name = table
                    .alias
                    .clone()
                    .unwrap_or_else(|| table.identifier.name.to_string());
                tables.insert(table_name);
                break; // Found the table for this column
            }
        }
        // Note: If we can't find the column in any schema, we don't add any table
        // This lets the caller handle the error case appropriately
    }

    Ok(tables)
}
