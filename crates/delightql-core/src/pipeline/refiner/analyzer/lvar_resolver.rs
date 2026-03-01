// lvar_resolver.rs - Handle Lvar bindings and USING column interpretation
//
// This module handles Lvar extraction and binding creation

use crate::pipeline::asts::resolved;
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::refiner::flattener::{FlatOperatorKind, FlatSegment};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Extract Lvar bindings from tables with positional patterns
pub(super) fn extract_lvar_bindings(segment: &FlatSegment) -> HashMap<String, Vec<LvarBinding>> {
    let mut lvar_map: HashMap<String, Vec<LvarBinding>> = HashMap::new();

    for table in &segment.tables {
        let table_name = table
            .alias
            .clone()
            .unwrap_or_else(|| table.identifier.name.to_string());

        // Extract Lvars from positional patterns
        let lvars = extract_lvars_from_domain_spec(&table.domain_spec);

        for (lvar_name, _position) in lvars {
            lvar_map
                .entry(lvar_name.clone())
                .or_default()
                .push(LvarBinding {
                    table: table_name.clone(),
                    operation_context: table.operation_context,
                });
        }

        // Also extract Lvars from anonymous table headers for implicit unification
        if let Some(ref anon_data) = table.anonymous_data {
            if let Some(ref headers) = anon_data.column_headers {
                for (_position, header) in headers.iter().enumerate() {
                    // Only pure unqualified Lvars participate in implicit unification
                    if let resolved::DomainExpression::Lvar {
                        name,
                        qualifier: None,
                        ..
                    } = header
                    {
                        // Anonymous tables use "_" as their internal alias for implicit unification
                        lvar_map
                            .entry(name.to_string())
                            .or_default()
                            .push(LvarBinding {
                                table: table_name.clone(),
                                operation_context: table.operation_context,
                            });
                    }
                }
            }
        }
    }

    lvar_map
}

/// Extract Lvars from a domain spec (for positional patterns)
fn extract_lvars_from_domain_spec(spec: &resolved::DomainSpec) -> Vec<(String, usize)> {
    match spec {
        resolved::DomainSpec::Positional(expressions) => {
            let mut lvars = Vec::new();
            for (position, expr) in expressions.iter().enumerate() {
                if let resolved::DomainExpression::Lvar {
                    name,
                    qualifier: _,
                    namespace_path: _,
                    alias: _,
                    provenance: _,
                } = expr
                {
                    lvars.push((name.to_string(), position));
                }
            }
            lvars
        }
        // Glob (*), GlobWithUsing (*.(cols)), Bare (.) — no positional lvar bindings.
        // GlobWithUsing's columns are handled separately via the USING join mechanism.
        resolved::DomainSpec::Glob
        | resolved::DomainSpec::GlobWithUsing(_)
        | resolved::DomainSpec::Bare => Vec::new(),
    }
}

/// Create USING predicates from shared Lvars (positional unification)
/// This makes implicit Prolog-style unification explicit
pub(super) fn create_lvar_using_predicates(
    predicates: &mut Vec<AnalyzedPredicate>,
    flat: &FlatSegment,
) {
    // Extract Lvar mappings first
    let lvar_map = extract_lvar_bindings(flat);

    // Process each join operator
    for (op_idx, op) in flat.operators.iter().enumerate() {
        if let FlatOperatorKind::Join { using_columns } = &op.kind {
            // Skip if we already have USING from GlobWithUsing
            if using_columns.is_some() {
                continue;
            }

            // Find Lvars shared between left and right operands
            let mut shared_lvars = Vec::new();

            for (lvar_name, bindings) in &lvar_map {
                // Skip anonymous variables
                if lvar_name == "_" {
                    continue;
                }

                // Check if Lvar appears in both left and right tables
                let in_left = bindings.iter().any(|b| op.left_tables.contains(&b.table));
                let in_right = bindings.iter().any(|b| op.right_tables.contains(&b.table));

                if in_left && in_right {
                    shared_lvars.push(lvar_name.clone());
                }
            }

            // Create synthetic USING predicate if shared Lvars exist
            if !shared_lvars.is_empty() {
                log::debug!(
                    "Creating USING predicate for join {} with shared Lvars: {:?}",
                    op_idx,
                    shared_lvars
                );

                // Sort for deterministic output
                shared_lvars.sort();

                let using_columns: Vec<resolved::UsingColumn> = shared_lvars
                    .into_iter()
                    .map(|name| {
                        resolved::UsingColumn::Regular(resolved::QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: name.into(),
                            grounding: None,
                        })
                    })
                    .collect();

                // Determine the tables involved (for PredicateClass)
                let left_table = op
                    .left_tables
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "_unknown_left".to_string());
                let right_table = op
                    .right_tables
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "_unknown_right".to_string());

                predicates.push(AnalyzedPredicate {
                    class: PredicateClass::FJC {
                        left: left_table,
                        right: right_table,
                    },
                    expr: resolved::BooleanExpression::Using {
                        columns: using_columns,
                    },
                    operator_ref: OperatorRef::Join { position: op_idx },
                    origin: resolved::FilterOrigin::Generated,
                });
            }
        }
    }
}
