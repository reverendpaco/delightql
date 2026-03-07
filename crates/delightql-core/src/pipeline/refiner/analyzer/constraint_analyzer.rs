// constraint_analyzer.rs - Extract constraints from table patterns and anonymous tables
//
// This module handles constraint extraction from positional patterns and anonymous table processing

use super::reference_extraction::extract_table_references;
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::refiner::flattener::{FlatOperator, FlatOperatorKind, FlatSegment, FlatTable};
use crate::pipeline::refiner::types::*;

/// Create join predicates for anonymous table constraints (Epoch 3 fix)
pub(super) fn create_anonymous_table_join_predicates(
    analyzed_predicates: &mut Vec<AnalyzedPredicate>,
    flat: &FlatSegment,
) {
    log::debug!(
        "create_anonymous_table_join_predicates called with {} tables",
        flat.tables.len()
    );
    // For each anonymous table with column headers that contain constraints
    for (table_idx, table) in flat.tables.iter().enumerate() {
        log::debug!(
            "Table {}: has anon_data? {}",
            table_idx,
            table.anonymous_data.is_some()
        );
        if let Some(ref anon_data) = table.anonymous_data {
            // EXISTS-mode anonymous tables (from IN literal desugaring) are self-contained
            // subqueries — their qualified column headers are correlation references to the
            // outer scope, not join conditions within this segment.
            if anon_data.exists_mode {
                log::debug!(
                    "Skipping exists-mode anonymous table at index {}",
                    table_idx
                );
                continue;
            }
            log::debug!("Processing anonymous table at index {}", table_idx);
            if let Some(ref headers) = anon_data.column_headers {
                log::debug!("Anonymous table has {} headers", headers.len());
                // Process each header - create constraints for all non-pure-Lvar expressions
                for (col_idx, header) in headers.iter().enumerate() {
                    log::debug!("Processing header {} : {:?}", col_idx, header);

                    // Determine column name and whether to create a constraint
                    let (column_name, should_create_constraint) = match header {
                        // Pure Lvars just introduce columns, not constraints
                        resolved::DomainExpression::Lvar {
                            name,
                            qualifier: None,
                            ..
                        } => (name.to_string(), false),
                        // Qualified Lvars - use the name part as column name
                        resolved::DomainExpression::Lvar {
                            name,
                            qualifier: Some(_),
                            ..
                        } => (name.to_string(), true),
                        // Everything else creates a constraint with generic column name
                        _ => (
                            crate::pipeline::naming::generate_domain_expression_column_name(
                                header, col_idx,
                            ),
                            true,
                        ),
                    };

                    // Create constraint for any non-pure-Lvar expression
                    if should_create_constraint {
                        // Generic constraint: _.column = domain_expression
                        let anon_table_alias = "_";

                        // Left side: anonymous table column
                        let left = resolved::DomainExpression::Lvar {
                            name: column_name.clone().into(),
                            qualifier: Some(anon_table_alias.into()),
                            alias: None,
                            namespace_path: NamespacePath::empty(),
                            provenance: resolved::PhaseBox::phantom(),
                        };

                        // Right side: the original expression (ANY DomainExpression!)
                        let right = header.clone();

                        // Check if we can use USING clause instead of ON
                        // This happens when the right side is a simple column reference with the same name
                        let predicate = match header {
                            resolved::DomainExpression::Lvar {
                                name: right_name,
                                qualifier: Some(_),
                                ..
                            } if right_name == &column_name => {
                                // Both sides have same column name - use USING for cleaner SQL
                                log::debug!("Creating USING predicate for column: {}", column_name);
                                resolved::BooleanExpression::Using {
                                    columns: vec![resolved::UsingColumn::Regular(
                                        resolved::QualifiedName {
                                            namespace_path: NamespacePath::empty(),
                                            name: column_name.clone().into(),
                                            grounding: None,
                                        },
                                    )],
                                }
                            }
                            _ => {
                                // Different names or complex expression - use ON
                                resolved::BooleanExpression::Comparison {
                                    operator: "traditional_eq".to_string(),
                                    left: Box::new(left),
                                    right: Box::new(right),
                                }
                            }
                        };

                        // Determine classification based on expression type
                        // Check if expression references other tables (for join vs filter classification)
                        let referenced_tables = extract_table_references(header);

                        let class = if !referenced_tables.is_empty() {
                            // Expression references other tables - create FJC (join condition)
                            // Use the first referenced table as the "left" side
                            PredicateClass::FJC {
                                left: referenced_tables[0].clone(),
                                right: anon_table_alias.to_string(),
                            }
                        } else {
                            // No table references - this is a filter on the anonymous table
                            PredicateClass::F {
                                table: anon_table_alias.to_string(),
                            }
                        };

                        // Find the join operator position
                        let operator_ref = if table_idx > 0 {
                            OperatorRef::Join {
                                position: table_idx - 1,
                            }
                        } else {
                            OperatorRef::TopLevel
                        };

                        log::debug!(
                            "Creating anonymous table constraint (generic): {}.{} = {:?}",
                            anon_table_alias,
                            column_name,
                            header
                        );

                        analyzed_predicates.push(AnalyzedPredicate {
                            class,
                            expr: predicate,
                            operator_ref,
                            origin: resolved::FilterOrigin::Generated,
                        });
                    }

                    // Note: column_name is used by the transformer to name the column
                }
            }
        }
    }
}

/// Process GlobWithUsing domain specs and update join operators with USING columns
pub(super) fn process_glob_with_using(
    mut operators: Vec<FlatOperator>,
    tables: &[FlatTable],
) -> Vec<FlatOperator> {
    // Look for tables with GlobWithUsing and update the next join operator
    for i in 0..tables.len() {
        if let resolved::DomainSpec::GlobWithUsing(using_cols) = &tables[i].domain_spec {
            log::debug!(
                "Found GlobWithUsing on table {} with columns: {:?}",
                tables[i]
                    .alias
                    .as_deref()
                    .unwrap_or(&tables[i].identifier.name),
                using_cols
            );

            // The join operator at position j-1 joins table[j-1] with table[j]
            // If table[j] has GlobWithUsing, it applies to join j-1
            // But actually: orders(*{user_id}) means the USING applies when joining TO orders
            // So if orders is at position 1, the join at position 0 should get the USING
            if i > 0 && i - 1 < operators.len() {
                log::debug!("Applying USING to join at position {}", i - 1);
                if let FlatOperatorKind::Join {
                    ref mut using_columns,
                } = &mut operators[i - 1].kind
                {
                    *using_columns = Some(using_cols.clone());
                }
            }
        }
    }

    operators
}

/// Process Using operators (.(cols)) in pipe expressions and update join operators with USING columns
///
/// Similar to process_glob_with_using, but handles the new `.(cols)` unary operator syntax
/// which is stored in the table's pipe_expr rather than domain_spec.
pub(super) fn process_using_operators(
    mut operators: Vec<FlatOperator>,
    tables: &[FlatTable],
) -> Vec<FlatOperator> {
    // Look for tables with pipe expressions containing Using operators
    for i in 0..tables.len() {
        if let Some(ref pipe_expr) = tables[i].pipe_expr {
            // Recursively search for Using operators in the pipe chain
            if let Some(using_cols) = extract_using_columns_from_pipe(pipe_expr) {
                log::debug!(
                    "Found Using operator on table {} with columns: {:?}",
                    tables[i]
                        .alias
                        .as_deref()
                        .unwrap_or(&tables[i].identifier.name),
                    using_cols
                );

                // Same logic as GlobWithUsing: the join operator at position i-1
                // joins table[i-1] with table[i]
                if i > 0 && i - 1 < operators.len() {
                    log::debug!("Applying USING to join at position {}", i - 1);
                    if let FlatOperatorKind::Join {
                        ref mut using_columns,
                    } = &mut operators[i - 1].kind
                    {
                        *using_columns = Some(using_cols);
                    }
                }
            }
        }
    }

    operators
}

/// Recursively search a pipe expression for a Using operator and extract its columns
#[stacksafe::stacksafe]
fn extract_using_columns_from_pipe(expr: &resolved::RelationalExpression) -> Option<Vec<String>> {
    match expr {
        resolved::RelationalExpression::Pipe(pipe) => {
            // Check if this pipe's operator is Using
            if let resolved::UnaryRelationalOperator::Using { columns } = &pipe.operator {
                return Some(columns.clone());
            }
            // Recursively check the source
            extract_using_columns_from_pipe(&pipe.source)
        }
        // Leaf and branching nodes: no Using operator in these
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::Filter { .. }
        | resolved::RelationalExpression::Join { .. }
        | resolved::RelationalExpression::SetOperation { .. } => None,
        // ER chains consumed before refiner
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before constraint analysis")
        }
    }
}
