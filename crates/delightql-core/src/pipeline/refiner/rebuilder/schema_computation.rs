use crate::pipeline::asts::refined::{self, JoinType, PhaseBox, SetOperator};
use crate::pipeline::asts::resolved::{self, ColumnMetadata, CprSchema, FqTable, TableName};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::refiner::rebuilder::collect_columns_from_schema;
use delightql_types::SqlIdentifier;

/// Extract schema from a refined relational expression
pub(super) fn extract_schema(expr: &refined::RelationalExpression) -> CprSchema {
    match expr {
        refined::RelationalExpression::Relation(rel) => {
            match rel {
                refined::Relation::Ground { cpr_schema, .. } => {
                    // Ground relations should have their schema from resolution
                    cpr_schema.get().clone()
                }
                refined::Relation::Anonymous {
                    column_headers,
                    cpr_schema,
                    ..
                } => {
                    // Anonymous tables have explicit headers
                    // column_headers is Option<Vec<DomainExpression>>, need to extract names
                    if let Some(headers) = column_headers {
                        let cols: Vec<ColumnMetadata> = headers
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| {
                                let col_name = match expr {
                                    refined::DomainExpression::Lvar { name, alias, .. } => {
                                        alias.as_ref().unwrap_or(name).clone()
                                    }
                                    _ => format!("expr_{}", i + 1).into(),
                                };
                                ColumnMetadata::new(
                                    resolved::ColumnProvenance::from_column(col_name),
                                    FqTable {
                                        parents_path: NamespacePath::empty(),
                                        name: TableName::Fresh,
                                        backend_schema: PhaseBox::from_optional_schema(None),
                                    },
                                    Some(i),
                                )
                            })
                            .collect();
                        CprSchema::Resolved(cols)
                    } else {
                        // No explicit headers — use the schema carried from resolution
                        // (e.g., column1, column2 for unnamed anonymous tables)
                        cpr_schema.get().clone()
                    }
                }
                refined::Relation::TVF { .. } => {
                    // TVFs have unknown schema until execution
                    CprSchema::Unknown
                }
                refined::Relation::InnerRelation { cpr_schema, .. } => {
                    // InnerRelation has schema from resolved subquery
                    cpr_schema.get().clone()
                }
                refined::Relation::ConsultedView { scoped, .. } => {
                    // ConsultedView body schema has internal names (Fresh from pipes,
                    // inner CTE names). Relabel with the scoped alias so joins
                    // see the external interface (e.g., Named("a") instead of Fresh).
                    let alias_name = scoped.get().alias();
                    let schema = scoped.get().schema().clone();
                    if let CprSchema::Resolved(cols) = &schema {
                        let relabeled: Vec<ColumnMetadata> = cols
                            .iter()
                            .map(|col| {
                                let mut c = col.clone();
                                c.fq_table.name = TableName::Named(alias_name.clone());
                                c
                            })
                            .collect();
                        CprSchema::Resolved(relabeled)
                    } else {
                        schema
                    }
                }

                refined::Relation::PseudoPredicate { .. } => {
                    panic!(
                        "INTERNAL ERROR: PseudoPredicate should not exist in this phase. \
                         Pseudo-predicates are executed and replaced during Phase 1.X (Effect Executor)."
                    )
                }
            }
        }
        refined::RelationalExpression::Join {
            left,
            right,
            cpr_schema,
            ..
        } => {
            // If we already have a schema, use it
            if let CprSchema::Resolved(_) = cpr_schema.get() {
                return cpr_schema.get().clone();
            }
            // Otherwise compute from operands
            merge_schemas_for_join(extract_schema(left), extract_schema(right))
        }
        refined::RelationalExpression::SetOperation { cpr_schema, .. } => {
            // Set operations should have their schema computed during rebuild
            cpr_schema.get().clone()
        }
        refined::RelationalExpression::Filter { source, .. } => {
            // Filters preserve source schema
            extract_schema(source)
        }
        refined::RelationalExpression::Pipe(pipe_expr) => {
            // Pipes should have their schema set
            pipe_expr.cpr_schema.get().clone()
        }
        refined::RelationalExpression::ErJoinChain { .. }
        | refined::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }
    }
}

/// Merge schemas for a join operation
pub(super) fn merge_schemas_for_join(left: CprSchema, right: CprSchema) -> CprSchema {
    match (left, right) {
        (CprSchema::Resolved(left_cols), CprSchema::Resolved(right_cols)) => {
            // Join concatenates columns from both sides
            let mut all_cols = left_cols;
            all_cols.extend(right_cols);
            CprSchema::Resolved(all_cols)
        }
        (CprSchema::Unknown, _) | (_, CprSchema::Unknown) => CprSchema::Unknown,
        (CprSchema::Failed { .. }, _) | (_, CprSchema::Failed { .. }) => {
            // If either side failed, the join fails
            CprSchema::Unknown
        }
        // Unresolved on either side: treat as unknown (can't reliably merge)
        (CprSchema::Unresolved(_), _) | (_, CprSchema::Unresolved(_)) => CprSchema::Unknown,
    }
}

/// Compute schema for a join with specific join type
pub(super) fn compute_join_schema(
    left: &refined::RelationalExpression,
    right: &refined::RelationalExpression,
    join_type: JoinType,
) -> PhaseBox<CprSchema, refined::Refined> {
    let left_schema = extract_schema(left);
    let right_schema = extract_schema(right);

    let merged = match (left_schema, right_schema) {
        (CprSchema::Resolved(left_cols), CprSchema::Resolved(right_cols)) => {
            match join_type {
                JoinType::Inner => {
                    // Inner join: all columns from both sides
                    let mut all_cols = left_cols;
                    all_cols.extend(right_cols);
                    CprSchema::Resolved(all_cols)
                }
                JoinType::LeftOuter => {
                    // Left outer: left columns + nullable right columns
                    let mut all_cols = left_cols;
                    // TODO: Mark right columns as nullable
                    all_cols.extend(right_cols);
                    CprSchema::Resolved(all_cols)
                }
                JoinType::RightOuter => {
                    // Right outer: nullable left columns + right columns
                    // TODO: Mark left columns as nullable
                    let mut all_cols = left_cols;
                    all_cols.extend(right_cols);
                    CprSchema::Resolved(all_cols)
                }
                JoinType::FullOuter => {
                    // Full outer: both sides nullable
                    // TODO: Mark all columns as nullable
                    let mut all_cols = left_cols;
                    all_cols.extend(right_cols);
                    CprSchema::Resolved(all_cols)
                }
            }
        }
        // Non-Resolved schemas (Unknown, Failed, Unresolved): degrade gracefully
        (CprSchema::Unknown, _) | (_, CprSchema::Unknown) => CprSchema::Unknown,
        (CprSchema::Failed { .. }, _) | (_, CprSchema::Failed { .. }) => CprSchema::Unknown,
        (CprSchema::Unresolved(_), _) | (_, CprSchema::Unresolved(_)) => CprSchema::Unknown,
    };

    // Create a Refined phase box directly
    // We can't use PhaseBox::new() because that creates Resolved phase
    // Instead, convert from Resolved to Refined
    let resolved_box = PhaseBox::new(merged);
    resolved_box.into_refined()
}

/// Compute schema for a filter operation
pub(super) fn compute_filter_schema(
    source: &refined::RelationalExpression,
) -> PhaseBox<CprSchema, refined::Refined> {
    // Filters preserve the source schema unchanged
    let schema = extract_schema(source);
    let resolved_box = PhaseBox::new(schema);
    resolved_box.into_refined()
}

/// Compute schema for a pipe operation with projections
/// Extract alias from any domain expression
fn extract_expr_alias(expr: &refined::DomainExpression) -> Option<&SqlIdentifier> {
    match expr {
        refined::DomainExpression::Lvar { alias, .. } => alias.as_ref(),
        refined::DomainExpression::Literal { alias, .. } => alias.as_ref(),
        refined::DomainExpression::ValuePlaceholder { alias } => alias.as_ref(),
        refined::DomainExpression::Predicate { alias, .. } => alias.as_ref(),
        refined::DomainExpression::PipedExpression { alias, .. } => alias.as_ref(),
        refined::DomainExpression::Parenthesized { alias, .. } => alias.as_ref(),
        // ColumnOrdinal should be resolved to Lvar before reaching refined phase
        refined::DomainExpression::Function(func) => match func {
            refined::FunctionExpression::Regular { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Bracket { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Infix { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Lambda { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Curried { .. } => None,
            refined::FunctionExpression::StringTemplate { .. } => {
                // StringTemplate should have been expanded to concat by resolver
                None
            }
            refined::FunctionExpression::CaseExpression { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::HigherOrder { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Curly { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::MetadataTreeGroup { alias, .. } => alias.as_ref(),
            refined::FunctionExpression::Window { alias, .. } => alias.as_ref(),
            _ => unimplemented!("JsonPath not yet implemented in this phase"),
        },
        other => panic!(
            "catch-all hit in rebuilder/schema_computation.rs extract_expr_alias: {:?}",
            other
        ),
    }
}

pub(super) fn compute_pipe_schema(
    projections: &[refined::DomainExpression],
) -> PhaseBox<CprSchema, refined::Refined> {
    let cols: Vec<ColumnMetadata> = projections
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            // First try to get the alias from any expression type
            if let Some(alias) = extract_expr_alias(expr) {
                // If there's an alias, use it
                ColumnMetadata::new(
                    resolved::ColumnProvenance::from_column(alias.clone()),
                    FqTable {
                        parents_path: NamespacePath::empty(),
                        name: TableName::Fresh,
                        backend_schema: PhaseBox::from_optional_schema(None),
                    },
                    Some(i),
                )
            } else {
                // No alias - use the expression's natural name
                match expr {
                    refined::DomainExpression::Lvar { name, .. } => ColumnMetadata::new(
                        resolved::ColumnProvenance::from_column(name.clone()),
                        FqTable {
                            parents_path: NamespacePath::empty(),
                            name: TableName::Fresh,
                            backend_schema: PhaseBox::from_optional_schema(None),
                        },
                        Some(i),
                    ),
                    refined::DomainExpression::Literal { .. } => ColumnMetadata::new(
                        resolved::ColumnProvenance::from_column(format!("literal_{}", i + 1)),
                        FqTable {
                            parents_path: NamespacePath::empty(),
                            name: TableName::Fresh,
                            backend_schema: PhaseBox::from_optional_schema(None),
                        },
                        Some(i),
                    ),
                    _ => {
                        // Other expressions without alias: generate a name
                        ColumnMetadata::new(
                            resolved::ColumnProvenance::from_column(
                                crate::pipeline::naming::generate_refined_domain_expression_column_name(expr, i)
                            ),
                            FqTable {
                                parents_path: NamespacePath::empty(),
                                name: TableName::Fresh,
                                backend_schema: PhaseBox::from_optional_schema(None),
                            },
                            Some(i),
                        )
                    }
                }
            }
        })
        .collect();

    let resolved_box = PhaseBox::new(CprSchema::Resolved(cols));
    resolved_box.into_refined()
}

/// Compute unified schema for a set operation
pub(super) fn compute_setop_schema(
    operator: SetOperator,
    operands: &[refined::RelationalExpression],
) -> PhaseBox<CprSchema, refined::Refined> {
    match operator {
        SetOperator::UnionAllPositional => {
            // Positional union: use first operand's schema
            if let Some(first) = operands.first() {
                let schema = extract_schema(first);
                let resolved_box = PhaseBox::new(schema);
                resolved_box.into_refined()
            } else {
                PhaseBox::phantom()
            }
        }
        _ => {
            // For other operators, build unified schema
            // This is a simplified version - should track all columns properly
            log::debug!(
                "compute_setop_schema: Building unified schema for operator {:?}",
                operator
            );
            let mut all_columns = indexmap::IndexSet::new();

            for operand in operands {
                let schema = extract_schema(operand);
                collect_columns_from_schema(&schema, &mut all_columns);
            }

            log::debug!(
                "compute_setop_schema: HashSet contains {} columns",
                all_columns.len()
            );
            let unified_cols: Vec<ColumnMetadata> = all_columns
                .into_iter()
                .enumerate()
                .map(|(i, name)| {
                    ColumnMetadata::new(
                        resolved::ColumnProvenance::from_column(name),
                        FqTable {
                            parents_path: NamespacePath::empty(),
                            name: TableName::Fresh,
                            backend_schema: PhaseBox::from_optional_schema(None),
                        },
                        Some(i),
                    )
                })
                .collect();

            let resolved_box = PhaseBox::new(CprSchema::Resolved(unified_cols));
            resolved_box.into_refined()
        }
    }
}
