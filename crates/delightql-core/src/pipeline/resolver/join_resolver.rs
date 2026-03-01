//! Join-specific resolution logic
//!
//! This module handles JOIN condition creation and anonymous table unification.

use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_unresolved;

/// Detect unification opportunities for anonymous tables
/// When an anonymous table has headers that match columns from the left side of a join,
/// create a USING clause for implicit unification.
pub(super) fn detect_anonymous_table_unification(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[ast_resolved::ColumnMetadata],
    right_columns: &[ast_resolved::ColumnMetadata],
) -> Result<Option<ast_resolved::BooleanExpression>> {
    let mut using_columns = Vec::new();
    let mut on_conditions = Vec::new();

    for (idx, header) in headers.iter().enumerate() {
        match header {
            // Handle qualified references like "r.rating" or "reviews.rating"
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: Some(_),
                ..
            } => {
                // Check if this column exists on the left side
                // The qualifier is a reference to the left table, so we look for the column name
                if left_columns
                    .iter()
                    .any(|col| super::col_name_eq(col.name(), name))
                {
                    using_columns.push(name.clone());
                }
            }
            // Handle simple column names like "rating" - implicit unification
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: None,
                ..
            } => {
                // Check if a column with this name exists on the left side
                if left_columns
                    .iter()
                    .any(|col| super::col_name_eq(col.name(), name))
                {
                    using_columns.push(name.clone());
                }
            }
            // Handle function expressions like upper:(description)
            ast_unresolved::DomainExpression::Function(func) => {
                // Check if function contains column references that exist on left side
                if let Some(on_cond) =
                    extract_function_unification(func, left_columns, right_columns, idx)?
                {
                    on_conditions.push(on_cond);
                }
            }
            // Other expression types don't participate in unification
            other => panic!(
                "catch-all hit in join_resolver.rs extract_unification_columns: {:?}",
                other
            ),
        }
    }

    // If we have function-based conditions, return ON clause
    if !on_conditions.is_empty() {
        // Combine multiple conditions with AND
        return Ok(Some(combine_conditions(on_conditions)));
    }

    // Otherwise, if we have simple column matches, return USING clause
    if !using_columns.is_empty() {
        let using_cols: Vec<ast_resolved::UsingColumn> = using_columns
            .into_iter()
            .map(|name| {
                ast_resolved::UsingColumn::Regular(ast_resolved::QualifiedName {
                    namespace_path: NamespacePath::empty(),
                    name: name.into(),
                    grounding: None,
                })
            })
            .collect();

        return Ok(Some(ast_resolved::BooleanExpression::Using {
            columns: using_cols,
        }));
    }

    Ok(None)
}

/// Extract unification from function expressions like upper:(description)
fn extract_function_unification(
    func: &ast_unresolved::FunctionExpression,
    left_columns: &[ast_resolved::ColumnMetadata],
    right_columns: &[ast_resolved::ColumnMetadata],
    column_index: usize,
) -> Result<Option<ast_resolved::BooleanExpression>> {
    // Handle both Regular and Curried functions
    let (name, arguments) = match func {
        ast_unresolved::FunctionExpression::Regular {
            name, arguments, ..
        } => (name, arguments),
        ast_unresolved::FunctionExpression::Curried {
            name, arguments, ..
        } => (name, arguments),
        _ => return Ok(None),
    };

    // For functions like upper:(description) or upper(description)
    // Check if the argument references a left-side column
    if arguments.len() == 1 {
        if let ast_unresolved::DomainExpression::Lvar {
            name: col_name,
            qualifier,
            ..
        } = &arguments[0]
        {
            // Check if this column exists on the left side
            if let Some(_left_col) = left_columns
                .iter()
                .find(|col| super::col_name_eq(col.name(), col_name))
            {
                // Get the actual column name from the right-side resolved schema
                // The column at this index in the right table has already been resolved
                let right_col_name = if column_index < right_columns.len() {
                    right_columns[column_index].name().to_string()
                } else {
                    // Fallback if index out of bounds
                    format!("column{}", column_index + 1)
                };

                // Create ON condition: function(left.column) = right.column
                // Left side: function applied to left column
                let left_func = ast_resolved::FunctionExpression::Regular {
                    name: name.clone(),
                    namespace: None,
                    arguments: vec![ast_resolved::DomainExpression::Lvar {
                        name: col_name.clone(),
                        qualifier: qualifier.clone(),
                        namespace_path: NamespacePath::empty(),
                        alias: None,
                        provenance: ast_resolved::PhaseBox::phantom(),
                    }],
                    alias: None,
                    conditioned_on: None,
                };

                // Right side: anonymous table column (use actual resolved name)
                let right_col = ast_resolved::DomainExpression::Lvar {
                    name: right_col_name.into(),
                    qualifier: None,
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    provenance: ast_resolved::PhaseBox::phantom(),
                };

                return Ok(Some(ast_resolved::BooleanExpression::Comparison {
                    operator: "traditional_eq".to_string(),
                    left: Box::new(ast_resolved::DomainExpression::Function(left_func)),
                    right: Box::new(right_col),
                }));
            }
        }
    }

    Ok(None)
}

/// Combine multiple boolean conditions with AND
fn combine_conditions(
    conditions: Vec<ast_resolved::BooleanExpression>,
) -> ast_resolved::BooleanExpression {
    if conditions.len() == 1 {
        return conditions.into_iter().next().unwrap();
    }

    conditions
        .into_iter()
        .reduce(|acc, cond| ast_resolved::BooleanExpression::And {
            left: Box::new(acc),
            right: Box::new(cond),
        })
        .unwrap()
}

/// Create USING condition for JOIN from a list of column names
pub(super) fn create_using_condition(
    using_columns: Vec<String>,
) -> Result<ast_resolved::BooleanExpression> {
    // Convert to UsingColumn format
    let using_cols: Vec<ast_resolved::UsingColumn> = using_columns
        .into_iter()
        .map(|name| {
            ast_resolved::UsingColumn::Regular(ast_resolved::QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: name.into(),
                grounding: None,
            })
        })
        .collect();

    Ok(ast_resolved::BooleanExpression::Using {
        columns: using_cols,
    })
}
