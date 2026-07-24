// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Join-specific resolution logic
//!
//! This module handles JOIN condition creation and anonymous table unification.

use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
use crate::pipeline::ast_unresolved;

/// A qualified header `q.name` matches a column by its FULL NAME:
/// the qualifier half matches the column's SQL qualifier, or the
/// access name it answers to (a consulted view's columns ride under a
/// synthetic SQL alias for hygiene; the law addresses the access name).
fn qualified_header_matches(
    col: &ast_resolved::ColumnMetadata,
    name: &str,
    q: &delightql_types::SqlIdentifier,
) -> bool {
    if !delightql_types::SqlIdentifier::str_eq(col.name(), name) {
        return false;
    }
    let sql_qualifier = matches!(
        col.qualifier(),
        ast_resolved::TableName::Named(t)
            if delightql_types::SqlIdentifier::str_eq(t.as_str(), q.as_str())
    );
    let access = col
        .access_name
        .as_ref()
        .is_some_and(|a| delightql_types::SqlIdentifier::str_eq(a.as_str(), q.as_str()));
    sql_qualifier || access
}

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
            // Qualified header: names a glob-exported lvar by its FULL
            // name — the qualifier must match the column's table/alias,
            // never just the column half (a junk qualifier must refuse,
            // not silently unify).
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: Some(q),
                ..
            } => {
                let matched = left_columns
                    .iter()
                    .any(|col| qualified_header_matches(col, name, q));
                if matched {
                    using_columns.push(name.clone());
                } else {
                    return Err(crate::error::DelightQLError::validation_error_categorized(
                        "resolution/anon/qualifier",
                        format!(
                            "anonymous-table header '{}.{}' names no column in scope",
                            q, name
                        ),
                        "the qualifier must be a relation or alias to the left, and the column must exist on it",
                    ));
                }
            }
            // Bare header: an lvar. Unification is by FULL-NAME identity,
            // so it unifies only with a DECLARED bare lvar (positional
            // binding, another anon header) — a glob column's full name
            // is qualified and is no partner, and a column that answers
            // to an ACCESS NAME (an aliased export) is not bare either.
            // A bare header that merely COLLIDES with such a column
            // refuses: silently unifying reads a name the user never
            // declared, silently expanding would duplicate the heading.
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: None,
                ..
            } => {
                let declared = left_columns.iter().any(|col| {
                    col.declared_bare
                        && col.access_name.is_none()
                        && delightql_types::SqlIdentifier::str_eq(col.name(), name)
                });
                if declared {
                    using_columns.push(name.clone());
                } else if left_columns
                    .iter()
                    .any(|col| delightql_types::SqlIdentifier::str_eq(col.name(), name))
                {
                    return Err(crate::error::DelightQLError::validation_error_categorized(
                        "resolution/anon/glob_collision",
                        format!(
                            "anonymous-table header '{}' collides with a wildcard column of the same name: globs and aliased exports address their lvars by QUALIFIED names, so bare '{}' is not it",
                            name, name
                        ),
                        "qualify the header (table.column or alias.column) to unify with the exported column, or rename it to declare a fresh column",
                    ));
                }
                // No name in scope at all: a fresh column — expansion.
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
            // Ground header: a membership probe (inverted In). It
            // declares nothing and unifies with nothing — membership
            // routing consumes it after this scan.
            ast_unresolved::DomainExpression::Literal { .. } => {}
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

/// Probe unification for an ALIASED anonymous table, refusal-free.
/// An alias closes the relation: its headers declare fresh columns
/// under the alias (x.city), so they neither unify bare nor collide —
/// the collision regime exists to disambiguate bare declarations, and
/// an aliased header is not bare. The probe still computes would-be
/// unification, because a table whose every header WOULD unify is
/// membership shape, and membership refuses the alias rather than
/// silently becoming a relational join.
///
/// Returns `Some(Using)` only when EVERY lvar header has a partner
/// (bare-declared, or qualified full-name); anything less is `None` —
/// a closed relation joins by explicit predicate, never partial USING.
pub(super) fn aliased_anon_would_unify(
    headers: &[ast_unresolved::DomainExpression],
    left_columns: &[ast_resolved::ColumnMetadata],
) -> Option<ast_resolved::BooleanExpression> {
    let mut using_columns = Vec::new();
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: Some(q),
                ..
            } => {
                if left_columns
                    .iter()
                    .any(|col| qualified_header_matches(col, name, q))
                {
                    using_columns.push(name.clone());
                } else {
                    return None;
                }
            }
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier: None,
                ..
            } => {
                let declared = left_columns.iter().any(|col| {
                    col.declared_bare
                        && col.access_name.is_none()
                        && delightql_types::SqlIdentifier::str_eq(col.name(), name)
                });
                if declared {
                    using_columns.push(name.clone());
                } else {
                    return None;
                }
            }
            // Ground headers are probes; they neither unify nor block.
            ast_unresolved::DomainExpression::Literal { .. } => {}
            // Function headers are relational — never membership shape.
            _ => return None,
        }
    }
    if using_columns.is_empty() {
        return None;
    }
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
    Some(ast_resolved::BooleanExpression::Using {
        columns: using_cols,
    })
}

/// Build the membership predicate for an anonymous table in join
/// position, when it has membership shape — every header a probe:
/// a ground literal, or an lvar unified with a column in scope.
///
/// Returns `Ok(None)` when the table is NOT a membership test (fresh
/// columns, function headers) and must stay relational. Refuses when
/// a witness marker, a ground header, or full unification demands
/// membership shape that an alias or a contradicting header denies —
/// membership exports no columns, so an alias never names one.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_anon_membership(
    headers: Option<&[ast_unresolved::DomainExpression]>,
    join_condition: &Option<ast_resolved::BooleanExpression>,
    left_columns: &[ast_resolved::ColumnMetadata],
    resolved_right: &ast_resolved::RelationalExpression,
    exists_mode: bool,
    negated: bool,
    alias: Option<&delightql_types::SqlIdentifier>,
) -> Result<Option<ast_resolved::BooleanExpression>> {
    let headers = match headers {
        Some(h) if !h.is_empty() => h,
        _ => {
            if exists_mode {
                return Err(crate::error::DelightQLError::validation_error_categorized(
                    "resolution/anon/witness_shape",
                    "a witness anonymous table (+_ or \\+_) is a membership test and needs headers: the probe on the left of @, the candidates as rows".to_string(),
                    "write +_(probe @ candidate; candidate) or drop the witness marker",
                ));
            }
            return Ok(None);
        }
    };

    let mut lvar_count = 0usize;
    let mut lit_count = 0usize;
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Lvar { .. } => lvar_count += 1,
            ast_unresolved::DomainExpression::Literal { .. } => lit_count += 1,
            _ => {
                // Function headers unify by ON-condition — relational,
                // never membership.
                if exists_mode {
                    return Err(crate::error::DelightQLError::validation_error_categorized(
                        "resolution/anon/witness_shape",
                        "a witness anonymous table (+_ or \\+_) is a membership test: every header must be a ground value or an lvar that unifies with a column in scope".to_string(),
                        "function headers are relational; drop the witness marker",
                    ));
                }
                return Ok(None);
            }
        }
    }

    let unified_count = match join_condition {
        Some(ast_resolved::BooleanExpression::Using { columns }) => columns.len(),
        _ => 0,
    };
    let all_lvars_unified = unified_count == lvar_count;

    if exists_mode {
        if alias.is_some() {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/witness_alias",
                "a witness anonymous table (+_ or \\+_) is a membership test and exports no columns: an alias names nothing".to_string(),
                "drop the alias, or drop the witness marker to make it a relation",
            ));
        }
        if !all_lvars_unified {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/witness_shape",
                "a witness anonymous table (+_ or \\+_) is a membership test: every header must be a ground value or an lvar that unifies with a column in scope".to_string(),
                "a header that unifies with nothing would declare a fresh column, and a membership test has no columns to declare",
            ));
        }
    } else if lit_count > 0 {
        if !all_lvars_unified {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/ground_mixed",
                "a ground header makes the anonymous table a membership test, but another header declares a fresh column: a membership test has no columns to declare".to_string(),
                "unify every lvar header with a column in scope, or remove the ground header",
            ));
        }
        if alias.is_some() {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/witness_alias",
                "a ground header makes the anonymous table a membership test, which exports no columns: an alias names nothing".to_string(),
                "drop the alias, or remove the ground header to make it a relation",
            ));
        }
    } else if !all_lvars_unified {
        // Plain form with fresh columns: a real relation.
        return Ok(None);
    } else if alias.is_some() {
        // Every header unifies: membership shape. Membership exports
        // no columns, so an alias names nothing — and accepted, the
        // alias would silently flip membership into a relational join
        // whose duplicate rows multiply matching outer rows. Naming
        // must never change cardinality.
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "resolution/anon/membership_alias",
            "an anonymous table whose every header unifies is a membership test, which exports no columns: an alias names nothing".to_string(),
            "drop the alias, or add a fresh column to make the anonymous table a relation",
        ));
    }

    // Membership shape confirmed. Build probe tuple from the headers.
    let mut probes: Vec<ast_resolved::DomainExpression> = Vec::with_capacity(headers.len());
    for header in headers {
        match header {
            ast_unresolved::DomainExpression::Literal { value, alias } => {
                probes.push(ast_resolved::DomainExpression::Literal {
                    value: value.clone(),
                    alias: alias.clone(),
                });
            }
            ast_unresolved::DomainExpression::Lvar {
                name, qualifier, ..
            } => {
                let bound = left_columns.iter().find(|col| match qualifier {
                    Some(q) => qualified_header_matches(col, name, q),
                    None => {
                        col.declared_bare
                            && delightql_types::SqlIdentifier::str_eq(col.name(), name)
                    }
                });
                let Some(col) = bound else {
                    unreachable!(
                        "membership header '{}' passed unification but binds no left column",
                        name
                    );
                };
                probes.push(ast_resolved::DomainExpression::Lvar {
                    name: col.name().into(),
                    qualifier: match col.qualifier() {
                        ast_resolved::TableName::Named(t) => Some(t.clone()),
                        ast_resolved::TableName::Fresh => None,
                    },
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    provenance: ast_resolved::PhaseBox::phantom(),
                });
            }
            _ => unreachable!("non-probe header survived membership shape check"),
        }
    }

    // Candidate tuples from the resolved rows.
    let rows = match resolved_right {
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
            rows,
            ..
        }) => rows,
        _ => return Ok(None),
    };
    for (i, row) in rows.iter().enumerate() {
        if row.values.len() != probes.len() {
            return Err(crate::error::DelightQLError::validation_error_categorized(
                "resolution/anon/membership_arity",
                format!(
                    "membership row {} has {} value(s) for {} header(s)",
                    i + 1,
                    row.values.len(),
                    probes.len()
                ),
                "every candidate row must match the probe's width",
            ));
        }
    }

    let value = if probes.len() == 1 {
        probes.pop().expect("one probe")
    } else {
        ast_resolved::DomainExpression::Tuple {
            elements: probes,
            alias: None,
        }
    };
    let set: Vec<ast_resolved::DomainExpression> = rows
        .iter()
        .map(|row| {
            if row.values.len() == 1 {
                row.values[0].clone()
            } else {
                ast_resolved::DomainExpression::Tuple {
                    elements: row.values.clone(),
                    alias: None,
                }
            }
        })
        .collect();

    Ok(Some(ast_resolved::BooleanExpression::In {
        value: Box::new(value),
        set,
        negated,
    }))
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
                .find(|col| delightql_types::SqlIdentifier::str_eq(col.name(), col_name))
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
