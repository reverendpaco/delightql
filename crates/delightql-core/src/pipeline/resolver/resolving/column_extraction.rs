// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};
use delightql_types::SqlIdentifier;

/// Check if a domain expression contains any qualified references
pub(in crate::pipeline::resolver) fn expr_has_qualified_ref(
    expr: &ast_resolved::DomainExpression,
) -> bool {
    match expr {
        ast_resolved::DomainExpression::Lvar { qualifier, .. } => qualifier.is_some(),
        ast_resolved::DomainExpression::Function(func) => {
            // Recursively check function arguments
            match func {
                ast_resolved::FunctionExpression::Regular { arguments, .. }
                | ast_resolved::FunctionExpression::Bracket { arguments, .. }
                | ast_resolved::FunctionExpression::Curried { arguments, .. } => {
                    arguments.iter().any(expr_has_qualified_ref)
                }
                ast_resolved::FunctionExpression::HigherOrder {
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    curried_arguments.iter().any(expr_has_qualified_ref)
                        || regular_arguments.iter().any(expr_has_qualified_ref)
                }
                ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                    expr_has_qualified_ref(left) || expr_has_qualified_ref(right)
                }
                ast_resolved::FunctionExpression::Lambda { body, .. } => {
                    expr_has_qualified_ref(body)
                }
                ast_resolved::FunctionExpression::StringTemplate { .. } => {
                    // StringTemplate should have been expanded to concat by resolver
                    false
                }
                ast_resolved::FunctionExpression::CaseExpression { .. } => {
                    // CaseExpression not yet implemented in resolver
                    false
                }
                ast_resolved::FunctionExpression::Curly { .. } => false,
                ast_resolved::FunctionExpression::Array { .. } => false,
                ast_resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
                ast_resolved::FunctionExpression::Window {
                    arguments,
                    partition_by,
                    order_by,
                    ..
                } => {
                    arguments.iter().any(expr_has_qualified_ref)
                        || partition_by.iter().any(expr_has_qualified_ref)
                        || order_by
                            .iter()
                            .any(|spec| expr_has_qualified_ref(&spec.column))
                }
                ast_resolved::FunctionExpression::JsonPath { source, .. } => {
                    // JsonPath: check if source has qualified references
                    expr_has_qualified_ref(source)
                }
            }
        }
        ast_resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            expr_has_qualified_ref(value)
                || transforms.iter().any(|(_, t)| match t {
                    ast_resolved::FunctionExpression::Regular { arguments, .. }
                    | ast_resolved::FunctionExpression::Curried { arguments, .. }
                    | ast_resolved::FunctionExpression::Bracket { arguments, .. } => {
                        arguments.iter().any(expr_has_qualified_ref)
                    }
                    ast_resolved::FunctionExpression::HigherOrder {
                        curried_arguments,
                        regular_arguments,
                        ..
                    } => {
                        curried_arguments.iter().any(expr_has_qualified_ref)
                            || regular_arguments.iter().any(expr_has_qualified_ref)
                    }
                    ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                        expr_has_qualified_ref(left) || expr_has_qualified_ref(right)
                    }
                    ast_resolved::FunctionExpression::Lambda { body, .. } => {
                        expr_has_qualified_ref(body)
                    }
                    ast_resolved::FunctionExpression::StringTemplate { .. } => {
                        // StringTemplate should have been expanded to concat by resolver
                        false
                    }
                    ast_resolved::FunctionExpression::CaseExpression { .. } => {
                        // TODO: Check CASE arms for qualified refs
                        false
                    }
                    ast_resolved::FunctionExpression::Curly { .. } => false,
                    ast_resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
                    ast_resolved::FunctionExpression::Window {
                        arguments,
                        partition_by,
                        order_by,
                        ..
                    } => {
                        arguments.iter().any(expr_has_qualified_ref)
                            || partition_by.iter().any(expr_has_qualified_ref)
                            || order_by
                                .iter()
                                .any(|spec| expr_has_qualified_ref(&spec.column))
                    }
                    _ => unimplemented!("JsonPath not yet implemented in this phase"),
                })
        }
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => {
            expr_has_qualified_ref(inner)
        }
        // Projection expressions: Glob can be qualified (u.*), others are leaves
        ast_resolved::DomainExpression::Projection(proj) => match proj {
            ProjectionExpr::Glob { qualifier, .. } => qualifier.is_some(),
            _ => false,
        },
        // Tuple: recurse into elements (multi-column expressions can contain qualified refs)
        ast_resolved::DomainExpression::Tuple { elements, .. } => {
            elements.iter().any(expr_has_qualified_ref)
        }
        // PivotOf: recurse into value and key columns
        ast_resolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => expr_has_qualified_ref(value_column) || expr_has_qualified_ref(pivot_key),
        // Predicate: boolean expressions may contain qualified refs, but we'd need a
        // separate walker for BooleanExpression. Conservative false — rare in aggregates.
        ast_resolved::DomainExpression::Predicate { .. } => false,
        // Leaf expressions: no table qualifiers possible.
        ast_resolved::DomainExpression::Literal { .. }
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::ColumnOrdinal(_) => false,
        // ScalarSubquery: inner scope — qualified refs inside don't count as outer qualified.
        ast_resolved::DomainExpression::ScalarSubquery { .. } => false,
    }
}

/// The single distinct source column (name, qualifier) feeding a
/// value-transforming resolved expression, if there is exactly one.
/// Opaque or grain-changing subtrees poison the answer to None.
///
/// Resolver twin of the transformer's `single_source_column`
/// (transformer_v4/builder/mod.rs). CAST v1 is a `Regular` function named
/// "cast" `(cast:(id, text))`, so its source is found by recursing the
/// Regular arm's arguments — the type-spelling literal contributes nothing.
fn single_source_lvar(
    expr: &ast_resolved::DomainExpression,
) -> Option<(&SqlIdentifier, Option<&SqlIdentifier>)> {
    let mut refs: Vec<(&SqlIdentifier, Option<&SqlIdentifier>)> = Vec::new();
    // None from the walk = a poisoning (opaque) subtree.
    collect_source_lvars(expr, &mut refs)?;

    // Distinctness is by identifier value, not spelling: (SqlIdentifier,
    // Option<SqlIdentifier>)'s Eq already folds ASCII case (STRING-FLOOR
    // Tier 3 — no ad hoc case ops at the site).
    let mut seen: Vec<(SqlIdentifier, Option<SqlIdentifier>)> = Vec::new();
    let mut unique: Option<(&SqlIdentifier, Option<&SqlIdentifier>)> = None;
    for (name, qual) in refs {
        let key = (name.clone(), qual.cloned());
        if !seen.contains(&key) {
            seen.push(key);
            unique = Some((name, qual));
        }
    }
    match seen.len() {
        1 => unique,
        _ => None,
    }
}

/// Walk `expr`, pushing every column reference into `out`. Returns None the
/// moment an opaque subtree is hit (it may reference columns the walk cannot
/// see). Exhaustive by construction: a future `DomainExpression` variant must
/// decide here whether it is transparent, recursive, or poisoning.
fn collect_source_lvars<'a>(
    expr: &'a ast_resolved::DomainExpression,
    out: &mut Vec<(&'a SqlIdentifier, Option<&'a SqlIdentifier>)>,
) -> Option<()> {
    match expr {
        ast_resolved::DomainExpression::Lvar {
            name, qualifier, ..
        } => {
            out.push((name, qualifier.as_ref()));
            Some(())
        }
        ast_resolved::DomainExpression::Literal { .. } => Some(()),
        // Parens are notation, not value transformation — see through them.
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => {
            collect_source_lvars(inner, out)
        }
        // Scalar function forms compute over their visible args — recurse (the
        // distinctness guard folds multi-source down to None). Cast rides the
        // Regular arm.
        ast_resolved::DomainExpression::Function(func) => collect_fn_source_lvars(func, out),
        // Boolean-expression-as-value: recurse pure scalar boolean ops; the
        // boolean walker poisons the EXISTS-like/relational ones.
        ast_resolved::DomainExpression::Predicate { expr, .. } => {
            collect_bool_source_lvars(expr, out)
        }
        // A tuple constructor's elements are its visible sources.
        ast_resolved::DomainExpression::Tuple { elements, .. } => {
            for element in elements {
                collect_source_lvars(element, out)?;
            }
            Some(())
        }
        // Opaque or grain/relation-crossing: these can reference columns the
        // walk cannot enumerate (substitution holes, @ placeholders, nested
        // pipes/subqueries), or expand to multiple columns (projection/pivot).
        // A single-source claim over them would be unsound — poison to None.
        ast_resolved::DomainExpression::Projection(_)
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::ColumnOrdinal(_)
        | ast_resolved::DomainExpression::PipedExpression { .. }
        | ast_resolved::DomainExpression::ScalarSubquery { .. }
        | ast_resolved::DomainExpression::PivotOf { .. } => None,
    }
}

/// Walk a resolved `FunctionExpression` for its source columns. Scalar forms
/// that compute over visible args recurse; forms that carry invisible refs
/// (CFE substitution, `@` lambda body), explode to multiple columns
/// (array/tree-group constructors), or cross grain (window) poison to None.
fn collect_fn_source_lvars<'a>(
    func: &'a ast_resolved::FunctionExpression,
    out: &mut Vec<(&'a SqlIdentifier, Option<&'a SqlIdentifier>)>,
) -> Option<()> {
    match func {
        ast_resolved::FunctionExpression::Regular { arguments, .. }
        | ast_resolved::FunctionExpression::Curried { arguments, .. }
        | ast_resolved::FunctionExpression::Bracket { arguments, .. } => {
            for arg in arguments {
                collect_source_lvars(arg, out)?;
            }
            Some(())
        }
        ast_resolved::FunctionExpression::Infix { left, right, .. } => {
            collect_source_lvars(left, out)?;
            collect_source_lvars(right, out)
        }
        ast_resolved::FunctionExpression::JsonPath { source, path, .. } => {
            collect_source_lvars(source, out)?;
            collect_source_lvars(path, out)
        }
        ast_resolved::FunctionExpression::StringTemplate { parts, .. } => {
            for part in parts {
                if let ast_resolved::StringTemplatePart::Interpolation(inner) = part {
                    collect_source_lvars(inner, out)?;
                }
            }
            Some(())
        }
        ast_resolved::FunctionExpression::CaseExpression { arms, .. } => {
            for arm in arms {
                collect_case_arm_source_lvars(arm, out)?;
            }
            Some(())
        }
        // Poison: HigherOrder/Lambda carry invisible refs (CFE substitution,
        // `@` piped value); Curly/Array/MetadataTreeGroup explode into many
        // columns (tree-group/destructure); Window crosses grain (mirrors the
        // transformer's WindowFunction poison).
        ast_resolved::FunctionExpression::HigherOrder { .. }
        | ast_resolved::FunctionExpression::Lambda { .. }
        | ast_resolved::FunctionExpression::Curly { .. }
        | ast_resolved::FunctionExpression::Array { .. }
        | ast_resolved::FunctionExpression::MetadataTreeGroup { .. }
        | ast_resolved::FunctionExpression::Window { .. } => None,
    }
}

/// Walk a resolved `BooleanExpression` used as a domain value. Pure scalar
/// boolean ops recurse into their operands; relational forms (EXISTS, IN
/// subquery, USING, correlation, sigma constraints) poison to None.
fn collect_bool_source_lvars<'a>(
    bexpr: &'a ast_resolved::BooleanExpression,
    out: &mut Vec<(&'a SqlIdentifier, Option<&'a SqlIdentifier>)>,
) -> Option<()> {
    use crate::pipeline::asts::core::BooleanExpression as BExpr;
    match bexpr {
        BExpr::Comparison { left, right, .. } => {
            collect_source_lvars(left, out)?;
            collect_source_lvars(right, out)
        }
        BExpr::And { left, right } | BExpr::Or { left, right } => {
            collect_bool_source_lvars(left, out)?;
            collect_bool_source_lvars(right, out)
        }
        BExpr::Not { expr } => collect_bool_source_lvars(expr, out),
        BExpr::In { value, set, .. } => {
            collect_source_lvars(value, out)?;
            for item in set {
                collect_source_lvars(item, out)?;
            }
            Some(())
        }
        BExpr::BooleanLiteral { .. } => Some(()),
        // Poison: these reference columns the walk cannot enumerate (subquery
        // scopes, USING/correlation over full tuples, sigma constraints).
        BExpr::InnerExists { .. }
        | BExpr::InRelational { .. }
        | BExpr::Using { .. }
        | BExpr::Sigma { .. }
        | BExpr::GlobCorrelation { .. }
        | BExpr::OrdinalGlobCorrelation { .. } => None,
    }
}

/// Walk a resolved `CaseArm` for its source columns. Test/result domain
/// sub-expressions and searched conditions recurse; the arm's literal value
/// contributes nothing.
fn collect_case_arm_source_lvars<'a>(
    arm: &'a crate::pipeline::asts::core::expressions::CaseArm<
        crate::pipeline::asts::core::Resolved,
    >,
    out: &mut Vec<(&'a SqlIdentifier, Option<&'a SqlIdentifier>)>,
) -> Option<()> {
    use crate::pipeline::asts::core::expressions::CaseArm;
    match arm {
        CaseArm::Simple {
            test_expr, result, ..
        } => {
            collect_source_lvars(test_expr, out)?;
            collect_source_lvars(result, out)
        }
        CaseArm::CurriedSimple { result, .. } => collect_source_lvars(result, out),
        CaseArm::Searched { condition, result } => {
            collect_bool_source_lvars(condition, out)?;
            collect_source_lvars(result, out)
        }
        CaseArm::Default { result } => collect_source_lvars(result, out),
    }
}

/// The transform's diagnostic spelling for a Derived identity's `via` field
/// (never rendered). Parens unwrap to the inner expression's kind, mirroring
/// the transformer twin. Cast is a `Regular` function named "cast", so the
/// function-name path spells it "cast" without a special case.
fn derived_via_label(expr: &ast_resolved::DomainExpression) -> String {
    match expr {
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => derived_via_label(inner),
        ast_resolved::DomainExpression::Function(func) => match func {
            ast_resolved::FunctionExpression::Regular { name, .. }
            | ast_resolved::FunctionExpression::Curried { name, .. }
            | ast_resolved::FunctionExpression::HigherOrder { name, .. }
            | ast_resolved::FunctionExpression::Window { name, .. } => name.to_string(),
            ast_resolved::FunctionExpression::Infix { operator, .. } => operator.clone(),
            ast_resolved::FunctionExpression::Bracket { .. } => "bracket".to_string(),
            ast_resolved::FunctionExpression::JsonPath { .. } => "json_path".to_string(),
            ast_resolved::FunctionExpression::CaseExpression { .. } => "case".to_string(),
            ast_resolved::FunctionExpression::StringTemplate { .. } => "concat".to_string(),
            _ => "derived".to_string(),
        },
        ast_resolved::DomainExpression::Predicate { .. } => "predicate".to_string(),
        ast_resolved::DomainExpression::Tuple { .. } => "tuple".to_string(),
        _ => "derived".to_string(),
    }
}

/// The two-step input-column lookup: prefer a (name, scope) match, then fall
/// back to name-only. Factored out of the Lvar arm so the direct-reference
/// path and the cast-lineage derived path cannot diverge.
fn find_source_column<'a>(
    name: &SqlIdentifier,
    qualifier: Option<&SqlIdentifier>,
    input_columns: &'a [ast_resolved::ColumnMetadata],
) -> Option<&'a ast_resolved::ColumnMetadata> {
    input_columns
        .iter()
        .find(|c| {
            if !delightql_types::SqlIdentifier::str_eq(c.name(), name) {
                return false;
            }
            if let Some(qual) = qualifier {
                // If qualifier is specified, prefer matching scope
                matches!(c.qualifier(), ast_resolved::TableName::Named(t) if t == qual)
            } else {
                true
            }
        })
        .or_else(|| {
            // Fallback: match by name only (for cases where qualifier doesn't match a scope)
            input_columns
                .iter()
                .find(|c| delightql_types::SqlIdentifier::str_eq(c.name(), name))
        })
}

/// Derived provenance for a computed-value output column under the cast-lineage
/// ruling: if EXACTLY ONE distinct source column feeds `expr` and it is present
/// in `input_columns`, the output CONTINUES that column's identity stack, pushed
/// with `IdentityContext::Derived` (the value changed — lineage, not a
/// usable-as-is synonym). Returns None for multi/zero-source, opaque, or
/// absent-source expressions — the caller then builds an honest-Fresh column.
///
/// CRITICAL: this inherits ONLY the provenance (`info`). The caller keeps every
/// other ColumnMetadata field (declared_type, interior_schema, position,
/// has_user_name) at its fresh-path value — cloning the source's declared_type
/// onto a cast/function output would lie about the output's type and can corrupt
/// typed union pads.
fn try_derive_info(
    expr: &ast_resolved::DomainExpression,
    input_columns: &[ast_resolved::ColumnMetadata],
    final_name: &SqlIdentifier,
) -> Option<ast_resolved::ColumnProvenance> {
    let (src_name, src_qual) = single_source_lvar(expr)?;
    let source_col = find_source_column(src_name, src_qual, input_columns)?;
    let mut info = source_col.info.clone();
    info.push_identity(ast_resolved::ColumnIdentity {
        name: final_name.clone(),
        context: ast_resolved::IdentityContext::Derived {
            previous_name: source_col.name().to_string(),
            via: derived_via_label(expr),
        },
        phase: ast_resolved::TransformationPhase::Resolved,
        table_qualifier: source_col.qualifier().clone(),
    });
    Some(info)
}

/// Unwrap pure-notation paren layers: a `Parenthesized` that carries no alias
/// of its own is notation, not a rename site, so we see through it (mirrors
/// `collect_source_lvars`/`derived_via_label`). A paren WITH an alias is a
/// naming site and stops the unwrap. Lets `((id)) as x` reach the bare `id`.
fn strip_notation_parens(
    expr: &ast_resolved::DomainExpression,
) -> &ast_resolved::DomainExpression {
    match expr {
        ast_resolved::DomainExpression::Parenthesized { inner, alias: None } => {
            strip_notation_parens(inner)
        }
        _ => expr,
    }
}

/// Extract the column that a domain expression provides (if any).
/// This is the inductive solution - handles all domain expression types uniformly.
///
/// # Arguments
/// * `expr` - The expression to extract a column from
/// * `input_columns` - Available input columns
/// * `position` - The position of this expression in the projection (for generating unique names)
pub(in crate::pipeline::resolver) fn extract_provided_column_from_domain_expr(
    expr: &ast_resolved::DomainExpression,
    input_columns: &[ast_resolved::ColumnMetadata],
    position: usize,
) -> Option<ast_resolved::ColumnMetadata> {
    match expr {
        ast_resolved::DomainExpression::Lvar {
            name,
            alias,
            qualifier,
            ..
        } => {
            // An Lvar provides a column - either with its original name or with an alias.
            // When a qualifier is present, prefer matching by both name AND table scope
            // to avoid picking the wrong column in multi-table contexts (e.g., u.id vs o.id).
            if let Some(col) = find_source_column(name, qualifier.as_ref(), input_columns) {
                let mut output_col = col.clone();

                // Preserve the qualification status from the resolved expression.
                // Qualifiers here come from projection expansion (glob, pattern, etc.)
                // which is resolver-generated, not user-written.
                if qualifier.is_some() {
                    output_col.info = output_col.info.with_updated_qualification(
                        crate::pipeline::asts::core::QualificationSource::Resolver,
                    );
                }

                if let Some(alias_name) = alias {
                    // If there's an alias, the expression provides a column with that alias name
                    output_col.info = output_col.info.with_alias(alias_name.clone());
                }
                // Projection establishes column identity: the name is now known,
                // even if the source was a passthrough table with unnamed columns.
                output_col.has_user_name = true;
                Some(output_col)
            } else {
                // Column not found in input — either passthrough table (no schema)
                // or a new computed column. The user explicitly wrote this name in a
                // projection, so the column is user-named.
                let final_name = alias.as_ref().unwrap_or(name);
                // Honest Fresh: column absent from input schema (passthrough or new
                // computed column) — no source table is available here.
                let mut info = ast_resolved::ColumnProvenance::from_column(final_name.clone());

                // Even for computed columns, preserve qualification status
                if qualifier.is_some() {
                    info = info.with_updated_qualification(
                        crate::pipeline::asts::core::QualificationSource::Resolver,
                    );
                }

                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::TableName::Fresh,
                    None,
                    true, // Lvar in projection = user explicitly named this column
                ))
            }
        }
        ast_resolved::DomainExpression::Function(func) => {
            // Functions with aliases provide new columns
            let (alias, has_qualified_args) = match func {
                ast_resolved::FunctionExpression::Regular {
                    alias, arguments, ..
                } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Bracket {
                    alias, arguments, ..
                } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Infix {
                    alias, left, right, ..
                } => {
                    let qualified = expr_has_qualified_ref(left) || expr_has_qualified_ref(right);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Curried { arguments, .. } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (&None, qualified)
                }
                ast_resolved::FunctionExpression::HigherOrder {
                    alias,
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    let qualified = curried_arguments.iter().any(expr_has_qualified_ref)
                        || regular_arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Lambda { body, alias, .. } => {
                    let qualified = expr_has_qualified_ref(body);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::StringTemplate { .. } => {
                    // StringTemplate should have been expanded to concat by resolver
                    (&None, false)
                }
                ast_resolved::FunctionExpression::CaseExpression { alias, .. } => {
                    // CaseExpression - check if it has an alias
                    (alias, false)
                }
                ast_resolved::FunctionExpression::Curly { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::Array { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::MetadataTreeGroup { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::Window {
                    alias,
                    arguments,
                    partition_by,
                    order_by,
                    ..
                } => {
                    // Window function - check for qualified refs in all expressions
                    let qualified = arguments.iter().any(expr_has_qualified_ref)
                        || partition_by.iter().any(expr_has_qualified_ref)
                        || order_by
                            .iter()
                            .any(|spec| expr_has_qualified_ref(&spec.column));
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::JsonPath { alias, source, .. } => {
                    // JsonPath - check if source has qualified refs
                    let qualified = expr_has_qualified_ref(source);
                    (alias, qualified)
                }
            };

            if let Some(alias_name) = alias {
                // Function with alias creates a new column. Cast-lineage: when
                // exactly one source column feeds the function, the output
                // CONTINUES that column's identity stack (Derived); otherwise it
                // begins honestly fresh. Only the provenance is inherited — every
                // other field below keeps its fresh-path value.
                let mut info = try_derive_info(expr, input_columns, alias_name).unwrap_or_else(|| {
                    // Honest Fresh: a function result is a computed value, not a column
                    // of any source table.
                    ast_resolved::ColumnProvenance::from_column(alias_name.clone())
                });

                // If function arguments contain qualified references, propagate as resolver-qualified
                if has_qualified_args {
                    info = info.with_updated_qualification(
                        crate::pipeline::asts::core::QualificationSource::Resolver,
                    );
                }

                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::TableName::Fresh,
                    None,
                    alias.is_some(), // has_user_name true only if alias provided
                ))
            } else {
                // Function without alias still provides a column with a generated name
                // Use the naming utility to generate a unique name based on position
                let col_name: SqlIdentifier =
                    crate::pipeline::naming::generate_function_column_name(func, position).into();

                // Cast-lineage: a single-source function continues the source
                // column's identity stack even under a generated name; else
                // honestly fresh. Only the provenance is inherited.
                let mut info = try_derive_info(expr, input_columns, &col_name).unwrap_or_else(|| {
                    // Honest Fresh: generated name for a computed function result — no
                    // source table.
                    ast_resolved::ColumnProvenance::from_column(col_name.clone())
                });

                // Propagate qualification from arguments as resolver-qualified
                if has_qualified_args {
                    info = info.with_updated_qualification(
                        crate::pipeline::asts::core::QualificationSource::Resolver,
                    );
                }

                // ALWAYS create a column for function expressions, even without alias
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::TableName::Fresh,
                    None,
                    false, // No alias in this branch, so has_user_name is false
                ))
            }
        }
        ast_resolved::DomainExpression::Literal { alias, value: _ } => {
            // Literals provide columns - use alias if provided, otherwise generate name
            let col_name: SqlIdentifier = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Use naming utility for consistency (though literals usually have their value as name)
                format!("literal_{}", position + 1).into()
            };

            // Honest Fresh: a literal value has no source table.
            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::TableName::Fresh,
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Predicate { alias, .. } => {
            // Predicates provide boolean columns - use alias if provided
            let col_name = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Generate a default name for the predicate column
                "predicate".into()
            };

            // Honest Fresh: a predicate yields a computed boolean, not a table column.
            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::TableName::Fresh,
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Projection(proj) => match proj {
            // Globs don't provide individual columns - they expand to multiple columns
            // This needs to be handled separately by the operator
            ProjectionExpr::Glob { .. } => None,
            // These should have been resolved/expanded to Lvars by now
            ProjectionExpr::ColumnRange(_) | ProjectionExpr::Pattern { .. } => None,
            // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral handling
            // JsonPathLiteral provides a column like a literal value
            ProjectionExpr::JsonPathLiteral { alias, .. } => {
                let col_name: SqlIdentifier = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    // Generate a default name for path literal
                    format!("path_literal_{}", position + 1).into()
                };

                // Honest Fresh: a path literal is a computed value with no source table.
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_column(col_name),
                    ast_resolved::TableName::Fresh,
                    None,
                    alias.is_some(), // has_user_name true only if alias provided
                ))
            }
        },
        ast_resolved::DomainExpression::NonUnifiyingUnderscore => {
            // Placeholders don't provide columns
            None
        }
        ast_resolved::DomainExpression::ColumnOrdinal(_) => {
            // These should have been resolved/expanded to Lvars by now
            None
        }
        ast_resolved::DomainExpression::ScalarSubquery { alias, .. } => {
            // Scalar subquery returns a single value - treat like a function
            let col_name = alias
                .clone()
                .unwrap_or_else(|| format!("scalar_subquery_{}", position).into());

            // Honest Fresh: a scalar subquery returns one computed value, not a
            // column drawn from a single source table.
            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::TableName::Fresh,
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Substitution(sub) => match sub {
            SubstitutionExpr::Parameter { name, alias }
            | SubstitutionExpr::CurriedParameter { name, alias } => {
                // Parameters/curried parameters provide columns (for CFE/HOCFE bodies)
                let col_name = alias.as_ref().unwrap_or(name).clone();
                // Honest Fresh: a CFE/HOCFE parameter hole has no source table.
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_column(col_name),
                    ast_resolved::TableName::Fresh,
                    None,
                    alias.is_some(),
                ))
            }
            SubstitutionExpr::ContextParameter { .. } => {
                // ContextParameter should never exist in resolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                None
            }
            SubstitutionExpr::ContextMarker => {
                // ContextMarker (..) should only appear in function call arguments
                // It doesn't provide columns itself
                None
            }
        },
        ast_resolved::DomainExpression::ValuePlaceholder { alias } => {
            // @ placeholder provides a column for the value that will be substituted
            let col_name = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Generate a default name for the placeholder
                "value".into()
            };

            // Honest Fresh: `@` is a substitution placeholder — no source table.
            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::TableName::Fresh,
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::PipedExpression { alias, .. } => {
            // Piped expression provides a column with the result of the pipeline
            let col_name = alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "piped_result".into());

            // Honest Fresh: a nested piped expression yields one scalar result with
            // no single source table.
            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::TableName::Fresh,
                None,
                alias.is_some(),
            ))
        }
        ast_resolved::DomainExpression::Parenthesized { inner, alias } => {
            // Parenthesized expression - check if inner expression provides a column
            // If it has an alias, use that; otherwise use the inner expression's column
            if let Some(alias_name) = alias {
                // Parens are pure notation (cast-lineage ruling). Split by inner:
                //
                //   (id) as x  — a paren-wrapped BARE column reference is a RENAME,
                //   identical to bare `id as x` (the Lvar arm): the source column's
                //   provenance gains a UserAlias, NOT a Derived push, because the
                //   underlying column is still usable as-is. Multi-layer `((id)) as
                //   x` unwraps through alias-less paren layers to the same bare Lvar.
                //
                //   (a + 1) as x — a value-transforming inner is a DERIVATION; it
                //   keeps Derived semantics via try_derive_info.
                //
                // NOTE (honest scope): only the identity STACK is mirrored to bare
                // rename; the outer ColumnMetadata.table stays Fresh as this arm
                // always built it (the bare-rename difference is a separate field,
                // not the identity treatment the ruling distinguishes).
                let info = match strip_notation_parens(inner) {
                    ast_resolved::DomainExpression::Lvar {
                        name,
                        qualifier,
                        alias: None,
                        ..
                    } => match find_source_column(name, qualifier.as_ref(), input_columns) {
                        Some(src) => {
                            let mut prov = src.info.clone();
                            if qualifier.is_some() {
                                prov = prov.with_updated_qualification(
                                    crate::pipeline::asts::core::QualificationSource::Resolver,
                                );
                            }
                            prov.with_alias(alias_name.clone())
                        }
                        // Bare name absent from input (passthrough / new column):
                        // honest Fresh named by the alias — matches the Lvar arm.
                        None => ast_resolved::ColumnProvenance::from_column(alias_name.clone()),
                    },
                    _ => try_derive_info(inner, input_columns, alias_name).unwrap_or_else(|| {
                        ast_resolved::ColumnProvenance::from_column(alias_name.clone())
                    }),
                };
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::TableName::Fresh,
                    None,
                    true,
                ))
            } else {
                extract_provided_column_from_domain_expr(inner, input_columns, position)
            }
        }
        ast_resolved::DomainExpression::Tuple { .. } => {
            // Tuples don't provide a single column - they should have been desugared
            None
        }

        // Pivot expressions expand to multiple columns, handled at modulo level
        ast_resolved::DomainExpression::PivotOf { .. } => None,
    }
}

#[cfg(test)]
mod cast_lineage_tests {
    use super::*;
    use crate::pipeline::asts::core::provenance::{IdentityContext, QualificationSource};
    use crate::pipeline::asts::core::LiteralValue;

    /// Input column drawn from a real source table, optionally declared-typed.
    fn src_col(name: &str, qual: &str, declared_type: Option<&str>) -> ast_resolved::ColumnMetadata {
        let info = ast_resolved::ColumnProvenance::from_table_column(
            name,
            ast_resolved::TableName::Named(qual.into()),
            QualificationSource::User,
        );
        ast_resolved::ColumnMetadata::new(info, ast_resolved::TableName::Named(qual.into()), Some(1))
            .with_declared_type(declared_type.map(|s| s.to_string()))
    }

    fn lvar(name: &str) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Lvar {
            name: name.into(),
            qualifier: None,
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        }
    }

    /// `func:(args) as alias` — a Regular function. Cast is this with name "cast".
    fn regular(
        name: &str,
        args: Vec<ast_resolved::DomainExpression>,
        alias: &str,
    ) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Regular {
            name: name.into(),
            namespace: None,
            arguments: args,
            alias: Some(alias.into()),
            conditioned_on: None,
        })
    }

    fn literal_str(s: &str) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Literal {
            value: LiteralValue::String(s.to_string()),
            alias: None,
        }
    }

    /// A bare `name as alias` rename — an Lvar carrying its own alias.
    fn lvar_aliased(name: &str, alias: &str) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Lvar {
            name: name.into(),
            qualifier: None,
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: Some(alias.into()),
            provenance: ast_resolved::PhaseBox::phantom(),
        }
    }

    /// `(inner) as alias` — an aliased parenthesized wrapper.
    fn paren(inner: ast_resolved::DomainExpression, alias: &str) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Parenthesized {
            inner: Box::new(inner),
            alias: Some(alias.into()),
        }
    }

    /// `(inner)` — a bare, alias-less parenthesized wrapper (pure notation).
    fn paren_noalias(inner: ast_resolved::DomainExpression) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Parenthesized {
            inner: Box::new(inner),
            alias: None,
        }
    }

    /// `func:(args)` — a Regular function WITHOUT its own alias (for paren inners).
    fn regular_noalias(
        name: &str,
        args: Vec<ast_resolved::DomainExpression>,
    ) -> ast_resolved::DomainExpression {
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Regular {
            name: name.into(),
            namespace: None,
            arguments: args,
            alias: None,
            conditioned_on: None,
        })
    }

    /// The top identity's Derived context, if the output inherited a stack.
    fn top_derived(col: &ast_resolved::ColumnMetadata) -> Option<(String, String)> {
        match col.info.identity_stack().first().map(|id| &id.context) {
            Some(IdentityContext::Derived { previous_name, via }) => {
                Some((previous_name.clone(), via.clone()))
            }
            _ => None,
        }
    }

    /// The top identity's UserAlias `previous_name` (a RENAME), if that is the top.
    fn top_user_alias(col: &ast_resolved::ColumnMetadata) -> Option<String> {
        match col.info.identity_stack().first().map(|id| &id.context) {
            Some(IdentityContext::UserAlias { previous_name }) => Some(previous_name.clone()),
            _ => None,
        }
    }

    #[test]
    fn function_of_one_column_inherits() {
        let cols = vec![src_col("id", "users", None)];
        // upper:(id) as label — single source `id`.
        let expr = regular("upper", vec![lvar("id")], "label");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("function provides a column");
        // Stack is CONTINUED: the source's OriginalTable underneath a Derived top.
        assert!(out.info.identity_stack().len() > 1, "stack should be continued");
        let (previous_name, via) = top_derived(&out).expect("top is Derived");
        assert_eq!(previous_name, "id");
        assert_eq!(via, "upper");
        assert_eq!(out.name(), "label");
    }

    #[test]
    fn cast_form_inherits_via_cast() {
        let cols = vec![src_col("id", "users", None)];
        // cast:(id, text) as user_id — Regular fn named "cast"; the type literal
        // contributes no source column.
        let expr = regular("cast", vec![lvar("id"), literal_str("text")], "user_id");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("cast provides a column");
        let (previous_name, via) = top_derived(&out).expect("top is Derived");
        assert_eq!(previous_name, "id");
        assert_eq!(via, "cast");
    }

    #[test]
    fn two_source_function_stays_fresh() {
        let cols = vec![src_col("first", "users", None), src_col("last", "users", None)];
        // concat:(first, last) as name — two distinct sources → honest fresh.
        let expr = regular("concat", vec![lvar("first"), lvar("last")], "name");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("function provides a column");
        assert!(top_derived(&out).is_none(), "multi-source must not inherit");
        assert_eq!(out.info.identity_stack().len(), 1, "fresh single-entry stack");
    }

    #[test]
    fn literal_only_stays_fresh() {
        let cols = vec![src_col("id", "users", None)];
        // abs:(1) as one — zero source columns → honest fresh.
        let expr = regular("abs", vec![literal_str("1")], "one");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("function provides a column");
        assert!(top_derived(&out).is_none(), "zero-source must not inherit");
    }

    #[test]
    fn declared_type_not_inherited() {
        // Source carries a declared_type; the derived output must NOT inherit it —
        // a cast's output type is not the source's (would corrupt typed union pads).
        let cols = vec![src_col("id", "users", Some("INTEGER"))];
        let expr = regular("cast", vec![lvar("id"), literal_str("text")], "user_id");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("cast provides a column");
        // Provenance WAS inherited (Derived top)...
        assert!(top_derived(&out).is_some(), "provenance inherited");
        // ...but declared_type keeps the fresh-path value (None), not "INTEGER".
        assert_eq!(out.declared_type, None, "declared_type must not be inherited");
    }

    #[test]
    fn paren_rename_is_rename_not_derived() {
        // `(id) as x` — a paren-wrapped BARE column reference. Parens are pure
        // notation (cast-lineage ruling), so this is a RENAME, identical to bare
        // `id as x`: the identity stack gains a UserAlias, NOT a Derived push.
        // Today's code routes it through try_derive_info → Derived{via:"derived"}
        // — this pins the fix.
        let cols = vec![src_col("id", "users", None)];
        let expr = paren(lvar("id"), "x");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("paren provides a column");
        assert!(top_derived(&out).is_none(), "(id) as x must NOT be Derived");
        assert_eq!(
            top_user_alias(&out).as_deref(),
            Some("id"),
            "(id) as x top must be a UserAlias rename of `id`"
        );
        // Stack CONTINUED: UserAlias over the source's OriginalTable (same shape
        // as bare `id as x`).
        assert!(
            out.info.identity_stack().len() > 1,
            "stack should be continued from the source"
        );
        assert_eq!(out.name(), "x");
    }

    #[test]
    fn paren_rename_matches_bare_rename_identity() {
        // The identity treatment of `(id) as x` must equal that of bare `id as x`.
        let cols = vec![src_col("id", "users", None)];
        let bare = extract_provided_column_from_domain_expr(&lvar_aliased("id", "x"), &cols, 0)
            .expect("bare rename provides a column");
        let parened = extract_provided_column_from_domain_expr(&paren(lvar("id"), "x"), &cols, 0)
            .expect("paren rename provides a column");
        assert_eq!(
            bare.info.identity_stack(),
            parened.info.identity_stack(),
            "paren-rename identity stack must mirror bare-rename's"
        );
    }

    #[test]
    fn double_paren_rename_is_rename() {
        // `((id)) as x` — multi-layer parens are still pure notation, still a
        // rename. strip_notation_parens unwraps alias-less layers to the bare Lvar.
        let cols = vec![src_col("id", "users", None)];
        let expr = paren(paren_noalias(lvar("id")), "x");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("double-paren provides a column");
        assert!(top_derived(&out).is_none(), "((id)) as x must NOT be Derived");
        assert_eq!(top_user_alias(&out).as_deref(), Some("id"), "still a rename");
    }

    #[test]
    fn paren_of_expression_keeps_derived() {
        // `(add:(id, 1)) as x` — a value-transforming inner is NOT a bare rename;
        // it must KEEP Derived semantics (only the bare-Lvar-in-parens case flips).
        let cols = vec![src_col("id", "users", None)];
        let expr = paren(regular_noalias("add", vec![lvar("id"), literal_str("1")]), "x");
        let out = extract_provided_column_from_domain_expr(&expr, &cols, 0)
            .expect("paren of expr provides a column");
        assert!(
            top_user_alias(&out).is_none(),
            "value-transforming inner must NOT be a rename"
        );
        let (previous_name, via) = top_derived(&out).expect("top is Derived");
        assert_eq!(previous_name, "id");
        assert_eq!(via, "add");
    }
}
