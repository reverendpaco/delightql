// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relational lowering: `r_lower_*` handlers.
//!
//! Each function lowers one AST node kind into builder operations.
//! `r_lower_*` functions take and return builders — they are the relational
//! algebra level of the transformation. Every function in this module starts
//! with `r_lower_` — no other prefixes, no exceptions.
//!
//! # Top-level handlers (called from `descend()`)
//!
//! - `r_lower_relation` — leaf: table, anonymous, TVF, inner relation
//! - `r_lower_filter` — WHERE predicate
//! - `r_lower_join` — JOIN two builders
//! - `r_lower_pipe` — left-fold pipe segments over a base builder
//! - `r_lower_set_op` — UNION / INTERSECT / EXCEPT
//!
//! # Pipe-segment handlers (called from `r_lower_pipe`)
//!
//! - `r_lower_projection` — SELECT list (`|> (cols)`)
//! - `r_lower_group_by` — GROUP BY + aggregates (`|> %(keys ~> aggs)`)
//! - `r_lower_order_by` — ORDER BY (`|> #(cols)`)
//! - `r_lower_limit` — LIMIT (`# < N`)
//! - `r_lower_distinct` — DISTINCT (`|> %(*)`)
//! - `r_lower_map_cover` — `|> $(fn:())(cols)`
//! - `r_lower_project_out` — `|> -(cols)`
//! - `r_lower_rename_cover` — `|> *(old as new)`
//! - `r_lower_transform` — `|> $$(expr as col)`
//! - `r_lower_aggregate_pipe` — `|~> agg:()`
//! - `r_lower_embed_map` — `|> +$(fn:())(cols)`
//! - `r_lower_meta_ize` — `|> ^` / `|> ^^`
//! - `r_lower_witness` — `|> exists(*)` / `|> notexists(*)`
//! - `r_lower_drill_down` — `|> .col(*)`
//! - `r_lower_dml_terminal` — `|> update!()(*)`

#![allow(unused_variables)]

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::asts::core::metadata::CprSchema;
use crate::pipeline::asts::core::{Addressed, PhaseBox};
use crate::pipeline::pipe_chain::PipeSegment;
use crate::pipeline::sql_ast_v3::TableExpression;

use super::builder::{col_name, col_qualifier, table_name_sql};
use super::builder::{Builder, NameGenerator, Projected, Qualify, Unprojected};
use super::scalar;
use super::tree_group;
use super::TransformCtx;
use crate::pipeline::asts::core::{ColumnMetadata, TableName};
use delightql_types::SqlIdentifier;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Extract column names from a slice of `DomainExpression::Lvar`s.
fn lvar_names(exprs: &[ast_addressed::DomainExpression]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|e| match e {
            ast_addressed::DomainExpression::Lvar { name, .. } => Some(name.as_str().to_string()),
            _ => None,
        })
        .collect()
}

/// Extract CprSchema from the outermost node of a RelationalExpression.
#[stacksafe::stacksafe]
fn extract_cpr_schema(expr: &ast_addressed::RelationalExpression) -> &CprSchema {
    use crate::pipeline::asts::core::expressions::relational::{Relation, RelationalExpression};
    match expr {
        RelationalExpression::Relation(rel) => match rel {
            Relation::Ground { cpr_schema, .. }
            | Relation::Anonymous { cpr_schema, .. }
            | Relation::TVF { cpr_schema, .. }
            | Relation::InnerRelation { cpr_schema, .. }
            | Relation::PseudoPredicate { cpr_schema, .. } => cpr_schema.get(),
            Relation::ConsultedView { scoped, .. } => scoped.get().schema(),
        },
        RelationalExpression::Join { cpr_schema, .. }
        | RelationalExpression::Filter { cpr_schema, .. }
        | RelationalExpression::SetOperation { cpr_schema, .. } => cpr_schema.get(),
        RelationalExpression::Pipe(pipe) => pipe.cpr_schema.get(),
        _ => &CprSchema::Unknown,
    }
}

/// Build a SQL column expression from a `ColumnMetadata`, properly qualified.
///
/// This is the universal "pass-through column" pattern — every projection
/// operator needs to turn a scope column into a `DomainExpression`.
fn qualified_col_expr(col: &ColumnMetadata) -> crate::pipeline::sql_ast_v3::DomainExpression {
    use crate::pipeline::sql_ast_v3::{ColumnQualifier, DomainExpression as SqlDomainExpr};

    let name = col_name(col);
    match col_qualifier(col) {
        Some(q) => SqlDomainExpr::with_qualifier(ColumnQualifier::table(q), name),
        None => SqlDomainExpr::column(name),
    }
}

/// Build a pass-through `SelectItem` from a `ColumnMetadata`.
fn passthrough_item(col: &ColumnMetadata) -> crate::pipeline::sql_ast_v3::SelectItem {
    use crate::pipeline::sql_ast_v3::SelectItem;

    SelectItem::Expression {
        expr: qualified_col_expr(col),
        alias: Some(col_name(col).to_string()),
    }
}

/// Project builder columns according to a CprSchema.
///
/// The CprSchema is the resolver's authoritative answer about which columns
/// survive and in what order. This function matches each CprSchema column to
/// the corresponding builder column using original/provenance names (which are
/// stable across the transformer's `_2` disambiguation).
///
/// Used by pipe operators that filter or reorder columns (project-out,
/// reposition, rename-cover, etc.) to ensure the transformer respects
/// the resolver's decisions.
fn select_items_from_cpr_schema(
    builder_columns: &[ColumnMetadata],
    cpr_schema: &CprSchema,
) -> Vec<crate::pipeline::sql_ast_v3::SelectItem> {
    let schema_cols = match cpr_schema {
        CprSchema::Resolved(cols) => cols,
        CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        CprSchema::Unresolved(cols) => cols,
        CprSchema::Unknown => {
            // Fallback: pass everything through
            return builder_columns
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
        }
    };

    let mut items = Vec::with_capacity(schema_cols.len());
    let mut used = vec![false; builder_columns.len()];

    for schema_col in schema_cols {
        let target_name = schema_col.name();
        let target_original = schema_col.info.original_name();

        // Find the matching builder column that hasn't been used yet.
        // Match by original name (bottom of provenance stack) on BOTH sides,
        // so renames (where name() differs from original_name()) still match.
        let found_idx = builder_columns.iter().enumerate().position(|(idx, bc)| {
            if used[idx] { return false; }

            if let Some(bc_orig) = bc.info.original_name() {
                // Primary: match original names (both sides look through renames/disambiguation)
                let schema_orig = target_original.unwrap_or(target_name);
                if SqlIdentifier::str_eq(bc_orig, schema_orig) {
                    // Verify table scope matches to handle same-named columns
                    // from different tables (e.g., two `name` columns)
                    if let (TableName::Named(a), TableName::Named(b)) =
                        (schema_col.qualifier(), bc.qualifier())
                    {
                        return a == b;
                    }
                    // Check identity stack for matching table qualifier
                    if let TableName::Named(schema_table) = schema_col.qualifier() {
                        if bc.info.identity_stack().iter().any(|id| {
                            matches!(&id.table_qualifier, TableName::Named(t) if t == schema_table)
                        }) {
                            return true;
                        }
                    }
                    // When qualifier is Fresh (after pipe boundary), check the
                    // most recent PipeBarrier's previous_table to distinguish
                    // same-named columns from different sources.
                    if matches!(schema_col.qualifier(), TableName::Fresh) {
                        // Find the FIRST (most recent) PipeBarrier — respect
                        // whatever it says, including Fresh.
                        let top_barrier = schema_col.info.identity_stack().iter().find_map(|id| {
                            if let crate::pipeline::asts::core::provenance::IdentityContext::PipeBarrier {
                                previous_table, ..
                            } = &id.context {
                                Some(previous_table)
                            } else {
                                None
                            }
                        });
                        if let Some(TableName::Named(prev_table)) = top_barrier {
                            // The column came from a named table before the pipe.
                            // Check bc's qualifier or identity stack for that name.
                            if let TableName::Named(bt) = bc.qualifier() {
                                if bt == prev_table {
                                    return true;
                                }
                            }
                            // Builder may use an alias — check if the table
                            // name appears anywhere in bc's provenance history.
                            let bc_knows_table = bc.info.identity_stack().iter().any(|id| {
                                matches!(&id.table_qualifier, TableName::Named(t) if t == prev_table)
                            });
                            return bc_knows_table;
                        }
                        // top_barrier is Fresh or absent — no table info to
                        // disambiguate. Fall through to name-only matching.
                    }
                    // No table info anywhere — match by original name alone
                    return true;
                }
                // Secondary: match current name (for non-renamed columns)
                if SqlIdentifier::str_eq(bc_orig, target_name) {
                    return true;
                }
            }
            // Fall back to current name
            SqlIdentifier::str_eq(col_name(bc), target_name)
        });

        if let Some(idx) = found_idx {
            used[idx] = true;
            // Use the CprSchema's name as the alias — this is the resolver's
            // authoritative output name (handles renames, project-out, etc.)
            let bc = &builder_columns[idx];
            let expr = qualified_col_expr(bc);
            items.push(crate::pipeline::sql_ast_v3::SelectItem::Expression {
                expr,
                alias: Some(target_name.to_string()),
            });
        }
    }

    items
}

/// Build a `json_each(source.column) AS alias` table-valued function expression.
///
/// Used by both `r_lower_melt_join` and `build_json_each_query` — the shared
/// pattern for expanding a JSON array column into rows. The column is always
/// an array the transformer built (a melt packet), so the TVF carries the
/// array-provenance internal name — spelled `json_each` canonically, but
/// respellable per-dialect where each-over-array needs a different form.
fn json_each_tvf(source_alias: &str, column: &str, tvf_alias: &str) -> TableExpression {
    use crate::pipeline::sql_ast_v3::TvfArgument;
    TableExpression::TVF {
        schema: None,
        function: crate::pipeline::naming::INTERNAL_JSON_EACH_ARRAY.to_string(),
        arguments: vec![TvfArgument::QualifiedRef {
            qualifier: source_alias.to_string(),
            column: column.to_string(),
        }],
        alias: Some(tvf_alias.to_string()),
    }
}

/// Apply a function expression to a column value, producing a SQL expression.
///
/// Dispatches on the FunctionExpression variant:
/// - Regular/Curried: `fn_name(column, extra_args...)`
/// - Lambda: substitute `@` in body with column, lower the result
/// - Window: substitute `@` in args/partition/order with column Lvar at AST
///   level, prepend column if args empty and no `@` found, then lower
fn apply_fn_to_column(
    function: &ast_addressed::FunctionExpression,
    column_sql: crate::pipeline::sql_ast_v3::DomainExpression,
    column_name: &str,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<crate::pipeline::sql_ast_v3::DomainExpression> {
    use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpr;

    match function {
        ast_addressed::FunctionExpression::Regular {
            name, arguments, ..
        }
        | ast_addressed::FunctionExpression::Curried {
            name, arguments, ..
        } => {
            let has_placeholder = has_placeholder_anywhere(function);
            // CFE expansion: build AST args with a qualified column Lvar,
            // expand at AST level, then lower through s_lower_expression.
            // The qualifier ensures the Lvar survives inner scope boundaries
            // (e.g., subquery CFE bodies like `orders:(, ... = param)`).
            if !has_placeholder {
                let qc = qualify.qualify(column_name)?;
                let col_lvar =
                    scalar::make_column_lvar_qualified(column_name, qc.qualifier.as_deref());
                let mut cfe_args = vec![col_lvar];
                cfe_args.extend(arguments.iter().cloned());
                if let Some(expanded) =
                    scalar::try_expand_cfe(name.as_str(), &cfe_args, qualify, ctx)?
                {
                    return scalar::s_lower_expression(expanded, qualify, ctx);
                }
            }
            if has_placeholder {
                // @ present: substitute @ → column_sql in args, don't prepend
                let args: Vec<_> = arguments
                    .iter()
                    .map(|a| {
                        scalar::s_lower_with_placeholder_pub(a.clone(), qualify, ctx, &column_sql)
                    })
                    .collect::<Result<_>>()?;
                Ok(SqlDomainExpr::function(name.as_str(), args))
            } else {
                // No @: prepend column as first arg
                let mut args = vec![column_sql];
                for a in arguments {
                    args.push(scalar::s_lower_expression(a.clone(), qualify, ctx)?);
                }
                Ok(SqlDomainExpr::function(name.as_str(), args))
            }
        }

        ast_addressed::FunctionExpression::Lambda { body, .. } => {
            scalar::s_lower_with_placeholder_pub(*body.clone(), qualify, ctx, &column_sql)
        }

        ast_addressed::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            // Substitute @ → column Lvar throughout the Window AST, then
            // if args are still empty, prepend column as implicit first arg.
            let col_lvar = scalar::make_column_lvar(column_name);
            let sub = |e: &ast_addressed::DomainExpression| -> ast_addressed::DomainExpression {
                scalar::substitute_placeholder_ast(e.clone(), &col_lvar)
            };

            let mut new_args: Vec<ast_addressed::DomainExpression> =
                arguments.iter().map(sub).collect();
            let new_partition: Vec<ast_addressed::DomainExpression> =
                partition_by.iter().map(sub).collect();
            let new_order: Vec<ast_addressed::OrderingSpec> = order_by
                .iter()
                .map(|spec| ast_addressed::OrderingSpec {
                    column: sub(&spec.column),
                    direction: spec.direction.clone(),
                })
                .collect();

            // Only prepend column as implicit first arg if:
            // - original args were empty, AND
            // - @ didn't appear anywhere in the Window (args, partition, order)
            // If @ was used (e.g., in ORDER BY), the column is already woven
            // into the expression and shouldn't also be a function argument.
            if arguments.is_empty() && !has_placeholder_anywhere(function) {
                new_args.insert(0, col_lvar);
            }

            let window_ast = ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::Window {
                    name: name.clone(),
                    arguments: new_args,
                    partition_by: new_partition,
                    order_by: new_order,
                    frame: frame.clone(),
                    alias: None,
                },
            );
            scalar::s_lower_expression(window_ast, qualify, ctx)
        }

        other => Err(DelightQLError::ParseError {
            message: format!(
                "apply_fn_to_column: unsupported function variant: {:?}",
                std::mem::discriminant(other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Check if `@` (ValuePlaceholder) appears anywhere in a FunctionExpression.
pub(super) fn has_placeholder_anywhere(func: &ast_addressed::FunctionExpression) -> bool {
    fn in_expr(e: &ast_addressed::DomainExpression) -> bool {
        match e {
            ast_addressed::DomainExpression::ValuePlaceholder { .. } => true,
            ast_addressed::DomainExpression::Function(f) => in_func(f),
            _ => false,
        }
    }
    fn in_func(f: &ast_addressed::FunctionExpression) -> bool {
        match f {
            ast_addressed::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                arguments.iter().any(in_expr)
                    || partition_by.iter().any(in_expr)
                    || order_by.iter().any(|s| in_expr(&s.column))
            }
            ast_addressed::FunctionExpression::Lambda { body, .. } => in_expr(body),
            ast_addressed::FunctionExpression::Regular { arguments, .. }
            | ast_addressed::FunctionExpression::Curried { arguments, .. } => {
                arguments.iter().any(in_expr)
            }
            ast_addressed::FunctionExpression::Infix { left, right, .. } => {
                in_expr(left) || in_expr(right)
            }
            _ => false,
        }
    }
    in_func(func)
}

/// Lower a TVF (Table-Valued Function) like `json_each(...)` or `pragma_table_info(...)`.
///
/// Converts each `HoArgument::Scalar` to a structured `TvfArgument`, preserving
/// literals, identifiers, and qualified references without stringifying.
fn r_lower_tvf(
    function: delightql_types::SqlIdentifier,
    ho_arguments: Vec<crate::pipeline::asts::core::operators::HoArgument<Addressed>>,
    alias: Option<delightql_types::SqlIdentifier>,
    backend_schema: crate::pipeline::asts::refined::PhaseBox<Option<String>, Addressed>,
    cpr_schema: crate::pipeline::asts::refined::PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::table::TvfArgument;

    let arguments: Vec<TvfArgument> = ho_arguments
        .into_iter()
        .filter_map(|arg| match arg {
            crate::pipeline::asts::core::operators::HoArgument::Scalar(expr) => {
                Some(lower_tvf_argument(expr))
            }
            crate::pipeline::asts::core::operators::HoArgument::Table(_) => None,
        })
        .collect();

    let fn_name = function.as_str().to_string();
    let schema = backend_schema.get().clone();

    let scope_name = match &alias {
        Some(a) => TableName::Named(SqlIdentifier::from(a.as_str())),
        None => TableName::Named(function.clone()),
    };

    let table_expr = TableExpression::TVF {
        schema,
        function: fn_name,
        arguments,
        alias: alias.as_ref().map(|a| a.as_str().to_string()),
    };

    let columns = columns_from_cpr_schema(cpr_schema.get(), &scope_name);

    Ok(Builder::from_table(
        table_expr,
        scope_name,
        columns,
        names.fork(),
    ))
}

/// Convert a scalar domain expression to a structured TVF argument.
fn lower_tvf_argument(
    expr: ast_addressed::DomainExpression,
) -> crate::pipeline::sql_ast_v3::table::TvfArgument {
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast_v3::table::TvfArgument;

    match expr {
        ast_addressed::DomainExpression::Literal { value, .. } => match value {
            LiteralValue::String(s) => TvfArgument::StringLiteral(s),
            LiteralValue::Number(n) => TvfArgument::NumberLiteral(n),
            LiteralValue::Boolean(b) => TvfArgument::Identifier(b.to_string()),
            LiteralValue::Null => TvfArgument::Identifier("NULL".to_string()),
        },
        ast_addressed::DomainExpression::Lvar {
            name,
            qualifier: Some(q),
            ..
        } => TvfArgument::QualifiedRef {
            qualifier: q.as_str().to_string(),
            column: name.as_str().to_string(),
        },
        ast_addressed::DomainExpression::Lvar { name, .. } => {
            TvfArgument::Identifier(name.as_str().to_string())
        }
        // Fallback: stringify the expression
        other => TvfArgument::Identifier(format!("{:?}", other)),
    }
}

/// Lower an anonymous relation (`_(1, 2, 3)`) into a `Builder<Unprojected>`.
///
/// Builds one `SELECT` per row (no FROM), folds with UNION ALL.
fn r_lower_anonymous(
    rows: Vec<crate::pipeline::asts::core::specs::Row<Addressed>>,
    alias: Option<delightql_types::SqlIdentifier>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{
        query::SetOperator, QueryExpression, SelectBuilder, SelectItem,
    };

    let col_names = columns_from_cpr_schema(cpr_schema.get(), &TableName::Fresh);
    let col_name_strs: Vec<String> = col_names.iter().map(|c| col_name(c).to_string()).collect();

    let dummy = DummyQualify;

    let mut row_queries: Vec<QueryExpression> = Vec::new();
    for (row_idx, row) in rows.into_iter().enumerate() {
        let mut sb = SelectBuilder::new();
        for (col_idx, val) in row.values.into_iter().enumerate() {
            let sql_expr = scalar::s_lower_expression(val, &dummy, ctx)?;
            let alias = col_name_strs.get(col_idx).cloned();
            // Only first row gets aliases (SQL UNION ALL infers from first branch)
            if row_idx == 0 {
                sb = sb.select(SelectItem::Expression {
                    expr: sql_expr,
                    alias,
                });
            } else {
                sb = sb.select(SelectItem::Expression {
                    expr: sql_expr,
                    alias: None,
                });
            }
        }
        let stmt = sb.build().map_err(|e| DelightQLError::ParseError {
            message: format!("r_lower_anonymous: {}", e),
            source: None,
            subcategory: None,
        })?;
        row_queries.push(QueryExpression::Select(Box::new(stmt)));
    }

    let query = row_queries
        .into_iter()
        .reduce(|left, right| QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(left),
            right: Box::new(right),
        })
        .ok_or_else(|| DelightQLError::ParseError {
            message: "r_lower_anonymous: empty rows".to_string(),
            source: None,
            subcategory: None,
        })?;

    let scope_name = match &alias {
        Some(a) => TableName::Named(SqlIdentifier::from(a.as_str())),
        None => TableName::Fresh,
    };
    let columns = columns_from_cpr_schema(cpr_schema.get(), &scope_name);

    Ok(Builder::from_frozen(
        query,
        scope_name,
        columns,
        names.fork(),
    ))
}

/// Lower an inner relation (interior subquery).
///
/// All patterns (UDT, CDT-SJ, CDT-GJ, CDT-WJ) share the same core:
/// recursively descend into the subquery, finalize to a QueryExpression,
/// and wrap as a Frozen builder with the inner relation's scope.
///
/// The subquery is a full `RelationalExpression` — pipes, filters, joins,
/// even nested inner relations — processed by the same `descend()` path
/// as any exterior query. Induction handles depth.
fn r_lower_inner_relation(
    pattern: ast_addressed::InnerRelationPattern,
    alias: Option<delightql_types::SqlIdentifier>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use super::descend;

    let (identifier, subquery, hygienic_injections) = match pattern {
        ast_addressed::InnerRelationPattern::Indeterminate { .. } => {
            return Err(DelightQLError::ParseError {
                message:
                    "r_lower_inner_relation: Indeterminate pattern should be classified by refiner"
                        .to_string(),
                source: None,
                subcategory: None,
            });
        }
        ast_addressed::InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            ..
        } => (identifier, subquery, vec![]),
        ast_addressed::InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            subquery,
            hygienic_injections,
            ..
        } => (identifier, subquery, hygienic_injections),
        ast_addressed::InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            subquery,
            hygienic_injections,
            ..
        } => (identifier, subquery, hygienic_injections),
    };

    // Recursive descent into the subquery — same path as any exterior query.
    let inner_names = names.fork();
    let inner_builder = descend::descend_as_query(*subquery, &inner_names, ctx)?;

    // Scope: alias if present, otherwise the identifier name.
    let scope_name = match &alias {
        Some(a) => TableName::Named(SqlIdentifier::from(a.as_str())),
        None => TableName::Named(SqlIdentifier::from(identifier.name.as_str())),
    };
    let cpr_columns = columns_from_cpr_schema(cpr_schema.get(), &scope_name);

    // Compare inner output names with CprSchema names. If they differ
    // (e.g., CprSchema says "fn" but inner outputs "first_name"), inject
    // a rename projection so the finalized SQL outputs the CprSchema names.
    let query = reconcile_inner_with_cpr(inner_builder, &cpr_columns)?;

    // Hygienic columns (__dql_corr_0 etc.) are in the subquery output for
    // JOIN ON but NOT in the published scope. The Qualify fallback uses the
    // scope's own name as qualifier, so join conditions still resolve correctly.

    // Return as Table with subquery — not Frozen. This way, the join
    // handler's into_table_expr() passes the TableExpression through
    // directly instead of wrapping it again with a generated alias.
    let alias_str = table_name_sql(&scope_name).to_string();
    let table_expr = TableExpression::subquery(query, alias_str);
    Ok(Builder::from_table(
        table_expr,
        scope_name,
        cpr_columns,
        names.fork(),
    ))
}

/// Lower a ConsultedView: view body inlined as a subquery with CprSchema reconciliation.
fn r_lower_consulted_view(
    body: ast_addressed::Query,
    scoped: PhaseBox<crate::pipeline::asts::core::ScopedSchema, Addressed>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let scoped_schema = scoped.get();
    let derived_alias = scoped_schema.alias().to_string();

    let scope_name = TableName::Named(SqlIdentifier::from(derived_alias.as_str()));
    let cpr_columns = columns_from_cpr_schema(scoped_schema.schema(), &scope_name);

    let body_sql = match body {
        ast_addressed::Query::Relational(expr) => {
            let inner_builder = super::descend::descend_as_final(expr, names, ctx)?;
            reconcile_inner_with_cpr(inner_builder, &cpr_columns)?
        }
        ast_addressed::Query::WithCtes { ctes, query: expr } => {
            let sql_ctes: Vec<crate::pipeline::sql_ast_v3::Cte> = ctes
                .into_iter()
                .map(|binding| lower_cte_binding(binding, names, ctx))
                .collect::<Result<_>>()?;

            let inner_builder = super::descend::descend_as_final(expr, names, ctx)?;
            let main_query = reconcile_inner_with_cpr(inner_builder, &cpr_columns)?;

            if sql_ctes.is_empty() {
                main_query
            } else {
                // Merge CTEs if main_query already has a WITH clause
                match main_query {
                    crate::pipeline::sql_ast_v3::QueryExpression::WithCte {
                        ctes: inner_ctes,
                        query: inner_query,
                    } => {
                        let mut merged = sql_ctes;
                        merged.extend(inner_ctes);
                        crate::pipeline::sql_ast_v3::QueryExpression::WithCte {
                            ctes: merged,
                            query: inner_query,
                        }
                    }
                    other => crate::pipeline::sql_ast_v3::QueryExpression::WithCte {
                        ctes: sql_ctes,
                        query: Box::new(other),
                    },
                }
            }
        }
        other => super::transform_with_names(other, names, ctx)?,
    };

    let table_expr = TableExpression::subquery(body_sql, &derived_alias);
    Ok(Builder::from_table(
        table_expr,
        scope_name,
        cpr_columns,
        names.fork(),
    ))
}

/// Lower a ground relation with positional (argumentative) access.
///
/// Emits `SELECT original AS alias, ... FROM table` — a rename projection
/// that drops underscored positions and renames columns per the user's
/// positional binding. The result is wrapped as a Frozen subquery so that
/// downstream consumers (joins, pipes) see the renamed columns.
///
/// Hygienic columns (literal grounding positions) are included in the
/// subquery SELECT but will be stripped by a wrapping layer when the
/// resolver-lifted Filter node is processed.
fn r_lower_positional_relation(
    table_expr: TableExpression,
    scope_name: TableName,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, QueryExpression, SelectBuilder,
        SelectItem,
    };
    let columns = match cpr_schema.get() {
        CprSchema::Resolved(cols)
        | CprSchema::Failed {
            resolved_columns: cols,
            ..
        } => cols,
        _ => {
            // Fallback: treat as glob
            let columns = columns_from_cpr_schema(cpr_schema.get(), &scope_name);
            return Ok(Builder::from_table(
                table_expr,
                scope_name,
                columns,
                names.fork(),
            ));
        }
    };

    // Qualify against the raw table name (not the positional scope name)
    let table_qualifier = match &table_expr {
        TableExpression::Table { name, alias, .. } => alias.as_deref().unwrap_or(name).to_string(),
        _ => table_name_sql(&scope_name).to_string(),
    };
    let mut select_items = Vec::new();
    let mut scope_columns: Vec<ColumnMetadata> = Vec::new();

    for col in columns {
        let original = col.original_name();
        let alias_name = col.name();

        let column_ref =
            SqlDomainExpr::with_qualifier(ColumnQualifier::table(&table_qualifier), original);

        select_items.push(SelectItem::Expression {
            expr: column_ref,
            alias: Some(alias_name.to_string()),
        });

        // Hygienic columns stay in scope so filters/joins can reference
        // them (e.g., positional `upper:(description)` creates a WHERE
        // clause that needs `description`). They are stripped later by
        // `project_all` / `disambiguated_select_items`.
        let mut new_col = col.clone();
        super::builder::state::push_scope_transition(
            &mut new_col,
            Some(alias_name),
            &scope_name,
            crate::pipeline::asts::core::provenance::IdentityContext::OriginalTable {
                table: scope_name.clone(),
                qualification:
                    crate::pipeline::asts::core::provenance::QualificationSource::Resolver,
            },
        );
        scope_columns.push(new_col);
    }

    let stmt = SelectBuilder::new()
        .select_all(select_items)
        .from_tables(vec![table_expr])
        .build()
        .map_err(|e| DelightQLError::ParseError {
            message: format!("r_lower_positional_relation: {}", e),
            source: None,
            subcategory: None,
        })?;

    let query = QueryExpression::Select(Box::new(stmt));
    Ok(Builder::from_frozen(
        query,
        scope_name,
        scope_columns,
        names.fork(),
    ))
}

// ---------------------------------------------------------------------------
// Top-level handlers (called from descend())
// ---------------------------------------------------------------------------

/// Lower a base `Relation` (table, anonymous, TVF, inner relation, etc.)
/// into a fresh `Builder<Unprojected>`.
///
/// This is the leaf case — the base of the dive-and-bubble recursion.
pub(super) fn r_lower_relation(
    rel: ast_addressed::Relation,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    match rel {
        ast_addressed::Relation::Ground {
            identifier,
            canonical_name,
            backend_schema,
            alias,
            domain_spec,
            cpr_schema,
            ..
        } => {
            let table_name = canonical_name
                .get()
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| identifier.name.as_str().to_string());

            // SQL table expression (with schema prefix for mounted databases)
            let schema_prefix = backend_schema.get().clone();
            let table_expr = match (&alias, &schema_prefix) {
                (Some(a), Some(schema)) => TableExpression::Table {
                    schema: Some(schema.clone()),
                    name: table_name.clone(),
                    alias: Some(a.as_str().to_string()),
                },
                (None, Some(schema)) => TableExpression::Table {
                    schema: Some(schema.clone()),
                    name: table_name.clone(),
                    alias: None,
                },
                (Some(a), None) => TableExpression::table_with_alias(&table_name, a.as_str()),
                (None, None) => TableExpression::table(&table_name),
            };

            // Scope name: alias if present, otherwise table name
            let scope_name = match &alias {
                Some(a) => TableName::Named(SqlIdentifier::from(a.as_str())),
                None => TableName::Named(SqlIdentifier::from(table_name.as_str())),
            };

            // Positional access: emit SELECT original AS alias for each column
            if matches!(domain_spec, ast_addressed::DomainSpec::Positional(_)) {
                return r_lower_positional_relation(table_expr, scope_name, cpr_schema, names);
            }

            // Glob/bare: all columns, no rename
            let columns = columns_from_cpr_schema(cpr_schema.get(), &scope_name);

            Ok(Builder::from_table(
                table_expr,
                scope_name,
                columns,
                names.fork(),
            ))
        }

        ast_addressed::Relation::Anonymous {
            rows,
            alias,
            cpr_schema,
            ..
        } => r_lower_anonymous(rows, alias, cpr_schema, names, ctx),

        ast_addressed::Relation::InnerRelation {
            pattern,
            alias,
            cpr_schema,
            ..
        } => r_lower_inner_relation(pattern, alias, cpr_schema, names, ctx),

        ast_addressed::Relation::ConsultedView { body, scoped, .. } => {
            r_lower_consulted_view(*body, scoped, names, ctx)
        }

        ast_addressed::Relation::TVF {
            function,
            ho_arguments,
            alias,
            backend_schema,
            cpr_schema,
            ..
        } => r_lower_tvf(
            function,
            ho_arguments,
            alias,
            backend_schema,
            cpr_schema,
            names,
        ),

        other => Err(DelightQLError::ParseError {
            message: format!(
                "r_lower_relation: unimplemented Relation variant: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Reconcile an inner builder's output columns with CprSchema column names.
///
/// If the inner builder outputs different column names from what the CprSchema
/// expects (e.g., inner has `first_name` but CprSchema says `fn`), inject a
/// rename projection before finalizing. This ensures the SQL output matches
/// the scope names — the alias-scope invariant.
///
/// For columns beyond the CprSchema count (hygienic columns like `__dql_corr_0`),
/// they are passed through unchanged.
/// Extract CprSchema column metadata from a CTE binding's expression.
/// Used by the CTE lowering in mod.rs to reconcile CTE body output columns.
///
/// Returns empty if the CprSchema has duplicate column names (e.g., from a join
/// before disambiguation), since reconciling would create duplicate SQL aliases.
pub(super) fn cte_cpr_columns(expr: &ast_addressed::RelationalExpression) -> Vec<ColumnMetadata> {
    let schema = extract_cpr_schema(expr);
    let cols = columns_from_cpr_schema(schema, &TableName::Fresh);

    // Guard: if CprSchema has duplicate names, skip reconciliation.
    // This happens with joins where both sides have columns with the same name.
    let mut seen = std::collections::HashSet::new();
    for c in &cols {
        if !seen.insert(col_name(c)) {
            return Vec::new();
        }
    }
    cols
}

/// RECURSION-CONTRACT.md B2 — argumentative binding on the recursive
/// self-reference (`c(m)` inside c's own definition) does not bind today:
/// the rename mis-merges into a NULL-padded two-column union and returns
/// SILENTLY WRONG results. Hard-refuse until the rename-hoist legalization
/// (`WITH c(m) AS (…)` — needs the Cte column list) lands. Checked here,
/// at the one site that lowers every CTE binding, with its own walk — the
/// upstream is_recursive flag is not trusted (it historically never
/// engaged).
fn check_recursive_argumentative_binding(binding: &ast_addressed::CteBinding) -> Result<()> {
    if expr_has_positional_self_ref(&binding.expression, &binding.name) {
        return Err(DelightQLError::ValidationError {
            message: format!(
                "the recursive reference to '{name}' uses argumentative binding \
                 ('{name}(…)') — renames and constraints on the self-reference \
                 do not bind inside a recursive definition yet. Use glob binding \
                 '{name}(*)' and rename or filter in a pipe stage. \
                 RECURSION-CONTRACT.md B2.",
                name = binding.name,
            ),
            context: "transformer::lower_cte_binding".to_string(),
            subcategory: Some(crate::uri_registry::subcat::RECURSION_ARGUMENTATIVE_BINDING),
        });
    }
    Ok(())
}

#[stacksafe::stacksafe]
fn expr_has_positional_self_ref(
    expr: &ast_addressed::RelationalExpression,
    name: &str,
) -> bool {
    use ast_addressed::RelationalExpression as E;
    match expr {
        E::Relation(rel) => relation_is_positional_self_ref(rel, name),
        E::Join { left, right, .. } => {
            expr_has_positional_self_ref(left, name) || expr_has_positional_self_ref(right, name)
        }
        E::Filter { source, .. } => expr_has_positional_self_ref(source, name),
        E::Pipe(pipe) => expr_has_positional_self_ref(&pipe.source, name),
        E::SetOperation { operands, .. } | E::IntersectCorresponding { operands, .. } => operands
            .iter()
            .any(|op| expr_has_positional_self_ref(op, name)),
        E::ErJoinChain { .. } | E::ErTransitiveJoin { .. } => false,
    }
}

fn relation_is_positional_self_ref(rel: &ast_addressed::Relation, name: &str) -> bool {
    match rel {
        ast_addressed::Relation::Ground {
            identifier,
            domain_spec,
            ..
        } => {
            identifier.name == delightql_types::SqlIdentifier::new(name)
                && matches!(domain_spec, ast_addressed::DomainSpec::Positional(_))
        }
        ast_addressed::Relation::InnerRelation { pattern, .. } => {
            use ast_addressed::InnerRelationPattern as P;
            match pattern {
                P::Indeterminate { subquery, .. }
                | P::UncorrelatedDerivedTable { subquery, .. }
                | P::CorrelatedScalarJoin { subquery, .. }
                | P::CorrelatedGroupJoin { subquery, .. } => {
                    expr_has_positional_self_ref(subquery, name)
                }
            }
        }
        _ => false,
    }
}

/// Lower a single CTE binding to a SQL CTE, reconciling body columns with CprSchema.
pub(super) fn lower_cte_binding(
    binding: ast_addressed::CteBinding,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<crate::pipeline::sql_ast_v3::Cte> {
    check_recursive_argumentative_binding(&binding)?;
    let cte_cpr = cte_cpr_columns(&binding.expression);
    let inner_builder = super::descend::descend_as_final(binding.expression, names, ctx)?;
    let cte_query = if cte_cpr.is_empty() {
        inner_builder.to_sql()?
    } else {
        reconcile_inner_with_cpr(inner_builder, &cte_cpr)?
    };
    let is_recursive = binding.is_recursive.get().clone();
    Ok(if is_recursive {
        crate::pipeline::sql_ast_v3::Cte::new_recursive(binding.name, cte_query)
    } else {
        crate::pipeline::sql_ast_v3::Cte::new(binding.name, cte_query)
    })
}

pub(super) fn reconcile_inner_with_cpr(
    inner_builder: Builder<Projected>,
    cpr_columns: &[ColumnMetadata],
) -> Result<crate::pipeline::sql_ast_v3::QueryExpression> {
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    // Snapshot inner column names/qualifiers before consuming the builder
    let inner_col_data: Vec<(String, Option<String>)> = inner_builder
        .columns()
        .iter()
        .map(|c| {
            (
                col_name(c).to_string(),
                col_qualifier(c).map(|s| s.to_string()),
            )
        })
        .collect();

    let cpr_count = cpr_columns.len();

    // Check if any CprSchema names differ from inner output names
    let needs_rename = inner_col_data.len() >= cpr_count
        && inner_col_data
            .iter()
            .zip(cpr_columns.iter())
            .any(|((inner_name, _), cpr_col)| inner_name != col_name(cpr_col));

    if !needs_rename {
        return inner_builder.to_sql();
    }

    // Build rename items: use bare column names (no qualifier), since
    // add_projection wraps as subquery first, changing the qualifier.
    let rename_items: Vec<SelectItem> = inner_col_data
        .iter()
        .enumerate()
        .map(|(i, (inner_name, _qualifier))| {
            let expr = SqlDomainExpr::column(inner_name);
            let target_name = if i < cpr_count {
                col_name(&cpr_columns[i]).to_string()
            } else {
                inner_name.clone() // hygienic: pass through
            };
            SelectItem::Expression {
                expr,
                alias: Some(target_name),
            }
        })
        .collect();

    inner_builder.add_projection(rename_items)?.to_sql()
}

/// Extract `Vec<ColumnMetadata>` from a `CprSchema`, pushing a scope transition
/// onto each column's identity stack so the qualifier reflects the given scope.
///
/// This is the translation boundary: CprSchema from the resolver/refiner flows
/// in, and the builder gets `Vec<ColumnMetadata>` with the identity stack updated
/// to reflect the current SQL scope. No information is discarded.
fn columns_from_cpr_schema(schema: &CprSchema, scope_name: &TableName) -> Vec<ColumnMetadata> {
    use super::builder::state::{push_scope_transition, unique_name};
    use crate::pipeline::asts::core::provenance::IdentityContext;
    use std::collections::HashSet;

    let cols = match schema {
        CprSchema::Resolved(cols)
        | CprSchema::Failed {
            resolved_columns: cols,
            ..
        } => cols,
        CprSchema::Unresolved(cols) => cols,
        CprSchema::Unknown => return Vec::new(),
    };

    // First pass: collect columns with scope transition
    let mut result: Vec<ColumnMetadata> = cols
        .iter()
        .map(|c| {
            let mut col = c.clone();
            push_scope_transition(
                &mut col,
                None,
                scope_name,
                IdentityContext::OriginalTable {
                    table: scope_name.clone(),
                    qualification:
                        crate::pipeline::asts::core::provenance::QualificationSource::Resolver,
                },
            );
            col
        })
        .collect();

    // Second pass: disambiguate duplicate names (e.g., join produces id, id → id, id_2).
    let has_duplicates = {
        let mut seen = HashSet::new();
        result.iter().any(|c| !seen.insert(col_name(c).to_string()))
    };
    if has_duplicates {
        let mut used = HashSet::new();
        for col in &mut result {
            let name = col_name(col).to_string();
            let disambiguated = unique_name(&name, &mut used);
            if disambiguated != name {
                push_scope_transition(
                    col,
                    Some(&disambiguated),
                    scope_name,
                    IdentityContext::OriginalTable {
                        table: scope_name.clone(),
                        qualification:
                            crate::pipeline::asts::core::provenance::QualificationSource::Resolver,
                    },
                );
            }
        }
    }

    result
}

/// The resolver's output schema as typed columns. Unknown → empty.
fn cpr_output_columns(schema: &CprSchema) -> &[ColumnMetadata] {
    match schema {
        CprSchema::Resolved(cols)
        | CprSchema::Failed {
            resolved_columns: cols,
            ..
        } => cols,
        CprSchema::Unresolved(cols) => cols,
        CprSchema::Unknown => &[],
    }
}

/// Spelling of a resolver output column where it crosses into SQL (alias
/// stamping). Fallback chain preserved from the retired Vec<String> seam —
/// NOT the same as the builder's col_name() ("_unnamed" fallback).
fn cpr_display_name(c: &ColumnMetadata) -> &str {
    c.info.name().or_else(|| c.info.original_name()).unwrap_or("?")
}

/// Stamp a resolver-assigned alias onto a single SelectItem iff it is
/// un-aliased. The name comes from an expression's own output stamp (the
/// per-expression source of truth) rather than a positional cpr lookup — the
/// seam carries typed columns and the spelling is extracted here at the
/// stamping border.
fn alias_unaliased(item: &mut crate::pipeline::sql_ast_v3::SelectItem, name: &str) {
    if let crate::pipeline::sql_ast_v3::SelectItem::Expression {
        alias: alias @ None,
        ..
    } = item
    {
        *alias = Some(name.to_string());
    }
}

/// Lower a `Filter` node: add WHERE to the child builder.
///
/// Filter is transparent — it passes through the child's scope.
/// The `origin` tracks where the filter came from (comma vs interior).
pub(super) fn r_lower_filter(
    child: Builder<Unprojected>,
    condition: ast_addressed::SigmaCondition,
    origin: ast_addressed::FilterOrigin,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    match condition {
        ast_addressed::SigmaCondition::Predicate(bool_expr) => {
            let predicate = scalar::s_lower_boolean(bool_expr, &child, ctx)?;
            child.add_where(predicate)
        }

        ast_addressed::SigmaCondition::TupleOrdinal(clause) => {
            // # < N → LIMIT N, with optional OFFSET
            child.add_limit(clause.value, clause.offset)
        }

        ast_addressed::SigmaCondition::Destructure {
            json_column,
            mode,
            pattern,
            ..
        } => r_lower_destructure(child, *json_column, mode, &pattern, ctx),

        sigma @ ast_addressed::SigmaCondition::SigmaCall { .. } => {
            let predicate = scalar::s_lower_sigma(sigma, &child, ctx)?;
            child.add_where(predicate)
        }
    }
}

/// Lower a `Join` node: combine two builders into a single joined builder.
///
/// Prepares both sides as join operands FIRST (which may wrap complex states
/// as subqueries with generated aliases), then lowers the join condition
/// against the post-wrap scopes. This ensures the condition's qualifiers
/// match the SQL aliases that actually appear in the output.
pub(super) fn r_lower_join(
    left: Builder<Unprojected>,
    right: Builder<Unprojected>,
    join_condition: Option<ast_addressed::BooleanExpression>,
    join_type: Option<ast_addressed::JoinType>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{JoinCondition as SqlJoinCondition, JoinType as SqlJoinType};

    let sql_join_type = match join_type {
        None | Some(ast_addressed::JoinType::Inner) => SqlJoinType::Inner,
        Some(ast_addressed::JoinType::LeftOuter) => SqlJoinType::Left,
        Some(ast_addressed::JoinType::RightOuter) => SqlJoinType::Right,
        Some(ast_addressed::JoinType::FullOuter) => SqlJoinType::Full,
    };

    // Prepare both sides — this may wrap Segment/Select/Frozen states as
    // subqueries, requalifying scope columns to the wrapper alias.
    let left_op = left.into_join_operand()?;
    let mut right_op = right.into_join_operand()?;

    // Resolve TVF QualifiedRef arguments (e.g., `anon.a` → `t_1.a`)
    // against the left side's post-wrap scope.
    right_op.resolve_tvf_args(&left_op);

    // Lower the join condition against the POST-WRAP scopes.
    // ChainedQualify lives in the builder module — the qualify logic stays
    // in one place instead of being reimplemented here.
    let condition = match join_condition {
        Some(ast_addressed::BooleanExpression::Using { columns }) => {
            let column_names: Vec<String> = columns
                .iter()
                .filter_map(|col| match col {
                    ast_addressed::UsingColumn::Regular(id) => Some(id.name.to_string()),
                    ast_addressed::UsingColumn::Negated(_) => None,
                })
                .collect();
            SqlJoinCondition::Using(column_names)
        }
        Some(bool_expr) => {
            let combined = super::builder::ChainedQualify {
                inner: &left_op,
                outer: &right_op,
            };
            let pred = scalar::s_lower_boolean(bool_expr, &combined, ctx)?;
            SqlJoinCondition::On(pred.into_expr())
        }
        None => SqlJoinCondition::Natural,
    };

    Ok(Builder::from_join(
        left_op,
        right_op,
        sql_join_type,
        condition,
    ))
}

/// Lower a join where the right side is an anonymous table.
///
/// When the anonymous table's row data contains column references (e.g.,
/// `u.first_name`), those references are correlated — they refer to the
/// left-side scope. A plain UNION ALL subquery can't reference outer scope
/// in SQL (no LATERAL support in SQLite).
///
/// Strategy:
/// - No column refs → fall through to normal `r_lower_anonymous` + `r_lower_join`
/// - Has column refs → JSON melt: pack row values into a `json_array()`
///   expression evaluated in the left scope, push as CTE, expand with
///   `json_each`, extract columns with `json_extract`
pub(super) fn r_lower_join_anonymous(
    left: Builder<Unprojected>,
    anon: ast_addressed::Relation,
    join_condition: Option<ast_addressed::BooleanExpression>,
    join_type: Option<ast_addressed::JoinType>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let ast_addressed::Relation::Anonymous {
        rows,
        alias,
        cpr_schema: anon_cpr_schema,
        exists_mode,
        ..
    } = anon
    else {
        unreachable!("r_lower_join_anonymous called with non-Anonymous relation")
    };

    // Check if any row value contains a column reference.
    let has_column_refs = rows
        .iter()
        .any(|row| row.values.iter().any(|v| contains_column_reference(v)));

    if !has_column_refs || exists_mode {
        // No correlated refs (or EXISTS mode) — use normal UNION ALL path.
        let right = r_lower_anonymous(rows, alias, anon_cpr_schema, names, ctx)?;
        return r_lower_join(left, right, join_condition, join_type, cpr_schema, ctx);
    }

    // --- JSON melt path ---
    r_lower_melt_join(
        left,
        rows,
        alias,
        anon_cpr_schema,
        join_condition,
        cpr_schema,
        names,
        ctx,
    )
}

/// Build a JSON melt: pack correlated anonymous-table rows into a json_array
/// on the left side, then expand with json_each + json_extract.
fn r_lower_melt_join(
    left: Builder<Unprojected>,
    rows: Vec<crate::pipeline::asts::core::specs::Row<Addressed>>,
    alias: Option<delightql_types::SqlIdentifier>,
    anon_cpr_schema: PhaseBox<CprSchema, Addressed>,
    _join_condition: Option<ast_addressed::BooleanExpression>,
    _cpr_schema: PhaseBox<CprSchema, Addressed>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, QueryExpression, SelectItem,
        SelectStatement,
    };

    let melt_col_names: Vec<String> = cpr_output_columns(anon_cpr_schema.get())
        .iter()
        .map(|c| cpr_display_name(c).to_string())
        .collect();
    let packet_col = "_melt_packet";
    let source_columns: Vec<ColumnMetadata> = left.columns().to_vec();
    let num_left = source_columns.len();

    // 1. Lower row values against left scope → json_array(json_array(v1,v2), ...).
    let row_exprs: Vec<SqlDomainExpr> = rows
        .iter()
        .map(|row| {
            let vals: Result<Vec<_>> = row
                .values
                .iter()
                .map(|v| scalar::s_lower_expression(v.clone(), &left, ctx))
                .collect();
            Ok(SqlDomainExpr::function("json_array", vals?))
        })
        .collect::<Result<_>>()?;

    // 2. Project left + melt_packet, wrap as subquery for json_each.
    let mut items: Vec<SelectItem> = left.columns().iter().map(|c| passthrough_item(c)).collect();
    items.push(SelectItem::Expression {
        expr: SqlDomainExpr::function("json_array", row_exprs),
        alias: Some(packet_col.to_string()),
    });
    let source_query = left.add_projection(items)?.to_sql()?;

    let source_alias_str = table_name_sql(&names.next_table_name("t")).to_string();
    let je_alias_str = table_name_sql(&names.next_table_name("_je")).to_string();

    // 3. Build outer SELECT: passthrough left columns + json_extract per melt column.
    let sq = ColumnQualifier::table(&source_alias_str);
    let jq = ColumnQualifier::table(&je_alias_str);

    let mut select_items: Vec<SelectItem> = source_columns
        .iter()
        .map(|c| {
            SelectItem::expression_with_alias(
                SqlDomainExpr::with_qualifier(sq.clone(), col_name(c)),
                col_name(c),
            )
        })
        .collect();
    for (i, name) in melt_col_names.iter().enumerate() {
        select_items.push(SelectItem::expression_with_alias(
            SqlDomainExpr::function(
                "json_extract",
                vec![
                    SqlDomainExpr::with_qualifier(jq.clone(), "value"),
                    SqlDomainExpr::literal(
                        crate::pipeline::asts::core::literals::LiteralValue::String(format!(
                            "$[{}]",
                            i
                        )),
                    ),
                ],
            ),
            name,
        ));
    }
    let select_items = super::builder::disambiguate_aliases(select_items);

    let select = SelectStatement::builder()
        .set_select(select_items.clone())
        .from_tables(vec![
            TableExpression::subquery(source_query, &source_alias_str),
            json_each_tvf(&source_alias_str, packet_col, &je_alias_str),
        ])
        .build()
        .map_err(|e| DelightQLError::ParseError {
            message: format!("melt query: {}", e),
            source: None,
            subcategory: None,
        })?;

    // 4. Build scope: left columns inherit provenance, melt columns get fresh.
    use crate::pipeline::asts::core::provenance::{ColumnProvenance, QualificationSource};
    let scope_name = match &alias {
        Some(a) => TableName::Named(SqlIdentifier::from(a.as_str())),
        None => TableName::Fresh,
    };
    let columns: Vec<ColumnMetadata> = select_items
        .iter()
        .enumerate()
        .filter_map(|(i, item)| {
            let SelectItem::Expression { alias, .. } = item else {
                return None;
            };
            let name = alias.as_deref().unwrap_or("_expr");
            let prov = if i < num_left {
                source_columns
                    .get(i)
                    .map(|c| c.info.clone())
                    .unwrap_or_else(|| {
                        ColumnProvenance::from_table_column(
                            name,
                            scope_name.clone(),
                            QualificationSource::Resolver,
                        )
                    })
            } else {
                ColumnProvenance::from_table_column(
                    name,
                    scope_name.clone(),
                    QualificationSource::Resolver,
                )
            };
            Some(ColumnMetadata::new(prov, scope_name.clone(), Some(i)))
        })
        .collect();

    Builder::from_query(
        QueryExpression::Select(Box::new(select)),
        scope_name,
        columns,
        names.fork(),
    )
    .demote()
}

/// Check if a domain expression contains column references.
///
/// Any Lvar counts — unqualified Lvars in melt rows are correlated
/// references to the left-side scope (e.g., `json` in `json:{.path}`).
/// False positives are harmless: the melt/json_each path is functionally
/// correct for non-correlated rows too, just slightly less optimal SQL.
fn contains_column_reference(expr: &ast_addressed::DomainExpression) -> bool {
    match expr {
        ast_addressed::DomainExpression::Lvar { .. } => true,
        ast_addressed::DomainExpression::Literal { .. } => false,
        ast_addressed::DomainExpression::Function(func) => {
            use ast_addressed::FunctionExpression;
            match func {
                FunctionExpression::Regular { arguments, .. }
                | FunctionExpression::Curried { arguments, .. }
                | FunctionExpression::Bracket { arguments, .. } => {
                    arguments.iter().any(contains_column_reference)
                }
                FunctionExpression::Infix { left, right, .. } => {
                    contains_column_reference(left) || contains_column_reference(right)
                }
                FunctionExpression::JsonPath { source, .. } => contains_column_reference(source),
                FunctionExpression::Lambda { body, .. } => contains_column_reference(body),
                FunctionExpression::Curly { members, .. } => {
                    use crate::pipeline::asts::core::expressions::functions::CurlyMember;
                    members.iter().any(|m| match m {
                        CurlyMember::Shorthand { .. } => true,
                        CurlyMember::KeyValue { value, .. } => contains_column_reference(value),
                        _ => false,
                    })
                }
                _ => false,
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            contains_column_reference(inner)
        }
        _ => false,
    }
}

/// Qualify implementation for contexts with no scope (anonymous table rows).
///
/// All columns come back unqualified — anonymous rows contain only literals
/// and expressions that don't reference any table columns.
struct DummyQualify;

impl Qualify for DummyQualify {
    fn qualify(&self, col_name: &str) -> crate::error::Result<super::builder::QualifiedColumn> {
        Ok(super::builder::QualifiedColumn {
            name: col_name.to_string(),
            qualifier: None,
        })
    }

    fn try_qualify_with_table(
        &self,
        col_name: &str,
        table: &str,
    ) -> Option<super::builder::QualifiedColumn> {
        Some(super::builder::QualifiedColumn {
            name: col_name.to_string(),
            qualifier: Some(table.to_string()),
        })
    }
}

/// Lower a pipe chain: left-fold segments over a base builder.
///
/// The fold starts with `Builder<Unprojected>` (the base) and produces
/// `Builder<Projected>` (the last segment must set a SELECT list).
pub(super) fn r_lower_pipe(
    base: Builder<Unprojected>,
    segments: Vec<PipeSegment<Addressed>>,
    names: &NameGenerator,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::operators::UnaryRelationalOperator;

    // If no segments, just project_all (SELECT *)
    if segments.is_empty() {
        return base.project_all();
    }

    // Left-fold: each segment transforms Unprojected → Projected.
    // Between segments, demote Projected → Unprojected for the next one.
    let mut current: Builder<Unprojected> = base;

    let last_idx = segments.len() - 1;
    for (i, segment) in segments.into_iter().enumerate() {
        let PipeSegment {
            operator,
            cpr_schema,
        } = segment;
        let result: Builder<Projected> = match operator {
            UnaryRelationalOperator::General { expressions, .. } => {
                r_lower_projection(current, expressions, Some(&cpr_schema), ctx)?
            }

            UnaryRelationalOperator::ProjectOut { expressions, .. } => {
                r_lower_project_out(current, expressions, &cpr_schema, ctx)?
            }

            UnaryRelationalOperator::RenameCover { specs } => {
                r_lower_rename_cover(current, specs, &cpr_schema, ctx)?
            }

            UnaryRelationalOperator::TupleOrdering { specs, .. } => {
                r_lower_order_by(current, specs, ctx)?
            }

            UnaryRelationalOperator::Modulo { spec, .. } => {
                r_lower_modulo(current, spec, &cpr_schema, ctx)?
            }

            UnaryRelationalOperator::Transform {
                transformations,
                conditioned_on,
                ..
            } => r_lower_transform(current, transformations, conditioned_on, ctx)?,

            UnaryRelationalOperator::MapCover {
                function,
                columns,
                conditioned_on,
                ..
            } => r_lower_map_cover(current, function, columns, conditioned_on, ctx)?,

            UnaryRelationalOperator::Reposition { .. } => {
                r_lower_reposition(current, cpr_schema.get(), ctx)?
            }

            UnaryRelationalOperator::EmbedMapCover {
                function,
                selector,
                alias_template,
                ..
            } => r_lower_embed_map(current, function, selector, alias_template, ctx)?,

            UnaryRelationalOperator::MetaIze { detailed } => {
                r_lower_meta_ize(current, detailed, &cpr_schema, ctx)?
            }

            UnaryRelationalOperator::Witness { exists } => r_lower_witness(current, exists, ctx)?,

            UnaryRelationalOperator::NarrowingDestructure { column, fields } => {
                r_lower_narrowing_destructure(current, column, fields, &cpr_schema, ctx)?
            }

            UnaryRelationalOperator::InteriorDrillDown {
                column,
                glob,
                columns,
                interior_schema,
                groundings,
            } => r_lower_interior_drill_down(
                current,
                column,
                glob,
                columns,
                interior_schema,
                groundings,
                &cpr_schema,
                ctx,
            )?,

            // No-ops at SQL level — qualification and USING semantics are
            // resolved in metadata by the refiner, not materialized in SQL.
            UnaryRelationalOperator::Qualify
            | UnaryRelationalOperator::Using { .. }
            | UnaryRelationalOperator::UsingAll => current.project_all()?,

            UnaryRelationalOperator::DmlTerminal { .. } => {
                unreachable!("DmlTerminal intercepted by dml.rs before r_lower_pipe")
            }

            other => {
                return Err(DelightQLError::ParseError {
                    message: format!(
                        "r_lower_pipe: unimplemented pipe operator: {:?}",
                        std::mem::discriminant(&other)
                    ),
                    source: None,
                    subcategory: None,
                });
            }
        };

        if i == last_idx {
            return Ok(result);
        }
        // Demote for next segment
        current = result.demote()?;
    }

    unreachable!("segments is non-empty")
}

/// Lower a set operation (UNION ALL, INTERSECT, EXCEPT, etc.).
///
/// Each operand is already a `Builder<Projected>` (a complete query).
/// The result is a new `Builder<Projected>` wrapping the combined set-op.
/// Operands are folded left-to-right with the appropriate SQL set operator.
pub(super) fn r_lower_set_op(
    operands: Vec<Builder<Projected>>,
    operator: ast_addressed::SetOperator,
    correlation: PhaseBox<Option<ast_addressed::BooleanExpression>, Addressed>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // UnionCorresponding needs NULL-padding for columns not in all operands.
    if matches!(operator, ast_addressed::SetOperator::UnionCorresponding) {
        return r_lower_union_corresponding(operands, cpr_schema, ctx);
    }

    let mut iter = operands.into_iter();
    let first = iter.next().ok_or_else(|| DelightQLError::ParseError {
        message: "r_lower_set_op: empty operands".to_string(),
        source: None,
        subcategory: None,
    })?;

    let combiner = match operator {
        ast_addressed::SetOperator::UnionAllPositional
        | ast_addressed::SetOperator::SmartUnionAll => {
            Builder::union_all
                as fn(Builder<Projected>, Builder<Projected>) -> Result<Builder<Projected>>
        }
        ast_addressed::SetOperator::MinusCorresponding => Builder::except,
        ast_addressed::SetOperator::UnionCorresponding => unreachable!(),
    };

    iter.try_fold(first, |acc, next| combiner(acc, next))
}

/// Lower UnionCorresponding: pads each operand with NULLs for columns
/// it doesn't have, so all operands have the same column layout.
fn r_lower_union_corresponding(
    operands: Vec<Builder<Projected>>,
    cpr_schema: PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::sql_ast_v3::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SetOperator as SqlSetOp,
        TableExpression,
    };

    if operands.is_empty() {
        return Err(DelightQLError::ParseError {
            message: "r_lower_set_op: empty operands".to_string(),
            source: None,
            subcategory: None,
        });
    }

    // Get output column names (and declared types, for typing the NULL
    // pads) from the cpr_schema.
    let output_col_names: Vec<String> = match cpr_schema.get() {
        CprSchema::Resolved(cols) => cols.iter().map(|c| col_name(c).to_string()).collect(),
        _ => {
            // Fallback: positional union_all (no padding possible without schema).
            let mut iter = operands.into_iter();
            let first = iter.next().unwrap();
            return iter.try_fold(first, |acc, next| acc.union_all(next));
        }
    };
    // A pad is typed by the column it stands in for: the unified schema's
    // ColumnMetadata is cloned from a branch that HAS the column, carrying
    // its catalog declared_type. Untyped NULL pads break strict targets —
    // postgres resolves UNION types pairwise, and two pad-only branches
    // collapse the column to text before a typed branch arrives.
    let pad_types: std::collections::HashMap<String, String> = match cpr_schema.get() {
        CprSchema::Resolved(cols) => cols
            .iter()
            .filter_map(|c| c.pad_type().map(|t| (col_name(c).to_string(), t.to_string())))
            .collect(),
        _ => Default::default(),
    };

    let names = ctx.names.clone();

    // For each operand, build a padded SELECT with all output columns.
    let mut padded_queries: Vec<QueryExpression> = Vec::new();

    for op in operands {
        let op_col_names: Vec<String> = op
            .columns()
            .iter()
            .map(|c| col_name(c).to_string())
            .collect();

        let op_query = op.to_sql()?;

        // Wrap operand as subquery so we can SELECT specific columns from it.
        let op_alias = names.next_name("ucorr");
        let items: Vec<SelectItem> = output_col_names
            .iter()
            .map(|out_name| {
                if op_col_names.contains(out_name) {
                    // Operand has this column — reference it.
                    SelectItem::Expression {
                        expr: SqlDomainExpr::Column {
                            name: out_name.clone(),
                            qualifier: Some(crate::pipeline::sql_ast_v3::ColumnQualifier::table(
                                &op_alias,
                            )),
                        },
                        alias: Some(out_name.clone()),
                    }
                } else {
                    // Operand doesn't have this column — pad with NULL,
                    // typed by the column it stands in for when the
                    // catalog knows (CAST(NULL AS t) is still NULL).
                    let pad = match pad_types.get(out_name) {
                        Some(t) => SqlDomainExpr::cast(
                            SqlDomainExpr::literal(LiteralValue::Null),
                            t.clone(),
                        ),
                        None => SqlDomainExpr::literal(LiteralValue::Null),
                    };
                    SelectItem::Expression {
                        expr: pad,
                        alias: Some(out_name.clone()),
                    }
                }
            })
            .collect();

        let padded = crate::pipeline::sql_ast_v3::SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![TableExpression::subquery(op_query, &op_alias)])
            .build()
            .map_err(|e| DelightQLError::ParseError {
                message: format!("UnionCorresponding pad: {}", e),
                source: None,
                subcategory: None,
            })?;

        padded_queries.push(QueryExpression::Select(Box::new(padded)));
    }

    // UNION ALL all padded queries.
    let combined = padded_queries
        .into_iter()
        .reduce(|left, right| QueryExpression::SetOperation {
            op: SqlSetOp::UnionAll,
            left: Box::new(left),
            right: Box::new(right),
        })
        .unwrap();

    // Build output scope from cpr_schema.
    let scope_name = names.next_table_name("ucorr_out");
    let output_cols = columns_from_cpr_schema(cpr_schema.get(), &scope_name);

    Builder::from_frozen(combined, scope_name, output_cols, names).project_all()
}

// ---------------------------------------------------------------------------
// Pipe-segment handlers (called from r_lower_pipe)
// ---------------------------------------------------------------------------

/// Lower a projection: `|> (col1, col2)`.
///
/// Sets the SELECT list, transitioning Unprojected → Projected.
///
/// When `cpr_schema` is provided, uses it to fill in aliases for select items
/// that don't have one (e.g., JSON path expressions where the AST node carries
/// no alias but the refiner has computed one).
pub(super) fn r_lower_projection(
    builder: Builder<Unprojected>,
    expressions: Vec<ast_addressed::DomainExpression>,
    cpr_schema: Option<&PhaseBox<CprSchema, Addressed>>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // Lower expressions first — this processes computed expressions,
    // function calls, etc. into SQL items via qualify().
    let items: Vec<_> = expressions
        .into_iter()
        .map(|e| scalar::s_lower_select_item(e, &builder, ctx))
        .collect::<Result<_>>()?;

    // Now use CprSchema to fix up aliases. The CprSchema has the resolver's
    // authoritative output names. Apply them positionally to the lowered items.
    let mut items = if let Some(cpr) = cpr_schema {
        let cpr_columns = cpr_output_columns(cpr.get());
        let mut name_idx = 0;
        items
            .into_iter()
            .map(|item| match item {
                crate::pipeline::sql_ast_v3::SelectItem::Star
                | crate::pipeline::sql_ast_v3::SelectItem::QualifiedStar { .. } => {
                    name_idx += builder.columns().len();
                    item
                }
                crate::pipeline::sql_ast_v3::SelectItem::Expression { expr, alias } => {
                    let cpr_alias = cpr_columns
                        .get(name_idx)
                        .map(|c| cpr_display_name(c).to_string())
                        .or(alias);
                    name_idx += 1;
                    crate::pipeline::sql_ast_v3::SelectItem::Expression {
                        expr,
                        alias: cpr_alias,
                    }
                }
            })
            .collect()
    } else {
        items
    };

    // Check for hygienic column references
    for item in &items {
        if let crate::pipeline::sql_ast_v3::SelectItem::Expression { expr, .. } = item {
            if let crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } = expr {
                if builder
                    .columns()
                    .iter()
                    .any(|c| col_name(c) == name.as_str() && c.needs_hygienic_alias)
                {
                    return Err(DelightQLError::ParseError {
                        message: format!(
                            "Column '{}' is not available for projection (internal/hygienic column)",
                            name
                        ),
                        source: None,
                        subcategory: None,
                    });
                }
            }
        }
    }

    builder.add_projection(items)
}

/// Lower ORDER BY: `|> #(col1, col2 descending)`.
///
/// Adds ORDER BY terms to the builder, then projects all (SELECT *).
pub(super) fn r_lower_order_by(
    builder: Builder<Unprojected>,
    specs: Vec<ast_addressed::OrderingSpec>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{OrderDirection as SqlDir, OrderTerm};

    // Ensure we're not Frozen before lowering expressions — add_order_by
    // on Frozen wraps as subquery, changing the scope. Expressions must be
    // qualified against the post-wrap scope.
    let builder = builder.ensure_not_frozen()?;

    let terms: Vec<OrderTerm> = specs
        .into_iter()
        .map(|spec| {
            let expr = scalar::s_lower_expression(spec.column, &builder, ctx)?;
            let dir = spec.direction.map(|d| match d {
                ast_addressed::OrderDirection::Ascending => SqlDir::Asc,
                ast_addressed::OrderDirection::Descending => SqlDir::Desc,
            });
            Ok(OrderTerm::new(expr, dir))
        })
        .collect::<Result<_>>()?;

    builder.add_order_by(terms)?.project_all()
}

/// Lower Modulo operator: DISTINCT (`ModuloSpec::Columns`) or GROUP BY (`ModuloSpec::GroupBy`).
fn r_lower_modulo(
    builder: Builder<Unprojected>,
    spec: ast_addressed::ModuloSpec,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    match spec {
        ast_addressed::ModuloSpec::Columns(exprs) => {
            // |> %(cols) → SELECT DISTINCT cols
            let items: Vec<_> = exprs
                .into_iter()
                .map(|e| scalar::s_lower_select_item(e, &builder, ctx))
                .collect::<Result<_>>()?;
            let projected = builder.add_projection(items)?;
            projected.add_distinct()
        }

        ast_addressed::ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            delegates,
        } => {
            let any_ordered = delegates.iter().any(|d| !d.order.is_empty());

            // All-arbitrary (empty-order) delegates lower as bare columns,
            // exactly as the old `~?` arbitrary did (Phase 0/1a behavior).
            if !any_ordered {
                // Arbitrary path lowers payloads as bare columns via the group-by
                // spec. The payload OutputDomainExpressions thread through with
                // their stamps intact so each arb item aliases from its own
                // delegate stamp (Batch 13) — no positional re-threading.
                let arbitrary = delegates
                    .into_iter()
                    .flat_map(|d| d.payload)
                    .collect();
                return r_lower_group_by_spec(
                    builder,
                    reducing_by,
                    reducing_on,
                    arbitrary,
                    cpr_schema,
                    ctx,
                );
            }

            // A single ordered delegate with no aggregates is the 1-arity
            // degenerate of the N-way join: one `row_number()=1` relation, no
            // join to make.
            if reducing_on.is_empty() && delegates.len() == 1 {
                let delegate = delegates.into_iter().next().unwrap();
                return r_lower_single_ordered_delegate(
                    builder,
                    reducing_by,
                    delegate,
                    cpr_schema,
                    ctx,
                );
            }

            // General case: an aggregate relation (when there are aggregates)
            // plus one `row_number()=1` relation per delegate, joined on the
            // group key.
            r_lower_n_way_delegate_join(
                builder,
                reducing_by,
                reducing_on,
                delegates,
                cpr_schema,
                ctx,
            )
        }
    }
}

/// Build one delegate relation — the `row_number()=1` filtered rows for a single
/// delegate — and return it (pre-projection) as a `Builder<Unprojected>`:
///
/// ```sql
/// SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY <keys> ORDER BY <order>)
///                           AS __dql_delegate_rn
///                 FROM <source> )
/// WHERE __dql_delegate_rn = 1
/// ```
///
/// This is the shared primitive: the single-delegate lowering projects one of
/// these; the N-way join builds one per delegate and joins them on the group
/// key. Partition/order use bare column names (they resolve against the wrapped
/// subquery). An empty `order` (arbitrary delegate) yields a window with no
/// ORDER BY — one arbitrary row per group.
fn build_delegate_relation(
    builder: Builder<Unprojected>,
    reducing_by: &[ast_addressed::OutputDomainExpression],
    order: &[ast_addressed::OrderingSpec],
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast_v3::{
        ordering::OrderDirection, BinaryOperator, DomainExpression as SqlDomainExpr, SqlPredicate,
    };

    const RN: &str = "__dql_delegate_rn";

    // Bare-column form of a lowered expression (strip qualifier) so the window
    // spec resolves against the wrapped subquery, mirroring the intersect path.
    let bare = |expr: ast_addressed::DomainExpression,
                q: &dyn Qualify|
     -> Result<SqlDomainExpr> {
        Ok(match scalar::s_lower_expression(expr, q, ctx)? {
            SqlDomainExpr::Column { name, .. } => SqlDomainExpr::column(name),
            other => other,
        })
    };

    // Keys carry an output stamp now (slice 4); the PARTITION BY reads `.expr`.
    let partition: Vec<SqlDomainExpr> = reducing_by
        .iter()
        .map(|e| bare(e.expr.clone(), &builder))
        .collect::<Result<_>>()?;
    let sql_order: Vec<(SqlDomainExpr, OrderDirection)> = order
        .iter()
        .map(|spec| {
            let col = bare(spec.column.clone(), &builder)?;
            let dir = match spec.direction {
                Some(ast_addressed::OrderDirection::Descending) => OrderDirection::Desc,
                _ => OrderDirection::Asc,
            };
            Ok((col, dir))
        })
        .collect::<Result<_>>()?;

    // Tag each row with row_number, wrap as a subquery, filter to the first.
    builder
        .project_all()?
        .add_window_column("ROW_NUMBER", vec![], partition, sql_order, RN)?
        .demote()?
        .add_where(SqlPredicate::new(SqlDomainExpr::Binary {
            left: Box::new(SqlDomainExpr::column(RN)),
            op: BinaryOperator::Equal,
            right: Box::new(SqlDomainExpr::literal(LiteralValue::Number("1".to_string()))),
        }))
}

/// Lower a single ordered delegate selection (no aggregates): the 1-arity
/// degenerate of the N-way join — build one delegate relation, project it
/// directly (no join). Output items are projected against the post-wrap builder,
/// whose scope carries `prior_identities` so qualification resolves
/// automatically.
fn r_lower_single_ordered_delegate(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::OutputDomainExpression>,
    delegate: ast_addressed::DelegateSpec,
    _cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let filtered = build_delegate_relation(builder, &reducing_by, &delegate.order, ctx)?;

    // Output = group keys + delegate payload, lowered against the POST-WRAP
    // builder so identities resolve through the wrap chain (trust the builder).
    // Group keys now carry their own output stamp (slice 4): each key aliases
    // from its stamp instead of being threaded by position over the cpr schema.
    // Keys are always lowered (a key still projects even if it stamped `None`);
    // the alias is attached only when the stamp is `Some` — byte-identical to
    // the retired offset-0 positional alias re-attach, whose arithmetic silently
    // shifted whenever a `None`-stamped key shortened the flat schema.
    let mut output_items: Vec<crate::pipeline::sql_ast_v3::SelectItem> = Vec::new();
    for ode in reducing_by {
        let ast_addressed::OutputDomainExpression { expr, output } = ode;
        let mut item = scalar::s_lower_select_item(expr, &filtered, ctx)?;
        if let Some(col) = output.get() {
            alias_unaliased(&mut item, cpr_display_name(col));
        }
        output_items.push(item);
    }
    // Each payload expression carries its own output stamp: `None` = the
    // resolver decided it yields no output column (a `(*)` payload that
    // duplicates a group key, already emitted in group position), `Some(col)`
    // = emit, aliased from the stamp. The dedup no longer lives here.
    for ode in delegate.payload {
        let Some(col) = ode.output.get() else {
            continue; // resolver stamped None — no output column
        };
        let name = cpr_display_name(col).to_string();
        let mut item = scalar::s_lower_select_item(ode.expr, &filtered, ctx)?;
        alias_unaliased(&mut item, &name);
        output_items.push(item);
    }

    filtered.add_projection(output_items)
}

/// Lower the general N-way delegate join: a GROUP BY relation (when there are
/// aggregates) plus one `row_number()=1` relation per ordered delegate, all
/// joined on the group key. This is the canonical decomposition; the single
/// ordered delegate with no aggregates is its 1-arity degenerate (handled by
/// `r_lower_single_ordered_delegate` — no join to make with one relation).
///
/// ```sql
/// SELECT agg.k, agg.<aggs>, d0.<payload0>, d1.<payload1>
/// FROM   (SELECT k, <aggs> FROM src GROUP BY k)                         AS agg
/// JOIN   (SELECT * FROM (.. ROW_NUMBER() OVER (PARTITION BY k ORDER BY o0)) WHERE rn=1) AS d0
///          ON agg.k IS NOT DISTINCT FROM d0.k
/// JOIN   (.. ORDER BY o1 .. WHERE rn=1)                                 AS d1
///          ON agg.k IS NOT DISTINCT FROM d1.k
/// ```
///
/// Each relation is built from a frozen copy of the source. The relations share
/// the source column names, so the join tree is kept flat (no intermediate
/// subquery wrap, via `Builder::from_joins`) and every output column is
/// explicitly qualified to the operand that owns it.
fn r_lower_n_way_delegate_join(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::OutputDomainExpression>,
    reducing_on: Vec<ast_addressed::OutputDomainExpression>,
    delegates: Vec<ast_addressed::DelegateSpec>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{
        BinaryOperator, ColumnQualifier, DomainExpression as SqlDomainExpr, JoinCondition,
        JoinType, SelectItem,
    };

    // Group-key column names. Each operand wraps the source as a subquery, so a
    // key must survive as a named column to be joined on. Expression keys
    // (e.g. `lower(name)`) combined with ordered delegates are a later slice.
    let key_names: Vec<String> = reducing_by
        .iter()
        .map(
            |e| match scalar::s_lower_expression(e.expr.clone(), &builder, ctx)? {
                SqlDomainExpr::Column { name, .. } => Ok(name),
                _ => Err(DelightQLError::ParseError {
                    message: "N-way delegate join requires plain column group keys \
                              (expression keys with ordered delegates are not yet supported)"
                        .to_string(),
                    source: None,
                    subcategory: None,
                }),
            },
        )
        .collect::<Result<_>>()?;
    let key_set: std::collections::HashSet<&str> = key_names.iter().map(|s| s.as_str()).collect();

    // Freeze the source once and rebuild a fresh frozen Builder per relation.
    // (Duplicating the source subquery is correct; CTE-hoisting it is a future
    // perf peephole, not a correctness concern.)
    let cols = builder.columns().to_vec();
    let names = builder.names().clone();
    let src = builder.project_all()?.to_sql()?;
    let fresh_source = |suffix: &str| {
        Builder::from_frozen(
            src.clone(),
            names.next_table_name(suffix),
            cols.clone(),
            names.clone(),
        )
    };

    let has_agg = !reducing_on.is_empty();

    // Operands in output order: [aggregate relation?] then one per delegate.
    let mut operands: Vec<super::builder::JoinOperand> = Vec::new();

    if has_agg {
        let agg = r_lower_group_by_spec(
            fresh_source("agg"),
            reducing_by.clone(),
            reducing_on,
            vec![],
            cpr_schema,
            ctx,
        )?;
        operands.push(agg.demote()?.into_join_operand()?);
    }

    // Each delegate → one `row_number()=1` relation. Remember its operand index
    // and payload so output columns can be mapped back to it.
    let mut delegate_slots: Vec<(usize, Vec<ast_addressed::OutputDomainExpression>)> = Vec::new();
    for d in delegates {
        let rel = build_delegate_relation(fresh_source("dlg"), &reducing_by, &d.order, ctx)?;
        delegate_slots.push((operands.len(), d.payload));
        operands.push(rel.into_join_operand()?);
    }

    // Each operand's post-wrap qualifier (à la intersect's left/right_qual).
    let quals: Vec<String> = operands
        .iter()
        .map(|op| {
            op.columns
                .first()
                .and_then(col_qualifier)
                .unwrap_or("_op")
                .to_string()
        })
        .collect();
    let anchor_qual = quals[0].clone();

    // Join conditions: anchor.key IS NOT DISTINCT FROM op_i.key (NULL-safe), one
    // per non-anchor operand.
    let conditions: Vec<(JoinType, JoinCondition)> = quals
        .iter()
        .skip(1)
        .map(|op_qual| {
            let conds: Vec<SqlDomainExpr> = key_names
                .iter()
                .map(|k| SqlDomainExpr::Binary {
                    left: Box::new(SqlDomainExpr::with_qualifier(
                        ColumnQualifier::table(&anchor_qual),
                        k,
                    )),
                    op: BinaryOperator::IsNotDistinctFrom,
                    right: Box::new(SqlDomainExpr::with_qualifier(
                        ColumnQualifier::table(op_qual),
                        k,
                    )),
                })
                .collect();
            (JoinType::Inner, JoinCondition::On(SqlDomainExpr::and(conds)))
        })
        .collect();

    // Output projection in cpr order: keys, aggregates, then per-delegate
    // payloads — each explicitly qualified to the operand that owns it, so the
    // qualifier-aware `find_input_column` attaches correct provenance even
    // though all operands share the source column names.
    let mut output_items: Vec<SelectItem> = Vec::new();

    // (a) group keys — from the anchor operand, each aliased from its OWN output
    // stamp (slice 4). The n-way path admits only plain-column keys (checked
    // above), so every key stamps `Some`; aliasing from the stamp is
    // byte-identical to the retired offset-0 positional re-attach, which
    // pulled the same name from the cpr schema by position.
    for (k, ode) in key_names.iter().zip(reducing_by.iter()) {
        let mut item = SelectItem::Expression {
            expr: SqlDomainExpr::with_qualifier(ColumnQualifier::table(&anchor_qual), k),
            alias: None,
        };
        if let Some(col) = ode.output.get() {
            alias_unaliased(&mut item, cpr_display_name(col));
        }
        output_items.push(item);
    }

    // (b) aggregates — from the aggregate operand (operands[0] when present).
    // Its columns are keys + aggregate outputs; the aggregates are the columns
    // whose names are not group keys, in order. Each aggregate column already
    // carries the resolver's chosen name (the agg subquery aliased it from its
    // own reducing_on stamp), so it self-aliases by its column name — again
    // byte-identical to the retired positional thread.
    if has_agg {
        for col in &operands[0].columns {
            let name = col_name(col);
            if !key_set.contains(name) {
                let mut item = SelectItem::Expression {
                    expr: SqlDomainExpr::with_qualifier(ColumnQualifier::table(&anchor_qual), name),
                    alias: None,
                };
                alias_unaliased(&mut item, name);
                output_items.push(item);
            }
        }
    }

    // (c) delegate payloads — each from its own operand. Each payload
    // expression carries its own output stamp: `None` = the resolver decided
    // it yields no output column (duplicates a group key already emitted in
    // group position), `Some(col)` = emit, aliased from the stamp. The dedup
    // no longer lives here; the stamp replaces the `key_set` membership check.
    for (op_idx, payload) in &delegate_slots {
        let op_qual = &quals[*op_idx];
        for ode in payload {
            let Some(col) = ode.output.get() else {
                continue; // resolver stamped None — no output column
            };
            let mut item =
                match scalar::s_lower_expression(ode.expr.clone(), &operands[*op_idx], ctx)? {
                    SqlDomainExpr::Column { name, .. } => SelectItem::Expression {
                        expr: SqlDomainExpr::with_qualifier(ColumnQualifier::table(op_qual), &name),
                        alias: None,
                    },
                    other => SelectItem::Expression {
                        // Non-column payload: emit the lowered expression as-is.
                        expr: other,
                        alias: None,
                    },
                };
            alias_unaliased(&mut item, cpr_display_name(col));
            output_items.push(item);
        }
    }

    // Keys and aggregates are aliased inline from their stamps / self-names
    // (slice 4); payloads arrive aliased from their delegate stamps. The
    // positional alias re-attach is retired.

    // Assemble the flat join and project.
    let joined = Builder::from_joins(operands, conditions);
    joined.add_projection(output_items)
}

/// Lower GROUP BY with keys and aggregate reductions.
///
/// Handles three cases:
/// 1. Simple aggregates (`count:(*), sum:(x)`) — straight GROUP BY
/// 2. Tree group in reducing_on without CTE (`{first_name, last_name}`) — GROUP BY + aggregate wrapper
/// 3. Tree group in reducing_on with CTE (nested `~>`) — CTE chain via push_cte
fn r_lower_group_by_spec(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::OutputDomainExpression>,
    reducing_on: Vec<ast_addressed::OutputDomainExpression>,
    arbitrary: Vec<ast_addressed::OutputDomainExpression>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use super::builder::GroupBySpec;

    // Group keys carry an output stamp now (slice 4), but this straight GROUP BY
    // path emits keys BARE (no alias) — verified byte-for-byte against the corpus
    // (e.g. `SELECT products.category_id, ...` renders unaliased today). Unlike
    // the delegate paths (which aliased keys via the retired positional thread),
    // nothing aliased these keys, so the stamp is unwrapped at the boundary and
    // the key SELECT items are lowered from `.expr` exactly as before. (Aliasing
    // them from stamps here would add redundant `AS <key>` clauses — a real diff,
    // NOT the sanctioned misalignment fix — so it is deliberately not done.)
    let reducing_by: Vec<ast_addressed::DomainExpression> =
        reducing_by.into_iter().map(|ode| ode.expr).collect();

    // Check for pivot expressions
    let has_pivot = reducing_on
        .iter()
        .any(|ode| matches!(&ode.expr, ast_addressed::DomainExpression::PivotOf { .. }));

    if has_pivot {
        // Pivot is 1:N and node-schema-owned: r_lower_pivot indexes over its own
        // single expression list, so unwrap the stamps at the boundary (stamps
        // are None for pivots anyway).
        let reducing_on = reducing_on.into_iter().map(|ode| ode.expr).collect();
        return r_lower_pivot(builder, reducing_by, reducing_on, cpr_schema, ctx);
    }

    // Check if any reducing_by expression is a tree group (Curly/MetadataTreeGroup with
    // nested reductions). This pattern: `|> %( {key, "nested": ~> {...}} as tg ~> count:(*) )`
    let by_needs_cte = reducing_by.iter().any(|e| match e {
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
            cte_requirements: Some(req),
            ..
        }) => req.needs_cte,
        ast_addressed::DomainExpression::Function(
            ast_addressed::FunctionExpression::MetadataTreeGroup { .. },
        ) => true,
        _ => false,
    });

    if by_needs_cte {
        // Tree-group-in-reducing_by lowering owns its output schema; unwrap the
        // reducing_on stamps at the boundary.
        let reducing_on = reducing_on.into_iter().map(|ode| ode.expr).collect();
        return tree_group::r_lower_tree_group_in_reducing_by(
            builder,
            reducing_by,
            reducing_on,
            cpr_schema,
            ctx,
        );
    }

    // Check if any reducing_on expression is a Curly or MetadataTreeGroup needing CTEs
    let needs_cte = reducing_on.iter().any(|ode| match &ode.expr {
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
            cte_requirements: Some(req),
            ..
        }) => req.needs_cte,
        ast_addressed::DomainExpression::Function(
            ast_addressed::FunctionExpression::MetadataTreeGroup {
                cte_requirements: Some(req),
                ..
            },
        ) => req.needs_cte,
        // MetadataTreeGroup always needs CTEs even without explicit requirements
        ast_addressed::DomainExpression::Function(
            ast_addressed::FunctionExpression::MetadataTreeGroup { .. },
        ) => true,
        _ => false,
    });

    if needs_cte {
        // Tree-group CTE lowering owns its output schema; unwrap the stamps.
        let reducing_on = reducing_on.into_iter().map(|ode| ode.expr).collect();
        return tree_group::r_lower_tree_group_cte(
            builder,
            reducing_by,
            reducing_on,
            cpr_schema,
            ctx,
        );
    }

    // Lower GROUP BY keys → SelectItems
    let keys: Vec<_> = reducing_by
        .into_iter()
        .map(|e| scalar::s_lower_select_item(e, &builder, ctx))
        .collect::<Result<_>>()?;

    // Lower aggregate reductions → SelectItems, aliasing each from its OWN output
    // stamp. The resolver assigns names like "count", "count_2" to aggregate
    // expressions; the stamp carries that decision on the expression, so no
    // positional cpr threading is needed. Curly expressions get the aggregate
    // wrapper; others use normal lowering.
    let mut aggregates: Vec<crate::pipeline::sql_ast_v3::SelectItem> = Vec::new();
    for ode in reducing_on {
        let ast_addressed::OutputDomainExpression { expr, output } = ode;
        let mut item = tree_group::s_lower_reducing_on_item(expr, &builder, ctx)?;
        if let Some(col) = output.get() {
            alias_unaliased(&mut item, cpr_display_name(col));
        }
        aggregates.push(item);
    }

    // Lower arbitrary delegate columns (bare `<~`, formerly `~?`) and stamp
    // each with the arbitrary-witness form (`__dql_arbitrary`). This is the
    // only site that knows the user wrote bare `<~`, so the FORM is chosen
    // here; the SPELLING is per-dialect — canonical/sqlite unwraps to the
    // bare column (relaxed GROUP BY), strict targets render `any_value(...)`.
    // Ordered delegates (`<~ #(order)`) lower via the N-way join, not here.
    // Each arb item aliases from its own delegate stamp; a `None` stamp = the
    // resolver decided this payload yields no column (dup-of-key already emitted
    // in group position), so it is skipped rather than positionally threaded.
    for ode in arbitrary {
        use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};
        let ast_addressed::OutputDomainExpression { expr, output } = ode;
        let Some(col) = output.get() else {
            continue; // resolver stamped None — no output column
        };
        let name = cpr_display_name(col).to_string();
        let mut item = match scalar::s_lower_select_item(expr, &builder, ctx)? {
            SelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: SqlDomainExpr::function(
                    crate::pipeline::naming::INTERNAL_ARBITRARY,
                    vec![expr],
                ),
                alias,
            },
            other => other,
        };
        alias_unaliased(&mut item, &name);
        aggregates.push(item);
    }

    builder.add_group_by(GroupBySpec { keys, aggregates })
}

/// Lower pivot: `|> %(keys ~> value_col of pivot_key)`.
///
/// Generates a JSON-based CTE pattern:
///   1. Optional _preagg CTE (when value columns contain aggregates)
///   2. _prepivot CTE with json_group_object
///   3. Outer SELECT with json_extract for each pivot value
fn r_lower_pivot(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::DomainExpression>,
    reducing_on: Vec<ast_addressed::DomainExpression>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // Resolved only — pivot trusts a fully resolved output schema and aliases
    // nothing otherwise (unlike cpr_output_columns, which admits partial schemas).
    let output_columns: &[ColumnMetadata] = match cpr_schema.get() {
        CprSchema::Resolved(cols) => cols,
        _ => &[],
    };

    let (pivot_groups, key_to_group, regular_aggs) = classify_pivot_groups(&reducing_on);

    let needs_preagg = pivot_groups.iter().any(|g| {
        g.value_columns
            .iter()
            .any(|(_, expr)| matches!(expr, ast_addressed::DomainExpression::Function(_)))
    });

    // Pre-lower all AST expressions against the builder's scope.
    // Builder is consumed by project_all, so lower everything first.
    let lowered = pre_lower_pivot_expressions(
        &builder,
        &reducing_by,
        &pivot_groups,
        &regular_aggs,
        needs_preagg,
        ctx,
    )?;

    let packet_aliases: Vec<String> = if pivot_groups.len() == 1 {
        vec!["_pivot_packet".to_string()]
    } else {
        (0..pivot_groups.len())
            .map(|i| format!("_pivot_packet_{}", i))
            .collect()
    };

    // Build CTE chain via push_cte
    let mut projected = builder.project_all()?;

    if needs_preagg {
        projected = push_preagg_cte(
            projected,
            &lowered.group_key_names,
            &pivot_groups,
            &lowered.value_col_sqls,
        )?;
    }

    projected = push_prepivot_cte(
        projected,
        &lowered,
        &pivot_groups,
        needs_preagg,
        &packet_aliases,
    )?;

    // Outer SELECT: json_extract per pivot value
    let outer_items = build_pivot_outer_select(
        &reducing_on,
        &lowered.group_key_names,
        &regular_aggs,
        &key_to_group,
        &packet_aliases,
        output_columns,
    );

    projected.add_projection(outer_items)
}

// ---------------------------------------------------------------------------
// Pivot helpers
// ---------------------------------------------------------------------------

struct PivotGroup {
    pivot_key_name: String,
    pivot_key_expr: ast_addressed::DomainExpression,
    value_columns: Vec<(String, ast_addressed::DomainExpression)>,
}

/// All pre-lowered SQL expressions needed by the pivot CTE chain.
struct PivotLowered {
    group_key_names: Vec<String>,
    pivot_key_sqls: Vec<crate::pipeline::sql_ast_v3::DomainExpression>,
    value_col_sqls: Vec<Vec<crate::pipeline::sql_ast_v3::DomainExpression>>,
    value_col_names: Vec<Vec<String>>,
    regular_agg_sqls: Vec<(String, crate::pipeline::sql_ast_v3::DomainExpression)>,
}

/// Parse reducing_on into PivotGroup structs, a key→group index map,
/// and regular (non-pivot) aggregate expressions.
fn classify_pivot_groups(
    reducing_on: &[ast_addressed::DomainExpression],
) -> (
    Vec<PivotGroup>,
    std::collections::HashMap<String, usize>,
    Vec<(String, ast_addressed::DomainExpression)>,
) {
    let mut pivot_groups: Vec<PivotGroup> = Vec::new();
    let mut key_to_group: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut regular_aggs: Vec<(String, ast_addressed::DomainExpression)> = Vec::new();
    let mut agg_idx = 0;

    for expr in reducing_on {
        match expr {
            ast_addressed::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let key_name =
                    extract_pivot_lvar_name(pivot_key).unwrap_or_else(|| "pivot_key".to_string());
                let val_name =
                    extract_pivot_lvar_name(value_column).unwrap_or_else(|| "value".to_string());

                if let Some(&idx) = key_to_group.get(&key_name) {
                    pivot_groups[idx]
                        .value_columns
                        .push((val_name, value_column.as_ref().clone()));
                } else {
                    let idx = pivot_groups.len();
                    key_to_group.insert(key_name.clone(), idx);
                    pivot_groups.push(PivotGroup {
                        pivot_key_name: key_name,
                        pivot_key_expr: pivot_key.as_ref().clone(),
                        value_columns: vec![(val_name, value_column.as_ref().clone())],
                    });
                }
            }
            other => {
                let alias = format!("_agg_{}", agg_idx);
                regular_aggs.push((alias, other.clone()));
                agg_idx += 1;
            }
        }
    }

    (pivot_groups, key_to_group, regular_aggs)
}

/// Lower all AST expressions against the builder's scope before it is consumed
/// by `project_all()`.
fn pre_lower_pivot_expressions(
    builder: &Builder<Unprojected>,
    reducing_by: &[ast_addressed::DomainExpression],
    pivot_groups: &[PivotGroup],
    regular_aggs: &[(String, ast_addressed::DomainExpression)],
    needs_preagg: bool,
    ctx: &TransformCtx,
) -> Result<PivotLowered> {
    use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpr;

    let group_key_names: Vec<String> = reducing_by
        .iter()
        .filter_map(|e| extract_pivot_lvar_name(e))
        .collect();

    let pivot_key_sqls: Vec<SqlDomainExpr> = pivot_groups
        .iter()
        .map(|g| scalar::s_lower_expression(g.pivot_key_expr.clone(), builder, ctx))
        .collect::<Result<_>>()?;

    let mut value_col_sqls: Vec<Vec<SqlDomainExpr>> = Vec::new();
    for group in pivot_groups {
        let mut group_sqls = Vec::new();
        for (_, val_expr) in &group.value_columns {
            group_sqls.push(scalar::s_lower_expression(val_expr.clone(), builder, ctx)?);
        }
        value_col_sqls.push(group_sqls);
    }

    let regular_agg_sqls: Vec<(String, SqlDomainExpr)> = if !needs_preagg {
        regular_aggs
            .iter()
            .map(|(alias, expr)| {
                let sql = scalar::s_lower_expression(expr.clone(), builder, ctx)?;
                Ok((alias.clone(), sql))
            })
            .collect::<Result<_>>()?
    } else {
        Vec::new()
    };

    let value_col_names: Vec<Vec<String>> = pivot_groups
        .iter()
        .map(|g| g.value_columns.iter().map(|(n, _)| n.clone()).collect())
        .collect();

    Ok(PivotLowered {
        group_key_names,
        pivot_key_sqls,
        value_col_sqls,
        value_col_names,
        regular_agg_sqls,
    })
}

/// Push the _preagg CTE: GROUP BY (keys + pivot_keys), aggregate value columns.
fn push_preagg_cte(
    projected: Builder<Projected>,
    group_key_names: &[String],
    pivot_groups: &[PivotGroup],
    value_col_sqls: &[Vec<crate::pipeline::sql_ast_v3::DomainExpression>],
) -> Result<Builder<Projected>> {
    use super::builder::CteBody;
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    let preagg_gk_names: Vec<String> = group_key_names.to_vec();
    let preagg_pk_names: Vec<String> = pivot_groups
        .iter()
        .map(|g| g.pivot_key_name.clone())
        .collect();
    let preagg_vals: Vec<Vec<SqlDomainExpr>> = value_col_sqls.to_vec();

    projected.push_cte(move |input| {
        let mut items = Vec::new();
        for n in &preagg_gk_names {
            items.push(SelectItem::expression(SqlDomainExpr::column(n)));
        }
        for n in &preagg_pk_names {
            items.push(SelectItem::expression(SqlDomainExpr::column(n)));
        }

        let mut val_aliases = Vec::new();
        for (gi, group_sqls) in preagg_vals.iter().enumerate() {
            for (ci, sql) in group_sqls.iter().enumerate() {
                let alias = format!("_pivot_val_{}_{}", gi, ci);
                let unqualified = strip_sql_qualifiers(sql);
                items.push(SelectItem::expression_with_alias(unqualified, &alias));
                val_aliases.push(alias);
            }
        }

        let mut group_by: Vec<SqlDomainExpr> = Vec::new();
        for n in &preagg_gk_names {
            group_by.push(SqlDomainExpr::column(n));
        }
        for n in &preagg_pk_names {
            group_by.push(SqlDomainExpr::column(n));
        }

        let from = table_name_sql(input.scope_name()).to_string();
        let query = crate::pipeline::sql_ast_v3::SelectBuilder::new()
            .set_select(items)
            .from_tables(vec![TableExpression::table(&from)])
            .group_by(group_by)
            .build()
            .map_err(|e| DelightQLError::ParseError {
                message: e,
                source: None,
                subcategory: None,
            })?;

        let mut out = preagg_gk_names.clone();
        out.extend(preagg_pk_names.clone());
        out.extend(val_aliases);
        Ok(CteBody {
            query: crate::pipeline::sql_ast_v3::QueryExpression::Select(Box::new(query)),
            output_columns: out,
        })
    })
}

/// Push the _prepivot CTE: json_group_object aggregation.
fn push_prepivot_cte(
    projected: Builder<Projected>,
    lowered: &PivotLowered,
    pivot_groups: &[PivotGroup],
    needs_preagg: bool,
    packet_aliases: &[String],
) -> Result<Builder<Projected>> {
    use super::builder::CteBody;
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    let gk_names = lowered.group_key_names.clone();
    let gk_sqls: Vec<SqlDomainExpr> = lowered
        .group_key_names
        .iter()
        .map(|n| SqlDomainExpr::column(n))
        .collect();

    let pk_sqls: Vec<SqlDomainExpr> = lowered
        .pivot_key_sqls
        .iter()
        .map(|sql| strip_sql_qualifiers(sql))
        .collect();

    let val_exprs: Vec<Vec<SqlDomainExpr>> = if needs_preagg {
        pivot_groups
            .iter()
            .enumerate()
            .map(|(gi, g)| {
                g.value_columns
                    .iter()
                    .enumerate()
                    .map(|(ci, _)| SqlDomainExpr::column(&format!("_pivot_val_{}_{}", gi, ci)))
                    .collect()
            })
            .collect()
    } else {
        lowered
            .value_col_sqls
            .iter()
            .map(|group| group.iter().map(|sql| strip_sql_qualifiers(sql)).collect())
            .collect()
    };

    let reg_aggs: Vec<(String, SqlDomainExpr)> = if !needs_preagg {
        lowered
            .regular_agg_sqls
            .iter()
            .map(|(alias, sql)| (alias.clone(), strip_sql_qualifiers(sql)))
            .collect()
    } else {
        Vec::new()
    };

    let vcn = lowered.value_col_names.clone();
    let pa: Vec<String> = packet_aliases.to_vec();

    projected.push_cte(move |input| {
        let mut items = Vec::new();

        for k in &gk_sqls {
            items.push(SelectItem::expression(k.clone()));
        }
        for (alias, sql) in &reg_aggs {
            items.push(SelectItem::expression_with_alias(sql.clone(), alias));
        }

        for (gi, (key_sql, val_names)) in pk_sqls.iter().zip(vcn.iter()).enumerate() {
            let mut json_obj_args = Vec::new();
            for (ci, vn) in val_names.iter().enumerate() {
                json_obj_args.push(SqlDomainExpr::literal(LiteralValue::String(vn.clone())));
                json_obj_args.push(val_exprs[gi][ci].clone());
            }
            let json_obj = SqlDomainExpr::function("json_object", json_obj_args);
            let json_group =
                SqlDomainExpr::function("json_group_object", vec![key_sql.clone(), json_obj]);
            items.push(SelectItem::expression_with_alias(json_group, &pa[gi]));
        }

        let from = table_name_sql(input.scope_name()).to_string();
        let mut sb = crate::pipeline::sql_ast_v3::SelectBuilder::new()
            .set_select(items)
            .from_tables(vec![TableExpression::table(&from)]);
        if !gk_sqls.is_empty() {
            sb = sb.group_by(gk_sqls.clone());
        }
        let query = sb.build().map_err(|e| DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;

        let mut out: Vec<String> = gk_names.clone();
        for (alias, _) in &reg_aggs {
            out.push(alias.clone());
        }
        out.extend(pa.clone());
        Ok(CteBody {
            query: crate::pipeline::sql_ast_v3::QueryExpression::Select(Box::new(query)),
            output_columns: out,
        })
    })
}

/// Build the outer SELECT items: json_extract per pivot value column.
/// The seam carries typed columns; spelling is extracted at the alias
/// borders via `col_name` ("_unnamed" fallback chain).
fn build_pivot_outer_select(
    reducing_on: &[ast_addressed::DomainExpression],
    group_key_names: &[String],
    regular_aggs: &[(String, ast_addressed::DomainExpression)],
    key_to_group: &std::collections::HashMap<String, usize>,
    packet_aliases: &[String],
    output_columns: &[ColumnMetadata],
) -> Vec<crate::pipeline::sql_ast_v3::SelectItem> {
    use crate::pipeline::asts::core::literals::LiteralValue;
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    let mut outer_items = Vec::new();
    let mut col_idx = 0;

    for key_name in group_key_names {
        let alias = output_columns
            .get(col_idx)
            .map(|c| col_name(c).to_string())
            .unwrap_or_else(|| key_name.clone());
        outer_items.push(SelectItem::expression_with_alias(
            SqlDomainExpr::column(key_name),
            &alias,
        ));
        col_idx += 1;
    }

    let mut agg_iter = regular_aggs.iter();
    for expr in reducing_on {
        match expr {
            ast_addressed::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let key_name =
                    extract_pivot_lvar_name(pivot_key).unwrap_or_else(|| "pivot_key".to_string());
                let val_name =
                    extract_pivot_lvar_name(value_column).unwrap_or_else(|| "value".to_string());
                let group_idx = key_to_group[&key_name];

                for pivot_value in pivot_values {
                    let alias = output_columns
                        .get(col_idx)
                        .map(|c| col_name(c).to_string())
                        .unwrap_or_else(|| pivot_value.to_lowercase());
                    let path = format!("$.{}.{}", pivot_value, val_name);
                    // Provenance: compiler-internal packet read — the value
                    // came from a typed column and may be compared
                    // numerically downstream, so it must stay NATIVE json
                    // (never a per-dialect *_string respell).
                    let extract = SqlDomainExpr::function(
                        crate::pipeline::naming::INTERNAL_JSON_EXTRACT_RAW,
                        vec![
                            SqlDomainExpr::column(&packet_aliases[group_idx]),
                            SqlDomainExpr::literal(LiteralValue::String(path)),
                        ],
                    );
                    outer_items.push(SelectItem::expression_with_alias(extract, &alias));
                    col_idx += 1;
                }
            }
            _ => {
                if let Some((agg_alias, _)) = agg_iter.next() {
                    let alias = output_columns
                        .get(col_idx)
                        .map(|c| col_name(c).to_string())
                        .unwrap_or_else(|| agg_alias.clone());
                    outer_items.push(SelectItem::expression_with_alias(
                        SqlDomainExpr::column(agg_alias),
                        &alias,
                    ));
                    col_idx += 1;
                }
            }
        }
    }

    outer_items
}

/// Strip qualifiers from a SQL expression (for use inside CTEs where columns
/// are available unqualified from the source CTE).
fn strip_sql_qualifiers(
    expr: &crate::pipeline::sql_ast_v3::DomainExpression,
) -> crate::pipeline::sql_ast_v3::DomainExpression {
    use crate::pipeline::sql_ast_v3::DomainExpression as SqlExpr;
    match expr {
        SqlExpr::Column { name, .. } => SqlExpr::column(name),
        SqlExpr::Function {
            name,
            args,
            distinct,
        } => SqlExpr::Function {
            name: name.clone(),
            args: args.iter().map(strip_sql_qualifiers).collect(),
            distinct: *distinct,
        },
        SqlExpr::Binary { left, op, right } => SqlExpr::Binary {
            left: Box::new(strip_sql_qualifiers(left)),
            op: op.clone(),
            right: Box::new(strip_sql_qualifiers(right)),
        },
        SqlExpr::Unary { op, expr } => SqlExpr::Unary {
            op: op.clone(),
            expr: Box::new(strip_sql_qualifiers(expr)),
        },
        SqlExpr::Parens(inner) => SqlExpr::Parens(Box::new(strip_sql_qualifiers(inner))),
        other => other.clone(),
    }
}

/// Extract the base name from an Lvar (for pivot key/value column names).
fn extract_pivot_lvar_name(expr: &ast_addressed::DomainExpression) -> Option<String> {
    match expr {
        ast_addressed::DomainExpression::Lvar { name, .. } => Some(name.as_str().to_string()),
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            extract_pivot_lvar_name(inner)
        }
        _ => None,
    }
}

/// Lower map-cover: `|> $(fn:())(cols)`.
///
/// For each scope column: if it appears in `columns`, wrap it with `function`;
/// otherwise pass through unchanged. The curried function's existing arguments
/// are kept — the column value is prepended as the first argument.
pub(super) fn r_lower_map_cover(
    builder: Builder<Unprojected>,
    function: ast_addressed::FunctionExpression,
    columns: Vec<ast_addressed::DomainExpression>,
    conditioned_on: Option<Box<ast_addressed::BooleanExpression>>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem, WhenClause};

    let targets: Vec<String> = lvar_names(&columns);

    // Lower the guard condition once (if present)
    let sql_condition: Option<SqlDomainExpr> = match conditioned_on {
        Some(cond) => Some(super::scalar::s_lower_boolean(*cond, &builder, ctx)?.into_expr()),
        None => None,
    };

    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| {
            let name = col_name(c).to_string();
            if targets.contains(&name) {
                let col_expr = qualified_col_expr(c);
                let result = apply_fn_to_column(&function, col_expr.clone(), &name, &builder, ctx)?;
                // Wrap in CASE WHEN guard THEN fn(col) ELSE col END
                let final_expr = match &sql_condition {
                    Some(cond) => SqlDomainExpr::Case {
                        expr: None,
                        when_clauses: vec![WhenClause::new(cond.clone(), result)],
                        else_clause: Some(Box::new(col_expr)),
                    },
                    None => result,
                };
                Ok(SelectItem::Expression {
                    expr: final_expr,
                    alias: Some(name),
                })
            } else {
                Ok(passthrough_item(c))
            }
        })
        .collect::<Result<_>>()?;

    builder.add_projection(items)
}

/// Lower project-out: `|> -(cols)`.
///
/// Trusts the CprSchema — the resolver already determined which columns survive.
pub(super) fn r_lower_project_out(
    builder: Builder<Unprojected>,
    _expressions: Vec<ast_addressed::DomainExpression>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    _ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema.get());
    builder.add_projection(items)
}

/// Lower rename-cover: `|> *(old as new)`.
///
/// Trusts the CprSchema — the resolver already determined the output names.
pub(super) fn r_lower_rename_cover(
    builder: Builder<Unprojected>,
    _specs: Vec<ast_addressed::RenameSpec>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    _ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema.get());
    builder.add_projection(items)
}

/// Lower transform (basic-cover): `|> $$(expr as col)`.
///
/// Projects all scope columns, replacing those whose name matches a
/// transformation alias with the transformed expression in place.
pub(super) fn r_lower_transform(
    builder: Builder<Unprojected>,
    transformations: Vec<(ast_addressed::DomainExpression, String, Option<String>)>,
    conditioned_on: Option<Box<ast_addressed::BooleanExpression>>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem, WhenClause};

    let replacements: Vec<(String, Option<String>, ast_addressed::DomainExpression)> =
        transformations
            .into_iter()
            .map(|(expr, alias, qualifier)| (alias, qualifier, expr))
            .collect();

    // Lower the guard condition once (if present)
    let sql_condition: Option<SqlDomainExpr> = match conditioned_on {
        Some(cond) => Some(super::scalar::s_lower_boolean(*cond, &builder, ctx)?.into_expr()),
        None => None,
    };

    let items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| {
            if let Some((_, _, replacement_expr)) = replacements.iter().find(|(alias, qual, _)| {
                if *alias != col_name(c) {
                    return false;
                }
                match qual {
                    // Qualified: match against the column's original table name
                    // (not the current scope qualifier, which changes through joins)
                    Some(q) => match c.qualifier() {
                        TableName::Named(tn) => tn == q,
                        TableName::Fresh => false,
                    },
                    None => true, // unqualified matches any
                }
            }) {
                let col_expr = qualified_col_expr(c);
                let sql_expr = scalar::s_lower_expression(replacement_expr.clone(), &builder, ctx)?;
                // Wrap in CASE WHEN guard THEN new_val ELSE original END
                let final_expr = match &sql_condition {
                    Some(cond) => SqlDomainExpr::Case {
                        expr: None,
                        when_clauses: vec![WhenClause::new(cond.clone(), sql_expr)],
                        else_clause: Some(Box::new(col_expr)),
                    },
                    None => sql_expr,
                };
                Ok(SelectItem::Expression {
                    expr: final_expr,
                    alias: Some(col_name(c).to_string()),
                })
            } else {
                Ok(passthrough_item(c))
            }
        })
        .collect::<Result<_>>()?;

    builder.add_projection(items)
}

/// Lower embed-map-cover: `|> +$(fn:() as :"{@}_suffix")(cols)`.
///
/// Keeps all existing columns, then appends new columns by applying the
/// function to each target column with a templated alias name.
pub(super) fn r_lower_embed_map(
    builder: Builder<Unprojected>,
    function: ast_addressed::FunctionExpression,
    selector: ast_addressed::ColumnSelector,
    alias_template: Option<ast_addressed::ColumnAlias>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::operators::{ColumnAlias, ColumnSelector};
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    // Extract target column names from selector
    let target_names: Vec<String> = match &selector {
        ColumnSelector::Explicit(exprs) => lvar_names(exprs),
        ColumnSelector::Resolved { columns, .. } => columns.clone(),
        ColumnSelector::All => builder
            .columns()
            .iter()
            .map(|c| col_name(c).to_string())
            .collect(),
        _ => vec![],
    };

    // Resolve alias template
    let template_str = match &alias_template {
        Some(ColumnAlias::Template(t)) => Some(t.template.clone()),
        Some(ColumnAlias::Literal(s)) => Some(s.clone()),
        None => None,
    };

    // Extract function name for default alias generation
    let fn_name_for_alias = match &function {
        ast_addressed::FunctionExpression::Regular { name, .. }
        | ast_addressed::FunctionExpression::Curried { name, .. }
        | ast_addressed::FunctionExpression::Window { name, .. } => name.as_str().to_string(),
        ast_addressed::FunctionExpression::Lambda { .. } => "lambda".to_string(),
        _ => "fn".to_string(),
    };

    // Part 1: all existing columns pass through
    let mut items: Vec<SelectItem> = builder
        .columns()
        .iter()
        .map(|c| passthrough_item(c))
        .collect();

    // Part 2: for each target column, append fn(col) AS templated_name
    for target in &target_names {
        let col = builder
            .columns()
            .iter()
            .find(|c| col_name(c) == target.as_str());
        let col_expr = col
            .map(|c| qualified_col_expr(c))
            .unwrap_or_else(|| SqlDomainExpr::column(target));

        let fn_expr = apply_fn_to_column(&function, col_expr, target, &builder, ctx)?;

        let alias = match &template_str {
            Some(t) => t.replace("{@}", target),
            None => format!("{}_{}", target, fn_name_for_alias),
        };

        items.push(SelectItem::Expression {
            expr: fn_expr,
            alias: Some(alias),
        });
    }

    builder.add_projection(items)
}

/// Lower meta-ize: `|> ^` (basic) or `|> ^^` (detailed).
///
/// Synthesizes a VALUES relation from the source's column metadata.
/// - `^`  → columns: scope, column_name, ordinal
/// - `^^` → columns: scope, column_name, ordinal, data_type, nullable
pub(super) fn r_lower_meta_ize(
    builder: Builder<Unprojected>,
    detailed: bool,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::provenance::ColumnProvenance;
    use crate::pipeline::sql_ast_v3::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
        SetOperator,
    };

    // Read input columns from the source builder's scope
    let source_columns = builder.columns().to_vec();
    if source_columns.is_empty() {
        return Err(DelightQLError::ParseError {
            message: "MetaIze: input relation has no columns".to_string(),
            source: None,
            subcategory: None,
        });
    }

    // Output schema: the meta-ize output columns
    let scope_name = TableName::Named(SqlIdentifier::from("_meta"));
    let output_col_names: Vec<&str> = if detailed {
        vec!["scope", "column_name", "ordinal", "data_type", "nullable"]
    } else {
        vec!["scope", "column_name", "ordinal"]
    };

    // Build inline UNION ALL of single-row SELECTs instead of VALUES
    // (SQLite VALUES doesn't support column names, producing column1/column2/...)
    let mut rows_iter = source_columns.iter().enumerate();
    let (first_idx, first_col) = rows_iter.next().unwrap(); // non-empty checked above

    let make_row = |idx: usize, col: &ColumnMetadata| {
        let col_name = col
            .info
            .name()
            .or_else(|| col.info.original_name())
            .unwrap_or("?")
            .to_string();
        let scope = match col.qualifier() {
            TableName::Named(name) => name.to_string(),
            TableName::Fresh => "_".to_string(),
        };
        let mut vals = vec![
            SqlDomainExpr::literal(ast_addressed::LiteralValue::String(scope)),
            SqlDomainExpr::literal(ast_addressed::LiteralValue::String(col_name)),
            SqlDomainExpr::literal(ast_addressed::LiteralValue::Number((idx + 1).to_string())),
        ];
        if detailed {
            vals.push(SqlDomainExpr::literal(ast_addressed::LiteralValue::String(
                "unknown".to_string(),
            )));
            vals.push(SqlDomainExpr::literal(ast_addressed::LiteralValue::String(
                "true".to_string(),
            )));
        }
        vals
    };

    // First row: SELECT val AS scope, val AS column_name, val AS ordinal [, ...]
    let first_vals = make_row(first_idx, first_col);
    let first_select = SelectStatement::builder()
        .select_all(
            first_vals
                .into_iter()
                .zip(output_col_names.iter())
                .map(|(expr, name)| SelectItem::expression_with_alias(expr, *name))
                .collect(),
        )
        .build()
        .expect("meta-ize first SELECT");
    let mut query = QueryExpression::Select(Box::new(first_select));

    // Subsequent rows: SELECT val, val, val [, ...] (aliases inherited from first)
    for (idx, col) in rows_iter {
        let vals = make_row(idx, col);
        let select = SelectStatement::builder()
            .select_all(vals.into_iter().map(SelectItem::expression).collect())
            .build()
            .expect("meta-ize row SELECT");
        query = QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(query),
            right: Box::new(QueryExpression::Select(Box::new(select))),
        };
    }

    let columns: Vec<ColumnMetadata> = output_col_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            // Honest Fresh: meta-ize output columns are a synthetic name-as-data
            // vocabulary; scope_name is an internal scope, not a source table.
            ColumnMetadata::new(
                ColumnProvenance::from_column(*name),
                scope_name.clone(),
                Some(i),
            )
        })
        .collect();

    Ok(Builder::from_query(
        query,
        scope_name,
        columns,
        builder.names().fork(),
    ))
}

/// Lower witness: `|> +` or `|> \+`.
///
/// Generates:
///   `+`  → `SELECT EXISTS(SELECT 1 FROM (<source>)) AS "met"`
///   `\+` → `SELECT NOT EXISTS(SELECT 1 FROM (<source>)) AS "met"`
pub(super) fn r_lower_witness(
    builder: Builder<Unprojected>,
    exists: bool,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::asts::core::provenance::ColumnProvenance;
    use crate::pipeline::sql_ast_v3::{
        DomainExpression as SqlDomainExpr, QueryExpression, SelectItem, SelectStatement,
    };

    // Finalize the source into a query expression
    let names_fork = builder.names().fork();
    let source_query = builder.project_all()?.to_sql()?;

    // Build: EXISTS(<source>) or NOT EXISTS(<source>)
    let exists_expr = if exists {
        SqlDomainExpr::exists(source_query)
    } else {
        SqlDomainExpr::not_exists(source_query)
    };

    // Build: SELECT <exists_expr> AS "met"
    let select = SelectStatement::builder()
        .select(SelectItem::expression_with_alias(exists_expr, "met"))
        .build()
        .map_err(|e| DelightQLError::ParseError {
            message: format!("Witness: {}", e),
            source: None,
            subcategory: None,
        })?;

    let query = QueryExpression::Select(Box::new(select));
    let scope_name = TableName::Named(SqlIdentifier::from("_witness"));
    // Honest Fresh: "met" is the compiler-generated EXISTS result; "_witness" is a
    // synthetic scope, not a source table.
    let columns = vec![ColumnMetadata::new(
        ColumnProvenance::from_column("met"),
        scope_name.clone(),
        Some(0),
    )];

    Ok(Builder::from_query(query, scope_name, columns, names_fork))
}

/// Lower reposition: `|> *[col as pos]`.
///
/// Trusts the CprSchema — the resolver already computed the reordered column list.
pub(super) fn r_lower_reposition(
    builder: Builder<Unprojected>,
    cpr_schema: &CprSchema,
    _ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let items = select_items_from_cpr_schema(builder.columns(), cpr_schema);
    builder.add_projection(items)
}

/// Lower narrowing destructure: `|> .column{.field1, .field2}`.
///
/// Iterates a JSON array column via `json_each`, extracts named fields.
/// Output schema contains ONLY the extracted fields (no context carry-forward).
///
/// ```sql
/// SELECT json_extract(_narrow_0.value, '$.name') AS name,
///        json_extract(_narrow_0.value, '$.age') AS age
/// FROM (<source>) AS t_N, json_each(t_N."col") AS _narrow_0
/// ```
pub(super) fn r_lower_narrowing_destructure(
    builder: Builder<Unprojected>,
    column: String,
    fields: Vec<String>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    _ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, SelectItem,
    };

    let output_columns = cpr_output_columns(cpr_schema.get());

    builder.expand_with_json_each(
        &column,
        "_narrow",
        super::builder::JsonEachKind::Array,
        |_source_alias| vec![], // no context columns
        |tvf_alias| {
            let vref = SqlDomainExpr::with_qualifier(ColumnQualifier::table(tvf_alias), "value");
            fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let alias = output_columns
                        .get(i)
                        .map(|c| cpr_display_name(c).to_string())
                        .unwrap_or_else(|| field.rsplit('.').next().unwrap_or(field).to_string());
                    SelectItem::expression_with_alias(
                        SqlDomainExpr::function(
                            "json_extract",
                            vec![
                                vref.clone(),
                                SqlDomainExpr::literal(ast_addressed::LiteralValue::String(
                                    format!("$.{}", field),
                                )),
                            ],
                        ),
                        alias,
                    )
                })
                .collect()
        },
        &[],
    )
}

/// Lower interior drill-down: `|> .column(*)` or `|> .column(field1, field2)`.
///
/// Explodes an interior relation (tree group JSON array column) into rows
/// using `json_each`, carrying context columns through. This is the inverse
/// of tree-group aggregation.
///
/// ```sql
/// SELECT t_N.country,
///        json_extract(_drill_0.value, '$.first_name') AS first_name,
///        json_extract(_drill_0.value, '$.last_name') AS last_name
/// FROM (<source>) AS t_N, json_each(t_N."people") AS _drill_0
/// ```
fn r_lower_interior_drill_down(
    builder: Builder<Unprojected>,
    column: String,
    glob: bool,
    columns: Vec<String>,
    interior_schema: Option<Vec<crate::pipeline::asts::core::operators::InteriorColumnDef>>,
    groundings: Vec<(String, String)>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    _ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, SelectItem,
    };

    let schema = interior_schema.ok_or_else(|| DelightQLError::ParseError {
        message: format!(
            "InteriorDrillDown: no interior schema for column '{}'",
            column
        ),
        source: None,
        subcategory: None,
    })?;

    let interior_cols: Vec<&crate::pipeline::asts::core::operators::InteriorColumnDef> = if glob {
        schema.iter().collect()
    } else {
        columns
            .iter()
            .filter_map(|name| schema.iter().find(|d| d.name == *name))
            .collect()
    };

    let output_columns = cpr_output_columns(cpr_schema.get());

    // Context = everything except the drilled column.
    let context_col_names: Vec<String> = builder
        .columns()
        .iter()
        .filter_map(|c| {
            let name = col_name(c);
            if SqlIdentifier::str_eq(name, &column) {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect();
    let num_context = context_col_names.len();

    builder.expand_with_json_each(
        &column,
        "_drill",
        super::builder::JsonEachKind::Array,
        |source_alias| {
            let sq = ColumnQualifier::table(source_alias);
            context_col_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let alias = output_columns.get(i).map(|c| cpr_display_name(c)).unwrap_or(name);
                    SelectItem::expression_with_alias(
                        SqlDomainExpr::with_qualifier(sq.clone(), name.as_str()),
                        alias,
                    )
                })
                .collect()
        },
        |tvf_alias| {
            interior_cols
                .iter()
                .enumerate()
                .map(|(i, def)| {
                    let alias = output_columns
                        .get(num_context + i)
                        .map(|c| cpr_display_name(c).to_string())
                        .unwrap_or_else(|| def.name.clone());
                    SelectItem::expression_with_alias(
                        SqlDomainExpr::function(
                            "json_extract",
                            vec![
                                SqlDomainExpr::with_qualifier(
                                    ColumnQualifier::table(tvf_alias),
                                    "value",
                                ),
                                SqlDomainExpr::literal(ast_addressed::LiteralValue::String(
                                    format!("$.{}", def.name),
                                )),
                            ],
                        ),
                        alias,
                    )
                })
                .collect()
        },
        &groundings,
    )
}

/// Lower scalar destructure: `data ~= {first_name, last_name}`.
///
/// Lower a destructure pattern by walking the pattern tree inductively.
///
/// Scalar mode: extracts fields from a JSON value without row explosion.
/// Aggregate mode: first explodes the top-level array via `json_each`,
/// then walks the pattern against each element.
///
/// Nested `~>` patterns produce additional `json_each` joins at each level.
/// One recursive function handles everything: base extractions, nested `~>`
/// (KeyValue with nested_reduction), and MetadataTreeGroup (`key:~>`).
fn r_lower_destructure(
    builder: Builder<Unprojected>,
    json_column: ast_addressed::DomainExpression,
    mode: ast_addressed::DestructureMode,
    pattern: &ast_addressed::FunctionExpression,
    ctx: &TransformCtx,
) -> Result<Builder<Unprojected>> {
    let source_expr = scalar::s_lower_expression(json_column.clone(), &builder, ctx)?;

    if matches!(mode, ast_addressed::DestructureMode::Aggregate) {
        // Aggregate: explode the top-level array first, then walk pattern.
        let json_col_name = match &json_column {
            ast_addressed::DomainExpression::Lvar { name, .. } => name.as_str().to_string(),
            _ => {
                return Err(DelightQLError::ParseError {
                    message: "aggregate destructure: expected Lvar for json column".into(),
                    source: None,
                    subcategory: None,
                });
            }
        };
        // json_each on source column, then walk pattern against .value
        lower_with_json_each(builder, &json_col_name, pattern)
    } else {
        // Scalar: walk pattern directly. If there are nested ~> inside,
        // they'll each get their own json_each.
        lower_destructure_pattern(builder, &source_expr, pattern)
    }
}

/// The inductive core. Handles one pattern level, eats any `~>`, recurses.
///
/// At each level:
/// 1. Collect base extractions (json_extract items)
/// 2. For each `~>` member: project base items + temp col, json_each, recurse
/// 3. For MetadataTreeGroup: json_each(source), .key → column, recurse on .value
///
/// One function. Each call handles exactly one level. Depth is emergent.
fn lower_destructure_pattern(
    builder: Builder<Unprojected>,
    source: &crate::pipeline::sql_ast_v3::DomainExpression,
    pattern: &ast_addressed::FunctionExpression,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, SelectItem,
    };

    // Step 1: Classify each member as base or explosive.
    // Step 2: Project base extractions + temp cols for explosions.
    // Step 3: For each explosion, eat it (json_each) and recurse.

    match pattern {
        // --- MetadataTreeGroup: json_each(source) iterating object keys ---
        ast_addressed::FunctionExpression::MetadataTreeGroup {
            key_column,
            constructor,
            ..
        } => {
            let temp_col = format!("_mtg_src_{}", key_column);
            let context_cols: Vec<String> = builder
                .columns()
                .iter()
                .map(|c| col_name(c).to_string())
                .collect();

            // Project source as named column so expand_with_json_each can reference it
            let mut proj: Vec<SelectItem> = builder
                .columns()
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
            proj.push(SelectItem::expression_with_alias(source.clone(), &temp_col));
            let builder = builder.add_projection(proj)?.demote()?;

            let key_col_name = key_column.as_str().to_string();
            let constructor_clone = constructor.clone();
            let val_col_name = format!("_mtg_val_{}", key_column);

            // json_each on the object — produces .key and .value per entry
            let builder = builder
                .expand_with_json_each(
                    &temp_col,
                    "_je",
                    super::builder::JsonEachKind::Object,
                    |source_alias| {
                        let sq = ColumnQualifier::table(source_alias);
                        context_cols
                            .iter()
                            .map(|c| {
                                SelectItem::expression_with_alias(
                                    SqlDomainExpr::with_qualifier(sq.clone(), c.as_str()),
                                    c.as_str(),
                                )
                            })
                            .collect()
                    },
                    |tvf_alias| {
                        let sq = ColumnQualifier::table(tvf_alias);
                        // .key → key column
                        vec![
                            SelectItem::expression_with_alias(
                                SqlDomainExpr::with_qualifier(sq.clone(), "key"),
                                &key_col_name,
                            ),
                            // .value → pass through for recursion
                            SelectItem::expression_with_alias(
                                SqlDomainExpr::with_qualifier(sq, "value"),
                                &val_col_name,
                            ),
                        ]
                    },
                    &[],
                )?
                .demote()?;

            // Remove temp source column
            let builder = remove_column(builder, &temp_col)?;

            // ~> means iterate values as arrays, then apply constructor
            let builder = lower_with_json_each(builder, &val_col_name, &constructor_clone)?;

            // Remove the temp .value column
            remove_column(builder, &val_col_name)
        }

        // --- Curly: process members, each ~> is one explosion ---
        ast_addressed::FunctionExpression::Curly { members, .. } => {
            // Partition: base extractions, explosive (~>), and nested navigations
            let mut base_items = Vec::new();
            let mut explosions: Vec<(String, ast_addressed::FunctionExpression)> = Vec::new();
            let mut nested_navigations: Vec<(String, ast_addressed::FunctionExpression)> =
                Vec::new();

            for member in members {
                match member {
                    ast_addressed::CurlyMember::Shorthand { column, .. } => {
                        base_items.push(make_json_extract_item(
                            source,
                            &format!(".{}", column),
                            column.as_str(),
                        ));
                    }
                    ast_addressed::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        if *nested_reduction {
                            // Explosive: this ~> needs a json_each
                            if let ast_addressed::DomainExpression::Function(p) = value.as_ref() {
                                explosions.push((key.clone(), p.clone()));
                            }
                        } else if let ast_addressed::DomainExpression::Lvar { name, .. } =
                            value.as_ref()
                        {
                            base_items.push(make_json_extract_item(
                                source,
                                &format!(".{}", key),
                                name.as_str(),
                            ));
                        } else if let ast_addressed::DomainExpression::Function(nested_pat) =
                            value.as_ref()
                        {
                            // Nested object without ~>: navigate into sub-object
                            // and recurse (handles any ~> inside)
                            nested_navigations.push((key.clone(), nested_pat.clone()));
                        }
                    }
                    ast_addressed::CurlyMember::PathLiteral { path, alias } => {
                        if let Some((json_path, col)) = extract_path_literal_info(path, alias) {
                            base_items.push(make_json_extract_item(source, &json_path, &col));
                        }
                    }
                    ast_addressed::CurlyMember::Placeholder => {}
                    _ => {}
                }
            }

            // Project: existing columns + base items + temp cols
            // Enumerate explicitly (not SELECT *) so the builder can
            // disambiguate collisions between source and extracted columns.
            let mut proj: Vec<SelectItem> = builder
                .columns()
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
            proj.extend(base_items);
            for (key, _) in &explosions {
                proj.push(make_json_extract_raw_item(
                    source,
                    &format!(".{}", key),
                    &format!("_nested_{}", key),
                ));
            }
            for (key, _) in &nested_navigations {
                proj.push(make_json_extract_raw_item(
                    source,
                    &format!(".{}", key),
                    &format!("_nav_{}", key),
                ));
            }
            let mut builder = builder.add_projection(proj)?.demote()?;

            // Eat each explosion: json_each on temp col, recurse
            for (key, nested_pattern) in explosions {
                let temp = format!("_nested_{}", key);
                builder = lower_with_json_each(builder, &temp, &nested_pattern)?;
                builder = remove_column(builder, &temp)?;
            }

            // Navigate into nested objects (no json_each, just recurse)
            for (key, nested_pattern) in nested_navigations {
                let temp = format!("_nav_{}", key);
                let nav_source = SqlDomainExpr::column(&temp);
                builder = lower_destructure_pattern(builder, &nav_source, &nested_pattern)?;
                builder = remove_column(builder, &temp)?;
            }

            Ok(builder)
        }

        // --- Array: base extractions only (no ~> in array patterns) ---
        ast_addressed::FunctionExpression::Array { members, .. } => {
            let mut items: Vec<SelectItem> = builder
                .columns()
                .iter()
                .map(|c| passthrough_item(c))
                .collect();
            for member in members {
                let ast_addressed::ArrayMember::Index { path, alias } = member;
                if let Some((json_path, col)) = extract_path_literal_info(path, alias) {
                    items.push(make_json_extract_item(source, &json_path, &col));
                }
            }
            builder.add_projection(items)?.demote()
        }

        _ => Ok(builder),
    }
}

/// Eat a `~>`: wrap a column in json_each, then recurse into the nested pattern.
///
/// This is the bridge between levels. Each call produces exactly one json_each.
///
/// For MetadataTreeGroup patterns, the json_each's `.key` is captured as
/// the key column, and the constructor is walked against `.value`. This is
/// because `"key": ~> name:~> constructor` means ONE json_each on the object,
/// with `.key` → name column and `.value` → constructor source.
fn lower_with_json_each(
    builder: Builder<Unprojected>,
    col_name_str: &str,
    pattern: &ast_addressed::FunctionExpression,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{
        ColumnQualifier, DomainExpression as SqlDomainExpr, SelectItem,
    };

    let context_cols: Vec<String> = builder
        .columns()
        .iter()
        .map(|c| col_name(c).to_string())
        .collect();

    // If pattern is MTG, capture .key as the key column and recurse on .value
    // with the constructor — no separate json_each for the MTG.
    if let ast_addressed::FunctionExpression::MetadataTreeGroup {
        key_column,
        constructor,
        ..
    } = pattern
    {
        let key_col_name = key_column.as_str().to_string();
        let constructor_clone = constructor.clone();
        let val_temp = format!("_mtg_val_{}", key_column);

        let builder = builder
            .expand_with_json_each(
                col_name_str,
                "_je",
                super::builder::JsonEachKind::Object,
                |source_alias| {
                    let sq = ColumnQualifier::table(source_alias);
                    context_cols
                        .iter()
                        .map(|c| {
                            SelectItem::expression_with_alias(
                                SqlDomainExpr::with_qualifier(sq.clone(), c.as_str()),
                                c.as_str(),
                            )
                        })
                        .collect()
                },
                |tvf_alias| {
                    let sq = ColumnQualifier::table(tvf_alias);
                    vec![
                        SelectItem::expression_with_alias(
                            SqlDomainExpr::with_qualifier(sq.clone(), "key"),
                            &key_col_name,
                        ),
                        SelectItem::expression_with_alias(
                            SqlDomainExpr::with_qualifier(sq, "value"),
                            &val_temp,
                        ),
                    ]
                },
                &[],
            )?
            .demote()?;

        // ~> on MTG means iterate values as arrays, then apply constructor
        let builder = lower_with_json_each(builder, &val_temp, &constructor_clone)?;
        return remove_column(builder, &val_temp);
    }

    // Non-MTG: just pass .value through and recurse
    let pattern_clone = pattern.clone();
    let val_temp = format!("_val_{}", col_name_str);

    let builder = builder
        .expand_with_json_each(
            col_name_str,
            "_destr",
            super::builder::JsonEachKind::Array,
            |source_alias| {
                let sq = ColumnQualifier::table(source_alias);
                context_cols
                    .iter()
                    .map(|c| {
                        SelectItem::expression_with_alias(
                            SqlDomainExpr::with_qualifier(sq.clone(), c.as_str()),
                            c.as_str(),
                        )
                    })
                    .collect()
            },
            |tvf_alias| {
                vec![SelectItem::expression_with_alias(
                    SqlDomainExpr::with_qualifier(ColumnQualifier::table(tvf_alias), "value"),
                    &val_temp,
                )]
            },
            &[],
        )?
        .demote()?;

    let val_source = SqlDomainExpr::column(&val_temp);
    let builder = lower_destructure_pattern(builder, &val_source, &pattern_clone)?;
    remove_column(builder, &val_temp)
}

/// Extract JSON path and column name from a PathLiteral or Array Index member.
fn extract_path_literal_info(
    path: &ast_addressed::DomainExpression,
    alias: &Option<delightql_types::SqlIdentifier>,
) -> Option<(String, String)> {
    use crate::pipeline::asts::core::expressions::functions::PathSegment;

    if let ast_addressed::DomainExpression::Projection(
        crate::pipeline::asts::core::expressions::domain::ProjectionExpr::JsonPathLiteral {
            segments,
            ..
        },
    ) = path
    {
        let json_path = segments_to_json_path_sql(segments);
        let col = alias
            .as_ref()
            .map(|a| a.as_str().to_string())
            .unwrap_or_else(|| infer_col_name(segments));
        Some((json_path, col))
    } else {
        None
    }
}

/// Remove a column from the builder's output.
fn remove_column(
    builder: Builder<Unprojected>,
    col_to_remove: &str,
) -> Result<Builder<Unprojected>> {
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    let keep: Vec<String> = builder
        .columns()
        .iter()
        .map(|c| col_name(c).to_string())
        .filter(|n| n != col_to_remove)
        .collect();

    if keep.is_empty() {
        return Ok(builder);
    }

    let items: Vec<SelectItem> = keep
        .iter()
        .map(|c| SelectItem::expression_with_alias(SqlDomainExpr::column(c), c))
        .collect();
    builder.add_projection(items)?.demote()
}

/// Build a `json_extract(source, path) AS alias` SelectItem.
fn make_json_extract_item(
    source: &crate::pipeline::sql_ast_v3::DomainExpression,
    json_path: &str,
    alias: &str,
) -> crate::pipeline::sql_ast_v3::SelectItem {
    make_json_extract_item_named(source, json_path, alias, "json_extract")
}

/// Like [`make_json_extract_item`] but the extraction must stay NATIVE json
/// (never a per-dialect *_string respell): the temp column is fed straight
/// into `json_each`/recursive navigation, which breaks on a stringified
/// subtree.
fn make_json_extract_raw_item(
    source: &crate::pipeline::sql_ast_v3::DomainExpression,
    json_path: &str,
    alias: &str,
) -> crate::pipeline::sql_ast_v3::SelectItem {
    make_json_extract_item_named(
        source,
        json_path,
        alias,
        crate::pipeline::naming::INTERNAL_JSON_EXTRACT_RAW,
    )
}

fn make_json_extract_item_named(
    source: &crate::pipeline::sql_ast_v3::DomainExpression,
    json_path: &str,
    alias: &str,
    fn_name: &str,
) -> crate::pipeline::sql_ast_v3::SelectItem {
    use crate::pipeline::sql_ast_v3::{DomainExpression as SqlDomainExpr, SelectItem};

    let full_path = if json_path.starts_with('[') || json_path.starts_with('.') {
        format!("${}", json_path)
    } else {
        format!("$.{}", json_path)
    };

    SelectItem::expression_with_alias(
        SqlDomainExpr::function(
            fn_name,
            vec![
                source.clone(),
                SqlDomainExpr::literal(ast_addressed::LiteralValue::String(full_path)),
            ],
        ),
        alias,
    )
}

/// Convert path segments to JSON path suffix: `.key` or `[N]`.
fn segments_to_json_path_sql(
    segments: &[crate::pipeline::asts::core::expressions::functions::PathSegment],
) -> String {
    use crate::pipeline::asts::core::expressions::functions::PathSegment;
    let mut path = String::new();
    for seg in segments {
        match seg {
            PathSegment::ObjectKey(key) => {
                path.push('.');
                path.push_str(key);
            }
            PathSegment::ArrayIndex(idx) => {
                path.push_str(&format!("[{}]", idx));
            }
        }
    }
    path
}

/// Infer column name from path segments (joined with `_`).
fn infer_col_name(
    segments: &[crate::pipeline::asts::core::expressions::functions::PathSegment],
) -> String {
    use crate::pipeline::asts::core::expressions::functions::PathSegment;
    segments
        .iter()
        .map(|s| match s {
            PathSegment::ObjectKey(k) => k.clone(),
            PathSegment::ArrayIndex(i) => i.to_string(),
        })
        .collect::<Vec<_>>()
        .join("_")
}

/// Lower an `IntersectCorresponding` node into SQL.
///
/// Produces a bidirectional semijoin: for each operand, SELECT its rows
/// WHERE EXISTS a matching row in every other operand, then UNION ALL.
pub(super) fn r_lower_intersect_corresponding(
    operands: Vec<Builder<Projected>>,
    correlation: ast_addressed::BooleanExpression,
    min_multiplicity: bool,
    _cpr_schema: PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{
        ordering::OrderDirection, DomainExpression as SqlDomainExpr, JoinCondition, JoinType,
        SelectItem, SqlPredicate,
    };

    if operands.len() < 2 {
        return Err(DelightQLError::ParseError {
            message: "IntersectCorresponding requires at least 2 operands".to_string(),
            source: None,
            subcategory: None,
        });
    }

    let names = ctx.names.clone();

    // Step 1: Extract user aliases from the correlation expression.
    // The correlation has qualifiers like "first", "second" — these map
    // to operands in order.
    let user_aliases = extract_correlation_qualifiers(&correlation);

    // Step 2: Materialize each operand and record its info.
    // We need the QueryExpression and columns for each operand. Each operand's
    // query may be used multiple times (as both outer and inner), so we
    // materialize up front and clone as needed.
    let mut op_aliases = Vec::new();
    let mut op_columns = Vec::new();
    let mut op_queries = Vec::new();
    for op in &operands {
        op_columns.push(op.scope_columns());
        op_aliases.push(names.next_name("isect"));
    }
    for op in operands {
        op_queries.push(op.to_sql()?);
    }

    // Build alias map: user_alias → subquery_alias (positional)
    let alias_map: Vec<(Option<String>, String)> = user_aliases
        .into_iter()
        .zip(op_aliases.iter())
        .map(|(user, alias)| (user, alias.clone()))
        .collect();

    // Dispatch: min_multiplicity → bag intersection (ROW_NUMBER JOIN)
    if min_multiplicity && op_queries.len() == 2 {
        return r_lower_intersect_min_multiplicity(
            &correlation,
            &alias_map,
            op_queries,
            &op_columns,
            &names,
        );
    }

    // EXISTS path: For each operand i, build:
    //   SELECT * FROM (op_i) WHERE EXISTS (SELECT * FROM (op_j) WHERE correlation)
    // using Builder::from_frozen + add_where + project_all.
    let mut halves: Vec<Builder<Projected>> = Vec::new();
    for i in 0..op_queries.len() {
        let mut exists_conditions: Vec<SqlDomainExpr> = Vec::new();

        for j in 0..op_queries.len() {
            if j == i {
                continue;
            }

            let rewritten_corr = rewrite_correlation_qualifiers(
                &correlation,
                &alias_map,
                &[(i, op_aliases[i].as_str()), (j, op_aliases[j].as_str())],
            )?;

            // Build inner EXISTS subquery using Builder.
            // The scope name must match the alias used in rewrite_correlation_qualifiers.
            let inner_scope = TableName::Named(SqlIdentifier::from(op_aliases[j].as_str()));
            let inner = Builder::from_frozen(
                op_queries[j].clone(),
                inner_scope,
                op_columns[j].clone(),
                names.clone(),
            )
            .add_where(SqlPredicate::new(rewritten_corr))?
            .project_all()?
            .to_sql()?;

            exists_conditions.push(SqlDomainExpr::exists(inner));
        }

        // Build outer operand using Builder.
        // The scope name must match the alias used in rewrite_correlation_qualifiers.
        let outer_scope = TableName::Named(SqlIdentifier::from(op_aliases[i].as_str()));
        let mut outer = Builder::from_frozen(
            op_queries[i].clone(),
            outer_scope,
            op_columns[i].clone(),
            names.clone(),
        );

        if !exists_conditions.is_empty() {
            let combined = SqlDomainExpr::and(exists_conditions);
            outer = outer.add_where(SqlPredicate::new(combined))?;
        }

        halves.push(outer.project_all()?);
    }

    // UNION ALL all halves using Builder::union_all
    let combined = halves
        .into_iter()
        .reduce(|left, right| left.union_all(right).expect("union_all failed"))
        .unwrap();

    Ok(combined)
}

/// Bag intersection via ROW_NUMBER + JOIN for exactly 2 operands.
/// Preserves duplicate multiplicity: min(count_left, count_right) copies
/// of each matching group are kept.
fn r_lower_intersect_min_multiplicity(
    correlation: &ast_addressed::BooleanExpression,
    alias_map: &[(Option<String>, String)],
    mut op_queries: Vec<crate::pipeline::sql_ast_v3::QueryExpression>,
    op_columns: &[Vec<crate::pipeline::asts::resolved::ColumnMetadata>],
    names: &super::builder::names::NameGenerator,
) -> Result<Builder<Projected>> {
    use crate::pipeline::sql_ast_v3::{
        ordering::OrderDirection, DomainExpression as SqlDomainExpr, JoinCondition, JoinType,
        SelectItem,
    };

    let rn_col = "__dql_rn";

    // Extract column pairs from correlation: (left_col, right_col)
    let col_pairs = extract_isect_column_pairs(
        correlation,
        alias_map.first().and_then(|(u, _)| u.as_deref()),
        alias_map.get(1).and_then(|(u, _)| u.as_deref()),
    );

    let left_cols: Vec<String> = col_pairs.iter().map(|(l, _)| l.clone()).collect();
    let right_cols: Vec<String> = col_pairs.iter().map(|(_, r)| r.clone()).collect();

    // Rebuild operand Builders from frozen queries, then add ROW_NUMBER
    let left_scope = names.next_table_name("isect");
    let left_builder = Builder::from_frozen(
        op_queries.remove(0),
        left_scope,
        op_columns[0].clone(),
        names.clone(),
    )
    .project_all()?;

    let right_scope = names.next_table_name("isect");
    let right_builder = Builder::from_frozen(
        op_queries.remove(0),
        right_scope,
        op_columns[1].clone(),
        names.clone(),
    )
    .project_all()?;

    // Add ROW_NUMBER window column to each side
    let left_partition: Vec<SqlDomainExpr> =
        left_cols.iter().map(|c| SqlDomainExpr::column(c)).collect();
    let left_order: Vec<(SqlDomainExpr, OrderDirection)> = left_cols
        .iter()
        .map(|c| (SqlDomainExpr::column(c), OrderDirection::Asc))
        .collect();
    let left_rn =
        left_builder.add_window_column("ROW_NUMBER", vec![], left_partition, left_order, rn_col)?;

    let right_partition: Vec<SqlDomainExpr> = right_cols
        .iter()
        .map(|c| SqlDomainExpr::column(c))
        .collect();
    let right_order: Vec<(SqlDomainExpr, OrderDirection)> = right_cols
        .iter()
        .map(|c| (SqlDomainExpr::column(c), OrderDirection::Asc))
        .collect();
    let right_rn = right_builder.add_window_column(
        "ROW_NUMBER",
        vec![],
        right_partition,
        right_order,
        rn_col,
    )?;

    // Convert to join operands
    let left_op = left_rn.demote()?.into_join_operand()?;
    let right_op = right_rn.demote()?.into_join_operand()?;

    // Build JOIN ON condition using the post-wrap qualifiers from
    // the join operands' columns.
    let left_qual = left_op
        .columns
        .first()
        .and_then(|c| col_qualifier(c))
        .unwrap_or("_left")
        .to_string();
    let right_qual = right_op
        .columns
        .first()
        .and_then(|c| col_qualifier(c))
        .unwrap_or("_right")
        .to_string();

    let mut join_conds: Vec<SqlDomainExpr> = Vec::new();
    for (l, r) in &col_pairs {
        join_conds.push(SqlDomainExpr::Binary {
            left: Box::new(SqlDomainExpr::with_qualifier(
                crate::pipeline::sql_ast_v3::ColumnQualifier::table(&left_qual),
                l,
            )),
            op: crate::pipeline::sql_ast_v3::BinaryOperator::IsNotDistinctFrom,
            right: Box::new(SqlDomainExpr::with_qualifier(
                crate::pipeline::sql_ast_v3::ColumnQualifier::table(&right_qual),
                r,
            )),
        });
    }
    join_conds.push(SqlDomainExpr::Binary {
        left: Box::new(SqlDomainExpr::with_qualifier(
            crate::pipeline::sql_ast_v3::ColumnQualifier::table(&left_qual),
            rn_col,
        )),
        op: crate::pipeline::sql_ast_v3::BinaryOperator::Equal,
        right: Box::new(SqlDomainExpr::with_qualifier(
            crate::pipeline::sql_ast_v3::ColumnQualifier::table(&right_qual),
            rn_col,
        )),
    });
    let join_on = SqlDomainExpr::and(join_conds);

    // Build the join
    let joined = Builder::from_join(
        left_op,
        right_op,
        JoinType::Inner,
        JoinCondition::On(join_on),
    );

    // Project out __dql_rn — keep only the left side's original columns
    let output_cols = &op_columns[0];
    let output_items: Vec<SelectItem> = output_cols
        .iter()
        .map(|col| {
            let cname = col_name(col).to_string();
            SelectItem::Expression {
                expr: SqlDomainExpr::with_qualifier(
                    crate::pipeline::sql_ast_v3::ColumnQualifier::table(&left_qual),
                    &cname,
                ),
                alias: Some(cname),
            }
        })
        .collect();

    joined.add_projection(output_items)
}

/// Extract (left_col_name, right_col_name) pairs from a correlation.
fn extract_isect_column_pairs(
    correlation: &ast_addressed::BooleanExpression,
    left_user: Option<&str>,
    right_user: Option<&str>,
) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    collect_isect_pairs(correlation, left_user, right_user, &mut pairs);
    pairs
}

fn collect_isect_pairs(
    expr: &ast_addressed::BooleanExpression,
    left_user: Option<&str>,
    right_user: Option<&str>,
    out: &mut Vec<(String, String)>,
) {
    match expr {
        ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
            if let (
                ast_addressed::DomainExpression::Lvar {
                    name: ln,
                    qualifier: lq,
                    ..
                },
                ast_addressed::DomainExpression::Lvar {
                    name: rn,
                    qualifier: rq,
                    ..
                },
            ) = (left.as_ref(), right.as_ref())
            {
                let lq_str = lq.as_ref().map(|q| q.as_ref().to_string());
                let rq_str = rq.as_ref().map(|q| q.as_ref().to_string());
                let ln_s = ln.as_ref().to_string();
                let rn_s = rn.as_ref().to_string();

                let l_is_left = lq_str.as_deref() == left_user;
                let r_is_right = rq_str.as_deref() == right_user;
                let l_is_right = lq_str.as_deref() == right_user;
                let r_is_left = rq_str.as_deref() == left_user;

                if l_is_left && r_is_right {
                    out.push((ln_s, rn_s));
                } else if l_is_right && r_is_left {
                    out.push((rn_s, ln_s));
                }
            }
        }
        ast_addressed::BooleanExpression::And { left, right } => {
            collect_isect_pairs(left, left_user, right_user, out);
            collect_isect_pairs(right, left_user, right_user, out);
        }
        _ => {}
    }
}

/// Extract the distinct qualifiers from a correlation expression, in order
/// of first appearance. These correspond positionally to the operands.
fn extract_correlation_qualifiers(
    correlation: &ast_addressed::BooleanExpression,
) -> Vec<Option<String>> {
    let mut qualifiers: Vec<String> = Vec::new();
    collect_qualifiers(correlation, &mut qualifiers);
    qualifiers.into_iter().map(Some).collect()
}

fn collect_qualifiers(expr: &ast_addressed::BooleanExpression, out: &mut Vec<String>) {
    match expr {
        ast_addressed::BooleanExpression::Comparison { left, right, .. } => {
            collect_domain_qualifiers(left, out);
            collect_domain_qualifiers(right, out);
        }
        ast_addressed::BooleanExpression::And { left, right }
        | ast_addressed::BooleanExpression::Or { left, right } => {
            collect_qualifiers(left, out);
            collect_qualifiers(right, out);
        }
        _ => {}
    }
}

fn collect_domain_qualifiers(expr: &ast_addressed::DomainExpression, out: &mut Vec<String>) {
    match expr {
        ast_addressed::DomainExpression::Lvar { qualifier, .. } => {
            if let Some(q) = qualifier {
                let s = q.as_ref().to_string();
                if !out.contains(&s) {
                    out.push(s);
                }
            }
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            collect_domain_qualifiers(inner, out);
        }
        ast_addressed::DomainExpression::Function(func) => match func {
            ast_addressed::FunctionExpression::Infix { left, right, .. } => {
                collect_domain_qualifiers(left, out);
                collect_domain_qualifiers(right, out);
            }
            _ => {}
        },
        _ => {}
    }
}

/// Rewrite correlation qualifiers from user aliases to subquery aliases.
fn rewrite_correlation_qualifiers(
    correlation: &ast_addressed::BooleanExpression,
    alias_map: &[(Option<String>, String)],
    active_aliases: &[(usize, &str)],
) -> Result<crate::pipeline::sql_ast_v3::DomainExpression> {
    use crate::pipeline::sql_ast_v3::{BinaryOperator, DomainExpression as SqlDomainExpr};

    match correlation {
        ast_addressed::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } => {
            let sql_op = scalar::s_lower_comparison_op(operator)?;
            let left_expr = rewrite_corr_domain_expr(left, alias_map, active_aliases)?;
            let right_expr = rewrite_corr_domain_expr(right, alias_map, active_aliases)?;
            Ok(SqlDomainExpr::Binary {
                left: Box::new(left_expr),
                op: sql_op,
                right: Box::new(right_expr),
            })
        }
        ast_addressed::BooleanExpression::And { left, right } => {
            let l = rewrite_correlation_qualifiers(left, alias_map, active_aliases)?;
            let r = rewrite_correlation_qualifiers(right, alias_map, active_aliases)?;
            Ok(SqlDomainExpr::Binary {
                left: Box::new(l),
                op: BinaryOperator::And,
                right: Box::new(r),
            })
        }
        ast_addressed::BooleanExpression::Or { left, right } => {
            let l = rewrite_correlation_qualifiers(left, alias_map, active_aliases)?;
            let r = rewrite_correlation_qualifiers(right, alias_map, active_aliases)?;
            Ok(SqlDomainExpr::Binary {
                left: Box::new(l),
                op: BinaryOperator::Or,
                right: Box::new(r),
            })
        }
        _ => Err(DelightQLError::ParseError {
            message: format!(
                "IntersectCorresponding: unsupported correlation expression: {:?}",
                std::mem::discriminant(correlation)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// Rewrite a single domain expression in a correlation, mapping user qualifiers
/// to subquery aliases.
fn rewrite_corr_domain_expr(
    expr: &ast_addressed::DomainExpression,
    alias_map: &[(Option<String>, String)],
    active_aliases: &[(usize, &str)],
) -> Result<crate::pipeline::sql_ast_v3::DomainExpression> {
    use crate::pipeline::sql_ast_v3::{ColumnQualifier, DomainExpression as SqlDomainExpr};

    match expr {
        ast_addressed::DomainExpression::Lvar {
            name, qualifier, ..
        } => {
            let col_name = name.as_ref().to_string();

            // Map the user qualifier to the active subquery alias
            let sql_qualifier = if let Some(user_qual) = qualifier {
                let user_qual_str = user_qual.as_ref().to_string();
                // Find which operand index this user qualifier maps to
                let mapped = alias_map.iter().enumerate().find_map(|(idx, (user, _sq))| {
                    if user.as_deref() == Some(&user_qual_str) {
                        // Find the active alias for this index
                        active_aliases
                            .iter()
                            .find(|(i, _)| *i == idx)
                            .map(|(_, alias)| alias.to_string())
                    } else {
                        None
                    }
                });
                mapped.map(|a| ColumnQualifier::table(a))
            } else {
                None
            };

            Ok(SqlDomainExpr::Column {
                name: col_name,
                qualifier: sql_qualifier,
            })
        }
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => {
            let sql = rewrite_corr_domain_expr(inner, alias_map, active_aliases)?;
            Ok(SqlDomainExpr::Parens(Box::new(sql)))
        }
        ast_addressed::DomainExpression::Function(func_expr) => match func_expr {
            ast_addressed::FunctionExpression::Infix {
                operator,
                left,
                right,
                ..
            } => {
                let left_sql = rewrite_corr_domain_expr(left, alias_map, active_aliases)?;
                let right_sql = rewrite_corr_domain_expr(right, alias_map, active_aliases)?;
                super::scalar::s_lower_binary_sql_pub(operator, left_sql, right_sql)
            }
            _ => Err(DelightQLError::ParseError {
                message: format!(
                    "IntersectCorresponding: unsupported function in correlation: {:?}",
                    std::mem::discriminant(func_expr)
                ),
                source: None,
                subcategory: None,
            }),
        },
        ast_addressed::DomainExpression::Literal { value, .. } => {
            Ok(SqlDomainExpr::literal(value.clone()))
        }
        _ => Err(DelightQLError::ParseError {
            message: format!(
                "IntersectCorresponding: unsupported domain expression in correlation: {:?}",
                std::mem::discriminant(expr)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

#[cfg(test)]
mod resolver_alias_seam_tests {
    use super::*;
    use crate::pipeline::asts::core::provenance::ColumnProvenance;
    use crate::pipeline::sql_ast_v3::{DomainExpression, SelectItem};

    /// Column whose current spelling is `name`.
    fn col(name: &str) -> ColumnMetadata {
        ColumnMetadata::new(ColumnProvenance::from_column(name), TableName::Fresh, None)
    }

    fn unaliased_item() -> SelectItem {
        SelectItem::expression(DomainExpression::column("_"))
    }

    fn alias_of(item: &SelectItem) -> Option<&str> {
        match item {
            SelectItem::Expression { alias, .. } => alias.as_deref(),
            _ => None,
        }
    }

    // The positional multi-item alias re-attach was retired in slice 4 — every
    // consumer now aliases each item from its own output stamp via
    // `alias_unaliased`. The old positional-offset test is retired with the
    // function; the stamp-a-name and skip-aliased behaviors that still matter
    // carry over to `alias_unaliased` below. `col(..)` / `unaliased_item()`
    // remain the shared fixtures.

    #[test]
    fn alias_unaliased_stamps_name() {
        let mut item = unaliased_item();
        alias_unaliased(&mut item, cpr_display_name(&col("count_2")));
        assert_eq!(alias_of(&item), Some("count_2"));
    }

    #[test]
    fn alias_unaliased_leaves_aliased_item_untouched() {
        let mut item = SelectItem::expression_with_alias(DomainExpression::column("_"), "keep");
        alias_unaliased(&mut item, "count_2");
        assert_eq!(alias_of(&item), Some("keep"));
    }

    #[test]
    fn unknown_schema_empty() {
        assert!(cpr_output_columns(&CprSchema::Unknown).is_empty());
    }
}
