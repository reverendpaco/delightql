// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Tree-group lowering: `%(keys ~> {curly})` with nested reductions.
//!
//! Handles data-oriented tree groups (`~> {cols}`), metadata tree groups
//! (`country:~> {cols}`), and mixed aggregates. Produces CTE chains via
//! `push_cte` for nested reductions.
//!
//! Entry points called from `relational::r_lower_group_by_spec`:
//! - `r_lower_tree_group_cte` — nested tree groups requiring CTEs
//! - `s_lower_reducing_on_item` — single reducing_on item dispatch

use super::builder::{
    col_name, col_qualifier, table_name_sql, Builder, CteBody, Projected, Qualify, Unprojected,
};
use super::scalar;
use super::TransformCtx;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast_addressed;
use crate::pipeline::asts::core::expressions::CurlyMember;
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::metadata::CprSchema;
use crate::pipeline::asts::core::{Addressed, PhaseBox};
use crate::pipeline::sql_ast_v3::{
    BinaryOperator, ColumnQualifier, DomainExpression as SqlExpr, SelectBuilder, SelectItem,
    TableExpression, WhenClause,
};

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Lower a single reducing_on item.
///
/// Curly expressions get the aggregate wrapper (null-elision + GROUP_CONCAT).
/// Everything else (count, sum, avg, etc.) uses normal scalar lowering.
pub(super) fn s_lower_reducing_on_item(
    expr: ast_addressed::DomainExpression,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SelectItem> {
    match expr {
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
            members,
            alias,
            ..
        }) => {
            let agg_expr = s_lower_curly_aggregate(members, qualify, ctx)?;
            Ok(SelectItem::Expression {
                expr: agg_expr,
                alias: alias.as_ref().map(|a| a.as_str().to_string()),
            })
        }
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Bracket {
            arguments,
            alias,
            ..
        }) => {
            // Bracket in aggregate position: ~> [cols] → array of tuples.
            // Lower each argument, collect for null checks, wrap with aggregate.
            let mut lowered_args = Vec::new();
            let mut null_checks = Vec::new();
            for arg in arguments {
                let sql_expr = scalar::s_lower_expression(arg, qualify, ctx)?;
                null_checks.push(sql_expr.clone());
                lowered_args.push(sql_expr);
            }
            let json_array = SqlExpr::function("JSON_ARRAY", lowered_args);
            let agg_expr = build_aggregate_wrapper(json_array, null_checks);
            Ok(SelectItem::Expression {
                expr: agg_expr,
                alias: alias.as_ref().map(|a| a.as_str().to_string()),
            })
        }
        other => scalar::s_lower_select_item(other, qualify, ctx),
    }
}

/// Lower tree groups that require CTEs (nested `~>` reductions).
///
/// Builds a bottom-up CTE chain via `push_cte`. Each nesting level becomes a CTE:
/// - Innermost CTE: GROUP BY all keys, aggregate the leaf `{columns}`
/// - Each successive CTE: GROUP BY fewer keys, aggregate the previous CTE's output
/// - Metadata levels use `JSON_GROUP_OBJECT(key, json(value))` instead of GROUP_CONCAT
pub(super) fn r_lower_tree_group_cte(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::DomainExpression>,
    reducing_on: Vec<ast_addressed::DomainExpression>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    let item = reducing_on
        .into_iter()
        .next()
        .ok_or_else(|| DelightQLError::ParseError {
            message: "r_lower_tree_group_cte: empty reducing_on".into(),
            source: None,
            subcategory: None,
        })?;

    let mut metadata_keys: Vec<GroupKey> = Vec::new();
    let (members, alias) = extract_tree_group_item(item, &mut metadata_keys)?;

    let reducing_by_names: Vec<String> = reducing_by
        .iter()
        .filter_map(|e| {
            if let ast_addressed::DomainExpression::Lvar { name, .. } = e {
                Some(name.as_str().to_string())
            } else {
                None
            }
        })
        .collect();

    let metadata_keys: Vec<GroupKey> = metadata_keys;

    let has_metadata = !metadata_keys.is_empty();

    // Get the CprSchema's name for the tree group column — this is the
    // resolver's authoritative name. Use it instead of hardcoded "result".
    let cpr_tg_name: Option<String> = match cpr_schema.get() {
        CprSchema::Resolved(cols) => {
            // The tree group column is after the reducing_by keys
            cols.get(reducing_by_names.len())
                .map(|c| c.name().to_string())
        }
        _ => None,
    };

    let top_alias = if has_metadata {
        "constructor".to_string()
    } else {
        cpr_tg_name.clone().unwrap_or_else(|| "result".to_string())
    };

    let mut initial_group_keys: Vec<GroupKey> = reducing_by_names
        .iter()
        .map(|n| GroupKey::simple(n))
        .collect();
    initial_group_keys.extend(metadata_keys.iter().cloned());

    let mut levels: Vec<NestingLevel> = Vec::new();
    collect_levels(
        members,
        initial_group_keys,
        top_alias.to_string(),
        &mut levels,
    );
    append_metadata_levels(
        &metadata_keys,
        &reducing_by_names,
        &mut levels,
        cpr_tg_name.as_deref(),
    );

    // Build the CTE chain bottom-up via push_cte.
    let mut projected = builder.project_all()?;

    for level in &levels {
        projected = projected.push_cte(|input| build_cte_body(level, input, ctx))?;
    }

    // Final projection from the last CTE.
    build_final_projection(projected, &levels, &reducing_by, alias, cpr_schema)
}

/// Lower a tree group that appears in reducing_by (key) position.
///
/// Pattern: `|> %( {key, "nested": ~> {cols}} as tg_alias ~> count:(*), avg:(x) )`
///
/// The tree group's leaf keys become GROUP BY columns. Nested reductions become
/// GROUP_CONCAT aggregates. Extra reducing_on items (count, avg) share the same
/// GROUP BY. Final projection wraps leaf keys + nested aggs in JSON_OBJECT.
pub(super) fn r_lower_tree_group_in_reducing_by(
    builder: Builder<Unprojected>,
    reducing_by: Vec<ast_addressed::DomainExpression>,
    reducing_on: Vec<ast_addressed::DomainExpression>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
    ctx: &TransformCtx,
) -> Result<Builder<Projected>> {
    // Partition reducing_by into the tree group item and plain keys
    let mut tg_item = None;
    let mut plain_by = Vec::new();

    for item in reducing_by {
        if tg_item.is_none() && is_tree_group_expr(&item) {
            tg_item = Some(item);
        } else {
            plain_by.push(item);
        }
    }
    let tg_item = tg_item.ok_or_else(|| DelightQLError::ParseError {
        message: "r_lower_tree_group_in_reducing_by: no tree group in reducing_by".into(),
        source: None,
        subcategory: None,
    })?;

    // Extract members and alias from the tree group
    let (tg_members, tg_alias) =
        match tg_item {
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::Curly { members, alias, .. },
            ) => (members, alias),
            _ => {
                return Err(DelightQLError::ParseError {
                    message: "r_lower_tree_group_in_reducing_by: expected Curly".into(),
                    source: None,
                    subcategory: None,
                })
            }
        };

    let (leaf_members, nested_items) = separate_leaf_and_nested(tg_members);

    // Save counts for final projection offset calculation
    let plain_by_count = plain_by.len();
    let leaf_count = leaf_members.len();
    let nested_count = nested_items.len();

    // Build CTE: GROUP BY leaf keys, aggregate nested parts + extra reducing_on
    let projected = builder.project_all()?;
    let leaf_for_cte = leaf_members.clone();
    let nested_for_cte = nested_items.clone();

    let projected = projected.push_cte(move |input| {
        let mut group_by_exprs = Vec::new();
        let mut select_items = Vec::new();

        lower_keys_to_group_by(&plain_by, input, &mut group_by_exprs, &mut select_items)?;
        lower_leaf_members_to_group_by(
            &leaf_for_cte,
            input,
            ctx,
            &mut group_by_exprs,
            &mut select_items,
        )?;
        lower_nested_as_aggregates(&nested_for_cte, input, ctx, &mut select_items)?;

        for extra in &reducing_on {
            select_items.push(s_lower_reducing_on_item(extra.clone(), input, ctx)?);
        }

        assemble_cte(input, select_items, group_by_exprs)
    })?;

    // Final projection: JSON_OBJECT for tree group + pass-through extras
    let extra_start = plain_by_count + leaf_count + nested_count;
    build_reducing_by_final_projection(
        projected,
        &leaf_members,
        &nested_items,
        tg_alias,
        extra_start,
        cpr_schema,
    )
}

/// Check whether an expression is a tree group needing CTE treatment.
fn is_tree_group_expr(expr: &ast_addressed::DomainExpression) -> bool {
    match expr {
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
            cte_requirements: Some(req),
            ..
        }) => req.needs_cte,
        ast_addressed::DomainExpression::Function(
            ast_addressed::FunctionExpression::MetadataTreeGroup { .. },
        ) => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers for CTE body construction
// ---------------------------------------------------------------------------

/// Separate curly members into leaf (non-nested) and nested reductions.
///
/// Nested reductions (`"key": ~> {members}` or `"key": ~> [members]`) are
/// returned as `(key, inner_members, is_bracket)`. Everything else is a leaf.
fn separate_leaf_and_nested(
    members: Vec<CurlyMember<Addressed>>,
) -> (
    Vec<CurlyMember<Addressed>>,
    Vec<(String, Vec<CurlyMember<Addressed>>, bool)>,
) {
    let mut leaf = Vec::new();
    let mut nested = Vec::new();

    for member in members {
        match member {
            CurlyMember::KeyValue {
                key,
                value,
                nested_reduction: true,
            } => match *value {
                ast_addressed::DomainExpression::Function(
                    ast_addressed::FunctionExpression::Curly { members: inner, .. },
                ) => nested.push((key, inner, false)),
                ast_addressed::DomainExpression::Function(
                    ast_addressed::FunctionExpression::Bracket { arguments, .. },
                ) => {
                    let inner: Vec<CurlyMember<Addressed>> = arguments
                        .into_iter()
                        .filter_map(|arg| {
                            if let ast_addressed::DomainExpression::Lvar {
                                name, qualifier, ..
                            } = arg
                            {
                                Some(CurlyMember::Shorthand {
                                    column: name,
                                    qualifier,
                                    schema: None,
                                })
                            } else {
                                None
                            }
                        })
                        .collect();
                    nested.push((key, inner, true));
                }
                _ => {}
            },
            other => leaf.push(other),
        }
    }

    (leaf, nested)
}

/// Lower plain Lvar keys to GROUP BY expressions and SELECT items.
fn lower_keys_to_group_by(
    keys: &[ast_addressed::DomainExpression],
    input: &super::builder::CteInput,
    group_by_exprs: &mut Vec<SqlExpr>,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    for item in keys {
        if let ast_addressed::DomainExpression::Lvar {
            name, qualifier, ..
        } = item
        {
            let col = name.as_str();
            let qc = if let Some(q) = qualifier {
                input
                    .qualify_with_table(col, q.as_str())
                    .unwrap_or(input.qualify(col)?)
            } else {
                input.qualify(col)?
            };
            let sql_expr = match &qc.qualifier {
                Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc.name),
                None => SqlExpr::column(&qc.name),
            };
            group_by_exprs.push(sql_expr.clone());
            select_items.push(SelectItem::Expression {
                expr: sql_expr,
                alias: Some(qc.name.clone()),
            });
        }
    }
    Ok(())
}

/// Lower leaf curly members to GROUP BY expressions and SELECT items.
fn lower_leaf_members_to_group_by(
    members: &[CurlyMember<Addressed>],
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
    group_by_exprs: &mut Vec<SqlExpr>,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    for member in members {
        if let Some((col_name, sql_col, _)) = lower_leaf_member(member, input, ctx)? {
            group_by_exprs.push(sql_col.clone());
            select_items.push(SelectItem::Expression {
                expr: sql_col,
                alias: Some(col_name),
            });
        }
    }
    Ok(())
}

/// Lower nested reductions to aggregate SELECT items (GROUP_CONCAT pattern).
fn lower_nested_as_aggregates(
    nested: &[(String, Vec<CurlyMember<Addressed>>, bool)],
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    for (key, inner_members, is_bracket) in nested {
        if *is_bracket {
            let mut lowered_args = Vec::new();
            let mut null_checks = Vec::new();
            for member in inner_members {
                if let Some((_, sql_col, is_tree)) = lower_leaf_member(member, input, ctx)? {
                    null_checks.push(sql_col.clone());
                    lowered_args.push(if is_tree {
                        SqlExpr::function("json", vec![sql_col])
                    } else {
                        sql_col
                    });
                }
            }
            let json_array = SqlExpr::function("JSON_ARRAY", lowered_args);
            let agg_expr = build_aggregate_wrapper(json_array, null_checks);
            select_items.push(SelectItem::Expression {
                expr: agg_expr,
                alias: Some(key.clone()),
            });
        } else {
            let agg_expr = s_lower_curly_aggregate(inner_members.clone(), input, ctx)?;
            select_items.push(SelectItem::Expression {
                expr: agg_expr,
                alias: Some(key.clone()),
            });
        }
    }
    Ok(())
}

/// Assemble select_items and group_by_exprs into a CteBody.
fn assemble_cte(
    input: &super::builder::CteInput,
    select_items: Vec<SelectItem>,
    group_by_exprs: Vec<SqlExpr>,
) -> Result<CteBody> {
    let scope_name = table_name_sql(input.scope_name());
    let from = TableExpression::table(scope_name);
    let mut sb = SelectBuilder::new()
        .from_tables(vec![from])
        .set_select(select_items.clone());
    if !group_by_exprs.is_empty() {
        sb = sb.group_by(group_by_exprs);
    }

    let stmt = sb.build().map_err(|e| DelightQLError::ParseError {
        message: format!("assemble_cte: build failed: {}", e),
        source: None,
        subcategory: None,
    })?;
    let query = crate::pipeline::sql_ast_v3::QueryExpression::Select(Box::new(stmt));
    let output_cols: Vec<String> = select_items
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expression { alias, .. } => alias.clone(),
            _ => None,
        })
        .collect();

    Ok(CteBody {
        query,
        output_columns: output_cols,
    })
}

/// Build the final projection for a tree-group-in-reducing_by result.
///
/// Wraps leaf keys + nested agg references in JSON_OBJECT (the tree group alias),
/// then passes through extra aggregate columns.
fn build_reducing_by_final_projection(
    projected: Builder<Projected>,
    leaf_members: &[CurlyMember<Addressed>],
    nested_items: &[(String, Vec<CurlyMember<Addressed>>, bool)],
    tg_alias: Option<delightql_types::SqlIdentifier>,
    extra_start: usize,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
) -> Result<Builder<Projected>> {
    let mut final_items = Vec::new();
    let mut json_args = Vec::new();
    let columns = projected.columns();

    for member in leaf_members {
        let key_name = match member {
            CurlyMember::Shorthand { column, .. } => column.as_str().to_string(),
            CurlyMember::KeyValue { key, .. } => key.clone(),
            _ => continue,
        };
        if let Some(col) = columns.iter().find(|c| col_name(c) == key_name.as_str()) {
            let sql_expr = match col_qualifier(col) {
                Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &key_name),
                None => SqlExpr::column(&key_name),
            };
            json_args.push(SqlExpr::literal(LiteralValue::String(key_name)));
            json_args.push(sql_expr);
        }
    }
    for (key, _, _) in nested_items {
        if let Some(col) = columns.iter().find(|c| col_name(c) == key.as_str()) {
            let sql_expr = match col_qualifier(col) {
                Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), key),
                None => SqlExpr::column(key),
            };
            json_args.push(SqlExpr::literal(LiteralValue::String(key.clone())));
            json_args.push(SqlExpr::function("json", vec![sql_expr]));
        }
    }

    // Use CprSchema name for the tree group column (resolver's authoritative name)
    let cpr_names: Vec<String> = match cpr_schema.get() {
        CprSchema::Resolved(cols) => cols.iter().map(|c| c.name().to_string()).collect(),
        _ => Vec::new(),
    };
    let tg_final_alias = tg_alias
        .as_ref()
        .map(|a| a.as_str().to_string())
        .or_else(|| cpr_names.first().cloned());

    let json_object = SqlExpr::function("JSON_OBJECT", json_args);
    final_items.push(SelectItem::Expression {
        expr: json_object,
        alias: tg_final_alias,
    });

    for col in columns.iter().skip(extra_start) {
        let name = col_name(col);
        let sql_expr = match col_qualifier(col) {
            Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), name),
            None => SqlExpr::column(name),
        };
        final_items.push(SelectItem::Expression {
            expr: sql_expr,
            alias: Some(name.to_string()),
        });
    }

    projected.add_projection(final_items)
}

// ---------------------------------------------------------------------------
// Curly aggregate lowering (moved from scalar.rs)
// ---------------------------------------------------------------------------

/// Lower a Curly in aggregate position (inside `reducing_on`).
///
/// Lowers the members to a JSON_OBJECT, then wraps with the null-elision
/// aggregate pattern via `build_aggregate_wrapper`.
pub(super) fn s_lower_curly_aggregate(
    members: Vec<CurlyMember<Addressed>>,
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<SqlExpr> {
    let null_check_exprs = collect_null_check_columns(&members, qualify, ctx)?;
    let json_object = scalar::s_lower_curly_scalar(members, qualify, ctx)?;
    Ok(build_aggregate_wrapper(json_object, null_check_exprs))
}

/// Collect column references from curly members for IS NOT NULL checks.
fn collect_null_check_columns(
    members: &[CurlyMember<Addressed>],
    qualify: &dyn Qualify,
    ctx: &TransformCtx,
) -> Result<Vec<SqlExpr>> {
    let mut exprs = Vec::new();
    for member in members {
        match member {
            CurlyMember::Shorthand {
                column, qualifier, ..
            } => {
                let qual_str = qualifier.as_ref().map(|q| q.as_str());
                exprs.push(scalar::s_lower_lvar(
                    column.as_str(),
                    qual_str,
                    qualify,
                    ctx,
                )?);
            }
            CurlyMember::KeyValue {
                value,
                nested_reduction: false,
                ..
            } => {
                exprs.push(scalar::s_lower_expression((**value).clone(), qualify, ctx)?);
            }
            _ => {}
        }
    }
    Ok(exprs)
}

/// Build `col1 IS NOT NULL OR col2 IS NOT NULL OR ...` from column expressions.
fn build_null_check(exprs: Vec<SqlExpr>) -> SqlExpr {
    let checks: Vec<SqlExpr> = exprs
        .into_iter()
        .map(|e| SqlExpr::Binary {
            left: Box::new(e),
            op: BinaryOperator::IsNot,
            right: Box::new(SqlExpr::literal(LiteralValue::Null)),
        })
        .collect();

    match checks.len() {
        0 => SqlExpr::literal(LiteralValue::Boolean(true)),
        1 => checks.into_iter().next().unwrap(),
        _ => {
            let mut iter = checks.into_iter();
            let first = iter.next().unwrap();
            iter.fold(first, |acc, check| SqlExpr::Binary {
                left: Box::new(acc),
                op: BinaryOperator::Or,
                right: Box::new(check),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Nesting level collection
// ---------------------------------------------------------------------------

/// A GROUP BY key — column name with optional original table qualifier.
///
/// The qualifier enables correct resolution through the identity stack
/// when columns have been disambiguated (e.g., `o.id` → `id_2`).
#[derive(Clone)]
struct GroupKey {
    name: String,
    qualifier: Option<String>,
}

impl GroupKey {
    fn simple(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            qualifier: None,
        }
    }
    fn qualified(name: impl Into<String>, qualifier: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            qualifier: Some(qualifier.into()),
        }
    }
}

/// One level of CTE nesting.
struct NestingLevel {
    /// Non-nested members at this level (become JSON_OBJECT fields).
    /// Empty for metadata levels and sibling aggregate levels.
    leaf_members: Vec<CurlyMember<Addressed>>,
    /// GROUP BY key columns for this level.
    group_keys: Vec<GroupKey>,
    /// Alias for the aggregated column produced at this level.
    agg_alias: String,
    /// Aliases of inner CTE aggregate columns (for json() wrapping).
    inner_agg_refs: Vec<String>,
    /// For metadata levels: the key column whose values become JSON object keys.
    metadata_key: Option<GroupKey>,
    /// Concurrent sibling aggregates. When non-empty, each produces its own
    /// aggregate column in the same CTE (same GROUP BY, same row set).
    sibling_aggregates: Vec<SiblingAggregate>,
}

/// A single sibling aggregate within a concurrent level.
struct SiblingAggregate {
    alias: String,
    members: Vec<CurlyMember<Addressed>>,
    /// True when the source was bracket syntax `~> [cols]` (flat array),
    /// false for curly syntax `~> {cols}` (array of objects).
    is_bracket: bool,
}

/// Extract Curly members and metadata keys from a reducing_on item.
fn extract_tree_group_item(
    item: ast_addressed::DomainExpression,
    metadata_keys: &mut Vec<GroupKey>,
) -> Result<(
    Vec<CurlyMember<Addressed>>,
    Option<delightql_types::SqlIdentifier>,
)> {
    match item {
        ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
            members,
            alias,
            ..
        }) => Ok((members, alias)),
        ast_addressed::DomainExpression::Function(
            func @ ast_addressed::FunctionExpression::MetadataTreeGroup { .. },
        ) => Ok(unwrap_metadata_layers(func, metadata_keys)),
        _ => Err(DelightQLError::ParseError {
            message: "r_lower_tree_group_cte: expected Curly or MetadataTreeGroup in reducing_on"
                .into(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Unwrap MetadataTreeGroup layers, collecting metadata key columns.
fn unwrap_metadata_layers(
    func: ast_addressed::FunctionExpression,
    keys: &mut Vec<GroupKey>,
) -> (
    Vec<CurlyMember<Addressed>>,
    Option<delightql_types::SqlIdentifier>,
) {
    match func {
        ast_addressed::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            constructor,
            alias,
            ..
        } => {
            keys.push(match key_qualifier {
                Some(q) => GroupKey::qualified(key_column.as_str(), q.as_str()),
                None => GroupKey::simple(key_column.as_str()),
            });
            let (members, inner_alias) = unwrap_metadata_layers(*constructor, keys);
            (members, alias.or(inner_alias))
        }
        ast_addressed::FunctionExpression::Curly { members, alias, .. } => (members, alias),
        _ => (vec![], None),
    }
}

/// Recursively collect nesting levels bottom-up from Curly members.
///
/// `current_group_keys` accumulates as we recurse inward: each level's
/// leaf member column names become additional GROUP BY keys for deeper levels.
fn collect_levels(
    members: Vec<CurlyMember<Addressed>>,
    current_group_keys: Vec<GroupKey>,
    agg_alias: String,
    levels: &mut Vec<NestingLevel>,
) {
    let (leaf, nested_items) = separate_leaf_and_nested(members);

    if !nested_items.is_empty() {
        // Leaf member column references become GROUP BY keys for deeper levels
        let mut inner_keys = current_group_keys.clone();
        for member in &leaf {
            match member {
                CurlyMember::Shorthand {
                    column, qualifier, ..
                } => {
                    inner_keys.push(match qualifier {
                        Some(q) => GroupKey::qualified(column.as_str(), q.as_str()),
                        None => GroupKey::simple(column.as_str()),
                    });
                }
                CurlyMember::KeyValue {
                    value,
                    nested_reduction: false,
                    ..
                } => {
                    // Extract column reference from the value expression
                    if let ast_addressed::DomainExpression::Lvar {
                        name, qualifier, ..
                    } = value.as_ref()
                    {
                        inner_keys.push(match qualifier {
                            Some(q) => GroupKey::qualified(name.as_str(), q.as_str()),
                            None => GroupKey::simple(name.as_str()),
                        });
                    }
                }
                _ => {}
            }
        }

        // Check if all siblings are flat (no further nesting inside any of them).
        // Flat siblings can share a single CTE level with concurrent aggregates.
        let all_flat = nested_items.len() > 1
            && nested_items.iter().all(|(_, members, _)| {
                !members.iter().any(|m| {
                    matches!(
                        m,
                        CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        }
                    )
                })
            });

        let mut inner_agg_refs = Vec::new();

        if all_flat {
            // Concurrent siblings: one CTE level with multiple aggregate columns.
            let sibling_aggs: Vec<SiblingAggregate> = nested_items
                .into_iter()
                .map(|(key, members, is_bracket)| {
                    inner_agg_refs.push(key.clone());
                    SiblingAggregate {
                        alias: key,
                        members,
                        is_bracket,
                    }
                })
                .collect();

            levels.push(NestingLevel {
                leaf_members: vec![],
                group_keys: inner_keys,
                agg_alias: String::new(),
                inner_agg_refs: vec![],
                metadata_key: None,
                sibling_aggregates: sibling_aggs,
            });
        } else {
            // Single nested item or nested-with-further-nesting: recurse as before.
            for (nested_key, inner_members, _is_bracket) in nested_items {
                collect_levels(
                    inner_members,
                    inner_keys.clone(),
                    nested_key.clone(),
                    levels,
                );
                inner_agg_refs.push(nested_key);
            }
        }

        levels.push(NestingLevel {
            leaf_members: leaf,
            group_keys: current_group_keys,
            agg_alias,
            inner_agg_refs,
            metadata_key: None,
            sibling_aggregates: vec![],
        });
    } else {
        levels.push(NestingLevel {
            leaf_members: leaf,
            group_keys: current_group_keys,
            agg_alias,
            inner_agg_refs: Vec::new(),
            metadata_key: None,
            sibling_aggregates: vec![],
        });
    }
}

/// Append metadata NestingLevels (JSON_GROUP_OBJECT) after regular levels.
fn append_metadata_levels(
    metadata_keys: &[GroupKey],
    reducing_by_names: &[String],
    levels: &mut Vec<NestingLevel>,
    cpr_tg_name: Option<&str>,
) {
    for (i, key) in metadata_keys.iter().rev().enumerate() {
        let is_outermost = i == metadata_keys.len() - 1;
        let agg_alias = if is_outermost {
            cpr_tg_name.unwrap_or("result").to_string()
        } else {
            "constructor".to_string()
        };

        let original_idx = metadata_keys.len() - 1 - i;
        let mut group_keys: Vec<GroupKey> = reducing_by_names
            .iter()
            .map(|n| GroupKey::simple(n))
            .collect();
        group_keys.extend(metadata_keys[..original_idx].iter().cloned());

        let inner_ref = levels
            .last()
            .map(|l| l.agg_alias.clone())
            .unwrap_or_default();

        levels.push(NestingLevel {
            leaf_members: vec![],
            group_keys,
            agg_alias,
            inner_agg_refs: vec![inner_ref],
            metadata_key: Some(key.clone()),
            sibling_aggregates: vec![],
        });
    }
}

// ---------------------------------------------------------------------------
// CTE body construction
// ---------------------------------------------------------------------------

/// Build the SQL body for a single CTE level.
fn build_cte_body(
    level: &NestingLevel,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
) -> Result<CteBody> {
    let mut group_by_exprs = Vec::new();
    let mut select_items = Vec::new();

    // GROUP BY key columns
    for gk in &level.group_keys {
        let qc = if let Some(ref tbl) = gk.qualifier {
            input
                .qualify_with_table(&gk.name, tbl)
                .unwrap_or(input.qualify(&gk.name)?)
        } else {
            input.qualify(&gk.name)?
        };
        let sql_expr = match &qc.qualifier {
            Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc.name),
            None => SqlExpr::column(&qc.name),
        };
        group_by_exprs.push(sql_expr.clone());
        select_items.push(SelectItem::Expression {
            expr: sql_expr,
            alias: Some(qc.name.clone()),
        });
    }

    // Aggregate expression(s)
    if !level.sibling_aggregates.is_empty() {
        for sibling in &level.sibling_aggregates {
            build_sibling_aggregate(sibling, input, ctx, &mut select_items)?;
        }
    } else if let Some(ref meta_key) = level.metadata_key {
        build_metadata_aggregate(level, input, meta_key, &mut select_items)?;
    } else {
        build_regular_aggregate(level, input, ctx, &mut select_items)?;
    }

    assemble_cte(input, select_items, group_by_exprs)
}

/// Build `JSON_GROUP_OBJECT(key_col, json(inner_col))` for metadata levels.
fn build_metadata_aggregate(
    level: &NestingLevel,
    input: &super::builder::CteInput,
    meta_key: &GroupKey,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    let inner_alias = level
        .inner_agg_refs
        .first()
        .expect("metadata level must have inner ref");
    let qc_key = if let Some(ref tbl) = meta_key.qualifier {
        input
            .qualify_with_table(&meta_key.name, tbl)
            .unwrap_or(input.qualify(&meta_key.name)?)
    } else {
        input.qualify(&meta_key.name)?
    };
    let key_col = match &qc_key.qualifier {
        Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc_key.name),
        None => SqlExpr::column(&qc_key.name),
    };
    let qc_val = input.qualify(inner_alias)?;
    let val_col = match &qc_val.qualifier {
        Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc_val.name),
        None => SqlExpr::column(&qc_val.name),
    };
    let agg_expr = SqlExpr::function(
        "JSON_GROUP_OBJECT",
        vec![key_col, SqlExpr::function("json", vec![val_col])],
    );
    select_items.push(SelectItem::Expression {
        expr: agg_expr,
        alias: Some(level.agg_alias.clone()),
    });
    Ok(())
}

/// Lower a leaf CurlyMember to (json_key, sql_expr, is_tree) for
/// JSON_OBJECT construction.
///
/// Handles both Shorthand (`{col}` → key is column name) and non-nested
/// KeyValue (`"key": expr` → key is explicit, expr is scalar-lowered).
/// `is_tree` is true when the member is a bare reference to a column with
/// a known interior schema (a staged tree-group column): the caller must
/// wrap it in `json()` for the embedding to splice — the same treatment
/// `inner_agg_refs` members get — or the engine escapes it as TEXT.
fn lower_leaf_member(
    member: &CurlyMember<Addressed>,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
) -> Result<Option<(String, SqlExpr, bool)>> {
    match member {
        CurlyMember::Shorthand {
            column, qualifier, ..
        } => {
            let col_name = column.as_str();
            let qc = if let Some(q) = qualifier {
                input
                    .qualify_with_table(col_name, q.as_str())
                    .unwrap_or(input.qualify(col_name)?)
            } else {
                input.qualify(col_name)?
            };
            let sql_col = match &qc.qualifier {
                Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc.name),
                None => SqlExpr::column(&qc.name),
            };
            // Tree-valuedness comes from the SAME resolution that chose
            // the SQL spelling — one identity question, one answer.
            let is_tree = qc.has_interior_schema;
            Ok(Some((col_name.to_string(), sql_col, is_tree)))
        }
        CurlyMember::KeyValue {
            key,
            value,
            nested_reduction: false,
        } => {
            // Lower the value expression — typically an Lvar like `o.id`.
            // If the expression references columns not in the CTE scope
            // (e.g., nested curlies), skip gracefully.
            let is_tree = match &**value {
                ast_addressed::DomainExpression::Lvar {
                    name, qualifier, ..
                } => input.tree_valued(name.as_str(), qualifier.as_ref().map(|q| q.as_str())),
                _ => false,
            };
            match lower_domain_expr_for_cte(value, input, ctx) {
                Ok(sql_expr) => Ok(Some((key.clone(), sql_expr, is_tree))),
                Err(_) => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Lower a DomainExpression against a CteInput scope.
///
/// Delegates to the standard scalar lowering, using CteInput as the Qualify impl.
fn lower_domain_expr_for_cte(
    expr: &ast_addressed::DomainExpression,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
) -> Result<SqlExpr> {
    scalar::s_lower_expression(expr.clone(), input, ctx)
}

/// Build the null-elision GROUP_CONCAT aggregate for regular (data-oriented) levels.
fn build_regular_aggregate(
    level: &NestingLevel,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    let mut json_args = Vec::new();
    let mut null_checks = Vec::new();

    for member in &level.leaf_members {
        if let Some((key, sql_col, is_tree)) = lower_leaf_member(member, input, ctx)? {
            json_args.push(SqlExpr::literal(LiteralValue::String(key)));
            json_args.push(if is_tree {
                SqlExpr::function("json", vec![sql_col.clone()])
            } else {
                sql_col.clone()
            });
            null_checks.push(sql_col);
        }
    }

    for inner_alias in &level.inner_agg_refs {
        let qc = input.qualify(inner_alias)?;
        let inner_col = match &qc.qualifier {
            Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), &qc.name),
            None => SqlExpr::column(&qc.name),
        };
        json_args.push(SqlExpr::literal(LiteralValue::String(inner_alias.clone())));
        json_args.push(SqlExpr::function("json", vec![inner_col.clone()]));
        null_checks.push(inner_col);
    }

    let agg_expr =
        build_aggregate_wrapper(SqlExpr::function("JSON_OBJECT", json_args), null_checks);

    select_items.push(SelectItem::Expression {
        expr: agg_expr,
        alias: Some(level.agg_alias.clone()),
    });
    Ok(())
}

/// Build a single sibling's aggregate column (null-elision GROUP_CONCAT).
///
/// Bracket siblings (`~> [cols]`) produce flat arrays via JSON_ARRAY.
/// Curly siblings (`~> {cols}`) produce arrays of objects via JSON_OBJECT.
fn build_sibling_aggregate(
    sibling: &SiblingAggregate,
    input: &super::builder::CteInput,
    ctx: &TransformCtx,
    select_items: &mut Vec<SelectItem>,
) -> Result<()> {
    let mut null_checks = Vec::new();
    let mut cols = Vec::new();

    for member in &sibling.members {
        if let Some((key, sql_col, is_tree)) = lower_leaf_member(member, input, ctx)? {
            null_checks.push(sql_col.clone());
            let embedded = if is_tree {
                SqlExpr::function("json", vec![sql_col])
            } else {
                sql_col
            };
            cols.push((key, embedded));
        }
    }

    let json_expr = if sibling.is_bracket {
        // Bracket: each row is a tuple → JSON_ARRAY(col1, col2, ...)
        SqlExpr::function("JSON_ARRAY", cols.iter().map(|(_, c)| c.clone()).collect())
    } else {
        // Curly: array of objects → JSON_OBJECT('key1', val1, 'key2', val2, ...)
        let mut json_args = Vec::new();
        for (name, sql_col) in &cols {
            json_args.push(SqlExpr::literal(LiteralValue::String(name.clone())));
            json_args.push(sql_col.clone());
        }
        SqlExpr::function("JSON_OBJECT", json_args)
    };

    let agg_expr = build_aggregate_wrapper(json_expr, null_checks);

    select_items.push(SelectItem::Expression {
        expr: agg_expr,
        alias: Some(sibling.alias.clone()),
    });
    Ok(())
}

/// Build the null-elision aggregate wrapper around a JSON_OBJECT expression.
///
/// ```sql
/// COALESCE(
///   JSON('[' || GROUP_CONCAT(
///     CASE WHEN (c1 IS NOT NULL OR c2 IS NOT NULL)
///          THEN json_object_expr
///     END, ','
///   ) || ']'),
///   JSON('[]')
/// )
/// ```
fn build_aggregate_wrapper(json_object_expr: SqlExpr, null_check_exprs: Vec<SqlExpr>) -> SqlExpr {
    let null_check = build_null_check(null_check_exprs);

    let case_expr = SqlExpr::Case {
        expr: None,
        when_clauses: vec![WhenClause::new(null_check, json_object_expr)],
        else_clause: None,
    };

    let group_concat = SqlExpr::function(
        "GROUP_CONCAT",
        vec![
            case_expr,
            SqlExpr::literal(LiteralValue::String(",".to_string())),
        ],
    );

    let json_array = SqlExpr::function(
        "JSON",
        vec![SqlExpr::concat(
            SqlExpr::concat(
                SqlExpr::literal(LiteralValue::String("[".to_string())),
                group_concat,
            ),
            SqlExpr::literal(LiteralValue::String("]".to_string())),
        )],
    );

    SqlExpr::function(
        "COALESCE",
        vec![
            json_array,
            SqlExpr::function(
                "JSON",
                vec![SqlExpr::literal(LiteralValue::String("[]".to_string()))],
            ),
        ],
    )
}

// ---------------------------------------------------------------------------
// Final projection
// ---------------------------------------------------------------------------

/// Build the final SELECT from the last CTE.
fn build_final_projection(
    projected: Builder<Projected>,
    levels: &[NestingLevel],
    reducing_by: &[ast_addressed::DomainExpression],
    alias: Option<delightql_types::SqlIdentifier>,
    cpr_schema: &PhaseBox<CprSchema, Addressed>,
) -> Result<Builder<Projected>> {
    let last_level = levels.last().unwrap();
    let mut final_items = Vec::new();

    // Get the CprSchema column names for aliasing — the resolver's authoritative names.
    let cpr_names: Vec<String> = match cpr_schema.get() {
        CprSchema::Resolved(cols) => cols.iter().map(|c| c.name().to_string()).collect(),
        _ => Vec::new(),
    };

    let num_keys = reducing_by.len();

    for (i, key_expr) in reducing_by.iter().enumerate() {
        if let ast_addressed::DomainExpression::Lvar { name, .. } = key_expr {
            let key_name = name.as_str();
            let qc = projected.columns().iter().find(|c| col_name(c) == key_name);
            let sql_expr = if let Some(col) = qc {
                match col_qualifier(col) {
                    Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), key_name),
                    None => SqlExpr::column(key_name),
                }
            } else {
                SqlExpr::column(key_name)
            };
            // Use CprSchema name if available, otherwise key_name
            let out_alias = cpr_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| key_name.to_string());
            final_items.push(SelectItem::Expression {
                expr: sql_expr,
                alias: Some(out_alias),
            });
        }
    }

    let agg_name = &last_level.agg_alias;
    let agg_qc = projected
        .columns()
        .iter()
        .find(|c| col_name(c) == agg_name.as_str());
    let agg_expr = if let Some(col) = agg_qc {
        match col_qualifier(col) {
            Some(q) => SqlExpr::with_qualifier(ColumnQualifier::table(q), agg_name),
            None => SqlExpr::column(agg_name),
        }
    } else {
        SqlExpr::column(agg_name)
    };
    // Use CprSchema name for the aggregate column — this is the resolver's name
    // (e.g., "tree_group_") rather than the transformer's internal name ("result").
    let final_alias = alias
        .as_ref()
        .map(|a| a.as_str().to_string())
        .or_else(|| cpr_names.get(num_keys).cloned())
        .unwrap_or_else(|| agg_name.clone());
    final_items.push(SelectItem::Expression {
        expr: agg_expr,
        alias: Some(final_alias),
    });

    projected.add_projection(final_items)
}
