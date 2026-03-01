// Interior drill-down operator: .column(*)
// Explodes an interior relation (tree group) column into rows using json_each.
//
// Returns a flat Builder (not an Expression) so that table qualifiers from
// the source are preserved through downstream pipes. This matches how joins
// keep FROM flat — one code path for qualifier handling.

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::asts::core::operators::InteriorColumnDef;
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlExpr, SelectBuilder, SelectItem, TableExpression, TvfArgument,
};
use crate::pipeline::transformer_v3::QualifierScope;
use std::collections::HashSet;

use super::super::super::context::TransformContext;
use super::super::super::helpers::alias_generator::next_alias;

/// Extract the referenceable name from a TableExpression (alias if present, else table name).
fn table_ref_name(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Table { alias, name, .. } => {
            Some(alias.as_deref().unwrap_or(name).to_string())
        }
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::TVF { alias, .. } => alias.clone(),
        other => panic!(
            "catch-all hit in interior_drill_down.rs table_ref_name: {:?}",
            other
        ),
    }
}

/// Handle InteriorDrillDown operator: explode an interior relation column into rows.
///
/// Keeps the FROM flat by adding CROSS JOIN json_each directly to the builder's
/// FROM clause, rather than wrapping the source as a subquery. This preserves
/// table qualifiers so downstream `-(qualifier.*)` and filters work correctly.
///
/// When an interior column name collides with a context column name, the
/// interior column gets a disambiguated SQL alias (`__<drill>__<name>`) so
/// that subquery wrapping preserves both columns distinctly.
///
/// Generates SQL like:
/// ```sql
/// SELECT by_country.country,
///        json_extract(_drill_0.value, '$.first_name') AS first_name,
///        json_extract(_drill_0.value, '$.last_name') AS last_name
/// FROM by_country, json_each(by_country."people") AS _drill_0
/// ```
pub fn apply_interior_drill_down(
    builder: SelectBuilder,
    column: String,
    glob: bool,
    columns: Vec<String>,
    interior_schema: Option<Vec<InteriorColumnDef>>,
    groundings: Vec<(String, String)>,
    output_cpr_schema: &ast_addressed::CprSchema,
    source_schema: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<SelectBuilder> {
    let schema = interior_schema.ok_or_else(|| crate::error::DelightQLError::ParseError {
        message: format!(
            "InteriorDrillDown: no interior schema for column '{}'. \
             This should have been resolved during the resolver phase.",
            column
        ),
        source: None,
        subcategory: None,
    })?;

    // Get source table reference name from the builder's FROM clause.
    // This is the alias or table name we use to qualify column references.
    let source_tables = builder.get_from().cloned().unwrap_or_default();
    let source_ref = source_tables
        .first()
        .and_then(table_ref_name)
        .unwrap_or_else(|| next_alias());

    // Determine which interior columns to extract
    let interior_cols: Vec<&InteriorColumnDef> = if glob {
        schema.iter().collect()
    } else {
        columns
            .iter()
            .filter_map(|name| schema.iter().find(|d| d.name == *name))
            .collect()
    };

    // Build SELECT items
    let mut select_items = Vec::new();

    // 1. Context columns: all source columns EXCEPT the drilled column
    let source_cols = match source_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        other => panic!("catch-all hit in interior_drill_down.rs apply_interior_drill_down (CprSchema source_cols): {:?}", other),
    };

    // Collect context column names for collision detection
    let context_names: HashSet<String> = source_cols
        .iter()
        .filter_map(|col| {
            let name = col
                .info
                .alias_name()
                .or_else(|| col.info.original_name())
                .unwrap_or("?");
            if name.eq_ignore_ascii_case(&column) {
                None // Exclude the drilled column itself
            } else {
                Some(name.to_lowercase())
            }
        })
        .collect();

    // Check existing drill_column_mappings for columns disambiguated by a
    // previous drill-down. When chaining (e.g., .entities(*).columns(*)),
    // the second drill-down must reference disambiguated SQL aliases from the
    // first drill-down, not the DQL names from CprSchema.
    //
    // After subquery wrapping, qualifiers are Fresh, so we can't look up by
    // qualifier. Instead, use duplicate detection: by construction, context
    // columns come first in the CprSchema, interior columns second. When a
    // name appears for the second time, it's an interior column that may have
    // been disambiguated.
    let prior_mappings = _ctx.drill_column_mappings.borrow();
    let mut seen_names: HashSet<String> = HashSet::new();

    for col in source_cols {
        let col_name = col
            .info
            .alias_name()
            .or_else(|| col.info.original_name())
            .unwrap_or("?");
        if col_name.eq_ignore_ascii_case(&column) {
            continue; // Skip the drilled column
        }

        let is_duplicate = !seen_names.insert(col_name.to_lowercase());

        // For duplicate names, check if a prior drill-down disambiguated this
        // column. Look up by column name suffix across all mappings.
        let sql_name = if is_duplicate {
            prior_mappings
                .iter()
                .find(|(key, value)| {
                    key.ends_with(&format!(".{}", col_name)) && value.as_str() != col_name
                })
                .map(|(_, v)| v.as_str())
        } else {
            None
        };
        let sql_name = sql_name.unwrap_or(col_name);

        select_items.push(SelectItem::expression_with_alias(
            SqlExpr::with_qualifier(QualifierScope::structural(&source_ref), sql_name),
            sql_name,
        ));
    }
    drop(prior_mappings);

    // 2. Interior columns: json_extract(_drill_N.value, '$.col_name') AS col_name
    //    When a name collides with a context column, use a compound alias
    //    (__<drill_column>__<name>) so subquery wrapping preserves both.
    //    For positional binding (resolver renamed columns), use the CprSchema
    //    alias name instead of the interior schema name.
    let drill_alias = format!("_drill_{}", next_alias().replace("t", ""));
    let drill_alias_ref = drill_alias.clone(); // Keep before move into TVF

    // Extract output alias names from the CprSchema for interior columns.
    // The CprSchema has context columns first, then interior columns.
    let num_context = context_names.len();
    let output_cols = match output_cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols.as_slice(),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns.as_slice(),
        other => panic!("catch-all hit in interior_drill_down.rs apply_interior_drill_down (CprSchema output_cols): {:?}", other),
    };

    // Compute (schema_name, output_alias, sql_alias) for each interior column.
    // Used for both the SELECT items and the drill_column_mappings — they must
    // agree on the SQL alias to avoid referencing nonexistent columns.
    let mut interior_info: Vec<(&str, String, String)> = Vec::new();
    for (i, def) in interior_cols.iter().enumerate() {
        let cpr_name = output_cols
            .get(num_context + i)
            .and_then(|col| col.info.alias_name().or_else(|| col.info.original_name()));
        let output_name = cpr_name.unwrap_or(&def.name);

        let collides = context_names.contains(&output_name.to_lowercase());
        let sql_alias = if collides {
            format!("__{}_{}", column, output_name)
        } else {
            output_name.to_string()
        };
        interior_info.push((&def.name, output_name.to_string(), sql_alias));
    }

    for (schema_name, _output_name, sql_alias) in &interior_info {
        select_items.push(SelectItem::expression_with_alias(
            SqlExpr::RawSql(format!(
                "json_extract({}.value, '$.{}')",
                drill_alias, schema_name
            )),
            sql_alias,
        ));
    }

    // Build FROM: keep existing tables + add json_each TVF
    let json_each_tvf = TableExpression::TVF {
        schema: None,
        function: "json_each".to_string(),
        arguments: vec![TvfArgument::Identifier(format!(
            "\"{}\".\"{}\"",
            source_ref, column
        ))],
        alias: Some(drill_alias),
    };

    let mut from_tables = source_tables;
    from_tables.push(json_each_tvf);

    // Populate drill column mappings: maps (qualifier.name) → SQL alias.
    // Uses the same sql_alias as the SELECT items above so downstream
    // operators (project-out, expression transformer) reference columns
    // that actually exist in the SQL.
    {
        let mut mappings = _ctx.drill_column_mappings.borrow_mut();
        for (schema_name, _output_name, sql_alias) in &interior_info {
            let key = format!("{}.{}", column, schema_name);
            mappings.insert(key, sql_alias.clone());
        }
    }

    // Generate WHERE conditions for grounded positions.
    // Each grounding is (schema_column_name, literal_value) → produces
    // json_extract(_drill_N.value, '$.col') = 'val'.
    let mut result = builder.set_select(select_items).from_tables(from_tables);
    for (schema_name, value) in &groundings {
        let condition = SqlExpr::RawSql(format!(
            "json_extract({}.value, '$.{}') = '{}'",
            drill_alias_ref,
            schema_name,
            value.replace('\'', "''")
        ));
        result = result.and_where(condition);
    }

    Ok(result)
}
