// ProjectOut operator: |> -[...]

use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectItem, SelectStatement};

use super::super::super::context::TransformContext;
use super::super::shared::{check_schema_dependent_operation, source_column_ref};

/// Handle ProjectOut operator: |> -[...]
///
/// The output is a subset of the source columns (same order, some removed).
/// Uses sequential matching to find source provenance for each retained column.
pub fn apply_project_out(
    builder: SelectBuilder,
    cpr_schema: &ast_addressed::CprSchema,
    source_schema_updated: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<SelectStatement> {
    // Check if this operation is compatible with the schema
    check_schema_dependent_operation(cpr_schema, "ProjectOut")?;

    // ProjectOut: The resolver has already removed excluded columns from CprSchema
    // We just output what remains, using effective column names (aliases if present)
    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        _ => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "ProjectOut requires resolved schema".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };

    let source_cols = match source_schema_updated {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => Some(resolved_columns.as_slice()),
        other => panic!(
            "catch-all hit in project_out.rs apply_project_out (CprSchema): {:?}",
            other
        ),
    };

    // Match each output column to its source column by sequential name matching.
    // Project_out preserves relative order, so we walk both lists forward.
    let mut src_idx = 0;
    let mut select_items = Vec::with_capacity(columns.len());

    // Borrow drill column mappings once for the loop
    let drill_mappings = _ctx.drill_column_mappings.borrow();
    let mut seen_names: HashSet<String> = HashSet::new();

    // Pre-count output column name occurrences for de-disambiguation detection.
    // When the resolver excludes a context column that collided with an interior
    // column, it un-disambiguates (renames __entities_name back to name). We
    // detect this case: the name appears exactly once in the output AND a drill
    // mapping exists. When both columns survive (count=2), duplicate detection
    // handles it instead.
    let name_counts: HashMap<String, usize> = {
        let mut counts = HashMap::new();
        for c in columns {
            if let Some(name) = c.info.alias_name().or_else(|| c.info.original_name()) {
                *counts.entry(name.to_lowercase()).or_insert(0) += 1;
            }
        }
        counts
    };

    for col in columns {
        let effective_name = col
            .info
            .alias_name()
            .or_else(|| col.info.original_name())
            .unwrap_or("?")
            .to_string();

        // Check drill column mappings for disambiguated interior columns.
        // First try: look up by source column qualifier (works when qualifier
        // is Named, e.g., single drill-down without subquery wrapping).
        let source_qualifier = if let ast_addressed::TableName::Named(q) = &col.fq_table.name {
            Some(q.to_string())
        } else {
            None
        };

        let mapped_alias = if let Some(qualifier) = &source_qualifier {
            let key = format!("{}.{}", qualifier, effective_name);
            drill_mappings.get(&key).map(|s| s.as_str())
        } else {
            None
        };

        // Fallback: duplicate detection for Fresh qualifiers (chained drill-downs).
        // By construction, context columns come first, interior columns second.
        // The second occurrence of a name is an interior column from a prior drill.
        let is_duplicate = !seen_names.insert(effective_name.to_lowercase());
        let mapped_alias = mapped_alias.or_else(|| {
            if is_duplicate {
                drill_mappings
                    .iter()
                    .find(|(key, value)| {
                        key.ends_with(&format!(".{}", effective_name))
                            && value.as_str() != effective_name
                    })
                    .map(|(_, v)| v.as_str())
            } else {
                None
            }
        });

        // Fallback: de-disambiguation detection. When the resolver excluded the
        // context column and renamed __entities_name back to name, the name
        // appears exactly once. Use the drill mapping to find the correct source
        // SQL column (__entities_name) instead of the context column (name).
        let appears_once = name_counts
            .get(&effective_name.to_lowercase())
            .copied()
            .unwrap_or(0)
            == 1;
        let mapped_alias = mapped_alias.or_else(|| {
            if appears_once {
                drill_mappings
                    .iter()
                    .find(|(key, value)| {
                        key.ends_with(&format!(".{}", effective_name))
                            && value.as_str() != effective_name
                    })
                    .map(|(_, v)| v.as_str())
            } else {
                None
            }
        });

        // Find the next source column matching this name (or the mapped alias).
        let walk_name = mapped_alias.unwrap_or(&effective_name);
        let matched_source = source_cols.and_then(|src| {
            while src_idx < src.len() {
                let si = src_idx;
                src_idx += 1;
                let src_name = src[si].info.name().unwrap_or("");
                if src_name == walk_name {
                    return Some(&src[si]);
                }
            }
            None
        });

        // Respect the output column's provenance: if the resolver assigned Fresh
        // AND the source column originates from a CTE (which may be aliased in a
        // JOIN, making the CTE name invalid as a qualifier), emit unqualified refs.
        // For non-CTE sources (regular table aliases in JOINs), preserve qualifiers
        // even when output is Fresh — they're needed for disambiguation.
        let effective_source =
            if matches!(&col.fq_table.name, ast_addressed::TableName::Fresh)
                && matched_source
                    .map_or(false, |s| s.info.referenceable_cte_name().is_some())
            {
                None
            } else {
                matched_source
            };

        let (expr, alias) = if let Some(sql_alias) = mapped_alias {
            (
                crate::pipeline::sql_ast_v3::DomainExpression::RawSql(sql_alias.to_string()),
                &effective_name,
            )
        } else {
            (
                source_column_ref(&effective_name, effective_source),
                &effective_name,
            )
        };

        select_items.push(SelectItem::expression_with_alias(expr, alias));
    }
    drop(drill_mappings);

    if select_items.is_empty() {
        return Err(crate::error::DelightQLError::ParseError {
            message: "ProjectOut removed all columns".to_string(),
            source: None,
            subcategory: None,
        });
    }

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}
