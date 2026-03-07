use crate::error::{DelightQLError, Result};
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::domain_expressions::{
    resolve_expressions_with_schema, resolve_expressions_with_schema_internal,
};
use super::helpers::{emit_validation_warning, expand_column_template};

/// Resolve the ProjectOut operator
///
/// This removes specified columns from the schema.
/// Pattern matching can select zero columns (warning but valid - no-op).
pub(super) fn resolve_project_out(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve expressions to remove - allow zero matches for patterns
    let resolved_expressions = resolve_expressions_with_schema_internal(
        expressions,
        available,
        true,
        None,
        None,
        None,
        false,
    )?;

    // Compute output columns - remove specified columns from input
    let mut output_columns = available.to_vec();
    let removed_count = resolved_expressions.len();
    let mut any_qualified = false;
    for expr in &resolved_expressions {
        if let ast_resolved::DomainExpression::Lvar {
            name, qualifier, ..
        } = expr
        {
            if qualifier.is_some() {
                any_qualified = true;
            }
            output_columns.retain(|col| {
                if !crate::pipeline::resolver::col_name_eq(col.name(), name) {
                    return true; // Different name, keep
                }
                // Same name — if qualifier specified, only remove from matching table
                if let Some(qual) = qualifier {
                    !matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == qual)
                } else {
                    false // No qualifier, remove all with this name
                }
            });
        }
    }
    // If any spec used a qualifier, mark all output columns as qualified
    // so LAW1 keeps joins flat and the transformer can emit qualified refs.
    if any_qualified {
        for col in &mut output_columns {
            col.info = col.info.clone().with_updated_qualification(true);
        }
    }

    // Validate we're not removing all columns
    if output_columns.is_empty() {
        return Err(DelightQLError::parse_error(
            "Cannot remove all columns - would create empty table",
        ));
    }

    // Check if removal was pattern-based and matched nothing (no-op + warning)
    if removed_count == 0 && !available.is_empty() {
        // Pattern matched nothing - issue warning
        emit_validation_warning("ProjectOut pattern matched no columns - no changes made");
    }

    let resolved_op = ast_resolved::UnaryRelationalOperator::ProjectOut {
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        expressions: resolved_expressions,
    };

    Ok((resolved_op, output_columns))
}

/// Resolve the RenameCover operator
///
/// This renames columns based on pattern matching and templates.
/// Supports template expansion with {@} (column name) and {#} (position).
pub(super) fn resolve_rename_cover(
    specs: Vec<ast_unresolved::RenameSpec>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Build rename map by resolving patterns and expanding templates
    // Key is column position in available (to distinguish same-named columns from different tables)
    let mut rename_map: std::collections::HashMap<usize, (String, String)> =
        std::collections::HashMap::new();
    // Track which positions were identified via qualified references (for LAW1)
    let mut qualified_positions: std::collections::HashSet<usize> =
        std::collections::HashSet::new();

    for spec in specs {
        // Resolve the 'from' expression (may be pattern, glob, or column name)
        let resolved_columns =
            resolve_expressions_with_schema(vec![spec.from], available, None, None, None, false)?;

        // For each matched column, compute its new name
        for resolved_expr in &resolved_columns {
            if let ast_resolved::DomainExpression::Lvar {
                name, qualifier, ..
            } = resolved_expr
            {
                // Find the column metadata for template expansion, respecting qualifier
                let col_idx = if let Some(qual) = qualifier {
                    available.iter().position(|col| {
                        crate::pipeline::resolver::col_name_eq(col.name(), name)
                            && matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == qual)
                    })
                } else {
                    available
                        .iter()
                        .position(|col| crate::pipeline::resolver::col_name_eq(col.name(), name))
                };

                let col_idx = col_idx.ok_or_else(|| {
                    DelightQLError::column_not_found_error(
                        name.as_str(),
                        "in rename-cover operator",
                    )
                })?;
                let col_meta = &available[col_idx];

                // Compute new name based on RenameTarget
                let new_name = match &spec.to {
                    ast_unresolved::RenameTarget::Literal(lit) => lit.clone(),
                    ast_unresolved::RenameTarget::Template(alias) => {
                        // Expand template with column name and position
                        match alias {
                            ast_unresolved::ColumnAlias::Template(tpl) => expand_column_template(
                                &tpl.template,
                                name,
                                col_meta.table_position,
                            )?,
                            ast_unresolved::ColumnAlias::Literal(lit) => lit.clone(),
                        }
                    }
                };

                rename_map.insert(col_idx, (name.to_string(), new_name));
                if qualifier.is_some() {
                    qualified_positions.insert(col_idx);
                }
            }
        }
    }

    // Apply renames to output columns (using position-based map)
    let mut output_columns = Vec::new();
    for (idx, col) in available.iter().enumerate() {
        let mut output_col = col.clone();
        if let Some((_, new_name)) = rename_map.get(&idx) {
            output_col.info = output_col.info.with_alias(new_name.clone());
            // CRITICAL: When a column is renamed, it now has a user-provided name
            output_col.has_user_name = true;
        }
        // If this column was identified via a qualified reference, propagate
        // that qualification so LAW1 keeps joins flat in the transformer.
        if qualified_positions.contains(&idx) {
            output_col.info = output_col.info.with_updated_qualification(true);
        }
        output_columns.push(output_col);
    }

    // Create resolved specs for the AST (with literal names after expansion)
    let resolved_specs: Vec<ast_resolved::RenameSpec> = rename_map
        .into_iter()
        .map(|(_, (from_name, to_name))| ast_resolved::RenameSpec {
            from: ast_resolved::DomainExpression::Lvar {
                name: from_name.into(),
                qualifier: None,
                namespace_path: crate::pipeline::asts::resolved::NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            },
            to: ast_resolved::RenameTarget::Literal(to_name),
        })
        .collect();

    let resolved_op = ast_resolved::UnaryRelationalOperator::RenameCover {
        specs: resolved_specs,
    };

    Ok((resolved_op, output_columns))
}

/// Resolve the Reposition operator
///
/// This reorders columns to specific positions.
/// Supports both positive (1-based) and negative (from end) positioning.
/// Critical for CPR Law 0 - returns REORDERED columns.
pub(super) fn resolve_reposition(
    moves: Vec<ast_unresolved::RepositionSpec>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve each reposition spec and apply the reordering
    let mut resolved_moves = Vec::new();
    let num_columns = available.len();
    let mut result = vec![None; num_columns];
    let mut moved_indices = std::collections::HashSet::new();

    for spec in moves {
        // Resolve the column reference
        let resolved_column = resolve_expressions_with_schema(
            vec![spec.column.clone()],
            available,
            None,
            None,
            None,
            false,
        )?
        .into_iter()
        .next()
        .expect("resolve_expressions_with_schema returns same count as input");

        // Find which column this refers to
        let column_idx = match &spec.column {
            ast_unresolved::DomainExpression::Lvar { name, .. } => available
                .iter()
                .position(|col| crate::pipeline::resolver::col_name_eq(col.name(), name))
                .ok_or_else(|| {
                    crate::error::DelightQLError::column_not_found_error(
                        name.as_str(),
                        "in basic-cover operator",
                    )
                })?,
            ast_unresolved::DomainExpression::ColumnOrdinal(ordinal_box) => {
                // Handle column ordinals (|1|, |2|, |-1|, etc.)
                let ordinal = ordinal_box.get();
                let idx = if ordinal.reverse {
                    // Negative indexing from the end
                    let pos = (num_columns as i32) - (ordinal.position as i32);
                    if pos < 0 {
                        return Err(crate::error::DelightQLError::parse_error(format!(
                            "Column ordinal -{} out of range",
                            ordinal.position
                        )));
                    }
                    pos as usize
                } else {
                    // 1-based to 0-based
                    let pos = (ordinal.position as usize) - 1;
                    if pos >= num_columns {
                        return Err(crate::error::DelightQLError::parse_error(format!(
                            "Column ordinal {} out of range (max {})",
                            ordinal.position, num_columns
                        )));
                    }
                    pos
                };
                idx
            }
            _ => {
                return Err(crate::error::DelightQLError::parse_error(
                    "Reposition only supports column names and ordinals",
                ));
            }
        };

        // Check for duplicate column
        if moved_indices.contains(&column_idx) {
            return Err(crate::error::DelightQLError::parse_error(format!(
                "Column '{}' appears multiple times in reposition",
                available[column_idx].name()
            )));
        }

        // Normalize negative positions
        let mut target_pos = spec.position;
        if target_pos < 0 {
            target_pos = (num_columns as i32) + target_pos + 1;
        }

        // Validate position range
        if target_pos < 1 || target_pos > num_columns as i32 {
            return Err(crate::error::DelightQLError::parse_error(format!(
                "Position {} is out of range for {} columns",
                spec.position, num_columns
            )));
        }

        let target_idx = (target_pos - 1) as usize;

        // Check for duplicate target position
        if result[target_idx].is_some() {
            return Err(crate::error::DelightQLError::parse_error(format!(
                "Multiple columns cannot target position {}",
                target_pos
            )));
        }

        result[target_idx] = Some(available[column_idx].clone());
        moved_indices.insert(column_idx);

        resolved_moves.push(ast_resolved::RepositionSpec {
            column: resolved_column,
            position: spec.position,
        });
    }

    // Fill remaining positions with unmoved columns in order
    let mut remaining: Vec<ast_resolved::ColumnMetadata> = available
        .iter()
        .enumerate()
        .filter(|(idx, _)| !moved_indices.contains(idx))
        .map(|(_, col)| col.clone())
        .collect();

    for slot in result.iter_mut() {
        if slot.is_none() && !remaining.is_empty() {
            *slot = Some(remaining.remove(0));
        }
    }

    // Build the reordered output columns
    let output_columns: Vec<ast_resolved::ColumnMetadata> = result.into_iter().flatten().collect();

    let resolved_op = ast_resolved::UnaryRelationalOperator::Reposition {
        moves: resolved_moves,
    };

    // Return the REORDERED columns - this is critical for CPR Law 0!
    Ok((resolved_op, output_columns))
}

/// Resolve the Witness operator (+ or \+)
///
/// Witness reifies the input relation's existence as a 1-row, 1-column relation.
/// - `+` returns ExistsWitness: `met` = 1 if source has rows, 0 otherwise
/// - `\+` returns DoesNotExistWitness: `met` = 1 if source is empty, 0 otherwise
///
/// The output is always a single column named "met" with a single row.
pub(super) fn resolve_witness(
    exists: bool,
    _available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Helper to create a synthetic column for the witness relation
    fn make_witness_column(name: &str, position: usize) -> ast_resolved::ColumnMetadata {
        ast_resolved::ColumnMetadata::new(
            ast_resolved::ColumnProvenance::from_column(name.to_string()),
            ast_resolved::FqTable {
                parents_path: crate::pipeline::asts::unresolved::NamespacePath::empty(),
                name: ast_resolved::TableName::Fresh,
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(position),
        )
    }

    // Output: single column "met"
    let output_columns = vec![make_witness_column("met", 1)];

    let resolved_op = ast_resolved::UnaryRelationalOperator::Witness { exists };

    Ok((resolved_op, output_columns))
}

/// Resolve the MetaIze operator (^ or ^^)
///
/// MetaIze reifies the input relation's schema as queryable data.
/// - `^` returns basic schema: scope, column_name, ordinal
/// - `^^` returns detailed schema: scope, column_name, ordinal, data_type, nullable
///
/// The `scope` column shows which table owns each column:
/// - `users(*), products(*) ^` → scope is "users" or "products"
/// - `users(*) |> (*) ^` → scope is "_" (unqualified after projection)
///
/// This is compile-time schema synthesis - the output is a virtual relation
/// containing one row per column of the input relation.
pub(super) fn resolve_meta_ize(
    detailed: bool,
    _available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Helper to create a synthetic column for the meta relation
    fn make_meta_column(name: &str, position: usize) -> ast_resolved::ColumnMetadata {
        ast_resolved::ColumnMetadata::new(
            ast_resolved::ColumnProvenance::from_column(name.to_string()),
            ast_resolved::FqTable {
                parents_path: crate::pipeline::asts::unresolved::NamespacePath::empty(),
                name: ast_resolved::TableName::Fresh,
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(position),
        )
    }

    // Build output schema - these are the columns of the meta relation
    let output_columns = if detailed {
        // Detailed schema (??) includes: scope, column_name, ordinal, data_type, nullable
        vec![
            make_meta_column("scope", 1),
            make_meta_column("column_name", 2),
            make_meta_column("ordinal", 3),
            make_meta_column("data_type", 4),
            make_meta_column("nullable", 5),
        ]
    } else {
        // Basic schema (?) includes: scope, column_name, ordinal
        vec![
            make_meta_column("scope", 1),
            make_meta_column("column_name", 2),
            make_meta_column("ordinal", 3),
        ]
    };

    // The resolved operator preserves the detailed flag for emitting
    let resolved_op = ast_resolved::UnaryRelationalOperator::MetaIze { detailed };

    Ok((resolved_op, output_columns))
}

/// Resolve the Qualify operator: * - marks columns as qualified
///
/// This operator marks all columns as qualified (table-prefixed).
/// Qualified columns don't unify implicitly with same-named columns from other tables.
/// The output columns are identical to input, but with `was_qualified: true`.
pub(super) fn resolve_qualify(
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Mark all columns as qualified
    let output_columns: Vec<_> = available
        .iter()
        .map(|col| {
            let mut new_col = col.clone();
            new_col.info = new_col.info.with_updated_qualification(true);
            new_col
        })
        .collect();

    Ok((
        ast_resolved::UnaryRelationalOperator::Qualify,
        output_columns,
    ))
}

/// Resolve the Using operator: .(cols)
///
/// The Using operator performs USING semantics on the specified columns.
/// At resolution time, we just validate the columns exist and pass them through.
/// The actual USING join semantics are handled in the refiner at join-building time.
pub(super) fn resolve_using(
    columns: Vec<String>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Validate all USING columns exist in available schema
    for col_name in &columns {
        let exists = available.iter().any(|c| {
            c.info.name().map_or(false, |n| {
                crate::pipeline::resolver::col_name_eq(n, col_name)
            })
        });
        if !exists {
            let available_cols: Vec<_> = available.iter().filter_map(|c| c.info.name()).collect();
            return Err(crate::error::DelightQLError::column_not_found_error(
                col_name.clone(),
                format!("available columns: {}", available_cols.join(", ")),
            ));
        }
    }

    // Output schema is same as input - USING deduplication happens at join time
    Ok((
        ast_resolved::UnaryRelationalOperator::Using { columns },
        available.to_vec(),
    ))
}

/// Resolve the UsingAll operator: .*
///
/// At the pipe level, UsingAll is a pass-through — the actual shared-column
/// computation happens at join time in the resolver's join handler.
pub(super) fn resolve_using_all(
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    Ok((
        ast_resolved::UnaryRelationalOperator::UsingAll,
        available.to_vec(),
    ))
}

/// Resolve the DmlTerminal operator
///
/// DML terminals (delete!, update!, insert!, keep!) transform the upstream query
/// into a SQL DML statement. At resolution time, we resolve the DQL namespace
/// to the SQL schema name (via the same path as SELECT table resolution), then
/// pass through — the transformer handles actual DML statement generation.
pub(super) fn resolve_dml_terminal(
    kind: crate::pipeline::asts::core::operators::DmlKind,
    target: String,
    target_namespace: Option<String>,
    domain_spec: ast_unresolved::DomainSpec,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut crate::resolution::EntityRegistry,
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve DQL namespace → SQL schema (same path as SELECT table resolution)
    // Also track the connection_id so the execution engine routes DML to the correct database
    let resolved_namespace = if let Some(ref ns) = target_namespace {
        if let Some(system) = registry.database.system {
            let parts: Vec<String> = ns.split("::").map(|s| s.to_string()).collect();
            let ns_path = delightql_types::namespace::NamespacePath::from_parts(parts);
            match system.resolve_namespace_path(&ns_path) {
                Ok(Some((source_ns, connection_id))) => {
                    registry.track_connection_id(connection_id);
                    source_ns
                }
                _ => target_namespace,
            }
        } else {
            target_namespace
        }
    } else {
        target_namespace
    };

    // Convert DomainSpec from Unresolved to Resolved phase
    let resolved_domain_spec = match domain_spec {
        ast_unresolved::DomainSpec::Glob => ast_resolved::DomainSpec::Glob,
        ast_unresolved::DomainSpec::GlobWithUsing(cols) => {
            ast_resolved::DomainSpec::GlobWithUsing(cols)
        }
        ast_unresolved::DomainSpec::GlobWithUsingAll => {
            ast_resolved::DomainSpec::GlobWithUsingAll
        }
        ast_unresolved::DomainSpec::Positional(exprs) => {
            let resolved = super::super::domain_expressions::resolve_expressions_with_schema(
                exprs, available, None, None, None, false,
            )?;
            ast_resolved::DomainSpec::Positional(resolved)
        }
        ast_unresolved::DomainSpec::Bare => ast_resolved::DomainSpec::Bare,
    };

    Ok((
        ast_resolved::UnaryRelationalOperator::DmlTerminal {
            kind,
            target,
            target_namespace: resolved_namespace,
            domain_spec: resolved_domain_spec,
        },
        available.to_vec(),
    ))
}

/// Resolve the InteriorDrillDown operator
///
/// Explodes an interior relation (tree group) column into rows.
/// The column must exist in the input schema and have an interior_schema attached
/// (from tree group resolution or sys table lookup).
pub(super) fn resolve_interior_drill_down(
    column: String,
    glob: bool,
    columns: Vec<String>,
    _interior_schema: Option<Vec<crate::pipeline::asts::core::operators::InteriorColumnDef>>,
    unresolved_groundings: Vec<(String, String)>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    use crate::pipeline::asts::core::operators::InteriorColumnDef;

    // 1. Find the drilled column in the input schema
    let drilled_col =
        available
            .iter()
            .find(|col| crate::pipeline::resolver::col_name_eq(col.name(), &column))
            .ok_or_else(|| {
                crate::error::DelightQLError::validation_error(
                    format!(
                    "Interior drill-down: column '{}' not found in input relation. Available: {}",
                    column,
                    available.iter().map(|c| c.name()).collect::<Vec<_>>().join(", ")
                ),
                    "Check that the column name matches a tree group column in the input."
                        .to_string(),
                )
            })?;

    // 2. Get the interior schema from the column metadata
    let schema = drilled_col.interior_schema.clone().ok_or_else(|| {
        crate::error::DelightQLError::validation_error(
            format!(
                "Interior drill-down: column '{}' does not have a known interior schema. \
                 It may not be a tree group column, or its schema was not captured during resolution.",
                column
            ),
            "Use ~= destructuring for columns without a statically known interior schema.".to_string(),
        )
    })?;

    // 3. Build output columns
    let mut output_columns = Vec::new();

    // 3a. Add all input columns EXCEPT the drilled column (context carry-forward)
    for col in available {
        if !crate::pipeline::resolver::col_name_eq(col.name(), &column) {
            output_columns.push(col.clone());
        }
    }

    // 3b. Add interior columns from the schema
    //
    // Three modes:
    //   glob=true             → all interior columns, original names
    //   named (all match)     → selected interior columns by name
    //   positional (arity eq) → all interior columns, user-supplied aliases
    //                           ("_" entries → skip that column from output)
    let mut positional_aliases: Option<&Vec<String>> = None;
    let interior_cols: Vec<&InteriorColumnDef> = if glob {
        schema.iter().collect()
    } else {
        // Try name-based lookup first
        let mut selected = Vec::new();
        let mut all_matched = true;
        for col_name in &columns {
            if col_name == "_" {
                // Underscore can't be a named column — force positional path
                all_matched = false;
                break;
            }
            if let Some(def) = schema.iter().find(|d| d.name == *col_name) {
                selected.push(def);
            } else {
                all_matched = false;
                break;
            }
        }
        if all_matched {
            selected
        } else if columns.len() == schema.len() {
            // Positional binding: arity matches, names are fresh.
            // Select all interior columns; alias with user-supplied names.
            positional_aliases = Some(&columns);
            schema.iter().collect()
        } else {
            // Neither named nor positional — report the first bad name
            let bad = columns
                .iter()
                .find(|c| *c != "_" && !schema.iter().any(|d| d.name == **c))
                .unwrap_or(&columns[0]);
            return Err(crate::error::DelightQLError::validation_error(
                format!(
                    "Interior drill-down: column '{}' not found in interior schema of '{}'. \
                     Available: {}. (Positional binding requires exactly {} columns, got {}.)",
                    bad,
                    column,
                    schema.iter().map(|d| d.name.as_str()).collect::<Vec<_>>().join(", "),
                    schema.len(),
                    columns.len(),
                ),
                "Check column names against the tree group definition, or provide exactly the right number of names for positional binding.".to_string(),
            ));
        }
    };

    // Build resolved columns and output schema.
    // When positional aliases are present, "_" entries are skipped (not in output).
    let resolved_columns: Vec<String>;
    let mut resolved_col_list = Vec::new();
    for (idx, def) in interior_cols.iter().enumerate() {
        let output_name = if let Some(aliases) = positional_aliases {
            &aliases[idx]
        } else {
            &def.name
        };
        // Skip placeholder positions — they don't appear in output
        if output_name == "_" {
            continue;
        }
        resolved_col_list.push(def.name.clone());
        let mut col = ast_resolved::ColumnMetadata::new(
            ast_resolved::ColumnProvenance::from_column(output_name.clone()),
            ast_resolved::FqTable {
                parents_path: crate::pipeline::asts::unresolved::NamespacePath::empty(),
                name: ast_resolved::TableName::Named(column.clone().into()),
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(output_columns.len() + idx + 1),
        );
        // If this interior column has its own nested interior, mark it
        if def.child_interior.is_some() {
            col.interior_schema = def.child_interior.clone();
        }
        output_columns.push(col);
    }

    // For the resolved operator, pass only the non-skipped schema column names
    // so the transformer generates json_extract for the right subset.
    resolved_columns = if positional_aliases.is_some() {
        resolved_col_list
    } else {
        columns
    };

    // 4. Convert grounding positions to (schema_column_name, literal_value) pairs
    let resolved_groundings: Vec<(String, String)> = unresolved_groundings
        .iter()
        .filter_map(|(pos_str, value)| {
            let pos: usize = pos_str.parse().ok()?;
            schema.get(pos).map(|def| (def.name.clone(), value.clone()))
        })
        .collect();

    // 5. Build the resolved operator with the interior schema attached
    let resolved_op = ast_resolved::UnaryRelationalOperator::InteriorDrillDown {
        column: column.clone(),
        glob,
        columns: resolved_columns,
        interior_schema: Some(schema),
        groundings: resolved_groundings,
    };

    Ok((resolved_op, output_columns))
}

/// Resolve the NarrowingDestructure operator
///
/// Iterates a JSON array column via json_each, extracts named fields from
/// each element via json_extract. No context carry-forward — the output
/// schema contains only the named fields.
pub(super) fn resolve_narrowing_destructure(
    column: String,
    fields: Vec<String>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // 1. Verify column exists in input schema
    let _col = available
        .iter()
        .find(|col| crate::pipeline::resolver::col_name_eq(col.name(), &column))
        .ok_or_else(|| {
            crate::error::DelightQLError::validation_error(
                format!(
                    "Narrowing destructure: column '{}' not found in input relation. Available: {}",
                    column,
                    available
                        .iter()
                        .map(|c| c.name())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                "Check that the column name matches a column in the input.".to_string(),
            )
        })?;

    // 2. Build output columns — just the named fields (no context carry-forward)
    //    Output name is the last path segment: "name" → "name", "config.host" → "host"
    let mut output_columns = Vec::new();
    for (idx, field) in fields.iter().enumerate() {
        let output_name = field.rsplit('.').next().unwrap_or(field).to_string();
        let col = ast_resolved::ColumnMetadata::new(
            ast_resolved::ColumnProvenance::from_column(output_name),
            ast_resolved::FqTable {
                parents_path: crate::pipeline::asts::unresolved::NamespacePath::empty(),
                name: ast_resolved::TableName::Fresh,
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(idx + 1),
        );
        output_columns.push(col);
    }

    let resolved_op =
        ast_resolved::UnaryRelationalOperator::NarrowingDestructure { column, fields };

    Ok((resolved_op, output_columns))
}
