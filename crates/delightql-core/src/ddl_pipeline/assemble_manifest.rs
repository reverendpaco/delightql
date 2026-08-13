// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::ddl::manifest::{ConstraintRow, DefaultRow, SchemaRow};
use crate::pipeline::asts::core::Unresolved;
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlDefault, GeneratedKind};
use super::builder;

fn db_err(msg: impl std::fmt::Display) -> crate::DelightQLError {
    crate::DelightQLError::transpilation_error(msg.to_string(), "ddl_pipeline::assemble_manifest")
}

/// Build a `CreateTableDef<Unresolved>` from manifest data.
///
/// Mirrors `assemble_create_table_def()` but reads parameters directly
/// instead of querying companion sys tables.
pub fn assemble_from_manifest(
    table_name: &str,
    temp: bool,
    schema_rows: &[SchemaRow],
    constraint_rows: &[ConstraintRow],
    default_rows: &[DefaultRow],
) -> Result<CreateTableDef<Unresolved>> {
    if schema_rows.is_empty() {
        return Err(db_err(format!(
            "No schema rows for '{}' — cannot assemble CREATE TABLE",
            table_name
        )));
    }

    // Every constraint row is consumed below either as a column constraint or
    // by the "_" table-level sentinel. Check the complete schema declaration
    // first so an unwitnessed row cannot disappear between those two paths.
    for cr in constraint_rows {
        if cr.column != "_" && !schema_rows.iter().any(|sr| sr.name == cr.column) {
            return Err(crate::DelightQLError::validation_error_categorized(
                "imprint/manifest/constraint_column",
                format!(
                    "constraint '{}' for '{}' names unknown column '{}'",
                    cr.constraint_name, table_name, cr.column
                ),
                format!(
                    "declare '{}' in schema(\"{}\") or use \"_\" for a table-level constraint",
                    cr.column, table_name
                ),
            ));
        }
    }

    let mut columns: Vec<ColumnDef<Unresolved>> = Vec::new();
    for sr in schema_rows {
        // Collect constraints for this column
        let mut constraints = Vec::new();
        for cr in constraint_rows {
            if cr.column == sr.name {
                constraints.push(builder::build_constraint(&cr.constraint)?);
            }
        }

        // Collect default for this column
        let default = default_rows
            .iter()
            .find(|dr| dr.column == sr.name)
            .map(|dr| -> Result<DdlDefault<Unresolved>> {
                if let Some(gen_kind) = &dr.generated {
                    let base = builder::build_default(&dr.default_val)?;
                    match base {
                        DdlDefault::Value { expr } => {
                            let kind = GeneratedKind::parse(gen_kind)?;
                            Ok(DdlDefault::Generated { expr, kind })
                        }
                        other => Ok(other),
                    }
                } else {
                    builder::build_default(&dr.default_val)
                }
            })
            .transpose()?;

        columns.push(ColumnDef {
            name: sr.name.clone(),
            col_type: sr.col_type.clone(),
            constraints,
            default,
        });
    }

    // Table-level constraints: constraints where column == "_"
    let mut table_constraints = Vec::new();
    for cr in constraint_rows {
        if cr.column == "_" {
            table_constraints.push(builder::build_constraint(&cr.constraint)?);
        }
    }

    Ok(CreateTableDef {
        name: table_name.to_string(),
        temp,
        columns,
        table_constraints,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_column_must_be_declared_by_the_schema() {
        let schema = [SchemaRow {
            name: "age".to_string(),
            col_type: "INTEGER".to_string(),
        }];
        let constraints = [ConstraintRow {
            column: "agee".to_string(),
            constraint: "@ > 0".to_string(),
            constraint_name: "ck_age_positive".to_string(),
        }];

        let error =
            assemble_from_manifest("measurements", true, &schema, &constraints, &[]).unwrap_err();

        assert_eq!(
            error.error_uri(),
            "delightql-error://imprint/manifest/constraint_column"
        );
        assert!(error.to_string().contains("ck_age_positive"));
        assert!(error.to_string().contains("agee"));
    }
}
