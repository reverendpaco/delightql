// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::ddl::manifest_contract::{ConstraintRow, DefaultRow, SchemaRow};
use crate::diagnostic::{Constraint, DelightQLError, Manifest};
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef};
use super::builder;

fn db_err(msg: impl std::fmt::Display) -> crate::DelightQLError {
    DelightQLError::from(Constraint::General {
        message: msg.to_string(),
    })
}

/// Build a `CreateTableDef` from manifest data.
///
/// Mirrors `assemble_create_table_def()` but reads parameters directly
/// instead of querying companion sys tables.
pub fn assemble_from_manifest(
    table_name: &str,
    temp: bool,
    schema_rows: &[SchemaRow],
    constraint_rows: &[ConstraintRow],
    default_rows: &[DefaultRow],
) -> Result<CreateTableDef> {
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
            return Err(DelightQLError::from(Manifest::ConstraintColumn {
                message: format!(
                    "constraint '{}' for '{}' names unknown column '{}'",
                    cr.constraint_name, table_name, cr.column
                ),
            }));
        }
    }

    let mut columns: Vec<ColumnDef> = Vec::new();
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
            .map(|dr| builder::build_default(&dr.default_val))
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
            ordinal: crate::ddl::manifest_contract::OrdinalCell::Integer(1),
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
