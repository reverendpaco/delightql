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
                            let kind = match gen_kind.to_lowercase().as_str() {
                                "stored" => GeneratedKind::Stored,
                                _ => GeneratedKind::Virtual,
                            };
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
