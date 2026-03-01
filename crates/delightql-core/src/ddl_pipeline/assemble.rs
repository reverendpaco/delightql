use rusqlite::Connection;

use crate::pipeline::asts::core::Unresolved;
use crate::Result;

use super::asts::{ColumnDef, CreateTableDef, DdlConstraint, DdlDefault, GeneratedKind};
use super::builder;

fn db_err(msg: impl std::fmt::Display) -> crate::DelightQLError {
    crate::DelightQLError::transpilation_error(msg.to_string(), "ddl_pipeline::assemble")
}

/// Read companion sys tables and assemble a `CreateTableDef<Unresolved>`.
///
/// Queries `companion_schema`, `companion_constraint`, and `companion_default`
/// for the given entity_id, then builds the DDL AST by parsing sigil strings
/// through the builder.
pub fn assemble_create_table_def(
    bootstrap_conn: &Connection,
    entity_id: i32,
    table_name: &str,
    temp: bool,
) -> Result<CreateTableDef<Unresolved>> {
    // 1. Query companion_schema → column list
    let mut schema_stmt = bootstrap_conn
        .prepare(
            "SELECT column_name, column_type FROM companion_schema \
             WHERE entity_id = ?1 ORDER BY column_position",
        )
        .map_err(|e| db_err(format!("companion_schema query: {e}")))?;

    let schema_rows: Vec<(String, String)> = schema_stmt
        .query_map([entity_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| db_err(format!("companion_schema fetch: {e}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| db_err(format!("companion_schema row: {e}")))?;

    // 2. Query companion_constraint → constraint list
    let mut constraint_stmt = bootstrap_conn
        .prepare(
            "SELECT column_name, constraint_text, constraint_name FROM companion_constraint \
             WHERE entity_id = ?1",
        )
        .map_err(|e| db_err(format!("companion_constraint query: {e}")))?;

    let constraint_rows: Vec<(Option<String>, String, String)> = constraint_stmt
        .query_map([entity_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|e| db_err(format!("companion_constraint fetch: {e}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| db_err(format!("companion_constraint row: {e}")))?;

    // 3. Query companion_default → default list
    let mut default_stmt = bootstrap_conn
        .prepare(
            "SELECT column_name, default_text, generated FROM companion_default \
             WHERE entity_id = ?1",
        )
        .map_err(|e| db_err(format!("companion_default query: {e}")))?;

    let default_rows: Vec<(String, String, Option<String>)> = default_stmt
        .query_map([entity_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|e| db_err(format!("companion_default fetch: {e}")))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| db_err(format!("companion_default row: {e}")))?;

    // 4. Build ColumnDefs
    let mut columns: Vec<ColumnDef<Unresolved>> = Vec::new();
    for (col_name, col_type) in &schema_rows {
        // Collect constraints for this column
        let mut constraints: Vec<DdlConstraint<Unresolved>> = Vec::new();
        for (cname, ctext, _cident) in &constraint_rows {
            if cname.as_deref() == Some(col_name.as_str()) {
                constraints.push(builder::build_constraint(ctext)?);
            }
        }

        // Collect default for this column
        let default = default_rows
            .iter()
            .find(|(dname, _, _)| dname == col_name)
            .map(|(_, dtext, generated)| -> Result<DdlDefault<Unresolved>> {
                if let Some(gen_kind) = generated {
                    let base = builder::build_default(dtext)?;
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
                    builder::build_default(dtext)
                }
            })
            .transpose()?;

        columns.push(ColumnDef {
            name: col_name.clone(),
            col_type: col_type.clone(),
            constraints,
            default,
        });
    }

    // 5. Table-level constraints: constraints where column_name IS NULL
    let mut table_constraints: Vec<DdlConstraint<Unresolved>> = Vec::new();
    for (cname, ctext, _cident) in &constraint_rows {
        if cname.is_none() {
            table_constraints.push(builder::build_constraint(ctext)?);
        }
    }

    Ok(CreateTableDef {
        name: table_name.to_string(),
        temp,
        columns,
        table_constraints,
    })
}
