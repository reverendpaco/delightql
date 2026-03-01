//! Companion table storage — compile companion bodies and INSERT into sys tables.
//!
//! Companion definitions (`entity(^)`, `entity(+)`, `entity($)`) define schema,
//! constraints, and defaults as DQL anonymous tables. This module:
//!
//! 1. Preprocesses the body (strip sigil prefixes, normalize `->` arrows)
//! 2. Compiles the body through the normal pipeline → SQL SELECT
//! 3. Wraps the SELECT in INSERT INTO the appropriate `companion_*` sys table
//! 4. Executes against the bootstrap connection

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::ddl::CompanionKind;
#[cfg(not(target_arch = "wasm32"))]
use rusqlite::Connection;

/// Preprocess companion body text so the DQL parser can handle it.
///
/// Two transformations:
/// - Strip sigil function prefixes: `c:"..."` → `"..."`, `d:"..."` → `"..."`
///   (sigil semantics are implicit from the companion kind, not needed in pass 1)
/// - Replace functional-dependency arrows `->` with commas so the anonymous table
///   parses as a standard N-column table instead of failing on the `->` token
pub fn preprocess_companion_body(source: &str) -> String {
    let mut result = source.to_string();
    // Strip sigil prefixes (c: and d: before string literals)
    result = result.replace("c:\"", "\"");
    result = result.replace("d:\"", "\"");
    // Replace functional-dependency arrows with commas
    result = result.replace("->", ",");
    result
}

/// Empty schema for compiling companion bodies.
/// Companion bodies in chunk 2 are anonymous tables with no table references.
struct EmptySchema;

impl delightql_types::schema::DatabaseSchema for EmptySchema {
    fn get_table_columns(
        &self,
        _: Option<&str>,
        _: &str,
    ) -> Option<Vec<delightql_types::schema::ColumnInfo>> {
        None
    }
    fn table_exists(&self, _: Option<&str>, _: &str) -> bool {
        false
    }
}

/// Empty schema for imprint CTAS compilation fallback.
/// Used when no real schema is available for the target connection.
pub struct EmptySchemaForImprint;

impl delightql_types::schema::DatabaseSchema for EmptySchemaForImprint {
    fn get_table_columns(
        &self,
        _: Option<&str>,
        _: &str,
    ) -> Option<Vec<delightql_types::schema::ColumnInfo>> {
        None
    }
    fn table_exists(&self, _: Option<&str>, _: &str) -> bool {
        false
    }
}

/// Extract body text from a full definition string.
///
/// "employees(^) :- _(name, type ---- ...)" → "_(name, type ---- ...)"
fn extract_body_text(full_source: &str) -> &str {
    if let Some(pos) = full_source.find(":-") {
        full_source[pos + 2..].trim()
    } else if let Some(pos) = full_source.find(":=") {
        full_source[pos + 2..].trim()
    } else {
        full_source.trim()
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Compile companion body and INSERT into the appropriate sys table.
///
/// The body is extracted from `full_source`, preprocessed to valid DQL,
/// compiled through the pipeline to SQL, wrapped in an INSERT, and executed.
pub fn store_companion_data(
    bootstrap_conn: &Connection,
    entity_id: i32,
    kind: CompanionKind,
    full_source: &str,
) -> Result<()> {
    let body_text = extract_body_text(full_source);
    let preprocessed = preprocess_companion_body(body_text);

    // Compile body through the pipeline
    let select_sql = crate::pipeline::compile_source_to_sql(&preprocessed, &EmptySchema)?;

    // Build INSERT wrapping the compiled SELECT
    let insert_sql = match kind {
        CompanionKind::Schema => format!(
            "INSERT INTO companion_schema (entity_id, column_position, column_name, column_type) \
             SELECT {entity_id}, ROW_NUMBER() OVER(), name, \"type\" FROM ({select_sql})"
        ),
        CompanionKind::Constraint => format!(
            "INSERT INTO companion_constraint (entity_id, column_name, constraint_text, constraint_name) \
             SELECT {entity_id}, \"column\", \"constraint\", constraint_name FROM ({select_sql})"
        ),
        CompanionKind::Default => {
            // Default bodies may have 2 or 3 columns.
            // CAST to TEXT: numeric defaults like `0` must be stored as text '0'.
            if select_sql.to_lowercase().contains("generated") {
                format!(
                    "INSERT INTO companion_default (entity_id, column_name, default_text, generated) \
                     SELECT {entity_id}, \"column\", CAST(\"default\" AS TEXT), generated FROM ({select_sql})"
                )
            } else {
                format!(
                    "INSERT INTO companion_default (entity_id, column_name, default_text, generated) \
                     SELECT {entity_id}, \"column\", CAST(\"default\" AS TEXT), NULL FROM ({select_sql})"
                )
            }
        }
    };

    bootstrap_conn.execute_batch(&insert_sql).map_err(|e| {
        DelightQLError::database_error_with_source(
            format!(
                "Failed to store companion {} data for entity {}: {}",
                companion_kind_name(kind),
                entity_id,
                insert_sql,
            ),
            e.to_string(),
            Box::new(e),
        )
    })?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn companion_kind_name(kind: CompanionKind) -> &'static str {
    match kind {
        CompanionKind::Schema => "schema",
        CompanionKind::Constraint => "constraint",
        CompanionKind::Default => "default",
    }
}
