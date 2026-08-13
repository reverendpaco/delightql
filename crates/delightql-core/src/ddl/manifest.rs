// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Manifest reader — reads `_internal` HO entity data from bootstrap DB.
//!
//! `_internal` HO views (schema, constraints, defaults, imprinting) are consulted
//! entities stored in the bootstrap DB. Their clause bodies are anonymous table
//! facts — self-contained constants. This module extracts those facts by:
//!
//! 1. Finding entity + clause via bootstrap SQL (entity, entity_clause,
//!    ho_param, ho_param_ground_value)
//! 2. Extracting body text from `entity_clause.definition` (text after `:-`)
//! 3. Compiling body via `compile_source_to_sql(body, &EmptySchema)` → SQL
//! 4. Executing SQL on bootstrap connection → get rows

use rusqlite::{Connection, OptionalExtension};

use crate::error::{DelightQLError, Result};

/// How an imprinted entity is stored. Parsed at manifest-read from the
/// `imprinting()` `materialization` column; unknown spellings are rejected
/// loudly (`imprint/manifest/materialization`) instead of the old silent
/// fallback where `"veiw"` materialized a table. Pinned by
/// companion_linear--75 and `manifest::tests::materialization_rejects_typo`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Materialization {
    Table,
    View,
}

impl Materialization {
    /// Parse the manifest `materialization` string, rejecting unknown values.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "table" => Ok(Materialization::Table),
            "view" => Ok(Materialization::View),
            other => Err(DelightQLError::validation_error_categorized(
                "imprint/manifest/materialization",
                format!(
                    "imprinting() materialization '{}' is not recognized — \
                     valid values are \"table\" or \"view\"",
                    other
                ),
                "invalid materialization",
            )),
        }
    }
}

/// Whether an imprinted entity persists (`permanent`) or is a session-scoped
/// `temporary` object. Parsed at manifest-read; unknown spellings are rejected
/// loudly (`imprint/manifest/extent`) instead of the old silent fallback where
/// `"temp"` meant permanent. Pinned by companion_linear--76 and
/// `manifest::tests::extent_rejects_typo`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Extent {
    Permanent,
    Temporary,
}

impl Extent {
    /// Parse the manifest `extent` string, rejecting unknown values.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "permanent" => Ok(Extent::Permanent),
            "temporary" => Ok(Extent::Temporary),
            other => Err(DelightQLError::validation_error_categorized(
                "imprint/manifest/extent",
                format!(
                    "imprinting() extent '{}' is not recognized — \
                     valid values are \"permanent\" or \"temporary\"",
                    other
                ),
                "invalid extent",
            )),
        }
    }
}

/// Reject a manifest entity name that carries a `"`. The imprint DDL path
/// interpolates entity names into quoted identifiers; the declared-table
/// branch routes through the DDL generator (`ddl_pipeline::generator`, out of
/// this module), whose `write_quoted` does NOT double internal quotes, so an
/// embedded `"` would emit malformed/injected DDL there — `quote_ident` in the
/// imprint path cannot reach it. Rather than escape theater, we forbid the
/// character at the source (a triple-quoted DQL literal `"""a"b"""` is the only
/// way one reaches here). Pinned by
/// `manifest::tests::entity_name_rejects_embedded_quote`.
fn validate_entity_name(name: &str) -> Result<()> {
    if name.contains('"') {
        return Err(DelightQLError::validation_error_categorized(
            "imprint/manifest/entity_name",
            format!(
                "imprint entity name '{}' contains a '\"' — entity names may not \
                 contain double quotes",
                name
            ),
            "invalid entity name",
        ));
    }
    Ok(())
}

/// Row from `imprinting()`: (entity_name, materialization, extent)
pub struct ImprintingRow {
    pub entity: String,
    pub materialization: Materialization,
    pub extent: Extent,
}

/// Row from `schema()`: (column_name, column_type)
#[derive(Clone)]
pub struct SchemaRow {
    pub name: String,
    pub col_type: String,
}

/// Row from `constraints()`: (column_name, constraint_sigil, constraint_name)
pub struct ConstraintRow {
    pub column: String,
    pub constraint: String,
    pub constraint_name: String,
}

/// Row from `defaults()`: (column_name, default_value, generated_kind)
pub struct DefaultRow {
    pub column: String,
    pub default_val: String,
    pub generated: Option<String>,
}

/// Empty schema for compiling manifest bodies and imprint CTAS compilation.
/// These bodies are anonymous tables with no table references.
pub struct EmptySchema;

impl delightql_types::schema::DatabaseSchema for EmptySchema {
    fn get_table_columns(
        &self,
        _: Option<&str>,
        _: &str,
    ) -> delightql_types::Result<Option<Vec<delightql_types::schema::ColumnInfo>>> {
        Ok(None)
    }
    fn table_exists(&self, _: Option<&str>, _: &str) -> delightql_types::Result<bool> {
        Ok(false)
    }
}

/// Find the `_internal` child namespace ID for a given source namespace.
///
/// The `_internal` namespace is created by `(~~ddl:"_internal" ... ~~)` blocks
/// and has `fq_name = "{source_ns}::_internal"`.
pub fn find_internal_ns(conn: &Connection, source_ns: &str) -> Result<Option<i32>> {
    let internal_fq = format!("{}::_internal", source_ns);
    conn.query_row(
        "SELECT id FROM namespace WHERE fq_name = ?1",
        [&internal_fq],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to look up _internal namespace for '{}'", source_ns),
            e.to_string(),
        )
    })
}

/// Read `imprinting()` entity from `_internal` namespace.
///
/// Returns the list of (entity, materialization, extent) tuples.
/// Returns empty vec if `imprinting` entity doesn't exist.
pub fn read_imprinting(conn: &Connection, internal_ns_id: i32) -> Result<Vec<ImprintingRow>> {
    // imprinting is a regular (non-HO) entity — no ground value matching needed
    let clauses = read_entity_clauses(conn, internal_ns_id, "imprinting")?;
    if clauses.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for clause_def in &clauses {
        let body = crate::ddl::reconstruct::body_text(clause_def);
        let sql = compile_body(&body)?;
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to prepare imprinting SQL: {}", sql),
                e.to_string(),
            )
        })?;
        let result_rows = stmt
            .query_map([], |row| {
                let entity: String = row.get(0)?;
                let materialization: String = row.get(1)?;
                let extent: String = row.get(2)?;
                Ok((
                    strip_dql_quotes(&entity).to_string(),
                    strip_dql_quotes(&materialization).to_string(),
                    strip_dql_quotes(&extent).to_string(),
                ))
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to execute imprinting query", e.to_string())
            })?;
        // Parse enums / validate names OUTSIDE the rusqlite closure so the loud
        // manifest-validation errors (imprint/manifest/*) propagate as
        // DelightQLError, not swallowed into a rusqlite row error.
        for r in result_rows {
            let (entity, materialization, extent) = r.map_err(|e| {
                DelightQLError::database_error("Failed to read imprinting row", e.to_string())
            })?;
            validate_entity_name(&entity)?;
            rows.push(ImprintingRow {
                entity,
                materialization: Materialization::parse(&materialization)?,
                extent: Extent::parse(&extent)?,
            });
        }
    }

    Ok(rows)
}

/// Read `schema("entity_name")` from `_internal` namespace.
///
/// Uses HO clause matching: finds clauses where the first ground parameter
/// matches the given entity name.
pub fn read_schema(conn: &Connection, internal_ns_id: i32, entity: &str) -> Result<Vec<SchemaRow>> {
    let clauses = read_ho_clauses_by_ground_value(conn, internal_ns_id, "schema", entity)?;
    if clauses.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for clause_def in &clauses {
        let body = crate::ddl::reconstruct::body_text(clause_def);
        let sql = compile_body(&body)?;
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to prepare schema SQL: {}", sql),
                e.to_string(),
            )
        })?;
        let result_rows = stmt
            .query_map([], |row| {
                Ok(SchemaRow {
                    name: row.get(0)?,
                    col_type: row.get(1)?,
                })
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to execute schema query", e.to_string())
            })?;
        for r in result_rows {
            rows.push(r.map_err(|e| {
                DelightQLError::database_error("Failed to read schema row", e.to_string())
            })?);
        }
    }

    Ok(rows)
}

/// Read `constraints("entity_name")` from `_internal` namespace.
pub fn read_constraints(
    conn: &Connection,
    internal_ns_id: i32,
    entity: &str,
) -> Result<Vec<ConstraintRow>> {
    let clauses = read_ho_clauses_by_ground_value(conn, internal_ns_id, "constraints", entity)?;
    if clauses.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for clause_def in &clauses {
        let body = crate::ddl::reconstruct::body_text(clause_def);
        let sql = compile_body(&body)?;
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to prepare constraints SQL: {}", sql),
                e.to_string(),
            )
        })?;
        let result_rows = stmt
            .query_map([], |row| {
                Ok(ConstraintRow {
                    column: row.get(0)?,
                    constraint: row.get(1)?,
                    constraint_name: row.get(2)?,
                })
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to execute constraints query", e.to_string())
            })?;
        for r in result_rows {
            rows.push(r.map_err(|e| {
                DelightQLError::database_error("Failed to read constraint row", e.to_string())
            })?);
        }
    }

    Ok(rows)
}

/// Read `defaults("entity_name")` from `_internal` namespace.
///
/// Defaults may have 2 columns (column, default_val) or 3 columns
/// (column, default_val, generated). We detect the column count from the SQL.
pub fn read_defaults(
    conn: &Connection,
    internal_ns_id: i32,
    entity: &str,
) -> Result<Vec<DefaultRow>> {
    let clauses = read_ho_clauses_by_ground_value(conn, internal_ns_id, "defaults", entity)?;
    if clauses.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for clause_def in &clauses {
        let body = crate::ddl::reconstruct::body_text(clause_def);
        let sql = compile_body(&body)?;
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to prepare defaults SQL: {}", sql),
                e.to_string(),
            )
        })?;

        // Detect column count from the statement
        let col_count = stmt.column_count();

        let result_rows = stmt
            .query_map([], |row| {
                // default_val can be string or integer in the anonymous table
                let default_val: String = match row.get::<_, rusqlite::types::Value>(1)? {
                    rusqlite::types::Value::Text(s) => s,
                    rusqlite::types::Value::Integer(i) => i.to_string(),
                    rusqlite::types::Value::Real(f) => f.to_string(),
                    other => format!("{:?}", other),
                };
                Ok(DefaultRow {
                    column: row.get(0)?,
                    default_val,
                    generated: if col_count >= 3 { row.get(2)? } else { None },
                })
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to execute defaults query", e.to_string())
            })?;
        for r in result_rows {
            rows.push(r.map_err(|e| {
                DelightQLError::database_error("Failed to read default row", e.to_string())
            })?);
        }
    }

    Ok(rows)
}

/// Discover all entity names that have `schema()` entries in `_internal`.
///
/// Used as fallback when `imprinting()` is absent — we discover entities
/// from the ground values of `schema()` HO clauses.
pub fn discover_schema_entities(conn: &Connection, internal_ns_id: i32) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT hpgv.ground_value
             FROM ho_param_ground_value hpgv
             JOIN ho_param hp ON hpgv.ho_param_id = hp.id
             JOIN entity e ON hp.entity_id = e.id
             JOIN activated_entity ae ON ae.entity_id = e.id
             WHERE ae.namespace_id = ?1
               AND e.name = 'schema'
               AND hp.position = 0",
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to query schema entity names", e.to_string())
        })?;

    let rows = stmt
        .query_map([internal_ns_id], |row| row.get::<_, String>(0))
        .map_err(|e| {
            DelightQLError::database_error("Failed to execute schema entity query", e.to_string())
        })?;

    let names: Vec<String> = rows
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            DelightQLError::database_error("Failed to read schema entity names", e.to_string())
        })?;

    // Strip DQL string literal quotes from ground values, then validate: an
    // embedded `"` reaches the DDL generator unescaped (see validate_entity_name).
    names
        .into_iter()
        .map(|s| {
            let name = strip_dql_quotes(&s).to_string();
            validate_entity_name(&name)?;
            Ok(name)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read all entity_clause definitions for a non-HO entity in a namespace.
fn read_entity_clauses(
    conn: &Connection,
    namespace_id: i32,
    entity_name: &str,
) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(
            "SELECT ec.definition FROM entity_clause ec
             JOIN entity e ON ec.entity_id = e.id
             JOIN activated_entity ae ON ae.entity_id = e.id
             WHERE ae.namespace_id = ?1
               AND e.name = ?2
             ORDER BY ec.ordinal",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query entity clauses for '{}'", entity_name),
                e.to_string(),
            )
        })?;

    let rows = stmt
        .query_map(rusqlite::params![namespace_id, entity_name], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to execute clause query for '{}'", entity_name),
                e.to_string(),
            )
        })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to read clauses for '{}'", entity_name),
                e.to_string(),
            )
        })
}

/// Strip surrounding double quotes from a DQL string literal value.
/// `"products"` → `products`, `products` → `products` (no-op).
fn strip_dql_quotes(s: &str) -> &str {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Read entity_clause definitions for an HO entity, matched by ground value
/// on position 0.
///
/// The probe is an entity NAME, so it is looked up under the one stored
/// ground spelling a string value has — `LiteralValue::stored_ground` — and
/// nothing else. A second accepted spelling here would be a decoder the
/// encoder does not have.
fn read_ho_clauses_by_ground_value(
    conn: &Connection,
    namespace_id: i32,
    ho_entity_name: &str,
    ground_value: &str,
) -> Result<Vec<String>> {
    let stored =
        crate::pipeline::asts::core::LiteralValue::String(ground_value.to_string()).stored_ground();
    read_ho_clauses_by_ground_value_exact(conn, namespace_id, ho_entity_name, &stored)
}

fn read_ho_clauses_by_ground_value_exact(
    conn: &Connection,
    namespace_id: i32,
    ho_entity_name: &str,
    ground_value: &str,
) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(
            "SELECT ec.definition FROM entity_clause ec
             JOIN entity e ON ec.entity_id = e.id
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN ho_param hp ON hp.entity_id = e.id AND hp.position = 0
             JOIN ho_param_ground_value hpgv
               ON hpgv.ho_param_id = hp.id AND hpgv.clause_ordinal = ec.ordinal - 1
             WHERE ae.namespace_id = ?1
               AND e.name = ?2
               AND hpgv.ground_value = ?3
             ORDER BY ec.ordinal",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to query HO clauses for {}(\"{}\")",
                    ho_entity_name, ground_value
                ),
                e.to_string(),
            )
        })?;

    let rows = stmt
        .query_map(
            rusqlite::params![namespace_id, ho_entity_name, ground_value],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to execute HO clause query for {}(\"{}\")",
                    ho_entity_name, ground_value
                ),
                e.to_string(),
            )
        })?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to read HO clauses for {}(\"{}\")",
                    ho_entity_name, ground_value
                ),
                e.to_string(),
            )
        })
}

/// Compile an anonymous table body to SQL via the DQL pipeline.
fn compile_body(body: &str) -> Result<String> {
    crate::pipeline::compile_source_to_sql(body, &EmptySchema)
}

#[cfg(test)]
mod tests {
    //! Manifest-read validation. The imprinting()
    //! materialization/extent columns and entity names are validated the moment
    //! they leave the bootstrap DB, so a typo can never silently pick the wrong
    //! materialization/extent or inject an unescaped identifier downstream.
    use super::*;

    #[test]
    fn materialization_parses_known() {
        assert_eq!(
            Materialization::parse("table").unwrap(),
            Materialization::Table
        );
        assert_eq!(
            Materialization::parse("view").unwrap(),
            Materialization::View
        );
    }

    #[test]
    fn materialization_rejects_typo() {
        // A plain String comparison lets "veiw" fall through to a table.
        let err = Materialization::parse("veiw").unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://imprint/manifest/materialization"
        );
        assert!(err.to_string().contains("veiw"), "{}", err);
    }

    #[test]
    fn extent_parses_known() {
        assert_eq!(Extent::parse("permanent").unwrap(), Extent::Permanent);
        assert_eq!(Extent::parse("temporary").unwrap(), Extent::Temporary);
    }

    #[test]
    fn extent_rejects_typo() {
        // Comparing only against "temporary" makes "temp" mean permanent.
        let err = Extent::parse("temp").unwrap_err();
        assert_eq!(err.error_uri(), "delightql-error://imprint/manifest/extent");
        assert!(err.to_string().contains("temp"), "{}", err);
    }

    #[test]
    fn entity_name_accepts_plain() {
        assert!(validate_entity_name("seniors").is_ok());
    }

    #[test]
    fn entity_name_rejects_embedded_quote() {
        // Reachable via a triple-quoted DQL literal `"""a"b"""` → strip → a"b.
        let err = validate_entity_name("a\"b").unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://imprint/manifest/entity_name"
        );
    }
}
