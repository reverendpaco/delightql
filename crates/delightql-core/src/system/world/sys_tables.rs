// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The curated `sys::*` relations registered while the canonical catalog is
//! constructed.

use crate::bootstrap::SourceType;
use crate::diagnostic::Runtime;
use crate::error::Result;
use rusqlite::{Connection, OptionalExtension};

/// Register the session's finding table as `sys::diagnostics.finding`.
/// Rows are written by [`DelightQLSystem::record_finding`](crate::system::DelightQLSystem::record_finding); the relation
/// is read-only from DQL like every bootstrap relation.
pub(super) fn register_sys_diagnostics_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://diagnostics', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to create sys::diagnostics cartridge: {}", e), e.to_string())
        })?;
    let cartridge_id = bootstrap_conn.last_insert_rowid() as i32;
    let ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::diagnostics'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::diagnostics namespace: {}", e),
                e.to_string(),
            )
        })?;
    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('finding', 10, ?1)",
            rusqlite::params![cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::diagnostics.finding entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;
    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- sys::diagnostics.finding: the session''s refusals and findings')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::diagnostics.finding clause: {}", e),
                e.to_string(),
            )
        })?;
    let columns: &[(&str, &str, i32, bool)] = &[
        ("id", "INTEGER", 1, false),
        ("occurred_at", "TEXT", 2, false),
        ("kind", "TEXT", 3, false),
        ("uri", "TEXT", 4, false),
        ("message", "TEXT", 5, false),
        ("input", "TEXT", 6, true),
        ("provider", "TEXT", 7, false),
    ];
    for (name, data_type, position, nullable) in columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, name, data_type, position, nullable],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to insert sys::diagnostics.finding column '{name}': {e}"),
                    e.to_string(),
                )
            })?;
    }
    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, ns_id, cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to activate sys::diagnostics.finding: {}", e), e.to_string())
        })?;
    Ok(())
}

/// Register the engine-owned identifier registry as
/// `sys::identifiers.identifier`. The burned rows live in
/// bootstrap/schema.sql; CLI-shaped facts belong to the host instead.
pub(super) fn register_sys_identifier_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://identifiers', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to create sys::identifiers cartridge: {}", e), e.to_string())
        })?;
    let identifiers_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let identifiers_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::identifiers'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::identifiers namespace: {}", e),
                e.to_string(),
            )
        })?;

    // (table/entity name, columns as (name, sqlite type, nullable))
    type Col = (&'static str, &'static str, bool);
    let tables: &[(&str, &[Col])] = &[(
        "identifier",
        &[
            ("kind", "TEXT", false),
            ("hierarchy", "TEXT", false),
            ("summary", "TEXT", false),
            ("explanation", "TEXT", false),
            ("role", "TEXT", false),
        ],
    )];

    for (table, columns) in tables {
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
                rusqlite::params![table, identifiers_cartridge_id],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to insert sys::identifiers.{} entity: {}", table, e),
                    e.to_string(),
                )
            })?;
        let entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- engine-owned identifier registry')",
                rusqlite::params![entity_id],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to insert sys::identifiers.{} clause: {}", table, e),
                    e.to_string(),
                )
            })?;

        for (position, (col_name, data_type, nullable)) in columns.iter().enumerate() {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![
                        entity_id,
                        col_name,
                        data_type,
                        (position + 1) as i32,
                        *nullable
                    ],
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!(
                            "Failed to insert sys::identifiers.{} column '{}': {}",
                            table, col_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        bootstrap_conn
            .execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![entity_id, identifiers_ns_id, identifiers_cartridge_id],
            )
            .map_err(|e| {
                Runtime::catalog(format!("Failed to activate sys::identifiers.{}: {}", table, e), e.to_string())
            })?;
    }

    Ok(())
}

/// Register the burned formatter style-bundle table as
/// `sys::format.bundle`. The physical table and its 'book' row live in
/// bootstrap/schema.sql; the column list here mirrors the formatter's
/// knob registry plus the leading `bundle` key. Its own cartridge so
/// bulk activation cannot leak it into bare `sys`.
pub(super) fn register_sys_format_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://format', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to create sys::format cartridge: {}", e), e.to_string())
        })?;
    let format_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let format_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::format'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::format namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('bundle', 10, ?1)",
            rusqlite::params![format_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::format.bundle entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- formatter style bundles (book row = frozen defaults)')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::format.bundle clause: {}", e),
                e.to_string(),
            )
        })?;

    let columns: &[(&str, &str, bool)] = &[
        ("bundle", "TEXT", false),
        ("projection_length", "INTEGER", true),
        ("continuation_length", "INTEGER", true),
        ("pipe_indent", "INTEGER", true),
        ("continuation_indent", "INTEGER", true),
        ("map_cover_extra_indent", "INTEGER", true),
        ("aggregation_arrow_indent", "INTEGER", true),
        ("cte_indent", "INTEGER", true),
        ("cte_columnar_padding", "INTEGER", true),
        ("curly_member_indent", "INTEGER", true),
        ("curly_inducer_indent", "INTEGER", true),
        ("case_arm_indent", "INTEGER", true),
        ("pipe_break_width", "INTEGER", true),
        ("member_landing_pad", "INTEGER", true),
        ("pipe_break", "TEXT", true),
        ("comma_clause_break", "TEXT", true),
        ("comma_join_args", "TEXT", true),
        ("brace_padding", "TEXT", true),
        ("member_landing", "TEXT", true),
        ("closer_placement", "TEXT", true),
        ("tree_inducer_break", "TEXT", true),
        ("member_value_break", "TEXT", true),
        ("annotation_placement", "TEXT", true),
        ("blank_lines", "TEXT", true),
        ("cte_style", "TEXT", true),
        ("curly_opening_brace_inline", "INTEGER", true),
    ];
    for (position, (col_name, data_type, nullable)) in columns.iter().enumerate() {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    col_name,
                    data_type,
                    (position + 1) as i32,
                    *nullable
                ],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!(
                        "Failed to insert sys::format.bundle column '{}': {}",
                        col_name, e
                    ),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, format_ns_id, format_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to activate sys::format.bundle: {}", e), e.to_string())
        })?;

    Ok(())
}

/// Register the CURATED `connection` entity in sys::connections.
///
/// Register the `connection` entity in sys::connections as an explicit column
/// ALLOWLIST. Under the credential-sourcing policy (credentials come from the
/// environment, never embedded in a URI) no column
/// of `connection` carries a secret: `resource_uri` is guaranteed
/// credential-free, and `identity` is a resource fingerprint (what the
/// resource asserts about itself, for idempotent-mount / conflict detection),
/// not a credential. So every column is exposed and answers "what am I
/// connected to?".
///
/// It stays a curated, explicitly-enumerated entity (not the raw introspected
/// twin) so the exposure is DEFAULT-DENY: a column added to the physical table
/// later is NOT surfaced unless deliberately added here — the structural belt
/// to the policy's suspenders. The resolver guard in registry.rs makes these
/// registered attributes authoritative for bootstrap (connection_id==1)
/// tables. The raw introspected `connection` entity (cartridge 1) is left
/// orphaned; the `catalog` diagnostic dedups by name, so this activation
/// clears its warning. Own cartridge so no bulk activation sweeps it into bare
/// `sys`.
pub(super) fn register_sys_connection_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://connections', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to create sys::connections cartridge: {}", e), e.to_string())
        })?;
    let conn_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let conn_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::connections'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::connections namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('connection', 10, ?1)",
            rusqlite::params![conn_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::connections.connection entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- sys::connections curated safe subset')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::connections.connection clause: {}", e),
                e.to_string(),
            )
        })?;

    // Explicit allowlist of all current columns (none is a secret under the
    // credential-sourcing policy). Enumerated, not raw, so a future column is
    // default-deny. Order + nullability mirror the physical `connection` table.
    let connection_columns = [
        ("id", "INTEGER", 1, false),
        ("resource_uri", "TEXT", 2, false),
        ("mechanism", "TEXT", 3, false),
        ("identity", "TEXT", 4, true),
        ("connection_type", "INTEGER", 5, false),
        ("description", "TEXT", 6, true),
    ];
    for (col_name, data_type, position, nullable) in &connection_columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, col_name, data_type, position, nullable],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!(
                        "Failed to insert sys::connections.connection column '{}': {}",
                        col_name, e
                    ),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, conn_ns_id, conn_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to activate sys::connections.connection: {}", e), e.to_string())
        })?;

    Ok(())
}

/// Register ONE curated sys::ns catalog relation (the sys::connections
/// precedent): an explicit column
/// ALLOWLIST entity over a physical bootstrap table, so the public shape is
/// deliberate and a column added to the physical table later is
/// default-deny. The raw introspected entity stays orphaned by design —
/// bootstrap tables are not bulk-activated — and the shared implementation
/// exists so every curated relation gets identical registration mechanics.
fn register_curated_sys_ns_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
    table_name: &str,
    clause_comment: &str,
    columns: &[(&str, &str, i32, bool)],
) -> Result<()> {
    // ONE sys://ns cartridge shared by every curated relation — created on
    // the first registration, reused after.
    let existing: Option<i32> = bootstrap_conn
        .query_row(
            "SELECT id FROM cartridge WHERE source_uri = 'sys://ns' AND connection_id = ?1",
            [bootstrap_conn_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::ns cartridge for '{table_name}': {e}"),
                e.to_string(),
            )
        })?;
    let ns_cartridge_id = match existing {
        Some(id) => id,
        None => {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, 'sys://ns', NULL, 1, ?3, 0)",
                    rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
                )
                .map_err(|e| {
                    Runtime::catalog(format!("Failed to create sys::ns cartridge for '{table_name}': {e}"), e.to_string())
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        }
    };

    let ns_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::ns'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to query sys::ns namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
            rusqlite::params![table_name, ns_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::ns.{table_name} entity: {e}"),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, ?2)",
            rusqlite::params![entity_id, clause_comment],
        )
        .map_err(|e| {
            Runtime::catalog(
                format!("Failed to insert sys::ns.{table_name} clause: {e}"),
                e.to_string(),
            )
        })?;

    for (col_name, data_type, position, nullable) in columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, col_name, data_type, position, nullable],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("Failed to insert sys::ns.{table_name} column '{col_name}': {e}"),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, ns_ns_id, ns_cartridge_id],
        )
        .map_err(|e| {
            Runtime::catalog(format!("Failed to activate sys::ns.{table_name}: {e}"), e.to_string())
        })?;

    Ok(())
}

/// Register the CURATED sys::ns relations: `namespace` (the ratified public
/// shape) and `mount` (mount identity, queryable deliberately). A
/// consulted namespace's provenance is its `source_path`; the catalog
/// keeps no load history to expose.
pub(super) fn register_sys_ns_namespace_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    // Exactly the columns namespace(*) has always shown. Mount identity is
    // deliberately absent HERE — it lives in its own curated relation below,
    // not as columns grafted onto namespace.
    register_curated_sys_ns_table(
        bootstrap_conn,
        bootstrap_conn_id,
        "namespace",
        "-- sys::ns curated public columns",
        &[
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, false),
            ("pid", "INTEGER", 3, true),
            ("fq_name", "TEXT", 4, true),
            ("default_data_ns", "TEXT", 5, true),
            ("kind", "TEXT", 6, false),
            ("provenance", "TEXT", 7, true),
            ("source_path", "TEXT", 8, true),
            ("writable", "INTEGER", 9, false),
        ],
    )?;
    register_curated_sys_ns_table(
        bootstrap_conn,
        bootstrap_conn_id,
        "mount",
        "-- sys::ns.mount curated public columns",
        &[
            ("namespace_id", "INTEGER", 1, false),
            ("cartridge_id", "INTEGER", 2, false),
            ("attach_alias", "TEXT", 3, true),
            ("attachment", "TEXT", 4, true),
            ("qualification", "TEXT", 5, false),
            ("engine_schema", "TEXT", 6, true),
            ("class", "TEXT", 7, false),
        ],
    )?;
    Ok(())
}
