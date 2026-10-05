// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What a host tells core, stated once at boot.
//!
//! A host states its settings as rows when it opens a handle. Core admits
//! them against the keys it declares, writes them into its catalog, and from
//! then on reads them from the catalog and nowhere else — not the process
//! environment, not the process's working directory. A key only the host can
//! answer is REQUIRED: silence is refused, and "none" is an answer.
//!
//! Two lifetimes share one table. Boot rows are written before the handle's
//! pristine image is frozen, so every reset restores them; session rows are
//! written into the running instance, so a reset or a new session drops
//! them.

use rusqlite::Connection;

use crate::diagnostic::{Configuration, Runtime};
use crate::error::{DelightQLError, Result};

/// One setting core declares.
pub(crate) struct Key {
    pub(crate) name: &'static str,
    /// Only the host can know the answer, so the host must state it.
    pub(crate) required: bool,
    /// The host may state that it has no value.
    pub(crate) nullable: bool,
    /// A session may set its own value over the boot value.
    pub(crate) session: bool,
    pub(crate) summary: &'static str,
    check: fn(&str) -> std::result::Result<(), String>,
}

/// The directory a relative file path is resolved against.
pub const BASE_DIRECTORY: &str = "base_directory";

/// The SQL dialect every compilation emits.
pub const DIALECT: &str = "dialect";

/// Every key core reads. A key not declared here is refused at boot.
pub(crate) const KEYS: &[Key] = &[
    Key {
        name: BASE_DIRECTORY,
        required: true,
        nullable: true,
        session: true,
        summary: "The directory a relative file path is resolved against. None: this host \
                  has no filesystem, and a relative path refuses.",
        check: absolute_directory,
    },
    // Not required: core has an answer of its own, the dialect of the
    // connection each query routes to. Not nullable: stating none would be a
    // second spelling of leaving the key out.
    Key {
        name: DIALECT,
        required: false,
        nullable: false,
        session: false,
        summary: "The SQL dialect every compilation emits, over the dialect of the \
                  connection a query routes to. Unstated: each query takes its \
                  connection's dialect.",
        check: known_dialect,
    },
];

fn absolute_directory(value: &str) -> std::result::Result<(), String> {
    if std::path::Path::new(value).is_absolute() {
        Ok(())
    } else {
        Err(format!("'{value}' is not an absolute path"))
    }
}

fn known_dialect(value: &str) -> std::result::Result<(), String> {
    match crate::pipeline::generator::SqlDialect::from_family_name(value) {
        Some(_) => Ok(()),
        None => Err(format!(
            "'{value}' is not a dialect (sqlite, postgres, mysql, sqlserver, duckdb)"
        )),
    }
}

fn declared(name: &str) -> Option<&'static Key> {
    KEYS.iter().find(|key| key.name == name)
}

/// What a host states at boot: one row per key, a value or an explicit none.
#[derive(Debug, Clone, Default)]
pub struct BootSettings {
    rows: Vec<(String, Option<String>)>,
}

impl BootSettings {
    pub fn new() -> Self {
        Self::default()
    }

    /// State `key`. `None` says the host has no value for it — an answer,
    /// unlike leaving the key out.
    pub fn state(mut self, key: &str, value: Option<&str>) -> Self {
        self.rows.push((key.to_string(), value.map(str::to_string)));
        self
    }

    /// Admit the rows against the declared keys, naming every problem in
    /// one refusal.
    pub(crate) fn admit(&self) -> Result<Admitted> {
        let mut problems = Vec::new();
        let mut admitted: Vec<(&'static str, Option<String>)> = Vec::new();
        for (name, value) in &self.rows {
            let Some(key) = declared(name) else {
                problems.push(format!("'{name}' is not a key core declares"));
                continue;
            };
            if admitted.iter().any(|(stated, _)| *stated == key.name) {
                problems.push(format!("'{name}' is stated twice"));
                continue;
            }
            match value {
                None if !key.nullable => {
                    problems.push(format!("'{name}' cannot be stated as none"));
                }
                Some(value) => {
                    if let Err(why) = (key.check)(value) {
                        problems.push(format!("'{name}': {why}"));
                    }
                }
                None => {}
            }
            admitted.push((key.name, value.clone()));
        }
        for key in KEYS.iter().filter(|key| key.required) {
            if !admitted.iter().any(|(stated, _)| *stated == key.name) {
                problems.push(format!(
                    "'{}' is required and was not stated{}",
                    key.name,
                    if key.nullable {
                        " (state it as none if this host has no value)"
                    } else {
                        ""
                    }
                ));
            }
        }
        if problems.is_empty() {
            Ok(Admitted { rows: admitted })
        } else {
            Err(Configuration::BootTable {
                problems: problems.join("; "),
            }
            .into())
        }
    }
}

/// Boot rows that satisfied the contract. Only admission makes one.
pub(crate) struct Admitted {
    rows: Vec<(&'static str, Option<String>)>,
}

impl Admitted {
    /// Write the boot rows into a catalog that has not yet been frozen.
    pub(crate) fn write_boot(&self, conn: &Connection) -> Result<()> {
        for (key, value) in &self.rows {
            conn.execute(
                "INSERT INTO setting (key, layer, value) VALUES (?1, 'boot', ?2)",
                rusqlite::params![key, value],
            )
            .map_err(|e| Runtime::catalog("Failed to write a boot setting", e.to_string()))?;
        }
        Ok(())
    }
}

/// Project the declared keys into `setting_key`, part of the canonical
/// catalog every handle's image starts from.
pub(crate) fn project_keys(conn: &Connection) -> Result<()> {
    for key in KEYS {
        conn.execute(
            "INSERT INTO setting_key (key, required, nullable, session, summary)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                key.name,
                key.required,
                key.nullable,
                key.session,
                key.summary
            ],
        )
        .map_err(|e| Runtime::catalog("Failed to project a setting key", e.to_string()))?;
    }
    Ok(())
}

/// The value in force for `key`: the session's if it set one, else the
/// boot value. `None` means no row, which admission makes impossible for a
/// required key; `Some(None)` is a key stated as none.
pub(crate) fn in_force(conn: &Connection, key: &str) -> Result<Option<Option<String>>> {
    let mut statement = conn
        .prepare(
            "SELECT value FROM setting WHERE key = ?1
             ORDER BY CASE layer WHEN 'session' THEN 0 ELSE 1 END LIMIT 1",
        )
        .map_err(|e| Runtime::catalog("Failed to read a setting", e.to_string()))?;
    let mut rows = statement
        .query(rusqlite::params![key])
        .map_err(|e| Runtime::catalog("Failed to read a setting", e.to_string()))?;
    match rows
        .next()
        .map_err(|e| Runtime::catalog("Failed to read a setting", e.to_string()))?
    {
        Some(row) => Ok(Some(row.get::<_, Option<String>>(0).map_err(|e| {
            Runtime::catalog("Failed to read a setting", e.to_string())
        })?)),
        None => Ok(None),
    }
}

/// The dialect the host stated, if it stated one.
pub(crate) fn stated_dialect(
    conn: &Connection,
) -> Result<Option<crate::pipeline::generator::SqlDialect>> {
    match in_force(conn, DIALECT)? {
        None => Ok(None),
        Some(Some(name)) => crate::pipeline::generator::SqlDialect::from_family_name(&name)
            .map(Some)
            .ok_or_else(|| {
                crate::diagnostic::Internal::invariant(
                    "settings",
                    "a dialect row that admission would have refused",
                )
            }),
        Some(None) => Err(crate::diagnostic::Internal::invariant(
            "settings",
            "a dialect stated as none: the key is not nullable",
        )),
    }
}

/// Set `key` for the running session, over its boot value.
pub(crate) fn set_session(conn: &Connection, key: &str, value: Option<&str>) -> Result<()> {
    let Some(declared) = declared(key) else {
        return Err(Configuration::BootTable {
            problems: format!("'{key}' is not a key core declares"),
        }
        .into());
    };
    if !declared.session {
        return Err(Configuration::BootTable {
            problems: format!("'{key}' is set at boot only; a session cannot set it"),
        }
        .into());
    }
    let refusal = match value {
        None if !declared.nullable => Some(format!("'{key}' cannot be set to none")),
        Some(value) => (declared.check)(value)
            .err()
            .map(|why| format!("'{key}': {why}")),
        None => None,
    };
    if let Some(problems) = refusal {
        return Err(Configuration::BootTable { problems }.into());
    }
    conn.execute(
        "INSERT INTO setting (key, layer, value) VALUES (?1, 'session', ?2)
         ON CONFLICT(key, layer) DO UPDATE SET value = excluded.value",
        rusqlite::params![key, value],
    )
    .map_err(|e| Runtime::catalog("Failed to write a session setting", e.to_string()))?;
    Ok(())
}

/// Drop every session row: a new session starts from the boot values.
pub(crate) fn clear_session(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM setting WHERE layer = 'session'", [])
        .map_err(|e| Runtime::catalog("Failed to clear session settings", e.to_string()))?;
    Ok(())
}

/// Resolve `raw` against the base directory in force. An absolute path is
/// itself. A relative one joins the base; with a base of none it refuses,
/// and there is no fallback to the process's own directory.
pub(crate) fn resolve_path(conn: &Connection, raw: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(raw);
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    match in_force(conn, BASE_DIRECTORY)? {
        Some(Some(base)) => Ok(std::path::Path::new(&base).join(path)),
        Some(None) => Err(DelightQLError::from(Runtime::Io {
            message: format!(
                "'{raw}' is a relative path, and this host stated no base directory \
                 to resolve it against; use an absolute path"
            ),
        })),
        None => Err(crate::diagnostic::Internal::invariant(
            "settings",
            "no base_directory row: admission requires the host to state it",
        )),
    }
}

/// Publish `setting` and `setting_key` as `sys::config.setting` and
/// `sys::config.setting_key`, in their own cartridge so the bulk `sys`
/// activation cannot leak them into bare `sys`.
pub(crate) fn register_sys_config_tables(conn: &Connection, bootstrap_conn_id: i64) -> Result<()> {
    let fail = |what: &str, e: rusqlite::Error| {
        Runtime::catalog(format!("sys::config: {what}"), e.to_string())
    };
    conn.execute(
        "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
         VALUES (?1, ?2, 'sys://config', NULL, 1, ?3, 0)",
        rusqlite::params![3, crate::bootstrap::SourceType::Db.as_i32(), bootstrap_conn_id],
    )
    .map_err(|e| fail("create the cartridge", e))?;
    let cartridge_id = conn.last_insert_rowid() as i32;
    let namespace_id: i32 = conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::config'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| fail("find the namespace", e))?;

    let tables: &[(&str, &str, &[(&str, &str, bool)])] = &[
        (
            "setting",
            "-- what the host stated at boot and what the session set since",
            &[
                ("key", "TEXT", false),
                ("layer", "TEXT", false),
                ("value", "TEXT", true),
            ],
        ),
        (
            "setting_key",
            "-- the keys core declares",
            &[
                ("key", "TEXT", false),
                ("required", "INTEGER", false),
                ("nullable", "INTEGER", false),
                ("session", "INTEGER", false),
                ("summary", "TEXT", false),
            ],
        ),
    ];
    for (name, clause, columns) in tables {
        conn.execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
            rusqlite::params![name, cartridge_id],
        )
        .map_err(|e| fail("insert an entity", e))?;
        let entity_id = conn.last_insert_rowid() as i32;
        conn.execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, ?2)",
            rusqlite::params![entity_id, clause],
        )
        .map_err(|e| fail("insert an entity clause", e))?;
        for (position, (column, data_type, nullable)) in columns.iter().enumerate() {
            conn.execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    column,
                    data_type,
                    (position + 1) as i32,
                    *nullable
                ],
            )
            .map_err(|e| fail("insert a column", e))?;
        }
        conn.execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, namespace_id, cartridge_id],
        )
        .map_err(|e| fail("activate an entity", e))?;
    }
    project_keys(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_is_refused_and_none_is_an_answer() {
        let refused = BootSettings::new()
            .admit()
            .err()
            .expect("a missing key refuses");
        assert!(
            refused.to_string().contains("'base_directory' is required"),
            "{refused}"
        );
        assert!(BootSettings::new()
            .state(BASE_DIRECTORY, None)
            .admit()
            .is_ok());
    }

    #[test]
    fn every_problem_is_named_in_one_refusal() {
        let refused = BootSettings::new()
            .state("colour", Some("blue"))
            .state(BASE_DIRECTORY, Some("relative/dir"))
            .state(DIALECT, Some("oracle"))
            .admit()
            .err()
            .expect("refuses");
        let text = refused.to_string();
        assert!(
            text.contains("'colour' is not a key core declares"),
            "{text}"
        );
        assert!(text.contains("is not an absolute path"), "{text}");
        assert!(text.contains("'oracle' is not a dialect"), "{text}");
    }

    #[test]
    fn the_dialect_may_go_unstated_but_not_stated_as_none() {
        assert!(BootSettings::new()
            .state(BASE_DIRECTORY, None)
            .admit()
            .is_ok());
        let refused = BootSettings::new()
            .state(BASE_DIRECTORY, None)
            .state(DIALECT, None)
            .admit()
            .err()
            .expect("none refuses");
        assert!(
            refused
                .to_string()
                .contains("'dialect' cannot be stated as none"),
            "{refused}"
        );
    }
}
