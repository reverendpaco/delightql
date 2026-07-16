// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The CLI's embedded SQLite database images and their shared contract.
//!
//! The images are produced at build time (see `build.rs`), embedded via
//! `include_bytes!`, bound as `delightql-bytes://` names at
//! `connection::open_handle`, and mounted through the ordinary DQL
//! `mount!` path (BYTES-SCHEME-DESIGN.md). Byte-level validation happens
//! in the producers and again at bind time (core validates every bound
//! image in a scratch connection); the runtime check that remains is the
//! semantic one below, run through DQL against the mounted namespace.
//! No temp files: static images attach zero-copy from rodata, and the
//! runtime-built surface image attaches from SQLite-owned memory
//! (MOUNT-SPINE-PLAN.md Phase 3).

pub const BOOK_APPLICATION_ID: i64 = 0x4451_4c42; // DQLB
pub const MAN_APPLICATION_ID: i64 = 0x4451_4c4d; // DQLM
pub const SCHEMA_VERSION: i64 = 1;

pub const BOOK_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/book.sqlite"));
pub const MAN_BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/man.sqlite"));

/// Post-mount contract check (BYTES-SCHEME-DESIGN.md, review R3): the
/// mounted bundle's `bundle_meta.schema_version` must be the version this
/// binary's subcommand consumes. Runs through DQL against the mounted
/// namespace — the cheap, semantic runtime check that survives now that
/// byte-level validation lives in the producers and at bind time.
pub fn verify_bundle_schema_version(
    session: &mut dyn delightql_core::api::DqlSession,
    namespace: &str,
) -> anyhow::Result<()> {
    let res = crate::exec_ng::fetch_all(
        session,
        &format!("{namespace}.bundle_meta(*) |> (schema_version)"),
    )?;
    let found = res.rows.first().map(|r| r[0].clone());
    anyhow::ensure!(
        found.as_deref() == Some(SCHEMA_VERSION.to_string().as_str()),
        "embedded {namespace} bundle has schema version {}, expected {SCHEMA_VERSION}",
        found.unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read one integer pragma from an embedded image via a scratch
    /// connection (the same deserialize path bind-time validation uses).
    fn image_pragma(bytes: &[u8], pragma: &str) -> i64 {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.deserialize_read_exact("main", bytes, bytes.len(), true)
            .unwrap();
        conn.query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
            .unwrap()
    }

    /// The embedded images carry their producers' identity stamps: distinct
    /// application ids, and the schema version this binary consumes.
    #[test]
    fn embedded_databases_have_distinct_identities() {
        assert_eq!(image_pragma(BOOK_BYTES, "application_id"), BOOK_APPLICATION_ID);
        assert_eq!(image_pragma(MAN_BYTES, "application_id"), MAN_APPLICATION_ID);
        assert_eq!(image_pragma(BOOK_BYTES, "user_version"), SCHEMA_VERSION);
        assert_eq!(image_pragma(MAN_BYTES, "user_version"), SCHEMA_VERSION);
    }
}
