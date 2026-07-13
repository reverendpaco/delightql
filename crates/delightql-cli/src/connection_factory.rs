// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CLI implementation of ConnectionFactory
//!
//! Bridges the ConnectionFactory trait with the CLI's ConnectionManager,
//! which knows how to create connections for all supported backends
//! (SQLite, pipe://, fatboy://).

use delightql_core::api::{CreatedConnection, Handler};
use delightql_sqlite_relay::siso::SisoParty;
use delightql_sqlite_relay::SqlParty;

use crate::connection::ConnectionManager;

/// CLI connection factory.
///
/// Implements the API-level factory (returns CreatedConnection) for open().
pub struct CliConnectionFactory;

impl delightql_core::api::ConnectionFactory for CliConnectionFactory {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<CreatedConnection, Box<dyn std::error::Error + Send + Sync>> {
        // A `#schema` fragment is a client-side locator; strip it so the
        // engine only ever sees the base resource (Phase B). This API-level
        // door does not carry a schema (CreatedConnection has no such field);
        // schema threading rides the types-level `create` used by mount!.
        let (base, _schema) = crate::connection::split_schema_fragment(uri);
        let uri = base.as_str();
        let conn_mgr = ConnectionManager::new_file(uri)?;
        let handler = make_handler(&conn_mgr)?;
        let connection = conn_mgr.get_database_connection();
        let (introspector, db_type) = make_introspector_and_type(&conn_mgr)?;

        // Create a handler_factory closure that wraps the SAME underlying connection.
        // After mount! does ATTACH, new handlers from this factory see attached databases.
        let handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync> =
            match &conn_mgr {
                ConnectionManager::SQLite(sqlite_conn) => {
                    let arc = sqlite_conn.get_connection_arc();
                    Box::new(move || {
                        Box::new(SqlParty::new(arc.clone())) as Box<dyn Handler + Send>
                    })
                }
                ConnectionManager::Pipe(_) => {
                    let db_conn = conn_mgr.get_database_connection();
                    Box::new(move || {
                        Box::new(SisoParty::new(db_conn.clone())) as Box<dyn Handler + Send>
                    })
                }
                ConnectionManager::Fatboy(mgr) => {
                    // Fresh handler = fresh socket = fresh foreign-engine
                    // session (mirrors the protocol's connection scoping).
                    let mgr = mgr.clone();
                    Box::new(move || mgr.new_remote_handler())
                }
            };

        Ok(CreatedConnection {
            handler,
            handler_factory,
            connection,
            introspector,
            db_type,
        })
    }
}

/// Also implement types-level ConnectionFactory (used by system.rs import!/mount!).
impl delightql_types::ConnectionFactory for CliConnectionFactory {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<
        delightql_types::ConnectionComponents,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Phase B: strip the client-side `#schema` fragment before the base
        // reaches the engine (libpq / the DuckDB adapter never see it).
        let (base, schema) = crate::connection::split_schema_fragment(uri);
        let conn_mgr = ConnectionManager::new_file(&base)?;

        let connection = conn_mgr.get_database_connection();
        let mut components = conn_mgr.create_system_components(schema)?;
        components.connection = connection;

        Ok(components)
    }

    /// `mount_tree!`'s enumeration half (Phase C): open ONE connection and
    /// return one components per persistent schema, all sharing that
    /// connection (R-S1). Delegates to the fatboy path; SQLite/siso refuse.
    fn create_tree(
        &self,
        uri: &str,
    ) -> std::result::Result<
        Vec<(String, delightql_types::ConnectionComponents)>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // A fragment is meaningless here — mount_tree! mounts EVERY schema.
        let (base, schema) = crate::connection::split_schema_fragment(uri);
        if schema.is_some() {
            return Err("mount_tree! mounts every schema; drop the #schema fragment \
                        (use mount! to bind a single schema)"
                .into());
        }
        let conn_mgr = ConnectionManager::new_file(&base)?;
        match &conn_mgr {
            ConnectionManager::Fatboy(mgr) => {
                Ok(crate::fatboy_exec::create_fatboy_tree_components(mgr)?)
            }
            ConnectionManager::SQLite(_) => {
                Err("SQLite has no schemas; use mount! (mount_tree! is for \
                     Postgres and DuckDB targets)"
                    .into())
            }
            ConnectionManager::Pipe(_) => Err(
                "mount_tree! is not supported over a siso pipe; mount the \
                 Postgres/DuckDB resource directly"
                    .into(),
            ),
        }
    }
}

/// Create a `Box<dyn Handler>` from a ConnectionManager.
///
/// For SQLite: uses SqlParty (streaming cursors).
/// For Pipe: uses SisoParty (eager, buffered).
pub fn make_handler(
    conn_mgr: &ConnectionManager,
) -> Result<Box<dyn Handler + Send>, Box<dyn std::error::Error + Send + Sync>> {
    match conn_mgr {
        ConnectionManager::Pipe(_) => {
            let db_conn = conn_mgr.get_database_connection();
            Ok(Box::new(SisoParty::new(db_conn)))
        }
        // The engine's own protocol terms forward verbatim to the fatboy —
        // the relay's backend-facing side (FATBOY plan, step 4).
        ConnectionManager::Fatboy(mgr) => Ok(mgr.new_remote_handler()),
        ConnectionManager::SQLite(sqlite_conn) => {
            let arc = sqlite_conn.get_connection_arc();
            Ok(Box::new(SqlParty::new(arc)))
        }
    }
}

/// Create an introspector and db_type string from a ConnectionManager.
fn make_introspector_and_type(
    conn_mgr: &ConnectionManager,
) -> Result<
    (
        Box<dyn delightql_types::introspect::DatabaseIntrospector>,
        String,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    match conn_mgr {
        ConnectionManager::SQLite(sqlite_conn) => {
            let raw_conn_arc = sqlite_conn.get_connection_arc();
            let introspector = Box::new(delightql_backends::sqlite::SqliteIntrospector::new(
                raw_conn_arc,
            ));
            Ok((introspector, "sqlite".to_string()))
        }
        ConnectionManager::Pipe(mgr) => {
            let introspector = crate::pipe_exec::create_pipe_introspector(mgr)?;
            Ok((introspector, mgr.profile_name().to_string()))
        }
        ConnectionManager::Fatboy(mgr) => {
            let introspector =
                Box::new(crate::fatboy_exec::FatboyIntrospector::new(mgr.clone()));
            Ok((introspector, mgr.profile.clone()))
        }
    }
}
