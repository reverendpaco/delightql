//! CLI implementation of ConnectionFactory
//!
//! Bridges the ConnectionFactory trait with the CLI's ConnectionManager,
//! which knows how to create connections for all supported backends
//! (SQLite, DuckDB, pipe://).

use delightql_core::api::{CreatedConnection, Handler};
use delightql_sql_adapter::siso::SisoParty;
use delightql_sql_adapter::SqlParty;

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
                #[cfg(feature = "duckdb")]
                ConnectionManager::DuckDB(_) => {
                    let db_conn = conn_mgr.get_database_connection();
                    Box::new(move || {
                        Box::new(SisoParty::new(db_conn.clone())) as Box<dyn Handler + Send>
                    })
                }
                ConnectionManager::Pipe(_) => {
                    let db_conn = conn_mgr.get_database_connection();
                    Box::new(move || {
                        Box::new(SisoParty::new(db_conn.clone())) as Box<dyn Handler + Send>
                    })
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
        let conn_mgr = ConnectionManager::new_file(uri)?;

        let connection = conn_mgr.get_database_connection();
        let mut components = conn_mgr.create_system_components()?;
        components.connection = connection;

        Ok(components)
    }
}

/// Create a `Box<dyn Handler>` from a ConnectionManager.
///
/// For SQLite: uses SqlParty (streaming cursors).
/// For Pipe: uses SisoParty (eager, buffered).
/// For DuckDB: uses SisoParty.
pub fn make_handler(
    conn_mgr: &ConnectionManager,
) -> Result<Box<dyn Handler + Send>, Box<dyn std::error::Error + Send + Sync>> {
    match conn_mgr {
        ConnectionManager::Pipe(_) => {
            let db_conn = conn_mgr.get_database_connection();
            Ok(Box::new(SisoParty::new(db_conn)))
        }
        #[cfg(feature = "duckdb")]
        ConnectionManager::DuckDB(_) => {
            let db_conn = conn_mgr.get_database_connection();
            Ok(Box::new(SisoParty::new(db_conn)))
        }
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
        #[cfg(feature = "duckdb")]
        ConnectionManager::DuckDB(duckdb_conn) => {
            let duckdb_arc = duckdb_conn.get_connection_arc();
            let introspector = Box::new(delightql_backends::duckdb::DuckDBIntrospector::new(
                duckdb_arc,
            ));
            Ok((introspector, "duckdb".to_string()))
        }
        ConnectionManager::Pipe(mgr) => {
            let introspector = crate::pipe_exec::create_pipe_introspector(mgr)?;
            Ok((introspector, mgr.profile_name().to_string()))
        }
    }
}
