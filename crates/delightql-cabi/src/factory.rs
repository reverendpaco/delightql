// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! SQLite-only ConnectionFactory for the C-ABI crate.
//!
//! Mirrors the CLI's CliConnectionFactory but depends only on
//! delightql-backends (not delightql-cli).

use std::sync::{Arc, Mutex};

use delightql_backends::sqlite::connection::SqliteConnectionManager;
use delightql_backends::sqlite::{DynamicSqliteSchema, SqliteConnection, SqliteIntrospector};
use delightql_core::api::{CreatedConnection, Handler};
use delightql_sqlite_relay::SqlParty;

pub struct CabiConnectionFactory;

impl delightql_core::api::ConnectionFactory for CabiConnectionFactory {
    fn create(
        &self,
        uri: &str,
    ) -> Result<CreatedConnection, Box<dyn std::error::Error + Send + Sync>> {
        let sqlite_conn = SqliteConnectionManager::new_file(uri)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        let arc = sqlite_conn.get_connection_arc();
        let handler: Box<dyn Handler + Send> = Box::new(SqlParty::new(arc.clone()));

        let introspector = Box::new(SqliteIntrospector::new(arc.clone()));

        let adapter = SqliteConnection::new(arc.clone());
        let connection: Arc<Mutex<dyn delightql_types::DatabaseConnection>> =
            Arc::new(Mutex::new(adapter));

        let handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync> = {
            let arc = arc.clone();
            Box::new(move || Box::new(SqlParty::new(arc.clone())) as Box<dyn Handler + Send>)
        };

        Ok(CreatedConnection {
            handler,
            handler_factory,
            connection,
            introspector,
            db_type: "sqlite".to_string(),
        })
    }
}

impl delightql_types::ConnectionFactory for CabiConnectionFactory {
    fn create(
        &self,
        uri: &str,
    ) -> Result<delightql_types::ConnectionComponents, Box<dyn std::error::Error + Send + Sync>>
    {
        let sqlite_conn = SqliteConnectionManager::new_file(uri)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        let arc = sqlite_conn.get_connection_arc();

        let schema = Box::new(DynamicSqliteSchema::new(arc.clone()));
        let introspector = Box::new(SqliteIntrospector::new(arc.clone()));
        let adapter = SqliteConnection::new(arc.clone());
        let connection: Arc<Mutex<dyn delightql_types::DatabaseConnection>> =
            Arc::new(Mutex::new(adapter));

        let identity = arc
            .lock()
            .ok()
            .and_then(|c| c.path().map(|p| p.to_string()))
            .filter(|p| !p.is_empty())
            .and_then(|p| std::fs::canonicalize(&p).ok())
            .map(|abs| format!("realpath:{}", abs.display()));
        Ok(delightql_types::ConnectionComponents {
            connection,
            schema,
            introspector,
            db_type: "sqlite".to_string(),
            mechanism: "in-process".to_string(),
            identity,
        })
    }
}
