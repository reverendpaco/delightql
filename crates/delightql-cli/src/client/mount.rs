// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The client's private mount road: one live client connection into Core.
//!
//! [`ReplMountFactory`] is the client's types-level mount factory. It recognises exactly one private locator and answers it with
//! components backed by the connection [`super::database::ClientDatabase`]
//! retains; every other resource delegates to the ordinary
//! [`crate::connection_factory::CliConnectionFactory`]. The locator is a
//! capability understood only here — not a filesystem path, not a public
//! connection scheme, and not a resource any other host implements.
//!
//! Core's public API is untouched: the existing factory inversion already
//! lets a client supply a connection, and the handle learns nothing about
//! prompts, dot commands, or timeout policy.

use std::io::Write as _;
use std::sync::{Arc, Mutex};

use delightql_core::api::DqlHandle;
use delightql_types::DatabaseConnection;

use super::database::ClientDatabase;

/// The private session locator. Only the interactive client's mount factory
/// understands it.
pub const REPL_SESSION_LOCATOR: &str = "delightql-repl://session";

/// The one physical data mount. The public relations are thin projections
/// over it, so every REPL table stays on one connection and joins among the
/// public relations need no cross-connection plan.
pub const REPL_DATA_NAMESPACE: &str = "repl::data";

/// The fixed wrapper-definition program: for each public namespace, its one
/// projection body. Projections only — they may rename or arrange columns
/// but never keep a second copy of a row.
// `option` is stropped in BOTH positions: the admission law refuses a
// reserved word bare in every naming position, qualified or not.
const WRAPPER_DEFINITIONS: &[(&str, &str)] = &[
    (
        "repl::surface",
        "`dot_command`(*) :- repl::data.dot_command(*)\n",
    ),
    ("repl::config", "`option`(*) :- repl::data.`option`(*)\n"),
    ("repl::history", "`input`(*) :- repl::data.input(*)\n"),
    ("repl::errors", "incident(*) :- repl::data.incident(*)\n"),
    (
        "repl::context",
        "session(*) :- repl::data.session(*)\n\
         argument(*) :- repl::data.argument(*)\n\
         environment(*) :- repl::data.environment(*)\n",
    ),
];

/// The interactive client's types-level mount factory.
pub struct ReplMountFactory {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

impl delightql_types::ConnectionFactory for ReplMountFactory {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<
        delightql_types::ConnectionComponents,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        if uri != REPL_SESSION_LOCATOR {
            return delightql_types::ConnectionFactory::create(
                &crate::connection_factory::CliConnectionFactory,
                uri,
            );
        }
        let arc = Arc::clone(&self.connection);
        let schema = Box::new(delightql_backends::DynamicSqliteSchema::new(arc.clone()));
        let introspector = Box::new(delightql_backends::sqlite::SqliteIntrospector::new(
            arc.clone(),
        ));
        let adapter = delightql_backends::sqlite::SqliteConnection::new(arc);
        let connection: Arc<Mutex<dyn DatabaseConnection>> = Arc::new(Mutex::new(adapter));
        Ok(delightql_types::ConnectionComponents {
            schema,
            connection,
            introspector,
            db_type: "sqlite".to_string(),
            mechanism: "in-process".to_string(),
            identity: Some(REPL_SESSION_LOCATOR.to_string()),
            mounted_schema: None,
        })
    }

    fn create_tree(
        &self,
        uri: &str,
    ) -> std::result::Result<
        Vec<(String, delightql_types::ConnectionComponents)>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        if uri == REPL_SESSION_LOCATOR {
            return Err("the REPL session database has no schemas; mount! it directly".into());
        }
        delightql_types::ConnectionFactory::create_tree(
            &crate::connection_factory::CliConnectionFactory,
            uri,
        )
    }
}

/// Open a `DqlHandle` over the client database: the ordinary CLI handle —
/// same API factory, same embedded-image bindings, same `cli::surface`
/// attach — with the client mount factory in the types-level seat.
pub fn open_client_handle(db: &ClientDatabase) -> anyhow::Result<Box<dyn DqlHandle>> {
    let factory = Box::new(crate::connection_factory::CliConnectionFactory);
    let mount_factory = Box::new(ReplMountFactory {
        connection: db.connection_arc(),
    });
    let mut handle = delightql_core::api::open(factory, Some(mount_factory))
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    handle
        .bind_static_bytes("book", crate::embedded_db::BOOK_BYTES)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    handle
        .bind_static_bytes("man", crate::embedded_db::MAN_BYTES)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    handle
        .bind_static_bytes("editor", crate::embedded_db::EDITOR_BYTES)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    crate::cli_surface::attach(handle)
}

/// Mount the live database at `repl::data`, install the fixed wrapper
/// definitions, and verify one known public relation answers. Used when a
/// handle opens, in every mode, and again after a successful Core session
/// recovery — the catalog mapping dies with the session; the client-owned
/// connection does not.
pub fn install_repl_namespace(handle: &mut dyn DqlHandle) -> anyhow::Result<()> {
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    install_repl_namespace_with(&mut |dql| {
        crate::exec_ng::run_dql_query(dql, &mut *session).map(|r| r.rows.len())
    })
}

/// The same install over ONE executor of DQL text — the road a host takes
/// when it holds no session of its own. The server holds a protocol relay
/// (which borrows the handle for the connection's life) and reinstalls
/// through it after every reset the client sends: the catalog mapping
/// died with the reset, the client-owned connection did not. The executor
/// answers the row count of what it ran.
pub fn install_repl_namespace_with(
    run: &mut dyn FnMut(&str) -> anyhow::Result<usize>,
) -> anyhow::Result<()> {
    run(&format!(
        "mount!(\"{REPL_SESSION_LOCATOR}\", \"{REPL_DATA_NAMESPACE}\")(*)"
    ))?;

    // consult! is the one road that installs definitions into a named
    // namespace, and it reads a file; the program is fixed client text, so
    // it rides through a short-lived temp file that never carries session
    // data and is removed as soon as the consult returns.
    for (namespace, program) in WRAPPER_DEFINITIONS {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(program.as_bytes())?;
        file.flush()?;
        let path = file.path().display().to_string();
        run(&format!("consult!(\"{path}\", \"{namespace}\")(*)"))?;
    }

    // One known relation must answer before the namespace is called
    // restored: the session row exists in every build and mode (the
    // dot-command surface is empty without the REPL feature).
    let rows = run("repl::context.session(*)")?;
    anyhow::ensure!(
        rows == 1,
        "repl::context.session answered {rows} rows, not one"
    );
    Ok(())
}
