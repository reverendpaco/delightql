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

/// The fixed wrapper-definition programs: for each public namespace, its
/// projection bodies. Projections only — they may rename or arrange columns
/// but never keep a second copy of a row.
const WRAPPER_DEFINITIONS: &[(&str, &str)] = &[
    (
        "repl::surface",
        include_str!("../../autoload/repl/surface.dql"),
    ),
    (
        "repl::config",
        include_str!("../../autoload/repl/config.dql"),
    ),
    (
        "repl::history",
        include_str!("../../autoload/repl/history.dql"),
    ),
    (
        "repl::errors",
        include_str!("../../autoload/repl/errors.dql"),
    ),
    (
        "repl::context",
        include_str!("../../autoload/repl/context.dql"),
    ),
];

/// The client's utility namespace: rules for the person at the prompt,
/// enlisted with the session so they answer bare there and nowhere else.
pub const REPL_UTIL_NAMESPACE: &str = "repl::util";

const REPL_UTIL_PROGRAM: &str = include_str!("../../autoload/repl/util.dql");

/// The client profile's types-level mount factory. Only a handle opened
/// under `SessionProfile::Client` over a client database receives one; a
/// server handle's factory knows no session locator.
pub struct ReplMountFactory {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

impl ReplMountFactory {
    /// The factory over one client database's live connection.
    pub(crate) fn over(db: &ClientDatabase) -> Self {
        ReplMountFactory {
            connection: db.connection_arc(),
        }
    }
}

impl delightql_types::ConnectionFactory for ReplMountFactory {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<delightql_types::ConnectionComponents, delightql_types::DelightQLError>
    {
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
        delightql_types::DelightQLError,
    > {
        if uri == REPL_SESSION_LOCATOR {
            return Err(delightql_types::diagnostic::Mount::Locator {
                message: "the REPL session database has no schemas; mount! it directly".to_string(),
            }
            .into());
        }
        delightql_types::ConnectionFactory::create_tree(
            &crate::connection_factory::CliConnectionFactory,
            uri,
        )
    }
}

/// Mount the live database at `repl::data`, install the fixed wrapper
/// definitions, and verify one known public relation answers. Performed
/// by the client profile when it opens a handle over a client database,
/// and again after a successful interactive session recovery — the
/// catalog mapping dies with the session; the client-owned connection
/// does not. On a handle whose factory knows no session locator (the
/// server profile) the first `mount!` refuses and nothing is installed.
pub fn install_repl_namespace(handle: &mut dyn DqlHandle) -> anyhow::Result<()> {
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    install_repl_namespace_with(&mut |dql| {
        crate::exec_ng::run_dql_query(dql, &mut *session).map(|r| r.rows.len())
    })
}

/// The same install over ONE executor of DQL text — the road a host takes
/// when it holds no session of its own. The executor answers the row count
/// of what it ran.
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
    let utility = [(REPL_UTIL_NAMESPACE, REPL_UTIL_PROGRAM)];
    for (namespace, program) in WRAPPER_DEFINITIONS.iter().chain(&utility) {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(program.as_bytes())?;
        file.flush()?;
        let path = file.path().display().to_string();
        run(&format!("consult!(\"{path}\", \"{namespace}\")(*)"))?;
    }
    // The utilities answer bare at the prompt: the session enlists them,
    // so a reset — which returns the enlist set to its start — takes them
    // back with this install.
    run(&format!("enlist!(\"{REPL_UTIL_NAMESPACE}\")(*)"))?;

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
