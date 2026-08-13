// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! dql-fatboy-postgres — the Postgres fatboy.
//!
//! Speaks the relay protocol over its stdin/stdout (engine-facing side,
//! the LSP model), libpq on the database-facing side. dql spawns one per
//! `postgres://` connection and reaps it on drop; the pipe is
//! the lifecycle (EOF on stdin = the spawner is gone, so exit). No socket
//! file, no PDEATHSIG, no lease watchdog — portable with no per-OS code.
//!
//! Database selection is pre-protocol: the relay is content-blind and
//! carries no "connect to db X" term, so one fatboy process serves one
//! (server, database) pair, named at spawn via `--database`.

use clap::Parser;
use delightql_postgres::PgParty;
use delightql_protocol::socket::{read_client_message, write_server_message};
use delightql_protocol::{
    ClientMessage, ControlResult, ErrorKind, Handler, ServerMessage, ServerTerm,
};

#[derive(Parser)]
#[command(
    name = "dql-fatboy-postgres",
    version = delightql_buildinfo::human_static(),
    about = "Postgres fatboy: relay protocol over stdin/stdout, libpq behind it"
)]
struct Args {
    /// libpq connection string. Defaults assemble from PGHOST/PGPORT/
    /// PGUSER/PGDATABASE (falling back to the test-suite sweep container:
    /// 127.0.0.1:5433, postgres, dql_core).
    #[arg(long)]
    conninfo: Option<String>,

    /// Database name — overrides PGDATABASE. The fatboy spawn contract is
    /// just `--database D` (the transport is implicit stdin/stdout).
    #[arg(long)]
    database: Option<String>,
}

fn conninfo_from_env() -> String {
    let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    format!(
        "host={} port={} user={} dbname={}",
        var("PGHOST", "127.0.0.1"),
        var("PGPORT", "5433"),
        var("PGUSER", "postgres"),
        var("PGDATABASE", "dql_core"),
    )
}

fn main() {
    let args = Args::parse();
    if let Some(db) = &args.database {
        // --database wins over PGDATABASE.
        std::env::set_var("PGDATABASE", db);
    }
    let conninfo = args.conninfo.unwrap_or_else(conninfo_from_env);
    serve_stdio(&conninfo);
}

/// Serve exactly one relay session over stdin/stdout, then exit. stdout
/// is the protocol channel (binary frames) — nothing else may write to
/// it; diagnostics go to stderr. When the spawner dies the pipe closes,
/// read returns EOF, and we exit.
/// Env-complete a conninfo the way libpq would: rust-postgres reads no
/// environment, so missing pieces (host/port/user/dbname/password) fill
/// from PG* variables, falling back to the test-suite sweep container.
/// This is what makes the worldly `postgres:///dbname` form work.
fn complete_config(conninfo: &str) -> Result<postgres::Config, String> {
    let mut cfg: postgres::Config = conninfo
        .parse()
        .map_err(|e| format!("invalid conninfo '{conninfo}': {e}"))?;
    let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    if cfg.get_hosts().is_empty() {
        cfg.host(&var("PGHOST", "127.0.0.1"));
    }
    if cfg.get_ports().is_empty() {
        cfg.port(var("PGPORT", "5433").parse().unwrap_or(5433));
    }
    if cfg.get_user().is_none() {
        cfg.user(&var("PGUSER", "postgres"));
    }
    if cfg.get_dbname().is_none() {
        cfg.dbname(&var("PGDATABASE", "dql_core"));
    }
    if cfg.get_password().is_none() {
        if let Ok(pw) = std::env::var("PGPASSWORD") {
            cfg.password(&pw);
        }
    }
    Ok(cfg)
}

fn serve_stdio(conninfo: &str) {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();

    // One Postgres session, fail-closed: if the database is unreachable,
    // answer the first term with a Connection error (not a silent EOF).
    let mut party = match complete_config(conninfo)
        .map_err(|e| e.to_string())
        .and_then(|cfg| PgParty::connect_config(&cfg).map_err(|e| e.to_string()))
    {
        Ok(p) => p,
        Err(e) => {
            let mut buf = Vec::new();
            if read_client_message(&mut reader, &mut buf).is_ok() {
                let _ = write_server_message(
                    &mut writer,
                    &ServerMessage::Data(ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: b"delightql-error://target/postgres/connect".to_vec(),
                        message: format!("cannot reach postgres: {e}").into_bytes(),
                    }),
                );
            }
            return;
        }
    };

    let mut buf = Vec::new();
    loop {
        let msg = match read_client_message(&mut reader, &mut buf) {
            Ok(m) => m,
            Err(_) => return, // spawner gone (stdin EOF) or hung up
        };
        let response = match msg {
            ClientMessage::Data(term) => ServerMessage::Data(party.handle(term)),

            // Reset = a fresh Postgres session: handles gone, TEMP tables
            // gone — the honest interpretation for a backend whose session
            // state lives server-side.
            ClientMessage::Control(delightql_protocol::ControlOp::Reset) => {
                match PgParty::connect(conninfo) {
                    Ok(fresh) => {
                        party = fresh;
                        ServerMessage::Control(ControlResult::Ok)
                    }
                    Err(e) => ServerMessage::Control(ControlResult::Error {
                        message: format!("reset failed: cannot reach postgres: {e}"),
                    }),
                }
            }
            ClientMessage::Control(delightql_protocol::ControlOp::Shutdown) => {
                let _ =
                    write_server_message(&mut writer, &ServerMessage::Control(ControlResult::Ok));
                return;
            }
            ClientMessage::Control(delightql_protocol::ControlOp::Cwd(_)) => {
                ServerMessage::Control(ControlResult::Error {
                    message: "cwd is not applicable to the postgres fatboy".into(),
                })
            }
        };
        if write_server_message(&mut writer, &response).is_err() {
            return;
        }
    }
}
