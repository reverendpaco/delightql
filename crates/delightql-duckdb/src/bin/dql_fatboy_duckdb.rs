// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! dql-fatboy-duckdb — the DuckDB fatboy.
//!
//! Pattern sibling of dql-fatboy-postgres: relay protocol over its
//! stdin/stdout (the LSP model), the duckdb crate behind it. dql spawns
//! one per duckdb-file connection and reaps it on drop; the pipe is the
//! lifecycle. The database opens WRITABLE by default (E-T3b, ruled
//! 2026-07-11) and LAZILY — the first Query takes DuckDB's exclusive
//! write lock, which then excludes ALL other opens of the file (even
//! read-only ones) for this process's lifetime. `--readonly` opts back
//! into the old posture: concurrent read-only opens across processes,
//! every write refused by the engine.

use clap::Parser;
use delightql_duckdb::DuckParty;
use delightql_protocol::socket::{read_client_message, write_server_message};
use delightql_protocol::{
    ClientMessage, ControlResult, ErrorKind, Handler, ServerMessage, ServerTerm,
};

#[derive(Parser)]
#[command(
    name = "dql-fatboy-duckdb",
    version = delightql_buildinfo::human_static(),
    about = "DuckDB fatboy: relay protocol over stdin/stdout, duckdb behind it"
)]
struct Args {
    /// Database: path to a .duckdb file (must exist), or ":memory:".
    /// dql's spawn contract is just `--database D`.
    #[arg(long, default_value = ":memory:")]
    database: String,

    /// Open the database read-only (the pre-write-mode posture).
    /// Read-only opens can share one file across processes; the default
    /// writable open takes an exclusive lock at first query. dql does
    /// not pass this flag today (no dql-side surface yet); it serves
    /// direct invocation and future spawn-chain wiring.
    #[arg(long)]
    readonly: bool,
}

fn main() {
    let args = Args::parse();
    serve_stdio(&args.database, args.readonly);
}

fn connect(database: &str, readonly: bool) -> Result<DuckParty, String> {
    if readonly {
        DuckParty::connect_readonly(database)
    } else {
        DuckParty::connect(database)
    }
}

/// Serve exactly one relay session over stdin/stdout, then exit. stdout
/// is the protocol channel; diagnostics go to stderr. When the spawner
/// dies the pipe closes, read returns EOF, and we exit.
fn serve_stdio(database: &str, readonly: bool) {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();

    let mut party = match connect(database, readonly) {
        Ok(p) => p,
        Err(e) => {
            let mut buf = Vec::new();
            if read_client_message(&mut reader, &mut buf).is_ok() {
                let _ = write_server_message(
                    &mut writer,
                    &ServerMessage::Data(ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: b"delightql-error://target/duckdb/connect".to_vec(),
                        message: format!("cannot open duckdb database: {e}").into_bytes(),
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
            Err(_) => return,
        };
        let response = match msg {
            ClientMessage::Data(term) => ServerMessage::Data(party.handle(term)),
            ClientMessage::Control(delightql_protocol::ControlOp::Reset) => {
                match connect(database, readonly) {
                    Ok(fresh) => {
                        party = fresh;
                        ServerMessage::Control(ControlResult::Ok)
                    }
                    Err(e) => ServerMessage::Control(ControlResult::Error {
                        message: format!("reset failed: {e}"),
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
                    message: "cwd is not applicable to the duckdb fatboy".into(),
                })
            }
        };
        if write_server_message(&mut writer, &response).is_err() {
            return;
        }
    }
}
