// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `--replay-repl` against the real binary: the driver spawns the REPL on
//! a pty, synchronizes on the ready byte, and the replayed session leaves
//! its own files behind.

#![cfg(feature = "repl")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use delightql_cli::client::context::Mode;
use delightql_cli::client::database::{ClientDatabase, InputKind, InputOutcome};
use delightql_cli::client::mount::{install_repl_namespace, open_client_handle};

fn dql_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dql"))
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for d in chars.by_ref() {
                    if ('@'..='~').contains(&d) {
                        break;
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn read_one(dir: &Path, prefix: &str) -> (String, String) {
    let name = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.starts_with(prefix))
        .unwrap_or_else(|| panic!("no {prefix}* in {}", dir.display()));
    let text = std::fs::read_to_string(dir.join(&name)).unwrap();
    (name, text)
}

/// A bare script under the current `--db`: the child runs the rich road,
/// every block lands, and its files say so.
#[test]
fn a_script_replays_through_the_real_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let db = dir.path().join("t.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (7);").unwrap();
    drop(conn);
    let script = dir.path().join("replay-script.1");
    std::fs::write(
        &script,
        "# dql replay 1\n# session test\n\n# 1 dql\nt(*)\n\n# 2 dql\nnosuch(*)\n\n# 3 dql\nt(*)\n  |> (x)\n\n# 4 dot_command\n.exit\n",
    )
    .unwrap();

    let out = Command::new(dql_exe())
        .args(["query", "--db"])
        .arg(&db)
        .arg("--replay-repl")
        .arg(&script)
        .env("DQL_STATE_DIR", &state)
        .env_remove("RUST_BACKTRACE")
        .output()
        .expect("run the replay");
    let transcript = strip_ansi(&String::from_utf8_lossy(&out.stdout));
    assert_eq!(out.status.code(), Some(0), "{transcript}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(transcript.contains("Goodbye!"), "{transcript}");
    assert!(transcript.contains("resolution/table"), "the refusal shows: {transcript}");
    assert!(transcript.contains("│ 7"), "the rows show: {transcript}");

    // The CHILD's files: an interactive session, on the rich road, with
    // the four blocks in its ledger and the refusal in its log.
    let (_, context) = read_one(&state, "context.");
    assert!(context.contains("\"mode\": \"repl\""), "{context}");
    assert!(context.contains("\"editor_road\": \"rich\""), "the per-keystroke hooks ran: {context}");
    let (_, log) = read_one(&state, "error.log.");
    assert!(log.contains("\"input\": \"nosuch(*)\""), "{log}");
    let (_, ledger) = read_one(&state, "replay-script.");
    assert!(ledger.contains("# 1 dql\nt(*)\n"), "{ledger}");
    assert!(ledger.contains("t(*)\n  |> (x)\n"), "a multi-line block pasted whole: {ledger}");
    assert!(ledger.contains("dot_command\n.exit\n"), "{ledger}");
    // The DRIVER wrote nothing of its own: one session on disk.
    let sessions = std::fs::read_dir(&state)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("context."))
        .count();
    assert_eq!(sessions, 1);
}

/// A bug tarball replays with its own argv and the extracted database.
#[test]
fn a_tarball_replays_with_its_recorded_arguments_and_extracted_database() {
    let dir = tempfile::tempdir().unwrap();
    let original_state = dir.path().join("original");
    std::env::set_var("DQL_STATE_DIR", &original_state);
    let user_db = dir.path().join("orders.db");
    let conn = rusqlite::Connection::open(&user_db).unwrap();
    conn.execute_batch("CREATE TABLE orders (id INTEGER); INSERT INTO orders VALUES (1);").unwrap();
    drop(conn);

    // A recorded session: argv names the database; the ledger queries it.
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).unwrap());
    let mut handle = open_client_handle(&db).expect("handle");
    install_repl_namespace(&mut *handle).expect("install repl::*");
    let (id, _) = db.record_input(InputKind::Dql, "orders(*)");
    db.close_input(id, InputOutcome::Succeeded, None, None, Some(1.0));
    let report = delightql_cli::client::bug::write_bug_report(&db, &mut *handle, None, Some(&user_db))
        .expect("bug report");
    // The recorded argv is this test harness's; give the tarball the argv
    // a REPL session would have carried.
    let (context_name, context) = read_one(&report.files.directory, "context.");
    let mut rows: Vec<String> = context.lines().map(|l| l.to_string()).collect();
    rows.retain(|l| !l.contains("\"relation\": \"argument\""));
    for (i, v) in ["dql", "query", "--db", user_db.to_str().unwrap()].iter().enumerate() {
        rows.push(format!(
            "{{\"relation\": \"argument\", \"ordinal\": \"{i}\", \"value\": \"{v}\"}}"
        ));
    }
    std::fs::write(report.files.directory.join(&context_name), rows.join("\n") + "\n").unwrap();
    // Rebuild the tarball with the edited context.
    let stamp = report.files.stamp;
    let tgz = dir.path().join("bug.tgz");
    {
        let file = std::fs::File::create(&tgz).unwrap();
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar = tar::Builder::new(enc);
        let prefix = format!("bug-{stamp}");
        for name in [
            format!("error.log.{stamp}"),
            context_name.clone(),
            format!("replay-script.{stamp}"),
        ] {
            tar.append_path_with_name(report.files.directory.join(&name), format!("{prefix}/{name}"))
                .unwrap();
        }
        tar.append_path_with_name(&user_db, format!("{prefix}/db/orders.db")).unwrap();
        tar.into_inner().unwrap().finish().unwrap();
    }
    // The original database goes away: only the extracted copy can serve.
    std::fs::remove_file(&user_db).unwrap();

    let replay_state = dir.path().join("replayed");
    let out = Command::new(dql_exe())
        .args(["query", "--replay-repl"])
        .arg(&tgz)
        .env("DQL_STATE_DIR", &replay_state)
        .env_remove("RUST_BACKTRACE")
        .output()
        .expect("run the replay");
    let transcript = strip_ansi(&String::from_utf8_lossy(&out.stdout));
    assert_eq!(out.status.code(), Some(0), "{transcript}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(transcript.contains("/db/orders.db"), "the extracted copy is mounted: {transcript}");
    assert!(transcript.contains("│ 1"), "the recorded query answers from it: {transcript}");
    let (_, context) = read_one(&replay_state, "context.");
    assert!(context.contains("\"mode\": \"repl\""), "{context}");
}
