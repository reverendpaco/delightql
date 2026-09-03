// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The exit road: the three session files, each a projection of tables,
//! written once per process wherever the process ends.
//!
//! `error.log.<MS>` is the union of core's `sys::diagnostics.finding` and
//! the client's `repl::errors.incident`. Core's table lives on the
//! bootstrap connection and the client's on its own, and the engine
//! refuses a set operation across connections (`federation-prohibited`),
//! so the two projections are merged here by `occurred_at` — the one
//! place the union is Rust. `context.<MS>` and `replay-script.<MS>` are
//! one query each.
//!
//! Core's table dies with the handle, so the road that owns a handle
//! calls [`finish`] with it while it is alive; `main`'s fallback, with
//! no handle, projects the client's side alone through a fresh handle.
//! The first call wins; every later one is a no-op.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use delightql_core::api::DqlHandle;

use super::context::{process_database, Mode};
use super::database::ClientDatabase;
use crate::exec_ng::{fetch_all_typed, TypedRows};

/// What the road wrote, for the one stderr line and for `.bug`.
#[derive(Debug, Clone)]
pub struct SessionFiles {
    pub stamp: i64,
    pub directory: PathBuf,
    pub error_log: PathBuf,
    pub context: PathBuf,
    pub replay_script: PathBuf,
}

static FINISHED: AtomicBool = AtomicBool::new(false);
static WRITTEN: std::sync::Mutex<Option<SessionFiles>> = std::sync::Mutex::new(None);

/// The directory the session files land in. `DQL_STATE_DIR` names it
/// outright (tests, and anyone who wants them elsewhere); otherwise the
/// platform's state directory (`~/.local/state/delightql` under XDG),
/// falling back to the local data directory where there is no state
/// directory (macOS).
pub fn state_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("DQL_STATE_DIR") {
        return PathBuf::from(dir);
    }
    match directories::ProjectDirs::from("", "", "delightql") {
        Some(dirs) => dirs
            .state_dir()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| dirs.data_local_dir().to_path_buf()),
        None => std::env::temp_dir().join("delightql"),
    }
}

/// Close the session: drain the panic queue, stamp the exit, and write the
/// three files. Interactive sessions always write. Every other mode
/// writes only when the log carries a client ERROR or a PANIC. A
/// refusal the user just read on stderr adds nothing on disk (a suite
/// run is thousands of those), and neither does a warning about a flag
/// the user chose — both stay rows, said once. Returns what was written,
/// or `None`.
pub fn finish(handle: Option<&mut dyn DqlHandle>, exit_code: i32) -> Option<SessionFiles> {
    if FINISHED.swap(true, Ordering::SeqCst) {
        return None;
    }
    let db = process_database()?;
    // The parser worker is a child of a client that records for it: a
    // worker's panic travels as an answer, its protocol refusals are the
    // parent's incidents. It writes nothing of its own.
    if db.mode() == Mode::Worker {
        return None;
    }
    db.drain_panics();
    db.flush_pending();
    db.record_exit(exit_code);

    // A handle is needed for the projections. Without one (main's
    // fallback), open a fresh one over the client database: the client's
    // tables are all there; core's findings from the dropped system are
    // not, and the log says nothing false — it simply lacks them.
    let mut fresh;
    let handle: &mut dyn DqlHandle = match handle {
        Some(h) => h,
        None => {
            fresh = crate::connection::open_handle_over(Some(db.clone())).ok()?.0;
            &mut *fresh
        }
    };
    let mut session = handle.session().ok()?;

    let error_log = error_log_rows(&mut *session);
    let write = db.mode() == Mode::Repl || error_log.rows.iter().any(earns_files);
    if !write {
        return None;
    }

    let context = fetch_all_typed(&mut *session, CONTEXT_QUERY).ok()?;
    let ledger = fetch_all_typed(&mut *session, REPLAY_QUERY).ok()?;

    let files = write_files(&db, &error_log, &context, &ledger)?;
    if let Ok(mut written) = WRITTEN.lock() {
        *written = Some(files.clone());
    }
    Some(files)
}

/// Say what `finish` wrote, once, as the LAST line on stderr — after the
/// error the human is reading, never before it. `main` calls this on
/// every exit road; a road that wrote nothing says nothing.
pub fn announce() {
    let files = WRITTEN.lock().ok().and_then(|mut w| w.take());
    if let Some(files) = files {
        eprintln!(
            "session {}: error.log, context, replay-script → {}/",
            files.stamp,
            files.directory.display()
        );
    }
}

/// The three files for the current state of the session — the `.bug`
/// road, which wants them now, mid-session, with the handle alive.
pub fn snapshot(db: &ClientDatabase, handle: &mut dyn DqlHandle) -> Option<SessionFiles> {
    db.drain_panics();
    db.flush_pending();
    let mut session = handle.session().ok()?;
    let error_log = error_log_rows(&mut *session);
    let context = fetch_all_typed(&mut *session, CONTEXT_QUERY).ok()?;
    let ledger = fetch_all_typed(&mut *session, REPLAY_QUERY).ok()?;
    write_files(db, &error_log, &context, &ledger)
}

/// One row shape for both sides of the log.
const CORE_QUERY: &str = "sys::diagnostics.finding(*) \
    |> (\"core\" as origin, occurred_at, kind, uri, message, input, provider as road) \
    |> #(occurred_at)";
const CLIENT_QUERY: &str = "repl::errors.incident(*) \
    |> (\"client\" as origin, last_seen_at as occurred_at, kind, uri, message, input, road) \
    |> #(occurred_at)";

/// `context.<MS>`: the session row, argv, the environment census and the
/// effective options, as one corresponding union with `relation` naming
/// each row's shape.
const CONTEXT_QUERY: &str = "\
repl::context.session(*) |> (\"session\" as relation, session_id, mode, started_ms, started_at, \
    exited_at, exit_code, pid, cwd, dql_build, stdin_is_tty, stdout_is_tty, editor_road, \
    terminal_columns, terminal_rows) : s
repl::context.argument(*) |> (\"argument\" as relation, ordinal, value) : a
repl::context.environment(*) |> (\"environment\" as relation, name, is_set, value) : e
repl::config.`option`(*) |> (\"option\" as relation, name, value, value_kind, default_value, source, changed_at) : o
s(*) ; a(*) ; e(*) ; o(*)";

const REPLAY_QUERY: &str = "repl::history.input(*) |> (id, kind, input) |> #(id)";

/// A row of the merged log that earns a non-interactive session its
/// files: a client error, or a panic on either side. The row shape is
/// fixed by `error_log_rows` (origin, occurred_at, kind, …).
fn earns_files(row: &Vec<Option<String>>) -> bool {
    let origin = row.first().and_then(|c| c.as_deref());
    let kind = row.get(2).and_then(|c| c.as_deref());
    kind == Some("panic") || (origin == Some("client") && kind == Some("error"))
}

fn error_log_rows(session: &mut dyn delightql_core::api::DqlSession) -> TypedRows {
    let core = fetch_all_typed(session, CORE_QUERY).ok();
    let client = fetch_all_typed(session, CLIENT_QUERY).ok();
    let columns: Vec<String> = ["origin", "occurred_at", "kind", "uri", "message", "input", "road"]
        .iter()
        .map(|c| c.to_string())
        .collect();
    let mut descriptors: Vec<String> = vec![String::new(); columns.len()];
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    for side in [core, client].into_iter().flatten() {
        // Realign by name: the projections spell the same columns, but the
        // merge must not depend on their order. Descriptors come from
        // whichever side answered first; both sides are text.
        let index: Vec<usize> = columns
            .iter()
            .map(|c| side.columns.iter().position(|s| s == c).unwrap_or(usize::MAX))
            .collect();
        for (k, &i) in index.iter().enumerate() {
            if descriptors[k].is_empty() {
                if let Some(d) = side.descriptors.get(i) {
                    descriptors[k] = d.clone();
                }
            }
        }
        for row in side.rows {
            rows.push(
                index
                    .iter()
                    .map(|&i| row.get(i).cloned().flatten())
                    .collect(),
            );
        }
    }
    rows.sort_by(|a, b| a[1].cmp(&b[1]));
    TypedRows {
        columns,
        descriptors,
        rows,
    }
}

fn write_files(
    db: &ClientDatabase,
    error_log: &TypedRows,
    context: &TypedRows,
    ledger: &TypedRows,
) -> Option<SessionFiles> {
    let stamp = db.started_ms();
    let directory = state_dir();
    std::fs::create_dir_all(&directory).ok()?;
    let files = SessionFiles {
        stamp,
        error_log: directory.join(format!("error.log.{stamp}")),
        context: directory.join(format!("context.{stamp}")),
        replay_script: directory.join(format!("replay-script.{stamp}")),
        directory,
    };
    std::fs::write(&files.error_log, error_log.to_jsonl()).ok()?;
    std::fs::write(&files.context, context.to_jsonl()).ok()?;
    std::fs::write(
        &files.replay_script,
        render_replay_script(db.session_id(), ledger),
    )
    .ok()?;
    Some(files)
}

/// The replay script: what a person would type. Every ledger row is a
/// block headed by `# <id> <kind>`; a block runs to the next header. A
/// dot command is typed and entered; a dql/sql block is pasted whole and
/// submitted. Headers, not blank lines, delimit blocks, so an input that
/// contains blank lines survives verbatim.
pub fn render_replay_script(session_id: &str, ledger: &TypedRows) -> String {
    let col = |name: &str| ledger.columns.iter().position(|c| c == name);
    let (Some(id), Some(kind), Some(input)) = (col("id"), col("kind"), col("input")) else {
        return String::from("# dql replay 1\n");
    };
    let cell = |row: &Vec<Option<String>>, i: usize| row[i].clone().unwrap_or_default();
    let mut out = format!("# dql replay 1\n# session {session_id}\n");
    for row in &ledger.rows {
        out.push('\n');
        out.push_str(&format!("# {} {}\n", cell(row, id), cell(row, kind)));
        out.push_str(cell(row, input).trim_end_matches('\n'));
        out.push('\n');
    }
    out
}

/// One parsed block of a replay script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBlock {
    pub id: String,
    pub kind: String,
    pub input: String,
}

/// Parse a replay script back into its blocks. Text before the first
/// header is the preamble (the `# dql replay` line and comments) and is
/// ignored; a malformed header is an error, never a guessed block.
pub fn parse_replay_script(text: &str) -> Result<Vec<ReplayBlock>, String> {
    let mut blocks: Vec<ReplayBlock> = Vec::new();
    let mut current: Option<(String, String, Vec<&str>)> = None;
    let mut saw_magic = false;
    for (n, line) in text.lines().enumerate() {
        if let Some(rest) = line.strip_prefix("# ") {
            if rest.starts_with("dql replay ") {
                saw_magic = true;
                continue;
            }
            let mut parts = rest.splitn(2, ' ');
            let first = parts.next().unwrap_or("");
            if first.chars().all(|c| c.is_ascii_digit()) && !first.is_empty() {
                let kind = parts.next().unwrap_or("").trim();
                if !matches!(kind, "dql" | "sql" | "dot_command") {
                    return Err(format!("line {}: block kind '{kind}' is not dql, sql or dot_command", n + 1));
                }
                if let Some((id, kind, lines)) = current.take() {
                    blocks.push(finish_block(id, kind, lines));
                }
                current = Some((first.to_string(), kind.to_string(), Vec::new()));
                continue;
            }
            if current.is_none() {
                continue; // preamble comment
            }
        }
        if let Some((_, _, lines)) = current.as_mut() {
            lines.push(line);
        }
    }
    if let Some((id, kind, lines)) = current.take() {
        blocks.push(finish_block(id, kind, lines));
    }
    if !saw_magic {
        return Err("not a replay script: the '# dql replay 1' line is missing".to_string());
    }
    Ok(blocks)
}

fn finish_block(id: String, kind: String, lines: Vec<&str>) -> ReplayBlock {
    let mut input = lines.join("\n");
    while input.ends_with('\n') {
        input.pop();
    }
    ReplayBlock { id, kind, input }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(rows: &[(&str, &str, &str)]) -> TypedRows {
        TypedRows {
            columns: vec!["id".into(), "kind".into(), "input".into()],
            descriptors: vec![String::new(); 3],
            rows: rows
                .iter()
                .map(|(i, k, t)| vec![Some(i.to_string()), Some(k.to_string()), Some(t.to_string())])
                .collect(),
        }
    }

    /// Render and parse are inverses, blank lines inside a block included.
    #[test]
    fn a_replay_script_round_trips_its_blocks() {
        let l = ledger(&[
            ("1", "dot_command", ".multiline on"),
            ("2", "dql", "users(*)\n\n|> (id)"),
            ("3", "sql", "select 1"),
        ]);
        let text = render_replay_script("dql-1-2", &l);
        assert!(text.starts_with("# dql replay 1\n# session dql-1-2\n"));
        let blocks = parse_replay_script(&text).unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0], ReplayBlock { id: "1".into(), kind: "dot_command".into(), input: ".multiline on".into() });
        assert_eq!(blocks[1].input, "users(*)\n\n|> (id)", "an internal blank line is content");
        assert_eq!(blocks[2].kind, "sql");
    }

    #[test]
    fn a_script_without_the_magic_line_is_refused() {
        assert!(parse_replay_script("# 1 dql\nusers(*)\n").is_err());
        assert!(parse_replay_script("# dql replay 1\n# 1 bogus\nx\n").is_err());
        assert_eq!(parse_replay_script("# dql replay 1\n").unwrap(), Vec::<ReplayBlock>::new());
    }
}
