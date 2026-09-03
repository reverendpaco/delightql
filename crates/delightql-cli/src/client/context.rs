// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The process context: what a replay needs to be faithful, captured once
//! at startup and written into the client database's `session`,
//! `argument` and `environment` tables. Also the ONE process-wide client
//! database, so the exit road and the panic hook can reach it without
//! threading a handle through every command.

use std::io::IsTerminal;
use std::sync::{Arc, OnceLock};

use super::database::ClientDatabase;

/// The road the process is on. A closed set: the replay driver and the
/// exit policy branch on it, and "which subcommand" is already in
/// `argument`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The interactive prompt.
    Repl,
    /// `dql query` with a query, a file, or piped stdin.
    Query,
    /// `dql server`.
    Server,
    /// The parser containment worker.
    Worker,
    /// Every other subcommand, and library use (tests).
    Other,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Repl => "repl",
            Mode::Query => "query",
            Mode::Server => "server",
            Mode::Worker => "worker",
            Mode::Other => "other",
        }
    }
}

/// Which line-editing road the REPL took, decided by rustyline from the
/// terminal it found. A transcript certifies its road with this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EditorRoad {
    /// Raw mode, per-keystroke hooks, highlighting.
    Rich,
    /// Plain line reads: stdin is not a tty or TERM is unsupported.
    Plain,
}

impl EditorRoad {
    pub fn as_str(self) -> &'static str {
        match self {
            EditorRoad::Rich => "rich",
            EditorRoad::Plain => "plain",
        }
    }
}

/// Every environment variable this workspace's own shipped source
/// consults, by name — the dql binary, its in-tree library crates, and
/// the fatboy targets it spawns (they inherit this environment). EXACT:
/// the drift test scans `crates/*/src` for a literal env read and fails
/// when this list and the source disagree in either direction. Sorted
/// bytewise.
pub const WORKSPACE_CONSULTED: &[&str] = &[
    "ConEmuPID",
    "DELIGHTQL_NO_HISTORY",
    "DQL_DIALECT",
    "DQL_FMT_DEBUG",
    "DQL_FORMAT",
    "DQL_PROBE",
    "DQL_REPL_READY_FD",
    "DQL_STATE_DIR",
    "DQL_TEST_PANIC",
    "DQL_TEST_WORKER_HANG",
    "DQL_TEST_WORKER_PANIC",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "NO_COLOR",
    "PATH",
    "PGPASSWORD",
    "RUST_BACKTRACE",
    "TERM",
    "WT_SESSION",
];

/// Variables consulted by dependencies, which the scan cannot see. Each
/// names the dependency that reads it. Sorted bytewise.
pub const DEPENDENCY_CONSULTED: &[(&str, &str)] = &[
    ("COLORTERM", "crossterm"),
    ("RUST_LOG", "env_logger"),
    ("RUST_LOG_STYLE", "env_logger"),
    ("XDG_BIN_HOME", "directories"),
    ("XDG_CACHE_HOME", "directories"),
    ("XDG_CONFIG_HOME", "directories"),
    ("XDG_DATA_HOME", "directories"),
    ("XDG_RUNTIME_DIR", "directories"),
    ("XDG_STATE_HOME", "directories"),
];

/// One row of `environment`.
#[derive(Clone, Debug)]
pub struct EnvironmentRow {
    pub name: &'static str,
    /// `None` is UNSET — a recorded fact, distinct from not recorded.
    pub value: Option<String>,
}

/// The facts captured at startup. Argv is verbatim; the environment is
/// the census, unset entries included.
#[derive(Clone, Debug)]
pub struct ProcessContext {
    pub mode: Mode,
    pub started_ms: i64,
    pub started_at: String,
    pub pid: u32,
    pub cwd: String,
    pub arguments: Vec<String>,
    pub environment: Vec<EnvironmentRow>,
    pub stdin_is_tty: bool,
    pub stdout_is_tty: bool,
    pub columns: Option<u16>,
    pub rows: Option<u16>,
}

impl ProcessContext {
    pub fn capture(mode: Mode) -> Self {
        let now = chrono::Utc::now();
        let mut names: Vec<&'static str> = WORKSPACE_CONSULTED.to_vec();
        names.extend(DEPENDENCY_CONSULTED.iter().map(|(name, _)| *name));
        names.sort_unstable();
        names.dedup();
        let environment = names
            .into_iter()
            .map(|name| EnvironmentRow {
                name,
                value: std::env::var_os(name).map(|v| v.to_string_lossy().into_owned()),
            })
            .collect();
        let (columns, rows) = terminal_size();
        ProcessContext {
            mode,
            started_ms: now.timestamp_millis(),
            started_at: now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            pid: std::process::id(),
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            arguments: std::env::args().collect(),
            environment,
            stdin_is_tty: std::io::stdin().is_terminal(),
            stdout_is_tty: std::io::stdout().is_terminal(),
            columns,
            rows,
        }
    }
}

#[cfg(unix)]
fn terminal_size() -> (Option<u16>, Option<u16>) {
    let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
    // SAFETY: TIOCGWINSZ writes a winsize into the pointed-to struct and
    // nothing else; a failure leaves it zeroed.
    let rc = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) };
    if rc == 0 && ws.ws_col > 0 {
        (Some(ws.ws_col), Some(ws.ws_row))
    } else {
        (None, None)
    }
}

#[cfg(not(unix))]
fn terminal_size() -> (Option<u16>, Option<u16>) {
    (None, None)
}

static PROCESS: OnceLock<Option<Arc<ClientDatabase>>> = OnceLock::new();

fn open_once(mode: Mode) -> Option<Arc<ClientDatabase>> {
    match ClientDatabase::open_on(mode) {
        Ok(db) => Some(Arc::new(db)),
        Err(e) => {
            eprintln!(
                "warning: the client database could not be created ({e}); \
                 repl::* and the session record are unavailable this process"
            );
            None
        }
    }
}

/// Open the process's one client database on the named road. The first
/// call wins; a second explicit open on a different road is a programming
/// error and says so in debug builds. `None` means the in-memory engine
/// itself refused — the one failure with nowhere to be recorded, so it is
/// said on stderr.
pub fn open_process_database(mode: Mode) -> Option<Arc<ClientDatabase>> {
    let mut first = false;
    let opened = PROCESS.get_or_init(|| {
        first = true;
        open_once(mode)
    });
    if !first {
        if let Some(db) = opened {
            debug_assert_eq!(
                db.mode(),
                mode,
                "the process database was already opened on another road"
            );
        }
    }
    opened.clone()
}

/// The road the process database was opened on, WITHOUT opening one:
/// safe from the panic hook, which must not allocate a database mid-panic.
pub fn current_mode() -> Option<Mode> {
    PROCESS.get().and_then(|o| o.as_ref()).map(|db| db.mode())
}

/// The process's client database. Opens it on the `Other` road when
/// nothing opened it first (library use); never disputes the road an
/// earlier explicit open chose.
pub fn process_database() -> Option<Arc<ClientDatabase>> {
    PROCESS.get_or_init(|| open_once(Mode::Other)).clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::Path;

    /// Literal env reads in one file's SHIPPED text: everything before its
    /// first `#[cfg(test)]` (test modules sit at the bottom by convention).
    fn reads_in(text: &str, found: &mut BTreeSet<String>) {
        let shipped = text.split("#[cfg(test)]").next().unwrap_or("");
        for hit in shipped.match_indices("env::var") {
            let rest = &shipped[hit.0 + hit.1.len()..];
            let rest = rest.strip_prefix("_os").unwrap_or(rest);
            let Some(rest) = rest.strip_prefix('(') else {
                continue;
            };
            let Some(rest) = rest.trim_start().strip_prefix('"') else {
                continue;
            };
            let Some(end) = rest.find('"') else {
                continue;
            };
            let name = &rest[..end];
            let shaped = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_');
            if shaped {
                found.insert(name.to_string());
            }
        }
    }

    /// Walk one crate's `src`: `build.rs` and `tests/` are not shipped.
    fn source_consulted(dir: &Path, found: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                source_consulted(&path, found);
            } else if path.extension().is_some_and(|e| e == "rs") {
                reads_in(&std::fs::read_to_string(&path).unwrap(), found);
            }
        }
    }

    fn workspace_consulted() -> BTreeSet<String> {
        let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut found = BTreeSet::new();
        for entry in std::fs::read_dir(&crates).unwrap() {
            let src = entry.unwrap().path().join("src");
            if src.is_dir() {
                source_consulted(&src, &mut found);
            }
        }
        found
    }

    /// The census is exactly what the workspace source reads: a variable
    /// added to the code without a row here fails, and so does a row
    /// nothing reads any more.
    #[test]
    fn the_workspace_census_matches_the_source() {
        let found = workspace_consulted();
        let declared: BTreeSet<String> =
            WORKSPACE_CONSULTED.iter().map(|s| s.to_string()).collect();
        assert_eq!(found, declared, "WORKSPACE_CONSULTED drifted from the source");
        let mut sorted = WORKSPACE_CONSULTED.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, WORKSPACE_CONSULTED, "keep the census sorted");
        let mut deps: Vec<&str> = DEPENDENCY_CONSULTED.iter().map(|(n, _)| *n).collect();
        deps.sort_unstable();
        assert_eq!(
            deps,
            DEPENDENCY_CONSULTED.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            "keep the dependency census sorted"
        );
        assert!(
            deps.iter().all(|d| !declared.contains(*d)),
            "a variable is consulted by the workspace OR a dependency, listed once"
        );
    }

    #[test]
    fn capture_records_unset_variables_as_rows() {
        let context = ProcessContext::capture(Mode::Other);
        let names: BTreeSet<&str> = context.environment.iter().map(|r| r.name).collect();
        assert!(names.contains("DQL_TEST_PANIC"), "an unset variable is still a row");
        assert_eq!(
            names.len(),
            WORKSPACE_CONSULTED.len() + DEPENDENCY_CONSULTED.len()
        );
        assert!(!context.arguments.is_empty(), "argv[0] is always present");
        assert_eq!(context.pid, std::process::id());
    }
}
