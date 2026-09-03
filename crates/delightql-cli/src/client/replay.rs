// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `--replay-repl`: re-run a session's inputs through the REAL terminal
//! road. The driver spawns this executable's own `dql query …` on a pty
//! it owns, types each dot command and pastes each dql/sql block as a
//! bracketed paste, and waits for the ready byte (`DQL_REPL_READY_FD`)
//! before every keystroke it sends. The child's terminal output is
//! copied to stdout verbatim; the child writes its own session files at
//! exit; the replay's exit code is the child's.
//!
//! Given a bug tarball, `context.<MS>` supplies the original argv and
//! environment, and every database and DDL path the session named is
//! rewritten to the extracted copy. Given a bare script, the current
//! invocation's arguments and environment apply.

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use super::context::{DEPENDENCY_CONSULTED, WORKSPACE_CONSULTED};
use super::exit::{parse_replay_script, ReplayBlock};

/// One replay, loaded: the blocks, and — from a tarball — the original
/// argv, environment and the extracted resources.
pub struct Replay {
    pub blocks: Vec<ReplayBlock>,
    pub arguments: Option<Vec<String>>,
    /// `(name, value)`; `None` is unset in the original.
    pub environment: Vec<(String, Option<String>)>,
    pub terminal: (u16, u16),
    /// Original path (as typed) → extracted copy, for argv and inputs.
    pub rewrites: Vec<(String, PathBuf)>,
    /// Keeps the extracted tarball alive for the run.
    _extracted: Option<tempfile::TempDir>,
}

/// How long the driver waits for the child's next ready byte before
/// declaring the replay stuck. Generous: a replayed query may be slow.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

impl Replay {
    /// A bare script, or a `bug-<MS>.tgz`.
    pub fn load(source: &Path) -> anyhow::Result<Replay> {
        if is_tarball(source) {
            Self::load_tarball(source)
        } else {
            let text = std::fs::read_to_string(source)
                .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", source.display()))?;
            let blocks = parse_replay_script(&text).map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(Replay {
                blocks,
                arguments: None,
                environment: Vec::new(),
                terminal: (120, 40),
                rewrites: Vec::new(),
                _extracted: None,
            })
        }
    }

    fn load_tarball(source: &Path) -> anyhow::Result<Replay> {
        let dir = tempfile::tempdir()?;
        let file = std::fs::File::open(source)
            .map_err(|e| anyhow::anyhow!("cannot open {}: {e}", source.display()))?;
        tar::Archive::new(flate2::read::GzDecoder::new(file)).unpack(dir.path())?;
        let root = std::fs::read_dir(dir.path())?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.is_dir() && p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("bug-")))
            .ok_or_else(|| anyhow::anyhow!("{} holds no bug-<MS>/ directory", source.display()))?;
        let named = |prefix: &str| -> Option<PathBuf> {
            std::fs::read_dir(&root)
                .ok()?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with(prefix)))
        };
        let script = named("replay-script.")
            .ok_or_else(|| anyhow::anyhow!("{} holds no replay-script", source.display()))?;
        let blocks = parse_replay_script(&std::fs::read_to_string(&script)?)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut arguments: Option<Vec<String>> = None;
        let mut environment = Vec::new();
        let mut terminal = (120u16, 40u16);
        if let Some(context) = named("context.") {
            let text = std::fs::read_to_string(&context)?;
            let mut argv: Vec<(i64, String)> = Vec::new();
            for line in text.lines() {
                let Some(row) = parse_row(line) else { continue };
                match row.get("relation").and_then(|v| v.as_deref()) {
                    Some("argument") => {
                        if let (Some(Some(ordinal)), Some(Some(value))) = (row.get("ordinal"), row.get("value")) {
                            if let Ok(n) = ordinal.parse::<i64>() {
                                argv.push((n, value.clone()));
                            }
                        }
                    }
                    Some("environment") => {
                        if let Some(Some(name)) = row.get("name") {
                            let set = row.get("is_set").and_then(|v| v.as_deref()) == Some("1");
                            let value = if set { row.get("value").cloned().flatten() } else { None };
                            environment.push((name.clone(), value));
                        }
                    }
                    Some("session") => {
                        let n = |k: &str| row.get(k).and_then(|v| v.as_deref()).and_then(|v| v.parse::<u16>().ok());
                        if let (Some(c), Some(r)) = (n("terminal_columns"), n("terminal_rows")) {
                            terminal = (c, r);
                        }
                    }
                    _ => {}
                }
            }
            argv.sort_by_key(|(n, _)| *n);
            // argv[0] is the executable as invoked; the replay runs its own.
            arguments = Some(argv.into_iter().skip(1).map(|(_, v)| v).collect());
        }

        let mut rewrites = Vec::new();
        for sub in ["db", "ddl"] {
            let Ok(entries) = std::fs::read_dir(root.join(sub)) else { continue };
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().into_owned();
                rewrites.push((name, entry.path()));
            }
        }
        Ok(Replay {
            blocks,
            arguments,
            environment,
            terminal,
            rewrites,
            _extracted: Some(dir),
        })
    }

    /// A path as the original session typed it, redirected to the
    /// extracted copy when the tarball carries a file of that name.
    fn rewrite(&self, text: &str) -> String {
        let mut out = text.to_string();
        for (name, path) in &self.rewrites {
            let target = path.display().to_string();
            // Every spelling that ends in the file name: bare, or under any
            // directory the original machine had.
            let mut result = String::new();
            let mut rest = out.as_str();
            while let Some(i) = rest.find(name.as_str()) {
                let end = i + name.len();
                let boundary_after = rest[end..].chars().next().map_or(true, |c| !is_path_char(c));
                if boundary_after {
                    // Walk back over the directory part.
                    let mut start = i;
                    let bytes = rest.as_bytes();
                    while start > 0 && is_path_char(bytes[start - 1] as char) {
                        start -= 1;
                    }
                    result.push_str(&rest[..start]);
                    result.push_str(&target);
                } else {
                    result.push_str(&rest[..end]);
                }
                rest = &rest[end..];
            }
            result.push_str(rest);
            out = result;
        }
        out
    }
}

fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '~')
}

fn is_tarball(path: &Path) -> bool {
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    name.ends_with(".tgz") || name.ends_with(".tar.gz")
}

/// A JSONL row as written by the exit road: flat, string or null values.
/// Enough for context rows; not a JSON parser.
fn parse_row(line: &str) -> Option<std::collections::HashMap<String, Option<String>>> {
    let line = line.trim();
    let inner = line.strip_prefix('{')?.strip_suffix('}')?;
    let mut map = std::collections::HashMap::new();
    let mut rest = inner;
    loop {
        rest = rest.trim_start_matches([' ', ',']);
        if rest.is_empty() {
            break;
        }
        let (key, after) = json_string(rest)?;
        let after = after.trim_start().strip_prefix(':')?.trim_start();
        let (value, after) = if let Some(after) = after.strip_prefix("null") {
            (None, after)
        } else if after.starts_with('"') {
            let (v, a) = json_string(after)?;
            (Some(v), a)
        } else {
            let end = after.find(|c: char| c == ',' || c == '}').unwrap_or(after.len());
            (Some(after[..end].trim().to_string()), &after[end..])
        };
        map.insert(key, value);
        rest = after;
    }
    Some(map)
}

fn json_string(s: &str) -> Option<(String, &str)> {
    let mut chars = s.strip_prefix('"')?.char_indices();
    let mut out = String::new();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((out, &s[i + 2..])),
            '\\' => match chars.next()?.1 {
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'u' => {
                    let hex: String = (0..4).filter_map(|_| chars.next().map(|(_, c)| c)).collect();
                    if let Some(ch) = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                        out.push(ch);
                    }
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
    None
}

/// Run the replay. `fallback_args` is the current invocation's argument
/// list (without `--replay-repl` and its value), used when the source
/// carries no context of its own.
pub fn run(source: &Path, fallback_args: &[String]) -> anyhow::Result<i32> {
    let replay = Replay::load(source)?;
    let exe = std::env::current_exe()?;
    let args: Vec<String> = replay
        .arguments
        .clone()
        .unwrap_or_else(|| fallback_args.to_vec())
        .into_iter()
        .map(|a| replay.rewrite(&a))
        .collect();

    // The ready pipe: the child inherits the write end and names it by
    // number; the read end stays here.
    let mut fds = [0i32; 2];
    // SAFETY: a two-int array for pipe(2); O_CLOEXEC on the read end alone
    // so the child sees only the write end.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    anyhow::ensure!(rc == 0, "pipe: {}", std::io::Error::last_os_error());
    let (ready_r, ready_w) = (fds[0], fds[1]);
    unsafe {
        libc::fcntl(ready_r, libc::F_SETFD, libc::FD_CLOEXEC);
    }

    let mut command = Command::new(&exe);
    command.args(&args);
    for (name, value) in &replay.environment {
        // The machine's own PATH and HOME stay; the session directory is
        // this run's, not the original's; a name this build does not
        // consult (a context from another build) is left alone.
        if matches!(name.as_str(), "PATH" | "HOME" | "DQL_STATE_DIR") || !is_consulted(name) {
            continue;
        }
        match value {
            Some(v) => {
                command.env(name, v);
            }
            None => {
                command.env_remove(name);
            }
        }
    }
    let term_ok = |t: &str| !t.is_empty() && !matches!(t, "dumb" | "cons25" | "emacs");
    let recorded_term = replay
        .environment
        .iter()
        .find(|(n, _)| n == "TERM")
        .and_then(|(_, v)| v.clone())
        .or_else(|| std::env::var("TERM").ok());
    match recorded_term.filter(|t| term_ok(t)) {
        Some(t) => command.env("TERM", t),
        None => command.env("TERM", "xterm-256color"),
    };
    command.env("DQL_REPL_READY_FD", ready_w.to_string());

    let mut process = rexpect::process::PtyProcess::new(command)
        .map_err(|e| anyhow::anyhow!("cannot spawn the replayed dql on a pty: {e}"))?;
    // SAFETY: closing our copy of the write end; the child holds its own.
    unsafe {
        libc::close(ready_w);
    }
    process.set_kill_timeout(Some(2_000));
    let mut master = process
        .get_file_handle()
        .map_err(|e| anyhow::anyhow!("pty master: {e}"))?;
    set_window_size(master.as_raw_fd(), replay.terminal);

    let mut stdout = std::io::stdout().lock();
    let mut typed = 0usize;
    let mut ended_with_exit = false;
    let mut keystrokes: Vec<Vec<u8>> = Vec::new();
    for block in &replay.blocks {
        let input = replay.rewrite(&block.input);
        if block.kind == "dot_command" {
            if input.trim() == ".exit" || input.trim() == ".quit" {
                ended_with_exit = true;
            }
            keystrokes.push(format!("{input}\r").into_bytes());
        } else {
            // Pasted whole, then entered, then submitted with an empty
            // line — the REPL's own multiline convention; with multiline
            // off the empty line is a no-op.
            keystrokes.push(format!("\x1b[200~{input}\x1b[201~\r").into_bytes());
            keystrokes.push(b"\r".to_vec());
        }
    }
    if !ended_with_exit {
        keystrokes.push(b".exit\r".to_vec());
    }

    let mut buf = [0u8; 4096];
    let mut next = 0usize;
    let mut waiting_since = Instant::now();
    let mut child_gone = false;
    // `status()` reaps the child when it has exited; the status it
    // returns is the one and only, so it is kept, never waited for twice.
    let mut reaped: Option<rexpect::process::WaitStatus> = None;
    loop {
        let mut polled = [
            libc::pollfd { fd: master.as_raw_fd(), events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: ready_r, events: libc::POLLIN, revents: 0 },
        ];
        // SAFETY: two valid pollfds for the duration of the call.
        let n = unsafe { libc::poll(polled.as_mut_ptr(), 2, 100) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            anyhow::bail!("poll: {err}");
        }
        if polled[0].revents & (libc::POLLIN | libc::POLLHUP) != 0 {
            match master.read(&mut buf) {
                Ok(0) | Err(_) => child_gone = true,
                Ok(k) => {
                    stdout.write_all(&buf[..k])?;
                    stdout.flush()?;
                }
            }
        }
        if polled[1].revents & libc::POLLIN != 0 {
            let mut one = [0u8; 1];
            // SAFETY: one byte into a one-byte buffer.
            let k = unsafe { libc::read(ready_r, one.as_mut_ptr().cast(), 1) };
            if k == 1 {
                if next < keystrokes.len() {
                    master.write_all(&keystrokes[next])?;
                    master.flush()?;
                    next += 1;
                    typed += 1;
                    waiting_since = Instant::now();
                }
            } else {
                child_gone = true;
            }
        }
        if reaped.is_none() {
            if let Some(status) = process.status() {
                if !matches!(status, rexpect::process::WaitStatus::StillAlive) {
                    reaped = Some(status);
                }
            }
        }
        if child_gone || reaped.is_some() {
            // Drain what the child said last.
            while let Ok(k) = master.read(&mut buf) {
                if k == 0 {
                    break;
                }
                stdout.write_all(&buf[..k])?;
            }
            stdout.flush()?;
            break;
        }
        if next < keystrokes.len() && waiting_since.elapsed() > READY_TIMEOUT {
            let _ = process.kill(rexpect::process::Signal::SIGKILL);
            anyhow::bail!(
                "the replayed session did not come back to its prompt within {}s after keystroke {typed} of {}",
                READY_TIMEOUT.as_secs(),
                keystrokes.len()
            );
        }
    }
    unsafe {
        libc::close(ready_r);
    }
    let status = match reaped {
        Some(status) => status,
        None => process
            .wait()
            .map_err(|e| anyhow::anyhow!("waiting for the replayed dql: {e}"))?,
    };
    Ok(match status {
        rexpect::process::WaitStatus::Exited(_, code) => code,
        rexpect::process::WaitStatus::Signaled(_, sig, _) => 128 + sig as i32,
        _ => 1,
    })
}

fn set_window_size(fd: i32, (cols, rows): (u16, u16)) {
    let ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ reads a winsize; a failure changes nothing.
    unsafe {
        libc::ioctl(fd, libc::TIOCSWINSZ, &ws);
    }
}

/// The argument list of the current invocation with `--replay-repl` and
/// its value removed: what the child runs when the source carries no
/// context.
pub fn arguments_without_replay(argv: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut skip = false;
    for a in argv.iter().skip(1) {
        if skip {
            skip = false;
            continue;
        }
        if a == "--replay-repl" {
            skip = true;
            continue;
        }
        if let Some(_) = a.strip_prefix("--replay-repl=") {
            continue;
        }
        out.push(a.clone());
    }
    out
}

/// The census of names the environment rows may carry — a context row
/// naming anything else is from a different build and is refused, so
/// the replay never sets a variable this build would not read.
pub fn is_consulted(name: &str) -> bool {
    WORKSPACE_CONSULTED.contains(&name) || DEPENDENCY_CONSULTED.iter().any(|(n, _)| *n == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_rows_parse_with_nulls_and_escapes() {
        let row = parse_row(
            r#"{"relation": "environment", "name": "TERM", "is_set": "1", "value": "xterm-256color", "x": null}"#,
        )
        .unwrap();
        assert_eq!(row["name"].as_deref(), Some("TERM"));
        assert_eq!(row["value"].as_deref(), Some("xterm-256color"));
        assert_eq!(row["x"], None);
    }

    #[test]
    fn paths_are_rewritten_to_the_extracted_copies() {
        let replay = Replay {
            blocks: vec![],
            arguments: None,
            environment: vec![],
            terminal: (80, 24),
            rewrites: vec![("main.db".into(), PathBuf::from("/tmp/x/db/main.db"))],
            _extracted: None,
        };
        assert_eq!(replay.rewrite("/home/u/data/main.db"), "/tmp/x/db/main.db");
        assert_eq!(replay.rewrite("main.db"), "/tmp/x/db/main.db");
        assert_eq!(
            replay.rewrite("mount!(\"~/data/main.db\", \"m\")(*)"),
            "mount!(\"/tmp/x/db/main.db\", \"m\")(*)"
        );
        assert_eq!(replay.rewrite("main.dbx"), "main.dbx", "not a boundary");
    }

    #[test]
    fn the_replay_flag_is_stripped_from_the_fallback_arguments() {
        let argv: Vec<String> = ["dql", "query", "--db", "x.db", "--replay-repl", "s.txt", "-f", "json"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(arguments_without_replay(&argv), ["query", "--db", "x.db", "-f", "json"]);
    }
}
