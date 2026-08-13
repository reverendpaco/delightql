// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// dql-test-ball-runner — Run test balls (new schema: test_code/test_run)
//
// Reads a ball SQLite file, connects to a running `dql server`, executes
// each test_run with three-path dispatch (SEF/DDL/DML), and reports results.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use clap::Parser;
use sha2::{Digest, Sha256};

use delightql_protocol::socket::SocketTransport;
use delightql_protocol::{
    AgreedOrientation, Cell, Client, ControlResult, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult,
};

#[derive(Parser)]
#[command(
    name = "dql-test-ball-runner",
    version = delightql_buildinfo::human_static(),
    about = "Run test balls against a dql server"
)]
struct Args {
    /// Unix socket path to connect to
    #[arg(long)]
    socket: PathBuf,

    /// Ball file(s) to run
    balls: Vec<PathBuf>,

    /// Send Shutdown control op to the server after tests complete
    #[arg(long)]
    shutdown: bool,

    /// Write results to a SQLite database (created if missing, appended to if existing)
    #[arg(long)]
    results_db: Option<PathBuf>,

    /// Client threads per ball. Balls run concurrently and each also has a
    /// server-side pool, so this is sized against the machine rather than
    /// the core count.
    #[arg(long, default_value_t = 2)]
    workers: usize,

    /// Seconds to wait for the server to say anything before giving up on a
    /// test. A query that never answers is recorded as an error instead of
    /// holding the ball open. Zero waits forever.
    #[arg(long, default_value_t = 30)]
    query_timeout: u64,
}

/// The per-process limits the whole run reads, set once from the arguments.
///
/// A global because every worker thread and every reconnect needs them and
/// threading them through each phase would say nothing the name does not.
static LIMITS: std::sync::OnceLock<Limits> = std::sync::OnceLock::new();

#[derive(Clone, Copy)]
struct Limits {
    workers: usize,
    query_timeout: Option<std::time::Duration>,
}

fn limits() -> Limits {
    *LIMITS.get().expect("limits are set before any ball runs")
}

#[derive(Clone, Copy, PartialEq)]
enum HashMode {
    String,
    Byte,
}

#[derive(Debug)]
struct HashObservation {
    digest: String,
    empty_columns: Option<usize>,
}

struct TestResultRow {
    status: String,
    ball: String,
    test_name: String,
    detail: String,
    duration_ms: f64,
}

struct WorkerResult {
    passed: u32,
    failed: u32,
    errors: u32,
    meh: u32,
    output: Vec<String>,
    rows: Vec<TestResultRow>,
}

// ---------------------------------------------------------------------------
// Protocol helpers
// ---------------------------------------------------------------------------

/// Whether a failure means the SESSION is unusable, as opposed to the query
/// having been ANSWERED with a refusal.
///
/// A refusal is an answer: the frame arrived and the session is in step, so
/// the next request reads its own reply. A transport failure is not — a read
/// deadline leaves the abandoned request's response still to come, and the
/// next reader would take that late frame for its own. Reusing the session
/// across one is how a single silent query took the rest of a shard with it.
fn is_transport_failure(message: &str) -> bool {
    !message.starts_with("query error:")
}

/// What a caller is told when the link holds no session: the reconnect that
/// would have supplied one could not reach the server. It reads as a
/// transport failure, so the test that follows tries the reconnect again
/// instead of inheriting a dead link in silence.
const SESSION_LOST: &str = "session lost: the reconnect did not reach the server";

/// A session and the means to replace it.
///
/// Every phase holds one of these rather than a bare session, because the
/// recovery rule is the same everywhere: on a transport failure the session
/// is discarded, a new one is taken, and the test's required state is
/// established again on it.
struct Link {
    socket: PathBuf,
    query_timeout: Option<Duration>,
    /// Empty exactly while a replacement is being taken, and after a
    /// reconnect that failed. The option is what makes the poisoned session
    /// droppable BEFORE its replacement is opened; a plain field can only be
    /// overwritten after, which keeps the dead connection alive across the
    /// new handshake.
    session: Option<Session<SocketTransport>>,
    orientation: AgreedOrientation,
}

impl Link {
    fn connect(socket: &Path, query_timeout: Option<Duration>) -> Result<Self, String> {
        let (session, orientation) = open_session(socket, query_timeout)?;
        Ok(Link {
            socket: socket.to_path_buf(),
            query_timeout,
            session: Some(session),
            orientation,
        })
    }

    /// The live session, or the reason there is none.
    fn session(&mut self) -> Result<&mut Session<SocketTransport>, String> {
        self.session
            .as_mut()
            .ok_or_else(|| SESSION_LOST.to_string())
    }

    /// Discard the poisoned session and take a fresh one.
    ///
    /// The old session is taken and dropped BEFORE the replacement is opened,
    /// closing its socket first. A server serves one connection per worker
    /// until that connection closes: hold the poisoned one open across the
    /// replacement's connect and handshake and the worker that must service
    /// the replacement is still owned by the connection being abandoned. The
    /// replacement then waits behind it for another deadline, and the late
    /// response is written into a stream that is still open to read it.
    fn renew(&mut self) -> Result<(&mut Session<SocketTransport>, AgreedOrientation), String> {
        drop(self.session.take());
        let (session, orientation) = open_session(&self.socket, self.query_timeout)?;
        self.orientation = orientation;
        Ok((self.session.insert(session), orientation))
    }

    /// Establish a test's required session state, taking a fresh session if
    /// the current one has been poisoned.
    ///
    /// Setup is where a poisoned session shows itself: a late frame is what
    /// the next reset would read. Retrying ONCE on a new session is enough —
    /// a second failure is the server being gone, not a stale frame, and the
    /// caller records it against the test rather than abandoning the shard.
    fn establish(
        &mut self,
        setup: &dyn Fn(&mut Session<SocketTransport>, AgreedOrientation) -> Result<(), String>,
    ) -> Result<(), String> {
        let orientation = self.orientation;
        let first = match self.session.as_mut() {
            Some(session) => setup(session, orientation),
            None => Err(SESSION_LOST.to_string()),
        };
        match first {
            Ok(()) => Ok(()),
            Err(first) => {
                let (session, orientation) = self
                    .renew()
                    .map_err(|e| format!("{}; reconnect failed: {}", first, e))?;
                setup(session, orientation)
                    .map_err(|e| format!("{}; after reconnect: {}", first, e))
            }
        }
    }

    /// Answer a test's outcome, renewing the session first when the failure
    /// was the transport's. The row is the caller's to record — exactly one,
    /// whether the query answered, refused, or went silent.
    fn recover_if_poisoned(&mut self, outcome: &Result<HashObservation, String>) {
        if let Err(message) = outcome {
            if is_transport_failure(message) {
                eprintln!("runner: session lost ({}), reconnecting", message);
                if let Err(e) = self.renew() {
                    eprintln!("runner: reconnect failed: {}", e);
                }
            }
        }
    }
}

fn open_session(
    socket_path: &Path,
    query_timeout: Option<Duration>,
) -> Result<(Session<SocketTransport>, AgreedOrientation), String> {
    let stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect to {}: {}", socket_path.display(), e))?;
    // A deadline on READS, which is what a silent server looks like from
    // here. It bounds the wait between bytes rather than the whole query, so
    // it stops a hang without cutting a slow-but-answering stream short.
    if let Some(timeout) = query_timeout {
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| format!("set read timeout: {}", e))?;
    }
    let transport = SocketTransport::new(stream);
    let client = Client::new(transport);

    let session = match client
        .version(
            1_000_000,
            b"relay0".to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("version handshake: {}", e.message))?
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => {
            return Err(format!(
                "version rejected: {}",
                String::from_utf8_lossy(&message)
            ));
        }
    };

    let rows_orientation = session
        .agreed_orientation(Orientation::Rows)
        .ok_or("server does not support Rows orientation")?;

    Ok((session, rows_orientation))
}

fn send_reset(session: &mut Session<SocketTransport>) -> Result<(), String> {
    match session
        .reset()
        .map_err(|e| format!("reset: {}", e.message))?
    {
        ControlResult::Ok => Ok(()),
        ControlResult::Error { message } => Err(format!("reset: {}", message)),
    }
}

fn send_cwd(session: &mut Session<SocketTransport>, path: &str) -> Result<(), String> {
    match session
        .cwd(path.to_string())
        .map_err(|e| format!("cwd: {}", e.message))?
    {
        ControlResult::Ok => Ok(()),
        ControlResult::Error { message } => Err(format!("cwd: {}", message)),
    }
}

fn send_mount(
    session: &mut Session<SocketTransport>,
    db_filename: &str,
    rows_orientation: AgreedOrientation,
) -> Result<(), String> {
    let mount_query = format!("mount!(\"{}\",\"main\")(*)", db_filename);
    let handle = match session
        .query(mount_query.as_bytes().to_vec())
        .map_err(|e| format!("mount: {}", e.message))?
    {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error { message, .. } => {
            return Err(format!(
                "mount error: {}",
                String::from_utf8_lossy(&message)
            ));
        }
    };
    loop {
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| format!("mount fetch: {}", e.message))?
        {
            FetchResponse::Data { .. } => continue,
            FetchResponse::End => break,
            FetchResponse::Error { message, .. } => {
                return Err(format!(
                    "mount fetch error: {}",
                    String::from_utf8_lossy(&message)
                ));
            }
        }
    }
    let _ = session.close(handle);
    Ok(())
}

#[allow(dead_code)]
fn reset_and_mount(
    session: &mut Session<SocketTransport>,
    db_filename: &str,
    rows_orientation: AgreedOrientation,
) -> Result<(), String> {
    send_reset(session)?;
    send_mount(session, db_filename, rows_orientation)
}

// ---------------------------------------------------------------------------
// Hash computation
// ---------------------------------------------------------------------------

fn hex2hash(hex: &str) -> String {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect();

    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut encoder = base64::Base64Encoder::new(&mut buf);
        encoder.write_all(&bytes).unwrap();
    }
    let b64 = String::from_utf8(buf).unwrap();

    let safe: String = b64
        .chars()
        .map(|c| match c {
            '/' => '_',
            '+' => '-',
            _ => c,
        })
        .collect();

    safe[..8.min(safe.len())].to_string()
}

// Inline base64 encoder (no external dep)
mod base64 {
    use std::io::{self, Write};
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub struct Base64Encoder<'a> {
        out: &'a mut Vec<u8>,
        buf: [u8; 3],
        len: usize,
    }

    impl<'a> Base64Encoder<'a> {
        pub fn new(out: &'a mut Vec<u8>) -> Self {
            Self {
                out,
                buf: [0; 3],
                len: 0,
            }
        }
        fn flush_block(&mut self) {
            let b = self.buf;
            self.out.push(ALPHABET[(b[0] >> 2) as usize]);
            self.out
                .push(ALPHABET[((b[0] & 0x03) << 4 | b[1] >> 4) as usize]);
            if self.len > 1 {
                self.out
                    .push(ALPHABET[((b[1] & 0x0f) << 2 | b[2] >> 6) as usize]);
            } else {
                self.out.push(b'=');
            }
            if self.len > 2 {
                self.out.push(ALPHABET[(b[2] & 0x3f) as usize]);
            } else {
                self.out.push(b'=');
            }
            self.buf = [0; 3];
            self.len = 0;
        }
    }

    impl Write for Base64Encoder<'_> {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            for &byte in data {
                self.buf[self.len] = byte;
                self.len += 1;
                if self.len == 3 {
                    self.flush_block();
                }
            }
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.len > 0 {
                self.flush_block();
            }
            Ok(())
        }
    }

    impl Drop for Base64Encoder<'_> {
        fn drop(&mut self) {
            let _ = self.flush();
        }
    }
}

fn compute_data_hash(rows: &[Vec<Cell>]) -> String {
    let mut row_hashes: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut hasher = Sha256::new();
        for cell in row {
            match cell {
                Some(bytes) if !bytes.is_empty() => {
                    let text = String::from_utf8_lossy(bytes).to_string();
                    if text.is_empty() {
                        hasher.update(b"NULL");
                    } else {
                        hasher.update(text.as_bytes());
                    }
                }
                _ => hasher.update(b"NULL"),
            }
            hasher.update(b"|");
        }
        row_hashes.push(format!("{:x}", hasher.finalize()));
    }
    row_hashes.sort();
    let mut data_hasher = Sha256::new();
    data_hasher.update(b"ROWS:");
    for rh in &row_hashes {
        data_hasher.update(rh.as_bytes());
        data_hasher.update(b"\n");
    }
    format!("{:x}", data_hasher.finalize())
}

fn compute_byte_hash(rows: &[Vec<Cell>]) -> String {
    let mut row_hashes: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row_hasher = Sha256::new();
        for cell in row {
            let mut cell_hasher = Sha256::new();
            if let Some(bytes) = cell {
                cell_hasher.update(bytes);
            }
            row_hasher.update(cell_hasher.finalize());
        }
        row_hashes.push(format!("{:x}", row_hasher.finalize()));
    }
    row_hashes.sort();
    let mut data_hasher = Sha256::new();
    for rh in &row_hashes {
        data_hasher.update(rh.as_bytes());
    }
    format!("{:x}", data_hasher.finalize())
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn query_error(identity: &[u8], message: &[u8]) -> String {
    let identity = String::from_utf8_lossy(identity);
    let message = String::from_utf8_lossy(message);
    if identity.is_empty() {
        format!("query error: {message}")
    } else {
        format!("query error: {identity}: {message}")
    }
}

fn send_query_and_hash(
    session: &mut Session<SocketTransport>,
    query_text: &str,
    rows_orientation: AgreedOrientation,
) -> Result<HashObservation, String> {
    let (handle, column_count) = match session
        .query(query_text.as_bytes().to_vec())
        .map_err(|e| format!("query: {}", e.message))?
    {
        QueryResponse::Header { handle, dimensions } => (handle, dimensions.len()),
        QueryResponse::Error {
            identity, message, ..
        } => {
            return Err(query_error(&identity, &message));
        }
    };
    let mut all_rows: Vec<Vec<Cell>> = Vec::new();
    loop {
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| format!("fetch: {}", e.message))?
        {
            FetchResponse::Data { cells } => all_rows.extend(cells),
            FetchResponse::End => break,
            FetchResponse::Error { message, .. } => {
                return Err(format!(
                    "fetch error: {}",
                    String::from_utf8_lossy(&message)
                ));
            }
        }
    }
    let _ = session.close(handle);
    Ok(HashObservation {
        digest: compute_data_hash(&all_rows),
        empty_columns: all_rows.is_empty().then_some(column_count),
    })
}

fn send_query_and_bhash(
    session: &mut Session<SocketTransport>,
    query_text: &str,
    rows_orientation: AgreedOrientation,
) -> Result<HashObservation, String> {
    let (handle, column_count) = match session
        .query(query_text.as_bytes().to_vec())
        .map_err(|e| format!("query: {}", e.message))?
    {
        QueryResponse::Header { handle, dimensions } => (handle, dimensions.len()),
        QueryResponse::Error {
            identity, message, ..
        } => {
            return Err(query_error(&identity, &message));
        }
    };
    let mut all_rows: Vec<Vec<Cell>> = Vec::new();
    loop {
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| format!("fetch: {}", e.message))?
        {
            FetchResponse::Data { cells } => all_rows.extend(cells),
            FetchResponse::End => break,
            FetchResponse::Error { message, .. } => {
                return Err(format!(
                    "fetch error: {}",
                    String::from_utf8_lossy(&message)
                ));
            }
        }
    }
    let _ = session.close(handle);
    Ok(HashObservation {
        digest: compute_byte_hash(&all_rows),
        empty_columns: all_rows.is_empty().then_some(column_count),
    })
}

fn send_query_and_hash_dispatch(
    session: &mut Session<SocketTransport>,
    query_text: &str,
    rows_orientation: AgreedOrientation,
    mode: HashMode,
) -> Result<HashObservation, String> {
    match mode {
        HashMode::String => send_query_and_hash(session, query_text, rows_orientation),
        HashMode::Byte => send_query_and_bhash(session, query_text, rows_orientation),
    }
}

/// The AUTHORED extent of each query in a submission, in order.
///
/// The sequence root draws these boundaries; a text scan for a separator would
/// have to know which newline is inside a template and which ends a query — a
/// question the parse has already answered.
/// A DEFECTIVE SOURCE HAS NO BOUNDARIES, so it is ONE submission and the
/// server answers it. The runner splits where the sequence root draws lines
/// and has no parse teaching of its own to offer — inventing one here would
/// hide the compiler's, which is the answer the test is about.
fn split_queries(source: &str) -> Result<Vec<String>, String> {
    use delightql_cst::cst;

    let tree = delightql_cst::Parser::new().parse_query_sequence(source);
    if tree.has_defects() {
        return Ok(vec![source.to_string()]);
    }
    let Some(cst::SourceFileChild::QuerySequenceRoot(root)) = tree.root_branch() else {
        return Ok(vec![source.to_string()]);
    };
    let Some(sequence) = root.children().find_map(|child| match child {
        cst::QuerySequenceRootChild::QuerySequence(sequence) => Some(sequence),
        cst::QuerySequenceRootChild::QuerySequenceHeader(_) => None,
    }) else {
        return Ok(vec![source.to_string()]);
    };
    let queries: Vec<String> = sequence
        .children()
        .filter_map(|child| match child {
            cst::QuerySequenceChild::Relex(relex) => tree.byte_range(relex),
            cst::QuerySequenceChild::Effrelex(effrelex) => tree.byte_range(effrelex),
        })
        .map(|range| source[range].to_string())
        .collect();

    if queries.is_empty() {
        return Err("no queries found in source".into());
    }
    Ok(queries)
}

fn send_sequential_and_hash(
    session: &mut Session<SocketTransport>,
    dql: &str,
    rows_orientation: AgreedOrientation,
    mode: HashMode,
) -> Result<HashObservation, String> {
    let queries = split_queries(dql)?;
    let mut last = None;
    for q in &queries {
        last = Some(send_query_and_hash_dispatch(
            session,
            q,
            rows_orientation,
            mode,
        )?);
    }
    last.ok_or_else(|| "no queries found in source".to_string())
}

// ---------------------------------------------------------------------------
// Ball runner
// ---------------------------------------------------------------------------

struct BallTestRun {
    #[allow(dead_code)]
    run_id: i64,
    code_id: i64,
    name: String,
    kind: String,
    sequential: bool,
    dql: String,
    db_id: i64,
    db_path: String,
    hash: Option<String>,
    hashtype: Option<String>,
}

fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

fn observed_baseline(
    observation: &HashObservation,
    hashtype: Option<&str>,
    preserve_empty_shape: bool,
) -> String {
    if preserve_empty_shape {
        if let Some(columns) = observation.empty_columns {
            return format!("EMPTY:{columns}");
        }
    }
    if hashtype == Some("shash") {
        observation.digest.clone()
    } else {
        hex2hash(&observation.digest)
    }
}

fn judge(
    ball_name: &str,
    test_name: &str,
    exec_result: Result<HashObservation, String>,
    expected_hash: &Option<String>,
    hashtype: &Option<String>,
    elapsed: Duration,
    result: &mut WorkerResult,
) {
    let dur = format_duration(elapsed);
    let duration_ms = elapsed.as_secs_f64() * 1000.0;
    let is_error_test = hashtype.as_deref() == Some("error");
    let empty_error_expectation = is_error_test
        && expected_hash
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty();

    let (status, detail) = if empty_error_expectation {
        ("ERROR", "empty refusal expectation".to_string())
    } else if is_error_test {
        let pattern = expected_hash
            .as_deref()
            .expect("a non-empty error expectation was checked above");
        match &exec_result {
            Err(e) => {
                if e.contains(pattern) {
                    ("PASS", String::new())
                } else {
                    (
                        "FAIL",
                        format!("error expected to contain '{}' but got: {}", pattern, e),
                    )
                }
            }
            Ok(_) => ("FAIL", "expected error but query succeeded".to_string()),
        }
    } else {
        match &exec_result {
            Ok(actual_hex) => match expected_hash {
                None => {
                    let actual_short =
                        observed_baseline(actual_hex, hashtype.as_deref(), false);
                    ("MEH", actual_short)
                }
                Some(expected) => {
                    let actual_short = observed_baseline(
                        actual_hex,
                        hashtype.as_deref(),
                        expected.starts_with("EMPTY:"),
                    );
                    if *expected == actual_short {
                        ("PASS", String::new())
                    } else {
                        ("FAIL", format!("expected:{} actual:{}", expected, actual_short))
                    }
                }
            },
            Err(e) => ("ERROR", e.replace('\n', " ")),
        }
    };

    if detail.is_empty() {
        result.output.push(format!("[{}]\t{}\t{}\t\t{}", status, ball_name, test_name, dur));
    } else {
        result.output.push(format!("[{}]\t{}\t{}\t{}\t{}", status, ball_name, test_name, detail, dur));
    }

    result.rows.push(TestResultRow {
        status: status.to_string(),
        ball: ball_name.to_string(),
        test_name: test_name.to_string(),
        detail,
        duration_ms,
    });

    match status {
        "PASS" => result.passed += 1,
        "FAIL" => result.failed += 1,
        "ERROR" => result.errors += 1,
        "MEH" => result.meh += 1,
        _ => {}
    }
}

fn copy_databases_to_work_dir(
    work_dir: &Path,
    db_paths: &std::collections::HashMap<i64, PathBuf>,
) -> Result<(), String> {
    let databases_dir = work_dir.join("databases");
    std::fs::create_dir_all(&databases_dir).map_err(|e| format!("mkdir databases: {}", e))?;
    for (_id, path) in db_paths {
        let filename = path.file_name().unwrap_or_default();
        let dest = databases_dir.join(filename);
        if !dest.exists() {
            std::fs::copy(path, &dest)
                .map_err(|e| format!("copy db {}: {}", dest.display(), e))?;
        }
    }
    Ok(())
}

fn write_results_db(path: &Path, rows: &[TestResultRow]) -> Result<(), String> {
    let conn = Connection::open(path)
        .map_err(|e| format!("open results db {}: {}", path.display(), e))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| format!("set WAL: {}", e))?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS test_result (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
            status TEXT NOT NULL,
            ball TEXT NOT NULL,
            test_name TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            duration_ms REAL NOT NULL
        )"
    ).map_err(|e| format!("create table: {}", e))?;

    let tx = conn.unchecked_transaction()
        .map_err(|e| format!("begin transaction: {}", e))?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO test_result (status, ball, test_name, detail, duration_ms) VALUES (?1, ?2, ?3, ?4, ?5)"
        ).map_err(|e| format!("prepare insert: {}", e))?;
        for row in rows {
            stmt.execute(rusqlite::params![
                row.status, row.ball, row.test_name, row.detail, row.duration_ms
            ]).map_err(|e| format!("insert: {}", e))?;
        }
    }
    tx.commit().map_err(|e| format!("commit: {}", e))?;
    Ok(())
}

fn run_ball(ball_path: &Path, socket_path: &Path, results_db: Option<&Path>) -> Result<bool, String> {
    let ball_name = ball_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();

    let conn = Connection::open(ball_path)
        .map_err(|e| format!("open ball {}: {}", ball_path.display(), e))?;

    // Phase 1: Extract databases to temp directory
    let tmpdir = PathBuf::from(format!(
        "/tmp/dql-ball-{}-{}",
        std::process::id(),
        ball_path.file_stem().unwrap_or_default().to_string_lossy()
    ));
    let _ = std::fs::remove_dir_all(&tmpdir);
    std::fs::create_dir_all(&tmpdir).map_err(|e| format!("create tmpdir: {}", e))?;

    let mut db_stmt = conn
        .prepare("SELECT id, name, backend, path, blob FROM database ORDER BY id")
        .map_err(|e| format!("prepare database: {}", e))?;

    let databases: Vec<(i64, String, String, String, Option<Vec<u8>>)> = db_stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .map_err(|e| format!("query database: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read database: {}", e))?;

    let databases_dir = tmpdir.join("databases");
    std::fs::create_dir_all(&databases_dir).map_err(|e| format!("create databases dir: {}", e))?;

    let mut db_paths: std::collections::HashMap<i64, PathBuf> = std::collections::HashMap::new();
    for (id, _name, _backend, path, blob) in &databases {
        if let Some(blob) = blob {
            let decompressed =
                zstd::decode_all(&blob[..]).map_err(|e| format!("decompress db {}: {}", id, e))?;
            let dest = databases_dir.join(path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
            }
            std::fs::write(&dest, &decompressed)
                .map_err(|e| format!("write db {}: {}", dest.display(), e))?;
            db_paths.insert(*id, dest);
        }
    }

    // Phase 2: Load all test runs (joined)
    let mut run_stmt = conn
        .prepare(
            "SELECT tr.id, tc.id, tc.name, tc.kind, tc.sequential, tc.dql, \
                    d.id, d.path, tr.hash, tr.hashtype \
             FROM test_run tr \
             JOIN test_code tc ON tc.id = tr.test_code_id \
             JOIN database d ON d.id = tr.database_id \
             ORDER BY tr.id",
        )
        .map_err(|e| format!("prepare test_run join: {}", e))?;

    let all_runs: Vec<BallTestRun> = run_stmt
        .query_map([], |row| {
            Ok(BallTestRun {
                run_id: row.get(0)?,
                code_id: row.get(1)?,
                name: row.get(2)?,
                kind: row.get(3)?,
                sequential: row.get::<_, i64>(4)? != 0,
                dql: row.get(5)?,
                db_id: row.get(6)?,
                db_path: row.get(7)?,
                hash: row.get(8)?,
                hashtype: row.get(9)?,
            })
        })
        .map_err(|e| format!("query test_run: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read test_run: {}", e))?;

    // Load DDL files per test_code_id
    let mut ddl_map: std::collections::HashMap<i64, Vec<(String, String)>> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn
            .prepare("SELECT test_code_id, filename, content FROM test_ddl ORDER BY test_code_id")
            .map_err(|e| format!("prepare test_ddl: {}", e))?;
        for row in stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| format!("query test_ddl: {}", e))?
        {
            let (code_id, filename, content) =
                row.map_err(|e| format!("read test_ddl: {}", e))?;
            ddl_map
                .entry(code_id)
                .or_default()
                .push((filename, content));
        }
    }

    // Load init scripts per test_code_id
    let mut init_map: std::collections::HashMap<i64, Vec<(String, String, String)>> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn
            .prepare(
                "SELECT test_code_id, name, filename, content FROM test_init ORDER BY test_code_id",
            )
            .map_err(|e| format!("prepare test_init: {}", e))?;
        for row in stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| format!("query test_init: {}", e))?
        {
            let (code_id, name, filename, content) =
                row.map_err(|e| format!("read test_init: {}", e))?;
            init_map
                .entry(code_id)
                .or_default()
                .push((name, filename, content));
        }
    }

    // Phase 3: Partition by kind
    let mut sef_runs = Vec::new();
    let mut ddl_runs = Vec::new();
    let mut dml_runs = Vec::new();

    for run in all_runs {
        match run.kind.as_str() {
            "sef" => sef_runs.push(run),
            "ddl" => ddl_runs.push(run),
            "dml" => dml_runs.push(run),
            other => return Err(format!("unknown test kind: {}", other)),
        }
    }


    // Phase 4: Run three phases sequentially (SEF → DDL → DML), like pack-man.
    // Each phase spawns its own workers. No mixing of work unit types on a connection.
    let db_paths = Arc::new(db_paths);
    let ddl_map = Arc::new(ddl_map);
    let init_map = Arc::new(init_map);
    let tmpdir = Arc::new(tmpdir);

    let max_workers = limits().workers.max(1);

    let socket_owned = socket_path.to_owned();

    static ISOLATE_COUNTER: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut errors = 0u32;
    let mut meh = 0u32;
    let mut any_worker_error = false;
    let mut all_rows: Vec<TestResultRow> = Vec::new();

    // Helper to collect results from worker handles
    let mut collect = |handles: Vec<std::thread::JoinHandle<Result<WorkerResult, String>>>| {
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(Ok(wr)) => {
                    let stdout = std::io::stdout();
                    let mut lock = stdout.lock();
                    for line in &wr.output {
                        let _ = writeln!(lock, "{}", line);
                        let _ = lock.flush();
                    }
                    passed += wr.passed;
                    failed += wr.failed;
                    errors += wr.errors;
                    meh += wr.meh;
                    all_rows.extend(wr.rows);
                }
                Ok(Err(e)) => {
                    eprintln!("dql-test-ball-runner: worker {} error: {}", i, e);
                    any_worker_error = true;
                }
                Err(_) => {
                    eprintln!("dql-test-ball-runner: worker {} panicked", i);
                    any_worker_error = true;
                }
            }
        }
    };

    // ---- Phase 4a: SEF ----
    if !sef_runs.is_empty() {
        sef_runs.sort_by_key(|r| r.db_id);
        let mut sef_batches: Vec<Vec<usize>> = Vec::new();
        {
            let mut i = 0;
            while i < sef_runs.len() {
                let db_id = sef_runs[i].db_id;
                let mut batch = Vec::new();
                while i < sef_runs.len() && sef_runs[i].db_id == db_id {
                    batch.push(i);
                    i += 1;
                }
                sef_batches.push(batch);
            }
        }

        let num_workers = max_workers.min(sef_batches.len()).max(1);
        let mut shards: Vec<Vec<Vec<usize>>> = (0..num_workers).map(|_| Vec::new()).collect();
        for (i, batch) in sef_batches.into_iter().enumerate() {
            shards[i % num_workers].push(batch);
        }

        let sef_runs = Arc::new(sef_runs);
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let socket = socket_owned.clone();
                let ball_name = ball_name.clone();
                let db_paths = Arc::clone(&db_paths);
                let sef_runs = Arc::clone(&sef_runs);

                std::thread::spawn(move || -> Result<WorkerResult, String> {
                    let mut link = Link::connect(&socket, limits().query_timeout)?;
                    let rows_orientation = link.orientation;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(), rows: Vec::new(),
                    };
                    let mut needs_remount = true;
                    let mut current_mount_path: Option<String> = None;

                    for batch in shard {
                        if let Some(&first) = batch.first() {
                            let run = &sef_runs[first];
                            let mount_path = db_paths
                                .get(&run.db_id)
                                .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;
                            current_mount_path = Some(mount_path.to_string_lossy().to_string());
                            needs_remount = true;
                        }
                        for &idx in &batch {
                            let run = &sef_runs[idx];
                            let t0 = Instant::now();

                            // (Re)mount if needed (start of batch or after reconnect)
                            if needs_remount {
                                if let Some(ref mp) = current_mount_path {
                                    let mp = mp.clone();
                                    if let Err(e) = link.establish(&move |session, orientation| {
                                        send_reset(session)?;
                                        send_mount(session, &mp, orientation)
                                    }) {
                                        // The test still answers for itself:
                                        // a setup that could not be made to
                                        // hold is this test's error, not a
                                        // reason to drop the rest of the shard.
                                        judge(&ball_name, &run.name, Err(format!("session setup: {}", e)),
                                              &run.hash, &run.hashtype, t0.elapsed(), &mut result);
                                        continue;
                                    }
                                }
                                needs_remount = false;
                            }

                            let hash_mode = match run.hashtype.as_deref() {
                                Some("bhash") => HashMode::Byte,
                                _ => HashMode::String,
                            };
                            let exec = if run.sequential {
                                link.session().and_then(|session| {
                                    send_sequential_and_hash(session, &run.dql, rows_orientation, hash_mode)
                                })
                            } else {
                                link.session().and_then(|session| {
                                    send_query_and_hash_dispatch(session, &run.dql, rows_orientation, hash_mode)
                                })
                            };
                            let elapsed = t0.elapsed();

                            link.recover_if_poisoned(&exec);
                            if exec.as_ref().err().is_some_and(|e| is_transport_failure(e)) {
                                needs_remount = true;
                            }

                            judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, elapsed, &mut result);
                        }
                    }

                    Ok(result)
                })
            })
            .collect();

        collect(handles);
    }

    // ---- Phase 4b: DDL ----
    if !ddl_runs.is_empty() {
        let num_workers = max_workers.min(ddl_runs.len()).max(1);
        let mut shards: Vec<Vec<usize>> = (0..num_workers).map(|_| Vec::new()).collect();
        for i in 0..ddl_runs.len() {
            shards[i % num_workers].push(i);
        }

        let ddl_runs = Arc::new(ddl_runs);
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let socket = socket_owned.clone();
                let ball_name = ball_name.clone();
                let db_paths = Arc::clone(&db_paths);
                let ddl_map = Arc::clone(&ddl_map);
                let ddl_runs = Arc::clone(&ddl_runs);
                let tmpdir = Arc::clone(&tmpdir);

                std::thread::spawn(move || -> Result<WorkerResult, String> {
                    let mut link = Link::connect(&socket, limits().query_timeout)?;
                    let rows_orientation = link.orientation;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(), rows: Vec::new(),
                    };

                    for &idx in &shard {
                        let run = &ddl_runs[idx];
                        let has_ddl_files = ddl_map.contains_key(&run.code_id);

                        let work_dir = if has_ddl_files {
                            let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let dir = PathBuf::from(format!("/tmp/dql-ddl-{}-{}", std::process::id(), uid));
                            let _ = std::fs::remove_dir_all(&dir);
                            std::fs::create_dir_all(&dir).map_err(|e| format!("create ddl dir: {}", e))?;

                            copy_databases_to_work_dir(&dir, &db_paths)?;

                            for (filename, content) in &ddl_map[&run.code_id] {
                                let dest = dir.join("ddl").join(filename);
                                if let Some(parent) = dest.parent() {
                                    std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
                                }
                                std::fs::write(&dest, content).map_err(|e| format!("write ddl: {}", e))?;
                            }

                            Some(dir)
                        } else {
                            None
                        };

                        let cwd = match work_dir {
                            Some(ref dir) => dir.to_string_lossy().into_owned(),
                            None => tmpdir.to_string_lossy().into_owned(),
                        };
                        let mount_path = db_paths
                            .get(&run.db_id)
                            .ok_or_else(|| format!("no path for db_id {}", run.db_id))?
                            .to_string_lossy()
                            .into_owned();

                        let hash_mode = match run.hashtype.as_deref() {
                            Some("bhash") => HashMode::Byte,
                            _ => HashMode::String,
                        };
                        let t0 = Instant::now();

                        // Reset, CWD and mount are this test's required state.
                        // A failure to establish them is the TEST's error —
                        // it was once a `?` that took every remaining test in
                        // the shard out of the reported totals with it.
                        let setup = link.establish(&move |session, orientation| {
                            send_reset(session)?;
                            send_cwd(session, &cwd)?;
                            send_mount(session, &mount_path, orientation)
                        });
                        let exec = match setup {
                            Ok(()) => link.session().and_then(|session| {
                                send_sequential_and_hash(
                                    session, &run.dql, rows_orientation, hash_mode,
                                )
                            }),
                            Err(e) => Err(format!("session setup: {}", e)),
                        };
                        let elapsed = t0.elapsed();
                        link.recover_if_poisoned(&exec);
                        judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, elapsed, &mut result);

                        if let Some(ref dir) = work_dir {
                            let _ = std::fs::remove_dir_all(dir);
                        }
                    }

                    Ok(result)
                })
            })
            .collect();

        collect(handles);
    }

    // ---- Phase 4c: DML ----
    if !dml_runs.is_empty() {
        let num_workers = max_workers.min(dml_runs.len()).max(1);
        let mut shards: Vec<Vec<usize>> = (0..num_workers).map(|_| Vec::new()).collect();
        for i in 0..dml_runs.len() {
            shards[i % num_workers].push(i);
        }

        let dml_runs = Arc::new(dml_runs);
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let socket = socket_owned.clone();
                let ball_name = ball_name.clone();
                let db_paths = Arc::clone(&db_paths);
                let ddl_map = Arc::clone(&ddl_map);
                let init_map = Arc::clone(&init_map);
                let dml_runs = Arc::clone(&dml_runs);

                std::thread::spawn(move || -> Result<WorkerResult, String> {
                    let mut link = Link::connect(&socket, limits().query_timeout)?;
                    let rows_orientation = link.orientation;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(), rows: Vec::new(),
                    };

                    for &idx in &shard {
                        let run = &dml_runs[idx];
                        let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let isolate_dir = PathBuf::from(format!("/tmp/dql-dml-{}-{}", std::process::id(), uid));
                        let _ = std::fs::remove_dir_all(&isolate_dir);
                        std::fs::create_dir_all(&isolate_dir).map_err(|e| format!("create isolate dir: {}", e))?;

                        copy_databases_to_work_dir(&isolate_dir, &db_paths)?;

                        // Copy fixture database (DML mutates it)
                        let src_db = db_paths
                            .get(&run.db_id)
                            .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;
                        let dest_db = isolate_dir.join(&run.db_path);
                        if let Some(parent) = dest_db.parent() {
                            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
                        }
                        std::fs::copy(src_db, &dest_db).map_err(|e| format!("copy fixture: {}", e))?;

                        // Run init scripts
                        let mut mount_db = run.db_path.clone();
                        if let Some(inits) = init_map.get(&run.code_id) {
                            for (init_name, _filename, sql) in inits {
                                let init_db_path = isolate_dir.join(format!("{}.sqlite", init_name));
                                let init_conn = Connection::open(&init_db_path)
                                    .map_err(|e| format!("create init db: {}", e))?;
                                init_conn.execute_batch(sql)
                                    .map_err(|e| format!("init sql {}: {}", init_name, e))?;
                                // mount! is attach-only and rejects
                                // empty/headerless files. A schema-less init (e.g. a
                                // comment-only main.sql, as the companion
                                // imprint/define tests use) leaves a 0-byte
                                // db; force the SQLite header page out so the
                                // worker's mount! succeeds. Pinned by the
                                // companion ball.
                                init_conn.execute_batch("PRAGMA user_version = 0;")
                                    .map_err(|e| format!("init header {}: {}", init_name, e))?;
                            }
                            if inits.len() == 1 {
                                mount_db = format!("{}.sqlite", inits[0].0);
                            }
                        }

                        // Write DDL files if present
                        if let Some(ddls) = ddl_map.get(&run.code_id) {
                            for (filename, content) in ddls {
                                let dest = isolate_dir.join("ddl").join(filename);
                                if let Some(parent) = dest.parent() {
                                    std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
                                }
                                std::fs::write(&dest, content).map_err(|e| format!("write ddl: {}", e))?;
                            }
                        }

                        let hash_mode = match run.hashtype.as_deref() {
                            Some("bhash") => HashMode::Byte,
                            _ => HashMode::String,
                        };
                        let cwd = isolate_dir.to_string_lossy().into_owned();
                        let mount_db = mount_db.clone();
                        let t0 = Instant::now();

                        // Same law as DDL: the state this test needs is this
                        // test's to answer for, on a session that is replaced
                        // when its transport has failed.
                        let setup = link.establish(&move |session, orientation| {
                            send_reset(session)?;
                            send_cwd(session, &cwd)?;
                            send_mount(session, &mount_db, orientation)
                        });
                        let exec = match setup {
                            Ok(()) => link.session().and_then(|session| {
                                send_sequential_and_hash(
                                    session, &run.dql, rows_orientation, hash_mode,
                                )
                            }),
                            Err(e) => Err(format!("session setup: {}", e)),
                        };
                        let elapsed = t0.elapsed();
                        link.recover_if_poisoned(&exec);
                        judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, elapsed, &mut result);

                        let _ = std::fs::remove_dir_all(&isolate_dir);
                    }

                    Ok(result)
                })
            })
            .collect();

        collect(handles);
    }

    if let Some(db_path) = results_db {
        write_results_db(db_path, &all_rows)
            .unwrap_or_else(|e| eprintln!("dql-test-ball-runner: results-db: {}", e));
    }

    let total = passed + failed + errors + meh;
    eprintln!(
        "{}: Total:{} Pass:{} Fail:{} Error:{} Meh:{}",
        ball_name, total, passed, failed, errors, meh
    );

    let _ = std::fs::remove_dir_all(&*tmpdir);

    // Match pack-man semantics: exit code reflects infrastructure health,
    // not test findings. FAILs and ERRORs are reported results, not runner failures.
    Ok(!any_worker_error)
}

fn send_shutdown(socket_path: &Path) -> Result<(), String> {
    let stream = UnixStream::connect(socket_path).map_err(|e| format!("connect: {}", e))?;
    // Shutdown must not become the wait the query deadline just removed. A
    // server still working through a request the client abandoned answers
    // this handshake late or not at all, and the runner's last act would
    // otherwise be to block on it forever.
    if let Some(timeout) = limits().query_timeout {
        let _ = stream.set_read_timeout(Some(timeout));
        let _ = stream.set_write_timeout(Some(timeout));
    }
    let transport = SocketTransport::new(stream);
    let client = Client::new(transport);
    let mut session = match client
        .version(
            1_000_000,
            b"relay0".to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("version: {}", e.message))?
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => {
            return Err(format!("rejected: {}", String::from_utf8_lossy(&message)));
        }
    };
    let _ = session.shutdown();
    Ok(())
}

/// A server with ONE bounded worker: it answers the handshake, goes SILENT on
/// the first connection's query, and cannot service the next connection until
/// the first one is CLOSED.
///
/// That bound is the topology under test, not an incidental simplification.
/// `dql server` gives a connection a worker until the connection closes, so a
/// client that holds a poisoned connection open while opening its replacement
/// is queued behind itself: the replacement is connected but never serviced.
/// A stub that spawns a thread per connection cannot show this — every
/// replacement handshake succeeds there regardless of what the client still
/// holds open.
#[cfg(test)]
fn spawn_bounded_worker_server(socket: &Path) -> std::thread::JoinHandle<()> {
    use std::os::unix::net::UnixListener;

    let listener = UnixListener::bind(socket).expect("bind stub socket");
    std::thread::spawn(move || {
        // Serial, in the accepting thread: the one worker. Later connections
        // sit in the listen backlog — connected, as far as the client can
        // tell, and unserved.
        for (connection, stream) in listener.incoming().flatten().enumerate() {
            serve_stub_connection(connection, stream);
        }
    })
}

#[cfg(test)]
fn serve_stub_connection(connection: usize, mut stream: UnixStream) {
    use delightql_protocol::socket::{read_client_message, write_server_message};
    use delightql_protocol::{ClientMessage, ClientTerm, Dimension, ServerMessage, ServerTerm};

    let mut buf = Vec::new();
    loop {
        let Ok(message) = read_client_message(&mut stream, &mut buf) else {
            return;
        };
        let ClientMessage::Data(term) = message else {
            // Control ops (reset, cwd) are acknowledged so a caller's state
            // restoration can complete on the fresh session.
            let ok = ServerMessage::Control(delightql_protocol::ControlResult::Ok);
            if write_server_message(&mut stream, &ok).is_err() {
                return;
            }
            continue;
        };
        let reply = match term {
            ClientTerm::Version { .. } => ServerTerm::Version {
                max_message_size: 1_000_000,
                protocol_version: b"relay0".to_vec(),
                lease_ms: 300_000,
                orientations: vec![Orientation::Rows],
            },
            ClientTerm::Query { .. } if connection == 0 => {
                // The silence under test. A worker inside a query that
                // outlives the client's deadline answers nothing else on that
                // connection either — the reset the next test sends is read by
                // no one — so everything after this goes unanswered. The
                // connection stays OPEN: a closed socket is a different
                // failure, and the client's deadline is what must answer here.
                // These reads return only when the CLIENT closes the
                // connection, which is what frees this bounded worker.
                while read_client_message(&mut stream, &mut buf).is_ok() {}
                return;
            }
            ClientTerm::Query { .. } => ServerTerm::Header {
                handle: b"h1".to_vec(),
                dimensions: vec![Dimension {
                    position: 1,
                    name: b"k".to_vec(),
                    descriptor: b"INTEGER".to_vec(),
                }],
            },
            ClientTerm::Fetch { .. } => ServerTerm::End,
            _ => ServerTerm::Ok { count_hint: 0 },
        };
        if write_server_message(&mut stream, &ServerMessage::Data(reply)).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        compute_data_hash, judge, observed_baseline, query_error, HashObservation, WorkerResult,
    };

    fn empty(columns: usize) -> HashObservation {
        HashObservation {
            digest: compute_data_hash(&[]),
            empty_columns: Some(columns),
        }
    }

    fn worker_result() -> WorkerResult {
        WorkerResult {
            passed: 0,
            failed: 0,
            errors: 0,
            meh: 0,
            output: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// A link whose silent query has just answered as a transport failure,
    /// against a server with one bounded worker still owned by that
    /// connection. The elapsed wait is returned with it: it is the deadline,
    /// and what follows must not spend another.
    fn poisoned_link_against_a_bounded_worker(
        socket: &std::path::Path,
        deadline: Duration,
    ) -> (super::Link, Duration, String) {
        use super::{is_transport_failure, send_query_and_hash, spawn_bounded_worker_server, Link};

        let _ = std::fs::remove_file(socket);
        // The handle is dropped: the worker thread outlives the test by
        // design, blocked on an accept nothing will answer.
        let _server = spawn_bounded_worker_server(socket);

        let mut link = Link::connect(socket, Some(deadline)).expect("connect to stub");
        let orientation = link.orientation;

        let started = std::time::Instant::now();
        let first = send_query_and_hash(
            link.session().expect("a fresh link holds a session"),
            "k(*)",
            orientation,
        );
        let waited = started.elapsed();
        let message = first.expect_err("a silent server cannot produce a hash");
        assert!(
            is_transport_failure(&message),
            "a silent server is a transport failure, not a refusal: {message}"
        );
        assert!(
            waited < Duration::from_secs(5),
            "the deadline must bound the wait, waited {waited:?}"
        );

        (link, waited, message)
    }

    /// A silent request is reported ONCE and the shard keeps going, through
    /// the immediate road every phase loop takes after a query answers.
    ///
    /// The server's one worker is still owned by the poisoned connection, so
    /// the replacement is serviced only because `renew` drops that connection
    /// before opening it. Opening first queues the replacement behind the very
    /// connection it replaces, and it waits there for a second deadline: that
    /// is what the timing assertion catches.
    #[test]
    fn recovery_releases_the_bounded_worker_before_taking_a_replacement() {
        use super::send_query_and_hash;
        use std::path::PathBuf;

        let socket = PathBuf::from(format!(
            "/tmp/dql-runner-recovery-{}-{}.sock",
            std::process::id(),
            line!()
        ));
        let deadline = Duration::from_millis(300);
        let (mut link, waited, message) =
            poisoned_link_against_a_bounded_worker(&socket, deadline);

        let mut result = super::WorkerResult {
            passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(), rows: Vec::new(),
        };
        let outcome: Result<super::HashObservation, String> = Err(message);

        // The road all three phase loops take once a query has answered:
        // recover, record the row, run the next test.
        let recovery = std::time::Instant::now();
        link.recover_if_poisoned(&outcome);

        // Exactly one row for that test, and it is an error.
        judge("ball", "silent", outcome, &None, &None, waited, &mut result);
        assert_eq!(result.rows.len(), 1, "the timed-out test reports one row");
        assert_eq!(result.rows[0].status, "ERROR");
        assert_eq!(result.errors, 1);

        let orientation = link.orientation;
        let second = send_query_and_hash(
            link.session()
                .expect("recovery leaves a session to run the next test on"),
            "k(*)",
            orientation,
        );
        let recovered_in = recovery.elapsed();
        assert!(
            second.is_ok(),
            "the next test must run on the fresh session: {second:?}"
        );
        assert!(
            recovered_in < deadline,
            "the replacement must not wait on a second deadline, took {recovered_in:?}"
        );

        judge("ball", "after", second, &None, &None, Duration::from_millis(1), &mut result);
        assert_eq!(result.rows.len(), 2, "the following test reports its own row");
        assert_eq!(result.errors, 1, "recovery adds no second error");

        drop(link);
        let _ = std::fs::remove_file(&socket);
    }

    /// The same release, through the setup road the DDL and DML loops take.
    ///
    /// `establish` finds the poisoned session under it when the reset it
    /// sends goes unanswered, and the fresh session it takes must be one the
    /// bounded worker can actually serve. Carrying the poisoned session into
    /// the retry is how a `?` on the next reset takes every remaining test in
    /// the shard out of the reported totals.
    #[test]
    fn established_state_lands_on_a_session_the_bounded_worker_can_serve() {
        use super::send_query_and_hash;
        use std::path::PathBuf;

        let socket = PathBuf::from(format!(
            "/tmp/dql-runner-recovery-{}-{}.sock",
            std::process::id(),
            line!()
        ));
        let deadline = Duration::from_millis(300);
        let (mut link, _waited, _message) =
            poisoned_link_against_a_bounded_worker(&socket, deadline);

        // The reset probe spends one deadline on the poisoned session before
        // `establish` gives up on it; the reconnect and retry after that must
        // spend none.
        let started = std::time::Instant::now();
        link.establish(&|session, _| super::send_reset(session))
            .expect("required state is re-established on a fresh session");
        let established_in = started.elapsed();
        assert!(
            established_in < deadline * 2,
            "only the probe may wait on a deadline, took {established_in:?}"
        );

        let orientation = link.orientation;
        let next = send_query_and_hash(
            link.session()
                .expect("establish leaves a session to run the test on"),
            "k(*)",
            orientation,
        );
        assert!(
            next.is_ok(),
            "the test must run on the fresh session: {next:?}"
        );

        drop(link);
        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn query_refusal_preserves_its_structured_identity() {
        assert_eq!(
            query_error(
                b"delightql-error://semantic/setop/correspondence/ambiguous",
                b"more than one column corresponds"
            ),
            "query error: delightql-error://semantic/setop/correspondence/ambiguous: more than one column corresponds"
        );
    }

    #[test]
    fn shaped_empty_baseline_preserves_column_count() {
        assert_eq!(observed_baseline(&empty(7), Some("hash"), true), "EMPTY:7");
    }

    #[test]
    fn unshaped_empty_baseline_observes_the_raw_hash() {
        // A zero-row result hashes the same whatever its width, so a baseline
        // that did not ask for the shape is compared against that one value.
        assert_eq!(
            observed_baseline(&empty(7), Some("hash"), false),
            "5yt78PzT"
        );
    }

    #[test]
    fn empty_error_expectation_is_an_instrument_error() {
        let mut result = worker_result();
        judge(
            "ball",
            "empty-error-baseline",
            Err("an unrelated refusal".to_string()),
            &Some(" \n".to_string()),
            &Some("error".to_string()),
            Duration::from_secs(0),
            &mut result,
        );

        assert_eq!(result.errors, 1);
        assert_eq!(result.failed, 0);
        assert_eq!(result.passed, 0);
        assert_eq!(result.rows[0].status, "ERROR");
        assert_eq!(result.rows[0].detail, "empty refusal expectation");
    }

    #[test]
    fn nonempty_error_expectation_still_matches_the_refusal() {
        let mut result = worker_result();
        judge(
            "ball",
            "specific-error-baseline",
            Err("prefix: named refusal".to_string()),
            &Some("named refusal".to_string()),
            &Some("error".to_string()),
            Duration::from_secs(0),
            &mut result,
        );

        assert_eq!(result.passed, 1);
        assert_eq!(result.errors, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(result.rows[0].status, "PASS");
    }
}

fn main() {
    let args = Args::parse();
    LIMITS
        .set(Limits {
            workers: args.workers,
            query_timeout: (args.query_timeout > 0)
                .then(|| std::time::Duration::from_secs(args.query_timeout)),
        })
        .unwrap_or_else(|_| unreachable!("limits are set once, before any ball runs"));

    if args.balls.is_empty() {
        eprintln!("dql-test-ball-runner: no ball files specified");
        process::exit(1);
    }

    let mut all_ok = true;
    for ball_path in &args.balls {
        match run_ball(ball_path, &args.socket, args.results_db.as_deref()) {
            Ok(success) => {
                if !success {
                    all_ok = false;
                }
            }
            Err(e) => {
                eprintln!("dql-test-ball-runner: {}: {}", ball_path.display(), e);
                all_ok = false;
            }
        }
    }

    if args.shutdown {
        match send_shutdown(&args.socket) {
            Ok(()) => {}
            Err(e) => eprintln!("dql-test-ball-runner: shutdown error: {}", e),
        }
    }

    process::exit(if all_ok { 0 } else { 1 });
}
