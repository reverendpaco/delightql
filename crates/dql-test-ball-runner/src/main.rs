// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// dql-test-ball-runner — Run test balls (new schema: test_code/test_run)
//
// Reads a ball SQLite file, connects to a running `dql server`, executes
// each test_run with three-path dispatch (SEF/DDL/DML), and reports results.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use clap::Parser;
use sha2::{Digest, Sha256};

use delightql_protocol::socket::SocketTransport;
use delightql_protocol::{
    AgreedOrientation, Cell, FetchResponse, Projection, QueryResponse, Session,
};

mod world;
use world::{Link, World};

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

    /// Seconds a test may take before the runner abandons it and records an
    /// error. It bounds both the wait for a silent server and the test's whole
    /// wall clock, because a query that never finishes has two shapes and only
    /// one of them is silence: an endless row stream answers every read on
    /// time and never ends. Zero waits forever. A test may declare its own
    /// limit (`timeout.txt`), which also moves it to a private server.
    #[arg(long, default_value_t = 30)]
    query_timeout: u64,

    /// The order tests run in: `ball` (the packed order, kinds grouped),
    /// `reverse`, or `shuffle:<seed>`.
    ///
    /// Outcomes may not depend on it. Every test establishes its own world,
    /// so the order is free to move — and an isolation defect is exactly what
    /// makes it stop being free. This is the knob the isolation contract
    /// checker turns.
    #[arg(long, default_value = "ball")]
    order: RunOrder,

    /// The dql binary, used to start a private server for a test that
    /// declares its own wall clock. Such a test is expected not to answer,
    /// and `dql server` gives a connection a worker until the connection
    /// closes — a worker looping inside a compile or an unfold is never
    /// returned to the pool, so it must be a worker nothing else needs and a
    /// process the runner can kill.
    #[arg(long)]
    dql: Option<PathBuf>,
}

/// The arrangement the ball's tests are run in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RunOrder {
    /// Packed order, kinds grouped: SEF, then DDL, then DML.
    Ball,
    /// That order, backwards.
    Reverse,
    /// That order, permuted by a named seed. Deterministic: the same seed
    /// names the same arrangement, so a disagreement can be re-run.
    Shuffle(u64),
}

impl std::str::FromStr for RunOrder {
    type Err = String;

    fn from_str(spelling: &str) -> Result<Self, Self::Err> {
        match spelling {
            "ball" => Ok(RunOrder::Ball),
            "reverse" => Ok(RunOrder::Reverse),
            "shuffle" => Ok(RunOrder::Shuffle(0)),
            other => match other.strip_prefix("shuffle:") {
                Some(seed) => seed
                    .parse()
                    .map(RunOrder::Shuffle)
                    .map_err(|_| format!("shuffle seed must be a number, got '{seed}'")),
                None => Err(format!(
                    "unknown order '{other}': expected ball, reverse, or shuffle[:<seed>]"
                )),
            },
        }
    }
}

impl RunOrder {
    /// Rearrange one already-grouped list of runs in place.
    fn arrange<T>(self, runs: &mut [T]) {
        match self {
            RunOrder::Ball => {}
            RunOrder::Reverse => runs.reverse(),
            RunOrder::Shuffle(seed) => {
                // A named permutation, not a random one: the checker that
                // finds a disagreement must be able to hand the seed back.
                // SplitMix64, inline — a dependency for four lines of
                // arithmetic would be the larger cost.
                let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut next = move || {
                    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                    let mut z = state;
                    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                    z ^ (z >> 31)
                };
                for i in (1..runs.len()).rev() {
                    runs.swap(i, (next() % (i as u64 + 1)) as usize);
                }
            }
        }
    }
}

/// The per-process limits the whole run reads, set once from the arguments.
///
/// A global because every worker thread and every reconnect needs them and
/// threading them through each phase would say nothing the name does not.
static LIMITS: std::sync::OnceLock<Limits> = std::sync::OnceLock::new();

#[derive(Clone)]
struct Limits {
    workers: usize,
    query_timeout: Option<std::time::Duration>,
    dql_binary: Option<PathBuf>,
    order: RunOrder,
}

fn limits() -> Limits {
    LIMITS
        .get()
        .expect("limits are set before any ball runs")
        .clone()
}

/// The wall clock a test runs under, and what to say when it expires.
///
/// A deadline, not a duration: it is set once when the test starts and every
/// wait the test performs afterwards reads the time that is LEFT. A per-read
/// duration would restart on every answered fetch, which is exactly the wait a
/// runaway unfold never exceeds.
#[derive(Clone, Copy)]
struct Deadline {
    at: Option<Instant>,
    budget: Duration,
}

impl Deadline {
    fn starting_now(budget: Option<Duration>) -> Self {
        Deadline {
            at: budget.map(|b| Instant::now() + b),
            budget: budget.unwrap_or_default(),
        }
    }

    /// The time left, or the expiry error. Also the read deadline to install:
    /// a socket that waits longer than the test is allowed to live turns a
    /// wall clock back into a per-read one.
    fn remaining(&self) -> Result<Option<Duration>, String> {
        match self.at {
            None => Ok(None),
            Some(at) => {
                let now = Instant::now();
                if now >= at {
                    Err(self.expired())
                } else {
                    Ok(Some(at - now))
                }
            }
        }
    }

    fn expired(&self) -> String {
        format!("timeout: no answer within {}s", self.budget.as_secs())
    }

    /// The transport's account of a failure, unless the clock had already run
    /// out — in which case the clock is the true account. The read deadline is
    /// set to the same budget, so a test abandoned mid-wait reports itself as
    /// a timeout rather than as an unavailable socket.
    fn attribute(&self, transport: String) -> String {
        match self.at {
            Some(at) if Instant::now() >= at => self.expired(),
            _ => transport,
        }
    }
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
    /// The `test_run` row this observation answers. A test name is not an
    /// execution identity — one test runs once per database it is linked
    /// to, and those runs publish the same name — so a consumer counting
    /// names cannot tell one execution omitted and another repeated from a
    /// complete run.
    run_id: i64,
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
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

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
    deadline: Deadline,
) -> Result<HashObservation, String> {
    deadline.remaining()?;
    let (handle, column_count) = match session
        .query(query_text.as_bytes().to_vec())
        .map_err(|e| deadline.attribute(format!("query: {}", e.message)))?
    {
        QueryResponse::Header { handle, dimensions } => (handle, dimensions.len()),
        QueryResponse::Error(error) => {
            return Err(query_error(error.identity(), error.message()));
        }
    };
    let mut all_rows: Vec<Vec<Cell>> = Vec::new();
    loop {
        // An unfold that never closes answers every fetch promptly and simply
        // never sends End. Nothing but the wall clock stops it, and nothing
        // but stopping it bounds the rows accumulating here.
        deadline.remaining()?;
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| deadline.attribute(format!("fetch: {}", e.message)))?
        {
            FetchResponse::Data { cells } => all_rows.extend(cells),
            FetchResponse::End => break,
            FetchResponse::Error(error) => {
                return Err(format!(
                    "fetch error: {}",
                    String::from_utf8_lossy(error.message())
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
    deadline: Deadline,
) -> Result<HashObservation, String> {
    deadline.remaining()?;
    let (handle, column_count) = match session
        .query(query_text.as_bytes().to_vec())
        .map_err(|e| deadline.attribute(format!("query: {}", e.message)))?
    {
        QueryResponse::Header { handle, dimensions } => (handle, dimensions.len()),
        QueryResponse::Error(error) => {
            return Err(query_error(error.identity(), error.message()));
        }
    };
    let mut all_rows: Vec<Vec<Cell>> = Vec::new();
    loop {
        // An unfold that never closes answers every fetch promptly and simply
        // never sends End. Nothing but the wall clock stops it, and nothing
        // but stopping it bounds the rows accumulating here.
        deadline.remaining()?;
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| deadline.attribute(format!("fetch: {}", e.message)))?
        {
            FetchResponse::Data { cells } => all_rows.extend(cells),
            FetchResponse::End => break,
            FetchResponse::Error(error) => {
                return Err(format!(
                    "fetch error: {}",
                    String::from_utf8_lossy(error.message())
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
    deadline: Deadline,
) -> Result<HashObservation, String> {
    match mode {
        HashMode::String => send_query_and_hash(session, query_text, rows_orientation, deadline),
        HashMode::Byte => send_query_and_bhash(session, query_text, rows_orientation, deadline),
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
    deadline: Deadline,
) -> Result<HashObservation, String> {
    let queries = split_queries(dql)?;
    let mut last = None;
    for q in &queries {
        // The clock belongs to the TEST, so the whole submission spends one
        // budget: a file whose setup is instant and whose last query never
        // returns must not get a fresh allowance at each step.
        last = Some(send_query_and_hash_dispatch(
            session,
            q,
            rows_orientation,
            mode,
            deadline,
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
    /// The wall clock this test declared for itself, if any. Present means
    /// ISOLATED: run alone, against a server of its own that gets killed.
    timeout_secs: Option<u64>,
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
    run_id: i64,
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
                    let actual_short = observed_baseline(actual_hex, hashtype.as_deref(), false);
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
                        (
                            "FAIL",
                            format!("expected:{} actual:{}", expected, actual_short),
                        )
                    }
                }
            },
            Err(e) => ("ERROR", e.clone()),
        }
    };

    // One test, one line: a refusal that quotes generated SQL carries the
    // newlines of that SQL, and a detail that spans lines stops being
    // tab-separated output.
    let detail = detail.replace('\n', " ");

    if detail.is_empty() {
        result.output.push(format!(
            "[{}]\t{}\t{}\t\t{}",
            status, ball_name, test_name, dur
        ));
    } else {
        result.output.push(format!(
            "[{}]\t{}\t{}\t{}\t{}",
            status, ball_name, test_name, detail, dur
        ));
    }

    result.rows.push(TestResultRow {
        status: status.to_string(),
        ball: ball_name.to_string(),
        run_id,
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

/// A `dql server` this runner owns outright, for one test.
///
/// Killable is the point. A test that declares a wall clock is a test that may
/// be looping inside a compile or an unfold when the clock runs out, and the
/// only way to stop that — and to give back the memory it has been taking at
/// hundreds of megabytes a second — is to end the process.
struct PrivateServer {
    child: process::Child,
    socket: PathBuf,
}

impl PrivateServer {
    fn start(dql: &Path) -> Result<Self, String> {
        let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let socket = PathBuf::from(format!(
            "/tmp/dql-isolated-{}-{}.sock",
            std::process::id(),
            uid
        ));
        let _ = std::fs::remove_file(&socket);
        let child = process::Command::new(dql)
            .arg("server")
            .arg("--workers")
            .arg("1")
            .arg("--socket")
            .arg(&socket)
            // A backstop only: this server is killed by name when its test
            // ends. PR_SET_PDEATHSIG covers a runner that dies outright.
            .arg("--idle-timeout")
            .arg("120")
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .spawn()
            .map_err(|e| format!("start private server ({}): {}", dql.display(), e))?;

        let mut server = PrivateServer { child, socket };
        let waited_from = Instant::now();
        while !server.socket.exists() {
            if waited_from.elapsed() > Duration::from_secs(15) {
                let socket = server.socket.display().to_string();
                server.stop();
                return Err(format!("private server never bound {}", socket));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(server)
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.socket);
    }
}

impl Drop for PrivateServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The filesystem and session state one test is given, and the lifetime of
/// both.
///
/// The directory belongs to the test: it is removed when the workspace is
/// dropped, so a road that forgets to clean up does not exist. The world is
/// what the link must establish before the test's first statement.
struct Workspace {
    dir: Option<PathBuf>,
    world: World,
}

impl Workspace {
    fn world(&self) -> &World {
        &self.world
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// What one test may see, decided by its KIND and by nothing else.
///
/// The one site that answers that question, for the shared server and for a
/// private one alike: a wall clock changes when a test is abandoned, never
/// what it can see.
fn prepare_workspace(
    run: &BallTestRun,
    db_paths: &std::collections::HashMap<i64, PathBuf>,
    ddl_map: &std::collections::HashMap<i64, Vec<(String, String)>>,
    init_map: &std::collections::HashMap<i64, Vec<(String, String, String)>>,
    ball_tmpdir: &Path,
) -> Result<Workspace, String> {
    let mount_path = db_paths
        .get(&run.db_id)
        .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;

    match run.kind.as_str() {
        // Side-effect-free: no files of its own, and no session directory —
        // a relative path it never writes is a path it never resolves.
        "sef" => Ok(Workspace {
            dir: None,
            world: World {
                cwd: None,
                mount: mount_path.to_string_lossy().into_owned(),
            },
        }),
        "ddl" => {
            let dir = prepare_ddl_workspace(run.code_id, ddl_map, db_paths)?;
            let cwd = dir
                .as_deref()
                .unwrap_or(ball_tmpdir)
                .to_string_lossy()
                .into_owned();
            Ok(Workspace {
                dir,
                world: World {
                    cwd: Some(cwd),
                    mount: mount_path.to_string_lossy().into_owned(),
                },
            })
        }
        "dml" => {
            let (dir, mount) = prepare_dml_workspace(run, ddl_map, init_map, db_paths)?;
            let cwd = dir.to_string_lossy().into_owned();
            Ok(Workspace {
                dir: Some(dir),
                world: World {
                    cwd: Some(cwd),
                    mount,
                },
            })
        }
        other => Err(format!("unknown test kind: {}", other)),
    }
}

/// Run one test's statements on a session whose world is already established.
///
/// Its only caller is a `Link::in_world` closure, which is what makes the
/// establishment unskippable.
fn execute(
    session: &mut Session<SocketTransport>,
    run: &BallTestRun,
    orientation: AgreedOrientation,
    deadline: Deadline,
) -> Result<HashObservation, String> {
    let mode = match run.hashtype.as_deref() {
        Some("bhash") => HashMode::Byte,
        _ => HashMode::String,
    };
    if run.sequential {
        send_sequential_and_hash(session, &run.dql, orientation, mode, deadline)
    } else {
        send_query_and_hash_dispatch(session, &run.dql, orientation, mode, deadline)
    }
}

/// Run one test that declared its own wall clock, alone, on a server of its
/// own, and end that server whatever the outcome.
fn run_isolated(
    run: &BallTestRun,
    db_paths: &std::collections::HashMap<i64, PathBuf>,
    ddl_map: &std::collections::HashMap<i64, Vec<(String, String)>>,
    init_map: &std::collections::HashMap<i64, Vec<(String, String, String)>>,
    ball_tmpdir: &Path,
    budget: Duration,
) -> Result<HashObservation, String> {
    let dql = limits().dql_binary.ok_or_else(|| {
        "this test declares a wall clock and needs a private server, but the \
         runner was not told where dql is (--dql)"
            .to_string()
    })?;

    let workspace = prepare_workspace(run, db_paths, ddl_map, init_map, ball_tmpdir)?;

    let mut server = PrivateServer::start(&dql)?;
    let mut link = Link::connect(&server.socket, Some(budget))?;
    // The clock starts at the FIRST request, not at spawn: the server's
    // startup is the harness's cost, not the test's.
    let deadline = Deadline::starting_now(Some(budget));
    let exec = link.in_world(workspace.world(), |session, orientation| {
        execute(session, run, orientation, deadline)
    });
    // The connection closes before the process is ended, in that order:
    // the server is mid-request and will never read the close, but a
    // half-open socket outliving the kill is one more thing to explain.
    drop(link);
    server.stop();
    exec
}

/// Distinguishes the private directories of tests running at the same time.
static ISOLATE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The filesystem a DDL test needs, or None when it brings no files of its own
/// and can read the ball's shared extraction.
///
/// Shared by the ordinary DDL phase and the isolated phase: a test must not
/// see a different working directory because of which phase ran it.
fn prepare_ddl_workspace(
    code_id: i64,
    ddl_map: &std::collections::HashMap<i64, Vec<(String, String)>>,
    db_paths: &std::collections::HashMap<i64, PathBuf>,
) -> Result<Option<PathBuf>, String> {
    let Some(files) = ddl_map.get(&code_id) else {
        return Ok(None);
    };
    let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = PathBuf::from(format!("/tmp/dql-ddl-{}-{}", std::process::id(), uid));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| format!("create ddl dir: {}", e))?;
    copy_databases_to_work_dir(&dir, db_paths)?;
    write_ddl_files(&dir, files)?;
    Ok(Some(dir))
}

fn write_ddl_files(dir: &Path, files: &[(String, String)]) -> Result<(), String> {
    for (filename, content) in files {
        let dest = dir.join("ddl").join(filename);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
        }
        std::fs::write(&dest, content).map_err(|e| format!("write ddl: {}", e))?;
    }
    Ok(())
}

/// The filesystem a DML test needs: its own directory, its own copy of every
/// database, its init databases built, and its ddl/ files written. Returns the
/// directory and the database name the session must mount.
fn prepare_dml_workspace(
    run: &BallTestRun,
    ddl_map: &std::collections::HashMap<i64, Vec<(String, String)>>,
    init_map: &std::collections::HashMap<i64, Vec<(String, String, String)>>,
    db_paths: &std::collections::HashMap<i64, PathBuf>,
) -> Result<(PathBuf, String), String> {
    let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let isolate_dir = PathBuf::from(format!("/tmp/dql-dml-{}-{}", std::process::id(), uid));
    let _ = std::fs::remove_dir_all(&isolate_dir);
    std::fs::create_dir_all(&isolate_dir).map_err(|e| format!("create isolate dir: {}", e))?;

    copy_databases_to_work_dir(&isolate_dir, db_paths)?;

    // Copy fixture database (DML mutates it)
    let src_db = db_paths
        .get(&run.db_id)
        .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;
    let dest_db = isolate_dir.join(&run.db_path);
    if let Some(parent) = dest_db.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
    }
    std::fs::copy(src_db, &dest_db).map_err(|e| format!("copy fixture: {}", e))?;

    let mut mount_db = run.db_path.clone();
    if let Some(inits) = init_map.get(&run.code_id) {
        for (init_name, _filename, sql) in inits {
            let init_db_path = isolate_dir.join(format!("{}.sqlite", init_name));
            let init_conn =
                Connection::open(&init_db_path).map_err(|e| format!("create init db: {}", e))?;
            init_conn
                .execute_batch(sql)
                .map_err(|e| format!("init sql {}: {}", init_name, e))?;
            // mount! is attach-only and rejects empty/headerless files. A
            // schema-less init (e.g. a comment-only main.sql, as the companion
            // imprint/define tests use) leaves a 0-byte db; force the SQLite
            // header page out so the worker's mount! succeeds. Pinned by the
            // companion ball.
            init_conn
                .execute_batch("PRAGMA user_version = 0;")
                .map_err(|e| format!("init header {}: {}", init_name, e))?;
        }
        if inits.len() == 1 {
            mount_db = format!("{}.sqlite", inits[0].0);
        }
    }

    if let Some(files) = ddl_map.get(&run.code_id) {
        write_ddl_files(&isolate_dir, files)?;
    }

    Ok((isolate_dir, mount_db))
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
            std::fs::copy(path, &dest).map_err(|e| format!("copy db {}: {}", dest.display(), e))?;
        }
    }
    Ok(())
}

fn write_results_db(path: &Path, rows: &[TestResultRow]) -> Result<(), String> {
    let conn =
        Connection::open(path).map_err(|e| format!("open results db {}: {}", path.display(), e))?;
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
            duration_ms REAL NOT NULL,
            run_id INTEGER NOT NULL DEFAULT -1
        )",
    )
    .map_err(|e| format!("create table: {}", e))?;

    // The shared results database outlives any one schema — reviews cite
    // row ranges in it going back months — so a database written before run
    // identity existed is migrated in place rather than refused. The column
    // is looked up rather than added-and-ignored: swallowing every ALTER
    // error would swallow the ones that matter too.
    let has_run_id = conn
        .prepare("SELECT 1 FROM pragma_table_info('test_result') WHERE name = 'run_id'")
        .and_then(|mut stmt| stmt.exists([]))
        .map_err(|e| format!("inspect results schema: {}", e))?;
    if !has_run_id {
        conn.execute_batch(
            "ALTER TABLE test_result ADD COLUMN run_id INTEGER NOT NULL DEFAULT -1",
        )
        .map_err(|e| format!("add run_id: {}", e))?;
    }

    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin transaction: {}", e))?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO test_result (status, ball, test_name, detail, duration_ms, run_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        ).map_err(|e| format!("prepare insert: {}", e))?;
        for row in rows {
            stmt.execute(rusqlite::params![
                row.status,
                row.ball,
                row.test_name,
                row.detail,
                row.duration_ms,
                row.run_id
            ])
            .map_err(|e| format!("insert: {}", e))?;
        }
    }
    tx.commit().map_err(|e| format!("commit: {}", e))?;
    Ok(())
}

fn run_ball(
    ball_path: &Path,
    socket_path: &Path,
    results_db: Option<&Path>,
) -> Result<bool, String> {
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
                    d.id, d.path, tr.hash, tr.hashtype, tc.timeout_secs \
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
                timeout_secs: row.get(10)?,
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
            let (code_id, filename, content) = row.map_err(|e| format!("read test_ddl: {}", e))?;
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

    // Phase 3: take the isolated tests out, and arrange the rest.
    //
    // A test that declared a wall clock is a test that may not answer, and a
    // worker it wedges is a worker the rest of the ball never gets back. It
    // therefore leaves the shared server entirely and runs last, on a server
    // of its own.
    //
    // Everything else runs on one shared server through one loop. There is no
    // per-kind loop any more: a kind decides what a test's workspace holds
    // (`prepare_workspace`), never whether that workspace is established.
    let mut shared_runs = Vec::new();
    let mut isolated_runs = Vec::new();

    for run in all_runs {
        if !matches!(run.kind.as_str(), "sef" | "ddl" | "dml") {
            return Err(format!("unknown test kind: {}", run.kind));
        }
        if run.timeout_secs.is_some() {
            isolated_runs.push(run);
        } else {
            shared_runs.push(run);
        }
    }

    // The packed order with kinds grouped — SEF, then DDL, then DML — is the
    // arrangement the corpus has always been read in, so it stays the
    // default. It is a default and not a requirement: outcomes may not depend
    // on it, and `--order` is how that is checked.
    let kind_rank = |kind: &str| match kind {
        "sef" => 0,
        "ddl" => 1,
        _ => 2,
    };
    shared_runs.sort_by_key(|r| (kind_rank(&r.kind), r.run_id));
    isolated_runs.sort_by_key(|r| (kind_rank(&r.kind), r.run_id));
    let order = limits().order;
    order.arrange(&mut shared_runs);
    order.arrange(&mut isolated_runs);

    // Phase 4: run them.
    let db_paths = Arc::new(db_paths);
    let ddl_map = Arc::new(ddl_map);
    let init_map = Arc::new(init_map);
    let tmpdir = Arc::new(tmpdir);

    let max_workers = limits().workers.max(1);

    let socket_owned = socket_path.to_owned();

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

    // ---- Phase 4a: the shared server ----
    if !shared_runs.is_empty() {
        let num_workers = max_workers.min(shared_runs.len()).max(1);
        let mut shards: Vec<Vec<usize>> = (0..num_workers).map(|_| Vec::new()).collect();
        for i in 0..shared_runs.len() {
            shards[i % num_workers].push(i);
        }

        let shared_runs = Arc::new(shared_runs);
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let socket = socket_owned.clone();
                let ball_name = ball_name.clone();
                let db_paths = Arc::clone(&db_paths);
                let ddl_map = Arc::clone(&ddl_map);
                let init_map = Arc::clone(&init_map);
                let shared_runs = Arc::clone(&shared_runs);
                let tmpdir = Arc::clone(&tmpdir);

                std::thread::spawn(move || -> Result<WorkerResult, String> {
                    let mut link = Link::connect(&socket, limits().query_timeout)?;
                    let mut result = WorkerResult {
                        passed: 0,
                        failed: 0,
                        errors: 0,
                        meh: 0,
                        output: Vec::new(),
                        rows: Vec::new(),
                    };

                    for &idx in &shard {
                        let run = &shared_runs[idx];
                        let t0 = Instant::now();

                        // The workspace lives exactly as long as the test:
                        // built here, dropped at the end of this arm. The
                        // world inside it is established by `in_world` and
                        // cannot be inherited by the next test.
                        let exec =
                            match prepare_workspace(run, &db_paths, &ddl_map, &init_map, &tmpdir) {
                                Ok(workspace) => {
                                    let deadline = Deadline::starting_now(limits().query_timeout);
                                    link.in_world(workspace.world(), |session, orientation| {
                                        execute(session, run, orientation, deadline)
                                    })
                                }
                                // A workspace that could not be built is this
                                // test's error, not a reason to drop the rest of
                                // the shard.
                                Err(e) => Err(format!("workspace: {}", e)),
                            };

                        judge(
                            &ball_name,
                            run.run_id,
                            &run.name,
                            exec,
                            &run.hash,
                            &run.hashtype,
                            t0.elapsed(),
                            &mut result,
                        );
                    }

                    Ok(result)
                })
            })
            .collect();

        collect(handles);
    }

    // ---- Phase 4b: isolated ----
    // Last, and each on a server of its own. These are the tests that declared
    // they might not answer; nothing that expects an answer is still waiting on
    // a worker when they run.
    if !isolated_runs.is_empty() {
        let num_workers = max_workers.min(isolated_runs.len()).max(1);
        let mut shards: Vec<Vec<usize>> = (0..num_workers).map(|_| Vec::new()).collect();
        for i in 0..isolated_runs.len() {
            shards[i % num_workers].push(i);
        }

        let isolated_runs = Arc::new(isolated_runs);
        let handles: Vec<_> = shards
            .into_iter()
            .map(|shard| {
                let ball_name = ball_name.clone();
                let db_paths = Arc::clone(&db_paths);
                let ddl_map = Arc::clone(&ddl_map);
                let init_map = Arc::clone(&init_map);
                let isolated_runs = Arc::clone(&isolated_runs);
                let tmpdir = Arc::clone(&tmpdir);

                std::thread::spawn(move || -> Result<WorkerResult, String> {
                    let mut result = WorkerResult {
                        passed: 0,
                        failed: 0,
                        errors: 0,
                        meh: 0,
                        output: Vec::new(),
                        rows: Vec::new(),
                    };
                    for &idx in &shard {
                        let run = &isolated_runs[idx];
                        let budget = Duration::from_secs(
                            run.timeout_secs
                                .expect("isolated runs declare a wall clock"),
                        );
                        let t0 = Instant::now();
                        let exec =
                            run_isolated(run, &db_paths, &ddl_map, &init_map, &tmpdir, budget);
                        judge(
                            &ball_name,
                            run.run_id,
                            &run.name,
                            exec,
                            &run.hash,
                            &run.hashtype,
                            t0.elapsed(),
                            &mut result,
                        );
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

/// A server with ONE bounded worker: it answers the handshake, answers
/// `answers_before_silence` queries on the first connection, then goes SILENT
/// on that connection and cannot service the next connection until the first
/// one is CLOSED.
///
/// That bound is the topology under test, not an incidental simplification.
/// `dql server` gives a connection a worker until the connection closes, so a
/// client that holds a poisoned connection open while opening its replacement
/// is queued behind itself: the replacement is connected but never serviced.
/// A stub that spawns a thread per connection cannot show this — every
/// replacement handshake succeeds there regardless of what the client still
/// holds open.
///
/// The allowance is what decides WHICH request meets the silence: zero puts
/// it on the mount a world is established with, one puts it on the test's own
/// query. Both roads must release the bounded worker.
#[cfg(test)]
fn spawn_bounded_worker_server(
    socket: &Path,
    answers_before_silence: usize,
) -> std::thread::JoinHandle<()> {
    use std::os::unix::net::UnixListener;

    let listener = UnixListener::bind(socket).expect("bind stub socket");
    std::thread::spawn(move || {
        // Serial, in the accepting thread: the one worker. Later connections
        // sit in the listen backlog — connected, as far as the client can
        // tell, and unserved.
        for (connection, stream) in listener.incoming().flatten().enumerate() {
            serve_stub_connection(connection, stream, answers_before_silence);
        }
    })
}

#[cfg(test)]
fn serve_stub_connection(
    connection: usize,
    mut stream: std::os::unix::net::UnixStream,
    answers_before_silence: usize,
) {
    use delightql_protocol::socket::{read_client_message, write_server_message};
    use delightql_protocol::{
        ClientMessage, ClientTerm, Dimension, Orientation, ServerMessage, ServerTerm,
    };

    let mut buf = Vec::new();
    let mut answered = 0usize;
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
            ClientTerm::Query { .. } if connection == 0 && answered >= answers_before_silence => {
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
            ClientTerm::Query { .. } => {
                answered += 1;
                ServerTerm::Header {
                    handle: b"h1".to_vec(),
                    dimensions: vec![Dimension {
                        position: 1,
                        name: b"k".to_vec(),
                        descriptor: b"INTEGER".to_vec(),
                    }],
                }
            }
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
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use super::{
        compute_data_hash, judge, observed_baseline, query_error, send_query_and_hash, Deadline,
        HashObservation, Link, RunOrder, WorkerResult, World,
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

    fn a_world() -> World {
        World {
            cwd: None,
            mount: "stub.sqlite".to_string(),
        }
    }

    fn stub_socket(line: u32) -> PathBuf {
        PathBuf::from(format!(
            "/tmp/dql-runner-recovery-{}-{}.sock",
            std::process::id(),
            line
        ))
    }

    /// One test on a link, through the one road a test has.
    fn run_test_on(link: &mut Link) -> Result<HashObservation, String> {
        link.in_world(&a_world(), |session, orientation| {
            send_query_and_hash(session, "k(*)", orientation, Deadline::starting_now(None))
        })
    }

    /// A silent TEST is reported once and the shard keeps going.
    ///
    /// The server's one worker is still owned by the poisoned connection, so
    /// the replacement is serviced only because the recovery drops that
    /// connection before opening it. Opening first queues the replacement
    /// behind the very connection it replaces, and it waits there for a second
    /// deadline: that is what the timing assertion catches.
    #[test]
    fn a_silent_test_releases_the_bounded_worker_before_the_next_one_runs() {
        let socket = stub_socket(line!());
        let deadline = Duration::from_millis(300);
        let _ = std::fs::remove_file(&socket);
        // The handle is dropped: the worker thread outlives the test by
        // design, blocked on an accept nothing will answer.
        // One answer allowance: the world is established, the TEST goes silent.
        let _server = super::spawn_bounded_worker_server(&socket, 1);

        let mut link = Link::connect(&socket, Some(deadline)).expect("connect to stub");
        let mut result = worker_result();

        let started = Instant::now();
        let silent = run_test_on(&mut link);
        let waited = started.elapsed();
        let message = silent
            .as_ref()
            .err()
            .expect("a silent server cannot produce a hash")
            .clone();
        assert!(
            crate::world::is_transport_failure(&message),
            "a silent server is a transport failure, not a refusal: {message}"
        );
        assert!(
            waited < Duration::from_secs(5),
            "the deadline must bound the wait, waited {waited:?}"
        );

        // Exactly one row for that test, and it is an error.
        judge("ball", 1, "silent", silent, &None, &None, waited, &mut result);
        assert_eq!(result.rows.len(), 1, "the timed-out test reports one row");
        assert_eq!(result.rows[0].status, "ERROR");
        assert_eq!(result.errors, 1);

        // The next test, through the same road, on the replacement.
        let recovery = Instant::now();
        let second = run_test_on(&mut link);
        let recovered_in = recovery.elapsed();
        assert!(
            second.is_ok(),
            "the next test must run on the fresh session: {second:?}"
        );
        assert!(
            recovered_in < deadline,
            "the replacement must not wait on a second deadline, took {recovered_in:?}"
        );

        judge(
            "ball",
            2,
            "after",
            second,
            &None,
            &None,
            Duration::from_millis(1),
            &mut result,
        );
        assert_eq!(
            result.rows.len(),
            2,
            "the following test reports its own row"
        );
        assert_eq!(result.errors, 1, "recovery adds no second error");

        drop(link);
        let _ = std::fs::remove_file(&socket);
    }

    /// The same release, through the ESTABLISHMENT road.
    ///
    /// The mount a world is established with is the silent request here, so
    /// `in_world` finds the poisoned session under it before the test runs at
    /// all. The fresh session it takes must be one the bounded worker can
    /// actually serve: carrying the poisoned session into the retry is how a
    /// `?` on the next setup took every remaining test in the shard out of the
    /// reported totals.
    #[test]
    fn an_unestablishable_world_lands_on_a_session_the_bounded_worker_can_serve() {
        let socket = stub_socket(line!());
        let deadline = Duration::from_millis(300);
        let _ = std::fs::remove_file(&socket);
        // No answer allowance: the establishment's own mount meets the silence.
        let _server = super::spawn_bounded_worker_server(&socket, 0);

        let mut link = Link::connect(&socket, Some(deadline)).expect("connect to stub");

        // The setup probe spends one deadline on the poisoned session before
        // the retry gives up on it; the reconnect and retry after that spend
        // none, and the test itself then runs.
        let started = Instant::now();
        let outcome = run_test_on(&mut link);
        let established_in = started.elapsed();
        assert!(
            outcome.is_ok(),
            "the test runs once its world is established on a fresh session: {outcome:?}"
        );
        assert!(
            established_in < deadline * 2,
            "only the probe may wait on a deadline, took {established_in:?}"
        );

        drop(link);
        let _ = std::fs::remove_file(&socket);
    }

    /// A named shuffle is a NAME: the same seed is the same arrangement, so a
    /// disagreement the checker finds can be handed back and re-run.
    #[test]
    fn a_shuffle_seed_names_one_arrangement() {
        let source: Vec<u32> = (0..64).collect();

        let mut once = source.clone();
        RunOrder::Shuffle(7).arrange(&mut once);
        let mut again = source.clone();
        RunOrder::Shuffle(7).arrange(&mut again);
        assert_eq!(once, again, "one seed, one arrangement");

        let mut other = source.clone();
        RunOrder::Shuffle(8).arrange(&mut other);
        assert_ne!(once, other, "a different seed is a different arrangement");

        let mut sorted = once.clone();
        sorted.sort();
        assert_eq!(sorted, source, "a shuffle is a permutation, not a filter");
    }

    #[test]
    fn an_order_spelling_that_is_not_one_refuses() {
        assert_eq!("ball".parse(), Ok(RunOrder::Ball));
        assert_eq!("reverse".parse(), Ok(RunOrder::Reverse));
        assert_eq!("shuffle".parse(), Ok(RunOrder::Shuffle(0)));
        assert_eq!("shuffle:12".parse(), Ok(RunOrder::Shuffle(12)));
        assert!("sideways".parse::<RunOrder>().is_err());
        assert!("shuffle:soon".parse::<RunOrder>().is_err());
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
            1,
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
            1,
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
            dql_binary: args.dql.clone(),
            order: args.order,
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
        match world::send_shutdown(&args.socket, limits().query_timeout) {
            Ok(()) => {}
            Err(e) => eprintln!("dql-test-ball-runner: shutdown error: {}", e),
        }
    }

    process::exit(if all_ok { 0 } else { 1 });
}
