// dql-pack-man — Thin relay protocol client for hash computation
//
// Connects to a running `dql server` over a Unix socket, sends a query,
// collects all result rows, and prints a SHA-256 data hash to stdout.
// The hash algorithm matches `dql query --to hash` (fingerprint.rs data_hash).

use std::io::{self, Read, Write as _};
use std::os::unix::net::UnixStream;
use std::path::{Display, Path, PathBuf};
use std::process;
use std::sync::Arc;

use rusqlite::Connection;

use clap::Parser;
use sha2::{Digest, Sha256};
use tree_sitter::Language;

use delightql_protocol::socket::SocketTransport;
use delightql_protocol::{
    AgreedOrientation, Cell, Client, ControlResult, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult, decode_cell_to_text,
};

extern "C" {
    fn tree_sitter_delightql_v2() -> Language;
}

#[derive(Parser)]
#[command(
    name = "dql-pack-man",
    about = "Thin relay client for DQL hash computation"
)]
struct Args {
    /// Unix socket path to connect to (not required for --extract)
    #[arg(long)]
    socket: Option<PathBuf>,

    /// Database to mount into "main" after reset (sends mount! query)
    #[arg(long)]
    db: Option<PathBuf>,

    /// Send Reset control op before mount/query
    #[arg(long)]
    reset: bool,

    /// Query string (if omitted, reads from stdin or --file)
    query: Option<String>,

    /// Read query from file
    #[arg(long, conflicts_with = "query")]
    file: Option<PathBuf>,

    /// Sequential mode: split multi-query source and send each query individually
    #[arg(long)]
    sequential: bool,

    /// Run tests from a SQLite test-case database
    #[arg(long, conflicts_with_all = ["query", "file", "sequential"])]
    test_case_db: Option<PathBuf>,

    /// Extract databases and DDL files from a test-case database
    #[arg(long, conflicts_with_all = ["query", "file", "sequential", "test_case_db"])]
    extract: Option<PathBuf>,

    /// Output directory for --extract mode
    #[arg(long, requires = "extract")]
    to_dir: Option<PathBuf>,

    /// Output mode (only "hash" is supported)
    #[arg(long, default_value = "hash")]
    to: String,

    /// Number of parallel workers for test-case-db mode (0 = available CPUs)
    #[arg(long, default_value = "0")]
    workers: usize,

    /// Send Shutdown control op to the server after tests complete
    #[arg(long)]
    shutdown: bool,

    /// Write structured results to a SQLite database
    #[arg(long)]
    results_db: Option<PathBuf>,

    /// Source fingerprint (hash of code + grammar + test inputs)
    #[arg(long)]
    source_fingerprint: Option<String>,
}

#[derive(Clone)]
enum TestOutcome {
    Pass,
    Fail { expected: String, actual: String },
    Error { message: String },
    Meh,
}

#[derive(Clone)]
struct TestCaseResult {
    file: String,
    outcome: TestOutcome,
    run_id: i64,
}

fn main() {
    let args = Args::parse();

    if args.to != "hash" {
        eprintln!("dql-pack-man: only --to hash is supported");
        process::exit(1);
    }

    // Extract mode: unpack databases and DDL files from a ball (no socket needed)
    if let Some(ref ball_path) = args.extract {
        let out_dir = args.to_dir.as_deref().unwrap_or_else(|| Path::new("."));
        match extract_ball(ball_path, out_dir) {
            Ok(()) => process::exit(0),
            Err(e) => {
                eprintln!("dql-pack-man: extract: {}", e);
                process::exit(1);
            }
        }
    }

    // All remaining modes require --socket
    let socket = match args.socket {
        Some(ref s) => s.clone(),
        None => {
            eprintln!("dql-pack-man: --socket is required");
            process::exit(1);
        }
    };

    // Test-case-db mode: batch runner
    if let Some(ref db_path) = args.test_case_db {
        let num_workers = if args.workers == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            args.workers
        };
        match run_test_case_db(db_path, &socket, args.shutdown, num_workers, args.results_db.as_deref(), args.source_fingerprint.as_deref()) {
            Ok(success) => process::exit(if success { 0 } else { 1 }),
            Err(e) => {
                eprintln!("dql-pack-man: {}", e);
                if args.shutdown {
                    let _ = send_shutdown(&socket);
                }
                process::exit(1);
            }
        }
    }

    // Normal single-query mode
    let query_text = if let Some(ref q) = args.query {
        q.clone()
    } else if let Some(ref path) = args.file {
        match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("dql-pack-man: failed to read {}: {}", path.display(), e);
                process::exit(1);
            }
        }
    } else {
        let mut buf = String::new();
        if let Err(e) = io::stdin().read_to_string(&mut buf) {
            eprintln!("dql-pack-man: failed to read stdin: {}", e);
            process::exit(1);
        }
        buf
    };

    let query_text = query_text.trim().to_string();
    if query_text.is_empty() {
        eprintln!("dql-pack-man: empty query");
        process::exit(1);
    }

    match run(
        &socket,
        &query_text,
        args.db.as_deref(),
        args.reset,
        args.sequential,
    ) {
        Ok(hash) => println!("{}", hash),
        Err(e) => {
            eprintln!("dql-pack-man: {}", e);
            process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Tree-sitter query splitting
// ---------------------------------------------------------------------------

/// Split source into individual query texts using tree-sitter CST boundaries.
/// Returns Err if parse errors are found or if top-level ddl_annotation nodes
/// are present (those need the CLI's sequential runner).
fn split_queries(source: &str) -> Result<Vec<String>, String> {
    let mut parser = tree_sitter::Parser::new();
    let language = unsafe { tree_sitter_delightql_v2() };
    parser
        .set_language(&language)
        .map_err(|e| format!("language: {e}"))?;

    let tree = parser
        .parse(source, None)
        .ok_or("tree-sitter parse failed")?;
    let root = tree.root_node();

    if root.has_error() {
        return Err(find_first_error(&root, source));
    }

    // Reject files with top-level ddl_annotation nodes — those need the CLI's
    // sequential runner which handles inline DDL extraction.
    let mut cursor = root.walk();
    let has_top_level_ddl = root
        .children(&mut cursor)
        .any(|c| c.kind() == "ddl_annotation");
    if has_top_level_ddl {
        return Err("contains top-level ddl_annotation (use CLI sequential mode)".into());
    }

    let mut cursor = root.walk();
    let queries: Vec<String> = root
        .children(&mut cursor)
        .filter(|c| c.kind() == "query")
        .map(|c| source[c.start_byte()..c.end_byte()].to_string())
        .collect();

    if queries.is_empty() {
        return Err("no queries found in source".into());
    }
    Ok(queries)
}

/// Walk the CST to find the first ERROR node and produce a useful message.
fn find_first_error(node: &tree_sitter::Node, source: &str) -> String {
    if node.kind() == "ERROR" || node.is_error() {
        let start = node.start_position();
        let snippet: String = source[node.start_byte()..node.end_byte()]
            .chars()
            .take(40)
            .collect();
        return format!(
            "syntax error at line {}:{}: {}",
            start.row + 1,
            start.column + 1,
            snippet
        );
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.has_error() {
            let msg = find_first_error(&child, source);
            if !msg.is_empty() {
                return msg;
            }
        }
    }
    "syntax error (unknown location)".into()
}

// ---------------------------------------------------------------------------
// Protocol helpers
// ---------------------------------------------------------------------------

/// Send a single query, fetch all rows, close the handle, return data hash.
fn send_query_and_hash(
    session: &mut Session<SocketTransport>,
    query_text: &str,
    rows_orientation: AgreedOrientation,
) -> Result<String, String> {
    let handle = match session
        .query(query_text.as_bytes().to_vec())
        .map_err(|e| format!("query: {}", e.message))?
    {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error { message, .. } => {
            return Err(format!(
                "query error: {}",
                String::from_utf8_lossy(&message)
            ));
        }
    };

    let mut all_rows: Vec<Vec<Cell>> = Vec::new();
    loop {
        match session
            .fetch(&handle, Projection::All, 10000, rows_orientation)
            .map_err(|e| format!("fetch: {}", e.message))?
        {
            FetchResponse::Data { cells } => {
                all_rows.extend(cells);
            }
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
    Ok(compute_data_hash(&all_rows))
}

// ---------------------------------------------------------------------------
// Test-case-db mode
// ---------------------------------------------------------------------------

struct DataDatabase {
    id: i64,
    filename: String,
}

struct BallRun {
    id: i64,
    dbid: i64,
    name: String,
}

struct TestCase {
    file: String,
    dql: String,
    hash: Option<String>,
    dbid: i64,
    should_fail: bool,
    isolate_dbs: Vec<IsolateDb>,
    ddl_files: Vec<DdlFile>,
    run_id: i64,
}

struct IsolateDb {
    name: String,
    source: String,
    setup_sql: Option<String>,
    fixture_dbid: Option<i64>,
}

struct DdlFile {
    filename: String,
    content: String,
}

/// Convert a hex SHA-256 string to the 8-char filename-safe base64 used by .hash baselines.
/// Matches the shell hex2hash function in test_template.sh.
fn hex2hash(hex: &str) -> String {
    // Decode hex pairs to bytes
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect();

    // Standard base64 encode
    let b64 = {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let triple = (b0 << 16) | (b1 << 8) | b2;
            out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
            out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(CHARS[(triple & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    };

    // Make filename-safe: / → _, + → -
    let safe: String = b64
        .chars()
        .map(|c| match c {
            '/' => '_',
            '+' => '-',
            _ => c,
        })
        .collect();

    // Take first 8 chars
    safe[..8.min(safe.len())].to_string()
}

fn send_reset(session: &mut Session<SocketTransport>) -> Result<(), String> {
    match session
        .reset()
        .map_err(|e| format!("reset: {}", e.message))?
    {
        ControlResult::Ok => {}
        ControlResult::Error { message } => {
            return Err(format!("reset: {}", message));
        }
    }
    Ok(())
}

fn send_cwd(session: &mut Session<SocketTransport>, path: &str) -> Result<(), String> {
    match session
        .cwd(path.to_string())
        .map_err(|e| format!("cwd: {}", e.message))?
    {
        ControlResult::Ok => {}
        ControlResult::Error { message } => {
            return Err(format!("cwd: {}", message));
        }
    }
    Ok(())
}

fn send_mount(
    session: &mut Session<SocketTransport>,
    db_filename: &str,
    rows_orientation: AgreedOrientation,
) -> Result<(), String> {
    let mount_query = format!("mount!(\"{}\",\"main\")", db_filename);
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

fn reset_and_mount(
    session: &mut Session<SocketTransport>,
    db_filename: &str,
    rows_orientation: AgreedOrientation,
) -> Result<(), String> {
    send_reset(session)?;
    send_mount(session, db_filename, rows_orientation)
}

/// Check if the data_database_contents table exists in the test-case db.
fn has_blob_table(conn: &Connection) -> bool {
    conn.prepare("SELECT 1 FROM data_database_contents LIMIT 0")
        .is_ok()
}

/// Extract embedded database blobs to temp files, returning dbid → temp path.
/// Decompresses zstd blobs and writes them to /tmp/dql-test-<dbid>.db.
fn extract_blobs(
    conn: &Connection,
    databases: &[DataDatabase],
) -> Result<std::collections::HashMap<i64, PathBuf>, String> {
    let mut map = std::collections::HashMap::new();
    for db in databases {
        let blob: Vec<u8> = conn
            .query_row(
                "SELECT blob FROM data_database_contents WHERE dbid = ?1",
                [db.id],
                |row| row.get(0),
            )
            .map_err(|e| format!("read blob for dbid {}: {}", db.id, e))?;

        let decompressed = zstd::decode_all(&blob[..])
            .map_err(|e| format!("decompress blob for dbid {}: {}", db.id, e))?;

        let tmp_path = PathBuf::from(format!("/tmp/dql-test-{}.db", db.id));
        let mut f = std::fs::File::create(&tmp_path)
            .map_err(|e| format!("create temp db {}: {}", tmp_path.display(), e))?;
        f.write_all(&decompressed)
            .map_err(|e| format!("write temp db {}: {}", tmp_path.display(), e))?;

        map.insert(db.id, tmp_path);
    }
    Ok(map)
}

/// Read the ball's `run` table (one row per fixture for SEF, one row for SES).
fn read_ball_runs(conn: &Connection) -> Result<Vec<BallRun>, String> {
    let has_run_table = conn.prepare("SELECT 1 FROM run LIMIT 0").is_ok();
    if !has_run_table {
        return Err("test ball uses old schema (missing run table). Run 'make' to regenerate.".into());
    }

    let mut stmt = conn
        .prepare("SELECT id, dbid, name FROM run ORDER BY id")
        .map_err(|e| format!("prepare run: {}", e))?;

    let rows = stmt
        .query_map([], |row| {
            Ok(BallRun {
                id: row.get(0)?,
                dbid: row.get(1)?,
                name: row.get(2)?,
            })
        })
        .map_err(|e| format!("query run: {}", e))?;

    let result: Result<Vec<_>, _> = rows.collect();
    result.map_err(|e| format!("read run: {}", e))
}

struct WorkerResult {
    passed: u32,
    failed: u32,
    errors: u32,
    meh: u32,
    output: Vec<String>,
    details: Vec<TestCaseResult>,
}

/// Open a fresh connection to the server and return a session.
fn connect_session(
    socket_path: &Path,
) -> Result<(Session<SocketTransport>, AgreedOrientation), String> {
    let stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect to {}: {}", socket_path.display(), e))?;
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

/// Run a shard of test cases on its own connection.
/// Cases should be sorted by dbid to minimize mount switches.
fn run_worker(
    socket_path: &Path,
    db_map: &std::collections::HashMap<i64, String>,
    mut cases: Vec<TestCase>,
) -> Result<WorkerResult, String> {
    // Sort by dbid to minimize reset/mount switches
    cases.sort_by_key(|tc| tc.dbid);

    let (mut session, rows_orientation) = connect_session(socket_path)?;

    let mut result = WorkerResult {
        passed: 0,
        failed: 0,
        errors: 0,
        meh: 0,
        output: Vec::new(),
        details: Vec::new(),
    };

    let mut current_dbid: Option<i64> = None;

    for tc in &cases {
        if current_dbid != Some(tc.dbid) {
            let mount_path = db_map
                .get(&tc.dbid)
                .ok_or_else(|| format!("unknown dbid {} in side_effect_free", tc.dbid))?;
            reset_and_mount(&mut session, mount_path, rows_orientation)?;
            current_dbid = Some(tc.dbid);
        }

        let label = &tc.file;

        if tc.should_fail {
            match send_query_and_hash(&mut session, &tc.dql, rows_orientation) {
                Err(_) => {
                    result.output.push(format!("  [PASS] {}", label));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Pass, run_id: tc.run_id });
                    result.passed += 1;
                }
                Ok(_) => {
                    result
                        .output
                        .push(format!("  [FAIL] {} (expected error, got success)", label));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Fail { expected: "error".into(), actual: "success".into() }, run_id: tc.run_id });
                    result.failed += 1;
                }
            }
        } else {
            match send_query_and_hash(&mut session, &tc.dql, rows_orientation) {
                Ok(actual_hex) => match &tc.hash {
                    None => {
                        result.output.push(format!("  [MEH]  {}", label));
                        result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Meh, run_id: tc.run_id });
                        result.meh += 1;
                    }
                    Some(expected) => {
                        let actual_short = hex2hash(&actual_hex);
                        if *expected == actual_short {
                            result.output.push(format!("  [PASS] {}", label));
                            result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Pass, run_id: tc.run_id });
                            result.passed += 1;
                        } else {
                            result.output.push(format!(
                                "  [FAIL] {} (expected {}, got {})",
                                label, expected, actual_short
                            ));
                            result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Fail { expected: expected.clone(), actual: actual_short }, run_id: tc.run_id });
                            result.failed += 1;
                        }
                    }
                },
                Err(e) => {
                    result.output.push(format!("  [ERROR] {} ({})", label, e));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Error { message: e }, run_id: tc.run_id });
                    result.errors += 1;
                }
            }
        }
    }

    Ok(result)
}

/// Provision an isolate directory for a .mut test case.
/// Creates temp dir, copies/creates DBs, writes DDL files.
/// Returns the isolate directory path.
fn provision_isolate(
    tc: &TestCase,
    blob_cache: &std::collections::HashMap<i64, PathBuf>,
    unique_id: u64,
) -> Result<PathBuf, String> {
    let pid = std::process::id();
    let isolate = PathBuf::from(format!("/tmp/dql-isolate-{}-{}", pid, unique_id));
    let _ = std::fs::remove_dir_all(&isolate);
    std::fs::create_dir_all(&isolate)
        .map_err(|e| format!("create isolate dir: {}", e))?;

    for idb in &tc.isolate_dbs {
        let dest = isolate.join(&idb.name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
        }
        match idb.source.as_str() {
            "fixture" => {
                let dbid = idb.fixture_dbid.ok_or("fixture isolate missing dbid")?;
                let cached = blob_cache
                    .get(&dbid)
                    .ok_or_else(|| format!("no cached blob for dbid {}", dbid))?;
                std::fs::copy(cached, &dest)
                    .map_err(|e| format!("copy fixture to {}: {}", dest.display(), e))?;
            }
            "setup" => {
                let sql = idb.setup_sql.as_ref().ok_or("setup isolate missing SQL")?;
                let conn = Connection::open(&dest)
                    .map_err(|e| format!("create isolate db {}: {}", dest.display(), e))?;
                conn.execute_batch("CREATE TABLE _dql_init(x); DROP TABLE _dql_init;")
                    .map_err(|e| format!("init isolate db: {}", e))?;
                conn.execute_batch(sql)
                    .map_err(|e| format!("setup isolate db: {}", e))?;
            }
            other => return Err(format!("unknown isolate source: {}", other)),
        }
    }

    for ddl in &tc.ddl_files {
        let dest = isolate.join(&ddl.filename);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
        }
        std::fs::write(&dest, &ddl.content)
            .map_err(|e| format!("write DDL {}: {}", dest.display(), e))?;
    }

    Ok(isolate)
}

/// Pre-decompress unique fixture blobs from the ball into a temp cache.
/// Returns dbid → cached file path.
fn pre_decompress_blobs(
    conn: &Connection,
    isolate_dbs: &[&IsolateDb],
) -> Result<std::collections::HashMap<i64, PathBuf>, String> {
    let mut cache = std::collections::HashMap::new();
    let mut seen = std::collections::HashSet::new();
    let pid = std::process::id();

    for idb in isolate_dbs {
        if idb.source != "fixture" {
            continue;
        }
        let dbid = match idb.fixture_dbid {
            Some(id) => id,
            None => continue,
        };
        if !seen.insert(dbid) {
            continue;
        }

        let blob: Vec<u8> = conn
            .query_row(
                "SELECT blob FROM data_database_contents WHERE dbid = ?1",
                [dbid],
                |row| row.get(0),
            )
            .map_err(|e| format!("read blob for dbid {}: {}", dbid, e))?;

        let decompressed = zstd::decode_all(&blob[..])
            .map_err(|e| format!("decompress blob for dbid {}: {}", dbid, e))?;

        let path = PathBuf::from(format!("/tmp/dql-blob-cache-{}-{}.db", pid, dbid));
        std::fs::write(&path, &decompressed)
            .map_err(|e| format!("write blob cache {}: {}", path.display(), e))?;

        cache.insert(dbid, path);
    }

    Ok(cache)
}

/// Determine mount path for an isolated test.
fn determine_isolate_mount_path(
    tc: &TestCase,
    db_map: &std::collections::HashMap<i64, String>,
) -> String {
    if tc.isolate_dbs.iter().any(|db| db.name == "main.db") {
        return "main.db".to_string();
    }
    db_map.get(&tc.dbid).cloned().unwrap_or_else(|| "main.db".to_string())
}

/// SES worker: each test gets a fresh reset_and_mount, then sends queries sequentially.
/// The hash of the LAST query's result is compared to the baseline.
fn run_ses_worker(
    socket_path: &Path,
    db_map: &std::collections::HashMap<i64, String>,
    cases: Vec<TestCase>,
    blob_cache: &std::collections::HashMap<i64, PathBuf>,
) -> Result<WorkerResult, String> {
    let (mut session, rows_orientation) = connect_session(socket_path)?;

    let mut result = WorkerResult {
        passed: 0,
        failed: 0,
        errors: 0,
        meh: 0,
        output: Vec::new(),
        details: Vec::new(),
    };

    static ISOLATE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    for tc in &cases {
        let isolate_dir = if !tc.isolate_dbs.is_empty() {
            let uid = ISOLATE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(provision_isolate(tc, blob_cache, uid)?)
        } else {
            None
        };

        // Reset session state
        send_reset(&mut session)?;

        if let Some(ref isolate) = isolate_dir {
            // Set CWD to isolate directory so mount!/consult! resolve relative paths
            send_cwd(&mut session, &isolate.to_string_lossy())?;
            let mount_path = determine_isolate_mount_path(tc, db_map);
            send_mount(&mut session, &mount_path, rows_orientation)?;
        } else {
            let mount_path = db_map
                .get(&tc.dbid)
                .ok_or_else(|| format!("unknown dbid {} in side_effectful_on_system", tc.dbid))?;
            send_mount(&mut session, mount_path, rows_orientation)?;
        }

        let label = &tc.file;

        // Split DQL into individual queries and send sequentially
        let exec_result = send_sequential_and_hash(&mut session, &tc.dql, rows_orientation);

        if tc.should_fail {
            match exec_result {
                Err(_) => {
                    result.output.push(format!("  [PASS] {}", label));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Pass, run_id: tc.run_id });
                    result.passed += 1;
                }
                Ok(_) => {
                    result
                        .output
                        .push(format!("  [FAIL] {} (expected error, got success)", label));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Fail { expected: "error".into(), actual: "success".into() }, run_id: tc.run_id });
                    result.failed += 1;
                }
            }
        } else {
            match exec_result {
                Ok(actual_hex) => match &tc.hash {
                    None => {
                        result.output.push(format!("  [MEH]  {}", label));
                        result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Meh, run_id: tc.run_id });
                        result.meh += 1;
                    }
                    Some(expected) => {
                        let actual_short = hex2hash(&actual_hex);
                        if *expected == actual_short {
                            result.output.push(format!("  [PASS] {}", label));
                            result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Pass, run_id: tc.run_id });
                            result.passed += 1;
                        } else {
                            result.output.push(format!(
                                "  [FAIL] {} (expected {}, got {})",
                                label, expected, actual_short
                            ));
                            result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Fail { expected: expected.clone(), actual: actual_short }, run_id: tc.run_id });
                            result.failed += 1;
                        }
                    }
                },
                Err(e) => {
                    result.output.push(format!("  [ERROR] {} ({})", label, e));
                    result.details.push(TestCaseResult { file: label.clone(), outcome: TestOutcome::Error { message: e }, run_id: tc.run_id });
                    result.errors += 1;
                }
            }
        }

        // Clean up isolate directory
        if let Some(ref isolate) = isolate_dir {
            let _ = std::fs::remove_dir_all(isolate);
        }
    }

    Ok(result)
}

/// Split DQL text into queries using tree-sitter, send each sequentially,
/// return the hash of the LAST query's result.
fn send_sequential_and_hash(
    session: &mut Session<SocketTransport>,
    dql: &str,
    rows_orientation: AgreedOrientation,
) -> Result<String, String> {
    let queries = split_queries(dql)?;
    let mut last_hash = String::new();
    for q in &queries {
        last_hash = send_query_and_hash(session, q, rows_orientation)?;
    }
    Ok(last_hash)
}

fn run_test_case_db(
    db_path: &Path,
    socket_path: &Path,
    shutdown_after: bool,
    num_workers: usize,
    results_db: Option<&Path>,
    source_fingerprint: Option<&str>,
) -> Result<bool, String> {
    let conn = Connection::open(db_path)
        .map_err(|e| format!("open test-case-db {}: {}", db_path.display(), e))?;

    // Auto-detect mode: check which test tables exist
    let has_sef = conn
        .prepare("SELECT 1 FROM side_effect_free LIMIT 0")
        .is_ok();
    let has_ses = conn
        .prepare("SELECT 1 FROM side_effectful_on_system LIMIT 0")
        .is_ok();

    if has_ses {
        return run_ses_test_case_db(&conn, db_path, socket_path, shutdown_after, num_workers, results_db, source_fingerprint);
    }
    if !has_sef {
        return Err("no test tables found (side_effect_free or side_effectful_on_system)".into());
    }

    // --- SEF mode (side-effect-free) ---

    // Read ball runs (one per fixture)
    let ball_runs = read_ball_runs(&conn)?;

    // Build run_id → dbid mapping
    let run_dbid_map: std::collections::HashMap<i64, i64> =
        ball_runs.iter().map(|r| (r.id, r.dbid)).collect();

    // Read databases
    let mut db_stmt = conn
        .prepare("SELECT id, filename FROM data_database ORDER BY id")
        .map_err(|e| format!("prepare data_database: {}", e))?;

    let databases: Vec<DataDatabase> = db_stmt
        .query_map([], |row| {
            Ok(DataDatabase {
                id: row.get(0)?,
                filename: row.get(1)?,
            })
        })
        .map_err(|e| format!("query data_database: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read data_database: {}", e))?;

    if databases.is_empty() {
        return Err("no databases in data_database table".into());
    }

    // Extract embedded blobs to temp files (if available), else fall back to filenames
    let use_blobs = has_blob_table(&conn);
    let blob_paths = if use_blobs {
        extract_blobs(&conn, &databases)?
    } else {
        std::collections::HashMap::new()
    };

    // Build dbid → mount path lookup
    let db_map: std::collections::HashMap<i64, String> = databases
        .iter()
        .map(|d| {
            if let Some(tmp) = blob_paths.get(&d.id) {
                (d.id, tmp.to_string_lossy().into_owned())
            } else {
                (d.id, d.filename.clone())
            }
        })
        .collect();

    // Read test cases (ordered by run_id for grouping)
    let mut tc_stmt = conn
        .prepare("SELECT file, dql, hash, run_id, should_fail FROM side_effect_free ORDER BY run_id")
        .map_err(|e| format!("prepare side_effect_free: {}", e))?;

    let cases: Vec<TestCase> = tc_stmt
        .query_map([], |row| {
            let run_id: i64 = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                run_id,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|e| format!("query side_effect_free: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read side_effect_free: {}", e))?
        .into_iter()
        .map(|(file, dql, hash, run_id, should_fail)| {
            let dbid = *run_dbid_map.get(&run_id).unwrap_or(&1);
            TestCase {
                file,
                dql,
                hash,
                dbid,
                should_fail: should_fail != 0,
                isolate_dbs: Vec::new(),
                ddl_files: Vec::new(),
                run_id,
            }
        })
        .collect();

    if cases.is_empty() {
        eprintln!("dql-pack-man: no test cases in {}", db_path.display());
        return Ok(true);
    }

    let result = run_sharded_workers(socket_path, &db_map, cases, num_workers, false, None)?;

    // Clean up temp files
    for tmp_path in blob_paths.values() {
        let _ = std::fs::remove_file(tmp_path);
    }

    print_summary(&result, db_path.display());

    if let Some(rdb) = results_db {
        let ball_name = db_path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        write_results(rdb, &ball_name, "sef", &result, source_fingerprint, &ball_runs);
    }

    // Send shutdown if requested (on a fresh connection after all workers done)
    if shutdown_after {
        send_shutdown_quiet(socket_path);
    }

    // Exit code reflects pack-man infrastructure health, not test findings.
    // FAILs and ERRORs are reported results, not pack-man failures.
    Ok(!result.any_worker_error)
}

// ---------------------------------------------------------------------------
// SES mode (side-effectful on system)
// ---------------------------------------------------------------------------

fn run_ses_test_case_db(
    conn: &Connection,
    db_path: &Path,
    socket_path: &Path,
    shutdown_after: bool,
    num_workers: usize,
    results_db: Option<&Path>,
    source_fingerprint: Option<&str>,
) -> Result<bool, String> {
    // Read ball runs
    let ball_runs = read_ball_runs(conn)?;

    // Read databases
    let mut db_stmt = conn
        .prepare("SELECT id, filename FROM data_database ORDER BY id")
        .map_err(|e| format!("prepare data_database: {}", e))?;

    let databases: Vec<DataDatabase> = db_stmt
        .query_map([], |row| {
            Ok(DataDatabase {
                id: row.get(0)?,
                filename: row.get(1)?,
            })
        })
        .map_err(|e| format!("query data_database: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read data_database: {}", e))?;

    if databases.is_empty() {
        return Err("no databases in data_database table".into());
    }

    // SES uses relative paths (databases/{name}.db) — server resolves from CWD.
    // No blob extraction needed; files are already on disk from --extract.
    let db_map: std::collections::HashMap<i64, String> = databases
        .iter()
        .map(|d| (d.id, d.filename.clone()))
        .collect();

    // Read test cases
    let mut tc_stmt = conn
        .prepare(
            "SELECT id, file, dql, hash, run_id, dbid, should_fail \
             FROM side_effectful_on_system ORDER BY id",
        )
        .map_err(|e| format!("prepare side_effectful_on_system: {}", e))?;

    let raw_cases: Vec<(i64, String, String, Option<String>, i64, i64, bool)> = tc_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6).unwrap_or(0) != 0,
            ))
        })
        .map_err(|e| format!("query side_effectful_on_system: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read side_effectful_on_system: {}", e))?;

    // Load test_isolate_database rows (if table exists)
    let has_isolate_table = conn
        .prepare("SELECT 1 FROM test_isolate_database LIMIT 0")
        .is_ok();

    let mut isolate_map: std::collections::HashMap<i64, Vec<IsolateDb>> =
        std::collections::HashMap::new();
    if has_isolate_table {
        let mut iso_stmt = conn
            .prepare(
                "SELECT test_id, name, source, setup_sql, fixture_dbid \
                 FROM test_isolate_database ORDER BY test_id, id",
            )
            .map_err(|e| format!("prepare test_isolate_database: {}", e))?;

        let iso_rows: Vec<(i64, String, String, Option<String>, Option<i64>)> = iso_stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .map_err(|e| format!("query test_isolate_database: {}", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("read test_isolate_database: {}", e))?;

        for (test_id, name, source, setup_sql, fixture_dbid) in iso_rows {
            isolate_map.entry(test_id).or_default().push(IsolateDb {
                name,
                source,
                setup_sql,
                fixture_dbid,
            });
        }
    }

    // Load DDL files per test for isolated tests
    let has_ddl_table = conn
        .prepare("SELECT 1 FROM side_effectful_on_system_ddl LIMIT 0")
        .is_ok();

    let mut ddl_map: std::collections::HashMap<i64, Vec<DdlFile>> =
        std::collections::HashMap::new();
    if has_ddl_table {
        let mut ddl_stmt = conn
            .prepare(
                "SELECT test_id, filename, content FROM side_effectful_on_system_ddl ORDER BY test_id",
            )
            .map_err(|e| format!("prepare ddl: {}", e))?;

        let ddl_rows: Vec<(i64, String, String)> = ddl_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(|e| format!("query ddl: {}", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("read ddl: {}", e))?;

        for (test_id, filename, content) in ddl_rows {
            ddl_map.entry(test_id).or_default().push(DdlFile {
                filename,
                content,
            });
        }
    }

    // Build TestCase structs with isolate data attached
    let cases: Vec<TestCase> = raw_cases
        .into_iter()
        .map(|(id, file, dql, hash, run_id, dbid, should_fail)| {
            let isolate_dbs = isolate_map.remove(&id).unwrap_or_default();
            let ddl_files = if !isolate_dbs.is_empty() {
                ddl_map.remove(&id).unwrap_or_default()
            } else {
                Vec::new()
            };
            TestCase {
                file,
                dql,
                hash,
                dbid,
                should_fail,
                isolate_dbs,
                ddl_files,
                run_id,
            }
        })
        .collect();

    if cases.is_empty() {
        eprintln!("dql-pack-man: no test cases in {}", db_path.display());
        return Ok(true);
    }

    // Count isolated tests
    let isolated_count = cases.iter().filter(|tc| !tc.isolate_dbs.is_empty()).count();
    eprintln!(
        "dql-pack-man: ses mode, {} test cases ({} isolated)",
        cases.len(),
        isolated_count
    );

    // Pre-decompress fixture blobs needed by isolated tests (dedup by dbid)
    let all_isolate_refs: Vec<&IsolateDb> = cases
        .iter()
        .flat_map(|tc| tc.isolate_dbs.iter())
        .collect();
    let blob_cache = if has_blob_table(conn) && !all_isolate_refs.is_empty() {
        pre_decompress_blobs(conn, &all_isolate_refs)?
    } else {
        std::collections::HashMap::new()
    };
    let blob_cache = Arc::new(blob_cache);

    // SES tests are side-effectful: each test needs reset_and_mount before execution.
    // We still shard across workers — each worker resets before every test.
    let result = run_sharded_workers(socket_path, &db_map, cases, num_workers, true, Some(&blob_cache))?;

    // Clean up blob cache
    for path in blob_cache.values() {
        let _ = std::fs::remove_file(path);
    }

    print_summary(&result, db_path.display());

    if let Some(rdb) = results_db {
        let ball_name = db_path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        write_results(rdb, &ball_name, "ses", &result, source_fingerprint, &ball_runs);
    }

    if shutdown_after {
        send_shutdown_quiet(socket_path);
    }

    // Exit code reflects pack-man infrastructure health, not test findings.
    Ok(!result.any_worker_error)
}

// ---------------------------------------------------------------------------
// Extract mode
// ---------------------------------------------------------------------------

fn extract_ball(ball_path: &Path, out_dir: &Path) -> Result<(), String> {
    let conn =
        Connection::open(ball_path).map_err(|e| format!("open {}: {}", ball_path.display(), e))?;

    // Extract databases
    let mut db_stmt = conn
        .prepare("SELECT id, filename FROM data_database ORDER BY id")
        .map_err(|e| format!("prepare data_database: {}", e))?;

    let databases: Vec<DataDatabase> = db_stmt
        .query_map([], |row| {
            Ok(DataDatabase {
                id: row.get(0)?,
                filename: row.get(1)?,
            })
        })
        .map_err(|e| format!("query data_database: {}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read data_database: {}", e))?;

    // Extract database blobs
    if has_blob_table(&conn) {
        for db in &databases {
            let blob: Vec<u8> = conn
                .query_row(
                    "SELECT blob FROM data_database_contents WHERE dbid = ?1",
                    [db.id],
                    |row| row.get(0),
                )
                .map_err(|e| format!("read blob for dbid {}: {}", db.id, e))?;

            let decompressed = zstd::decode_all(&blob[..])
                .map_err(|e| format!("decompress blob for dbid {}: {}", db.id, e))?;

            let dest = out_dir.join(&db.filename);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
            }
            let mut f = std::fs::File::create(&dest)
                .map_err(|e| format!("create {}: {}", dest.display(), e))?;
            f.write_all(&decompressed)
                .map_err(|e| format!("write {}: {}", dest.display(), e))?;

            eprintln!("  extracted {}", db.filename);
        }
    }

    // Extract DDL files (if ses ball)
    let has_ddl = conn
        .prepare("SELECT 1 FROM side_effectful_on_system_ddl LIMIT 0")
        .is_ok();

    if has_ddl {
        let mut ddl_stmt = conn
            .prepare("SELECT DISTINCT filename, content FROM side_effectful_on_system_ddl")
            .map_err(|e| format!("prepare ddl: {}", e))?;

        let ddl_files: Vec<(String, String)> = ddl_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| format!("query ddl: {}", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("read ddl: {}", e))?;

        for (filename, content) in &ddl_files {
            let dest = out_dir.join(filename);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
            }
            std::fs::write(&dest, content)
                .map_err(|e| format!("write {}: {}", dest.display(), e))?;
        }

        eprintln!("  extracted {} DDL files", ddl_files.len());
    }

    eprintln!("dql-pack-man: extraction complete → {}", out_dir.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared worker infrastructure
// ---------------------------------------------------------------------------

struct AggregateResult {
    passed: u32,
    failed: u32,
    errors: u32,
    meh: u32,
    any_worker_error: bool,
    all_results: Vec<TestCaseResult>,
}

fn run_sharded_workers(
    socket_path: &Path,
    db_map: &std::collections::HashMap<i64, String>,
    cases: Vec<TestCase>,
    num_workers: usize,
    sequential_queries: bool,
    blob_cache: Option<&Arc<std::collections::HashMap<i64, PathBuf>>>,
) -> Result<AggregateResult, String> {
    let num_workers = num_workers.min(cases.len()).max(1);
    let mut shards: Vec<Vec<TestCase>> = (0..num_workers).map(|_| Vec::new()).collect();
    for (i, tc) in cases.into_iter().enumerate() {
        shards[i % num_workers].push(tc);
    }

    eprintln!(
        "dql-pack-man: running with {} worker{}",
        num_workers,
        if num_workers == 1 { "" } else { "s" }
    );

    let db_map_arc = Arc::new(db_map.clone());
    let socket_owned = socket_path.to_owned();
    let blob_cache_arc = blob_cache.cloned().unwrap_or_else(|| Arc::new(std::collections::HashMap::new()));
    let handles: Vec<_> = shards
        .into_iter()
        .map(|shard| {
            let socket = socket_owned.clone();
            let db_map = Arc::clone(&db_map_arc);
            let cache = Arc::clone(&blob_cache_arc);
            let seq = sequential_queries;
            std::thread::spawn(move || {
                if seq {
                    run_ses_worker(&socket, &db_map, shard, &cache)
                } else {
                    run_worker(&socket, &db_map, shard)
                }
            })
        })
        .collect();

    let mut total = AggregateResult {
        passed: 0,
        failed: 0,
        errors: 0,
        meh: 0,
        any_worker_error: false,
        all_results: Vec::new(),
    };

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(result)) => {
                for line in &result.output {
                    println!("{}", line);
                }
                total.passed += result.passed;
                total.failed += result.failed;
                total.errors += result.errors;
                total.meh += result.meh;
                total.all_results.extend(result.details);
            }
            Ok(Err(e)) => {
                eprintln!("dql-pack-man: worker {} error: {}", i, e);
                total.any_worker_error = true;
            }
            Err(_) => {
                eprintln!("dql-pack-man: worker {} panicked", i);
                total.any_worker_error = true;
            }
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// Results persistence
// ---------------------------------------------------------------------------

fn write_results(
    db_path: &Path,
    ball_name: &str,
    ball_mode: &str,
    result: &AggregateResult,
    source_fingerprint: Option<&str>,
    ball_runs: &[BallRun],
) {
    let write = || -> Result<(), String> {
        let conn = Connection::open(db_path)
            .map_err(|e| format!("open results db {}: {}", db_path.display(), e))?;

        // Enable WAL mode and set busy timeout for concurrent writes from parallel make
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| format!("set pragmas: {}", e))?;

        conn.execute_batch(include_str!("results_schema.sql"))
            .map_err(|e| format!("create schema: {}", e))?;

        // VCS revision: try jj first, fall back to git
        let git_rev = std::process::Command::new("jj")
            .args(["log", "-r", "@", "--no-graph", "-T", "change_id"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| {
                let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                if s.is_empty() { None } else { Some(s) }
            })
            .or_else(|| {
                std::process::Command::new("git")
                    .args(["rev-parse", "--short", "HEAD"])
                    .output()
                    .ok()
                    .filter(|o| o.status.success())
                    .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            });

        // Binary hashes (first 16 hex chars of SHA-256)
        let pm_hash = hash_file_prefix(&std::env::current_exe().unwrap_or_default());
        let dql_hash = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("dql")))
            .and_then(|p| hash_file_prefix(&p));

        // Build run name map from ball runs
        let run_name_map: std::collections::HashMap<i64, &str> =
            ball_runs.iter().map(|r| (r.id, r.name.as_str())).collect();

        // Group results by ball run_id
        let mut by_run: std::collections::BTreeMap<i64, Vec<&TestCaseResult>> =
            std::collections::BTreeMap::new();
        for tc in &result.all_results {
            by_run.entry(tc.run_id).or_default().push(tc);
        }

        let mut run_stmt = conn
            .prepare(
                "INSERT INTO run (ball, ball_mode, source_fingerprint, git_rev, \
                 dql_binary_hash, pm_binary_hash, fixture, total, passed, failed, errors, meh) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )
            .map_err(|e| format!("prepare insert run: {}", e))?;

        let mut tc_stmt = conn
            .prepare(
                "INSERT INTO test_result (run_id, file, status, expected, actual, error_msg) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .map_err(|e| format!("prepare insert test_result: {}", e))?;

        // Create one results-DB run per ball run (one per fixture for SEF)
        for (ball_run_id, results) in &by_run {
            let fixture = run_name_map.get(ball_run_id).copied();

            // Compute per-run counts
            let mut passed = 0u32;
            let mut failed = 0u32;
            let mut errors = 0u32;
            let mut meh = 0u32;
            for tc in results {
                match &tc.outcome {
                    TestOutcome::Pass => passed += 1,
                    TestOutcome::Fail { .. } => failed += 1,
                    TestOutcome::Error { .. } => errors += 1,
                    TestOutcome::Meh => meh += 1,
                }
            }
            let total = passed + failed + errors + meh;

            run_stmt
                .execute(rusqlite::params![
                    ball_name,
                    ball_mode,
                    source_fingerprint,
                    git_rev,
                    dql_hash,
                    pm_hash,
                    fixture,
                    total,
                    passed,
                    failed,
                    errors,
                    meh,
                ])
                .map_err(|e| format!("insert run: {}", e))?;

            let db_run_id = conn.last_insert_rowid();

            for tc in results {
                let (status, expected, actual, error_msg) = match &tc.outcome {
                    TestOutcome::Pass => ("PASS", None, None, None),
                    TestOutcome::Fail { expected, actual } => {
                        ("FAIL", Some(expected.as_str()), Some(actual.as_str()), None)
                    }
                    TestOutcome::Error { message } => {
                        ("ERROR", None, None, Some(message.as_str()))
                    }
                    TestOutcome::Meh => ("MEH", None, None, None),
                };
                tc_stmt
                    .execute(rusqlite::params![
                        db_run_id, tc.file, status, expected, actual, error_msg
                    ])
                    .map_err(|e| format!("insert test_result: {}", e))?;
            }
        }

        Ok(())
    };

    if let Err(e) = write() {
        eprintln!("dql-pack-man: warning: failed to write results: {}", e);
    }
}

fn hash_file_prefix(path: &Path) -> Option<String> {
    let data = std::fs::read(path).ok()?;
    let hash = <Sha256 as Digest>::digest(&data);
    Some(format!("{:x}", hash).chars().take(16).collect())
}

fn print_summary(result: &AggregateResult, display: Display) {
    let total = result.passed + result.failed + result.errors + result.meh;
    println!();
    println!(
        "Total:{}\t Passed:{}\t Failed:{}\t Error:{}\t Meh:{}\t {}",
        total, result.passed, result.failed, result.errors, result.meh, display
    );
}

fn send_shutdown_quiet(socket_path: &Path) {
    match send_shutdown(socket_path) {
        Ok(()) => {
            eprintln!("dql-pack-man: shutdown sent");
        }
        Err(e) => {
            eprintln!("dql-pack-man: shutdown error: {}", e);
        }
    }
}

/// Send a standalone Shutdown control op to the server (for error-path cleanup).
fn send_shutdown(socket_path: &Path) -> Result<(), String> {
    let stream = UnixStream::connect(socket_path).map_err(|e| format!("connect: {}", e))?;
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

// ---------------------------------------------------------------------------
// Main run logic
// ---------------------------------------------------------------------------

fn run(
    socket_path: &PathBuf,
    query_text: &str,
    db: Option<&Path>,
    reset: bool,
    sequential: bool,
) -> Result<String, String> {
    let stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect to {}: {}", socket_path.display(), e))?;

    let transport = SocketTransport::new(stream);
    let client = Client::new(transport);

    // Version handshake
    let mut session = match client
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

    // Reset if requested
    if reset {
        match session
            .reset()
            .map_err(|e| format!("reset transport: {}", e.message))?
        {
            ControlResult::Ok => {}
            ControlResult::Error { message } => {
                return Err(format!("reset: {}", message));
            }
        }
    }

    // Mount database if specified
    if let Some(db_path) = db {
        let mount_query = format!("mount!(\"{}\",\"main\")", db_path.display());
        let handle = match session
            .query(mount_query.as_bytes().to_vec())
            .map_err(|e| format!("mount query: {}", e.message))?
        {
            QueryResponse::Header { handle, .. } => handle,
            QueryResponse::Error { message, .. } => {
                return Err(format!(
                    "mount error: {}",
                    String::from_utf8_lossy(&message)
                ));
            }
        };
        // Drain the mount result
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
    }

    // Execute query (or queries in sequential mode)
    if sequential {
        let queries = split_queries(query_text)?;
        let mut last_hash = String::new();
        for q in &queries {
            last_hash = send_query_and_hash(&mut session, q, rows_orientation)?;
        }
        Ok(last_hash)
    } else {
        send_query_and_hash(&mut session, query_text, rows_orientation)
    }
}

/// Replicate the data_hash algorithm from fingerprint.rs:
/// 1. Hash each row: for each cell, decode and hash value (or "NULL" for empty/None) + "|" separator
/// 2. Sort row hashes
/// 3. Hash "ROWS:" + sorted row hashes joined by "\n"
fn compute_data_hash(rows: &[Vec<Cell>]) -> String {
    let mut row_hashes: Vec<String> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut hasher = Sha256::new();
        for cell in row {
            match cell {
                Some(bytes) if !bytes.is_empty() => {
                    let text = decode_cell_to_text(bytes);
                    if text.is_empty() {
                        hasher.update(b"NULL");
                    } else {
                        hasher.update(text.as_bytes());
                    }
                }
                _ => {
                    // None or Some(empty) → NULL
                    hasher.update(b"NULL");
                }
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
