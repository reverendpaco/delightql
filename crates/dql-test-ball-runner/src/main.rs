// dql-test-ball-runner — Run test balls (new schema: test_code/test_run)
//
// Reads a ball SQLite file, connects to a running `dql server`, executes
// each test_run with three-path dispatch (SEF/DDL/DML), and reports results.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;

use rusqlite::Connection;

use clap::Parser;
use sha2::{Digest, Sha256};
use tree_sitter::Language;

use delightql_protocol::socket::SocketTransport;
use delightql_protocol::{
    AgreedOrientation, Cell, Client, ControlResult, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult, cell_content_bytes, decode_cell_to_text,
};

extern "C" {
    fn tree_sitter_delightql_v2() -> Language;
}

#[derive(Parser)]
#[command(
    name = "dql-test-ball-runner",
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
}

#[derive(Clone, Copy, PartialEq)]
enum HashMode {
    String,
    Byte,
}

struct WorkerResult {
    passed: u32,
    failed: u32,
    errors: u32,
    meh: u32,
    output: Vec<String>,
}

// ---------------------------------------------------------------------------
// Protocol helpers
// ---------------------------------------------------------------------------

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
                    let text = decode_cell_to_text(bytes);
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
                cell_hasher.update(cell_content_bytes(bytes));
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
    Ok(compute_data_hash(&all_rows))
}

fn send_query_and_bhash(
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
    Ok(compute_byte_hash(&all_rows))
}

fn send_query_and_hash_dispatch(
    session: &mut Session<SocketTransport>,
    query_text: &str,
    rows_orientation: AgreedOrientation,
    mode: HashMode,
) -> Result<String, String> {
    match mode {
        HashMode::String => send_query_and_hash(session, query_text, rows_orientation),
        HashMode::Byte => send_query_and_bhash(session, query_text, rows_orientation),
    }
}

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

fn send_sequential_and_hash(
    session: &mut Session<SocketTransport>,
    dql: &str,
    rows_orientation: AgreedOrientation,
    mode: HashMode,
) -> Result<String, String> {
    let queries = split_queries(dql)?;
    let mut last_hash = String::new();
    for q in &queries {
        last_hash = send_query_and_hash_dispatch(session, q, rows_orientation, mode)?;
    }
    Ok(last_hash)
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

fn judge(
    ball_name: &str,
    test_name: &str,
    exec_result: Result<String, String>,
    expected_hash: &Option<String>,
    hashtype: &Option<String>,
    result: &mut WorkerResult,
) {
    let is_error_test = hashtype.as_deref() == Some("error");

    if is_error_test {
        match exec_result {
            Err(e) => {
                // Expected an error and got one — check optional pattern
                if let Some(pattern) = expected_hash.as_ref().filter(|p| !p.is_empty()) {
                    if e.contains(pattern.as_str()) {
                        result.output.push(format!("[PASS]\t{}\t{}\t", ball_name, test_name));
                        result.passed += 1;
                    } else {
                        result.output.push(format!(
                            "[FAIL]\t{}\t{}\terror expected to contain '{}' but got: {}",
                            ball_name, test_name, pattern, e
                        ));
                        result.failed += 1;
                    }
                } else {
                    result.output.push(format!("[PASS]\t{}\t{}\t", ball_name, test_name));
                    result.passed += 1;
                }
            }
            Ok(_) => {
                result.output.push(format!(
                    "[FAIL]\t{}\t{}\texpected error but query succeeded",
                    ball_name, test_name
                ));
                result.failed += 1;
            }
        }
        return;
    }

    match exec_result {
        Ok(actual_hex) => match expected_hash {
            None => {
                let actual_short = hex2hash(&actual_hex);
                result
                    .output
                    .push(format!("[MEH]\t{}\t{}\t{}", ball_name, test_name, actual_short));
                result.meh += 1;
            }
            Some(expected) => {
                let actual_short = if hashtype.as_deref() == Some("shash") {
                    actual_hex.clone()
                } else {
                    hex2hash(&actual_hex)
                };
                if *expected == actual_short {
                    result.output.push(format!("[PASS]\t{}\t{}\t", ball_name, test_name));
                    result.passed += 1;
                } else {
                    result.output.push(format!(
                        "[FAIL]\t{}\t{}\texpected:{} actual:{}",
                        ball_name, test_name, expected, actual_short
                    ));
                    result.failed += 1;
                }
            }
        },
        Err(e) => {
            let e_oneline = e.replace('\n', " ");
            result.output.push(format!("[ERROR]\t{}\t{}\t{}", ball_name, test_name, e_oneline));
            result.errors += 1;
        }
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

fn run_ball(ball_path: &Path, socket_path: &Path) -> Result<bool, String> {
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

    let max_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);

    let socket_owned = socket_path.to_owned();

    static ISOLATE_COUNTER: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut errors = 0u32;
    let mut meh = 0u32;
    let mut any_worker_error = false;

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
                    let (mut session, rows_orientation) = connect_session(&socket)?;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(),
                    };

                    for batch in shard {
                        if let Some(&first) = batch.first() {
                            let run = &sef_runs[first];
                            let mount_path = db_paths
                                .get(&run.db_id)
                                .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;
                            send_reset(&mut session)?;
                            send_mount(&mut session, &mount_path.to_string_lossy(), rows_orientation)?;
                        }
                        for &idx in &batch {
                            let run = &sef_runs[idx];
                            let hash_mode = match run.hashtype.as_deref() {
                                Some("bhash") => HashMode::Byte,
                                _ => HashMode::String,
                            };
                            let exec = if run.sequential {
                                send_sequential_and_hash(&mut session, &run.dql, rows_orientation, hash_mode)
                            } else {
                                send_query_and_hash_dispatch(&mut session, &run.dql, rows_orientation, hash_mode)
                            };
                            judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, &mut result);
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
                    let (mut session, rows_orientation) = connect_session(&socket)?;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(),
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

                        send_reset(&mut session)?;

                        let cwd = match work_dir {
                            Some(ref dir) => dir.to_string_lossy().into_owned(),
                            None => tmpdir.to_string_lossy().into_owned(),
                        };
                        send_cwd(&mut session, &cwd)?;

                        let mount_path = db_paths
                            .get(&run.db_id)
                            .ok_or_else(|| format!("no path for db_id {}", run.db_id))?;
                        send_mount(&mut session, &mount_path.to_string_lossy(), rows_orientation)?;

                        let hash_mode = match run.hashtype.as_deref() {
                            Some("bhash") => HashMode::Byte,
                            _ => HashMode::String,
                        };
                        let exec = send_sequential_and_hash(&mut session, &run.dql, rows_orientation, hash_mode);
                        judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, &mut result);

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
                    let (mut session, rows_orientation) = connect_session(&socket)?;
                    let mut result = WorkerResult {
                        passed: 0, failed: 0, errors: 0, meh: 0, output: Vec::new(),
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

                        send_reset(&mut session)?;
                        send_cwd(&mut session, &isolate_dir.to_string_lossy())?;
                        send_mount(&mut session, &mount_db, rows_orientation)?;

                        let hash_mode = match run.hashtype.as_deref() {
                            Some("bhash") => HashMode::Byte,
                            _ => HashMode::String,
                        };
                        let exec = send_sequential_and_hash(&mut session, &run.dql, rows_orientation, hash_mode);
                        judge(&ball_name, &run.name, exec, &run.hash, &run.hashtype, &mut result);

                        let _ = std::fs::remove_dir_all(&isolate_dir);
                    }

                    Ok(result)
                })
            })
            .collect();

        collect(handles);
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

fn main() {
    let args = Args::parse();

    if args.balls.is_empty() {
        eprintln!("dql-test-ball-runner: no ball files specified");
        process::exit(1);
    }

    let mut all_ok = true;
    for ball_path in &args.balls {
        match run_ball(ball_path, &args.socket) {
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
