// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Committed runs survive a later failure: `;` opens a new transaction
//! context, `,` composes one (receipt-algebra law, THE IMPLICIT RUN and
//! TERMINAL DISPOSITIONS AND ASSERTION).
//!
//! Why an integration test and not a ball: a corpus cell stops at its first
//! error and each cell gets a fresh database, so a cell that pins the
//! expected error passes whether or not the earlier run's write survived.
//! These tests compile and execute the authored DQL through a live host
//! session over a SQLite FILE, then read the file through an independent
//! connection, and keep using the same session after the failure.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use delightql_cli::connection::{open_handle, SessionProfile};
use delightql_core::api::{ApiError, DqlHandle, DqlSession, SessionHooks};

const INIT: &str = "CREATE TABLE log(t INTEGER);\n\
                    CREATE TABLE uniq(t INTEGER PRIMARY KEY);\n\
                    INSERT INTO uniq VALUES (5);";

/// Query-local wrappers: a `;` arm that pipes must be wrapped, since a
/// continuation after a union applies to the whole union.
const ONE: &str = "one!(*) : _(t @ 1) |> insert!(log(*))(*)\n";
const TWO: &str = "two!(*) : _(t @ 2) |> insert!(log(*))(*)\n";
const THREE: &str = "three!(*) : _(t @ 3) |> insert!(log(*))(*)\n";
const ABORT: &str = "abort!(\"stop\", _(x @ 1))(*)";

type Rows = Vec<Vec<Option<String>>>;
type Shipped = Arc<Mutex<Vec<Rows>>>;

/// A SQLite file holding `INIT`, and a host handle whose `main` is that
/// file — the road `dql server` takes for `--db`.
struct Live {
    _dir: tempfile::TempDir,
    db: PathBuf,
    handle: Box<dyn DqlHandle>,
}

fn live() -> Live {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.sqlite");
    rusqlite::Connection::open(&db)
        .expect("create database")
        .execute_batch(INIT)
        .expect("initialize database");
    let mut handle = open_handle(SessionProfile::Server).expect("handle");
    {
        let mut session = handle.session().expect("session");
        run(
            &mut *session,
            &format!("mount!(\"{}\", \"main\")(*)", db.display()),
        )
        .expect("mount main");
    }
    Live {
        _dir: dir,
        db,
        handle,
    }
}

impl Live {
    /// A session whose mid-run `stdout!` sets are kept, in order.
    fn session(&mut self) -> (Box<dyn DqlSession + '_>, Shipped) {
        let shipped: Shipped = Arc::default();
        let sink = Arc::clone(&shipped);
        let session = self
            .handle
            .session_with_hooks(SessionHooks {
                on_ship: Some(Box::new(move |_columns, rows| {
                    sink.lock().unwrap().push(text_rows(rows));
                })),
            })
            .expect("session");
        (session, shipped)
    }

    /// `log`, read through an independent connection to the file.
    fn log(&self) -> Vec<i64> {
        read_ints(&self.db, "SELECT t FROM log ORDER BY rowid")
    }
}

fn read_ints(db: &Path, sql: &str) -> Vec<i64> {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("independent read-only connection");
    let mut statement = conn.prepare(sql).expect("prepare read");
    statement
        .query_map([], |row| row.get(0))
        .expect("read")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn text_rows(rows: &[Vec<Option<Vec<u8>>>]) -> Rows {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref()
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                })
                .collect()
        })
        .collect()
}

/// Submit what a user typed at a prompt and fetch every row.
fn run(session: &mut dyn DqlSession, typed: &str) -> Result<Rows, ApiError> {
    let result = session.query(&delightql_cst::prompt_wrap(typed))?;
    let mut rows = Vec::new();
    loop {
        let fetched = session.fetch(&result.handle, u64::MAX)?;
        if fetched.finished {
            break;
        }
        rows.extend(text_rows(&fetched.rows));
    }
    session.close(result.handle)?;
    Ok(rows)
}

fn identity(result: Result<Rows, ApiError>) -> String {
    match result {
        Ok(rows) => panic!("expected a refusal, got rows {rows:?}"),
        Err(error) => error.identity.unwrap_or_else(|| error.message),
    }
}

fn cells(values: &[&str]) -> Rows {
    values
        .iter()
        .map(|value| vec![Some(value.to_string())])
        .collect()
}

const AUTHORED_ABORT: &str = "delightql-error://authored/abort";

/// THE WITNESS: `;` separates three runs. The first commits its insert, the
/// second prints, the third aborts. The abort is the answer, the marker was
/// printed, and the committed row is in the file — and the same session
/// answers the next request from it.
#[test]
fn a_later_abort_leaves_the_committed_run_and_the_session_usable() {
    let mut live = live();
    let (mut session, shipped) = live.session();
    let answer = run(
        &mut *session,
        &format!("{ONE}one!(*) ; stdout!(_(m @ \"reached\"))(*) ; {ABORT}"),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    assert_eq!(*shipped.lock().unwrap(), vec![cells(&["reached"])]);
    assert_eq!(
        run(&mut *session, "log(*) ~> count:(*) as n").expect("the session is usable"),
        cells(&["1"])
    );
    drop(session);
    assert_eq!(live.log(), vec![1]);
}

/// The comma control: one run, rolled back whole — the print happened and
/// is not undone, the write is.
#[test]
fn a_comma_run_rolls_back_whole() {
    let mut live = live();
    let (mut session, shipped) = live.session();
    let answer = run(
        &mut *session,
        &format!("{ONE}one!(*), stdout!(_(m @ \"reached\"))(*), {ABORT}"),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    assert_eq!(*shipped.lock().unwrap(), vec![cells(&["reached"])]);
    drop(session);
    assert_eq!(live.log(), Vec::<i64>::new());
}

/// Four runs, failing in the third: both earlier runs committed — each
/// write counted exactly once — and the fourth never started.
#[test]
fn each_earlier_run_commits_once_and_a_successor_is_skipped() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!("{ONE}{THREE}one!(*) ; one!(*) ; {ABORT} ; three!(*)"),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    drop(session);
    assert_eq!(live.log(), vec![1, 1]);
}

/// Grouping inside runs: `g1 ; g2` where each is a `,` conjunction. The
/// first run's two writes commit together; the second run's write rolls
/// back with its abort.
#[test]
fn a_comma_run_after_a_semicolon_rolls_back_alone() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!(
            "{ONE}{TWO}{THREE}g1!(*) : one!(*), two!(*)\n\
             g2!(*) : three!(*), {ABORT}\n\
             g1!(*) ; g2!(*)"
        ),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    drop(session);
    assert_eq!(live.log(), vec![1, 2]);
}

/// A demanded query-local wrapper whose body is `a ; abort` is the
/// statement's run sequence: its body's `;` separates runs.
#[test]
fn a_demanded_wrapper_body_keeps_its_run_boundary() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!("w!(*) : _(t @ 1) |> insert!(log(*))(*) ; {ABORT}\nw!(*)"),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    drop(session);
    assert_eq!(live.log(), vec![1]);
}

/// A consulted `main!` demanded by `run_namespace!`: `main!(*) :- a ; b`
/// is two runs through the consulted definition's expansion.
#[test]
fn a_consulted_main_keeps_its_run_boundary() {
    let mut live = live();
    let rules = live._dir.path().join("fx.dql");
    std::fs::write(
        &rules,
        format!(
            "?- enlist!(\"main\")(*)\n\
             one!(*) :- _(t @ 1) |> insert!(log(*))(*)\n\
             main!(*) :- one!(*) ; {ABORT}\n"
        ),
    )
    .expect("write rules");
    let (mut session, _) = live.session();
    run(
        &mut *session,
        &format!("consult!(\"{}\", \"fx\")(*)", rules.display()),
    )
    .expect("consult");
    assert_eq!(
        identity(run(&mut *session, "run_namespace!(fx)(*)")),
        AUTHORED_ABORT
    );
    drop(session);
    assert_eq!(live.log(), vec![1]);
}

/// A backend failure in a later run keeps its own identity and rolls back
/// only its run.
#[test]
fn a_later_backend_failure_leaves_the_committed_run() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!("{ONE}bad!(*) : _(t @ 5) |> insert!(uniq(*))(*)\none!(*) ; bad!(*)"),
    );
    assert!(
        identity(answer).starts_with("delightql-error://target/sqlite/constraint"),
        "the backend's own identity"
    );
    drop(session);
    assert_eq!(live.log(), vec![1]);
    assert_eq!(read_ints(&live.db, "SELECT t FROM uniq"), vec![5]);
}

/// A failed assertion in a later run aborts that run alone, and its failing
/// verdict is recorded outside the rolled-back transaction.
#[test]
fn a_later_failed_assertion_leaves_the_committed_run() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!(
            "{ONE}none(T(*))(*) : T(*), x = 99\n\
             one!(*) ; assert!(none(*), \"no 99\", _(x @ 1))(*)"
        ),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    assert_eq!(
        run(
            &mut *session,
            "sys.assertions(*), name = \"no 99\" |> (outcome)"
        )
        .expect("the verdict ledger is readable"),
        cells(&["fail"])
    );
    drop(session);
    assert_eq!(live.log(), vec![1]);
}

/// Graceful exit commits and stops: the run before it and its own run
/// commit; the run after it never starts.
#[test]
fn exit_commits_and_skips_later_runs() {
    let mut live = live();
    let (mut session, _) = live.session();
    run(
        &mut *session,
        &format!("{ONE}{TWO}one!(*) ; exit!(*) ; two!(*)"),
    )
    .expect("exit! is graceful");
    drop(session);
    assert_eq!(live.log(), vec![1]);
}

/// Controls: an abort whose demanded input is empty is ordinary NO, so
/// every run commits and the statement answers the union of its receipts;
/// a pure `;` is the ordinary bag union.
#[test]
fn an_empty_abort_and_a_pure_union_are_ordinary() {
    let mut live = live();
    let (mut session, _) = live.session();
    let receipts = run(
        &mut *session,
        &format!(
            "{ONE}{TWO}quiet!(*) : _(x @ 1), x = 99 |> abort!(\"empty\")(*)\n\
             one!(*) ; quiet!(*) ; two!(*)"
        ),
    )
    .expect("an empty abort input is NO");
    assert_eq!(receipts.len(), 2, "one receipt per write: {receipts:?}");
    assert_eq!(
        run(&mut *session, "_(a @ 1) ; _(a @ 2)").expect("a pure union"),
        cells(&["1", "2"])
    );
    drop(session);
    assert_eq!(live.log(), vec![1, 2]);
}

/// A statement that does not compile executes nothing: runtime commit law
/// applies to runs that execute.
#[test]
fn a_compile_failure_runs_nothing() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(&mut *session, &format!("{ONE}one!(*) ; nosuch!(*)"));
    assert_eq!(
        identity(answer),
        "delightql-error://semantic/effect/transform/unsupported"
    );
    drop(session);
    assert_eq!(live.log(), Vec::<i64>::new());
}

/// The objects a committed run created exist after a later abort, and the
/// session resolves them: the durable table in the file, the session temp
/// table by name on the next request.
#[test]
fn objects_created_by_a_committed_run_are_registered() {
    let mut live = live();
    let (mut session, _) = live.session();
    assert_eq!(
        identity(run(
            &mut *session,
            &format!("_(k @ 1) |> table!(kept(*))(*) ; {ABORT}")
        )),
        AUTHORED_ABORT
    );
    assert_eq!(
        identity(run(
            &mut *session,
            &format!("_(k @ 2) |> temp_table!(staged(*))(*) ; {ABORT}")
        )),
        AUTHORED_ABORT
    );
    assert_eq!(
        run(&mut *session, "kept(*)").expect("the durable table resolves"),
        cells(&["1"])
    );
    assert_eq!(
        run(&mut *session, "staged(*)").expect("the temp table resolves"),
        cells(&["2"])
    );
    drop(session);
    assert_eq!(read_ints(&live.db, "SELECT k FROM kept"), vec![1]);
}

const SEVEN: &str = "seven!(*) : _(t @ 7) |> insert!(log(*))(*)\n";

/// A demanded HO rule stages its input as the invocation's own step, so a
/// body whose first arm marks no step (`returning!`) still opens its `;`
/// runs: the abort is reached, the session stays usable.
#[test]
fn an_invocation_input_does_not_straddle_its_bodys_run_boundary() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!("w!(T(*))(*) : returning!(T(*))(*) ; {ABORT}\nw!(_(a @ 1))(*)"),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    assert_eq!(
        run(&mut *session, "_(x @ 2)").expect("the session is usable"),
        cells(&["2"])
    );
}

/// Forwarded input: the outer rule pipes its input into an inner rule
/// whose body is `returning ; write ; abort`. The write's run commits.
#[test]
fn a_forwarded_input_keeps_the_inner_bodys_runs() {
    let mut live = live();
    let (mut session, _) = live.session();
    let answer = run(
        &mut *session,
        &format!(
            "{SEVEN}inner!(U(*))(*) : returning!(U(*))(*) ; seven!(*) ; {ABORT}\n\
             outer!(T(*))(*) : T(*) |> inner!(*)\n\
             outer!(_(a @ 1))(*)"
        ),
    );
    assert_eq!(identity(answer), AUTHORED_ABORT);
    drop(session);
    assert_eq!(live.log(), vec![7]);
}

/// A guard that closes the demand leaves the staged input empty, not
/// missing: the rule's receipt value still reads it, and answers nothing.
#[test]
fn a_gated_off_invocation_reads_an_empty_input() {
    let mut live = live();
    let (mut session, _) = live.session();
    let rows = run(
        &mut *session,
        &format!(
            "{SEVEN}w!(T(*))(*) : returning!(T(*))(*) ; seven!(*)\n\
             _(g @ 1), g = 2, w!(_(a @ 1))(*)"
        ),
    )
    .expect("a closed guard answers NO");
    assert_eq!(rows, Rows::new());
    drop(session);
    assert_eq!(live.log(), Vec::<i64>::new());
}

const MAKE_AND_WRAP: &str = "make!(*) : _(a @ 1) |> temp_table!(staged(*))(*)\n\
                             w!(T(*))(*) : returning!(T(*))(*)\n";

/// Whether any cell of `rows` holds `needle`.
fn holds(rows: &Rows, needle: &str) -> bool {
    rows.iter()
        .flatten()
        .any(|cell| cell.as_deref().is_some_and(|text| text.contains(needle)))
}

/// A taken `exit!` skips a later creation AND the invocation that would
/// read it: the declined invocation prepares no read of the table that was
/// never created, and the session answers the next request. The open
/// demand reads the created table.
#[test]
fn an_exit_skipped_creation_is_never_read_by_a_later_invocation() {
    let mut live = live();
    let (mut session, _) = live.session();
    let declined = run(
        &mut *session,
        &format!("{MAKE_AND_WRAP}exit!(*) ; make!(*) ; w!(staged(*))(*)"),
    )
    .expect("exit! is graceful");
    assert_eq!(declined, Rows::new());
    assert_eq!(
        run(&mut *session, "_(x @ 2)").expect("the session is usable"),
        cells(&["2"])
    );
    let open = run(
        &mut *session,
        &format!("{MAKE_AND_WRAP}make!(*) ; w!(staged(*))(*)"),
    )
    .expect("the open demand reads the created table");
    assert_eq!(open.len(), 2, "both receipts: {open:?}");
    assert!(
        holds(&open, "\"a\":1"),
        "the payload is the created row: {open:?}"
    );
}

/// The same under an ordinary closed guard: the creation and the
/// invocation are both declined, the answer is NO, and nothing reads the
/// table that was never created. The open guard reads it.
#[test]
fn a_guard_skipped_creation_is_never_read_by_its_invocation() {
    let mut live = live();
    let (mut session, _) = live.session();
    let declined = run(
        &mut *session,
        &format!("{MAKE_AND_WRAP}_(g @ 1), g = 2, make!(*), w!(staged(*))(*)"),
    )
    .expect("a closed guard answers NO");
    assert_eq!(declined, Rows::new());
    assert_eq!(
        run(&mut *session, "_(x @ 2)").expect("the session is usable"),
        cells(&["2"])
    );
    let open = run(
        &mut *session,
        &format!("{MAKE_AND_WRAP}_(g @ 1), g = 1, make!(*), w!(staged(*))(*)"),
    )
    .expect("an open guard reads the created table");
    assert_eq!(open.len(), 1, "{open:?}");
    assert!(
        holds(&open, "\"a\":1"),
        "the payload is the created row: {open:?}"
    );
}

/// A source whose catalog types SQLite reports unquoted — punctuation, an
/// embedded quote, a multiword and parameterized types, an ordinary type and
/// none — stages for an open demand and a closed one alike: the empty
/// carrier every invocation creates takes those types without executing the
/// report as SQL, and the open demand's payload is the source's own row.
#[test]
fn an_invocation_stages_any_source_the_ordinary_read_admits() {
    let mut live = live();
    rusqlite::Connection::open(&live.db)
        .expect("open database")
        .execute_batch(
            "CREATE TABLE typed(q \"foo)bar\", r \"a\"\"b\", m double precision, \
             p VARCHAR(20), n NUMERIC(10,2), i INTEGER, z);\n\
             INSERT INTO typed VALUES (7, 'x', 1.5, 'v', 2.25, 3, 'zz');",
        )
        .expect("typed source");
    let (mut session, _) = live.session();
    run(&mut *session, "refresh!(\"main\")(*)").expect("refresh the catalog");
    let wrapped = "w!(T(*))(*) : returning!(T(*))(*)\n";
    let open = run(&mut *session, &format!("{wrapped}w!(typed(*))(*)"))
        .expect("the open demand stages the typed source");
    assert_eq!(open.len(), 1, "{open:?}");
    for member in [
        "\"q\":7",
        "\"r\":\"x\"",
        "\"m\":1.5",
        "\"p\":\"v\"",
        "\"n\":2.25",
        "\"i\":3",
        "\"z\":\"zz\"",
    ] {
        assert!(holds(&open, member), "{member} in {open:?}");
    }
    let declined = run(
        &mut *session,
        &format!("{wrapped}_(g @ 1), g = 2, w!(typed(*))(*)"),
    )
    .expect("a closed guard answers NO");
    assert_eq!(declined, Rows::new());
    assert_eq!(
        run(&mut *session, "_(x @ 2)").expect("the session is usable"),
        cells(&["2"])
    );
}

/// The CLI road the witness was found on: `dql query --db` over a file.
#[test]
fn the_cli_query_road_keeps_the_committed_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("main.sqlite");
    rusqlite::Connection::open(&db)
        .expect("create database")
        .execute_batch(INIT)
        .expect("initialize database");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dql"))
        .current_dir(dir.path())
        .env("DQL_STATE_DIR", dir.path().join("state"))
        .args(["query", "--db"])
        .arg(&db)
        .arg(format!(
            "ins!(*) : _(t @ 1) |> insert!(log(*))(*)\n\
             ins!(*) ; stdout!(_(m @ \"reached\"))(*) ; {ABORT}"
        ))
        .output()
        .expect("run dql");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains(AUTHORED_ABORT), "{stderr}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("reached"));
    assert_eq!(read_ints(&db, "SELECT t FROM log"), vec![1]);
}
