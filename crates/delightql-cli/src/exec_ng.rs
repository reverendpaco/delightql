// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Query execution module.
//!
//! The CLI calls session.query() and session.fetch(). Nothing else
//! crosses the boundary.
//!
//! THE ROAD IS DECIDED HERE, ONCE. `execute_query` receives the handle, not
//! a session: the `--to` value chooses whether the statement runs on an
//! executing session or an observing one, and no caller can pair an
//! inspection with a session that executes.
use anyhow::Result;
use delightql_backends::QueryResults;
use delightql_core::api::{DqlHandle, DqlSession};

use crate::args::Stage;
use crate::output_format::{Digest, OutputFormat};

pub struct ResultMetadata {
    pub columns: Vec<String>,
    pub row_count: usize,
}

/// How an executed result is shown.
#[derive(Clone, Copy)]
pub struct Rendering {
    pub format: OutputFormat,
    pub no_headers: bool,
    pub no_sanitize: bool,
}

/// Where the mid-run `stdout!` sets of an executing statement go
/// (EFFECT-ALGEBRA §5; the run's return value never passes through here).
///
/// The one-shot query road prints them live on the console; the tools and
/// the prompt execute and discard them. A digest rendering discards them
/// on every road: its output is one machine value (pinned by
/// `tests/stdout_ship.rs`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ShippedSets {
    Console,
    Discarded,
}

/// THE OUTPUT-MODE INVENTORY, judged once.
///
/// `--to results` (the default) executes. Every other `--to` value
/// observes: a compile stage renders through `sys::execution.compile`, and a
/// digest stage runs the statement only if the compiler classifies it
/// effect-free — an effect refuses before any dispatcher runs, because the
/// session it runs on cannot execute one. The formats under `-f` never
/// change the road: `-f hash` under `--to results` executes and digests.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Road {
    Execute,
    Observe,
}

fn road_of(to: Option<Stage>) -> Road {
    match to {
        None | Some(Stage::Results) => Road::Execute,
        Some(_) => Road::Observe,
    }
}

/// Send one query: text the user typed, or the CLI wrote, as one goal. The
/// prompt wrap is the host's — the session reads what it is sent as
/// canonical text — so every query this module sends crosses here.
pub(crate) fn query(
    session: &mut dyn DqlSession,
    dql: &str,
) -> std::result::Result<delightql_core::api::QueryResult, delightql_core::api::ApiError> {
    session.query(&delightql_cst::prompt_wrap(dql))
}

/// Send definitions: canonical text as written, admitted into `home` the
/// way an unnamed `(~~ddl ~~)` block is. Always on an executing session —
/// there is no compilation of definitions to observe. Answers what they
/// defined, as `(namespace, entity)`.
pub fn define(handle: &mut dyn DqlHandle, text: &str) -> Result<Vec<(String, String)>> {
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    let result = session.query(text)?;
    let mut defined = Vec::new();
    loop {
        let fetched = session.fetch(&result.handle, u64::MAX)?;
        for row in &fetched.rows {
            let [Some(namespace), Some(entity)] = row.as_slice() else {
                anyhow::bail!("a definitions receipt row is (namespace, entity), got {row:?}");
            };
            defined.push((
                String::from_utf8_lossy(namespace).into_owned(),
                String::from_utf8_lossy(entity).into_owned(),
            ));
        }
        if fetched.finished {
            break;
        }
    }
    let _ = session.close(result.handle);
    Ok(defined)
}

/// One row of protocol cells as text, absence kept: the renderer decides
/// what an absent cell becomes, because that depends on whether the
/// format has a reader (`output_format::format_output`).
pub(crate) fn cells_to_text(row: &[Option<Vec<u8>>]) -> Vec<Option<String>> {
    row.iter()
        .map(|cell| {
            cell.as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).to_string())
        })
        .collect()
}

/// A relation with its cells still nullable and its columns' type
/// descriptors: the shape a file with readers needs (`json_object_row`
/// renders NULL as null and numbers unquoted), as opposed to the console
/// display road, where a missing cell becomes the text `NULL`.
pub struct TypedRows {
    pub columns: Vec<String>,
    pub descriptors: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl TypedRows {
    /// One JSON object per row, one line each.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for row in &self.rows {
            out.push_str(&crate::output_format::json_object_row(
                &self.columns,
                &self.descriptors,
                row,
            ));
            out.push('\n');
        }
        out
    }
}

/// Fetch ALL rows of a DQL query with their nullability and descriptors
/// intact.
pub fn fetch_all_typed(session: &mut dyn DqlSession, dql: &str) -> Result<TypedRows> {
    let qr = query(session, dql)?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let descriptors: Vec<String> = qr.columns.iter().map(|c| c.descriptor.clone()).collect();
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    loop {
        let fr = session.fetch(&qr.handle, u64::MAX)?;
        if fr.finished {
            break;
        }
        for row in &fr.rows {
            rows.push(
                row.iter()
                    .map(|cell| {
                        cell.as_ref()
                            .map(|b| String::from_utf8_lossy(b).to_string())
                    })
                    .collect(),
            );
        }
    }
    let _ = session.close(qr.handle);
    Ok(TypedRows {
        columns,
        descriptors,
        rows,
    })
}

/// Fetch ALL rows from a DQL session into QueryResults.
pub(crate) fn fetch_all(session: &mut dyn DqlSession, dql: &str) -> Result<QueryResults> {
    fetch_all_named(session, dql).map(|(results, _)| results)
}

/// [`fetch_all`], with the Header's naming for each column beside it.
fn fetch_all_named(
    session: &mut dyn DqlSession,
    dql: &str,
) -> Result<(QueryResults, Vec<delightql_core::api::Naming>)> {
    let qr = query(session, dql)?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let naming: Vec<delightql_core::api::Naming> = qr.columns.iter().map(|c| c.naming).collect();

    let mut all_rows: Vec<Vec<String>> = Vec::new();

    loop {
        let fr = session.fetch(&qr.handle, u64::MAX)?;

        if fr.finished {
            break;
        }

        for row in &fr.rows {
            all_rows.push(crate::output_format::console_cells(&cells_to_text(row)));
        }
    }

    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    let row_count = all_rows.len();
    Ok((
        QueryResults {
            columns,
            rows: all_rows,
            row_count,
        },
        naming,
    ))
}

/// Fetch ALL rows preserving raw protocol cells (no string coercion), for a
/// caller that has to tell an absent value from a present one — a display
/// rendering cannot answer that question.
#[allow(clippy::type_complexity)]
pub(crate) fn fetch_all_raw(
    session: &mut dyn DqlSession,
    dql: &str,
) -> Result<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>)> {
    let qr = query(session, dql)?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();

    let mut all_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();

    loop {
        let fr = session.fetch(&qr.handle, u64::MAX)?;

        if fr.finished {
            break;
        }

        for row in fr.rows {
            all_rows.push(row);
        }
    }

    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    Ok((columns, all_rows))
}

/// Query and stream results to the terminal.
///
/// A RECORD-COMMITTED rendering: every line printed is a complete record,
/// and a failure after some were printed leaves those records standing with
/// the error on stderr — the streaming contract of a console table.
fn display_results(
    session: &mut dyn DqlSession,
    dql: &str,
    rendering: Rendering,
) -> Result<ResultMetadata> {
    use crate::output_format::format_output;

    let qr = query(session, dql)?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();

    let mut total_rows = 0usize;

    loop {
        let fr = session.fetch(&qr.handle, 100)?;

        if fr.finished {
            break;
        }

        let rows: Vec<Vec<Option<String>>> = fr.rows.iter().map(|row| cells_to_text(row)).collect();

        let is_first_batch = total_rows == 0;
        total_rows += rows.len();

        let show_headers = is_first_batch && !rendering.no_headers;
        let output = format_output(
            &columns,
            &rows,
            rendering.format,
            !show_headers,
            rendering.no_sanitize,
        );
        print!("{}", output);
    }

    // An empty relation still has a heading: formats whose contract can
    // carry one emit it for zero rows — table/tsv/csv print the header
    // line, box the header frame. Rows-only contracts (list) stay
    // zero-byte, like the machine formats with their own empty spellings
    // (json `[]`, jsonl zero lines, raw zero bytes) on their own paths.
    if total_rows == 0
        && !rendering.no_headers
        && matches!(
            rendering.format,
            OutputFormat::Table | OutputFormat::Tsv | OutputFormat::Csv | OutputFormat::Box
        )
    {
        let output = format_output(
            &columns,
            &[],
            rendering.format,
            false,
            rendering.no_sanitize,
        );
        print!("{}", output);
    }

    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// ONE document, rendered whole before any of it is published.
///
/// The commit boundary of a structured rendering: a `Document` exists only
/// once every row it contains has been produced, so a failure during
/// production — at the first row or after a thousand — leaves nothing to
/// commit and nothing on stdout. The bytes reach stdout through `commit`
/// alone.
pub(crate) struct Document(String);

impl Document {
    pub(crate) fn commit(self) {
        print!("{}", self.0);
    }

    #[cfg(test)]
    fn text(&self) -> &str {
        &self.0
    }
}

/// `-f json` — one JSON array of objects, typed: NULL is null (never the
/// string "NULL"), numbers are unquoted when the column's declared type is
/// numeric AND the text round-trips, and columns keep relation order.
///
/// Rendered as a `Document`: the array is not a stream of records but one
/// value, and a value with a beginning and no end is not a value. The rows
/// are collected across every fetch batch; the document is returned only
/// when the last batch has reported finished.
pub(crate) fn json_document(
    session: &mut dyn DqlSession,
    dql: &str,
) -> Result<(Document, ResultMetadata)> {
    use crate::output_format::json_object_row;

    let qr = query(session, dql)?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let descriptors: Vec<String> = qr.columns.iter().map(|c| c.descriptor.clone()).collect();

    let mut lines: Vec<String> = Vec::new();
    loop {
        let fr = session.fetch(&qr.handle, 100)?;
        if fr.finished {
            break;
        }
        for row in &fr.rows {
            let cells: Vec<Option<String>> = row
                .iter()
                .map(|c| {
                    c.as_ref()
                        .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                })
                .collect();
            lines.push(json_object_row(&columns, &descriptors, &cells));
        }
    }
    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    let mut text = String::from("[");
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        text.push_str("\n  ");
        text.push_str(line);
    }
    if !lines.is_empty() {
        text.push('\n');
    }
    text.push_str("]\n");
    Ok((
        Document(text),
        ResultMetadata {
            columns,
            row_count: lines.len(),
        },
    ))
}

/// `-f jsonl` — one JSON object per line, pipe-friendly. Each line is a
/// complete document, so this is a RECORD-COMMITTED stream: a batch's
/// lines are written once that batch has been produced.
fn display_results_jsonl(session: &mut dyn DqlSession, dql: &str) -> Result<ResultMetadata> {
    use crate::output_format::json_object_row;
    use std::io::Write;

    let qr = query(session, dql)?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let descriptors: Vec<String> = qr.columns.iter().map(|c| c.descriptor.clone()).collect();

    let stdout = std::io::stdout().lock();
    let mut out = std::io::BufWriter::new(stdout);
    let mut total_rows = 0usize;

    loop {
        let fr = session.fetch(&qr.handle, 100)?;
        if fr.finished {
            break;
        }
        for row in &fr.rows {
            let cells: Vec<Option<String>> = row
                .iter()
                .map(|c| {
                    c.as_ref()
                        .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                })
                .collect();
            out.write_all(json_object_row(&columns, &descriptors, &cells).as_bytes())?;
            out.write_all(b"\n")?;
            total_rows += 1;
        }
    }
    out.flush()?;

    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// `-f raw` — the byte-preservation doctrine's user-facing exit: verbatim
/// cell bytes, no separators ever (a separator would corrupt binary),
/// NULL writes zero bytes, multi-row = byte-stream concatenation.
/// Single column ONLY — multi-column concatenation ("1John2Jane") is
/// never what anyone wants; refuse and teach.
fn display_results_raw(session: &mut dyn DqlSession, dql: &str) -> Result<ResultMetadata> {
    use std::io::{IsTerminal, Write};

    let qr = query(session, dql)?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    if columns.len() != 1 {
        anyhow::bail!(
            "raw is byte-faithful extraction of ONE column; this result has \
             {} ({}). Project the column you want: |> (col)",
            columns.len(),
            columns.join(", ")
        );
    }
    if std::io::stdout().is_terminal() {
        // The poweruser sharp edge, --no-sanitize style: verbatim bytes
        // to a terminal are an injection surface. Warn, never block;
        // silent when piped (the intended use).
        crate::client::incident::warning(
            "argument",
            delightql_types::diagnostic::Client::SanitizeDisabled {
                message: "-f raw writes verbatim bytes (terminal control sequences \
             included); intended for pipes and files"
                    .to_string(),
            },
        );
    }
    let mut stdout = std::io::stdout().lock();
    let mut total_rows = 0usize;

    loop {
        let fr = session.fetch(&qr.handle, 100)?;

        if fr.finished {
            break;
        }

        for row in &fr.rows {
            for cell in row {
                if let Some(bytes) = cell {
                    stdout.write_all(bytes)?;
                }
                // NULL → zero bytes (nothing written)
            }
        }
        total_rows += fr.rows.len();
    }

    stdout.flush()?;

    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// THE ONE DIGEST RENDERING: fetch the whole result, print its digest.
///
/// Reached from an observing road (`--to hash` and siblings, where the
/// session has already refused any effect) and from an executing one
/// (`-f hash` under `--to results`); the digest itself does not know
/// which, and must not — the road was decided before the statement ran.
fn render_digest(
    session: &mut dyn DqlSession,
    dql: &str,
    digest: Digest,
) -> Result<ResultMetadata> {
    let (line, meta) = digest_of(session, dql, digest)?;
    println!("{}", line);
    Ok(meta)
}

/// The line a digest prints, and the result it was computed over.
///
/// The digest observes the protocol cells as fetched — never a display
/// rendering, where an absent cell has already become the text `NULL`.
fn digest_of(
    session: &mut dyn DqlSession,
    dql: &str,
    digest: Digest,
) -> Result<(String, ResultMetadata)> {
    use crate::util::fingerprint::{digest_heading, Fingerprint};

    let qr = query(session, dql)?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let naming: Vec<delightql_core::api::Naming> = qr.columns.iter().map(|c| c.naming).collect();
    let mut observation = delightql_protocol::digest::Observation::new();
    loop {
        let fr = session.fetch(&qr.handle, u64::MAX)?;
        if fr.finished {
            break;
        }
        observation.rows(&fr.rows);
    }
    let _ = session.close(qr.handle).map_err(anyhow::Error::new);

    let heading = digest_heading(&columns, &naming);
    let line = match digest {
        Digest::Hash => observation.data().hex(),
        Digest::TotalHash => observation.table(&heading).hex(),
        Digest::Fingerprint => serde_json::to_string(&Fingerprint::of(heading, &observation))
            .map_err(|e| anyhow::anyhow!("Failed to render fingerprint: {}", e))?,
    };
    Ok((
        line,
        ResultMetadata {
            columns,
            row_count: observation.row_count(),
        },
    ))
}

/// Run a DQL query and return structured results (no display).
///
/// An EXECUTING road: the host's own statements — a mount, an attach, a
/// catalog read — run here on the session the caller opened.
pub fn run_dql_query(dql: &str, session: &mut dyn DqlSession) -> Result<QueryResults> {
    fetch_all(session, dql)
}

/// The session a road runs on, opened from the handle by the road alone.
///
/// The observing road cannot receive a console sink: nothing it runs ships.
/// The executing road receives one only when the caller asked for it AND
/// the rendering is not a digest.
fn open_session<'h>(
    handle: &'h mut dyn DqlHandle,
    road: Road,
    rendering: Rendering,
    shipped: ShippedSets,
) -> Result<Box<dyn DqlSession + 'h>> {
    let session = match road {
        Road::Observe => handle.observation_session(),
        Road::Execute => {
            let console = shipped == ShippedSets::Console && rendering.format.digest().is_none();
            let hooks = if console {
                delightql_core::api::SessionHooks {
                    on_ship: Some(Box::new(
                        move |columns: &[String], rows: &[Vec<Option<Vec<u8>>>]| {
                            let display_rows: Vec<Vec<Option<String>>> =
                                rows.iter().map(|row| cells_to_text(row)).collect();
                            let output = crate::output_format::format_output(
                                columns,
                                &display_rows,
                                rendering.format,
                                rendering.no_headers,
                                rendering.no_sanitize,
                            );
                            print!("{}", output);
                        },
                    )),
                }
            } else {
                delightql_core::api::SessionHooks::default()
            };
            handle.session_with_hooks(hooks)
        }
    };
    session.map_err(|e| anyhow::anyhow!("{}", e))
}

/// Execute a DQL query: choose the road, open its session, run, render.
///
/// When `sequential` is true, multi-query input is split client-side via
/// `split_queries()`. Each query is sent as a separate `session.query()`
/// call ON THE SAME SESSION — so under an observing `--to`, every statement
/// of the sequence is judged, not only the last — earlier statements run
/// for their effects and only the FINAL statement's result is
/// displayed/returned. Without `sequential`, multi-query input is rejected
/// by the relay per the protocol contract.
pub fn execute_query(
    source_code: &str,
    handle: &mut dyn DqlHandle,
    target_stage: Option<Stage>,
    rendering: Rendering,
    shipped: ShippedSets,
    sequential: bool,
) -> Result<Option<ResultMetadata>> {
    let mut session = open_session(handle, road_of(target_stage), rendering, shipped)?;
    if sequential {
        let queries = delightql_core::api::split_queries(source_code)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        for q in &queries[..queries.len() - 1] {
            fetch_all(&mut *session, q)?;
        }

        let last = queries.last().unwrap();
        return execute_single_query(last, &mut *session, target_stage, rendering);
    }

    execute_single_query(source_code, &mut *session, target_stage, rendering)
}

fn execute_single_query(
    source_code: &str,
    session: &mut dyn DqlSession,
    target_stage: Option<Stage>,
    rendering: Rendering,
) -> Result<Option<ResultMetadata>> {
    if let Some(stage) = target_stage.and_then(Stage::compile_stage) {
        return display_compile_stage(session, stage, source_code, rendering);
    }
    if let Some(digest) = target_stage.and_then(Stage::digest) {
        return render_digest(session, source_code, digest).map(Some);
    }

    let meta = match rendering.format {
        OutputFormat::Digest(digest) => render_digest(session, source_code, digest)?,
        OutputFormat::Raw => display_results_raw(session, source_code)?,
        OutputFormat::Json => {
            let (document, meta) = json_document(session, source_code)?;
            document.commit();
            meta
        }
        OutputFormat::Jsonl => display_results_jsonl(session, source_code)?,
        OutputFormat::Table
        | OutputFormat::Box
        | OutputFormat::Csv
        | OutputFormat::Tsv
        | OutputFormat::List => display_results(session, source_code, rendering)?,
    };
    Ok(Some(meta))
}

/// Build a `sys::execution.compile(stage, b64:source)` DQL string.
/// Projects BOTH representation and error — the caller must consult the
/// error column, never print a NULL representation as if it were output.
fn compile_stage_dql(stage: &str, source: &str) -> String {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(source.as_bytes());
    format!(
        "sys::execution.compile(\"{}\", b64:\"{}\") |> (representation, error, error_message)",
        stage, encoded
    )
}

/// Display a compile stage (`--to sql`, `--to ast-*`, …). A failed compile
/// surfaces its error and exits non-zero — the inspection surface must
/// never print a literal NULL where the user asked to see the compilation.
fn display_compile_stage(
    session: &mut dyn DqlSession,
    stage: &str,
    source_code: &str,
    rendering: Rendering,
) -> Result<Option<ResultMetadata>> {
    use crate::output_format::format_output;

    let dql = compile_stage_dql(stage, source_code);
    let (_, rows) = fetch_all_raw(session, &dql)?;
    let row = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("sys::execution.compile returned no rows"))?;

    if let Some(uri) = &row[1] {
        // The full message rides alongside the URI: printing only the URI
        // and telling the user to re-run without --to would withhold the
        // message this call already has, at exactly the moment the user
        // asked the CLI to explain itself.
        let uri = String::from_utf8_lossy(uri);
        let message = row[2]
            .as_ref()
            .map(|m| String::from_utf8_lossy(m).to_string())
            .unwrap_or_else(|| "compilation failed".to_string());
        anyhow::bail!(
            "[{uri}] {message}\n\
             (run `dql explain {uri}` for the identifier's prose)"
        );
    }

    let representation = match &row[0] {
        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
        None => anyhow::bail!("sys::execution.compile returned neither output nor error"),
    };
    let columns = vec!["representation".to_string()];

    // Raw = the pasteable artifact itself: bare text, real newlines, no
    // header. A digest of a representation is not a thing anyone asked
    // for; the digests render executed results and are refused here.
    match rendering.format {
        OutputFormat::Raw => {
            println!("{}", representation);
            return Ok(Some(ResultMetadata {
                columns,
                row_count: 1,
            }));
        }
        OutputFormat::Digest(_) => anyhow::bail!(
            "a digest format renders an executed result; `--to {stage}` renders a \
             compilation. Drop -f, or use `--to results`"
        ),
        _ => {}
    }

    let display_rows = vec![vec![Some(representation)]];
    let output = format_output(
        &columns,
        &display_rows,
        rendering.format,
        rendering.no_headers,
        rendering.no_sanitize,
    );
    print!("{}", output);
    Ok(Some(ResultMetadata {
        columns,
        row_count: 1,
    }))
}

#[cfg(test)]
mod tests {
    //! The road inventory and the document commit boundary. The SQLite
    //! backend reports a row-level failure before delivering any row, so a
    //! failure AFTER rows were produced is reachable only through the
    //! interface itself: a real session whose second fetch is made to fail.

    use super::*;
    use delightql_core::api::{ApiError, FetchResult, QueryHandle, QueryResult};

    /// A real session whose row production fails after its first batch.
    struct FailsAfterFirstBatch<'a> {
        inner: Box<dyn DqlSession + 'a>,
        fetches: usize,
    }

    impl DqlSession for FailsAfterFirstBatch<'_> {
        fn query(&mut self, text: &str) -> Result<QueryResult, ApiError> {
            self.inner.query(text)
        }

        fn fetch(&mut self, handle: &QueryHandle, count: u64) -> Result<FetchResult, ApiError> {
            self.fetches += 1;
            if self.fetches > 1 {
                return Err(ApiError::from("malformed JSON"));
            }
            self.inner.fetch(handle, count)
        }

        fn close(&mut self, handle: QueryHandle) -> Result<(), ApiError> {
            self.inner.close(handle)
        }
    }

    fn handle() -> Box<dyn DqlHandle> {
        crate::connection::open_handle(crate::connection::SessionProfile::client())
            .expect("an in-process handle opens")
    }

    /// A digest that includes column names reads a minted name by its
    /// position, not its drawn spelling, so two compilations of one query
    /// digest alike.
    #[test]
    fn a_name_bearing_digest_repeats_across_compilations() {
        let query = "_(a @ 1) |> (a, a + 1) ; _(a @ 2) |> (a, a + 1)";
        for digest in [Digest::TotalHash, Digest::Fingerprint] {
            let mut digests = Vec::new();
            for _ in 0..2 {
                let mut handle = handle();
                let mut session = handle.session().expect("session");
                let (line, _) = digest_of(&mut *session, query, digest).expect("digest");
                digests.push(line);
            }
            assert_eq!(digests[0], digests[1], "{digest:?}");
        }
    }

    #[test]
    fn every_stage_but_results_observes() {
        assert_eq!(road_of(None), Road::Execute);
        assert_eq!(road_of(Some(Stage::Results)), Road::Execute);
        for stage in [
            Stage::Cst,
            Stage::AstUnresolved,
            Stage::AstResolved,
            Stage::AstRefined,
            Stage::AstSql,
            Stage::Sql,
            Stage::Fingerprint,
            Stage::Hash,
            Stage::TotalHash,
        ] {
            assert_eq!(road_of(Some(stage)), Road::Observe, "{stage:?}");
            assert!(
                stage.compile_stage().is_some() != stage.digest().is_some(),
                "{stage:?} is exactly one of a compile stage or a digest"
            );
        }
    }

    /// The later-failure discriminator: a batch of rows is produced, then
    /// production fails. No document exists to commit.
    #[test]
    fn a_failure_after_rows_were_produced_yields_no_document() {
        let mut handle = handle();
        let mut session = FailsAfterFirstBatch {
            inner: handle.session().expect("session"),
            fetches: 0,
        };
        let outcome = json_document(&mut session, "_(a @ 1; 2)");
        let error = outcome
            .err()
            .expect("the failure is the outcome")
            .to_string();
        assert!(error.contains("malformed JSON"), "{error}");
        assert!(
            session.fetches > 1,
            "the first batch was produced before the failure"
        );
    }

    /// The control: a successful production renders the one document, in
    /// the bytes the streaming array renderer produced.
    #[test]
    fn a_successful_production_renders_one_document() {
        let mut handle = handle();
        let mut session = handle.session().expect("session");
        let (document, meta) = json_document(&mut *session, "_(a @ 1; 2)").expect("renders");
        assert_eq!(document.text(), "[\n  {\"a\": 1},\n  {\"a\": 2}\n]\n");
        assert_eq!(meta.row_count, 2);

        let (document, meta) = json_document(&mut *session, "_(a @ 1), a = 2").expect("renders");
        assert_eq!(document.text(), "[]\n");
        assert_eq!(meta.row_count, 0);
    }
}
