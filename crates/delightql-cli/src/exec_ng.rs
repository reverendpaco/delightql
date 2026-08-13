// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Query execution module.
///
/// The CLI calls session.query() and session.fetch(). Nothing else
/// crosses the boundary.
use anyhow::Result;
use delightql_backends::QueryResults;
use delightql_core::api::DqlSession;

use crate::args::Stage;
use crate::output_format::OutputFormat;
use std::cell::RefCell;

thread_local! {
    pub static ZEBRA_MODE: RefCell<Option<usize>> = const { RefCell::new(None) };
}

pub struct ResultMetadata {
    pub columns: Vec<String>,
    pub row_count: usize,
}

/// One row of protocol cells as the console shows it.
///
/// A display boundary, and the only kind of place a cell's absence is
/// allowed to become the four characters `NULL`: what is printed here has
/// no reader that could mistake it for the value again.
pub(crate) fn cells_to_display(row: &[Option<Vec<u8>>]) -> Vec<String> {
    row.iter()
        .map(|cell| match cell {
            Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
            None => "NULL".to_string(),
        })
        .collect()
}

/// Fetch ALL rows from a DQL session into QueryResults.
pub(crate) fn fetch_all(session: &mut dyn DqlSession, dql: &str) -> Result<QueryResults> {
    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();

    let mut all_rows: Vec<Vec<String>> = Vec::new();

    loop {
        let fr = session
            .fetch(&qr.handle, u64::MAX)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        if fr.finished {
            break;
        }

        for row in &fr.rows {
            all_rows.push(cells_to_display(row));
        }
    }

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

    let row_count = all_rows.len();
    Ok(QueryResults {
        columns,
        rows: all_rows,
        row_count,
    })
}

/// Fetch ALL rows preserving raw protocol cells (no string coercion).
/// Used by `--to bhash` to hash bytes directly, and by any caller that has
/// to tell an absent value from a present one — a display rendering
/// cannot answer that question.
#[allow(clippy::type_complexity)]
pub(crate) fn fetch_all_raw(
    session: &mut dyn DqlSession,
    dql: &str,
) -> Result<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>)> {
    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();

    let mut all_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();

    loop {
        let fr = session
            .fetch(&qr.handle, u64::MAX)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        if fr.finished {
            break;
        }

        for row in fr.rows {
            all_rows.push(row);
        }
    }

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

    Ok((columns, all_rows))
}

/// Query and stream results to the terminal.
fn display_results(
    session: &mut dyn DqlSession,
    dql: &str,
    output_format: OutputFormat,
    zebra_mode: Option<usize>,
    no_headers: bool,
    no_sanitize: bool,
) -> Result<ResultMetadata> {
    use crate::output_format::format_output_with_zebra;

    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;

    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();

    let mut total_rows = 0usize;

    loop {
        let fr = session
            .fetch(&qr.handle, 100)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        if fr.finished {
            break;
        }

        let rows: Vec<Vec<String>> = fr.rows.iter().map(|row| cells_to_display(row)).collect();

        let is_first_batch = total_rows == 0;
        total_rows += rows.len();

        let show_headers = is_first_batch && !no_headers;
        let output = format_output_with_zebra(
            &columns,
            &rows,
            output_format,
            zebra_mode,
            !show_headers,
            no_sanitize,
        );
        print!("{}", output);
    }

    // An empty relation still has a heading: formats whose contract can
    // carry one emit it for zero rows — table/tsv/csv print the header
    // line, box the header frame. Rows-only contracts (list) stay
    // zero-byte, like the machine formats with their own empty spellings
    // (json `[]`, jsonl zero lines, raw zero bytes) on their own paths.
    if total_rows == 0
        && !no_headers
        && matches!(
            output_format,
            crate::output_format::OutputFormat::Table
                | crate::output_format::OutputFormat::Tsv
                | crate::output_format::OutputFormat::Csv
                | crate::output_format::OutputFormat::Box
        )
    {
        let output =
            format_output_with_zebra(&columns, &[], output_format, zebra_mode, false, no_sanitize);
        print!("{}", output);
    }

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// Stream raw cell bytes to stdout (no text conversion, no formatting).
/// `-f json` / `-f jsonl` — the typed emission path.
/// Reads the protocol's nullable cells and column descriptors directly:
/// NULL is null (never the string "NULL"), numbers are unquoted when
/// the column's declared type is numeric AND the text round-trips, and
/// columns keep relation order. Streaming: json emits one valid array
/// across all batches (the old per-batch path emitted concatenated
/// arrays past 100 rows — invalid JSON); jsonl emits one object per
/// line, pipe-friendly.
fn display_results_json(
    session: &mut dyn DqlSession,
    dql: &str,
    array_mode: bool,
) -> Result<ResultMetadata> {
    use crate::output_format::json_object_row;
    use std::io::Write;

    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
    let descriptors: Vec<String> = qr.columns.iter().map(|c| c.descriptor.clone()).collect();

    let stdout = std::io::stdout().lock();
    let mut out = std::io::BufWriter::new(stdout);
    let mut total_rows = 0usize;

    if array_mode {
        out.write_all(b"[")?;
    }
    loop {
        let fr = session
            .fetch(&qr.handle, 100)
            .map_err(|e| anyhow::anyhow!("{}", e))?;
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
            let line = json_object_row(&columns, &descriptors, &cells);
            if array_mode {
                if total_rows > 0 {
                    out.write_all(b",")?;
                }
                out.write_all(b"\n  ")?;
                out.write_all(line.as_bytes())?;
            } else {
                out.write_all(line.as_bytes())?;
                out.write_all(b"\n")?;
            }
            total_rows += 1;
        }
    }
    if array_mode {
        if total_rows > 0 {
            out.write_all(b"\n")?;
        }
        out.write_all(b"]\n")?;
    }
    out.flush()?;

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

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

    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;
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
        eprintln!(
            "warning: -f raw writes verbatim bytes (terminal control \
             sequences included); intended for pipes and files"
        );
    }
    let mut stdout = std::io::stdout().lock();
    let mut total_rows = 0usize;

    loop {
        let fr = session
            .fetch(&qr.handle, 100)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

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

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// Run a DQL query and return structured results (no display).
pub fn run_dql_query(dql: &str, session: &mut dyn DqlSession) -> Result<QueryResults> {
    fetch_all(session, dql)
}

/// Execute a DQL query: query, display.
///
/// Receives a DqlSession trait object. Calls session.query() and session.fetch().
/// Nothing else.
///
/// When `sequential` is true, multi-query input is split client-side via
/// `split_queries()`. Each query is sent as a separate `session.query()`
/// call; earlier statements run for their effects and only the FINAL
/// statement's result is displayed/returned. Without `sequential`, multi-query
/// input is rejected by the relay per the protocol contract.
pub fn execute_query(
    source_code: &str,
    session: &mut dyn DqlSession,
    target_stage: Option<Stage>,
    output_format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
    sequential: bool,
) -> Result<Option<ResultMetadata>> {
    if sequential {
        let queries = delightql_core::api::split_queries(source_code)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        for q in &queries[..queries.len() - 1] {
            run_dql_query(q, session)?;
        }

        let last = queries.last().unwrap();
        return execute_single_query(
            last,
            session,
            target_stage,
            output_format,
            no_headers,
            no_sanitize,
        );
    }

    execute_single_query(
        source_code,
        session,
        target_stage,
        output_format,
        no_headers,
        no_sanitize,
    )
}

fn execute_single_query(
    source_code: &str,
    session: &mut dyn DqlSession,
    target_stage: Option<Stage>,
    output_format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
) -> Result<Option<ResultMetadata>> {
    let zebra_mode = ZEBRA_MODE.with(|z| *z.borrow());

    let compile_stage = match target_stage {
        Some(Stage::Sql) => Some("sql"),
        Some(Stage::AstUnresolved) => Some("ast-unresolved"),
        Some(Stage::AstResolved) => Some("ast-resolved"),
        Some(Stage::AstRefined) => Some("ast-refined"),
        Some(Stage::AstSql) => Some("ast-sql"),
        Some(Stage::Cst) => Some("cst"),
        _ => None,
    };
    if let Some(stage) = compile_stage {
        return display_compile_stage(
            session,
            stage,
            source_code,
            output_format,
            zebra_mode,
            no_headers,
            no_sanitize,
        );
    }

    let dql = match target_stage {
        Some(Stage::Sql)
        | Some(Stage::AstUnresolved)
        | Some(Stage::AstResolved)
        | Some(Stage::AstRefined)
        | Some(Stage::AstSql)
        | Some(Stage::Cst) => unreachable!("handled by display_compile_stage"),
        Some(Stage::ByteHash) => {
            let (columns, raw_rows) = fetch_all_raw(session, source_code)?;
            let bhash = crate::util::fingerprint::compute_byte_hash(&raw_rows);
            println!("{}", bhash);
            return Ok(Some(ResultMetadata {
                columns,
                row_count: raw_rows.len(),
            }));
        }
        Some(stage @ (Stage::Hash | Stage::TotalHash | Stage::Fingerprint)) => {
            let results = fetch_all(session, source_code)?;
            let fingerprint =
                crate::util::fingerprint::ResultFingerprint::from_results_only(&results)
                    .map_err(|e| anyhow::anyhow!("Failed to generate fingerprint: {}", e))?;
            // Three DISTINCT contracts (man dql-query): hash = data only;
            // totalhash = schema+data (column names participate); fingerprint
            // = the structured JSON. Collapsing them onto data_hash would
            // make totalhash blind to a column rename and fingerprint emit
            // a bare digest instead of the structured JSON its name promises.
            match stage {
                Stage::Hash => println!("{}", fingerprint.data_hash),
                Stage::TotalHash => println!("{}", fingerprint.result_hash),
                Stage::Fingerprint => println!(
                    "{}",
                    fingerprint
                        .to_json_string()
                        .map_err(|e| anyhow::anyhow!("Failed to render fingerprint: {}", e))?
                ),
                _ => unreachable!(),
            }
            return Ok(Some(ResultMetadata {
                columns: results.columns,
                row_count: results.row_count,
            }));
        }
        None | Some(Stage::Results) => source_code.to_string(),
    };

    if output_format == OutputFormat::Raw {
        let meta = display_results_raw(session, &dql)?;
        return Ok(Some(meta));
    }
    if matches!(output_format, OutputFormat::Json | OutputFormat::Jsonl) {
        let meta = display_results_json(session, &dql, output_format == OutputFormat::Json)?;
        return Ok(Some(meta));
    }

    let meta = display_results(
        session,
        &dql,
        output_format,
        zebra_mode,
        no_headers,
        no_sanitize,
    )?;
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
    output_format: OutputFormat,
    zebra_mode: Option<usize>,
    no_headers: bool,
    no_sanitize: bool,
) -> Result<Option<ResultMetadata>> {
    use crate::output_format::format_output_with_zebra;

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
    // header. Falling through to the display paths panics — their Raw
    // arm is (correctly) unreachable!(), so this caller must handle Raw
    // itself. `--to sql -f raw` is the clean just-the-SQL spelling.
    if output_format == OutputFormat::Raw {
        println!("{}", representation);
        return Ok(Some(ResultMetadata {
            columns,
            row_count: 1,
        }));
    }

    let display_rows = vec![vec![representation]];
    let output = format_output_with_zebra(
        &columns,
        &display_rows,
        output_format,
        zebra_mode,
        no_headers,
        no_sanitize,
    );
    print!("{}", output);
    Ok(Some(ResultMetadata {
        columns,
        row_count: 1,
    }))
}
