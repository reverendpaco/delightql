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

/// Fetch ALL rows from a DQL session into QueryResults.
fn fetch_all(session: &mut dyn DqlSession, dql: &str) -> Result<QueryResults> {
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
            all_rows.push(
                row.iter()
                    .map(|cell| match cell {
                        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect(),
            );
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
/// Used by `--to bhash` to hash bytes directly.
fn fetch_all_raw(
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

        let rows: Vec<Vec<String>> = fr
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect()
            })
            .collect();

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

    let _ = session
        .close(qr.handle)
        .map_err(|e| anyhow::anyhow!("{}", e));

    Ok(ResultMetadata {
        columns,
        row_count: total_rows,
    })
}

/// Stream raw cell bytes to stdout (no text conversion, no formatting).
fn display_results_raw(session: &mut dyn DqlSession, dql: &str) -> Result<ResultMetadata> {
    use std::io::Write;

    let qr = session.query(dql).map_err(|e| anyhow::anyhow!("{}", e))?;
    let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
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
/// call and all results are displayed. Without `sequential`, multi-query
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
        Some(Stage::RecursionDepth) => Some("recursion-depth"),
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
        | Some(Stage::Cst)
        | Some(Stage::RecursionDepth) => unreachable!("handled by display_compile_stage"),
        Some(Stage::ByteHash) => {
            let (columns, raw_rows) = fetch_all_raw(session, source_code)?;
            let bhash = crate::util::fingerprint::compute_byte_hash(&raw_rows);
            println!("{}", bhash);
            return Ok(Some(ResultMetadata {
                columns,
                row_count: raw_rows.len(),
            }));
        }
        Some(Stage::Hash) | Some(Stage::TotalHash) | Some(Stage::Fingerprint) => {
            let results = fetch_all(session, source_code)?;
            let fingerprint =
                crate::util::fingerprint::ResultFingerprint::from_results_only(&results)
                    .map_err(|e| anyhow::anyhow!("Failed to generate fingerprint: {}", e))?;
            println!("{}", fingerprint.data_hash);
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
        "sys::execution.compile(\"{}\", b64:\"{}\") |> (representation, error)",
        stage, encoded
    )
}

/// Display a compile stage (`--to sql`, `--to ast-*`, …). A failed compile
/// surfaces its error and exits non-zero — the inspection surface must
/// never print a literal NULL where the user asked to see the compilation
/// (RECURSION-CONTRACT.md B4).
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
        let uri = String::from_utf8_lossy(uri);
        anyhow::bail!(
            "compilation failed: {uri}\n\
             (run `dql explain {uri}` for the identifier's prose, or run \
             the query without --to for the full message)"
        );
    }

    let representation = match &row[0] {
        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
        None => anyhow::bail!("sys::execution.compile returned neither output nor error"),
    };
    let columns = vec!["representation".to_string()];
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
