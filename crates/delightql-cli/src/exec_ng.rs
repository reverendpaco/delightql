/// Query execution module.
///
/// The CLI calls session.query() and session.fetch(). Nothing else
/// crosses the boundary.
use anyhow::Result;
use delightql_backends::QueryResults;
use delightql_core::api::DqlSession;
use delightql_protocol::decode_cell_to_text;

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
                        Some(bytes) => decode_cell_to_text(bytes),
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
                        Some(bytes) => decode_cell_to_text(bytes),
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

    let dql = match target_stage {
        Some(Stage::Sql) => compile_stage_dql("sql", source_code),
        Some(Stage::AstUnresolved) => compile_stage_dql("ast-unresolved", source_code),
        Some(Stage::AstResolved) => compile_stage_dql("ast-resolved", source_code),
        Some(Stage::AstRefined) => compile_stage_dql("ast-refined", source_code),
        Some(Stage::AstSql) => compile_stage_dql("ast-sql", source_code),
        Some(Stage::Cst) => compile_stage_dql("cst", source_code),
        Some(Stage::RecursionDepth) => compile_stage_dql("recursion-depth", source_code),
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
fn compile_stage_dql(stage: &str, source: &str) -> String {
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(source.as_bytes());
    format!("sys::execution.compile(\"{}\", b64:\"{}\")", stage, encoded)
}
