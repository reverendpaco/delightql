//! Filemunge command handler
//!
//! Handles: dql tools filemunge --table name:format[:noheader] path ... '<query>'
//! Loads multiple tables from files (csv, tsv, json-singleton) into a temp DB,
//! then runs the user's DQL query against all of them.

use crate::args;
use crate::connection;
use crate::output_format::OutputFormat;
use anyhow::Result;
use rusqlite::Connection;
use std::io::Read;
use std::path::Path;

fn strop(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct TableSpec {
    name: String,
    format: TableFormat,
    has_headers: bool,
    path: String,
}

enum TableFormat {
    Csv,
    Tsv,
    JsonSingleton,
}

fn parse_table_spec(spec_part: &str, path: &str) -> Result<TableSpec> {
    let parts: Vec<&str> = spec_part.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        anyhow::bail!(
            "Invalid table spec '{}': expected name:format or name:format:noheader",
            spec_part
        );
    }

    let name = parts[0].to_string();
    if name.is_empty() {
        anyhow::bail!("Table name cannot be empty in spec '{}'", spec_part);
    }

    let (format, default_headers) = match parts[1] {
        "csv" => (TableFormat::Csv, true),
        "tsv" => (TableFormat::Tsv, true),
        "json-singleton" => (TableFormat::JsonSingleton, false),
        other => anyhow::bail!("Unknown format '{}'. Expected: csv, tsv, json-singleton", other),
    };

    let has_headers = if parts.len() == 3 {
        match parts[2] {
            "header" => true,
            "noheader" => false,
            other => anyhow::bail!(
                "Unknown header mode '{}'. Expected: header or noheader",
                other
            ),
        }
    } else {
        default_headers
    };

    Ok(TableSpec {
        name,
        format,
        has_headers,
        path: path.to_string(),
    })
}

fn load_csv_table(
    conn: &Connection,
    spec: &TableSpec,
    delimiter: u8,
) -> Result<()> {
    let data = read_file(&spec.path)?;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(spec.has_headers)
        .delimiter(delimiter)
        .from_reader(data.as_slice());

    let col_names: Vec<String> = if spec.has_headers {
        rdr.headers()?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        let width = match rdr.records().next() {
            Some(Ok(ref rec)) => rec.len(),
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("No records in '{}'", spec.path),
        };
        (1..=width).map(|i| format!("c{}", i)).collect()
    };

    if col_names.is_empty() {
        anyhow::bail!("Zero columns in '{}'", spec.path);
    }

    let col_defs: Vec<String> = col_names.iter().map(|n| strop(n)).collect();
    let table_name = strop(&spec.name);
    conn.execute(
        &format!("CREATE TABLE {} ({})", table_name, col_defs.join(", ")),
        [],
    )?;

    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("?{}", i)).collect();
    let insert_sql = format!(
        "INSERT INTO {} VALUES ({})",
        table_name,
        placeholders.join(", ")
    );

    // Re-read for noheader mode (first record consumed during width detection)
    if !spec.has_headers {
        drop(rdr);
        let mut rdr2 = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .from_reader(data.as_slice());
        insert_records(conn, &insert_sql, col_names.len(), rdr2.records())?;
    } else {
        insert_records(conn, &insert_sql, col_names.len(), rdr.records())?;
    }

    Ok(())
}

fn load_json_singleton_table(conn: &Connection, spec: &TableSpec) -> Result<()> {
    let data = read_file(&spec.path)?;
    let text = String::from_utf8(data)
        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in '{}': {}", spec.path, e))?;

    if text.trim().is_empty() {
        anyhow::bail!("Empty JSON file '{}'", spec.path);
    }

    let table_name = strop(&spec.name);
    conn.execute(
        &format!("CREATE TABLE {} (j TEXT)", table_name),
        [],
    )?;
    conn.execute(
        &format!("INSERT INTO {} (j) VALUES (?1)", table_name),
        [&text],
    )?;

    Ok(())
}

fn read_file(path: &str) -> Result<Vec<u8>> {
    let p = Path::new(path);
    // Support process substitution (/dev/fd/N) and regular files
    let mut f = std::fs::File::open(p)
        .map_err(|e| anyhow::anyhow!("Cannot open '{}': {}", path, e))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}

fn insert_records(
    conn: &Connection,
    insert_sql: &str,
    ncols: usize,
    records: csv::StringRecordsIter<&[u8]>,
) -> Result<()> {
    let mut stmt = conn.prepare(insert_sql)?;
    for rec in records {
        let rec = rec?;
        let params: Vec<Option<&str>> = (0..ncols).map(|i| rec.get(i)).collect();
        stmt.execute(rusqlite::params_from_iter(params.iter()))?;
    }
    Ok(())
}

pub fn handle_filemunge_command(
    query: &str,
    tables: &[String],
    format: Option<OutputFormat>,
    to: Option<args::Stage>,
    _base_args: &args::CliArgs,
) -> Result<()> {
    if tables.is_empty() {
        anyhow::bail!("No --table specs provided");
    }

    if tables.len() % 2 != 0 {
        anyhow::bail!("Each --table requires a SPEC and a PATH");
    }

    let specs: Vec<TableSpec> = tables
        .chunks(2)
        .map(|pair| parse_table_spec(&pair[0], &pair[1]))
        .collect::<Result<_>>()?;

    let temp_path =
        std::env::temp_dir().join(format!("dql_filemunge_{}.db", std::process::id()));

    {
        let conn = Connection::open(&temp_path)?;
        for spec in &specs {
            match spec.format {
                TableFormat::Csv => load_csv_table(&conn, spec, b',')?,
                TableFormat::Tsv => load_csv_table(&conn, spec, b'\t')?,
                TableFormat::JsonSingleton => load_json_singleton_table(&conn, spec)?,
            }
        }
    }

    let db_path_str = temp_path.to_string_lossy().to_string();
    let conn = connection::ConnectionManager::new_memory()?;
    let output_format = format.unwrap_or(OutputFormat::Table);

    let mut handle = conn.open_handle()?;
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;

    crate::exec_ng::run_dql_query(
        &format!("mount!(\"{}\", \"main\")", db_path_str),
        &mut *session,
    )?;

    let result =
        crate::exec_ng::execute_query(query, &mut *session, to, output_format, false, false, false);

    drop(session);
    drop(handle);
    drop(conn);
    let _ = std::fs::remove_file(&temp_path);

    result.map(|_| ())
}
