/// Module for creating special database from stdin relations
use anyhow::{bail, Result};
use rusqlite::Connection;
use std::io::{self, Read};
use std::path::PathBuf;

/// Read stdin and create special database file with input table
/// Returns the path to the temporary database file
pub fn create_special_database(format: &str) -> Result<PathBuf> {
    match format {
        "json" => create_json_relation(),
        _ => bail!(
            "Unsupported relation format: '{}'. Currently supported: json",
            format
        ),
    }
}

/// Read all stdin as JSON and create special database with input(packet TEXT) table
/// Returns path to temp file
fn create_json_relation() -> Result<PathBuf> {
    // Read all stdin
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;

    // Error on empty stdin
    if buffer.trim().is_empty() {
        bail!("Empty stdin provided with -r json. Expected JSON data.");
    }

    // Create temp file for special database
    let temp_path = std::env::temp_dir().join(format!("dql_special_{}.db", std::process::id()));

    // Create database at temp path
    let conn = Connection::open(&temp_path)?;

    // Create input table with packet column
    conn.execute("CREATE TABLE input (packet TEXT)", [])?;

    // Insert the JSON data as a single row
    conn.execute("INSERT INTO input (packet) VALUES (?1)", [&buffer])?;

    // Close connection to ensure data is flushed
    drop(conn);

    Ok(temp_path)
}
