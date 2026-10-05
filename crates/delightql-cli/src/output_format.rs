// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Output format handling for DelightQL CLI
///
/// This module provides functionality for formatting query results in different
/// output formats including table, JSON, CSV, TSV, list, and box formats.
use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Table, // Default pipe-delimited table
    Box,   // Unicode box-drawing table (like SQLite's .mode box)
    Json,  // JSON array of objects
    Jsonl, // One JSON object per row (streams; composes with jq)
    Csv,   // Comma-separated values
    Tsv,   // Tab-separated values
    List,  // Key=value pairs
    Raw,   // Raw bytes (no formatting, no text conversion)
    /// One machine value standing for the whole executed result.
    Digest(Digest),
}

/// A digest of an executed result: the rendering `--to hash`,
/// `--to totalhash` and `--to fingerprint` print for a pure statement, and
/// the rendering an EXECUTING road (`--to results -f hash` and siblings)
/// prints after running the statement, effects included.
///
/// Three distinct contracts over one digest of the protocol cells (man
/// dql-query): hash = data only; totalhash = schema+data (column names
/// participate); fingerprint = the structured JSON. Collapsing them would
/// make totalhash blind to a column rename and fingerprint emit a bare
/// digest instead of the structured JSON its name promises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Digest {
    Hash,
    TotalHash,
    Fingerprint,
}

impl OutputFormat {
    /// The digest this format is, if it is one. A digest is a single
    /// machine value, so no console sink ships mid-run sets beside it.
    pub fn digest(self) -> Option<Digest> {
        match self {
            OutputFormat::Digest(digest) => Some(digest),
            _ => None,
        }
    }
}

impl OutputFormat {
    /// Resolve output format from three sources (highest priority first):
    /// 1. Explicit `--format` flag
    /// 2. `$DQL_FORMAT` env var
    /// 3. Auto-detect: TTY → Box (Table fallback if no unicode), pipe → TSV
    pub fn resolve(explicit: Option<OutputFormat>) -> OutputFormat {
        if let Some(fmt) = explicit {
            return fmt;
        }
        if let Ok(val) = std::env::var("DQL_FORMAT") {
            if let Some(fmt) = OutputFormat::from_str(&val) {
                return fmt;
            }
        }
        if io::stdout().is_terminal() {
            if should_use_box_format() {
                OutputFormat::Box
            } else {
                OutputFormat::Table
            }
        } else {
            OutputFormat::Tsv
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "table" => Some(OutputFormat::Table),
            "box" => Some(OutputFormat::Box),
            "json" => Some(OutputFormat::Json),
            "jsonl" => Some(OutputFormat::Jsonl),
            "csv" => Some(OutputFormat::Csv),
            "tsv" => Some(OutputFormat::Tsv),
            "list" => Some(OutputFormat::List),
            "raw" => Some(OutputFormat::Raw),
            "hash" => Some(OutputFormat::Digest(Digest::Hash)),
            "totalhash" => Some(OutputFormat::Digest(Digest::TotalHash)),
            "fingerprint" => Some(OutputFormat::Digest(Digest::Fingerprint)),
            _ => None,
        }
    }

    /// The spelling `--format` and `.format` take for this format.
    pub fn name(self) -> &'static str {
        match self {
            OutputFormat::Table => "table",
            OutputFormat::Box => "box",
            OutputFormat::Json => "json",
            OutputFormat::Jsonl => "jsonl",
            OutputFormat::Csv => "csv",
            OutputFormat::Tsv => "tsv",
            OutputFormat::List => "list",
            OutputFormat::Raw => "raw",
            OutputFormat::Digest(Digest::Hash) => "hash",
            OutputFormat::Digest(Digest::TotalHash) => "totalhash",
            OutputFormat::Digest(Digest::Fingerprint) => "fingerprint",
        }
    }

    pub fn all_formats() -> &'static [&'static str] {
        &[
            "table",
            "box",
            "json",
            "jsonl",
            "csv",
            "tsv",
            "list",
            "raw",
            "hash",
            "totalhash",
            "fingerprint",
        ]
    }
}

/// Format query results with optional header suppression and sanitization.
pub fn format_output(
    columns: &[String],
    rows: &[Vec<Option<String>>],
    format: OutputFormat,
    no_headers: bool,
    no_sanitize: bool,
) -> String {
    // THE FORMATS WITH A READER take their cells still nullable: each owns
    // one representation of absence and one escape law, so a reader
    // recovers every value — SQL NULL, the text `NULL` and the empty text
    // are three values and render three ways.
    //
    // TWO LAWS MEET HERE, and the terminal-safety law is not repealed by a
    // format having a reader: what the CLI prints reaches a terminal unless
    // the user says otherwise, and a record format is what a pipe gets by
    // default. So the ordinary rendering is a DISPLAY of the value — a
    // dangerous control byte becomes its `\xHH` spelling before the
    // format's own escaping, exactly as on the console — and the explicit
    // opt-out is the faithful serialization. The display is injective too:
    // the safety spelling is reserved (`sanitize.rs`), tab, newline,
    // quotes, carriage return and absence are left to the format's own
    // encoding, so the values the observation contract separates stay
    // separate either way. JSON needs no such step: its escape spells
    // every control character as `\u00XX`, which is both safe and
    // faithful.
    match format {
        OutputFormat::Json => return format_as_json(columns, rows),
        OutputFormat::Jsonl => return format_as_jsonl(columns, rows),
        OutputFormat::Csv | OutputFormat::Tsv => {
            let (columns, rows) = if no_sanitize {
                (columns.to_vec(), rows.to_vec())
            } else {
                sanitize_nullable(columns, rows)
            };
            return match format {
                OutputFormat::Csv => format_as_csv(&columns, &rows, no_headers),
                _ => format_as_tsv(&columns, &rows, no_headers),
            };
        }
        _ => {}
    }
    let rows: Vec<Vec<String>> = rows.iter().map(|row| console_cells(row)).collect();
    let rows = &rows;

    // Sanitize cell values unless opted out.
    let needs_sanitize = !no_sanitize
        && (columns
            .iter()
            .any(|c| crate::sanitize::needs_sanitization(c))
            || rows
                .iter()
                .any(|r| r.iter().any(|c| crate::sanitize::needs_sanitization(c))));

    if needs_sanitize {
        let (safe_cols, safe_rows, widths) =
            crate::sanitize::sanitize_rows_with_widths(columns, rows);
        format_output_inner(&safe_cols, &safe_rows, format, no_headers, &widths)
    } else {
        let widths = crate::sanitize::compute_column_widths(columns, rows);
        format_output_inner(columns, rows, format, no_headers, &widths)
    }
}

/// The terminal-safety pass over nullable cells: text takes the reserved
/// safety spelling, absence stays absent.
fn sanitize_nullable(
    columns: &[String],
    rows: &[Vec<Option<String>>],
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let columns = columns
        .iter()
        .map(|name| crate::sanitize::sanitize_record_text(name).into_owned())
        .collect();
    let rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_deref()
                        .map(|text| crate::sanitize::sanitize_record_text(text).into_owned())
                })
                .collect()
        })
        .collect();
    (columns, rows)
}

/// One row as the console shows it: THE DISPLAY BOUNDARY, and the only
/// kind of place a cell's absence is allowed to become the four characters
/// `NULL` — what is printed here has no reader that could mistake it for
/// the value again.
pub fn console_cells(row: &[Option<String>]) -> Vec<String> {
    row.iter()
        .map(|cell| cell.clone().unwrap_or_else(|| "NULL".to_string()))
        .collect()
}

/// Max column width before we abandon box formatting and fall back to plain table.
/// Box format pads every cell to the widest value, which is wasteful for very wide data.
const MAX_BOX_COLUMN_WIDTH: usize = 200;

/// Inner dispatch after optional sanitization
fn format_output_inner(
    columns: &[String],
    rows: &[Vec<String>],
    format: OutputFormat,
    no_headers: bool,
    column_widths: &[usize],
) -> String {
    match format {
        OutputFormat::Table => format_as_table(columns, rows, no_headers),
        OutputFormat::Box => {
            // Fall back to plain table if any column is too wide for box formatting
            let too_wide = column_widths.iter().any(|&w| w > MAX_BOX_COLUMN_WIDTH);
            if !too_wide && should_use_box_format() {
                format_as_box(columns, rows, no_headers, column_widths)
            } else {
                format_as_table(columns, rows, no_headers)
            }
        }
        OutputFormat::List => format_as_list(columns, rows),
        OutputFormat::Json | OutputFormat::Jsonl | OutputFormat::Csv | OutputFormat::Tsv => {
            unreachable!(
                "a format with a reader renders its nullable cells before the console road"
            )
        }
        OutputFormat::Raw => unreachable!("Raw format handled before display_results"),
        OutputFormat::Digest(_) => unreachable!("a digest is rendered by exec_ng::render_digest"),
    }
}

/// Check if we should use box format (terminal supports it and not piped)
fn should_use_box_format() -> bool {
    // Check if stdout is a terminal (not piped/redirected)
    io::stdout().is_terminal() && supports_unicode()
}

/// Check if terminal supports Unicode
fn supports_unicode() -> bool {
    // Check LANG/LC_ALL environment variables for UTF-8 support
    std::env::var("LANG")
        .unwrap_or_default()
        .to_uppercase()
        .contains("UTF-8")
        || std::env::var("LANG")
            .unwrap_or_default()
            .to_uppercase()
            .contains("UTF8")
        || std::env::var("LC_ALL")
            .unwrap_or_default()
            .to_uppercase()
            .contains("UTF-8")
        || std::env::var("LC_ALL")
            .unwrap_or_default()
            .to_uppercase()
            .contains("UTF8")
}

fn format_as_table(columns: &[String], rows: &[Vec<String>], no_headers: bool) -> String {
    let mut output = String::new();

    if !columns.is_empty() && !no_headers {
        output.push_str(&columns.join(" | "));
        output.push('\n');
        let dashes: Vec<String> = columns.iter().map(|col| "-".repeat(col.len())).collect();
        output.push_str(&dashes.join("-|-"));
        output.push('\n');
    }

    for row in rows {
        output.push_str(&row.join(" | "));
        output.push('\n');
    }

    output
}

/// Left-pad `text` to `width` characters without using `format!` width parameter,
/// which panics when width > 65535.
fn pad_left(text: &str, width: usize) -> String {
    let len = text.chars().count();
    if len >= width {
        text.to_string()
    } else {
        let mut s = text.to_string();
        s.extend(std::iter::repeat(' ').take(width - len));
        s
    }
}

/// Center `text` in `width` characters.
fn pad_center(text: &str, width: usize) -> String {
    let len = text.chars().count();
    if len >= width {
        text.to_string()
    } else {
        let total = width - len;
        let left = total / 2;
        let right = total - left;
        let mut s = String::with_capacity(width);
        s.extend(std::iter::repeat(' ').take(left));
        s.push_str(text);
        s.extend(std::iter::repeat(' ').take(right));
        s
    }
}

fn format_as_box(
    columns: &[String],
    rows: &[Vec<String>],
    no_headers: bool,
    column_widths: &[usize],
) -> String {
    let mut output = String::new();

    // Handle empty result set with columns
    if columns.is_empty() {
        return output;
    }

    // If no_headers is true, just output rows in simple format
    if no_headers {
        // Output rows without box drawing
        for row in rows {
            output.push_str(&row.join(" | "));
            output.push('\n');
        }
        return output;
    }

    // Use pre-computed column widths, add padding (1 space on each side)
    let widths: Vec<usize> = column_widths.iter().map(|w| w + 2).collect();

    // Box drawing characters
    const TOP_LEFT: char = '┌';
    const TOP_RIGHT: char = '┐';
    const BOTTOM_LEFT: char = '└';
    const BOTTOM_RIGHT: char = '┘';
    const HORIZONTAL: char = '─';
    const VERTICAL: char = '│';
    const TOP_JUNCTION: char = '┬';
    const BOTTOM_JUNCTION: char = '┴';
    const LEFT_JUNCTION: char = '├';
    const RIGHT_JUNCTION: char = '┤';
    const CROSS: char = '┼';

    // Helper function to draw a horizontal line
    let draw_line = |left: char, middle: char, right: char| -> String {
        let mut line = String::new();
        line.push(left);
        for (i, width) in widths.iter().enumerate() {
            for _ in 0..*width {
                line.push(HORIZONTAL);
            }
            if i < widths.len() - 1 {
                line.push(middle);
            }
        }
        line.push(right);
        line.push('\n');
        line
    };

    // Draw top border
    output.push_str(&draw_line(TOP_LEFT, TOP_JUNCTION, TOP_RIGHT));

    // Draw header row
    output.push(VERTICAL);
    for (i, col) in columns.iter().enumerate() {
        output.push_str(&format!(" {} ", pad_left(col, widths[i] - 2)));
        output.push(VERTICAL);
    }
    output.push('\n');

    // Draw header separator
    if !rows.is_empty() {
        output.push_str(&draw_line(LEFT_JUNCTION, CROSS, RIGHT_JUNCTION));
    }

    // Draw data rows
    if rows.is_empty() {
        // Special case for empty result set
        output.push_str(&draw_line(LEFT_JUNCTION, BOTTOM_JUNCTION, RIGHT_JUNCTION));
        output.push(VERTICAL);
        let total_width: usize = widths.iter().sum::<usize>() + widths.len() - 1;
        let no_results = "(no results)";
        output.push_str(&pad_center(no_results, total_width));
        output.push(VERTICAL);
        output.push('\n');
    } else {
        for row in rows {
            output.push(VERTICAL);
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    output.push_str(&format!(" {} ", pad_left(cell, widths[i] - 2)));
                } else {
                    // Handle row with more cells than columns (shouldn't happen normally)
                    output.push_str(&format!(" {} ", cell));
                }
                output.push(VERTICAL);
            }
            // Handle row with fewer cells than columns
            for width in widths.iter().take(columns.len()).skip(row.len()) {
                output.push_str(&format!(" {:<width$} ", "", width = width - 2));
                output.push(VERTICAL);
            }
            output.push('\n');
        }
    }

    // Draw bottom border
    output.push_str(&draw_line(BOTTOM_LEFT, BOTTOM_JUNCTION, BOTTOM_RIGHT));

    output
}

/// SQLite type affinity of a declared type, reduced to what JSON
/// emission needs.
enum JsonAffinity {
    Integer,
    Numeric, // REAL / NUMERIC / BOOLEAN — try integer, then float
    Text,
}

fn json_affinity(descriptor: &str) -> JsonAffinity {
    let d = descriptor.to_ascii_uppercase();
    if d.contains("INT") {
        JsonAffinity::Integer
    } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        JsonAffinity::Text
    } else if d.is_empty() || d.contains("BLOB") {
        // No declaration (expression/CTE column) or blob: no knowledge,
        // no typed strengthening — the typed-gate doctrine at the
        // presentation seam. Strings.
        JsonAffinity::Text
    } else {
        JsonAffinity::Numeric // REAL, FLOAT, DOUBLE, NUMERIC, DECIMAL, BOOLEAN
    }
}

/// One JSON scalar from a nullable cell + its column's declared type.
/// Numbers are emitted ONLY when the declaration is numeric AND the
/// text round-trips exactly (parse-then-reprint equality) — a TEXT
/// '007' stays "007", a REAL '3.50' stays "3.50". NULL is null.
/// Emission never loses bytes; it only unquotes what is provably a
/// number — the JSON formatter must not lie about types.
pub fn json_cell(descriptor: &str, cell: Option<&str>) -> String {
    let Some(s) = cell else {
        return "null".to_string();
    };
    match json_affinity(descriptor) {
        JsonAffinity::Text => json_escape(s),
        JsonAffinity::Integer | JsonAffinity::Numeric => {
            if let Ok(i) = s.parse::<i64>() {
                if i.to_string() == s {
                    return s.to_string();
                }
            }
            if let Ok(f) = s.parse::<f64>() {
                if f.is_finite() && f.to_string() == s {
                    return s.to_string();
                }
            }
            json_escape(s) // declared numeric but doesn't round-trip: keep the bytes
        }
    }
}

/// One JSON object per row, columns in RELATION ORDER (the old serde
/// Map alphabetized them), compact.
pub fn json_object_row(
    columns: &[String],
    descriptors: &[String],
    row: &[Option<String>],
) -> String {
    let mut out = String::from("{");
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&json_escape(col));
        out.push_str(": ");
        let desc = descriptors.get(i).map(|s| s.as_str()).unwrap_or("");
        out.push_str(&json_cell(desc, row.get(i).and_then(|c| c.as_deref())));
    }
    out.push('}');
    out
}

/// JSON string escaping per RFC 8259: quotes, backslash, and all
/// control chars (so terminal-injection sanitization is inherent).
/// Also used by main's --error-format json records.
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// JSON without descriptors (the tools paths): NULL is `null`, every
/// other cell a string, in relation order. The typed road with declared
/// column descriptors is exec_ng's `json_document`.
fn format_as_json(columns: &[String], rows: &[Vec<Option<String>>]) -> String {
    if rows.is_empty() {
        return "[]\n".to_string();
    }
    let descriptors: Vec<String> = vec![String::new(); columns.len()];
    let mut out = String::from("[\n");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str("  ");
        out.push_str(&json_object_row(columns, &descriptors, row));
    }
    out.push_str("\n]\n");
    out
}

fn format_as_jsonl(columns: &[String], rows: &[Vec<Option<String>>]) -> String {
    let descriptors: Vec<String> = vec![String::new(); columns.len()];
    let mut out = String::new();
    for row in rows {
        out.push_str(&json_object_row(columns, &descriptors, row));
        out.push('\n');
    }
    out
}

/// CSV, one record per line, the heading first unless suppressed.
fn format_as_csv(columns: &[String], rows: &[Vec<Option<String>>], no_headers: bool) -> String {
    let mut output = String::new();
    if !columns.is_empty() && !no_headers {
        output.push_str(&join_fields(
            columns.iter().map(|name| csv_field(Some(name))),
            ",",
        ));
        output.push('\n');
    }
    for row in rows {
        output.push_str(&join_fields(
            row.iter().map(|cell| csv_field(cell.as_deref())),
            ",",
        ));
        output.push('\n');
    }
    output
}

/// TSV, one record per line, the heading first unless suppressed.
fn format_as_tsv(columns: &[String], rows: &[Vec<Option<String>>], no_headers: bool) -> String {
    let mut output = String::new();
    if !columns.is_empty() && !no_headers {
        output.push_str(&join_fields(
            columns.iter().map(|name| tsv_field(Some(name))),
            "\t",
        ));
        output.push('\n');
    }
    for row in rows {
        output.push_str(&join_fields(
            row.iter().map(|cell| tsv_field(cell.as_deref())),
            "\t",
        ));
        output.push('\n');
    }
    output
}

fn join_fields(fields: impl Iterator<Item = String>, separator: &str) -> String {
    fields.collect::<Vec<_>>().join(separator)
}

fn format_as_list(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut output = String::new();

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            output.push('\n');
        }

        for (i, column) in columns.iter().enumerate() {
            let value = row.get(i).map(|s| s.as_str()).unwrap_or("");
            output.push_str(&format!("{} = {}\n", column, value));
        }
    }

    output
}

/// THE CSV CELL LAW. SQL NULL is the empty unquoted field; the empty text
/// is `""`; text holding a comma, a quote or a line break is quoted with
/// its quotes doubled; every other text is itself. So the text `NULL`
/// renders `NULL`, and the three are pairwise distinct.
fn csv_field(cell: Option<&str>) -> String {
    match cell {
        None => String::new(),
        Some("") => "\"\"".to_string(),
        Some(field) if field.contains([',', '"', '\n', '\r']) => {
            format!("\"{}\"", field.replace('"', "\"\""))
        }
        Some(field) => field.to_string(),
    }
}

/// THE TSV CELL LAW. SQL NULL is `\N`; text escapes its own alphabet — a
/// backslash as `\\`, a tab as `\t`, a newline as `\n`, a carriage return
/// as `\r` — and every other byte is literal. Escaping the backslash first
/// is what keeps the law injective: the text `\n` renders `\\n`, a newline
/// renders `\n`, and the text `\N` renders `\\N`.
fn tsv_field(cell: Option<&str>) -> String {
    match cell {
        None => "\\N".to_string(),
        Some(field) => field
            .replace('\\', "\\\\")
            .replace('\t', "\\t")
            .replace('\n', "\\n")
            .replace('\r', "\\r"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nullable(rows: &[Vec<String>]) -> Vec<Vec<Option<String>>> {
        rows.iter()
            .map(|row| row.iter().cloned().map(Some).collect())
            .collect()
    }

    fn boxed(columns: &[String], rows: &[Vec<String>]) -> String {
        let widths = crate::sanitize::compute_column_widths(columns, rows);
        format_as_box(columns, rows, false, &widths)
    }

    #[test]
    fn test_output_format_from_str() {
        assert_eq!(OutputFormat::from_str("table"), Some(OutputFormat::Table));
        assert_eq!(OutputFormat::from_str("TABLE"), Some(OutputFormat::Table));
        assert_eq!(OutputFormat::from_str("box"), Some(OutputFormat::Box));
        assert_eq!(OutputFormat::from_str("BOX"), Some(OutputFormat::Box));
        assert_eq!(OutputFormat::from_str("json"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::from_str("JSON"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::from_str("csv"), Some(OutputFormat::Csv));
        assert_eq!(OutputFormat::from_str("CSV"), Some(OutputFormat::Csv));
        assert_eq!(OutputFormat::from_str("tsv"), Some(OutputFormat::Tsv));
        assert_eq!(OutputFormat::from_str("TSV"), Some(OutputFormat::Tsv));
        assert_eq!(OutputFormat::from_str("list"), Some(OutputFormat::List));
        assert_eq!(OutputFormat::from_str("LIST"), Some(OutputFormat::List));
        assert_eq!(OutputFormat::from_str("raw"), Some(OutputFormat::Raw));
        assert_eq!(OutputFormat::from_str("RAW"), Some(OutputFormat::Raw));
        assert_eq!(OutputFormat::from_str("invalid"), None);
        assert_eq!(OutputFormat::from_str(""), None);
    }

    #[test]
    fn test_output_format_all_formats() {
        let formats = OutputFormat::all_formats();
        assert_eq!(formats.len(), 11);
        assert!(formats.contains(&"hash"));
        assert!(formats.contains(&"fingerprint"));
        assert!(formats.contains(&"jsonl"));
        assert!(formats.contains(&"table"));
        assert!(formats.contains(&"box"));
        assert!(formats.contains(&"json"));
        assert!(formats.contains(&"csv"));
        assert!(formats.contains(&"tsv"));
        assert!(formats.contains(&"list"));
    }

    /// Every spelling reads as the format whose name it is.
    #[test]
    fn every_format_is_named_by_its_spelling() {
        for spelling in OutputFormat::all_formats() {
            let format = OutputFormat::from_str(spelling).expect("a listed spelling parses");
            assert_eq!(format.name(), *spelling);
        }
    }

    #[test]
    fn test_format_as_table() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];

        let result = format_as_table(&columns, &rows, false);
        let expected = "name | age\n-----|----\nAlice | 30\nBob | 25\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_as_json() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];

        let rows = nullable(&rows);
        let result = format_as_json(&columns, &rows);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 2);

        let first_row = &parsed[0];
        assert_eq!(first_row["name"], "Alice");
        assert_eq!(first_row["age"], "30");
    }

    #[test]
    fn test_format_as_csv() {
        let columns = vec!["name".to_string(), "city".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "New York".to_string()],
            vec!["Bob".to_string(), "Los Angeles".to_string()],
        ];

        let result = format_as_csv(&columns, &nullable(&rows), false);
        let expected = "name,city\nAlice,New York\nBob,Los Angeles\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_as_tsv() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];

        let result = format_as_tsv(&columns, &nullable(&rows), false);
        let expected = "name\tage\nAlice\t30\nBob\t25\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_format_as_list() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];

        let result = format_as_list(&columns, &rows);
        let expected = "name = Alice\nage = 30\n\nname = Bob\nage = 25\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_csv_escaping() {
        let columns = vec!["name".to_string(), "description".to_string()];
        let rows = vec![
            vec!["John, Jr.".to_string(), "A \"nice\" person".to_string()],
            vec!["Jane".to_string(), "Normal text".to_string()],
        ];

        let result = format_as_csv(&columns, &nullable(&rows), false);
        assert!(result.contains("\"John, Jr.\""));
        assert!(result.contains("\"A \"\"nice\"\" person\""));
        assert!(result.contains("Jane"));
    }

    #[test]
    fn test_tsv_escaping() {
        let columns = vec!["name".to_string(), "notes".to_string()];
        let rows = vec![vec![
            "Alice".to_string(),
            "Has\ttabs and\nnewlines".to_string(),
        ]];

        let result = format_as_tsv(&columns, &nullable(&rows), false);
        assert!(result.contains("Has\\ttabs and\\nnewlines"));
    }

    /// THE CROSSING: a dangerous control byte in a record format is spelled
    /// out unless the opt-out is explicit, in CSV and TSV alike, and the
    /// opt-out yields the raw byte. The observation contract's values stay
    /// distinct on both sides of the crossing.
    #[test]
    fn record_formats_cross_the_terminal_safety_boundary_like_the_console() {
        let columns = vec!["x".to_string()];
        let red = vec![vec![Some("\x1b[31mRED".to_string())]];
        for format in [OutputFormat::Csv, OutputFormat::Tsv] {
            let displayed = format_output(&columns, &red, format, true, false);
            assert!(
                !displayed.contains('\x1b'),
                "{format:?} let ESC through: {displayed:?}"
            );
            assert!(
                displayed.contains("\\x1B[31mRED"),
                "{format:?}: {displayed:?}"
            );
            let faithful = format_output(&columns, &red, format, true, true);
            assert!(
                faithful.contains("\x1b[31mRED"),
                "{format:?} opt-out: {faithful:?}"
            );
            // ESC and the authored text `\x1B` are two values on both sides.
            let authored = vec![vec![Some("\\x1B[31mRED".to_string())]];
            for no_sanitize in [false, true] {
                assert_ne!(
                    format_output(&columns, &red, format, true, no_sanitize),
                    format_output(&columns, &authored, format, true, no_sanitize),
                    "{format:?} no_sanitize={no_sanitize}: ESC and authored \\x1B collide"
                );
            }

            let three = vec![
                vec![None],
                vec![Some("NULL".to_string())],
                vec![Some(String::new())],
            ];
            let rendered = |rows: &[Vec<Option<String>>], no_sanitize: bool| -> Vec<String> {
                format_output(&columns, rows, format, true, no_sanitize)
                    .lines()
                    .map(str::to_string)
                    .collect()
            };
            for no_sanitize in [false, true] {
                let lines = rendered(&three, no_sanitize);
                assert_eq!(lines.len(), 3);
                assert_ne!(lines[0], lines[1]);
                assert_ne!(lines[1], lines[2]);
                assert_ne!(lines[0], lines[2]);
                let spelled = vec![
                    vec![Some("a\\nb".to_string())],
                    vec![Some("a\nb".to_string())],
                ];
                let lines = rendered(&spelled, no_sanitize);
                assert_ne!(lines[0], lines[1], "{format:?} no_sanitize={no_sanitize}");
            }
        }
    }

    /// The record formats are injective over distinct cells: SQL NULL, the
    /// text `NULL` and the empty text render three ways, and text spelling
    /// an escape is told from the character it spells.
    #[test]
    fn csv_and_tsv_tell_every_value_apart() {
        let columns = vec!["x".to_string()];
        let three = vec![
            vec![None],
            vec![Some("NULL".to_string())],
            vec![Some(String::new())],
        ];
        assert_eq!(format_as_csv(&columns, &three, true), "\nNULL\n\"\"\n");
        assert_eq!(format_as_tsv(&columns, &three, true), "\\N\nNULL\n\n");

        let spelled = |text: &str| vec![vec![Some(text.to_string())]];
        assert_ne!(
            format_as_tsv(&columns, &spelled("a\\nb"), true),
            format_as_tsv(&columns, &spelled("a\nb"), true)
        );
        assert_ne!(
            format_as_tsv(&columns, &spelled("a\\tb"), true),
            format_as_tsv(&columns, &spelled("a\tb"), true)
        );
        assert_eq!(format_as_tsv(&columns, &spelled("\\N"), true), "\\\\N\n");
        assert_eq!(
            format_as_csv(&columns, &spelled("a\nb"), true),
            "\"a\nb\"\n"
        );
    }

    #[test]
    fn test_empty_data() {
        let columns = vec![];
        let rows = vec![];

        assert_eq!(format_as_table(&columns, &rows, false), "");
        assert_eq!(format_as_json(&columns, &nullable(&rows)), "[]\n");
        assert_eq!(format_as_csv(&columns, &nullable(&rows), false), "");
        assert_eq!(format_as_tsv(&columns, &nullable(&rows), false), "");
        assert_eq!(format_as_list(&columns, &rows), "");
    }

    #[test]
    fn test_empty_rows_with_headers() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![];

        let table_result = format_as_table(&columns, &rows, false);
        assert!(table_result.contains("name | age"));

        let csv_result = format_as_csv(&columns, &nullable(&rows), false);
        assert_eq!(csv_result, "name,age\n");

        let json_result = format_as_json(&columns, &nullable(&rows));
        assert_eq!(json_result, "[]\n");
    }

    #[test]
    fn test_format_as_box() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
        ];

        let result = boxed(&columns, &rows);

        // Check for box drawing characters
        assert!(result.contains('┌'));
        assert!(result.contains('┐'));
        assert!(result.contains('└'));
        assert!(result.contains('┘'));
        assert!(result.contains('│'));
        assert!(result.contains('─'));
        assert!(result.contains('┬'));
        assert!(result.contains('┴'));
        assert!(result.contains('├'));
        assert!(result.contains('┤'));
        assert!(result.contains('┼'));

        // Check content
        assert!(result.contains("id"));
        assert!(result.contains("name"));
        assert!(result.contains("age"));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
        assert!(result.contains("30"));
        assert!(result.contains("25"));
    }

    #[test]
    fn test_format_as_box_empty_results() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows = vec![];

        let result = boxed(&columns, &rows);

        // Check for box drawing characters
        assert!(result.contains('┌'));
        assert!(result.contains('┐'));
        assert!(result.contains('└'));
        assert!(result.contains('┘'));
        assert!(result.contains('│'));

        // Check for headers
        assert!(result.contains("id"));
        assert!(result.contains("name"));

        // Check for "no results" message
        assert!(result.contains("(no results)"));
    }

    #[test]
    fn test_format_as_box_column_width() {
        let columns = vec!["x".to_string(), "long_column_name".to_string()];
        let rows = vec![
            vec!["short".to_string(), "y".to_string()],
            vec!["a".to_string(), "very long value here".to_string()],
        ];

        let result = boxed(&columns, &rows);

        // The columns should be padded appropriately
        // First column should be at least as wide as "short" + padding
        // Second column should be as wide as "very long value here" + padding
        let lines: Vec<&str> = result.lines().collect();

        // Check that all lines with vertical bars have consistent positions
        for line in &lines {
            if line.contains('│') {
                // Each data line should have the same structure
                let pipes: Vec<_> = line
                    .char_indices()
                    .filter(|(_, c)| *c == '│')
                    .map(|(i, _)| i)
                    .collect();
                // Should have consistent pipe positions
                assert!(pipes.len() >= 2);
            }
        }
    }

    #[test]
    fn test_format_as_box_single_column() {
        let columns = vec!["value".to_string()];
        let rows = vec![vec!["123".to_string()], vec!["456".to_string()]];

        let result = boxed(&columns, &rows);

        // Check it handles single column correctly
        assert!(result.contains("value"));
        assert!(result.contains("123"));
        assert!(result.contains("456"));

        // Should not have any junction characters (┬, ┴, ┼) since only one column
        assert!(!result.contains('┬'));
        assert!(!result.contains('┴'));
        assert!(!result.contains('┼'));
    }
}
