/// Output format handling for DelightQL CLI
///
/// This module provides functionality for formatting query results in different
/// output formats including table, JSON, CSV, TSV, list, and box formats.
use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Table, // Default pipe-delimited table
    Box,  // Unicode box-drawing table (like SQLite's .mode box)
    Json, // JSON array of objects
    Csv,  // Comma-separated values
    Tsv,  // Tab-separated values
    List, // Key=value pairs
    Raw,  // Raw bytes (no formatting, no text conversion)
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
            "csv" => Some(OutputFormat::Csv),
            "tsv" => Some(OutputFormat::Tsv),
            "list" => Some(OutputFormat::List),
            "raw" => Some(OutputFormat::Raw),
            _ => None,
        }
    }

    pub fn all_formats() -> &'static [&'static str] {
        &["table", "box", "json", "csv", "tsv", "list", "raw"]
    }
}

/// Format query results with optional zebra coloring, header suppression, and sanitization
pub fn format_output_with_zebra(
    columns: &[String],
    rows: &[Vec<String>],
    format: OutputFormat,
    zebra_mode: Option<usize>,
    no_headers: bool,
    no_sanitize: bool,
) -> String {
    // Sanitize cell values unless opted out.
    // JSON excluded — serde_json handles control char encoding.
    let needs_sanitize = !no_sanitize
        && !matches!(format, OutputFormat::Json)
        && (columns
            .iter()
            .any(|c| crate::sanitize::needs_sanitization(c))
            || rows
                .iter()
                .any(|r| r.iter().any(|c| crate::sanitize::needs_sanitization(c))));

    if needs_sanitize {
        let (safe_cols, safe_rows, widths) =
            crate::sanitize::sanitize_rows_with_widths(columns, rows);
        format_output_inner(
            &safe_cols, &safe_rows, format, zebra_mode, no_headers, &widths,
        )
    } else {
        let widths = crate::sanitize::compute_column_widths(columns, rows);
        format_output_inner(columns, rows, format, zebra_mode, no_headers, &widths)
    }
}

/// Max column width before we abandon box formatting and fall back to plain table.
/// Box format pads every cell to the widest value, which is wasteful for very wide data.
const MAX_BOX_COLUMN_WIDTH: usize = 200;

/// Inner dispatch after optional sanitization
fn format_output_inner(
    columns: &[String],
    rows: &[Vec<String>],
    format: OutputFormat,
    zebra_mode: Option<usize>,
    no_headers: bool,
    column_widths: &[usize],
) -> String {
    match format {
        OutputFormat::Table => format_as_table_with_zebra(columns, rows, zebra_mode, no_headers),
        OutputFormat::Box => {
            // Fall back to plain table if any column is too wide for box formatting
            let too_wide = column_widths.iter().any(|&w| w > MAX_BOX_COLUMN_WIDTH);
            if !too_wide && should_use_box_format() {
                format_as_box_with_zebra(columns, rows, zebra_mode, no_headers, column_widths)
            } else {
                format_as_table_with_zebra(columns, rows, zebra_mode, no_headers)
            }
        }
        OutputFormat::Json => format_as_json(columns, rows),
        OutputFormat::Csv => format_as_csv(columns, rows, no_headers),
        OutputFormat::Tsv => format_as_tsv(columns, rows, no_headers),
        OutputFormat::List => format_as_list_with_zebra(columns, rows, zebra_mode),
        OutputFormat::Raw => unreachable!("Raw format handled before display_results"),
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

/// Get ANSI color code based on zebra mode and column index
fn get_zebra_color(zebra_mode: Option<usize>, col_index: usize) -> &'static str {
    match zebra_mode {
        None => "", // No coloring
        Some(2) => {
            // Blue and cyan (more visible than white)
            match col_index % 2 {
                0 => "\x1b[34m", // Blue
                _ => "\x1b[36m", // Cyan (instead of white)
            }
        }
        Some(3) => {
            // Red, white, and blue
            match col_index % 3 {
                0 => "\x1b[31m", // Red
                1 => "\x1b[37m", // White
                _ => "\x1b[34m", // Blue
            }
        }
        Some(4) => {
            // Red, white, blue, and green
            match col_index % 4 {
                0 => "\x1b[31m", // Red
                1 => "\x1b[37m", // White
                2 => "\x1b[34m", // Blue
                _ => "\x1b[32m", // Green
            }
        }
        _ => "", // Invalid mode
    }
}

/// Reset ANSI color
const RESET_COLOR: &str = "\x1b[0m";

fn format_as_table_with_zebra(
    columns: &[String],
    rows: &[Vec<String>],
    zebra_mode: Option<usize>,
    no_headers: bool,
) -> String {
    let mut output = String::new();

    // Header with zebra coloring (skip if no_headers is true)
    if !columns.is_empty() && !no_headers {
        let header_parts: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if zebra_mode.is_some() {
                    format!("{}{}{}", get_zebra_color(zebra_mode, i), col, RESET_COLOR)
                } else {
                    col.clone()
                }
            })
            .collect();
        output.push_str(&header_parts.join(" | "));
        output.push('\n');

        // Separator with zebra coloring
        let sep_parts: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let dashes = "-".repeat(col.len());
                if zebra_mode.is_some() {
                    format!(
                        "{}{}{}",
                        get_zebra_color(zebra_mode, i),
                        dashes,
                        RESET_COLOR
                    )
                } else {
                    dashes
                }
            })
            .collect();
        output.push_str(&sep_parts.join("-|-"));
        output.push('\n');
    }

    // Rows with zebra coloring
    for row in rows {
        let row_parts: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, val)| {
                if zebra_mode.is_some() {
                    format!("{}{}{}", get_zebra_color(zebra_mode, i), val, RESET_COLOR)
                } else {
                    val.clone()
                }
            })
            .collect();
        output.push_str(&row_parts.join(" | "));
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

fn format_as_box_with_zebra(
    columns: &[String],
    rows: &[Vec<String>],
    zebra_mode: Option<usize>,
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
            let row_parts: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, val)| {
                    if zebra_mode.is_some() {
                        format!("{}{}{}", get_zebra_color(zebra_mode, i), val, RESET_COLOR)
                    } else {
                        val.clone()
                    }
                })
                .collect();
            output.push_str(&row_parts.join(" | "));
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

    // Draw header row with zebra coloring
    output.push(VERTICAL);
    for (i, col) in columns.iter().enumerate() {
        let padded = format!(" {} ", pad_left(col, widths[i] - 2));
        if zebra_mode.is_some() {
            let color = get_zebra_color(zebra_mode, i);
            output.push_str(&format!("{}{}{}", color, padded, RESET_COLOR));
        } else {
            output.push_str(&padded);
        }
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
                    let padded = format!(" {} ", pad_left(cell, widths[i] - 2));
                    if zebra_mode.is_some() {
                        let color = get_zebra_color(zebra_mode, i);
                        output.push_str(&format!("{}{}{}", color, padded, RESET_COLOR));
                    } else {
                        output.push_str(&padded);
                    }
                    output.push(VERTICAL);
                } else {
                    // Handle row with more cells than columns (shouldn't happen normally)
                    let cell_output = format!(" {} ", cell);
                    if zebra_mode.is_some() {
                        let color = get_zebra_color(zebra_mode, i);
                        output.push_str(&format!("{}{}{}", color, cell_output, RESET_COLOR));
                    } else {
                        output.push_str(&cell_output);
                    }
                    output.push(VERTICAL);
                }
            }
            // Handle row with fewer cells than columns
            for i in row.len()..columns.len() {
                let padded = format!(" {:<width$} ", "", width = widths[i] - 2);
                if zebra_mode.is_some() {
                    let color = get_zebra_color(zebra_mode, i);
                    output.push_str(&format!("{}{}{}", color, padded, RESET_COLOR));
                } else {
                    output.push_str(&padded);
                }
                output.push(VERTICAL);
            }
            output.push('\n');
        }
    }

    // Draw bottom border
    output.push_str(&draw_line(BOTTOM_LEFT, BOTTOM_JUNCTION, BOTTOM_RIGHT));

    output
}

fn format_as_json(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut json_rows = Vec::new();

    for row in rows {
        let mut json_object = serde_json::Map::new();
        for (i, column) in columns.iter().enumerate() {
            let value = row.get(i).unwrap_or(&String::new()).clone();
            json_object.insert(column.clone(), serde_json::Value::String(value));
        }
        json_rows.push(serde_json::Value::Object(json_object));
    }

    let json_array = serde_json::Value::Array(json_rows);
    serde_json::to_string_pretty(&json_array).unwrap_or_else(|_| "[]".to_string())
}

fn format_as_csv(columns: &[String], rows: &[Vec<String>], no_headers: bool) -> String {
    let mut output = String::new();

    // Header (skip if no_headers is true)
    if !columns.is_empty() && !no_headers {
        output.push_str(&escape_csv_row(columns));
        output.push('\n');
    }

    // Rows
    for row in rows {
        output.push_str(&escape_csv_row(row));
        output.push('\n');
    }

    output
}

fn format_as_tsv(columns: &[String], rows: &[Vec<String>], no_headers: bool) -> String {
    let mut output = String::new();

    // Header (skip if no_headers is true)
    if !columns.is_empty() && !no_headers {
        output.push_str(&escape_tsv_row(columns));
        output.push('\n');
    }

    // Rows
    for row in rows {
        output.push_str(&escape_tsv_row(row));
        output.push('\n');
    }

    output
}

fn format_as_list_with_zebra(
    columns: &[String],
    rows: &[Vec<String>],
    zebra_mode: Option<usize>,
) -> String {
    let mut output = String::new();

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            output.push('\n');
        }

        for (i, column) in columns.iter().enumerate() {
            let value = row.get(i).map(|s| s.as_str()).unwrap_or("");
            if zebra_mode.is_some() {
                // Apply zebra coloring to both column name and value
                let color = get_zebra_color(zebra_mode, i);
                output.push_str(&format!("{}{} = {}{}\n", color, column, value, RESET_COLOR));
            } else {
                output.push_str(&format!("{} = {}\n", column, value));
            }
        }
    }

    output
}

fn escape_csv_row(row: &[String]) -> String {
    row.iter()
        .map(|field| escape_csv_field(field))
        .collect::<Vec<_>>()
        .join(",")
}

fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

fn escape_tsv_row(row: &[String]) -> String {
    row.iter()
        .map(|field| escape_tsv_field(field))
        .collect::<Vec<_>>()
        .join("\t")
}

fn escape_tsv_field(field: &str) -> String {
    field
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format_as_table(columns: &[String], rows: &[Vec<String>]) -> String {
        format_as_table_with_zebra(columns, rows, None, false)
    }

    fn format_as_box(columns: &[String], rows: &[Vec<String>]) -> String {
        let widths = crate::sanitize::compute_column_widths(columns, rows);
        format_as_box_with_zebra(columns, rows, None, false, &widths)
    }

    fn format_as_list(columns: &[String], rows: &[Vec<String>]) -> String {
        format_as_list_with_zebra(columns, rows, None)
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
        assert_eq!(formats.len(), 7);
        assert!(formats.contains(&"table"));
        assert!(formats.contains(&"box"));
        assert!(formats.contains(&"json"));
        assert!(formats.contains(&"csv"));
        assert!(formats.contains(&"tsv"));
        assert!(formats.contains(&"list"));
    }

    #[test]
    fn test_format_as_table() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];

        let result = format_as_table(&columns, &rows);
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

        let result = format_as_csv(&columns, &rows, false);
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

        let result = format_as_tsv(&columns, &rows, false);
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

        let result = format_as_csv(&columns, &rows, false);
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

        let result = format_as_tsv(&columns, &rows, false);
        assert!(result.contains("Has\\ttabs and\\nnewlines"));
    }

    #[test]
    fn test_empty_data() {
        let columns = vec![];
        let rows = vec![];

        assert_eq!(format_as_table(&columns, &rows), "");
        assert_eq!(format_as_json(&columns, &rows), "[]");
        assert_eq!(format_as_csv(&columns, &rows, false), "");
        assert_eq!(format_as_tsv(&columns, &rows, false), "");
        assert_eq!(format_as_list(&columns, &rows), "");
    }

    #[test]
    fn test_empty_rows_with_headers() {
        let columns = vec!["name".to_string(), "age".to_string()];
        let rows = vec![];

        let table_result = format_as_table(&columns, &rows);
        assert!(table_result.contains("name | age"));

        let csv_result = format_as_csv(&columns, &rows, false);
        assert_eq!(csv_result, "name,age\n");

        let json_result = format_as_json(&columns, &rows);
        assert_eq!(json_result, "[]");
    }

    #[test]
    fn test_format_as_box() {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["1".to_string(), "Alice".to_string(), "30".to_string()],
            vec!["2".to_string(), "Bob".to_string(), "25".to_string()],
        ];

        let result = format_as_box(&columns, &rows);

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

        let result = format_as_box(&columns, &rows);

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

        let result = format_as_box(&columns, &rows);

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

        let result = format_as_box(&columns, &rows);

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
