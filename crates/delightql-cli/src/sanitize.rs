/// Terminal output sanitization for CLI cell values.
///
/// Database cell values can contain bytes that are dangerous when rendered
/// to a terminal — ESC sequences that reposition cursors, change window
/// titles, or inject text into the shell's input buffer. This module
/// provides a single-pass character scan that escapes control characters
/// while preserving TAB, LF, and CR+LF pairs.
use std::borrow::Cow;
use std::fmt::Write;

/// Returns true if the character is a dangerous control character.
fn is_dangerous(c: char) -> bool {
    match c {
        '\x00'..='\x08' => true,         // C0 before TAB
        '\x09' => false,                 // TAB — allowed
        '\x0A' => false,                 // LF — allowed
        '\x0B'..='\x0C' => true,         // VT, FF
        '\x0D' => true,                  // CR — context-dependent, handled in sanitize_cell
        '\x0E'..='\x1F' => true,         // rest of C0 (includes ESC at 0x1B)
        '\x7F' => true,                  // DEL
        '\u{0080}'..='\u{009F}' => true, // C1 controls
        _ => false,
    }
}

/// Fast check: does any character need escaping?
pub fn needs_sanitization(value: &str) -> bool {
    value.chars().any(|c| is_dangerous(c))
}

/// Sanitize a cell value for terminal-safe display.
///
/// Returns Cow::Borrowed for clean values (common case, zero allocation).
/// Returns Cow::Owned with \xHH escaping for values with control chars.
/// Bare CR is escaped; CR+LF pairs pass through as LF (CR stripped).
pub fn sanitize_cell(value: &str) -> Cow<'_, str> {
    if !needs_sanitization(value) {
        return Cow::Borrowed(value);
    }

    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\r' {
            if chars.peek() == Some(&'\n') {
                // CR+LF → emit just LF (strip CR)
                chars.next();
                out.push('\n');
            } else {
                // bare CR → escape
                out.push_str("\\x0D");
            }
        } else if is_dangerous(c) {
            // For multi-byte C1 controls, escape all UTF-8 bytes
            let mut buf = [0u8; 4];
            let bytes = c.encode_utf8(&mut buf);
            for b in bytes.bytes() {
                write!(&mut out, "\\x{:02X}", b).unwrap();
            }
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Sanitize all rows, returning owned copies.
pub fn sanitize_rows(columns: &[String], rows: &[Vec<String>]) -> (Vec<String>, Vec<Vec<String>>) {
    let (cols, rows, _widths) = sanitize_rows_with_widths(columns, rows);
    (cols, rows)
}

/// Sanitize all rows and compute max column display widths in a single pass.
///
/// Returns (sanitized_columns, sanitized_rows, column_widths).
/// Widths reflect the sanitized (escaped) string lengths, including headers.
pub fn sanitize_rows_with_widths(
    columns: &[String],
    rows: &[Vec<String>],
) -> (Vec<String>, Vec<Vec<String>>, Vec<usize>) {
    let num_cols = columns.len();
    let mut widths = vec![0usize; num_cols];

    let safe_columns: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let safe = sanitize_cell(c);
            widths[i] = safe.len();
            safe.into_owned()
        })
        .collect();

    let safe_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, cell)| {
                    let safe = sanitize_cell(cell);
                    if i < num_cols {
                        widths[i] = widths[i].max(safe.len());
                    }
                    safe.into_owned()
                })
                .collect()
        })
        .collect();

    (safe_columns, safe_rows, widths)
}

/// Compute max column display widths without sanitizing (for clean data).
pub fn compute_column_widths(columns: &[String], rows: &[Vec<String>]) -> Vec<usize> {
    let num_cols = columns.len();
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < num_cols {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    widths
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_value_borrows() {
        let v = "hello world";
        let result = sanitize_cell(v);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result, "hello world");
    }

    #[test]
    fn tab_and_lf_allowed() {
        let v = "col1\tcol2\nrow2";
        assert!(!needs_sanitization(v));
        assert!(matches!(sanitize_cell(v), Cow::Borrowed(_)));
    }

    #[test]
    fn esc_sequence_escaped() {
        let v = "\x1b[31mRED\x1b[0m";
        assert!(needs_sanitization(v));
        let result = sanitize_cell(v);
        assert_eq!(result, "\\x1B[31mRED\\x1B[0m");
    }

    #[test]
    fn null_byte_escaped() {
        let v = "before\x00after";
        let result = sanitize_cell(v);
        assert_eq!(result, "before\\x00after");
    }

    #[test]
    fn del_escaped() {
        let v = "a\x7Fb";
        let result = sanitize_cell(v);
        assert_eq!(result, "a\\x7Fb");
    }

    #[test]
    fn crlf_passes_as_lf() {
        let v = "line1\r\nline2";
        let result = sanitize_cell(v);
        assert_eq!(result, "line1\nline2");
    }

    #[test]
    fn bare_cr_escaped() {
        let v = "col1\rcol2";
        let result = sanitize_cell(v);
        assert_eq!(result, "col1\\x0Dcol2");
    }

    #[test]
    fn c1_control_escaped() {
        // U+0085 (NEXT LINE) encodes as 0xC2 0x85 in UTF-8
        let v = "a\u{0085}b";
        assert!(needs_sanitization(v));
        let result = sanitize_cell(v);
        assert_eq!(result, "a\\xC2\\x85b");
    }

    #[test]
    fn c1_csi_escaped() {
        // U+009B (CSI) encodes as 0xC2 0x9B in UTF-8
        let v = "a\u{009B}31mb";
        let result = sanitize_cell(v);
        assert_eq!(result, "a\\xC2\\x9B31mb");
    }

    #[test]
    fn sanitize_rows_works() {
        let cols = vec!["name".to_string(), "val".to_string()];
        let rows = vec![
            vec!["ok".to_string(), "fine".to_string()],
            vec!["bad\x1b[0m".to_string(), "good".to_string()],
        ];
        let (safe_cols, safe_rows) = sanitize_rows(&cols, &rows);
        assert_eq!(safe_cols, vec!["name", "val"]);
        assert_eq!(safe_rows[0], vec!["ok", "fine"]);
        assert_eq!(safe_rows[1], vec!["bad\\x1B[0m", "good"]);
    }

    #[test]
    fn sanitize_rows_with_widths_clean() {
        let cols = vec!["name".to_string(), "age".to_string()];
        let rows = vec![
            vec!["Alice".to_string(), "30".to_string()],
            vec!["Bob".to_string(), "25".to_string()],
        ];
        let (_safe_cols, _safe_rows, widths) = sanitize_rows_with_widths(&cols, &rows);
        // "Alice" (5) > "name" (4) > "Bob" (3)
        assert_eq!(widths[0], 5);
        // "age" (3) > "30" (2) = "25" (2)
        assert_eq!(widths[1], 3);
    }

    #[test]
    fn sanitize_rows_with_widths_dirty() {
        let cols = vec!["val".to_string()];
        let rows = vec![
            vec!["\x1b".to_string()],  // 1 byte → "\\x1B" = 4 bytes
            vec!["hello".to_string()], // 5 bytes, clean
        ];
        let (_safe_cols, _safe_rows, widths) = sanitize_rows_with_widths(&cols, &rows);
        // "hello" (5) > "\\x1B" (4) > "val" (3)
        assert_eq!(widths[0], 5);
    }

    #[test]
    fn compute_column_widths_works() {
        let cols = vec!["id".to_string(), "description".to_string()];
        let rows = vec![
            vec!["1".to_string(), "short".to_string()],
            vec!["2".to_string(), "a longer value".to_string()],
        ];
        let widths = compute_column_widths(&cols, &rows);
        // "id" (2) > "1" (1) = "2" (1)
        assert_eq!(widths[0], 2);
        // "a longer value" (14) > "description" (11) > "short" (5)
        assert_eq!(widths[1], 14);
    }
}
