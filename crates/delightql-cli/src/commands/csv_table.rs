// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CSV → one staged SQLite table: the ONE place a CSV field acquires its
//! value domain, shared by `csvstruct` and `filemunge`.
//!
//! CSV carries no types. The tool contract (man dql-tools) promises that a
//! field spelling a number is that number — `c(*), age > 26` is the
//! documented example — and the law that realizes it is the engine's own:
//! every column is declared with SQLite's NUMERIC affinity, so a field that
//! is a well-formed integer or real literal is stored as that number and
//! any other field is stored as text. Comparisons then obey the values, and
//! a text literal compared with such a column is converted the same way
//! (`age = "30"` matches the number 30). An EMPTY field is an absent value
//! and is stored as NULL: it qualifies for no comparison. A BLANK LINE is
//! one empty field: it is a record of a one-column table — one NULL row —
//! and a record of no wider table, where it is skipped. That the affinity
//! also reads `007`, ` 30 ` and `1e3` as numbers is the engine's reading; a
//! column mixing numeric and non-numeric spellings compares each cell by
//! its own storage class.

use anyhow::Result;
use rusqlite::Connection;

fn strop(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The value a CSV field binds as. Empty is absence; everything else is
/// the field's text, which the column's NUMERIC affinity classifies.
fn field_value(field: &str) -> Option<&str> {
    if field.is_empty() {
        None
    } else {
        Some(field)
    }
}

/// Stage `data` as the table `table`: `CREATE` under the value law above,
/// then one row per record. `source` names the input in refusals.
pub fn stage_csv_table(
    conn: &Connection,
    table: &str,
    has_headers: bool,
    delimiter: u8,
    data: &[u8],
    source: &str,
) -> Result<()> {
    let reader = || {
        csv::ReaderBuilder::new()
            .has_headers(has_headers)
            .delimiter(delimiter)
            .from_reader(data)
    };
    let mut rdr = reader();

    let col_names: Vec<String> = if has_headers {
        rdr.headers()?.iter().map(|h| h.to_string()).collect()
    } else {
        let width = match rdr.records().next() {
            Some(Ok(ref rec)) => rec.len(),
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("No CSV records found in {source}"),
        };
        (1..=width).map(|i| format!("c{}", i)).collect()
    };
    if col_names.is_empty() {
        anyhow::bail!("CSV {source} has zero columns");
    }

    let col_defs: Vec<String> = col_names
        .iter()
        .map(|n| format!("{} NUMERIC", strop(n)))
        .collect();
    let table_name = strop(table);
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
    let mut stmt = conn.prepare(&insert_sql)?;

    // Without headers the first record was consumed for its width; read
    // the input again from the start.
    let mut rdr = if has_headers { rdr } else { reader() };
    if has_headers {
        rdr.headers()?;
    }
    // THE BLANK LINES ARE RECORDS THE READER DOES NOT YIELD. It skips them
    // silently, but it records where it began reading each record — the
    // byte after the previous record's terminator — so the line
    // terminators standing there, before the record's own first byte, are
    // the blank lines it skipped, and the terminators ending the input past
    // the last record's own are the blank lines after it. A record's text
    // never begins or ends with a terminator (an unquoted field holds none
    // and a quoted field is delimited by its quotes), so each count stops
    // at the record and no second quoting law is applied to the bytes.
    let width = col_names.len();
    let blank_records = |stmt: &mut rusqlite::Statement<'_>, count: u64| -> Result<()> {
        if width == 1 {
            for _ in 0..count {
                stmt.execute([None::<&str>])?;
            }
        }
        Ok(())
    };
    let mut preceded = has_headers;
    for rec in rdr.records() {
        let rec = rec?;
        if let Some(start) = rec.position().map(|position| position.byte() as usize) {
            blank_records(&mut stmt, terminators_from(data, start))?;
        }
        let params: Vec<Option<&str>> = (0..width)
            .map(|i| rec.get(i).and_then(field_value))
            .collect();
        stmt.execute(rusqlite::params_from_iter(params.iter()))?;
        preceded = true;
    }
    let trailing = terminators_before(data, data.len());
    blank_records(&mut stmt, trailing.saturating_sub(u64::from(preceded)))?;
    Ok(())
}

/// The line terminators standing at `start`, counted forwards until a byte
/// that is not one: `\r\n` is one terminator, and so is a lone `\n` or
/// `\r`, as the reader reads them. The reader ends a record at the `\r`
/// of a `\r\n` pair and records the next start at the `\n`, so a `\n`
/// at `start` that follows a `\r` completes the previous terminator and
/// is no line of its own.
fn terminators_from(data: &[u8], start: usize) -> u64 {
    let mut count = 0;
    let mut at = start;
    if at < data.len() && data[at] == b'\n' && at > 0 && data[at - 1] == b'\r' {
        at += 1;
    }
    while at < data.len() {
        match data[at] {
            b'\r' => {
                at += 1;
                if at < data.len() && data[at] == b'\n' {
                    at += 1;
                }
            }
            b'\n' => at += 1,
            _ => break,
        }
        count += 1;
    }
    count
}

/// The line terminators standing immediately before `end`, counted
/// backwards under the same reading.
fn terminators_before(data: &[u8], end: usize) -> u64 {
    let mut count = 0;
    let mut at = end;
    while at > 0 {
        match data[at - 1] {
            b'\n' => {
                at -= 1;
                if at > 0 && data[at - 1] == b'\r' {
                    at -= 1;
                }
            }
            b'\r' => at -= 1,
            _ => break,
        }
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staged(data: &str) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        stage_csv_table(&conn, "c", true, b',', data.as_bytes(), "stdin").unwrap();
        conn
    }

    fn cells(conn: &Connection, sql: &str) -> Vec<(String, String)> {
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0).unwrap(),
                r.get::<_, Option<String>>(1)
                    .unwrap()
                    .unwrap_or_else(|| "<null>".to_string()),
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    /// The value law, cell by cell: a numeric spelling is a number, an
    /// empty field is NULL, and text is text.
    #[test]
    fn fields_acquire_their_value_domain_at_staging() {
        let conn = staged("name,age\nA,30\nB,7\nC,\nD,x\n");
        assert_eq!(
            cells(&conn, "SELECT name, typeof(age) FROM c ORDER BY name"),
            vec![
                ("A".into(), "integer".into()),
                ("B".into(), "integer".into()),
                ("C".into(), "null".into()),
                ("D".into(), "text".into()),
            ]
        );
        // The textual column stays text, numeric-looking or not.
        let conn = staged("id,name\n1,A\n2,B\n");
        assert_eq!(
            cells(&conn, "SELECT typeof(id), typeof(name) FROM c LIMIT 1"),
            vec![("integer".into(), "text".into())]
        );
    }

    /// NUMERIC affinity is the ruled CSV value law, including its less
    /// obvious consequences: numeric spellings normalize, empty is absence,
    /// and a nonnumeric cell in the same column remains text and compares by
    /// SQLite's storage-class order.
    #[test]
    fn numeric_affinity_decides_ambiguous_spellings_and_mixed_columns() {
        let conn = staged("name,code\nA,007\nB, 30 \nC,1e3\nD,\nE,unknown\n");
        assert_eq!(
            cells(
                &conn,
                "SELECT name, typeof(code) || '|' || quote(code) FROM c ORDER BY name"
            ),
            vec![
                ("A".into(), "integer|7".into()),
                ("B".into(), "integer|30".into()),
                ("C".into(), "integer|1000".into()),
                ("D".into(), "null|NULL".into()),
                ("E".into(), "text|'unknown'".into()),
            ]
        );

        let greater: String = conn
            .query_row(
                "SELECT group_concat(name, ',') FROM \
                 (SELECT name FROM c WHERE code > 26 ORDER BY name)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(greater, "B,C,E");

        let textual_match: String = conn
            .query_row("SELECT name FROM c WHERE code = '30'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(textual_match, "B");
    }

    /// A blank line is one empty field: in a one-column input it is one NULL
    /// record, wherever it stands — between records, after the last, or as
    /// the only record; a line break inside a quoted field is not a blank
    /// line; and a quoted empty field is the same absence.
    #[test]
    fn a_blank_line_is_one_null_record_of_a_one_column_table() {
        let rows = |data: &str| -> Vec<Option<i64>> {
            let conn = staged(data);
            let mut stmt = conn.prepare("SELECT age FROM c").unwrap();
            stmt.query_map([], |r| r.get::<_, Option<i64>>(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(rows("age\n30\n\n40\n"), vec![Some(30), None, Some(40)]);
        assert_eq!(rows("age\n30\n\"\"\n40\n"), vec![Some(30), None, Some(40)]);
        assert_eq!(
            rows("age\r\n30\r\n\r\n40\r\n"),
            vec![Some(30), None, Some(40)]
        );
        assert_eq!(rows("age\n30\n\n"), vec![Some(30), None]);
        assert_eq!(rows("age\n\n\n30"), vec![None, None, Some(30)]);
        assert_eq!(rows("age\n\n"), vec![None]);

        let conn = staged("note\n\"a\nb\"\n\nc\n");
        assert_eq!(
            cells(
                &conn,
                "SELECT COALESCE(note, '<null>'), typeof(note) FROM c"
            ),
            vec![
                ("a\nb".into(), "text".into()),
                ("<null>".into(), "null".into()),
                ("c".into(), "text".into()),
            ]
        );
    }

    /// A blank line is one field, so it is a record of no table wider than
    /// one column: there it is skipped, as the reader skips it.
    #[test]
    fn a_blank_line_is_not_a_record_of_a_wider_table() {
        let conn = staged("name,age\nA,30\n\nB,7\n");
        assert_eq!(
            cells(&conn, "SELECT name, typeof(age) FROM c ORDER BY name"),
            vec![
                ("A".into(), "integer".into()),
                ("B".into(), "integer".into())
            ]
        );
    }

    /// Headerless input names its columns by position under the same law.
    #[test]
    fn headerless_input_is_staged_whole() {
        let conn = Connection::open_in_memory().unwrap();
        stage_csv_table(&conn, "t", false, b'\t', b"1\tx\n2\t\n", "file").unwrap();
        assert_eq!(
            cells(
                &conn,
                "SELECT CAST(c1 AS TEXT), typeof(c2) FROM t ORDER BY c1"
            ),
            vec![("1".into(), "text".into()), ("2".into(), "null".into())]
        );
    }
}
