use crate::error::Result;

/// Parse CSV text into column headers and rows of string values.
///
/// If `has_headers` is true, the first row is treated as column names.
/// Otherwise columns are named `col0`, `col1`, etc.
pub fn parse_csv(text: &str, has_headers: bool) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .from_reader(text.as_bytes());

    let columns = if has_headers {
        reader
            .headers()?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        // Peek first record for column count
        Vec::new()
    };

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|f| f.to_string()).collect();
        rows.push(row);
    }

    // If no headers, generate column names from first row width
    let columns = if !has_headers && !rows.is_empty() {
        (0..rows[0].len())
            .map(|i| format!("col{}", i))
            .collect()
    } else {
        columns
    };

    Ok((columns, rows))
}

/// Parse TSV text into column headers and rows of string values.
pub fn parse_tsv(text: &str, has_headers: bool) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .delimiter(b'\t')
        .from_reader(text.as_bytes());

    let columns = if has_headers {
        reader
            .headers()?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        Vec::new()
    };

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|f| f.to_string()).collect();
        rows.push(row);
    }

    let columns = if !has_headers && !rows.is_empty() {
        (0..rows[0].len())
            .map(|i| format!("col{}", i))
            .collect()
    } else {
        columns
    };

    Ok((columns, rows))
}
