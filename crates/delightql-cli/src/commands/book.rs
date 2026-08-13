// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql book` — a projection of the CLI-owned `book.sqlite` database.

use anyhow::Result;
use delightql_core::api::DqlSession;

const NAMESPACE: &str = "cli::book";

fn list_books(session: &mut dyn DqlSession) -> Result<Vec<String>> {
    let results = crate::exec_ng::fetch_all(
        session,
        "cli::book.book_meta(*) |> #(book_name) |> (book_name)",
    )?;
    Ok(results.rows.into_iter().map(|row| row[0].clone()).collect())
}

pub fn handle_book(
    name: Option<&str>,
    export_images: Option<&std::path::Path>,
    db: Option<&std::path::Path>,
) -> Result<()> {
    let mut handle = crate::connection::open_handle()?;

    {
        let mut session = handle.session().map_err(|e| anyhow::anyhow!(e))?;
        // Default source: the embedded image, bound as "book" by
        // open_handle and mounted via its locator — attach-class,
        // read-only, zero-copy from rodata, no temp file.
        //
        // --db <file>: a compliant bundle from disk instead — same mount
        // spine, same schema-version gate, same output. This is the
        // authoring loop (bundle, then render without recompiling) and
        // the distribution story (a downloaded book is consumed through
        // the same front door). The embedded path stays the shipping
        // truth.
        let source = match db {
            None => "delightql-bytes://book".to_string(),
            Some(path) => {
                anyhow::ensure!(path.exists(), "book database not found: {}", path.display());
                let spelled = path.display().to_string();
                anyhow::ensure!(
                    !spelled.contains('"'),
                    "book database path must not contain '\"'"
                );
                spelled
            }
        };
        crate::exec_ng::run_dql_query(
            &format!("mount!(\"{source}\", \"{NAMESPACE}\")(*)"),
            &mut *session,
        )?;
        crate::embedded_db::verify_bundle_schema_version(&mut *session, NAMESPACE)?;

        let names = list_books(&mut *session)?;
        let Some(name) = name else {
            anyhow::ensure!(
                export_images.is_none(),
                "--export-images requires a book name"
            );
            for name in names {
                println!("{name}");
            }
            return Ok(());
        };
        anyhow::ensure!(
            valid_book_name(name),
            "invalid book name '{name}': expected [a-z0-9][a-z0-9._-]*"
        );

        let results = crate::exec_ng::fetch_all(
            &mut *session,
            &format!(
                "cli::book.book(*), cli::book.base_content(*.(slug)), book_name = \"{name}\" \
                 |> #(ordinal) |> (heading_shift, content)"
            ),
        )?;
        if results.rows.is_empty() {
            anyhow::bail!("no book named '{name}'\navailable: {}", names.join(", "));
        }

        // Read as CELLS: a book with no frontmatter and a book whose
        // frontmatter is the four characters `NULL` are different books,
        // and only absence suppresses the leading block.
        let (_meta_columns, meta_rows) = crate::exec_ng::fetch_all_raw(
            &mut *session,
            &format!("cli::book.book_meta(*), book_name = \"{name}\" |> (frontmatter)"),
        )?;
        if let Some(frontmatter) = meta_rows
            .first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.as_deref())
            .map(String::from_utf8_lossy)
        {
            if !frontmatter.is_empty() {
                print!("{frontmatter}");
                if !frontmatter.ends_with('\n') {
                    println!();
                }
                println!();
            }
        }
        for row in results.rows {
            let shift = row[0].parse::<usize>().unwrap_or(0);
            let content = shift_headings(&row[1], shift);
            print!("{content}");
            if !content.ends_with('\n') {
                println!();
            }
            println!();
        }

        if let Some(dir) = export_images {
            export_pool_images(&mut *session, dir)?;
        }
    }
    Ok(())
}

/// Materialize the bundle's images so the emitted markdown's relative
/// `images/<name>` references resolve for pandoc/typst. The WHOLE pool,
/// deliberately — filtering to the images a book references would mean
/// scanning the prose, and a dangling reference already fails visibly
/// at press time. Bytes travel through the query pipeline hex-spelled
/// so binary formats survive rows-as-text.
fn export_pool_images(session: &mut dyn DqlSession, dir: &std::path::Path) -> Result<()> {
    let rows = crate::exec_ng::fetch_all(
        session,
        &format!("{NAMESPACE}.image(*) |> #(name) |> (name, hex:(content))"),
    )?;
    std::fs::create_dir_all(dir)
        .map_err(|e| anyhow::anyhow!("create image directory {}: {e}", dir.display()))?;
    for row in &rows.rows {
        let name = &row[0];
        anyhow::ensure!(
            !name.contains('/') && !name.contains('\\') && !name.starts_with('.'),
            "refusing image name '{name}' (not a plain basename)"
        );
        let bytes = decode_hex(&row[1])
            .ok_or_else(|| anyhow::anyhow!("image '{name}': malformed hex content"))?;
        let path = dir.join(name);
        std::fs::write(&path, bytes)
            .map_err(|e| anyhow::anyhow!("write {}: {e}", path.display()))?;
    }
    eprintln!("exported {} images -> {}", rows.rows.len(), dir.display());
    Ok(())
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn valid_book_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
}

/// A heading carries the shift marker when its trailing pandoc attribute
/// block contains the `.dqlh` class. Pandoc allows ONE attribute block per
/// heading, so ids and classes merge: `# Projection {#sec:x .dqlh}`.
/// The marker is the author's contract — the bundler never reads the
/// prose; this emitter is the only machine that interprets it.
fn has_dqlh_marker(line: &str) -> bool {
    let trimmed = line.trim_end();
    if !trimmed.ends_with('}') {
        return false;
    }
    let Some(open) = trimmed.rfind('{') else {
        return false;
    };
    trimmed[open + 1..trimmed.len() - 1]
        .split_whitespace()
        .any(|token| token == ".dqlh")
}

/// Serving-side code-awareness: 4+ leading spaces = literal (indented
/// code); fences are tracked by char AND length, so a ``` quoted inside
/// a ```` block cannot close it; 0-3 leading spaces = structural
/// candidate. This is plumbing, not validation — it exists for the one
/// case authoring responsibility cannot cover: a code block QUOTING a
/// marked heading has no way to protect itself from a naive shifter.
fn shift_headings(content: &str, shift: usize) -> String {
    if shift == 0 {
        return content.to_string();
    }
    let mut output = String::new();
    // (char, opening run length) while inside a fenced block.
    let mut fence: Option<(char, usize)> = None;
    for line in content.split_inclusive('\n') {
        let indent = line.len() - line.trim_start_matches(' ').len();
        let stripped = line.trim();
        if let Some((open_char, open_len)) = fence {
            let run = stripped.chars().take_while(|c| *c == open_char).count();
            if indent <= 3 && run >= open_len && stripped.chars().all(|c| c == open_char) {
                fence = None;
            }
            output.push_str(line);
            continue;
        }
        if indent >= 4 {
            output.push_str(line);
            continue;
        }
        let delimiter = if stripped.starts_with("```") {
            Some('`')
        } else if stripped.starts_with("~~~") {
            Some('~')
        } else {
            None
        };
        if let Some(c) = delimiter {
            let run = stripped.chars().take_while(|ch| *ch == c).count();
            fence = Some((c, run));
            output.push_str(line);
            continue;
        }
        if stripped.starts_with('#') && has_dqlh_marker(stripped) {
            let existing = stripped.chars().take_while(|c| *c == '#').count();
            let (lead, rest) = line.split_at(indent);
            output.push_str(lead);
            output.push_str(&"#".repeat(shift.min(6usize.saturating_sub(existing))));
            output.push_str(rest);
            continue;
        }
        output.push_str(line);
    }
    output
}

// No unit tests here, deliberately (tests-are-invariants): every emitter
// invariant — per-depth shifting, merged attribute blocks, unmarked
// pass-through, indented-heading hash placement, indented-code and
// nested-fence immunity, name validation — is stated as fixture content
// and pinned at the process boundary by tests/book_fixture_emission.rs
// (the golden file) and the bin book test (stage agreement). The
// functions in this file are free to be renamed, split, or replaced.
