// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql man` — render the embedded manual pages, as a projection of the
//! CLI-owned `man.sqlite` mounted at `cli::man`.
//!
//! Grammar: name tokens hyphen-join (man-db's own behavior); if the
//! joined name misses, a `dql-` prefix is inferred; a leading numeric
//! token is a section; no tokens means dql(1) itself.
//!
//! Rendering: never invoke the formatter directly — hand the troff to
//! `man`, which owns typesetting, terminal width, and the pager. The
//! portable spelling is a temp FILE, not stdin: `-l` is a man-db
//! Linux-ism and BSD/macOS man cannot read stdin, but every man
//! (man-db, BSD, mandoc) treats an argument containing a slash as a
//! local file. Fallback chain for hostile environments: `man <path>` →
//! `groff -man -Tutf8 <path>` → plain text derived from the burned troff at
//! serve time (curl --manual style; scrubbed by man_scrub). Apropos is
//! deliberately not
//! mirrored: authored troff remains queryable as `cli::man.man_page` inside
//! this command's isolated session.

use anyhow::Result;
use std::io::Write;

struct Page {
    name: String,
    section: i64,
    troff: String,
    plain: String,
}

fn load_pages() -> Result<Vec<Page>> {
    let mut handle = crate::connection::open_handle()?;
    let pages = {
        let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
        // The embedded image is bound as "man" by open_handle and mounted
        // via its locator: attach-class, read-only,
        // zero-copy from rodata — no temp file, no guard.
        crate::exec_ng::run_dql_query(
            "mount!(\"delightql-bytes://man\", \"cli::man\")(*)",
            &mut *session,
        )?;
        crate::embedded_db::verify_bundle_schema_version(&mut *session, "cli::man")?;
        let results = crate::exec_ng::fetch_all(
            &mut *session,
            "cli::man.man_page(*) |> #(name, section) |> (name, section, troff)",
        )?;
        results
            .rows
            .into_iter()
            .map(|row| {
                let name = row[0].clone();
                let troff = row[2].clone();
                let plain = crate::man_scrub::scrub(&troff).map_err(|e| {
                    anyhow::anyhow!("shipped page {name} outside house dialect: {e}")
                })?;
                Ok(Page {
                    name,
                    section: row[1].parse().unwrap_or(1),
                    troff,
                    plain,
                })
            })
            .collect::<Result<Vec<_>>>()?
    };
    Ok(pages)
}

pub fn handle_man(name_tokens: &[String], dump: Option<&std::path::Path>) -> Result<()> {
    let pages = load_pages()?;

    if let Some(dir) = dump {
        std::fs::create_dir_all(dir)?;
        for p in &pages {
            let path = dir.join(format!("{}.{}", p.name, p.section));
            std::fs::write(&path, &p.troff)?;
            println!("{}", path.display());
        }
        return Ok(());
    }

    // Grammar: leading numeric token = section (no page is named with
    // digits); remaining tokens hyphen-join; empty = dql itself.
    let (section, tokens) = match name_tokens.first().and_then(|t| t.parse::<i64>().ok()) {
        Some(s) => (Some(s), &name_tokens[1..]),
        None => (None, name_tokens),
    };
    let joined = if tokens.is_empty() {
        "dql".to_string()
    } else {
        tokens.join("-")
    };

    let matches_name =
        |p: &Page, name: &str| p.name == name && section.map(|s| p.section == s).unwrap_or(true);
    let page = pages.iter().find(|p| matches_name(p, &joined)).or_else(|| {
        let prefixed = format!("dql-{}", joined);
        pages.iter().find(|p| matches_name(p, &prefixed))
    });

    let Some(page) = page else {
        let mut known: Vec<String> = pages
            .iter()
            .map(|p| format!("{}({})", p.name, p.section))
            .collect();
        known.sort();
        anyhow::bail!(
            "no manual page for '{}'{}\navailable: {}\n\
             (the embedded page source lives in cli::man.man_page)",
            joined,
            section
                .map(|s| format!(" in section {}", s))
                .unwrap_or_default(),
            known.join(", "),
        );
    };

    render(page)
}

/// The ruled fallback chain: man <path> → groff -man -Tutf8 <path> →
/// plain text derived from burned troff. A temp file rather than stdin so
/// BSD/macOS man works
/// (no -l, no stdin), and so the pager keeps the terminal on stdin. A
/// rung's stderr is discarded — a failing rung's usage spew is not the
/// user's problem, the next rung is.
///
/// Piped stdout skips man entirely: pipe = plumbing face. The user is
/// composing (| less, | bat, | grep); typesetting, width, and pager
/// belong to a terminal. The derived plain projection serves pipes
/// directly — byte-identical on every platform, because it never
/// depends on which man/groff is installed.
fn render(page: &Page) -> Result<()> {
    use std::io::IsTerminal;
    if !std::io::stdout().is_terminal() {
        print!("{}", page.plain);
        return Ok(());
    }

    let mut tmp = tempfile::Builder::new()
        .prefix("dql-man-")
        .suffix(&format!(".{}", page.section))
        .tempfile()?;
    tmp.write_all(page.troff.as_bytes())?;
    tmp.flush()?;

    for (bin, args) in [("man", &[][..]), ("groff", &["-man", "-Tutf8"][..])] {
        let status = std::process::Command::new(bin)
            .args(args)
            .arg(tmp.path())
            .stderr(std::process::Stdio::null())
            .status();
        if matches!(status, Ok(s) if s.success()) {
            return Ok(());
        }
        // binary absent or nonzero exit: next rung
    }
    // Last rung: the plain projection derived from burned troff.
    print!("{}", page.plain);
    Ok(())
}
