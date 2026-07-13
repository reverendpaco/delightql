// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql man` — render the embedded manual pages, as a projection of
//! `sys::help.man_page(*)` (SYS-HELP-DESIGN.md phase 3, grammar and
//! rendering per the 2026-07-05 rulings).
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
//! `groff -man -Tutf8 <path>` → the burned `plain` column (curl
//! --manual style; scrubbed at seed time by man_scrub). Apropos is deliberately NOT mirrored:
//! that is a query — sys::help.man_page(*), +like(plain, "%...%").

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
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    let results = crate::exec_ng::fetch_all(
        &mut *session,
        "sys::help.man_page(*) |> (name, section, troff, plain)",
    )?;
    Ok(results
        .rows
        .into_iter()
        .map(|row| Page {
            name: row[0].clone(),
            section: row[1].parse().unwrap_or(1),
            troff: row[2].clone(),
            plain: row[3].clone(),
        })
        .collect())
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

    let matches_name = |p: &Page, name: &str| {
        p.name == name && section.map(|s| p.section == s).unwrap_or(true)
    };
    let page = pages
        .iter()
        .find(|p| matches_name(p, &joined))
        .or_else(|| {
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
             (search the pages as a relation: \
             sys::help.man_page(*), +like(plain, \"%{}%\"))",
            joined,
            section
                .map(|s| format!(" in section {}", s))
                .unwrap_or_default(),
            known.join(", "),
            joined
        );
    };

    render(page)
}

/// The ruled fallback chain: man <path> → groff -man -Tutf8 <path> →
/// burned plain. A temp file rather than stdin so BSD/macOS man works
/// (no -l, no stdin), and so the pager keeps the terminal on stdin. A
/// rung's stderr is discarded — a failing rung's usage spew is not the
/// user's problem, the next rung is.
///
/// Piped stdout skips man entirely: pipe = plumbing face. The user is
/// composing (| less, | bat, | grep); typesetting, width, and pager
/// belong to a terminal. The burned plain column serves pipes
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
    // Last rung: the burned plain column (curl --manual style).
    print!("{}", page.plain);
    Ok(())
}
