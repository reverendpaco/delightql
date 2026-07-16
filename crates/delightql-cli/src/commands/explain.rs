// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// `dql explain <identifier>` — the registry-backed identifier explainer.
//
// The window into the compiler's identifier taxonomy (URI-DESIGN.md §3).
// Since SYS-HELP-DESIGN.md phase 1, the registry IS a burned relation:
// this command queries `sys::identifiers.identifier(*)` from the in-memory
// bootstrap (rows authored in bootstrap/schema.sql), so `dql explain`,
// `sys::identifiers.identifier(*)` in a query, and every future projection
// (website, man pages) read one source and can never disagree.
// Spelling normalization (badge form / canonical URL / bare hierarchy)
// stays in code: delightql_core::uri_registry.

use anyhow::Result;
use delightql_core::uri_registry::{
    canonical_url, kind_from_word, parse_identifier, IdentifierEntry, UriKind,
};

/// Load the identifier rows by standing up the in-memory system and
/// querying the burned table — the same ~25ms a `dql query` pays, so
/// explain's latency is unchanged (measured, SYS-HELP finding #5).
fn load_rows() -> Result<Vec<IdentifierEntry>> {
    let mut handle = crate::connection::open_handle()?;
    let mut session = handle.session().map_err(|e| anyhow::anyhow!("{}", e))?;
    let results = crate::exec_ng::fetch_all(
        &mut *session,
        "sys::identifiers.identifier(*) |> (kind, hierarchy, summary, explanation)",
    )?;
    results
        .rows
        .into_iter()
        .map(|row| {
            let kind = kind_from_word(&row[0])
                .ok_or_else(|| anyhow::anyhow!("bad kind word in identifier row: {}", row[0]))?;
            Ok(IdentifierEntry {
                kind,
                hierarchy: row[1].clone(),
                summary: row[2].clone(),
                explanation: row[3].clone(),
            })
        })
        .collect()
}

pub fn handle_explain(identifier: &str) -> Result<()> {
    let rows = load_rows()?;
    match parse_identifier(identifier) {
        Some((kind, hierarchy)) => explain_one(&rows, kind, &hierarchy),
        None => {
            // Bare hierarchy: search across kinds.
            let bare = identifier.trim_matches('/');
            let hits: Vec<&IdentifierEntry> =
                rows.iter().filter(|e| e.hierarchy == bare).collect();
            match hits.len() {
                1 => {
                    let (kind, hierarchy) = (hits[0].kind, hits[0].hierarchy.clone());
                    explain_one(&rows, kind, &hierarchy)
                }
                0 => {
                    anyhow::bail!(
                        "'{identifier}' is not a DelightQL identifier.\n\
                         Accepted forms: delightql-<kind>://<hierarchy> \
                         (kinds: error, danger, config),\n\
                         https://delightql.org/uri/<kind>/<hierarchy>, \
                         or a bare hierarchy known to the registry."
                    );
                }
                _ => {
                    println!("'{bare}' exists in more than one kind:");
                    for e in hits {
                        println!("  {}{}", e.kind.scheme(), e.hierarchy);
                    }
                    println!("\nRe-run with the full badge form.");
                    Ok(())
                }
            }
        }
    }
}

fn explain_one(rows: &[IdentifierEntry], kind: UriKind, hierarchy: &str) -> Result<()> {
    println!("{}{}", kind.scheme(), hierarchy);
    println!("  → {}", canonical_url(kind, hierarchy));
    println!();

    match rows
        .iter()
        .find(|e| e.kind == kind && e.hierarchy == hierarchy)
    {
        Some(entry) => {
            println!("{}", entry.summary);
            println!();
            println!("{}", wrap(&entry.explanation, 74));
            print_kind_facts(entry);
        }
        None => {
            println!(
                "No registry entry yet for this identifier. Identifiers are\n\
                 append-only: if the compiler minted it, it is valid — the\n\
                 prose is pending. The canonical URL above is where the\n\
                 documentation will live."
            );
        }
    }

    // Registered descendants (segment-prefix semantics — the same
    // family matching error hooks use).
    let prefix = format!("{}/", hierarchy);
    let kids: Vec<&IdentifierEntry> = rows
        .iter()
        .filter(|e| e.kind == kind && e.hierarchy.starts_with(&prefix))
        .collect();
    if !kids.is_empty() {
        println!();
        println!("Registered under this family:");
        for e in &kids {
            println!("  {}{}  — {}", kind.scheme(), e.hierarchy, e.summary);
        }
    }
    Ok(())
}

fn print_kind_facts(entry: &IdentifierEntry) {
    match entry.kind {
        UriKind::Danger => {
            // Semantic-class gates are inline-only; the registry delegates
            // to the compiler's own enforcement, so this cannot disagree
            // with what the CLI actually accepts.
            let overridable =
                delightql_core::uri_registry::danger_cli_overridable(&entry.hierarchy);
            println!();
            if overridable {
                println!(
                    "Spellings: inline (~~danger://{} <STATE>~~)  |  CLI --danger {}=<STATE>",
                    entry.hierarchy, entry.hierarchy
                );
            } else {
                println!(
                    "Spelling: inline only — (~~danger://{} <STATE>~~). Semantic-class: \
                     it changes what the query MEANS, so it must be visible in the \
                     query text, never a shell flag.",
                    entry.hierarchy
                );
            }
            println!("Session state: query sys.danger(*)");
        }
        UriKind::Config => {
            println!();
            println!(
                "Spellings: inline (~~config://{} <STATE>~~)  |  CLI --config {}=<STATE>",
                entry.hierarchy, entry.hierarchy
            );
        }
        UriKind::Error => {
            println!();
            println!(
                "Hook: (~~error://{} ~~) matches this error and its family",
                entry.hierarchy
            );
        }
        UriKind::Diagnostic => {
            println!();
            println!("Surfaced by: dql selftest (provider '{}')", {
                entry.hierarchy.split('/').next().unwrap_or(&entry.hierarchy)
            });
        }
    }
}

/// Minimal greedy line wrap for terminal prose.
fn wrap(text: &str, width: usize) -> String {
    let mut out = String::new();
    let mut line = 0usize;
    for word in text.split_whitespace() {
        if line > 0 && line + 1 + word.len() > width {
            out.push('\n');
            line = 0;
        } else if line > 0 {
            out.push(' ');
            line += 1;
        }
        out.push_str(word);
        line += word.len();
    }
    out
}
