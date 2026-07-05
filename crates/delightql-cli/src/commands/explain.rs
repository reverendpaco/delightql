// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// `dql explain <identifier>` — the registry-backed identifier explainer.
//
// The window into the compiler's identifier taxonomy (URI-DESIGN.md §3):
// reads the same compiler-owned registry the delightql.org/uri/ pages are
// generated from, so the CLI and the website cannot disagree.

use anyhow::Result;
use delightql_core::uri_registry::{
    canonical_url, children, find, find_bare, parse_identifier, RegistryEntry, UriKind,
};

pub fn handle_explain(identifier: &str) -> Result<()> {
    match parse_identifier(identifier) {
        Some((kind, hierarchy)) => explain_one(kind, &hierarchy),
        None => {
            // Bare hierarchy: search across kinds.
            let bare = identifier.trim_matches('/');
            let hits = find_bare(bare);
            match hits.len() {
                1 => explain_one(hits[0].kind, hits[0].hierarchy),
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

fn explain_one(kind: UriKind, hierarchy: &str) -> Result<()> {
    println!("{}{}", kind.scheme(), hierarchy);
    println!("  → {}", canonical_url(kind, hierarchy));
    println!();

    match find(kind, hierarchy) {
        Some(entry) => {
            println!("{}", entry.summary);
            println!();
            println!("{}", wrap(entry.explanation, 74));
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

    let kids = children(kind, hierarchy);
    if !kids.is_empty() {
        println!();
        println!("Registered under this family:");
        for e in &kids {
            println!("  {}{}  — {}", kind.scheme(), e.hierarchy, e.summary);
        }
    }
    Ok(())
}

fn print_kind_facts(entry: &RegistryEntry) {
    match entry.kind {
        UriKind::Danger => {
            // Semantic-class gates are inline-only; the registry delegates
            // to the compiler's own enforcement, so this cannot disagree
            // with what the CLI actually accepts.
            let overridable =
                delightql_core::uri_registry::danger_cli_overridable(entry.hierarchy);
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
