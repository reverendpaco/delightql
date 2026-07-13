// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// URI registry — the compiler-owned catalog behind `dql explain`.
//
// URI-DESIGN.md §3: "the authority is generated from the compiler
// registry" — this module IS that registry. `dql explain` reads it today;
// the delightql.org/uri/ pages are generated from it later, so the CLI
// and the website can never disagree.
//
// Identifiers are append-only (§3): entries may gain text or successors,
// but a hierarchy, once minted, is never reused for a different meaning.

/// One identifier kind (one compound scheme).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriKind {
    Error,
    Danger,
    Config,
    Diagnostic,
}

impl UriKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            UriKind::Error => "delightql-error://",
            UriKind::Danger => "delightql-danger://",
            UriKind::Config => "delightql-config://",
            UriKind::Diagnostic => "delightql-diagnostic://",
        }
    }

    pub fn word(&self) -> &'static str {
        match self {
            UriKind::Error => "error",
            UriKind::Danger => "danger",
            UriKind::Config => "config",
            UriKind::Diagnostic => "diagnostic",
        }
    }

    pub fn all() -> &'static [UriKind] {
        &[
            UriKind::Error,
            UriKind::Danger,
            UriKind::Config,
            UriKind::Diagnostic,
        ]
    }
}

/// One identifier row, as read from the burned `sys::help.identifier`
/// table (SYS-HELP-DESIGN.md phase 1). The rows are AUTHORED in
/// bootstrap/schema.sql — this module keeps only spelling
/// normalization and identity vocabulary; the registry data itself
/// lives as data.
pub struct IdentifierEntry {
    pub kind: UriKind,
    /// Bare hierarchy, e.g. "semantic/resolution/column".
    pub hierarchy: String,
    /// One-line summary.
    pub summary: String,
    /// Longer explanation shown by `dql explain`.
    pub explanation: String,
}

/// UriKind from its URL word ("error" | "danger" | "config") — the
/// spelling the `kind` column of sys::help.identifier uses.
pub fn kind_from_word(word: &str) -> Option<UriKind> {
    UriKind::all().iter().copied().find(|k| k.word() == word)
}

/// Parse any accepted identifier spelling into (kind, bare hierarchy).
///
/// Accepted: the badge form (`delightql-error://semantic/cast`), the
/// canonical URL (`https://delightql.org/uri/error/semantic/cast`), or a
/// bare hierarchy (searched across all kinds — kind-ambiguous input is
/// the caller's problem to disambiguate via [`find_bare`]).
pub fn parse_identifier(input: &str) -> Option<(UriKind, String)> {
    for kind in UriKind::all() {
        if let Some(rest) = input.strip_prefix(kind.scheme()) {
            return Some((*kind, rest.trim_matches('/').to_string()));
        }
    }
    for base in ["https://delightql.org/uri/", "http://delightql.org/uri/"] {
        if let Some(rest) = input.strip_prefix(base) {
            let rest = rest.trim_matches('/');
            let (word, hier) = rest.split_once('/')?;
            for kind in UriKind::all() {
                if kind.word() == word {
                    return Some((*kind, hier.to_string()));
                }
            }
            return None;
        }
    }
    None
}

/// The canonical https form of an identifier (URI-DESIGN.md §2 binding).
pub fn canonical_url(kind: UriKind, hierarchy: &str) -> String {
    format!("https://delightql.org/uri/{}/{}", kind.word(), hierarchy)
}

/// Whether a danger gate may be overridden from the CLI. Semantic-class
/// gates (they change what the query MEANS) are inline-only. Delegates to
/// the compiler's own enforcement so `dql explain` can never advertise a
/// spelling the CLI would reject.
pub fn danger_cli_overridable(hierarchy: &str) -> bool {
    crate::pipeline::danger_gates::is_cli_overridable(
        &crate::pipeline::danger_gates::canonical_danger_uri(hierarchy),
    )
}

/// The mintable top segments of the error kind — the closed set ratified
/// by the vocabulary audit (URI-DESIGN.md §7). `error_uri()` mints only
/// under these; the soundness test below keeps the registry inside them.
pub const ERROR_TOP_SEGMENTS: &[&str] = &[
    "parse",
    "semantic",
    "dml",
    "operational",
    "runtime",
    "target",
    // Added 2026-07-05 with the CLI panic hook (main.rs mints
    // delightql-error://internal/panic on any Rust panic): dql's own
    // bugs get their own top segment, distinct from runtime/ (the
    // query failed) — internal/ means DQL failed.
    "internal",
    // Added 2026-07-07 with blueprint-inertness enforcement (Change 3):
    // imprint!'s linear lifecycle refusals (imprint/blueprint/inert — an
    // archived blueprint namespace is visible but inert). Its own top
    // segment: not a query semantic error, a lifecycle-policy refusal.
    "imprint",
    // Added 2026-07-08 with the system name guard (namespace work step 3a,
    // catechism Deviation #3): USER-facing namespace creation refuses the
    // reserved system name pool (exact sys/std/home, sys*/std* prefixes, `_`
    // machinery segments, the sys::/std:: subtree). Its own top segment: a
    // creation-policy refusal, not a query semantic error.
    "namespace",
];

/// The mintable top segments of the diagnostic kind — one per provider
/// (DIAGNOSTIC-URI-RFP.md). Only `autoload` emits today; the rest are
/// reserved by the provider inventory so the taxonomy is stable before the
/// providers land. The soundness test keeps diagnostic rows inside this set.
pub const DIAGNOSTIC_TOP_SEGMENTS: &[&str] = &[
    "autoload",
    "adapter",
    "identity",
    "catalog",
    "connectivity",
];

/// Subcategory constants (STRING-FLOOR.md Tier 2a). Error sites reference
/// these — never raw string literals — and the `subcategory_constants_are_
/// registered` test below asserts every constant resolves to a registered
/// hierarchy under its family's render prefix (`error_uri()`: Validation →
/// `semantic/<sub>`, Parse → `parse/<sub>`). A typo'd subcategory can no
/// longer silently mint a phantom identifier.
pub mod subcat {
    /// ValidationError family — rendered as `semantic/<const>`.
    pub const RECURSION_LIMIT_BOUND: &str = "recursion/limit_bound";
    pub const RECURSION_ARGUMENTATIVE_BINDING: &str = "recursion/argumentative_binding";
    pub const RECURSION_CONSULTED_CLAUSE_ORDER: &str = "recursion/consulted_clause_order";
    pub const COMPOUND_SCALAR_COLUMN: &str = "compound/scalar_column";
    pub const SEMANTIC_FAMILY: &[&str] = &[
        RECURSION_LIMIT_BOUND,
        RECURSION_ARGUMENTATIVE_BINDING,
        RECURSION_CONSULTED_CLAUSE_ORDER,
        COMPOUND_SCALAR_COLUMN,
    ];

    /// ParseError family — rendered as `parse/<const>`.
    pub const PARSE_DDL: &str = "ddl";
    pub const PARSE_SIGIL: &str = "sigil";
    pub const PARSE_FAMILY: &[&str] = &[PARSE_DDL, PARSE_SIGIL];
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_accepted_spellings() {
        assert_eq!(
            parse_identifier("delightql-error://semantic/cast"),
            Some((UriKind::Error, "semantic/cast".to_string()))
        );
        assert_eq!(
            parse_identifier("https://delightql.org/uri/danger/cardinality/nulljoin"),
            Some((UriKind::Danger, "cardinality/nulljoin".to_string()))
        );
        assert_eq!(parse_identifier("no-scheme-here"), None);
        assert_eq!(parse_identifier("mailto://x"), None);
    }

    #[test]
    fn canonical_url_is_the_binding() {
        assert_eq!(
            canonical_url(UriKind::Error, "semantic/cast"),
            "https://delightql.org/uri/error/semantic/cast"
        );
    }

    /// The burned rows, loaded exactly the way the live system loads
    /// them: by executing bootstrap/schema.sql. The table is the source
    /// (SYS-HELP-DESIGN.md phase 1); these tests keep it sound.
    fn burned_rows() -> Vec<(UriKind, String, String, String)> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::bootstrap::BOOTSTRAP_SCHEMA).unwrap();
        let mut stmt = conn
            .prepare("SELECT kind, hierarchy, summary, explanation FROM identifier")
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .map(|(k, h, s, e)| (kind_from_word(&k).expect("bad kind word in identifier row"), h, s, e))
            .collect::<Vec<_>>();
        assert!(!rows.is_empty(), "identifier table must be seeded");
        rows
    }

    #[test]
    fn burned_registry_lookups() {
        let rows = burned_rows();
        let find = |kind: UriKind, h: &str| {
            rows.iter().any(|(k, hh, _, _)| *k == kind && hh == h)
        };
        assert!(find(UriKind::Error, "semantic/resolution/column"));
        assert!(!find(UriKind::Error, "not/a/thing"));
        // family listing (segment-prefix semantics)
        let kids = rows
            .iter()
            .filter(|(k, h, _, _)| *k == UriKind::Error && h.starts_with("semantic/resolution/"))
            .count();
        assert!(kids >= 3);
        // bare search is unambiguous for this one
        assert_eq!(
            rows.iter().filter(|(_, h, _, _)| h == "cardinality/nulljoin").count(),
            1
        );
    }

    #[test]
    fn every_registered_danger_and_config_exists_in_its_runtime_registry() {
        use crate::pipeline::{danger_gates, option_map};
        for (kind, hierarchy, _, _) in burned_rows() {
            match kind {
                UriKind::Danger => assert!(
                    danger_gates::known_danger_hierarchies().contains(&hierarchy.as_str()),
                    "identifier row documents unknown danger {}",
                    hierarchy
                ),
                UriKind::Config => assert!(
                    option_map::known_config_hierarchies().contains(&hierarchy.as_str()),
                    "identifier row documents unknown config {}",
                    hierarchy
                ),
                // No separate runtime registry to reconcile against — the
                // diagnostic providers are the source. Soundness is the
                // top-segment test below.
                UriKind::Error | UriKind::Diagnostic => {}
            }
        }
    }

    #[test]
    fn error_entries_stay_inside_the_mintable_top_segments() {
        // Soundness: an identifier row whose hierarchy starts outside the
        // ratified top set documents a phantom nothing can mint.
        for (kind, hierarchy, _, _) in burned_rows() {
            if kind == UriKind::Error {
                let top = hierarchy.split('/').next().unwrap();
                assert!(
                    ERROR_TOP_SEGMENTS.contains(&top),
                    "error identifier row '{}' is outside the mintable top segments",
                    hierarchy
                );
            }
        }
    }

    #[test]
    fn diagnostic_entries_stay_inside_the_provider_top_segments() {
        // Every diagnostic row's top segment is a known provider
        // (DIAGNOSTIC-URI-RFP.md) — a row outside them documents a check no
        // provider emits.
        for (kind, hierarchy, _, _) in burned_rows() {
            if kind == UriKind::Diagnostic {
                let top = hierarchy.split('/').next().unwrap();
                assert!(
                    DIAGNOSTIC_TOP_SEGMENTS.contains(&top),
                    "diagnostic identifier row '{}' is outside the provider top segments",
                    hierarchy
                );
            }
        }
    }

    #[test]
    fn every_runtime_gate_and_config_is_documented() {
        use crate::pipeline::{danger_gates, option_map};
        let rows = burned_rows();
        let find = |kind: UriKind, h: &str| {
            rows.iter().any(|(k, hh, _, _)| *k == kind && hh == h)
        };
        for h in danger_gates::known_danger_hierarchies() {
            assert!(
                find(UriKind::Danger, h),
                "danger {} has no identifier row — document it",
                h
            );
        }
        for h in option_map::known_config_hierarchies() {
            assert!(
                find(UriKind::Config, h),
                "config {} has no identifier row — document it",
                h
            );
        }
    }

    /// STRING-FLOOR.md Tier 2a: every subcategory constant must resolve to
    /// an identifier row under its family's render prefix (error_uri:
    /// Validation → semantic/<sub>, Parse → parse/<sub>). Error sites use
    /// the constants, never raw literals — so a typo'd subcategory fails
    /// HERE instead of silently minting a phantom identifier at runtime.
    /// (Ported to the burned rows at the sys::help phase-1 cutover.)
    #[test]
    fn subcategory_constants_are_registered() {
        let rows = burned_rows();
        let find = |h: &str| {
            rows.iter()
                .any(|(k, hh, _, _)| *k == UriKind::Error && hh == h)
        };
        for sub in subcat::SEMANTIC_FAMILY {
            let h = format!("semantic/{}", sub);
            assert!(
                find(&h),
                "subcategory constant '{}' has no identifier row at '{}' — \
                 register it (append-only) or fix the constant",
                sub,
                h
            );
        }
        for sub in subcat::PARSE_FAMILY {
            let h = format!("parse/{}", sub);
            assert!(
                find(&h),
                "subcategory constant '{}' has no identifier row at '{}' — \
                 register it (append-only) or fix the constant",
                sub,
                h
            );
        }
    }

    #[test]
    fn identifier_rows_are_wellformed() {
        // Append-only hygiene the schema cannot express: prose non-empty,
        // hierarchies lowercase slash-paths, no accidental scheme prefixes.
        for (_, hierarchy, summary, explanation) in burned_rows() {
            assert!(!summary.trim().is_empty(), "{hierarchy}: empty summary");
            assert!(!explanation.trim().is_empty(), "{hierarchy}: empty explanation");
            assert!(
                !hierarchy.contains("://") && !hierarchy.starts_with('/'),
                "{hierarchy}: hierarchy must be a bare slash-path"
            );
        }
    }
}
