// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// URI registry — the compiler-owned catalog behind `dql explain`.
//
// The registry is the burned `sys::identifiers.identifier` relation. For the
// ERROR kind its rows are a PROJECTION of the typed hierarchy in
// `delightql_types::diagnostic`: bootstrap walks the hierarchy's declared
// inventory (families, emitted leaves, external roots, retired identities)
// and inserts one row per hierarchy with its role. The Rust declaration is
// the active mint authority; nothing here or in bootstrap/schema.sql authors
// an error row by hand, so the table cannot list an identity the compiler
// cannot emit, nor omit one it can. Danger, config and diagnostic rows keep
// their own authorities (bootstrap/schema.sql beside their runtime registries).
//
// Identifier permanence begins at the first public release, or at an
// explicit earlier vocabulary freeze (URI-DESIGN.md §3). From that boundary
// on the hierarchy is append-only: an identity is retired into the RETIRED
// population rather than deleted. Before it, a hierarchy that has appeared
// in no released version may be deleted outright.

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

/// One identifier row, as read from the burned `sys::identifiers.identifier`
/// table.
pub struct IdentifierEntry {
    pub kind: UriKind,
    /// Bare hierarchy, e.g. "semantic/resolution/column".
    pub hierarchy: String,
    /// One-line summary.
    pub summary: String,
    /// Longer explanation shown by `dql explain`.
    pub explanation: String,
    /// The declared role word (`family`, `leaf`, `family_leaf`,
    /// `external_root`, `retired`); every non-error kind is a `leaf`.
    pub role: String,
}

/// UriKind from its URL word ("error" | "danger" | "config") — the
/// spelling the `kind` column of sys::identifiers.identifier uses.
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

/// The canonical https form of an identifier — the fixed binding between
/// a hierarchy and its URL.
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

/// The mintable top segments of the diagnostic kind — one per provider.
/// Only `autoload` emits today; the rest are
/// reserved by the provider inventory so the taxonomy is stable before the
/// providers land. The soundness test keeps diagnostic rows inside this set.
pub const DIAGNOSTIC_TOP_SEGMENTS: &[&str] =
    &["autoload", "adapter", "identity", "catalog", "connectivity"];

#[cfg(test)]
mod tests {
    use super::*;
    use delightql_types::diagnostic::{inventory, Role};
    use std::collections::HashMap;

    #[test]
    fn parses_all_accepted_spellings() {
        assert_eq!(
            parse_identifier("delightql-error://semantic/cast"),
            Some((UriKind::Error, "semantic/cast".to_string()))
        );
        assert_eq!(
            parse_identifier("https://delightql.org/uri/danger/cardinality/cartesian"),
            Some((UriKind::Danger, "cardinality/cartesian".to_string()))
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

    /// The burned rows, loaded exactly the way the live system loads them:
    /// schema plus the bootstrap seeding that projects the typed hierarchy.
    fn burned_rows() -> Vec<(UriKind, String, String, String, String)> {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        let mut stmt = conn
            .prepare("SELECT kind, hierarchy, summary, explanation, role FROM identifier")
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .map(|(k, h, s, e, role)| {
                (
                    kind_from_word(&k).expect("bad kind word in identifier row"),
                    h,
                    s,
                    e,
                    role,
                )
            })
            .collect::<Vec<_>>();
        assert!(!rows.is_empty(), "identifier table must be seeded");
        rows
    }

    #[test]
    fn burned_registry_lookups() {
        let rows = burned_rows();
        let find = |kind: UriKind, h: &str| rows.iter().any(|(k, hh, ..)| *k == kind && hh == h);
        assert!(find(UriKind::Error, "semantic/resolution/column"));
        assert!(!find(UriKind::Error, "not/a/thing"));
        // family listing (segment-prefix semantics)
        let kids = rows
            .iter()
            .filter(|(k, h, ..)| *k == UriKind::Error && h.starts_with("semantic/resolution/"))
            .count();
        assert!(kids >= 3);
        // bare search is unambiguous for this one
        assert_eq!(
            rows.iter()
                .filter(|(_, h, ..)| h == "cardinality/cartesian")
                .count(),
            1
        );
    }

    /// THE REGISTRY IS THE HIERARCHY'S PROJECTION. Every error row is exactly
    /// one declared inventory row with the same role and prose, and every
    /// inventory row is in the table: the two cannot drift because one is
    /// computed from the other, and this pins the computation.
    #[test]
    fn error_rows_are_the_typed_inventory() {
        let declared: HashMap<String, (Role, String, String)> = inventory()
            .into_iter()
            .map(|row| (row.hierarchy, (row.role, row.summary, row.explanation)))
            .collect();
        let mut seen = 0;
        for (kind, hierarchy, summary, explanation, role) in burned_rows() {
            if kind != UriKind::Error {
                continue;
            }
            seen += 1;
            let Some((declared_role, declared_summary, declared_explanation)) =
                declared.get(&hierarchy)
            else {
                panic!("error row '{hierarchy}' is not declared by the typed hierarchy")
            };
            assert_eq!(role, declared_role.word(), "{hierarchy}: role");
            assert_eq!(&summary, declared_summary, "{hierarchy}: summary");
            assert_eq!(
                &explanation, declared_explanation,
                "{hierarchy}: explanation"
            );
        }
        assert_eq!(seen, declared.len(), "every declared hierarchy is a row");
    }

    /// Every error row's top segment is a family the root declares — the
    /// mintable top set is the root enum, not a list kept beside it.
    #[test]
    fn error_entries_stay_inside_the_root_families() {
        let tops: Vec<String> = inventory()
            .into_iter()
            .filter(|row| !row.hierarchy.contains('/'))
            .map(|row| row.hierarchy)
            .collect();
        for (kind, hierarchy, ..) in burned_rows() {
            if kind == UriKind::Error {
                let top = hierarchy.split('/').next().unwrap();
                assert!(
                    tops.iter().any(|t| t == top),
                    "error identifier row '{}' is outside the root families",
                    hierarchy
                );
            }
        }
    }

    #[test]
    fn every_registered_danger_and_config_exists_in_its_runtime_registry() {
        use crate::pipeline::{danger_gates, option_map};
        for (kind, hierarchy, ..) in burned_rows() {
            match kind {
                UriKind::Danger => assert!(
                    danger_gates::known_danger_hierarchies().contains(&hierarchy.as_str()),
                    "identifier row documents unknown danger {} \
(a documented danger is either registered or tombstoned)",
                    hierarchy
                ),
                UriKind::Config => assert!(
                    option_map::known_config_hierarchies().contains(&hierarchy.as_str()),
                    "identifier row documents unknown config {}",
                    hierarchy
                ),
                // Error rows are reconciled by `error_rows_are_the_typed_inventory`;
                // the diagnostic providers are their own source.
                UriKind::Error | UriKind::Diagnostic => {}
            }
        }
    }

    #[test]
    fn diagnostic_entries_stay_inside_the_provider_top_segments() {
        // Every diagnostic row's top segment is a known provider —
        // a row outside them documents a check no provider emits.
        for (kind, hierarchy, ..) in burned_rows() {
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
        let find = |kind: UriKind, h: &str| rows.iter().any(|(k, hh, ..)| *k == kind && hh == h);
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

    #[test]
    fn identifier_rows_are_wellformed() {
        // Row hygiene the schema cannot express: prose non-empty,
        // hierarchies lowercase slash-paths, no accidental scheme prefixes.
        for (_, hierarchy, summary, explanation, role) in burned_rows() {
            assert!(!summary.trim().is_empty(), "{hierarchy}: empty summary");
            assert!(
                !explanation.trim().is_empty(),
                "{hierarchy}: empty explanation"
            );
            assert!(
                !hierarchy.contains("://") && !hierarchy.starts_with('/'),
                "{hierarchy}: hierarchy must be a bare slash-path"
            );
            assert!(
                matches!(
                    role.as_str(),
                    "family" | "leaf" | "family_leaf" | "external_root" | "retired"
                ),
                "{hierarchy}: unknown role {role}"
            );
        }
    }
}
