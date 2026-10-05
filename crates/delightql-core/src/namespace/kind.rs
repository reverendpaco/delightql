// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A namespace's kind: which producer made it, and so which lifecycle verbs
//! apply to it.
//!
//! The catalog stores the kind as text. The population is closed: the
//! bootstrap schema's `CHECK` admits exactly the spellings below. The
//! lifecycle directives read a kind only through [`NamespaceKind::decode`]
//! and match it exhaustively, so a new kind is a compile error at each
//! directive that has not decided it, never a fall-through at run time.

use std::fmt;

use crate::diagnostic::Internal;
use crate::error::Result;

/// The `kind` column of the catalog's `namespace` relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NamespaceKind {
    /// Bootstrap territory: `sys`, `std` and their modules.
    System,
    /// A structural node a mount path created above its data namespace.
    Container,
    /// A mount: its truth lives in an external connection.
    Data,
    /// A consulted library: its truth is authored source.
    Lib,
    /// Definitions authored in the session with `(~~ddl … ~~)`.
    Scratch,
    /// A world `ground!` derived from a library.
    Grounded,
    /// The root of an `imprint!` archive, `{target}::_N_blueprint`. Its
    /// descendants keep their own kinds.
    Blueprint,
    /// A node whose producer recorded no kind: the structural ancestors a
    /// consult path creates, and the column's default.
    Unknown,
}

impl NamespaceKind {
    /// Every kind, in declaration order.
    #[cfg(test)]
    pub(crate) const ALL: [NamespaceKind; 8] = [
        NamespaceKind::System,
        NamespaceKind::Container,
        NamespaceKind::Data,
        NamespaceKind::Lib,
        NamespaceKind::Scratch,
        NamespaceKind::Grounded,
        NamespaceKind::Blueprint,
        NamespaceKind::Unknown,
    ];

    /// The kind the catalog spells `spelled` for the namespace `fq`. A `NULL`
    /// cell reads as [`NamespaceKind::Unknown`], the column's default. Any
    /// other spelling refuses: no producer writes it, so the catalog is
    /// corrupt, and no verb may guess what it means.
    pub(crate) fn decode(fq: &str, spelled: Option<&str>) -> Result<Self> {
        Ok(match spelled {
            None | Some("unknown") => NamespaceKind::Unknown,
            Some("system") => NamespaceKind::System,
            Some("container") => NamespaceKind::Container,
            Some("data") => NamespaceKind::Data,
            Some("lib") => NamespaceKind::Lib,
            Some("scratch") => NamespaceKind::Scratch,
            Some("grounded") => NamespaceKind::Grounded,
            Some("blueprint") => NamespaceKind::Blueprint,
            Some(other) => {
                return Err(Internal::invariant(
                    "namespace_kind",
                    format!("corrupt catalog: namespace '{fq}' has kind '{other}', which no producer writes"),
                ))
            }
        })
    }

    /// The catalog's spelling of this kind.
    pub(crate) fn spelling(self) -> &'static str {
        match self {
            NamespaceKind::System => "system",
            NamespaceKind::Container => "container",
            NamespaceKind::Data => "data",
            NamespaceKind::Lib => "lib",
            NamespaceKind::Scratch => "scratch",
            NamespaceKind::Grounded => "grounded",
            NamespaceKind::Blueprint => "blueprint",
            NamespaceKind::Unknown => "unknown",
        }
    }
}

impl fmt::Display for NamespaceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.spelling())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_decodes_from_its_own_spelling() {
        for kind in NamespaceKind::ALL {
            assert_eq!(
                NamespaceKind::decode("n", Some(kind.spelling())).unwrap(),
                kind
            );
        }
        assert_eq!(
            NamespaceKind::decode("n", None).unwrap(),
            NamespaceKind::Unknown
        );
    }

    #[test]
    fn a_spelling_no_producer_writes_refuses_without_a_panic() {
        for spelled in ["Blueprint", "archive", "", " lib"] {
            let error = NamespaceKind::decode("lib::x", Some(spelled))
                .expect_err("an unrecognized kind must refuse");
            assert_eq!(error.error_uri(), "delightql-error://internal/invariant");
            assert!(error.to_string().contains("lib::x"), "{error}");
        }
    }

    /// The store and the decoder agree on the population: the bootstrap
    /// schema admits every kind's spelling and refuses any other.
    #[test]
    fn the_bootstrap_store_admits_exactly_the_decodable_kinds() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        for kind in NamespaceKind::ALL {
            let fq = format!("probe::{}", kind.spelling());
            conn.execute(
                "INSERT INTO namespace (name, fq_name, kind) VALUES ('p', ?1, ?2)",
                rusqlite::params![fq, kind.spelling()],
            )
            .unwrap_or_else(|e| panic!("the store must admit '{kind}': {e}"));
            let stored: String = conn
                .query_row(
                    "SELECT kind FROM namespace WHERE fq_name = ?1",
                    [&fq],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(NamespaceKind::decode(&fq, Some(&stored)).unwrap(), kind);
        }
        conn.execute(
            "INSERT INTO namespace (name, fq_name) VALUES ('d', 'probe::default')",
            [],
        )
        .unwrap();
        let defaulted: String = conn
            .query_row(
                "SELECT kind FROM namespace WHERE fq_name = 'probe::default'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            NamespaceKind::decode("probe::default", Some(&defaulted)).unwrap(),
            NamespaceKind::Unknown
        );
        for spelled in ["archive", "Blueprint", ""] {
            assert!(
                conn.execute(
                    "INSERT INTO namespace (name, fq_name, kind) VALUES ('x', 'probe::x', ?1)",
                    [spelled],
                )
                .is_err(),
                "the store must refuse kind '{spelled}'"
            );
        }
    }
}
