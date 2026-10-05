// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A namespace's subtree, enumerated from the session catalog.

use rusqlite::Connection;

use crate::diagnostic::{Internal, Runtime};
use crate::error::Result;

use super::NamespaceKind;

/// One row of the catalog's `namespace` relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceNode {
    id: i64,
    fq: String,
    kind: NamespaceKind,
}

impl NamespaceNode {
    pub(crate) fn id(&self) -> i64 {
        self.id
    }

    pub(crate) fn fq(&self) -> &str {
        &self.fq
    }

    pub(crate) fn kind(&self) -> NamespaceKind {
        self.kind
    }
}

/// A namespace and every namespace under it.
///
/// Membership is judged row by row over the WHOLE `namespace` relation with
/// [`super::is_under`]: no pattern pre-filters the rows, nothing walks
/// `pid`, and nothing stops at a depth. A row that cannot be read, or whose
/// kind does not decode, refuses the enumeration rather than dropping out of
/// it, so the descendants are the complete population.
#[derive(Debug)]
pub(crate) struct Subtree {
    root: NamespaceNode,
    descendants: Vec<NamespaceNode>,
}

impl Subtree {
    /// The subtree rooted at the namespace spelled `fq`. `Ok(None)`: no
    /// namespace is spelled `fq`.
    pub(crate) fn of(conn: &Connection, fq: &str) -> Result<Option<Self>> {
        let mut statement = conn
            .prepare("SELECT id, fq_name, kind FROM namespace")
            .map_err(|e| Runtime::catalog("prepare the namespace tree", e.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(|e| Runtime::catalog("read the namespace tree", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| Runtime::catalog("decode the namespace tree", e.to_string()))?;
        let nodes = rows
            .into_iter()
            .map(|(id, fq, kind)| {
                let kind = NamespaceKind::decode(&fq, kind.as_deref())?;
                Ok(NamespaceNode { id, fq, kind })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut roots = Vec::new();
        let mut descendants = Vec::new();
        for node in nodes {
            if node.fq == fq {
                roots.push(node);
            } else if super::is_under(&node.fq, fq) {
                descendants.push(node);
            }
        }
        let root = match roots.len() {
            0 => return Ok(None),
            1 => roots.remove(0),
            n => {
                return Err(Internal::invariant(
                    "namespace_tree",
                    format!("corrupt catalog: {n} namespaces are spelled '{fq}'"),
                ))
            }
        };
        // Deepest first: a namespace is removed before any namespace its
        // `pid` could name.
        descendants.sort_by(|a, b| {
            super::depth(&b.fq)
                .cmp(&super::depth(&a.fq))
                .then_with(|| b.id.cmp(&a.id))
        });
        Ok(Some(Self { root, descendants }))
    }

    pub(crate) fn root(&self) -> &NamespaceNode {
        &self.root
    }

    /// Every namespace under the root, deepest first.
    pub(crate) fn descendants(&self) -> &[NamespaceNode] {
        &self.descendants
    }

    /// The shallowest descendants, in spelling order. `None`: the root
    /// stands alone.
    pub(crate) fn nearest_descendants(&self) -> Option<Vec<&NamespaceNode>> {
        let depth = self
            .descendants
            .iter()
            .map(|node| super::depth(&node.fq))
            .min()?;
        let mut nearest: Vec<&NamespaceNode> = self
            .descendants
            .iter()
            .filter(|node| super::depth(&node.fq) == depth)
            .collect();
        nearest.sort_by(|a, b| a.fq.cmp(&b.fq));
        Some(nearest)
    }

    /// The row `id` is the root or one of its descendants.
    pub(crate) fn contains(&self, id: i64) -> bool {
        self.root.id == id || self.descendants.iter().any(|node| node.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(rows: &[(i64, &str, &str)]) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE namespace (id INTEGER PRIMARY KEY, fq_name TEXT, kind TEXT NOT NULL)",
        )
        .unwrap();
        for (id, fq, kind) in rows {
            conn.execute(
                "INSERT INTO namespace (id, fq_name, kind) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, fq, kind],
            )
            .unwrap();
        }
        conn
    }

    fn members(subtree: &Subtree) -> Vec<&str> {
        subtree
            .descendants()
            .iter()
            .map(NamespaceNode::fq)
            .collect()
    }

    const ROWS: &[(i64, &str, &str)] = &[
        (1, "_", "system"),
        (2, "lib::a_b", "lib"),
        (3, "lib::a_b::x", "lib"),
        (4, "lib::acb::x", "lib"),
        (5, "lib::a_b::x::y", "lib"),
        (6, "lib::a_bc", "lib"),
        (7, "lib::A", "lib"),
        (8, "lib::a::x", "lib"),
        (9, "lib::A::z", "data"),
        (10, "lib::a_b::w", "grounded"),
        (11, "lib::a_b::x::y::v::u::t", "lib"),
    ];

    #[test]
    fn the_subtree_is_exact_complete_and_deepest_first() {
        let conn = catalog(ROWS);
        let subtree = Subtree::of(&conn, "lib::a_b").unwrap().unwrap();
        assert_eq!(subtree.root().id(), 2);
        assert_eq!(subtree.root().kind(), NamespaceKind::Lib);
        assert_eq!(
            members(&subtree),
            [
                "lib::a_b::x::y::v::u::t",
                "lib::a_b::x::y",
                "lib::a_b::w",
                "lib::a_b::x"
            ]
        );
        for outside in [1, 4, 6, 7, 8, 9] {
            assert!(
                !subtree.contains(outside),
                "row {outside} is not under lib::a_b"
            );
        }
        for inside in [2, 3, 5, 10, 11] {
            assert!(
                subtree.contains(inside),
                "row {inside} is in lib::a_b's subtree"
            );
        }
    }

    #[test]
    fn case_distinguishes_subtrees() {
        let conn = catalog(ROWS);
        let upper = Subtree::of(&conn, "lib::A").unwrap().unwrap();
        assert_eq!(members(&upper), ["lib::A::z"]);
        assert!(Subtree::of(&conn, "lib::a").unwrap().is_none());
        assert!(Subtree::of(&conn, "LIB::A").unwrap().is_none());
    }

    #[test]
    fn the_nearest_descendants_are_the_shallowest_in_spelling_order() {
        let conn = catalog(ROWS);
        let subtree = Subtree::of(&conn, "lib::a_b").unwrap().unwrap();
        let nearest: Vec<&str> = subtree
            .nearest_descendants()
            .unwrap()
            .into_iter()
            .map(NamespaceNode::fq)
            .collect();
        assert_eq!(nearest, ["lib::a_b::w", "lib::a_b::x"]);
        let leaf = Subtree::of(&conn, "lib::acb::x").unwrap().unwrap();
        assert!(leaf.nearest_descendants().is_none());
    }

    #[test]
    fn a_leaf_has_no_descendants() {
        let conn = catalog(ROWS);
        let leaf = Subtree::of(&conn, "lib::acb::x").unwrap().unwrap();
        assert!(leaf.descendants().is_empty());
        assert!(leaf.contains(4));
    }

    #[test]
    fn the_root_row_holds_every_namespace() {
        let conn = catalog(ROWS);
        let whole = Subtree::of(&conn, "_").unwrap().unwrap();
        assert_eq!(whole.descendants().len(), ROWS.len() - 1);
    }

    #[test]
    fn a_doubly_spelled_root_refuses() {
        let conn = catalog(&[(1, "lib::a_b", "lib"), (2, "lib::a_b", "lib")]);
        let err = Subtree::of(&conn, "lib::a_b").unwrap_err();
        assert!(
            err.to_string()
                .contains("2 namespaces are spelled 'lib::a_b'"),
            "{err}"
        );
    }

    #[test]
    fn an_unreadable_row_refuses_rather_than_dropping_out() {
        let conn = catalog(&[(1, "lib::a_b", "lib")]);
        conn.execute(
            "INSERT INTO namespace (id, fq_name, kind) VALUES (2, NULL, 'lib')",
            [],
        )
        .unwrap();
        assert!(Subtree::of(&conn, "lib::a_b").is_err());
    }

    #[test]
    fn an_undecodable_kind_refuses_rather_than_dropping_out() {
        let conn = catalog(&[(1, "lib::a_b", "lib"), (2, "lib::a_b::x", "archive")]);
        let err = Subtree::of(&conn, "lib::a_b").unwrap_err();
        assert_eq!(err.error_uri(), "delightql-error://internal/invariant");
        assert!(err.to_string().contains("lib::a_b::x"), "{err}");
    }
}
