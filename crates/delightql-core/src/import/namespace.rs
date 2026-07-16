// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Namespace Creation and Management
//
// This module handles creating and managing the namespace hierarchy.
// Namespaces provide logical organization for entities.
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

use anyhow::Result;
use rusqlite::Connection;

/// Namespace definition for batch creation
#[derive(Debug, Clone)]
pub struct NamespaceSpec {
    /// Namespace ID (for deterministic ordering)
    pub id: i32,
    /// Namespace segment name (e.g., "sys", "cartridges")
    pub name: String,
    /// Parent namespace ID (None for root)
    pub pid: Option<i32>,
    /// Fully-qualified name (e.g., "sys::cartridges")
    pub fq_name: String,
    /// Namespace kind: system, container, data, lib, grounded, scratch, unknown
    pub kind: String,
    /// How the namespace was created: bootstrap, file, uri, scratch, ground
    pub provenance: Option<String>,
    /// Original file path or URI (for refresh/reconsult)
    pub source_path: Option<String>,
    /// Can accept new definitions (true = scratch only)
    pub writable: bool,
}

/// Create a single namespace
///
/// This is Step 4 of the bootstrap process (REUSABLE).
/// Creates a namespace with a given name and parent.
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `id` - Namespace ID (for deterministic IDs)
/// * `name` - Namespace segment name
/// * `pid` - Parent namespace ID (None for root)
/// * `fq_name` - Fully-qualified namespace path
/// * `kind` - Namespace kind (system, container, data, lib, grounded, scratch, unknown)
/// * `provenance` - How it was created (bootstrap, file, uri, scratch, ground)
/// * `source_path` - Original file path or URI
/// * `writable` - Whether the namespace can accept new definitions
pub fn create_namespace(
    conn: &Connection,
    id: i32,
    name: &str,
    pid: Option<i32>,
    fq_name: &str,
    kind: &str,
    provenance: Option<&str>,
    source_path: Option<&str>,
    writable: bool,
) -> Result<i32> {
    conn.execute(
        "INSERT INTO namespace (id, name, pid, fq_name, kind, provenance, source_path, writable)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            id,
            name,
            pid,
            fq_name,
            kind,
            provenance,
            source_path,
            writable as i32
        ],
    )?;

    Ok(id)
}

/// Create multiple namespaces in a hierarchy
///
/// Convenient batch operation for creating a namespace tree.
/// Ensures proper ordering (parents before children).
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `specs` - List of namespace specifications
///
/// # Returns
/// * `Ok(())` - If all namespaces created successfully
/// * `Err(anyhow::Error)` - If any creation fails
///
/// # Example
/// ```
/// use delightql_core::import::namespace::{create_namespace_hierarchy, NamespaceSpec};
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory().unwrap();
/// // ... initialize bootstrap schema ...
///
/// let specs = vec![
///     NamespaceSpec { id: 1, name: "_".into(), pid: None, fq_name: "_".into() },
///     NamespaceSpec { id: 2, name: "sys".into(), pid: Some(1), fq_name: "sys".into() },
/// ];
///
/// create_namespace_hierarchy(&conn, &specs).unwrap();
/// ```
pub fn create_namespace_hierarchy(conn: &Connection, specs: &[NamespaceSpec]) -> Result<()> {
    for spec in specs {
        create_namespace(
            conn,
            spec.id,
            &spec.name,
            spec.pid,
            &spec.fq_name,
            &spec.kind,
            spec.provenance.as_deref(),
            spec.source_path.as_deref(),
            spec.writable,
        )?;
    }
    Ok(())
}

/// Create the bootstrap namespace hierarchy
///
/// Creates the standard DelightQL bootstrap namespaces:
/// - _ (root)
/// - sys (system)
///   - sys::cartridges
///   - sys::entities
///   - sys::ns
/// - main (user default)
/// - std (standard library)
///   - std::predicates
/// - home (user scratch container)
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
///
/// # Returns
/// * `Ok(())` - If all namespaces created
/// * `Err(anyhow::Error)` - If creation fails
pub fn create_bootstrap_namespaces(conn: &Connection) -> Result<()> {
    let specs = vec![
        NamespaceSpec {
            id: 1,
            name: "_".into(),
            pid: None,
            fq_name: "_".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 2,
            name: "sys".into(),
            pid: Some(1),
            fq_name: "sys".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 3,
            name: "cartridges".into(),
            pid: Some(2),
            fq_name: "sys::cartridges".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 4,
            name: "entities".into(),
            pid: Some(2),
            fq_name: "sys::entities".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 5,
            name: "ns".into(),
            pid: Some(2),
            fq_name: "sys::ns".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 6,
            name: "main".into(),
            pid: Some(1),
            fq_name: "main".into(),
            kind: "data".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 7,
            name: "std".into(),
            pid: Some(1),
            fq_name: "std".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 8,
            name: "predicates".into(),
            pid: Some(7),
            fq_name: "std::predicates".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 9,
            name: "meta".into(),
            pid: Some(2),
            fq_name: "sys::meta".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 10,
            name: "execution".into(),
            pid: Some(2),
            fq_name: "sys::execution".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 11,
            name: "identifiers".into(),
            pid: Some(2),
            fq_name: "sys::identifiers".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 12,
            name: "targeting".into(),
            pid: Some(2),
            fq_name: "sys::targeting".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 13,
            name: "connections".into(),
            pid: Some(2),
            fq_name: "sys::connections".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        // id 14 is intentionally skipped: reserved for `sys::session`
        // (assertions/danger/errors), deferred as Part 1b — see
        // SYS-NAMESPACE-TAXONOMY.md. Those tables stay bare in `sys` for now.
        NamespaceSpec {
            id: 15,
            name: "ho".into(),
            pid: Some(4),
            fq_name: "sys::entities::ho".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        NamespaceSpec {
            id: 16,
            name: "interior".into(),
            pid: Some(4),
            fq_name: "sys::entities::interior".into(),
            kind: "system".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: false,
        },
        // `home` — the user's scratch container (catechism §II/§IV, Deviation #2).
        // Parented to the root `_` (id=1), like `main`/`sys`/`std`. It is the
        // enlisted destination for in-session authored definitions. Ships typed
        // as `lib` from day one (the source text you just typed is its truth),
        // and is the only writable bootstrap namespace: scratch DDL lands here.
        // id 14 is reserved for a future `sys::session`, so home takes 17 (the
        // next genuinely free id); dynamic namespaces autoincrement above it.
        NamespaceSpec {
            id: 17,
            name: "home".into(),
            pid: Some(1),
            fq_name: "home".into(),
            kind: "lib".into(),
            provenance: Some("bootstrap".into()),
            source_path: None,
            writable: true,
        },
    ];

    create_namespace_hierarchy(conn, &specs)
}

/// Parse a namespace path string into NamespaceSpecs
///
/// Converts a qualified namespace path (e.g., "std::prelude") into a vector
/// of NamespaceSpecs for batch creation. Automatically determines IDs based
/// on existing namespaces in the database.
///
/// # Arguments
///
/// * `conn` - Connection to _bootstrap database (to query existing namespaces)
/// * `namespace_path` - Qualified namespace path (e.g., "std::prelude", "main")
///
/// # Returns
///
/// * `Ok(Vec<NamespaceSpec>)` - Specs for all namespaces in the path
/// * `Err(anyhow::Error)` - If parsing or database query fails
///
/// # Example
///
/// ```
/// // For "std::prelude":
/// // Creates specs for: "_", "std", "std::prelude"
/// // (assuming "_" and "std" don't exist yet)
/// ```
pub fn parse_namespace_path(conn: &Connection, namespace_path: &str) -> Result<Vec<NamespaceSpec>> {
    let segments: Vec<&str> = namespace_path.split("::").collect();

    // Query highest existing namespace ID
    let max_id: i32 = conn
        .query_row("SELECT COALESCE(MAX(id), 0) FROM namespace", [], |row| {
            row.get(0)
        })
        .unwrap_or(0);

    let mut next_id = max_id + 1;
    let mut specs = Vec::new();
    let mut current_fq_path = String::new();
    let mut _parent_id: Option<i32> = None;

    let root_exists: bool = conn
        .query_row("SELECT 1 FROM namespace WHERE fq_name = '_'", [], |_| {
            Ok(true)
        })
        .unwrap_or(false);

    if !root_exists {
        specs.push(NamespaceSpec {
            id: next_id,
            name: "_".to_string(),
            pid: None,
            fq_name: "_".to_string(),
            kind: "unknown".into(),
            provenance: None,
            source_path: None,
            writable: false,
        });
        _parent_id = Some(next_id);
        next_id += 1;
    } else {
        // Root exists, get its ID
        _parent_id = Some(conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = '_'",
            [],
            |row| row.get(0),
        )?);
    }

    for (i, segment) in segments.iter().enumerate() {
        if i == 0 {
            current_fq_path = segment.to_string();
        } else {
            current_fq_path = format!("{}::{}", current_fq_path, segment);
        }

        // Check if this namespace already exists
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM namespace WHERE fq_name = ?1",
                [&current_fq_path],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if !exists {
            specs.push(NamespaceSpec {
                id: next_id,
                name: segment.to_string(),
                pid: _parent_id,
                fq_name: current_fq_path.clone(),
                kind: "unknown".into(),
                provenance: None,
                source_path: None,
                writable: false,
            });
            _parent_id = Some(next_id);
            next_id += 1;
        } else {
            _parent_id = Some(conn.query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [&current_fq_path],
                |row| row.get(0),
            )?);
        }
    }

    Ok(specs)
}

/// Create namespace hierarchy from a path string and return the final namespace ID
///
/// Convenience function that parses a namespace path and creates all necessary
/// namespaces, then returns the ID of the final (leaf) namespace.
///
/// # Arguments
///
/// * `conn` - Connection to _bootstrap database
/// * `namespace_path` - Qualified namespace path (e.g., "std::prelude")
///
/// # Returns
///
/// * `Ok(namespace_id)` - ID of the final namespace in the path
/// * `Err(anyhow::Error)` - If creation fails
pub fn create_namespace_from_path(conn: &Connection, namespace_path: &str) -> Result<i32> {
    let specs = parse_namespace_path(conn, namespace_path)?;

    if specs.is_empty() {
        // Namespace already exists, just get its ID
        let id = conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [namespace_path],
            |row| row.get(0),
        )?;
        return Ok(id);
    }

    // Create the hierarchy
    create_namespace_hierarchy(conn, &specs)?;

    // Return the ID of the final namespace
    let final_id = specs.last().unwrap().id;
    Ok(final_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::initialize_bootstrap_db;

    /// The root row `_` is the tree's root: id=1, pid=NULL, kind=system.
    /// home parents to it (catechism §II).
    #[test]
    fn test_root_row_is_id_1() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();
        create_bootstrap_namespaces(&conn).unwrap();

        let (id, pid, kind): (i32, Option<i32>, String) = conn
            .query_row(
                "SELECT id, pid, kind FROM namespace WHERE fq_name = '_'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(id, 1, "root `_` must be id=1");
        assert_eq!(pid, None, "root `_` must have NULL parent");
        assert_eq!(kind, "system", "root `_` must be system-kind");
    }

    /// `home` ships as a real bootstrap namespace: parented to root, lib-kind,
    /// writable, bootstrap-provenance (catechism §II/§IV, Deviation #2).
    #[test]
    fn test_home_bootstrap_row() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();
        create_bootstrap_namespaces(&conn).unwrap();

        // Root's id, to assert home is parented to it (not by hardcoded 1).
        let root_id: i32 = conn
            .query_row("SELECT id FROM namespace WHERE fq_name = '_'", [], |r| {
                r.get(0)
            })
            .unwrap();

        let (id, name, pid, fq_name, kind, provenance, writable): (
            i32,
            String,
            Option<i32>,
            String,
            String,
            Option<String>,
            i32,
        ) = conn
            .query_row(
                "SELECT id, name, pid, fq_name, kind, provenance, writable
                 FROM namespace WHERE fq_name = 'home'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .expect("home bootstrap row must exist");

        assert_eq!(name, "home");
        assert_eq!(fq_name, "home");
        assert_eq!(pid, Some(root_id), "home parents to root `_`");
        assert_eq!(kind, "lib", "home ships typed as lib (scratch container)");
        assert_eq!(
            provenance.as_deref(),
            Some("bootstrap"),
            "home is a bootstrap namespace"
        );
        assert_eq!(writable, 1, "home is user territory — writable");
        // id 14 is reserved for sys::session; home takes the next free id.
        assert_eq!(id, 17, "home takes id 17 (14 reserved for sys::session)");
    }
}
