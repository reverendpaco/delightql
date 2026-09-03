// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE REACH: which namespaces a lexical world can name — captured ONCE,
//! through the statement's catalog read, when the world is built.
//!
//! A world is rooted at one namespace and reaches the session's
//! enlistments into that root, the enlistments the root's own file
//! declared, and their exposures, transitively. A SESSION world answers
//! qualifier aliases from the session's alias table; a DECLARATION world
//! answers them from the root file's own local aliases. The catalog holds
//! exactly the current edges and families, and the statement's read keeps
//! them current for the statement's extent — so the reach records which
//! NAMESPACES a world names, never which load declared them.

use std::collections::HashMap;

use crate::error::{DelightQLError, Result};

/// One namespace as the reach captured it.
#[derive(Debug, Clone)]
pub(crate) struct ReachedNamespace {
    pub(crate) id: i64,
    pub(crate) fq: String,
    pub(crate) kind: String,
    /// The data world an explicit grounding published for this namespace,
    /// or a scratch namespace's ambient data world.
    pub(crate) default_data_ns: Option<String>,
}

/// Which kind of world roots the reach — the alias table it answers from.
#[derive(Debug, Clone, Copy)]
pub(in crate::defuse) enum World {
    /// The session scope: the session's aliases.
    Session,
    /// A declaration: the root namespace's own local aliases.
    Declaration,
}

/// The captured reach of one lexical world. Private fields: a lookup asks
/// the reach a question and receives candidates; nothing reads the
/// namespace lists back out to compose a lookup of its own.
#[derive(Debug, Clone)]
pub(crate) struct DeclarationReach {
    root: ReachedNamespace,
    /// Every namespace reachable from the root — the root first, then its
    /// enlistments, local enlistments, and exposures, transitively.
    namespaces: Vec<ReachedNamespace>,
    /// Qualifier aliases: the session's at the prompt, the root namespace's
    /// own local aliases inside a declaration.
    aliases: HashMap<String, ReachedNamespace>,
    /// Every catalog namespace at capture, by fully qualified name, for
    /// qualified references that name a namespace outside the reach.
    catalog: HashMap<String, ReachedNamespace>,
    /// THE DERIVED WORLD'S CLOSURE, when the root is a derivative of a
    /// grounding: each source namespace the grounding derived, keyed by
    /// id, to the derivative standing for it in this world. A qualified
    /// reference from inside the world names its source's derivative
    /// through this map; every other world's map is empty.
    closure: HashMap<i64, ReachedNamespace>,
}

impl DeclarationReach {
    /// A reach that names nothing — the registry built without a system
    /// (unit tests, the DDL manifest road) and the WASM build.
    pub(crate) fn empty(root_fq: &str) -> Self {
        let root = ReachedNamespace {
            id: -1,
            fq: root_fq.to_string(),
            kind: "unknown".to_string(),
            default_data_ns: None,
        };
        DeclarationReach {
            namespaces: vec![root.clone()],
            root,
            aliases: HashMap::new(),
            catalog: HashMap::new(),
            closure: HashMap::new(),
        }
    }

    pub(crate) fn root_fq(&self) -> &str {
        &self.root.fq
    }

    pub(crate) fn root_default_data_ns(&self) -> Option<&str> {
        self.root.default_data_ns.as_deref()
    }

    /// The root namespace's kind — `grounded` for a product of `ground!`.
    pub(crate) fn root_kind(&self) -> &str {
        &self.root.kind
    }

    /// The captured facts of a namespace named by a qualifier: the reach's
    /// own entry when the namespace is reachable, the catalog's capture
    /// otherwise. `None` when no such namespace existed at capture.
    pub(crate) fn namespace(&self, fq: &str) -> Option<&ReachedNamespace> {
        self.namespaces
            .iter()
            .find(|namespace| namespace.fq == fq)
            .or_else(|| self.catalog.get(fq))
    }

    /// The namespace a qualifier alias names, if the alias is one this
    /// world declared.
    pub(crate) fn alias_target(&self, alias: &str) -> Option<&ReachedNamespace> {
        self.aliases.get(alias)
    }

    /// The namespace this world reads for a namespace a qualifier named:
    /// inside a derived world, a source the grounding derived answers as
    /// its derivative; everywhere else, and for every namespace the
    /// grounding did not derive, the namespace itself.
    pub(crate) fn derivative_of(&self, namespace: &ReachedNamespace) -> ReachedNamespace {
        self.closure
            .get(&namespace.id)
            .cloned()
            .unwrap_or_else(|| namespace.clone())
    }

    /// The SQL list of every namespace id in the reach.
    pub(in crate::defuse) fn namespace_id_list(&self) -> String {
        int_list(self.namespaces.iter().map(|namespace| namespace.id))
    }
}

/// A SQL `IN` list of integers. Integers only: the list is built from ids
/// the catalog handed back, never from authored text. An empty list renders
/// `(NULL)` — an empty IN list is a SQLite syntax error, and `(NULL)`
/// matches nothing, which is what an empty reach answers.
pub(in crate::defuse) fn int_list(ids: impl Iterator<Item = i64>) -> String {
    let list: Vec<String> = ids.map(|id| id.to_string()).collect();
    if list.is_empty() {
        "(NULL)".to_string()
    } else {
        format!("({})", list.join(","))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::defuse) fn capture(
    catalog: super::super::CatalogRead<'_>,
    root_fq: &str,
    world: World,
) -> Result<DeclarationReach> {
    let conn = catalog.connection("Failed to acquire bootstrap lock for reach capture")?;
    capture_on(&conn, root_fq, world)
}

/// The capture itself, on an open bootstrap connection.
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::defuse) fn capture_on(
    conn: &rusqlite::Connection,
    root_fq: &str,
    world: World,
) -> Result<DeclarationReach> {
    // Every namespace's facts, once. The catalog is small (tens of rows);
    // reading it whole is what lets qualified references outside the reach
    // answer from the same capture.
    let mut catalog: HashMap<String, ReachedNamespace> = HashMap::new();
    {
        let mut stmt = conn
            .prepare("SELECT n.id, n.fq_name, n.kind, n.default_data_ns FROM namespace n")
            .map_err(|e| DelightQLError::database_error("prepare reach capture", e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(|e| DelightQLError::database_error("run reach capture", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| DelightQLError::database_error("decode reach capture", e.to_string()))?;
        for (id, fq, kind, default_data_ns) in rows {
            let Some(fq) = fq else {
                continue;
            };
            catalog.insert(
                fq.clone(),
                ReachedNamespace {
                    id,
                    fq,
                    kind,
                    default_data_ns,
                },
            );
        }
    }

    let Some(root) = catalog.get(root_fq).cloned() else {
        return Ok(DeclarationReach {
            root: ReachedNamespace {
                id: -1,
                fq: root_fq.to_string(),
                kind: "unknown".to_string(),
                default_data_ns: None,
            },
            namespaces: Vec::new(),
            aliases: HashMap::new(),
            catalog,
            closure: HashMap::new(),
        });
    };

    // The derived world's closure, when the root is a derivative: the
    // grounding's own record of which derivative stands for which source.
    let closure: HashMap<i64, ReachedNamespace> = if root.kind == "grounded" {
        crate::defuse::grounded_world::closure_of(conn, root.id)?
            .into_iter()
            .filter_map(|(source_id, derived_id)| {
                catalog
                    .values()
                    .find(|namespace| namespace.id == derived_id)
                    .cloned()
                    .map(|derivative| (source_id, derivative))
            })
            .collect()
    } else {
        HashMap::new()
    };

    // The reachable set: the root, the session's enlistments into it, the
    // root's own declared enlistments, and their exposures, transitively.
    let walk = "WITH RECURSIVE
         reach(ns_id) AS (
             SELECT ?1
             UNION
             SELECT en.from_namespace_id
             FROM enlisted_namespace en
             WHERE en.to_namespace_id = ?1
             UNION
             SELECT nle.enlisted_namespace_id
             FROM namespace_local_enlist nle
             WHERE nle.namespace_id = ?1
             UNION
             SELECT exp.exposed_namespace_id
             FROM exposed_namespace exp
             JOIN reach r ON r.ns_id = exp.exposing_namespace_id
         )
         SELECT ns_id FROM reach";
    let reached: Vec<i64> = {
        let mut stmt = conn
            .prepare(walk)
            .map_err(|e| DelightQLError::database_error("prepare reach walk", e.to_string()))?;
        let rows = stmt
            .query_map([root.id], |row| row.get::<_, i64>(0))
            .map_err(|e| DelightQLError::database_error("run reach walk", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| DelightQLError::database_error("decode reach walk", e.to_string()))?;
        rows
    };

    let mut namespaces = vec![root.clone()];
    for id in reached {
        if id == root.id || namespaces.iter().any(|namespace| namespace.id == id) {
            continue;
        }
        if let Some(facts) = catalog.values().find(|namespace| namespace.id == id) {
            namespaces.push(facts.clone());
        }
    }

    // Qualifier aliases: the session's at the prompt, the root's own local
    // aliases inside a declaration.
    let aliases: HashMap<String, ReachedNamespace> = {
        let sql = match world {
            World::Session => "SELECT alias, target_namespace_id FROM namespace_alias".to_string(),
            World::Declaration => format!(
                "SELECT alias, target_namespace_id FROM namespace_local_alias \
                 WHERE namespace_id = {}",
                root.id
            ),
        };
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| DelightQLError::database_error("prepare alias capture", e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|e| DelightQLError::database_error("run alias capture", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| DelightQLError::database_error("decode alias capture", e.to_string()))?;
        let mut aliases = HashMap::new();
        for (alias, target_id) in rows {
            if let Some(facts) = catalog.values().find(|namespace| namespace.id == target_id) {
                aliases.insert(alias, facts.clone());
            }
        }
        aliases
    };

    Ok(DeclarationReach {
        root,
        namespaces,
        aliases,
        catalog,
        closure,
    })
}
