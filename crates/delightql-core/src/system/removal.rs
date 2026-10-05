// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! One-namespace removal: a remover refuses while a grounding outside the
//! namespace's subtree borrows from it or while any namespace stands beneath
//! it, then destroys exactly the row it read. Also what leaves the catalog
//! when a namespace's contents are cleared.

use super::entity_rows;
use super::{BootstrapTxn, DelightQLSystem};
use crate::ddl::lifecycle::{admit_kind, Verb};
use crate::diagnostic::Runtime;
use crate::error::{DelightQLError, Result};
use log::debug;
use rusqlite::Connection;

/// A remover takes exactly the namespace it names: while any namespace
/// stands beneath it, whatever act created that namespace, it refuses and
/// names the nearest ones.
pub(super) fn refuse_while_children_remain(
    verb: &str,
    namespace: &str,
    removal: &crate::namespace::Subtree,
) -> Result<()> {
    let Some(nearest) = removal.nearest_descendants() else {
        return Ok(());
    };
    Err(DelightQLError::from(Runtime::General {
        message: format!(
            "Cannot {verb} '{namespace}' — {} stands beneath it. {verb}!() removes one \
             namespace; remove what stands beneath it first.",
            nearest
                .iter()
                .map(|child| format!("'{}'", child.fq()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        details: "Namespace has children".to_string(),
    }))
}

/// A remover may not leave a published definition outside its removal set
/// depending on the namespace it removes: a definition whose qualified
/// reference routes into it from the definition's own site, or a declared
/// lexical edge into it (a consulted file's own enlistment or alias, or an
/// exposure). An inline block's captured edges are the session's ambient
/// ones and are no dependency. The refusal names every blocking definition
/// and its namespace.
pub(super) fn refuse_while_dependents_remain(catalog: &Connection, verb: &str, namespace: &str) -> Result<()> {
    let removed = |fq: &str| fq == namespace;
    // A qualified reference reaches the namespace by the route its own
    // body's site gives the written qualifier: the new middle's one route
    // judgment, never a reading of the spelling here.
    let mut blockers: Vec<String> = crate::pipeline::middle::api::routed_into(catalog, namespace)?;
    {
        let mut statement = catalog
            .prepare(
                "SELECT o.fq_name, i.fq_name
                 FROM lexical_import li
                 JOIN namespace o ON o.id = li.namespace_id
                 JOIN namespace i ON i.id = li.imported_namespace_id
                 JOIN cartridge c ON c.id = li.cartridge_id
                 WHERE c.source_uri <> 'file://(inline)'
                 UNION ALL
                 SELECT o.fq_name, t.fq_name
                 FROM namespace_local_alias a
                 JOIN namespace o ON o.id = a.namespace_id
                 JOIN namespace t ON t.id = a.target_namespace_id
                 JOIN cartridge c ON c.id = a.cartridge_id
                 WHERE c.source_uri <> 'file://(inline)'
                 UNION ALL
                 SELECT o.fq_name, x.fq_name
                 FROM exposed_namespace e
                 JOIN namespace o ON o.id = e.exposing_namespace_id
                 JOIN namespace x ON x.id = e.exposed_namespace_id
                 ORDER BY 1",
            )
            .map_err(|e| Runtime::catalog("prepare a removal's lexical-edge census", e.to_string()))?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
            .map_err(|e| Runtime::catalog("read a removal's lexical-edge census", e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| Runtime::catalog("decode a removal's lexical-edge census", e.to_string()))?;
        for (owner, imported) in rows {
            let blocker = format!("{owner} (its declared edge)");
            if !removed(&owner) && removed(&imported) && !blockers.contains(&blocker) {
                blockers.push(blocker);
            }
        }
    }
    if blockers.is_empty() {
        return Ok(());
    }
    Err(DelightQLError::from(crate::diagnostic::Constraint::General {
        message: format!(
            "Cannot {verb} '{namespace}' — {} depend{} on it. A remover never leaves a published definition \
             depending on what it removes: replace or remove the dependent definitions first.",
            blockers.join(", "),
            if blockers.len() == 1 { "s" } else { "" }
        ),
    }))
}

/// How a borrow reaches a namespace a remover would take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BorrowedAs {
    /// The member is a grounding's data world.
    Data,
    /// The member is a grounding's source library.
    Library,
}

/// A grounding rooted outside a remover's subtree that borrows a namespace in it.
#[derive(Debug)]
pub(super) struct OutsideBorrow {
    borrowed_as: BorrowedAs,
    /// The member borrowed.
    pub(super) member: String,
    /// The borrowing derived world's root.
    pub(super) borrower: String,
}

/// The borrow that refuses a removal: a grounding rooted outside `scope` that
/// borrows a namespace in it, as its data world or as its source library.
/// Read from the whole `grounding` relation and judged only by the subtree's
/// membership. `first` picks which borrow a refusal names when both exist.
///
/// A remover judges its namespace's whole subtree here before refusing on a
/// child, so a borrowed descendant — the obstacle that must be cleared first
/// — is the one named.
pub(super) fn outside_borrow(
    catalog: &Connection,
    scope: &crate::namespace::Subtree,
    first: BorrowedAs,
) -> Result<Option<OutsideBorrow>> {
    let mut statement = catalog
        .prepare(
            "SELECT g.data_namespace_id, d.fq_name,
                    g.lib_namespace_id, l.fq_name,
                    g.root_namespace_id, r.fq_name
             FROM grounding g
             JOIN namespace d ON d.id = g.data_namespace_id
             JOIN namespace l ON l.id = g.lib_namespace_id
             JOIN namespace r ON r.id = g.root_namespace_id
             ORDER BY g.id",
        )
        .map_err(|e| Runtime::catalog("prepare a removal's borrows", e.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                (row.get::<_, i64>(0)?, row.get::<_, String>(1)?),
                (row.get::<_, i64>(2)?, row.get::<_, String>(3)?),
                (row.get::<_, i64>(4)?, row.get::<_, String>(5)?),
            ))
        })
        .map_err(|e| Runtime::catalog("read a removal's borrows", e.to_string()))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| Runtime::catalog("decode a removal's borrows", e.to_string()))?;

    let mut outside = Vec::new();
    for ((data_id, data_fq), (lib_id, lib_fq), (root_id, root_fq)) in rows {
        if scope.contains(root_id) {
            continue;
        }
        for (borrowed_as, id, fq) in [
            (BorrowedAs::Data, data_id, data_fq),
            (BorrowedAs::Library, lib_id, lib_fq),
        ] {
            if scope.contains(id) {
                outside.push(OutsideBorrow {
                    borrowed_as,
                    member: fq,
                    borrower: root_fq.clone(),
                });
            }
        }
    }
    let named = outside
        .iter()
        .position(|borrow| borrow.borrowed_as == first);
    Ok(match named {
        Some(index) => Some(outside.swap_remove(index)),
        None => outside.into_iter().next(),
    })
}

impl DelightQLSystem {
    /// Destroy a namespace and cascade-delete all its bootstrap metadata.
    ///
    /// Returns `(connection_id, source_ns)` from the cartridge so the caller
    /// can handle physical cleanup (DETACH, connection_map removal).
    ///
    /// Catalog-only destruction of one namespace, the row its remover's
    /// [`crate::namespace::Subtree`] read. Takes the bootstrap connection
    /// from the CALLER: unmount holds one guard — and one transaction
    /// window — across the destroy AND the physical DETACH, so a failed
    /// DETACH rolls the catalog back instead of losing the cleanup identity.
    pub(super) fn destroy_namespace(
        bootstrap_conn: &Connection,
        member: &crate::namespace::NamespaceNode,
    ) -> Result<(Option<i64>, Option<String>)> {
        let namespace_id = member.id();

        // Find ALL cartridge(s) and their connection info. The mount's own
        // cartridge comes from the STORED link —
        // authoritative even for a
        // valid-but-EMPTY image, so its alias always DETACHes. The entity
        // join still contributes any entity-bearing cartridges. The old
        // source-match fallback (and its double-empty ambiguity) is
        // REPEALED: identity is read, never inferred.
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                     FROM cartridge c
                     WHERE c.id = (SELECT cartridge_id FROM mount WHERE namespace_id = ?1)
                        OR c.id IN (SELECT e.cartridge_id FROM entity e
                                    JOIN activated_entity ae ON ae.entity_id = e.id
                                    WHERE ae.namespace_id = ?1)",
                )
                .map_err(|e| Runtime::catalog("Failed to query cartridges", e.to_string()))?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| Runtime::catalog("Failed to query cartridges", e.to_string()))?;
            rows.flatten().collect()
        };

        // Physical-cleanup identity comes from the LINK, read BEFORE it is
        // cleared: the union above deliberately includes
        // entity-bearing auxiliary cartridges as a DELETION set, so taking
        // `.first()` of it could hand physical cleanup an auxiliary's
        // (connection, alias) and leave the real mount attached. Unlinked
        // data namespaces fall back to the union's first row, as before.
        // Only QueryReturnedNoRows means "unlinked": any
        // other SQLite error must ABORT the destroy rather than silently
        // falling back to the arbitrary auxiliary path — the exact behavior
        // this read exists to eliminate.
        let link_identity = Self::mount_binding(bootstrap_conn, namespace_id)?
            .map(|binding| (Some(binding.connection_id), binding.detachable_alias()));

        // Atomic cascade: the link clear and every
        // delete below share one savepoint — a mid-cascade failure rolls the
        // whole destroy back, identity fact included, instead of leaving
        // partial metadata with the link already lost.
        let txn = BootstrapTxn::begin(&bootstrap_conn, "destroy_namespace")?;

        // FK choreography: clear the
        // link BEFORE the cascade deletes its cartridge row — under the
        // restrictive FK an accidental ordering mistake is loud, never a
        // silently orphaned namespace.
        Self::clear_mount_binding(&bootstrap_conn, namespace_id)?;
        let (connection_id, source_ns) = link_identity.unwrap_or_else(|| {
            cartridge_infos
                .first()
                .map(|(_, conn_id, src_ns)| (*conn_id, src_ns.clone()))
                .unwrap_or((None, None))
        });

        // Cascade delete — order matters for FK constraints
        // 1. Namespace linking tables
        bootstrap_conn.execute(
            "DELETE FROM namespace_local_alias WHERE namespace_id = ?1 OR target_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| Runtime::catalog("Failed to delete namespace_local_alias", e.to_string()))?;

        // The namespace row is going: drop every captured-import row that
        // names it on EITHER side — its own imports and any body that
        // imported it — so no foreign key blocks the delete.
        bootstrap_conn
            .execute(
                "DELETE FROM lexical_import
                 WHERE namespace_id = ?1 OR imported_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete lexical_import", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM enlisted_entity WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete enlisted_entity", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| Runtime::catalog("Failed to delete enlisted_namespace", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1 OR exposed_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| Runtime::catalog("Failed to delete exposed_namespace", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete namespace_alias", e.to_string()))?;

        // The liminal ledger dies with its namespace (EFFECT-ALGEBRA §8:
        // catalog state, session-scoped; pinned by
        // `liminal_ledger_dies_with_namespace`).
        bootstrap_conn
            .execute(
                "DELETE FROM liminal_receipt WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete liminal_receipt", e.to_string()))?;

        // 2. Grounding table
        bootstrap_conn
            .execute(
                "DELETE FROM grounding WHERE grounded_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete grounding", e.to_string()))?;

        // 3. Every load the namespace holds, retired whole
        if !cartridge_infos.is_empty() {
            for (cartridge_id, _, _) in &cartridge_infos {
                entity_rows::retire_load(bootstrap_conn, *cartridge_id)?;
            }
        } else {
            // No cartridge — still clean up activated_entity rows referencing this namespace
            bootstrap_conn
                .execute(
                    "DELETE FROM activated_entity WHERE namespace_id = ?1",
                    [namespace_id],
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to delete activated_entity", e.to_string())
                })?;
        }

        // 4. Delete namespace itself
        bootstrap_conn
            .execute("DELETE FROM namespace WHERE id = ?1", [namespace_id])
            .map_err(|e| Runtime::catalog("Failed to delete namespace", e.to_string()))?;

        txn.commit()?;

        Ok((connection_id, source_ns))
    }

    /// Unconsult a lib/grounded/scratch namespace or an imprint archive,
    /// removing all its definitions.
    ///
    /// Admits the namespace's kind through the lifecycle authority, then
    /// refuses while a grounding borrows from it or from beneath it, or while
    /// any namespace stands beneath it. Then deletes its bootstrap metadata.
    pub fn unconsult_namespace(&mut self, namespace: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // See unmount_database: pre-program deletions are uncompensable.
        self.refuse_preexisting_namespace_mutation_in_program(
            namespace,
            "unconsulting",
            |message| crate::diagnostic::Directive::UnconsultUncompensable { message }.into(),
        )?;
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for unconsult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. The namespace and every namespace under it: the scope the borrow
        //    and child judgments read.
        let tree = crate::namespace::Subtree::of(&bootstrap_conn, namespace)?.ok_or_else(|| {
            DelightQLError::from(Runtime::General {
                message: format!("Namespace '{}' not found", namespace),
                details: "Namespace not found".to_string(),
            })
        })?;

        admit_kind(Verb::Unconsult, namespace, tree.root().kind())?;

        // 2. Borrow check: a grounding rooted outside the subtree that
        //    borrows a namespace in it — a library as its source, a data namespace as
        //    its data world — refuses it (the borrower named is the derived
        //    world's ROOT).
        if let Some(borrow) = outside_borrow(&bootstrap_conn, &tree, BorrowedAs::Library)? {
            return Err(DelightQLError::from(Runtime::General {
                message: format!(
                    "Cannot unconsult '{}' — descendant '{}' is borrowed by grounded namespace '{}'. \
                     Unconsult the grounded namespace first.",
                    namespace, borrow.member, borrow.borrower
                ),
                details: "Namespace borrowed".to_string(),
            }));
        }
        // 3. One namespace: refuse while anything stands beneath it.
        refuse_while_children_remain("unconsult", namespace, &tree)?;
        refuse_while_dependents_remain(&bootstrap_conn, "unconsult", namespace)?;

        // 4. Remove the namespace. `destroy_namespace` holds its own
        // savepoint, so the tree commits or rolls back whole.
        Self::destroy_namespace(&bootstrap_conn, tree.root())?;

        debug!("unconsult_namespace: Unconsulted namespace '{}'", namespace);
        Ok(())
    }

    /// Clear all content tables for a namespace, preserving identity/shell.
    ///
    /// Deletes: cartridge(s) with their captured imports, entity(+all
    /// sub-tables), activated_entity, namespace_local_alias.
    /// Preserves: namespace row, enlisted_namespace, enlisted_entity,
    /// namespace_alias, grounding.
    ///
    /// Returns deleted cartridge metadata for physical cleanup by caller.
    pub(crate) fn clear_namespace_contents(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<Vec<(i64, Option<i64>, Option<String>)>> {
        // Collect ALL cartridge IDs for this namespace
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1",
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to query cartridges for clear", e.to_string())
                })?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    Runtime::catalog("Failed to query cartridges for clear", e.to_string())
                })?;
            rows.flatten().collect()
        };

        for (cartridge_id, _, _) in &cartridge_infos {
            entity_rows::retire_load(bootstrap_conn, *cartridge_id)?;
        }

        // Clean up namespace-local tables. The namespace's own captured
        // imports go so a reconsult re-captures the replacement's; rows
        // that name this namespace AS an import belong to other bodies and
        // are the removers' business, not a reconsult's.
        bootstrap_conn
            .execute(
                "DELETE FROM namespace_local_alias WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                Runtime::catalog("Failed to delete namespace_local_alias", e.to_string())
            })?;
        bootstrap_conn
            .execute(
                "DELETE FROM lexical_import WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to delete lexical_import", e.to_string()))?;

        // Safety: catch orphan activated_entity rows
        bootstrap_conn
            .execute(
                "DELETE FROM activated_entity WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                Runtime::catalog("Failed to delete activated_entity orphans", e.to_string())
            })?;

        // Reconsulting replaces the liminal ledger WHOLE (EFFECT-ALGEBRA §8:
        // the record describes THE load, not the history of loads; pinned by
        // `liminal_ledger_reconsult_replaces_whole`). The new file's receipts
        // are re-inserted by consult_file_inner in the same transaction.
        bootstrap_conn
            .execute(
                "DELETE FROM liminal_receipt WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| Runtime::catalog("Failed to clear liminal_receipt", e.to_string()))?;

        Ok(cartridge_infos)
    }
}
