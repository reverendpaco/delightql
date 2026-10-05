// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The catalog rows an entity owns: how they retire and how they copy.
//!
//! Every row that names an entity — its clauses, attributes, references and
//! their resolutions, higher-order parameters and their columns, join edges,
//! declared mode, interiors and their attributes, activations, session
//! overlay, enlistments and per-functor dialect rules — names it through a
//! foreign key the bootstrap schema declares `ON DELETE CASCADE`, and so does
//! every row that names one of those. Deleting the entity row is therefore
//! the entity's complete retirement, and it is the only retirement: a remover
//! that listed the rows it expected could list too few.
//!
//! A copy cannot be derived from the schema, because it re-points each row at
//! its new owner, so [`copy`] lists the rows it carries. A copied row points
//! only into its own entity: the store refuses a nested interior that reaches
//! into another entity, since retiring that entity would take the row with it.

use crate::diagnostic::{Internal, Runtime};
use crate::error::Result;
use rusqlite::Connection;

/// Retire one entity with every row that names it.
pub(crate) fn retire_entity(catalog: &Connection, entity_id: i64) -> Result<()> {
    catalog
        .execute("DELETE FROM entity WHERE id = ?1", [entity_id])
        .map_err(|e| Runtime::catalog("retire an entity", e.to_string()))?;
    Ok(())
}

/// Retire a load whole: every entity its cartridge registered, the imports
/// and aliases it captured, and the cartridge row. The captured rows go
/// before the cartridge their foreign keys name.
pub(crate) fn retire_load(catalog: &Connection, cartridge_id: i64) -> Result<()> {
    for (sql, label) in [
        (
            "DELETE FROM entity WHERE cartridge_id = ?1",
            "retire a load's entities",
        ),
        (
            "DELETE FROM lexical_import WHERE cartridge_id = ?1",
            "retire a load's captured imports",
        ),
        (
            "DELETE FROM namespace_local_alias WHERE cartridge_id = ?1",
            "retire a load's captured aliases",
        ),
        (
            "DELETE FROM cartridge WHERE id = ?1",
            "retire a load's cartridge",
        ),
    ] {
        catalog
            .execute(sql, [cartridge_id])
            .map_err(|e| Runtime::catalog(label, e.to_string()))?;
    }
    Ok(())
}

/// Copy the rows that describe `old_entity_id` onto `new_entity_id`: its
/// clauses, attributes, references, higher-order parameters with their
/// columns, join edges, declared mode, and interiors with their attributes.
/// Where the copy is published — its activation — is the caller's act.
pub(crate) fn copy(conn: &Connection, old_entity_id: i32, new_entity_id: i32) -> Result<()> {
    conn.execute(
        "INSERT INTO entity_clause (entity_id, ordinal, definition, location)
         SELECT ?1, ordinal, definition, location
         FROM entity_clause WHERE entity_id = ?2",
        rusqlite::params![new_entity_id, old_entity_id],
    )
    .map_err(|e| Runtime::catalog("Failed to copy entity_clause", e.to_string()))?;

    conn.execute(
        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
         SELECT ?1, attribute_name, attribute_type, data_type, position, is_nullable, default_value
         FROM entity_attribute WHERE entity_id = ?2",
        rusqlite::params![new_entity_id, old_entity_id],
    )
    .map_err(|e| Runtime::catalog("Failed to copy entity_attribute", e.to_string()))?;

    conn.execute(
        "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id, location)
         SELECT name, namespace, apparent_type, ?1, location
         FROM referenced_entity WHERE containing_entity_id = ?2",
        rusqlite::params![new_entity_id, old_entity_id],
    )
    .map_err(|e| Runtime::catalog("Failed to copy referenced_entity", e.to_string()))?;

    // ho_param + ho_param_column (FK chain: entity → ho_param → child)
    {
        let mut stmt = conn
            .prepare(
                "SELECT id, param_name, position, kind, column_name, stropped
                 FROM ho_param WHERE entity_id = ?1",
            )
            .map_err(|e| Runtime::catalog("Failed to query ho_param", e.to_string()))?;
        let old_params: Vec<(i32, String, i32, String, Option<String>, bool)> = stmt
            .query_map([old_entity_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .map_err(|e| Runtime::catalog("Failed to query ho_param", e.to_string()))?
            .collect::<rusqlite::Result<_>>()
            .map_err(|e| Runtime::catalog("Failed to read ho_param", e.to_string()))?;

        for (old_hp_id, param_name, position, kind, column_name, stropped) in &old_params {
            conn.execute(
                "INSERT INTO ho_param (entity_id, param_name, position, kind, column_name, stropped) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![new_entity_id, param_name, position, kind, column_name, stropped],
            )
            .map_err(|e| Runtime::catalog("Failed to copy ho_param", e.to_string()))?;
            let new_hp_id = conn.last_insert_rowid() as i32;

            conn.execute(
                "INSERT INTO ho_param_column (ho_param_id, column_name, column_position, stropped)
                 SELECT ?1, column_name, column_position, stropped
                 FROM ho_param_column WHERE ho_param_id = ?2",
                rusqlite::params![new_hp_id, old_hp_id],
            )
            .map_err(|e| Runtime::catalog("Failed to copy ho_param_column", e.to_string()))?;
        }
    }

    conn.execute(
        "INSERT INTO join_edge (entity_id, left_spelling, right_spelling, context_name, clause_ordinal)
         SELECT ?1, left_spelling, right_spelling, context_name, clause_ordinal
         FROM join_edge WHERE entity_id = ?2",
        rusqlite::params![new_entity_id, old_entity_id],
    )
    .map_err(|e| Runtime::catalog("Failed to copy join_edge", e.to_string()))?;

    // functional_dependency — the declared mode travels with the entity
    // it is a capability of, or the copy would be relation-only.
    conn.execute(
        "INSERT INTO functional_dependency (entity_id, role, position, attribute_name, stropped)
         SELECT ?1, role, position, attribute_name, stropped
         FROM functional_dependency WHERE entity_id = ?2",
        rusqlite::params![new_entity_id, old_entity_id],
    )
    .map_err(|e| Runtime::catalog("Failed to copy functional_dependency", e.to_string()))?;

    copy_interiors(conn, old_entity_id, new_entity_id)
}

/// Every interior first, then their attributes, so a nested interior's
/// pointer is rewritten to the copy's own interior for it.
fn copy_interiors(conn: &Connection, old_entity_id: i32, new_entity_id: i32) -> Result<()> {
    let old_interiors: Vec<(i32, String)> = {
        let mut stmt = conn
            .prepare(
                "SELECT id, column_name FROM interior_entity
                 WHERE parent_entity_id = ?1 ORDER BY id",
            )
            .map_err(|e| Runtime::catalog("Failed to query interior_entity", e.to_string()))?;
        let rows = stmt
            .query_map([old_entity_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| Runtime::catalog("Failed to query interior_entity", e.to_string()))?
            .collect::<rusqlite::Result<_>>()
            .map_err(|e| Runtime::catalog("Failed to read interior_entity", e.to_string()))?;
        rows
    };

    let mut copied: Vec<(i32, i32)> = Vec::with_capacity(old_interiors.len());
    for (old_interior, column_name) in &old_interiors {
        conn.execute(
            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
            rusqlite::params![new_entity_id, column_name],
        )
        .map_err(|e| Runtime::catalog("Failed to copy interior_entity", e.to_string()))?;
        copied.push((*old_interior, conn.last_insert_rowid() as i32));
    }

    for (old_interior, new_interior) in &copied {
        let attributes: Vec<(String, i32, Option<i32>)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT attribute_name, position, child_interior_entity_id
                     FROM interior_entity_attribute
                     WHERE interior_entity_id = ?1 ORDER BY id",
                )
                .map_err(|e| {
                    Runtime::catalog("Failed to query interior_entity_attribute", e.to_string())
                })?;
            let rows = stmt
                .query_map([old_interior], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    Runtime::catalog("Failed to query interior_entity_attribute", e.to_string())
                })?
                .collect::<rusqlite::Result<_>>()
                .map_err(|e| {
                    Runtime::catalog("Failed to read interior_entity_attribute", e.to_string())
                })?;
            rows
        };
        for (attribute_name, position, old_child) in attributes {
            let child = match old_child {
                None => None,
                Some(old_child) => Some(
                    copied
                        .iter()
                        .find(|(old, _)| *old == old_child)
                        .map(|(_, new)| *new)
                        .ok_or_else(|| {
                            Internal::invariant(
                                "system::entity_rows::copy",
                                format!(
                                    "entity {old_entity_id}'s interior {old_interior} nests \
                                     interior {old_child}, which is not among its own"
                                ),
                            )
                        })?,
                ),
            };
            conn.execute(
                "INSERT INTO interior_entity_attribute
                 (interior_entity_id, attribute_name, position, child_interior_entity_id)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![new_interior, attribute_name, position, child],
            )
            .map_err(|e| {
                Runtime::catalog("Failed to copy interior_entity_attribute", e.to_string())
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;
    use std::collections::{BTreeMap, BTreeSet};

    fn catalog() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::bootstrap::initialize_bootstrap_db(&conn).unwrap();
        conn
    }

    /// Every `(child table, column, parent table, on_delete)` foreign key the
    /// installed catalog declares.
    fn foreign_keys(conn: &Connection) -> Vec<(String, String, String, String)> {
        let mut statement = conn
            .prepare(
                "SELECT m.name, f.\"from\", f.\"table\", f.on_delete
                 FROM sqlite_master m, pragma_foreign_key_list(m.name) f
                 WHERE m.type = 'table'
                 ORDER BY m.name, f.id",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    /// `entity` and every table holding a foreign key into one of them.
    fn owned_tables(conn: &Connection) -> BTreeSet<String> {
        let keys = foreign_keys(conn);
        let mut owned = BTreeSet::from(["entity".to_string()]);
        loop {
            let before = owned.len();
            for (child, _, parent, _) in &keys {
                if owned.contains(parent) {
                    owned.insert(child.clone());
                }
            }
            if owned.len() == before {
                return owned;
            }
        }
    }

    fn row_counts(conn: &Connection, tables: &BTreeSet<String>) -> BTreeMap<String, i64> {
        tables
            .iter()
            .map(|table| {
                let count = conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                (table.clone(), count)
            })
            .collect()
    }

    /// A load on its own connection, cartridge and namespaces.
    fn seed_load(conn: &Connection) {
        conn.execute_batch(
            "INSERT INTO connection (id, resource_uri, connection_type)
                 VALUES (901, 'test://entity-rows', (SELECT MIN(id) FROM connection_type_enum));
             INSERT INTO cartridge (id, language, source_type_enum, source_uri, connected,
                                    connection_id, is_universal)
                 VALUES (901, (SELECT MIN(id) FROM language), (SELECT MIN(id) FROM source_type_enum),
                         'test://entity-rows', 1, 901, 0);
             INSERT INTO namespace (id, name, fq_name, kind) VALUES (901, 'owner', 'test::owner', 'data');
             INSERT INTO namespace (id, name, fq_name, kind) VALUES (902, 'other', 'test::other', 'data');",
        )
        .unwrap();
    }

    /// An entity with one or more rows in every table that names it,
    /// directly or through another row it owns. Returns its id.
    fn populated_entity(conn: &Connection, name: &str) -> i64 {
        conn.execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, 901)",
            [name],
        )
        .unwrap();
        let entity = conn.last_insert_rowid();
        conn.execute_batch(&format!(
            "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES ({entity}, 1, 'body');
             INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position)
                 VALUES ({entity}, 'p', 'output_column', 1);
             INSERT INTO referenced_entity (name, containing_entity_id) VALUES ('source', {entity});
             INSERT INTO entity_resolution (entity_id, referenced_entity_id)
                 VALUES ({entity}, last_insert_rowid());
             INSERT INTO ho_param (entity_id, param_name, position, kind) VALUES ({entity}, 'r', 0, 'glob');
             INSERT INTO ho_param_column (ho_param_id, column_name, column_position)
                 VALUES (last_insert_rowid(), 'c', 0);
             INSERT INTO join_edge (entity_id, left_spelling, right_spelling, context_name, clause_ordinal)
                 VALUES ({entity}, 'a', 'b', 'ctx', 1);
             INSERT INTO functional_dependency (entity_id, role, position, attribute_name)
                 VALUES ({entity}, 'input', 0, 'p');
             INSERT INTO interior_entity (parent_entity_id, column_name) VALUES ({entity}, 'sub');
             INSERT INTO interior_entity (parent_entity_id, column_name) VALUES ({entity}, 'p');
             INSERT INTO interior_entity_attribute (interior_entity_id, attribute_name, position,
                                                    child_interior_entity_id)
                 VALUES (last_insert_rowid(), 'sub', 0, last_insert_rowid() - 1);
             INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES ({entity}, 901, 901);
             INSERT INTO session_overlay (entity_id, durable_namespace_id) VALUES ({entity}, 902);
             INSERT INTO enlisted_entity (name, entity_id, from_namespace_id, to_namespace_id)
                 VALUES ('{name}', {entity}, 901, 902);
             INSERT INTO dialect_form_rule (form_type, dialect, entity_id, rule_kind, body)
                 VALUES (10, 'postgres', {entity}, 'template', '{{0}}');"
        ))
        .unwrap();
        entity
    }

    /// The store owns retirement: a reference into any row an entity owns
    /// must say what happens when the entity goes. A table added later with
    /// a plain foreign key would block the retirement or outlive it.
    #[test]
    fn every_reference_to_an_entity_owned_row_cascades() {
        let conn = catalog();
        let owned = owned_tables(&conn);
        // The walk reaches through owned rows, not only direct children.
        for transitive in [
            "ho_param_column",
            "interior_entity_attribute",
            "entity_resolution",
        ] {
            assert!(
                owned.contains(transitive),
                "{transitive} missing from {owned:?}"
            );
        }
        let blocking: Vec<_> = foreign_keys(&conn)
            .into_iter()
            .filter(|(_, _, parent, on_delete)| owned.contains(parent) && on_delete != "CASCADE")
            .collect();
        assert!(
            blocking.is_empty(),
            "references into entity-owned rows that do not retire with them: {blocking:?}"
        );
    }

    #[test]
    fn retiring_an_entity_takes_every_row_that_names_it() {
        let conn = catalog();
        seed_load(&conn);
        let owned = owned_tables(&conn);
        let kept = populated_entity(&conn, "kept");
        let before = row_counts(&conn, &owned);
        let retired = populated_entity(&conn, "retired");
        let populated = row_counts(&conn, &owned);
        let untouched: Vec<_> = owned
            .iter()
            .filter(|table| populated[*table] == before[*table])
            .collect();
        assert!(
            untouched.is_empty(),
            "the fixture must reach every owned table; it leaves {untouched:?} empty"
        );

        super::retire_entity(&conn, retired).unwrap();

        assert_eq!(
            row_counts(&conn, &owned),
            before,
            "exactly the retired entity's rows go"
        );
        let survivor: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity WHERE id = ?1", [kept], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(survivor, 1);
    }

    #[test]
    fn retiring_a_load_takes_its_entities_captures_and_cartridge() {
        let conn = catalog();
        seed_load(&conn);
        let owned = owned_tables(&conn);
        let before = row_counts(&conn, &owned);
        populated_entity(&conn, "first");
        populated_entity(&conn, "second");
        conn.execute_batch(
            "INSERT INTO lexical_import (namespace_id, imported_namespace_id, cartridge_id)
                 VALUES (901, 902, 901);",
        )
        .unwrap();

        super::retire_load(&conn, 901).unwrap();

        assert_eq!(row_counts(&conn, &owned), before);
        for table in ["cartridge", "lexical_import"] {
            let rows: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {table} WHERE {} = 901",
                        if table == "cartridge" {
                            "id"
                        } else {
                            "cartridge_id"
                        }
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(rows, 0, "{table}");
        }
    }

    /// A nested interior pointer across entities is what would let one
    /// entity's retirement take another's row; the store refuses it.
    #[test]
    fn a_nested_interior_never_reaches_into_another_entity() {
        let conn = catalog();
        seed_load(&conn);
        conn.execute_batch(
            "INSERT INTO entity (id, name, type, cartridge_id) VALUES (951, 'a', 10, 901);
             INSERT INTO entity (id, name, type, cartridge_id) VALUES (952, 'b', 10, 901);
             INSERT INTO interior_entity (id, parent_entity_id, column_name) VALUES (961, 951, 'p');
             INSERT INTO interior_entity (id, parent_entity_id, column_name) VALUES (962, 952, 'sub');
             INSERT INTO interior_entity (id, parent_entity_id, column_name) VALUES (963, 951, 'sub');",
        )
        .unwrap();
        let foreign = conn.execute(
            "INSERT INTO interior_entity_attribute
             (interior_entity_id, attribute_name, position, child_interior_entity_id)
             VALUES (961, 'sub', 0, 962)",
            [],
        );
        let message = foreign
            .expect_err("a pointer into entity b's interior")
            .to_string();
        assert!(
            message.contains("interior_child_shares_its_entity"),
            "{message}"
        );
        conn.execute(
            "INSERT INTO interior_entity_attribute
             (interior_entity_id, attribute_name, position, child_interior_entity_id)
             VALUES (961, 'sub', 0, 963)",
            [],
        )
        .expect("a pointer into the entity's own interior");
    }

    /// A copied nested interior points into the copy, so retiring the
    /// source leaves the copy whole.
    #[test]
    fn a_copy_owns_its_nested_interiors() {
        let conn = catalog();
        seed_load(&conn);
        let source = populated_entity(&conn, "source");
        conn.execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('copy', 10, 901)",
            [],
        )
        .unwrap();
        let copy = conn.last_insert_rowid();

        super::copy(&conn, source as i32, copy as i32).unwrap();

        let nested_owner = |entity: i64| -> Vec<(String, i64)> {
            let mut statement = conn
                .prepare(
                    "SELECT a.attribute_name, child.parent_entity_id
                     FROM interior_entity_attribute a
                     JOIN interior_entity holder ON holder.id = a.interior_entity_id
                     JOIN interior_entity child ON child.id = a.child_interior_entity_id
                     WHERE holder.parent_entity_id = ?1",
                )
                .unwrap();
            statement
                .query_map([entity], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        assert_eq!(nested_owner(copy), vec![("sub".to_string(), copy)]);

        let rows_of = |entity: i64| -> i64 {
            conn.query_row(
                "SELECT (SELECT COUNT(*) FROM interior_entity WHERE parent_entity_id = ?1)
                      + (SELECT COUNT(*) FROM interior_entity_attribute a
                         JOIN interior_entity ie ON ie.id = a.interior_entity_id
                         WHERE ie.parent_entity_id = ?1)
                      + (SELECT COUNT(*) FROM ho_param_column c
                         JOIN ho_param h ON h.id = c.ho_param_id WHERE h.entity_id = ?1)
                      + (SELECT COUNT(*) FROM entity_clause WHERE entity_id = ?1)
                      + (SELECT COUNT(*) FROM entity_attribute WHERE entity_id = ?1)
                      + (SELECT COUNT(*) FROM functional_dependency WHERE entity_id = ?1)
                      + (SELECT COUNT(*) FROM join_edge WHERE entity_id = ?1)
                      + (SELECT COUNT(*) FROM referenced_entity WHERE containing_entity_id = ?1)",
                [entity],
                |row| row.get(0),
            )
            .unwrap()
        };
        let copied = rows_of(copy);
        assert_eq!(copied, rows_of(source));

        super::retire_entity(&conn, source).unwrap();

        assert_eq!(
            rows_of(copy),
            copied,
            "the copy keeps every row it was given"
        );
        assert_eq!(nested_owner(copy), vec![("sub".to_string(), copy)]);
    }
}
