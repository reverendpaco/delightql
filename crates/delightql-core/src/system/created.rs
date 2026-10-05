// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Registering the objects a run's DDL directives created: read-back,
//! placement under the connection's session shadow or a durable namespace,
//! one reconciliation for the whole batch, and retirement of a connection's
//! session pool.

use super::entity_rows;
use super::{CatalogSavepoint, DelightQLSystem, PRIMARY_CONNECTION_ID};
use crate::bootstrap::SourceType;
use crate::diagnostic::{Constraint, Internal, Runtime};
use crate::error::{DelightQLError, Result};
use crate::external_effects::{
    CreatedObjectCatalog, CreatedObjectReadback, ObjectExistence, RegistrationOutcome,
};
use rusqlite::{Connection, OptionalExtension};

/// WHERE A MATERIALIZED OBJECT IS PUBLISHED, as one closed fact: a session
/// object is owned nominally by its connection-root shadow
/// (`sys::shadow::<root>`) and overlays the exact durable namespace its
/// creation target selected; a durable object is owned by that data
/// namespace. Both owners are catalog identities the registration act reads
/// off the creation target — never recovered from a connection, a source
/// URI, a spelling, or a same-named competitor. Constructed only inside
/// this module, by that act.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Placement {
    SessionShadow {
        durable_owner: i64,
        shadow_owner: i64,
    },
    Durable {
        owner: i64,
    },
}

impl Placement {
    /// The namespace the object's catalog row is activated in.
    pub(crate) fn nominal_owner(&self) -> i64 {
        match self {
            Placement::SessionShadow { shadow_owner, .. } => *shadow_owner,
            Placement::Durable { owner } => *owner,
        }
    }
}

/// Catalog input prepared from the creation target and its read-back: the
/// object's shape, its placement, its physical connection, and its
/// read-back heading, as ONE fact. Private fields and a private
/// constructor: only the registration act — which takes all of it from the
/// plan's creation target — constructs it, and the complete batch is handed
/// to one reconciliation boundary so a later object cannot leave an earlier
/// sibling committed in the bootstrap catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedObjectRegistration {
    name: String,
    shape: crate::pipeline::compiled_query::Shape,
    placement: Placement,
    connection_id: i64,
    attributes: Vec<(String, String)>,
    /// Positions among `attributes` that carry nested relation payloads,
    /// as the creating plan knew them.
    interior_positions: Vec<usize>,
}

impl CreatedObjectRegistration {
    /// The registration act's own constructor.
    fn judged(
        name: String,
        shape: crate::pipeline::compiled_query::Shape,
        placement: Placement,
        connection_id: i64,
        attributes: Vec<(String, String)>,
        interior_positions: Vec<usize>,
    ) -> Self {
        CreatedObjectRegistration {
            name,
            shape,
            placement,
            connection_id,
            attributes,
            interior_positions,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn shape(&self) -> crate::pipeline::compiled_query::Shape {
        self.shape
    }

    pub(crate) fn placement(&self) -> &Placement {
        &self.placement
    }

    pub(crate) fn connection_id(&self) -> i64 {
        self.connection_id
    }

    pub(crate) fn attributes(&self) -> &[(String, String)] {
        &self.attributes
    }

    pub(crate) fn interior_positions(&self) -> &[usize] {
        &self.interior_positions
    }
}

/// A RETIRED CONNECTION TAKES ITS TEMP POOL WITH IT. Once no routing reaches
/// a connection, the session objects registered on it can never be read
/// again, so their entities retire, and its recorded shadow clears: a later
/// session on the same resource opens a new pool and records its own.
pub(super) fn retire_session_pool(catalog: &Connection, connection_id: i64) -> Result<()> {
    let entity_ids: Vec<i64> = {
        let mut statement = catalog
            .prepare(
                "SELECT so.entity_id FROM session_overlay so
                 JOIN entity e ON e.id = so.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.connection_id = ?1",
            )
            .map_err(|e| Runtime::catalog("query a retired connection's pool", e.to_string()))?;
        let rows = statement
            .query_map([connection_id], |row| row.get(0))
            .map_err(|e| Runtime::catalog("query a retired connection's pool", e.to_string()))?;
        rows.collect::<std::result::Result<Vec<i64>, _>>()
            .map_err(|e| Runtime::catalog("read a retired connection's pool", e.to_string()))?
    };
    for entity_id in entity_ids {
        entity_rows::retire_entity(catalog, entity_id)?;
    }
    catalog
        .execute(
            "UPDATE connection SET shadow_namespace_id = NULL WHERE id = ?1",
            [connection_id],
        )
        .map_err(|e| Runtime::catalog("clear a retired connection's shadow", e.to_string()))?;
    Ok(())
}

/// The production implementation of the created-object catalog seam. The
/// caller owns the surrounding savepoint; this method only performs catalog
/// writes and returns the first failure so the savepoint can roll back the
/// complete batch.
pub(crate) struct RealCreatedObjectCatalog;

impl CreatedObjectCatalog for RealCreatedObjectCatalog {
    fn reconcile(
        &self,
        catalog: &Connection,
        registrations: &[CreatedObjectRegistration],
    ) -> Result<()> {
        for registration in registrations {
            // THE OBJECT'S CARTRIDGE names the act that materialized it on
            // its connection: one per (connection, nominal owner). Nothing
            // reads residence back off it — the placement is the namespace
            // the row is activated in.
            let owner = registration.placement().nominal_owner();
            let source_uri = format!("materialized://{owner}");
            let cartridge_id: i64 = match catalog
                .query_row(
                    "SELECT id FROM cartridge
                     WHERE source_uri = ?1 AND connection_id = ?2",
                    rusqlite::params![source_uri, registration.connection_id()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    Runtime::catalog("query session-materialization cartridge", e.to_string())
                })? {
                Some(id) => id,
                None => {
                    catalog
                        .execute(
                            "INSERT INTO cartridge (language, source_type_enum, source_uri, \
                             source_ns, connected, connection_id, is_universal)
                             VALUES (?1, ?2, ?3, NULL, 1, ?4, 0)",
                            rusqlite::params![
                                3,
                                SourceType::Db.as_i32(),
                                source_uri,
                                registration.connection_id(),
                            ],
                        )
                        .map_err(|e| {
                            Runtime::catalog(
                                "Failed to create session-materialization cartridge",
                                e.to_string(),
                            )
                        })?;
                    catalog.last_insert_rowid()
                }
            };

            // A CREATION REPLACES the object the same act published under
            // this name before (a session shadow's temp creations replace;
            // a durable clash refused at compile). The retirement is scoped
            // to the nominal owner: a same-named durable entity under the
            // data namespace keeps its registration when a shadow lands.
            let stale_ids: Vec<i64> = {
                let mut statement = catalog
                    .prepare(
                        "SELECT e.id FROM entity e
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE ae.namespace_id = ?1 AND e.name = ?2 COLLATE NOCASE
                           AND e.cartridge_id = ?3",
                    )
                    .map_err(|e| Runtime::catalog("query stale created entity", e.to_string()))?;
                let rows = statement
                    .query_map(
                        rusqlite::params![owner, registration.name(), cartridge_id],
                        |row| row.get(0),
                    )
                    .map_err(|e| Runtime::catalog("query stale created entity", e.to_string()))?;
                rows.collect::<std::result::Result<Vec<i64>, _>>()
                    .map_err(|e| Runtime::catalog("read stale created entity", e.to_string()))?
            };
            for entity_id in stale_ids {
                entity_rows::retire_entity(catalog, entity_id)?;
            }

            // THE KIND IS THE SHAPE × RESIDENCE the act carried: a session
            // shadow is a temporary table or view, a durable object a
            // permanent one.
            let kind = match (registration.shape(), registration.placement()) {
                (
                    crate::pipeline::compiled_query::Shape::Table,
                    crate::external_effects::Placement::SessionShadow { .. },
                ) => crate::enums::EntityType::DbTemporaryTable,
                (
                    crate::pipeline::compiled_query::Shape::View,
                    crate::external_effects::Placement::SessionShadow { .. },
                ) => crate::enums::EntityType::DbTemporaryView,
                (
                    crate::pipeline::compiled_query::Shape::Table,
                    crate::external_effects::Placement::Durable { .. },
                ) => crate::enums::EntityType::DbPermanentTable,
                (
                    crate::pipeline::compiled_query::Shape::View,
                    crate::external_effects::Placement::Durable { .. },
                ) => crate::enums::EntityType::DbPermanentView,
            };
            catalog
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        registration.name(),
                        kind.as_i32(),
                        cartridge_id,
                        "Materialized by a DDL directive",
                    ],
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!(
                            "Failed to register created object '{}'",
                            registration.name()
                        ),
                        e.to_string(),
                    )
                })?;
            let entity_id = catalog.last_insert_rowid();
            // A SESSION OBJECT RECORDS THE NAMESPACE IT OVERLAYS: bare
            // selection reads it inside that namespace, whatever its
            // connection-root shadow path spells.
            if let crate::external_effects::Placement::SessionShadow { durable_owner, .. } =
                registration.placement()
            {
                catalog
                    .execute(
                        "INSERT INTO session_overlay (entity_id, durable_namespace_id) \
                         VALUES (?1, ?2)",
                        rusqlite::params![entity_id, durable_owner],
                    )
                    .map_err(|e| {
                        Runtime::catalog(
                            format!(
                                "Failed to record the overlay of created object '{}'",
                                registration.name()
                            ),
                            e.to_string(),
                        )
                    })?;
            }
            for (position, (column_name, column_type)) in
                registration.attributes().iter().enumerate()
            {
                catalog
                    .execute(
                        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, \
                         data_type, position, is_nullable, default_value)
                         VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                        rusqlite::params![
                            entity_id,
                            column_name,
                            column_type,
                            position as i64 + 1,
                        ],
                    )
                    .map_err(|e| {
                        Runtime::catalog(format!(
                                "Failed to register attribute '{}' for '{}'",
                                column_name, registration.name()
                            ), e.to_string())
                    })?;
            }
            // THE SHAPE THE PLAN KNEW: a nested-payload column is recorded
            // as an interior entity of the created object, which is how a
            // later read learns to embed it as a tree rather than a string.
            // A position past the read-back heading is a disagreement
            // between the plan and the engine, and is refused.
            for position in registration.interior_positions() {
                let Some((column_name, _)) = registration.attributes().get(*position) else {
                    return Err(DelightQLError::from(Runtime::General {
                        message: format!(
                            "created object '{}' has no column at nested-payload position {}",
                            registration.name(),
                            position
                        ),
                        details: "the plan's heading and the engine's read-back disagree"
                            .to_string(),
                    }));
                };
                catalog
                    .execute(
                        "INSERT INTO interior_entity (parent_entity_id, column_name) \
                         VALUES (?1, ?2)",
                        rusqlite::params![entity_id, column_name],
                    )
                    .map_err(|e| {
                        Runtime::catalog(
                            format!(
                                "Failed to register interior '{}' for '{}'",
                                column_name,
                                registration.name()
                            ),
                            e.to_string(),
                        )
                    })?;
            }
            // PUBLICATION IS ACTIVATION UNDER THE NOMINAL OWNER: the shadow
            // namespace for a session shadow, the data namespace for a
            // durable object.
            catalog
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) \
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, owner, cartridge_id],
                )
                .map_err(|e| {
                    Runtime::catalog(
                        format!(
                            "Failed to activate created object '{}'",
                            registration.name()
                        ),
                        e.to_string(),
                    )
                })?;
        }
        Ok(())
    }
}

impl DelightQLSystem {
    /// READ A CREATED OBJECT BACK where its creation target placed it:
    /// whether it exists — a creation the exit latch skipped does not — and
    /// its engine columns.
    fn readback_created_object(
        &self,
        target: &crate::creation_target::CreationTarget,
    ) -> Result<CreatedObjectReadback> {
        let name = target.name();
        let connection_id = target.connection_id();
        let dialect = self.dialect_for_connection(Some(connection_id));
        let Some(probe) = target.probe(dialect) else {
            return Ok(CreatedObjectReadback {
                existence: ObjectExistence::Unsupported {
                    reason: format!(
                        "created-object existence is not implemented for {}",
                        dialect.family_name()
                    ),
                },
                attributes: Vec::new(),
            });
        };
        let conn_arc = if connection_id == PRIMARY_CONNECTION_ID {
            self.connection.clone()
        } else {
            self.get_connection(connection_id)?
        };
        let guard = conn_arc.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire connection lock for created-object read-back",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let (_existence_columns, existence_rows) =
            guard.query_all_rows(&probe.existence, &[]).map_err(|e| {
                Runtime::catalog(
                    format!("Failed to probe created object '{name}' existence"),
                    e.to_string(),
                )
            })?;
        if existence_rows.is_empty() {
            return Ok(CreatedObjectReadback {
                existence: ObjectExistence::Absent,
                attributes: Vec::new(),
            });
        }
        if existence_rows.iter().any(|row| row.is_empty()) {
            return Err(DelightQLError::from(Constraint::General {
                message: format!(
                    "created-object existence probe for '{name}' returned a malformed row"
                ),
            }));
        }
        let (metadata_columns, metadata_rows) =
            guard.query_all_rows(&probe.readback, &[]).map_err(|e| {
                Runtime::catalog(
                    format!("Failed to read created object '{name}' metadata"),
                    e.to_string(),
                )
            })?;
        let required_columns = probe.name_column.max(probe.type_column) + 1;
        if metadata_columns.len() < required_columns {
            return Err(DelightQLError::from(Constraint::General {
    message: format!(
                    "created-object metadata for '{name}' has {} columns; expected at least {required_columns}",
                    metadata_columns.len()
                ),
}));
        }
        let attributes = metadata_rows
            .into_iter()
            .map(|row| {
                if row.len() < required_columns {
                    return Err(DelightQLError::from(Constraint::General {
                        message: format!("created-object metadata row for '{name}' is truncated"),
                    }));
                }
                Ok((
                    row[probe.name_column].as_wire_text().unwrap_or_default(),
                    row[probe.type_column].as_wire_text().unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(CreatedObjectReadback {
            existence: ObjectExistence::Present,
            attributes,
        })
    }

    /// THE SESSION SHADOW NAMESPACE a creation target names —
    /// `sys::shadow::<root>`, system territory keyed by the connection's
    /// owning data root — minted the first time a session object registers
    /// on that connection and standing for the session. Its path is the
    /// target's; nothing spells it from a durable namespace or a source URI.
    fn shadow_namespace_on(conn: &Connection, shadow_fq: &str) -> Result<i64> {
        let existing: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [shadow_fq],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Runtime::catalog("query shadow namespace", e.to_string()))?;
        if let Some(id) = existing {
            return Ok(id);
        }
        let sys_id: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| Runtime::catalog("query sys namespace", e.to_string()))?;
        let shadow_root: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys::shadow'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Runtime::catalog("query sys::shadow", e.to_string()))?;
        let shadow_root = match shadow_root {
            Some(id) => id,
            None => {
                conn.execute(
                    "INSERT INTO namespace (name, pid, fq_name, kind, provenance, writable)
                     VALUES ('shadow', ?1, 'sys::shadow', 'system', 'bootstrap', 0)",
                    [sys_id],
                )
                .map_err(|e| Runtime::catalog("mint sys::shadow", e.to_string()))?;
                conn.last_insert_rowid()
            }
        };
        let leaf = shadow_fq.rsplit("::").next().unwrap_or(shadow_fq);
        conn.execute(
            "INSERT INTO namespace (name, pid, fq_name, kind, provenance, writable)
             VALUES (?1, ?2, ?3, 'system', 'bootstrap', 0)",
            rusqlite::params![leaf, shadow_root, shadow_fq],
        )
        .map_err(|e| Runtime::catalog("mint session shadow namespace", e.to_string()))?;
        Ok(conn.last_insert_rowid())
    }

    /// FIX A CONNECTION'S SHADOW with its first session object: every later
    /// judgment reads the recorded one, whatever mounts join or leave the
    /// connection. A plan that placed an object under another shadow than
    /// the one recorded contradicts the judgment it was compiled from.
    fn record_connection_shadow(
        conn: &Connection,
        connection_id: i64,
        shadow_owner: i64,
        shadow_fq: &str,
    ) -> Result<()> {
        conn.execute(
            "UPDATE connection SET shadow_namespace_id = ?1
             WHERE id = ?2 AND shadow_namespace_id IS NULL",
            rusqlite::params![shadow_owner, connection_id],
        )
        .map_err(|e| Runtime::catalog("record the connection's session shadow", e.to_string()))?;
        let recorded: Option<i64> = conn
            .query_row(
                "SELECT shadow_namespace_id FROM connection WHERE id = ?1",
                [connection_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| Runtime::catalog("read the connection's session shadow", e.to_string()))?
            .flatten();
        if recorded != Some(shadow_owner) {
            return Err(Internal::invariant(
                "system::record_connection_shadow",
                format!(
                    "a session object was placed under {shadow_fq}, but connection \
                     {connection_id} records another session shadow"
                ),
            ));
        }
        Ok(())
    }

    /// Register an object a run's DDL directive created
    /// (`temp_table!`/`table!`/`temp_view!` — "catalog-registered … its
    /// name resolves like any table's") so
    /// post-run statements resolve it bare. Called by the run's entry
    /// point (relay/entry.rs) after a successful run; pinned by the
    /// effects ball's ddl_receipt--12/--13/--14 and util--36 post-state
    /// reads.
    ///
    /// The recipe is `imprint_namespace`'s registration tail: read the
    /// engine-typed columns back where the creation target placed the
    /// object (`CreationTarget::probe` — PRAGMA table_info on SQLite/DuckDB,
    /// information_schema on postgres: PRAGMA on a PG connection silently
    /// registers nothing), take the PLACEMENT from the same target — a
    /// session object publishes under its connection-root shadow
    /// `sys::shadow::<root>` with the namespace it overlays recorded, a
    /// durable object under its data namespace — retire the same act's own
    /// stale registration under that owner (fresh scratch per run), then
    /// write entity + output_column attributes + activation there.
    ///
    /// A same-name durable entity keeps its registration when a shadow
    /// lands: the two are distinct identities in distinct namespaces, and
    /// selection applies the overlay inside the recorded owner while exact
    /// routes reach either. Pinned by session_shadow_tests.
    ///
    /// Returns `NotPresent` when an independent existence probe proves the
    /// object does not exist (an exit-flagged run skipped its CREATE).
    /// A missing catalog namespace is an internal registration failure, not
    /// evidence that the target object was absent.
    pub(crate) fn register_run_created_objects_with<C: CreatedObjectCatalog>(
        &mut self,
        objects: &[crate::pipeline::compiled_query::PlanCreatedObject],
        catalog: &C,
    ) -> Result<Vec<RegistrationOutcome>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let mut outcomes = Vec::with_capacity(objects.len());
        let mut registrations = Vec::new();
        let mut unsupported_reason = None;
        for object in objects {
            let target = object.target();
            let readback = self.readback_created_object(target)?;
            match readback.existence {
                ObjectExistence::Absent => outcomes.push(RegistrationOutcome::NotPresent),
                ObjectExistence::Unsupported { reason } => {
                    unsupported_reason.get_or_insert(reason.clone());
                    outcomes.push(RegistrationOutcome::Unsupported { reason });
                }
                ObjectExistence::Present => {
                    let bootstrap = self.bootstrap_connection.lock().map_err(|e| {
                        Runtime::poisoned(
                            "Failed to acquire bootstrap lock for created-object namespace",
                            format!("Connection was poisoned: {e}"),
                        )
                    })?;
                    // THE DURABLE OWNER is the namespace the creation target
                    // selected, by the identity the target recorded. It must
                    // still be that namespace: the plan ran, so a namespace
                    // that vanished or changed name under it is a catalog
                    // that no longer describes the object.
                    let standing: Option<String> = bootstrap
                        .query_row(
                            "SELECT fq_name FROM namespace WHERE id = ?1",
                            [target.namespace_id()],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|e| {
                            Runtime::catalog("query created-object namespace", e.to_string())
                        })?;
                    if standing.as_deref() != Some(target.namespace()) {
                        return Err(DelightQLError::from(Runtime::General {
                            message: format!(
                                "created-object target namespace '{}' is no longer in the \
                                 catalog",
                                target.namespace()
                            ),
                            details: "created-object catalog namespace is unavailable".to_string(),
                        }));
                    }
                    let placement = match target.shadow_namespace() {
                        None => crate::external_effects::Placement::Durable {
                            owner: target.namespace_id(),
                        },
                        Some(shadow) => {
                            let shadow_owner = Self::shadow_namespace_on(&bootstrap, shadow)?;
                            Self::record_connection_shadow(
                                &bootstrap,
                                target.connection_id(),
                                shadow_owner,
                                shadow,
                            )?;
                            crate::external_effects::Placement::SessionShadow {
                                durable_owner: target.namespace_id(),
                                shadow_owner,
                            }
                        }
                    };
                    drop(bootstrap);
                    registrations.push(CreatedObjectRegistration::judged(
                        target.name().to_string(),
                        target.materialization().shape(),
                        placement,
                        target.connection_id(),
                        readback.attributes,
                        object.interior_positions().to_vec(),
                    ));
                    outcomes.push(RegistrationOutcome::Registered);
                }
            }
        }
        if let Some(reason) = unsupported_reason {
            for outcome in &mut outcomes {
                if matches!(outcome, RegistrationOutcome::Registered) {
                    *outcome = RegistrationOutcome::Unsupported {
                        reason: format!("created-object batch was not reconciled: {reason}"),
                    };
                }
            }
            return Ok(outcomes);
        }
        if registrations.is_empty() {
            return Ok(outcomes);
        }

        let bootstrap = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for created-object reconciliation",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let transaction = CatalogSavepoint::begin(
            &bootstrap,
            "dql_created_object_reconcile",
            "Failed to begin created-object reconciliation",
        )?;
        catalog.reconcile(&bootstrap, &registrations)?;
        transaction.commit("Failed to commit created-object reconciliation")?;
        drop(bootstrap);
        Ok(outcomes)
    }
}

#[cfg(test)]
mod created_object_catalog_tests {
    use super::{CreatedObjectCatalog, RealCreatedObjectCatalog};
    use crate::external_effects::CreatedObjectRegistration;
    use crate::system::{CatalogSavepoint, ReadySystem};
    use delightql_types::introspect::DatabaseIntrospector;
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(
            &self,
        ) -> delightql_types::Result<Vec<delightql_types::introspect::DiscoveredEntity>> {
            Ok(Vec::new())
        }

        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<delightql_types::introspect::DiscoveredEntity>> {
            Ok(Vec::new())
        }
    }

    fn fresh_system() -> ReadySystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    #[test]
    fn failed_later_registration_rolls_back_the_complete_batch() {
        let system = fresh_system();
        let bootstrap = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        // A fault trigger on a protected catalog table is engine-level
        // surgery; the scoped capability is the one road that admits it.
        let window = system.bootstrap_guard.migration_window();
        bootstrap
            .execute_batch(
                "CREATE TRIGGER fail_second_created_object
                 BEFORE INSERT ON entity
                 WHEN NEW.name = 'second'
                 BEGIN
                     SELECT RAISE(FAIL, 'second registration refused');
                 END;",
            )
            .expect("install failure trigger");
        window.close(&bootstrap).expect("re-seal");

        let registrations = vec![
            CreatedObjectRegistration::judged(
                "first".to_string(),
                crate::pipeline::compiled_query::Shape::Table,
                crate::external_effects::Placement::Durable { owner: 1 },
                2,
                vec![("id".to_string(), "INTEGER".to_string())],
                Vec::new(),
            ),
            CreatedObjectRegistration::judged(
                "second".to_string(),
                crate::pipeline::compiled_query::Shape::Table,
                crate::external_effects::Placement::Durable { owner: 1 },
                2,
                vec![("id".to_string(), "INTEGER".to_string())],
                Vec::new(),
            ),
        ];
        let savepoint = CatalogSavepoint::begin(
            &bootstrap,
            "dql_test_created_object_batch",
            "begin test savepoint",
        )
        .expect("begin test savepoint");
        let _window = system.catalog_window();
        let error = RealCreatedObjectCatalog
            .reconcile(&bootstrap, &registrations)
            .expect_err("the trigger must reject the second registration");
        drop(savepoint);

        assert!(error.to_string().contains("second registration refused"));
        let count: i64 = bootstrap
            .query_row(
                "SELECT COUNT(*) FROM entity e
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.source_uri LIKE 'materialized://%'",
                [],
                |row| row.get(0),
            )
            .expect("count session materialized entities");
        assert_eq!(count, 0, "the failed batch must leave no sibling behind");
    }

    /// The session placement of `main`'s connection pool: the shadow
    /// namespace, created here, overlaying `main`.
    fn session_placement(bootstrap: &rusqlite::Connection) -> crate::external_effects::Placement {
        let main: i64 = bootstrap
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .expect("main namespace");
        bootstrap
            .execute(
                "INSERT INTO namespace (name, fq_name, kind) VALUES ('main', 'test::shadow::main', 'system')",
                [],
            )
            .expect("shadow namespace");
        crate::external_effects::Placement::SessionShadow {
            durable_owner: main,
            shadow_owner: bootstrap.last_insert_rowid(),
        }
    }

    /// `name(dept, p)`, with `p` a nested payload when `nested`.
    fn created(
        name: &str,
        placement: &crate::external_effects::Placement,
        connection_id: i64,
        nested: bool,
    ) -> CreatedObjectRegistration {
        CreatedObjectRegistration::judged(
            name.to_string(),
            crate::pipeline::compiled_query::Shape::Table,
            placement.clone(),
            connection_id,
            vec![
                ("dept".to_string(), "TEXT".to_string()),
                ("p".to_string(), "TEXT".to_string()),
            ],
            if nested { vec![1] } else { Vec::new() },
        )
    }

    /// Reconcile one batch the way the registration act does: inside a
    /// catalog window and a savepoint that commits only on success.
    fn register(
        system: &ReadySystem,
        bootstrap: &rusqlite::Connection,
        batch: &[CreatedObjectRegistration],
    ) -> crate::error::Result<()> {
        let _window = system.catalog_window();
        let savepoint =
            CatalogSavepoint::begin(bootstrap, "dql_test_created_objects", "begin test batch")?;
        RealCreatedObjectCatalog.reconcile(bootstrap, batch)?;
        savepoint.commit("commit test batch")
    }

    /// Every entity named `name` with the interior columns and overlay rows
    /// that name it, in id order.
    fn catalog_rows(bootstrap: &rusqlite::Connection, name: &str) -> Vec<(i64, Vec<String>, i64)> {
        let mut statement = bootstrap
            .prepare(
                "SELECT e.id,
                        (SELECT group_concat(column_name) FROM interior_entity
                         WHERE parent_entity_id = e.id),
                        (SELECT COUNT(*) FROM session_overlay WHERE entity_id = e.id)
                 FROM entity e WHERE e.name = ?1 ORDER BY e.id",
            )
            .expect("prepare catalog rows");
        statement
            .query_map([name], |row| {
                let interiors: Option<String> = row.get(1)?;
                Ok((
                    row.get(0)?,
                    interiors
                        .map(|list| list.split(',').map(str::to_string).collect())
                        .unwrap_or_default(),
                    row.get(2)?,
                ))
            })
            .expect("query catalog rows")
            .collect::<rusqlite::Result<_>>()
            .expect("read catalog rows")
    }

    fn table_count(bootstrap: &rusqlite::Connection, table: &str) -> i64 {
        bootstrap
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count rows")
    }

    /// Replacement retires the whole prior entity, nested payload included,
    /// however often it happens and whichever shape replaces it.
    #[test]
    fn replacing_a_nested_object_retires_all_of_its_prior_entity() {
        let system = fresh_system();
        let bootstrap = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let placement = session_placement(&bootstrap);
        let interiors = table_count(&bootstrap, "interior_entity");
        let overlays = table_count(&bootstrap, "session_overlay");

        for (round, nested) in [true, true, true, false, true].into_iter().enumerate() {
            register(&system, &bootstrap, &[created("t", &placement, 2, nested)])
                .unwrap_or_else(|e| panic!("replacement {round} must succeed: {e}"));
            let rows = catalog_rows(&bootstrap, "t");
            assert_eq!(rows.len(), 1, "round {round}: one entity answers: {rows:?}");
            let (_, interior, overlay) = &rows[0];
            let expected: Vec<String> = if nested {
                vec!["p".to_string()]
            } else {
                Vec::new()
            };
            assert_eq!(
                interior, &expected,
                "round {round}: the current payload shape"
            );
            assert_eq!(*overlay, 1, "round {round}: the current overlay");
            assert_eq!(
                table_count(&bootstrap, "interior_entity"),
                interiors + expected.len() as i64,
                "round {round}: no prior interior survives"
            );
            assert_eq!(table_count(&bootstrap, "session_overlay"), overlays + 1);
        }
    }

    /// A batch refused after its replacement retired the prior entity rolls
    /// the retirement back with it: the prior object stands whole.
    #[test]
    fn a_refused_replacement_leaves_the_prior_nested_object_whole() {
        let system = fresh_system();
        let bootstrap = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let placement = session_placement(&bootstrap);
        register(&system, &bootstrap, &[created("t", &placement, 2, true)])
            .expect("first creation");
        let prior = catalog_rows(&bootstrap, "t");

        let window = system.bootstrap_guard.migration_window();
        bootstrap
            .execute_batch(
                "CREATE TRIGGER fail_second_created_object
                 BEFORE INSERT ON entity
                 WHEN NEW.name = 'second'
                 BEGIN
                     SELECT RAISE(FAIL, 'second registration refused');
                 END;",
            )
            .expect("install failure trigger");
        window.close(&bootstrap).expect("re-seal");

        let error = register(
            &system,
            &bootstrap,
            &[
                created("t", &placement, 2, true),
                created("second", &placement, 2, false),
            ],
        )
        .expect_err("the trigger must reject the second registration");
        assert!(error.to_string().contains("second registration refused"));
        assert_eq!(
            catalog_rows(&bootstrap, "t"),
            prior,
            "the prior entity stands whole"
        );
        assert!(catalog_rows(&bootstrap, "second").is_empty());

        // Entity ids are reused, so the replacement may take the retired
        // id; it must not inherit the retired entity's interior.
        register(&system, &bootstrap, &[created("t", &placement, 2, false)])
            .expect("the catalog takes a later replacement");
        let rows = catalog_rows(&bootstrap, "t");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.is_empty(), "{rows:?}");
        assert_eq!(rows[0].2, 1);
    }

    /// A retired connection's pool takes its nested session objects whole.
    #[test]
    fn a_retired_pool_takes_its_nested_objects_whole() {
        let system = fresh_system();
        let bootstrap = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let placement = session_placement(&bootstrap);
        bootstrap
            .execute(
                "INSERT INTO connection (id, resource_uri, connection_type)
                 VALUES (901, 'test://pool', (SELECT MIN(id) FROM connection_type_enum))",
                [],
            )
            .expect("pool connection");
        let interiors = table_count(&bootstrap, "interior_entity");
        let overlays = table_count(&bootstrap, "session_overlay");
        register(
            &system,
            &bootstrap,
            &[
                created("nested", &placement, 901, true),
                created("flat", &placement, 901, false),
            ],
        )
        .expect("two session objects on the pool");
        assert_eq!(table_count(&bootstrap, "interior_entity"), interiors + 1);

        {
            let _window = system.catalog_window();
            super::retire_session_pool(&bootstrap, 901).expect("the pool retires");
        }

        assert!(catalog_rows(&bootstrap, "nested").is_empty());
        assert!(catalog_rows(&bootstrap, "flat").is_empty());
        assert_eq!(table_count(&bootstrap, "interior_entity"), interiors);
        assert_eq!(table_count(&bootstrap, "session_overlay"), overlays);
    }
}
