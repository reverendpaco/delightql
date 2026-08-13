// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Narrow, native-only seams for effects whose state lives outside the query
//! pipeline.  The production implementation is deliberately boring; the
//! seam exists so the lifecycle coordinator can be tested with a deterministic
//! boundary without adding a fault switch to the public API.

use crate::error::{DelightQLError, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// The operation the liminal catalog boundary performs when it closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiminalClose {
    Commit,
    Rollback,
}

/// The concrete bootstrap savepoint boundary used by production code.
///
/// Keeping the SQL here makes the coordinator's ownership rule explicit: a
/// failed close is returned to the caller, which decides whether the journal
/// can be compensated and whether the session remains usable.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RealLiminalCatalogBoundary;

pub(crate) trait LiminalCatalogBoundary: Send + Sync {
    fn begin(&self, catalog: &Connection) -> Result<()>;
    fn close(&self, catalog: &Connection, close: LiminalClose) -> Result<()>;
}

/// State outside the bootstrap catalog that an active liminal program must
/// undo if the program aborts. The catalog rows themselves live under the
/// savepoint; these effects do not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExternalEffect {
    AttachedSqlite { schema_alias: String },
    RegisteredExternalConnection { connection_id: i64 },
    CreatedFile {
        path: PathBuf,
        prior_state: CreatedFilePriorState,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreatedFilePriorState {
    Absent,
    Empty,
}

/// The only filesystem inverses the liminal journal needs. Keeping this
/// smaller than a filesystem façade makes the fault script name the exact
/// operation that failed.
pub(crate) trait LiminalFileOps: Send + Sync {
    fn remove_created(&self, path: &Path) -> Result<()>;
    fn restore_empty(&self, path: &Path) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RealLiminalFileOps;

impl LiminalFileOps for RealLiminalFileOps {
    fn remove_created(&self, path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(DelightQLError::database_error(
                format!("Failed to remove liminally-created file '{}'", path.display()),
                error.to_string(),
            )),
        }
    }

    fn restore_empty(&self, path: &Path) -> Result<()> {
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map(|_| ())
            .map_err(|error| {
                DelightQLError::database_error(
                    format!("Failed to restore liminal file '{}'", path.display()),
                    error.to_string(),
                )
            })
    }
}

#[derive(Debug)]
pub(crate) struct CompensationFailure {
    pub(crate) effect: ExternalEffect,
    pub(crate) error: DelightQLError,
}

/// The result of reconciling one target-created object into the session
/// catalog. `NotPresent` is reserved for an independent existence probe; an
/// empty attribute list is not evidence that the object was absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegistrationOutcome {
    Registered,
    NotPresent,
    Unsupported { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ObjectExistence {
    Present,
    Absent,
    Unsupported { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedObjectReadback {
    pub(crate) existence: ObjectExistence,
    pub(crate) attributes: Vec<(String, String)>,
}

/// Catalog input prepared entirely from target read-backs. The complete batch
/// is handed to one reconciliation boundary so a later object cannot leave an
/// earlier sibling committed in the bootstrap catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatedObjectRegistration {
    pub(crate) name: String,
    pub(crate) is_view: bool,
    pub(crate) connection_id: i64,
    pub(crate) namespace_id: i64,
    pub(crate) attributes: Vec<(String, String)>,
}

pub(crate) trait CreatedObjectCatalog: Send + Sync {
    fn reconcile(
        &self,
        catalog: &Connection,
        registrations: &[CreatedObjectRegistration],
    ) -> Result<()>;
}

impl LiminalCatalogBoundary for RealLiminalCatalogBoundary {
    fn begin(&self, catalog: &Connection) -> Result<()> {
        catalog
            .execute_batch("SAVEPOINT dql_liminal_program")
            .map_err(|error| {
                DelightQLError::database_error(
                    "Failed to begin liminal program transaction",
                    error.to_string(),
                )
            })
    }

    fn close(&self, catalog: &Connection, close: LiminalClose) -> Result<()> {
        let sql = match close {
            LiminalClose::Commit => "RELEASE SAVEPOINT dql_liminal_program",
            LiminalClose::Rollback => {
                "ROLLBACK TO SAVEPOINT dql_liminal_program; \
                 RELEASE SAVEPOINT dql_liminal_program"
            }
        };
        catalog.execute_batch(sql).map_err(|error| {
            DelightQLError::database_error(
                "Failed to close liminal program transaction",
                error.to_string(),
            )
        })
    }
}

/// The session-level state used when an external effect may have escaped its
/// catalog transaction.  The pending-effect inventory is added by the
/// compensation seam; this first landing keeps the incident plain data so it
/// can cross the relay without making `DelightQLError` cloneable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionHealth {
    Healthy,
    Quarantined(HealthIncident),
}

impl Default for SessionHealth {
    fn default() -> Self {
        Self::Healthy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HealthIncident {
    pub(crate) operation: String,
    pub(crate) message: String,
    pub(crate) pending_effects: Vec<ExternalEffect>,
}
