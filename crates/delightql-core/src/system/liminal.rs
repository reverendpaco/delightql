// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The outermost liminal program: its catalog savepoint, the journal of
//! effects outside the catalog, their compensation in reverse order, and
//! session health. A failed inverse quarantines the session until reset.

use super::DelightQLSystem;
use crate::diagnostic::Runtime;
use crate::error::{DelightQLError, Result};
use crate::external_effects::{
    CompensationFailure, CreatedFilePriorState, ExternalEffect, HealthIncident,
    LiminalCatalogBoundary, LiminalClose, LiminalFileOps, RealLiminalCatalogBoundary,
    RealLiminalFileOps, SessionHealth,
};
use rusqlite::OptionalExtension;
use std::path::PathBuf;

/// What kind of liminal program owns the current atomic boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiminalProgramKind {
    /// consult! / consult_tree! — a load;
    /// pre-program namespaces are strictly read-only for it.
    Consult,
    /// reconsult! — a reload; nested reloads of pre-existing CHILDREN are
    /// the tree-reload semantics and stay allowed (documented residue:
    /// they are reload-from-source, not compensable state).
    Reconsult,
}

/// State owned by the outermost liminal program. `namespace_mark` remains a
/// policy boundary (for operations deliberately forbidden against namespaces
/// that predate the program), not a substitute for transactional rollback.
#[derive(Debug)]
pub(super) struct ProgramContext {
    namespace_mark: i64,
    kind: LiminalProgramKind,
    external_effects: Vec<ExternalEffect>,
}

impl DelightQLSystem {
    /// Enter a liminal program. If no program is active, this call becomes
    /// the OUTERMOST one (owning the catalog savepoint and external journal)
    /// and
    /// `true` is returned — the caller must `end_liminal_program()` on
    /// every exit path. A nested call
    /// leaves the enclosing boundary in place and returns `false`.
    pub(crate) fn begin_liminal_program(
        &self,
        mark: i64,
        kind: LiminalProgramKind,
    ) -> Result<bool> {
        self.begin_liminal_program_with(&RealLiminalCatalogBoundary, mark, kind)
    }

    fn begin_liminal_program_with<B: LiminalCatalogBoundary>(
        &self,
        boundary: &B,
        mark: i64,
        kind: LiminalProgramKind,
    ) -> Result<bool> {
        if self.active_liminal_program.borrow().is_none() {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                Runtime::poisoned(
                    "Failed to acquire bootstrap lock for liminal program",
                    format!("Connection was poisoned: {e}"),
                )
            })?;
            boundary.begin(&conn)?;
            drop(conn);
            self.active_liminal_program.replace(Some(ProgramContext {
                namespace_mark: mark,
                kind,
                external_effects: Vec::new(),
            }));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Close the OUTERMOST liminal program (see `begin_liminal_program`).
    /// The catalog is one savepoint spanning directive execution through
    /// registration; failure restores pre-existing children as well as rows
    /// created by the program.
    pub(crate) fn end_liminal_program(&self, commit: bool) -> Result<()> {
        self.end_liminal_program_with(&RealLiminalCatalogBoundary, commit)
    }

    fn end_liminal_program_with<B: LiminalCatalogBoundary>(
        &self,
        boundary: &B,
        commit: bool,
    ) -> Result<()> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock to close liminal program",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let result = boundary.close(
            &conn,
            if commit {
                LiminalClose::Commit
            } else {
                LiminalClose::Rollback
            },
        );
        if result.is_ok() && !commit {
            // Rust-side caches of catalog rows must not outlive a rollback:
            // if this program CREATED the catalog
            // cartridge (a session that touched no catalog feature before
            // consulting), the memoized id now points at erased rows —
            // re-verify and forget it so the next use re-initializes.
            if let Some(id) = self.catalog_cartridge_id.get() {
                let still_there: bool = conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM cartridge WHERE id = ?1)",
                        [id],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);
                if !still_there {
                    self.catalog_cartridge_id.set(None);
                }
            }
        }
        drop(conn);
        // A failed close leaves the context, and therefore its compensation
        // journal, owned by the coordinator. Clearing it here would make a
        // failed RELEASE impossible to recover from.
        if result.is_ok() {
            self.active_liminal_program.replace(None);
        }
        result
    }

    /// Record a file materialized by `mount_new!` while a liminal program is
    /// active. The prior state is deliberately explicit: on abort an absent
    /// path is removed, while a caller-owned zero-byte placeholder is restored
    /// to zero bytes rather than deleted.
    pub(super) fn journal_created_file(&self, path: PathBuf, prior_state: CreatedFilePriorState) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::CreatedFile { path, prior_state });
        }
    }

    pub(super) fn journal_attached_sqlite(&self, schema_alias: String) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::AttachedSqlite { schema_alias });
        }
    }

    fn unjournal_attached_sqlite(&self, schema_alias: &str) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            if let Some(index) = context.external_effects.iter().rposition(|effect| {
                matches!(
                    effect,
                    ExternalEffect::AttachedSqlite { schema_alias: alias }
                        if alias == schema_alias
                )
            }) {
                context.external_effects.remove(index);
            }
        }
    }

    pub(super) fn mount_error_after_alias_rollback(
        &mut self,
        schema_alias: &str,
        cleanup: Result<()>,
        primary: DelightQLError,
    ) -> DelightQLError {
        self.unjournal_attached_sqlite(schema_alias);
        match cleanup {
            Ok(()) => primary,
            Err(cleanup_error) => {
                let primary_uri = primary.error_uri();
                let cleanup_uri = cleanup_error.error_uri();
                let message = format!(
                    "{primary}; mount cleanup failed: {cleanup_error} [{cleanup_uri}] [{primary_uri}]"
                );
                self.quarantine_session_with_pending(
                    "mount alias compensation",
                    message.clone(),
                    vec![ExternalEffect::AttachedSqlite {
                        schema_alias: schema_alias.to_string(),
                    }],
                );
                DelightQLError::from(crate::diagnostic::SessionHealth::ExternalEffect {
                    message: message.to_string(),
                })
            }
        }
    }

    pub(super) fn journal_external_connection(&self, connection_id: i64) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::RegisteredExternalConnection { connection_id });
        }
    }

    /// Reverse non-catalog effects in LIFO order. Every failed inverse is
    /// returned and transferred to session health; a cleanup problem cannot
    /// hide the program's original failure or disappear into `let _ =`.
    pub(crate) fn rollback_liminal_external_effects(&mut self) -> Vec<CompensationFailure> {
        self.compensate_liminal_external_effects_with(&RealLiminalFileOps)
    }

    fn compensate_liminal_external_effects_with<F: LiminalFileOps>(
        &mut self,
        file_ops: &F,
    ) -> Vec<CompensationFailure> {
        let effects = self
            .active_liminal_program
            .borrow_mut()
            .as_mut()
            .map(|context| std::mem::take(&mut context.external_effects))
            .unwrap_or_default();
        let failures = self.reverse_external_effects_with(effects, file_ops);
        if !failures.is_empty() {
            let pending_effects = failures
                .iter()
                .rev()
                .map(|failure| failure.effect.clone())
                .collect();
            let message = failures
                .iter()
                .map(|failure| format!("{} [{}]", failure.error, failure.error.error_uri()))
                .collect::<Vec<_>>()
                .join("; ");
            self.quarantine_session_with_pending(
                "liminal external-effect compensation",
                message,
                pending_effects,
            );
        }
        failures
    }

    /// Apply the inverse for each journal entry in LIFO order. This helper is
    /// shared by the ordinary rollback path and Reset's retry of a quarantined
    /// incident; it reports failures without deciding how session health is
    /// recorded.
    fn reverse_external_effects_with<F: LiminalFileOps>(
        &mut self,
        effects: Vec<ExternalEffect>,
        file_ops: &F,
    ) -> Vec<CompensationFailure> {
        let mut failures = Vec::new();
        for effect in effects.into_iter().rev() {
            let result = match &effect {
                ExternalEffect::AttachedSqlite { schema_alias } => {
                    let result = match self.connection.lock() {
                        Ok(conn) => {
                            let escaped = schema_alias.replace('\'', "''");
                            conn.execute(&format!("DETACH DATABASE '{escaped}'"), &[])
                                .map(|_| ())
                                .map_err(|error| {
                                    DelightQLError::from(Runtime::General {
                                        message: format!(
                                            "Failed to detach liminal alias '{}'",
                                            schema_alias
                                        ),
                                        details: error.to_string(),
                                    })
                                })
                        }
                        Err(error) => Err(Runtime::poisoned(
                            "Failed to acquire connection lock for liminal detach",
                            format!("Connection was poisoned: {error}"),
                        )),
                    };
                    result
                }
                ExternalEffect::RegisteredExternalConnection { connection_id } => {
                    self.connection_map.remove(&connection_id);
                    self.schema_map.remove(&connection_id);
                    self.introspector_map.remove(&connection_id);
                    Ok(())
                }
                ExternalEffect::CreatedFile { path, prior_state } => match prior_state {
                    CreatedFilePriorState::Absent => file_ops.remove_created(path),
                    CreatedFilePriorState::Empty => file_ops.restore_empty(path),
                },
            };
            if let Err(error) = result {
                failures.push(CompensationFailure { effect, error });
            }
        }
        failures
    }

    /// Retry the inverses recorded on a quarantined incident. Reset is the
    /// only recovery boundary: a failed inverse remains pending and prevents
    /// the health latch from clearing.
    pub(super) fn recover_pending_external_effects(&mut self) -> Result<()> {
        let effects = match &mut self.session_health {
            SessionHealth::Healthy => return Ok(()),
            SessionHealth::Quarantined(incident) => std::mem::take(&mut incident.pending_effects),
        };
        if effects.is_empty() {
            return Ok(());
        }

        let failures = self.reverse_external_effects_with(effects, &RealLiminalFileOps);
        if failures.is_empty() {
            return Ok(());
        }

        let pending_effects = failures
            .iter()
            .rev()
            .map(|failure| failure.effect.clone())
            .collect::<Vec<_>>();
        let message = failures
            .iter()
            .map(|failure| format!("{} [{}]", failure.error, failure.error.error_uri()))
            .collect::<Vec<_>>()
            .join("; ");
        if let SessionHealth::Quarantined(incident) = &mut self.session_health {
            incident.pending_effects = pending_effects;
            incident.message =
                format!("{}; reset compensation failed: {message}", incident.message);
        }
        Err(DelightQLError::from(
            crate::diagnostic::SessionHealth::ExternalEffect {
                message: format!(
                    "reset could not complete pending external-effect compensation: {message}"
                ),
            },
        ))
    }

    /// The active liminal program's (mark, kind), if any.
    pub(crate) fn active_liminal_program(&self) -> Option<(i64, LiminalProgramKind)> {
        self.active_liminal_program
            .borrow()
            .as_ref()
            .map(|context| (context.namespace_mark, context.kind))
    }

    /// Refuse new work once recovery has become uncertain. Close and reset
    /// remain legal protocol operations; the relay owns that distinction.
    pub(crate) fn require_healthy(&self) -> Result<()> {
        match &self.session_health {
            SessionHealth::Healthy => Ok(()),
            SessionHealth::Quarantined(incident) => Err(
                DelightQLError::from(crate::diagnostic::SessionHealth::ExternalEffect {
    message: format!(
                        "the session is quarantined after {}: {} — reset or reconnect before issuing another query",
                        incident.operation, incident.message
                    ),
}),
            ),
        }
    }

    /// Record the first uncertain recovery incident. Keeping the original
    /// incident avoids replacing a useful primary failure with a later one.
    pub(crate) fn quarantine_session(
        &mut self,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.quarantine_session_with_pending(operation, message, Vec::new());
    }

    pub(crate) fn quarantine_session_with_pending(
        &mut self,
        operation: impl Into<String>,
        message: impl Into<String>,
        pending_effects: Vec<ExternalEffect>,
    ) {
        if matches!(self.session_health, SessionHealth::Healthy) {
            self.session_health = SessionHealth::Quarantined(HealthIncident {
                operation: operation.into(),
                message: message.into(),
                pending_effects,
            });
        }
    }

    /// The quarantine incident, if one is latched: (operation, message).
    /// The typed answer behind `api::DqlHandle::session_health` — hosts read
    /// this, never error text.
    pub(crate) fn health_incident(&self) -> Option<(&str, &str)> {
        match &self.session_health {
            SessionHealth::Healthy => None,
            SessionHealth::Quarantined(incident) => {
                Some((incident.operation.as_str(), incident.message.as_str()))
            }
        }
    }

    /// The namespace row id for an fq name, if it exists.
    pub(crate) fn namespace_id(&self, fq: &str) -> Result<Option<i64>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for namespace id",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        conn.query_row("SELECT id FROM namespace WHERE fq_name = ?1", [fq], |r| {
            r.get(0)
        })
        .optional()
        .map_err(|e| Runtime::catalog("namespace id", e.to_string()))
    }

    /// Policy refusal for session-rearranging operations against namespaces
    /// that predate a consulted program. The catalog savepoint makes such an
    /// operation technically reversible; the refusal stands because a file
    /// describes its library rather than imperatively rearranging its caller's
    /// session. It lives on the shared road so every invocation route receives
    /// the policy inductively. The badge spells `uncompensable`; the reason it
    /// publishes is that policy, not an inability to undo.
    pub(crate) fn refuse_preexisting_namespace_mutation_in_program(
        &self,
        target_fq: &str,
        verb: &str,
        refuse: fn(String) -> DelightQLError,
    ) -> Result<()> {
        let Some((mark, _kind)) = self.active_liminal_program() else {
            return Ok(());
        };
        if let Some(id) = self.namespace_id(target_fq)? {
            if id <= mark {
                return Err(refuse(format!(
                    "a consulted file executes as ONE atomic program: if any part \
                     fails, everything the program created is torn down. \
                     '{target_fq}' existed BEFORE this program began, so {verb} it \
                     here could not be undone by that teardown — the program \
                     refuses rather than risk leaving the session half-changed. \
                     Do it at the prompt, outside the file"
                )));
            }
        }
        Ok(())
    }

    pub fn max_namespace_id(&self) -> Result<i64> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for namespace snapshot",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        Ok(conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM namespace", [], |r| {
                r.get(0)
            })
            .unwrap_or(0))
    }

    /// Does a namespace row exist for this fq name?
    pub fn namespace_exists(&self, fq: &str) -> Result<bool> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for namespace existence",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        Ok(conn
            .query_row("SELECT 1 FROM namespace WHERE fq_name = ?1", [fq], |_| {
                Ok(())
            })
            .optional()
            .map_err(|e| Runtime::catalog("namespace existence", e.to_string()))?
            .is_some())
    }
}

#[cfg(test)]
mod liminal_boundary_tests {
    use super::LiminalProgramKind;
    use crate::external_effects::{
        CreatedFilePriorState, ExternalEffect, LiminalCatalogBoundary, LiminalClose, LiminalFileOps,
    };
    use crate::system::ReadySystem;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use rusqlite::Connection;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> ReadySystem {
        let connection = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(connection, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh system should build")
    }

    /// A durable object on an engine whose unspelled schema is session
    /// state is refused when the target is judged, before any DDL exists.
    #[test]
    fn an_unspelled_postgres_durable_target_is_refused_before_ddl() {
        let mut system = fresh_system();
        system.db_type = "postgres".to_string();
        let data = {
            let conn = system.bootstrap_connection.lock().unwrap();
            crate::creation_target::DataTarget::read(
                &*conn,
                crate::definition_catalog::NamespaceKey::Fq("main"),
                "table!(staged)",
            )
            .expect("the unmounted main is backed by the primary")
        };
        let dialect = system.dialect_for_connection(Some(data.connection_id()));
        let error = crate::creation_target::CreationTarget::judge(
            data,
            "staged",
            crate::pipeline::compiled_query::Materialization::of_directive(
                crate::pipeline::asts::effects::DirectiveKind::Table,
            )
            .unwrap(),
            "table",
            dialect,
        )
        .expect_err("postgres with no spelled schema cannot place a durable object");
        assert!(
            error
                .error_uri()
                .contains("semantic/effect/ddl/durable_schema_unknown"),
            "{}",
            error.error_uri()
        );
    }

    struct ScriptedBoundary {
        fail_commit: bool,
        fail_rollback: bool,
        calls: Mutex<Vec<LiminalClose>>,
    }

    impl ScriptedBoundary {
        fn new(fail_commit: bool, fail_rollback: bool) -> Self {
            Self {
                fail_commit,
                fail_rollback,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl LiminalCatalogBoundary for ScriptedBoundary {
        fn begin(&self, _catalog: &Connection) -> Result<()> {
            Ok(())
        }

        fn close(&self, _catalog: &Connection, close: LiminalClose) -> Result<()> {
            self.calls.lock().unwrap().push(close);
            let failed = match close {
                LiminalClose::Commit => self.fail_commit,
                LiminalClose::Rollback => self.fail_rollback,
            };
            if failed {
                Err(crate::diagnostic::DelightQLError::from(
                    crate::diagnostic::Runtime::General {
                        message: "scripted liminal close failure".to_string(),
                        details: format!("{close:?}"),
                    },
                ))
            } else {
                Ok(())
            }
        }
    }

    struct ScriptedFileOps {
        fail_remove: bool,
        fail_restore: bool,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedFileOps {
        fn new(fail_remove: bool, fail_restore: bool) -> Self {
            Self {
                fail_remove,
                fail_restore,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl LiminalFileOps for ScriptedFileOps {
        fn remove_created(&self, path: &Path) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("remove:{}", path.display()));
            if self.fail_remove {
                Err(crate::diagnostic::DelightQLError::from(
                    crate::diagnostic::Runtime::General {
                        message: "scripted remove failure".to_string(),
                        details: path.display().to_string(),
                    },
                ))
            } else {
                Ok(())
            }
        }

        fn restore_empty(&self, path: &Path) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("restore:{}", path.display()));
            if self.fail_restore {
                Err(crate::diagnostic::DelightQLError::from(
                    crate::diagnostic::Runtime::General {
                        message: "scripted restore failure".to_string(),
                        details: path.display().to_string(),
                    },
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn failed_commit_close_keeps_the_program_context_until_rollback() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        let boundary = ScriptedBoundary::new(true, false);

        assert!(system
            .begin_liminal_program_with(&boundary, mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        assert!(system.active_liminal_program().is_some());

        let error = system
            .end_liminal_program_with(&boundary, true)
            .expect_err("scripted commit close must fail");
        assert!(error.to_string().contains("scripted liminal close failure"));
        assert!(
            system.active_liminal_program().is_some(),
            "a failed RELEASE must not discard the compensation journal"
        );

        system.rollback_liminal_external_effects();
        system
            .end_liminal_program_with(&boundary, false)
            .expect("rollback close should recover the boundary");
        assert!(system.active_liminal_program().is_none());
        assert_eq!(
            *boundary.calls.lock().unwrap(),
            vec![LiminalClose::Commit, LiminalClose::Rollback]
        );
    }

    #[test]
    fn quarantine_is_a_sticky_new_query_refusal_until_reset() {
        let mut system = fresh_system();
        assert!(system.require_healthy().is_ok());

        system.quarantine_session("test operation", "uncertain cleanup");
        let error = system
            .require_healthy()
            .expect_err("quarantine must refuse new work");
        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        assert!(system.health_incident().is_some());

        system
            .reinit_bootstrap()
            .expect("a successful reset clears the quarantine");
        assert!(system.require_healthy().is_ok());
        assert!(system.health_incident().is_none());
    }

    #[test]
    fn failed_file_inverse_is_returned_and_remains_pending() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::path::PathBuf::from("/tmp/dql-scripted-created.db");
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        let failures = system.compensate_liminal_external_effects_with(&file_ops);
        assert_eq!(failures.len(), 1);
        assert!(matches!(
            &failures[0].effect,
            ExternalEffect::CreatedFile { path: actual, prior_state: CreatedFilePriorState::Absent }
                if actual == &path
        ));
        assert!(failures[0]
            .error
            .to_string()
            .contains("scripted remove failure"));
        assert_eq!(
            file_ops.calls.lock().unwrap().as_slice(),
            &[format!("remove:{}", path.display())]
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
            }
            other => panic!("failed inverse must quarantine the session: {other:?}"),
        }
    }

    #[test]
    fn failed_mount_inverse_uses_health_identity_and_retains_both_uris() {
        let mut system = fresh_system();
        let primary =
            crate::diagnostic::DelightQLError::from(crate::diagnostic::Mount::Registration {
                message: "mount registration failed".to_string(),
            });
        let cleanup = crate::diagnostic::DelightQLError::from(crate::diagnostic::Mount::Detach {
            message: "detach failed".to_string(),
        });
        let primary_uri = primary.error_uri();
        let cleanup_uri = cleanup.error_uri();

        let error = system.mount_error_after_alias_rollback("_imported_7", Err(cleanup), primary);

        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        let message = error.to_string();
        assert!(
            message.contains(&primary_uri),
            "primary URI omitted: {message}"
        );
        assert!(
            message.contains(&cleanup_uri),
            "cleanup URI omitted: {message}"
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
                assert!(incident.message.contains(&primary_uri));
                assert!(incident.message.contains(&cleanup_uri));
            }
            other => panic!("failed mount inverse must quarantine: {other:?}"),
        }
    }

    #[test]
    fn absent_file_inverse_treats_missing_path_as_success() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path =
            std::env::temp_dir().join(format!("dql-file-inverse-absent-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);

        assert!(system.rollback_liminal_external_effects().is_empty());
        assert!(system.health_incident().is_none());
        assert!(!path.exists());
    }

    #[test]
    fn empty_file_inverse_refuses_to_recreate_a_missing_path() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path =
            std::env::temp_dir().join(format!("dql-file-inverse-empty-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        system.journal_created_file(path.clone(), CreatedFilePriorState::Empty);

        let failures = system.rollback_liminal_external_effects();
        assert_eq!(failures.len(), 1);
        assert!(system.health_incident().is_some());
        assert!(
            !path.exists(),
            "a failed inverse must not recreate the path"
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
            }
            other => panic!("missing prior-empty file must quarantine: {other:?}"),
        }
    }

    #[test]
    fn pending_inverses_keep_journal_order_for_the_next_reset() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let first = std::path::PathBuf::from("/tmp/dql-first-created.db");
        let second = std::path::PathBuf::from("/tmp/dql-second-created.db");
        system.journal_created_file(first.clone(), CreatedFilePriorState::Absent);
        system.journal_created_file(second.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        let failures = system.compensate_liminal_external_effects_with(&file_ops);
        assert_eq!(failures.len(), 2);
        assert_eq!(
            file_ops.calls.lock().unwrap().as_slice(),
            &[
                format!("remove:{}", second.display()),
                format!("remove:{}", first.display()),
            ]
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(
                    incident.pending_effects,
                    vec![
                        ExternalEffect::CreatedFile {
                            path: first,
                            prior_state: CreatedFilePriorState::Absent,
                        },
                        ExternalEffect::CreatedFile {
                            path: second,
                            prior_state: CreatedFilePriorState::Absent,
                        },
                    ]
                );
            }
            other => panic!("failed inverse must quarantine the session: {other:?}"),
        }
    }

    #[test]
    fn reset_retries_pending_inverse_and_keeps_quarantine_when_it_fails() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::env::temp_dir();
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        assert_eq!(
            system
                .compensate_liminal_external_effects_with(&file_ops)
                .len(),
            1
        );
        let error = system
            .reinit_bootstrap()
            .expect_err("reset must refuse while the pending inverse still fails");
        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        assert!(
            error.to_string().contains("delightql-error://"),
            "the compensation URI remains wrapped in the health message: {error}"
        );
        assert!(system.health_incident().is_some());
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
                assert!(incident.message.contains("reset compensation failed"));
            }
            other => panic!("failed reset must preserve quarantine: {other:?}"),
        }
    }

    #[test]
    fn reset_clears_quarantine_after_pending_inverse_and_rebuild_succeed() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::env::temp_dir().join(format!(
            "dql-reset-recovery-{}-created.db",
            std::process::id()
        ));
        std::fs::write(&path, b"").expect("create recovery fixture");
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        assert_eq!(
            system
                .compensate_liminal_external_effects_with(&file_ops)
                .len(),
            1
        );
        system
            .reinit_bootstrap()
            .expect("reset should retry and complete the pending inverse");
        assert!(system.health_incident().is_none());
        assert!(
            !path.exists(),
            "successful reset must remove the created file"
        );
    }
}
