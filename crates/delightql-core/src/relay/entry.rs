// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! PLAYING A COMPILED PLAN: the pump (`relay::pump`) plays its runs, and the
//! objects every committed run created are reconciled into the session
//! catalog before the plan's answer leaves.

use delightql_protocol::{ServerTerm, Transport};

use super::RelayParty;
use crate::error::DelightQLError;
#[cfg(all(test, not(target_arch = "wasm32")))]
use crate::external_effects::CreatedObjectCatalog;
use crate::host::{CompilerExecutionHost, CompilerHost};
use crate::pipeline::compiled_query::CompiledPlan;

fn error_term(e: &DelightQLError) -> ServerTerm {
    super::error_term(e)
}

/// The objects the plan's committed runs created — all of them once every
/// run committed, none for an untyped plan.
fn committed_objects(
    plan: &CompiledPlan,
    committed_runs: usize,
) -> Vec<crate::pipeline::compiled_query::PlanCreatedObject> {
    plan.typed
        .iter()
        .flat_map(|typed| typed.created_by(committed_runs))
        .cloned()
        .collect()
}

fn created_object_registration_error(message: String) -> ServerTerm {
    error_term(&crate::diagnostic::SessionHealth::ExternalEffect { message }.into())
}

impl<'a, T: Transport> RelayParty<'a, T> {
    /// Play the compiled plan. Every plan-scratch shell replaces residue
    /// adjacent to its CREATE, before guards or exit checks can observe it,
    /// so repeated runs on one session start with empty scratch. Pinned by
    /// the CLI integration test
    /// `run_twice_on_one_session_gets_fresh_scratch`.
    ///
    /// THE OBJECTS OF EVERY COMMITTED RUN are registered, whether or not a
    /// later run failed: a committed run's objects exist in the target, and
    /// the session's next statement must resolve what is there.
    pub(super) fn play_plan(&mut self, plan: &CompiledPlan) -> ServerTerm {
        if self
            .system
            .supplies(crate::host::Capability::ExternalEffects)
        {
            if let Some(typed) = &plan.typed {
                let _ = CompilerExecutionHost::observe_effect_plan(
                    self.system.compiler_host_mut(),
                    typed,
                );
            }
        }
        let super::pump::Played {
            term: response,
            committed_runs,
        } = self.play(plan);
        let created = committed_objects(plan, committed_runs);
        if created.is_empty() {
            return response;
        }
        match CompilerExecutionHost::reconcile_created_objects(
            self.system.compiler_host_mut(),
            &created,
        ) {
            Ok(crate::host::CreatedObjectReconciliation::Complete) => response,
            Ok(crate::host::CreatedObjectReconciliation::Unsupported(reason)) => {
                let primary: DelightQLError =
                    crate::diagnostic::SessionHealth::RegistrationUnsupported {
                        message: format!("created-object registration unsupported: {reason}"),
                    }
                    .into();
                self.fail_created_object_registration(
                    response,
                    format!("{primary} [{}]", primary.error_uri()),
                )
            }
            Err(error) => self.fail_created_object_registration(
                response,
                format!(
                    "created-object registration failed; the target object was created, \
                     but the session catalog could not be updated: {error} [{}]",
                    error.error_uri()
                ),
            ),
        }
    }

    /// Test seam for the post-run catalog boundary. Production uses the real
    /// catalog implementation; crate tests can inject a scripted failure
    /// without changing target execution or bootstrap state.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn play_plan_with_catalog<C: CreatedObjectCatalog>(
        &mut self,
        plan: &CompiledPlan,
        catalog: &C,
    ) -> ServerTerm {
        // Materialize the plan's observational projection
        // (sys::execution.effect_plan/…) — clear-then-insert is the
        // next-run-clears lifecycle. Best-effort: bookkeeping never
        // outranks the run.
        if let Some(typed) = &plan.typed {
            let _ = self.system.materialize_effect_plan(typed);
        }
        let super::pump::Played {
            term: response,
            committed_runs,
        } = self.play(plan);
        let created = committed_objects(plan, committed_runs);
        // Catalog registration is one reconciliation of the committed
        // runs' objects. Target read-backs happen before the bootstrap
        // savepoint, so a failure cannot leave an earlier sibling
        // registered. A skipped object is represented as NotPresent;
        // unsupported metadata is surfaced.
        if !created.is_empty() {
            match self
                .system
                .register_run_created_objects_with(&created, catalog)
            {
                Ok(outcomes) => {
                    if let Some(reason) = outcomes.iter().find_map(|outcome| match outcome {
                        crate::external_effects::RegistrationOutcome::Unsupported { reason } => {
                            Some(reason.clone())
                        }
                        _ => None,
                    }) {
                        let primary: DelightQLError =
                            crate::diagnostic::SessionHealth::RegistrationUnsupported {
                                message: format!(
                                    "created-object registration unsupported: {reason}"
                                ),
                            }
                            .into();
                        return self.fail_created_object_registration(
                            response,
                            format!("{primary} [{}]", primary.error_uri()),
                        );
                    }
                }
                Err(error) => {
                    return self.fail_created_object_registration(
                        response,
                        format!(
                            "created-object registration failed; the target object was created, \
                             but the session catalog could not be updated: {error} [{}]",
                            error.error_uri()
                        ),
                    );
                }
            }
        }
        response
    }

    /// A successful plan may already have allocated a final Header handle
    /// before its post-run catalog reconciliation fails. Retire that unsent
    /// handle before returning the health error; non-final hook deliveries are
    /// intentionally not retractable once they have been emitted.
    ///
    /// A plan that FAILED in a later run keeps its failure as the primary
    /// answer: the registration incident latches the session's health, which
    /// the next statement reports.
    fn fail_created_object_registration(
        &mut self,
        response: ServerTerm,
        message: String,
    ) -> ServerTerm {
        if matches!(response, ServerTerm::Error(_)) {
            self.system
                .quarantine_session("created-object registration", message);
            return response;
        }
        let handle = match &response {
            ServerTerm::Header { handle, .. } => Some(handle.clone()),
            _ => None,
        };
        let mut failure = message;
        if let Some(handle) = handle {
            if self.eager_buffers.remove(&handle).is_none() {
                if let Some(backend_handle) = self.handles.remove(&handle) {
                    match self.sql_session.close(backend_handle) {
                        Ok(delightql_protocol::CloseResponse::Ok) => {}
                        Ok(delightql_protocol::CloseResponse::Error(error)) => {
                            failure.push_str(&format!(
                                "; unsent handle close failed: {}",
                                String::from_utf8_lossy(error.message())
                            ));
                        }
                        Err(error) => {
                            failure.push_str(&format!(
                                "; unsent handle close failed: {}",
                                error.message
                            ));
                        }
                    }
                }
            }
        }
        self.system
            .quarantine_session("created-object registration", failure.clone());
        created_object_registration_error(failure)
    }
}
