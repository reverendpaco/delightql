// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The effect chain's ENTRY POINTS: where the execution directives (`run!`,
//! `run_namespace!`) and query-position DML/DDL directives leave today's
//! single-statement pipeline and take the effect chain — the transformer
//! (`pipeline::effect_transformer`) compiles, the pump
//! (`relay::pump::handle_plan`) plays.
//!
//! `handle_query` consults `classify_effect_entry` before the ordinary
//! compile path. Three shapes reroute, everything else stays byte-for-byte
//! on today's path (the classifier answers `None`):
//!
//! 1. `run_namespace!(ns)(*)` / `run_namespace!(ns)(*)` as the WHOLE statement
//!    — demand the consulted namespace's `main!`; refuse "has no main!
//!    to demand" when absent (effects ball main--22).
//! 2. `run!("file.dql")(*)` / `run!("file.dql")(*)` as the whole statement —
//!    consult-then-demand.
//! 3. A statement whose top-level expression pipes into a DML terminal
//!    (`insert!`/`update!`/`delete!`) or a DDL creation directive
//!    (`temp_table!`/`table!`/`temp_view!`) — the statement compiles as a
//!    one-clause effect body so it returns its RECEIPT (THE RECEIPT;
//!    effects ball dml_receipt--01..06 / ddl_receipt--11..15) instead
//!    of the `affected_rows` relation a raw statement returns.
//!
//! The classifier is deliberately conservative: it declines statements
//! carrying assertions, emit streams, error hooks (pre-screened by the
//! caller), danger/option annotations, or any parse/build trouble — those
//! keep today's path and today's messages.

use crate::pipeline::asts::core::AuthoredColumn;
use delightql_protocol::{ServerTerm, Transport};

use super::RelayParty;
use crate::error::DelightQLError;
use crate::external_effects::CreatedObjectCatalog;
use crate::pipeline::ast_unresolved::{Query, Relation};
use crate::pipeline::asts::core::literals::LiteralValue;
use crate::pipeline::asts::core::DomainExpression;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::effects::DirectiveDescriptor;
use crate::pipeline::compiled_query::CompiledPlan;
use crate::pipeline::effect_transformer;

/// A statement the effect chain owns.
#[derive(Debug)]
pub(super) enum EffectEntry {
    /// `run_namespace!(ns)(*)` — demand an already-consulted namespace's main!.
    RunNamespace {
        namespace: String,
        /// Non-glob receipt access: the exact-arity positional binding
        /// list. None = glob/bare — the
        /// execution family's payload-transparent dump.
        access: Option<Vec<String>>,
    },
    /// `run!("file.dql")(*)` — consult the file, then demand its main!.
    RunFile {
        path: String,
        /// See RunNamespace::access.
        access: Option<Vec<String>>,
    },
    /// A top-level directive-demanding statement: compile as an ad-hoc
    /// effect body; the run's value is the directive's receipt.
    AdhocBody {
        query: Box<Query>,
        danger_specs: Vec<crate::pipeline::asts::unresolved::DangerSpec>,
        ddl_blocks: Vec<crate::pipeline::asts::unresolved::InlineDdlSpec>,
    },
}

/// Classify one NORMALIZED statement. `Ordinary(goal)` hands the goal back
/// unchanged — it is not the effect chain's business and the caller proceeds
/// on the ordinary compilation path. `allow_adhoc` is false when CLI
/// danger/option overrides are active: the plan compiler applies default
/// gates only, so overridden DML/DDL statements keep that path.
/// run!/run_namespace! have no other path and always classify.
///
/// The goal arrives already read. Classification is a question about the
/// STATEMENT, and a classifier that re-parsed the text could answer it
/// differently from the compilation that follows.
/// WHAT ONE STATEMENT'S ROAD IS: the effect chain's, or the ordinary
/// compilation's with the goal handed back unchanged.
#[derive(Debug)]
pub(super) enum Classified {
    Effect(EffectEntry),
    Ordinary(crate::pipeline::normalize::Goal),
}

/// A statement whose row is malformed — two landed relations in one call —
/// is an ERROR, never an ordinary statement: the judgment that classifies
/// the spine is the same exhaustive judgment every consumer of the row makes.
pub(super) fn classify_effect_entry(
    goal: crate::pipeline::normalize::Goal,
    allow_adhoc: bool,
) -> crate::error::Result<Classified> {
    // Danger annotations are query-local refinement policy and travel into the
    // typed plan. Option overrides and inline DDL blocks still require the
    // ordinary compiler's broader configuration surface.
    if !goal.declared.options.is_empty() {
        return Ok(Classified::Ordinary(goal));
    }
    let crate::pipeline::normalize::Goal {
        query,
        declared,
        category,
        spelling,
    } = goal;
    Ok(match classify_query(query.clone())? {
        Some(EffectEntry::AdhocBody { query: body, .. }) if allow_adhoc => {
            Classified::Effect(EffectEntry::AdhocBody {
                query: body,
                danger_specs: declared.dangers,
                ddl_blocks: declared.ddl_blocks,
            })
        }
        Some(other) if !matches!(other, EffectEntry::AdhocBody { .. }) => Classified::Effect(other),
        _ => Classified::Ordinary(crate::pipeline::normalize::Goal {
            query,
            declared,
            category,
            spelling,
        }),
    })
}

#[stacksafe::stacksafe] // the Pipe payload is a StackSafe box
fn classify_query(query: Query) -> crate::error::Result<Option<EffectEntry>> {
    // A statement that BINDS an effect CTE is an effect body, whatever its
    // expression then does with the binding. A prompt statement is an
    // implicit run and its extent is the statement (THE IMPLICIT RUN), so
    // `n!(…)(*) : chain` and `chain : n!` bind here exactly what they bind
    // inside a rule, and the composed demand forms — `,` for one run of
    // two, `;` for two runs of one — are that run's as well. Classifying by
    // the expression's directive TAIL would see neither, and a bound effect
    // label the executor has never heard of refuses as an unknown directive.
    //
    // An effect CTE the body never demands is not an error: it does not
    // execute (laziness). The body is still this road's, because the
    // binding is.
    //
    // A CTE list with no effect mark in it does not decide the road: those
    // bindings are pure, but a directive terminal in the body still makes
    // the complete statement an ad-hoc effect body.
    if query
        .ctes()
        .iter()
        .any(|cte| cte.subject().declares_effect())
    {
        return Ok(Some(EffectEntry::AdhocBody {
            query: Box::new(query),
            danger_specs: Vec::new(),
            ddl_blocks: Vec::new(),
        }));
    }
    let expr = &query.body;
    // The receipt a direct invocation was written with: the access standing
    // in the effect position, which for a bare call is the read's own.
    let head_access = expr
        .head_access()
        .cloned()
        .unwrap_or(crate::pipeline::asts::core::Access::Unasked);
    // THE RUN FORMS stand at the outer head alone: `run_namespace!(ns)(*)`
    // and `run!("file")(*)`, with or without the `(*)` receipt access, and
    // under pure postfix steps. Non-glob receipt access is not classified
    // here — it falls through to the executor, whose run!/run_namespace!
    // entities refuse with their whole-statement policy until receipt
    // access lands more generally.
    if let Some(call) = spine_head(expr) {
        let reference = &call.call().callee;
        let run = match crate::pipeline::asts::effects::kind_for_reference(reference) {
            Some(crate::pipeline::asts::effects::DirectiveKind::RunNamespace) => Some(true),
            Some(crate::pipeline::asts::effects::DirectiveKind::Run) => Some(false),
            _ => None,
        };
        if let Some(is_namespace) = run {
            let arguments = call
                .call()
                .arguments
                .value_domains()
                .cloned()
                .collect::<Vec<_>>();
            // Glob/bare access = the payload-transparent dump (the
            // execution family's exception). A positional NAME list is the
            // exact-arity receipt binding; any other spec falls through to
            // the executor's refusal.
            let access = if head_access.is_whole() {
                None
            } else {
                let Some(binders) = head_access.binders() else {
                    return Ok(None);
                };
                Some(
                    binders
                        .into_iter()
                        .map(|binder| binder.name.to_string())
                        .collect::<Vec<_>>(),
                )
            };
            return Ok(if is_namespace {
                single_argument(&arguments)
                    .map(|namespace| EffectEntry::RunNamespace { namespace, access })
            } else {
                single_argument(&arguments).map(|path| EffectEntry::RunFile { path, access })
            });
        }
    }
    // THE EVALUATION SPINE: a directive demanded on it — at the outer head,
    // or in the relation a pipe landed in a pure call standing there —
    // makes the complete statement an ad-hoc effect body.
    if spine_directive(expr)?.is_some() {
        return Ok(Some(EffectEntry::AdhocBody {
            query: Box::new(query),
            danger_specs: Vec::new(),
            ddl_blocks: Vec::new(),
        }));
    }
    Ok(None)
}

/// THE HEAD A CHAIN'S EVALUATION SPINE STANDS ON, when that head is a
/// call: the structural forms — ordering, reposition, meta, the witnesses,
/// drill, narrowing — the pure pipe operators and an access past the
/// head's own read are the postfix steps the spine reads through, so what
/// stands under them is the spine's own head. Named by their exact
/// variants, never by a run-membership protocol. A member, a restriction
/// or a set operation is not a postfix step: the chain then has no single
/// spine head, and the answer is none.
fn spine_head(
    chain: &crate::pipeline::ast_unresolved::Chain,
) -> Option<&crate::pipeline::asts::core::SealedCall> {
    if !chain.steps().iter().all(|step| {
        matches!(
            step.form(),
            crate::pipeline::asts::core::Continuation::Pipe { .. }
                | crate::pipeline::asts::core::Continuation::Structural(_)
                | crate::pipeline::asts::core::Continuation::Access { .. }
        )
    }) {
        return None;
    }
    match chain.head().form() {
        crate::pipeline::asts::core::GroundForm::Reference(Relation::FunctorCall {
            call, ..
        }) => Some(call),
        _ => None,
    }
}

/// THE DIRECTIVE DEMANDED ON A CHAIN'S EVALUATION SPINE, if any: the
/// spine's head when it is a directive the statement road realizes — a
/// syntax pipe terminal by its descriptor's realization, or a user effect
/// rule by its reference — and otherwise, when the head is a PURE call,
/// the directive on the spine of the relation the pipe landed in it. A
/// pure call's authored relation and rule arguments are enclosed
/// positions: they are not the spine, and nothing here reads them. A
/// built-in that is not a pipe terminal — an entity, a session directive,
/// a run — is not this road's.
/// A pure call's row is read through the exhaustive judgment, so a
/// malformed row is an error here, never an absence that would send the
/// statement toward ordinary compilation.
#[stacksafe::stacksafe]
fn spine_directive(
    chain: &crate::pipeline::ast_unresolved::Chain,
) -> crate::error::Result<Option<&crate::pipeline::asts::core::SealedCall>> {
    let Some(call) = spine_head(chain) else {
        return Ok(None);
    };
    let reference = &call.call().callee;
    if adhoc_statement_call(call.call()) || user_directive(reference) {
        return Ok(Some(call));
    }
    if crate::pipeline::asts::effects::descriptor_for_reference(reference).is_some() {
        return Ok(None);
    }
    let judged = call.call().arguments.judged()?;
    let Some(landed) = judged.landed() else {
        return Ok(None);
    };
    spine_directive(landed.relation)
}

/// Does this call need the ad-hoc STATEMENT road?
///
/// Asked of the descriptor, which owns both halves of the answer: the
/// directive writes the database, and its meaning requires the relation a
/// pipe hands it. One classification serves the direct and the piped
/// occurrence, because the normalized call already says the same thing in
/// both positions and the enclosing position adds nothing to the question.
///
/// A list of the names that answer yes today would be a second population:
/// declaring one more descriptor would leave it unrouted while every other
/// authority described it completely, and changing a realization would leave
/// the old routing live.
fn adhoc_statement_call(call: &crate::pipeline::asts::core::FunctorCall) -> bool {
    crate::pipeline::asts::effects::descriptor_for_reference(&call.callee)
        .is_some_and(DirectiveDescriptor::is_adhoc_statement_terminal)
}

/// Is this a user directive — an effect rule rather than a prelude entity?
///
/// Asked of the complete-reference authority. A wrong qualifier selects no
/// built-in; entity-backed names stay on the entity road solely so its
/// visibility teaching can name the true identity, while other misses are
/// ordinary qualified effect-rule references.
fn user_directive(reference: &crate::pipeline::asts::vocabulary::Ref) -> bool {
    crate::pipeline::asts::effects::is_user_effect_reference(reference)
}

/// The single argument of a run form: a bare/`::`-qualified name (an Lvar
/// with the `::` text intact) or a string literal.
fn single_argument(arguments: &[DomainExpression]) -> Option<String> {
    let [value] = arguments else { return None };
    argument_value(value)
}
fn argument_value(value: &DomainExpression) -> Option<String> {
    match value {
        DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
            name,
            qualifier: None,
            ..
        }))) => Some(name.to_string()),
        DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::String(s)),
        ) => Some(s.clone()),
        _ => None,
    }
}

/// The namespace `run!("path/to/script.dql")(*)` consults into: the file stem,
/// sanitized to identifier characters. The directive's own syntax names no
/// namespace; the stem is what a human would type, and it leaves the
/// script addressable afterwards — a consulted script is thereby
/// runnable without re-consulting, via `run_namespace!`.
fn namespace_from_path(path: &str) -> String {
    let stem = std::path::Path::new(path)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let sanitized: String = stem
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "script".to_string()
    } else {
        sanitized
    }
}

fn error_term(e: &DelightQLError) -> ServerTerm {
    super::error_term(e)
}

fn created_object_registration_error(message: String) -> ServerTerm {
    error_term(&crate::diagnostic::SessionHealth::ExternalEffect { message }.into())
}

impl<'a, T: Transport> RelayParty<'a, T> {
    /// Play a classified effect entry: compile via the effect transformer,
    /// play via the pump. The run's return value is the demanded body's
    /// final shipped statement — the one wire response (protocol ruling).
    pub(super) fn handle_effect_entry(&mut self, entry: EffectEntry) -> ServerTerm {
        match entry {
            EffectEntry::RunNamespace { namespace, access } => {
                let term = self.demand_namespace_main(&namespace);
                match access {
                    None => term,
                    Some(names) => self.bind_run_receipt(
                        term,
                        "run_namespace!",
                        "namespace",
                        &namespace,
                        &names,
                    ),
                }
            }
            EffectEntry::RunFile { path, access } => {
                // Consult-then-demand. Liminal directives execute at
                // load; rules register; then main! is demanded exactly as
                // run_namespace! would.
                let namespace = match self.consult_for_run(&path) {
                    Ok(ns) => ns,
                    Err(e) => return error_term(&e),
                };
                let term = self.demand_namespace_main(&namespace);
                match access {
                    None => term,
                    Some(names) => self.bind_run_receipt(term, "run!", "path", &path, &names),
                }
            }
            EffectEntry::AdhocBody {
                query,
                danger_specs,
                ddl_blocks,
            } => {
                if let Err(error) =
                    crate::pipeline::inline_ddl::register_prompt_blocks(ddl_blocks, self.system)
                {
                    return error_term(&error);
                }
                match effect_transformer::compile_query_plan(
                    self.system,
                    &query,
                    None,
                    &danger_specs,
                ) {
                    Ok(plan) => self.play_plan(&plan),
                    Err(e) => error_term(&e),
                }
            }
        }
    }

    /// Bind an exact-arity positional access list against the run's
    /// REIFIED receipt: `(success, operation,
    /// path|namespace, returned)`. The payload is the run's response,
    /// packaged as the `returned` interior; a NO run (exit! latch taken)
    /// ships the EMPTY receipt — zero rows, declared heading.
    fn bind_run_receipt(
        &mut self,
        term: ServerTerm,
        operation: &str,
        echo_name: &str,
        echo_value: &str,
        names: &[String],
    ) -> ServerTerm {
        let ServerTerm::Header { handle, dimensions } = term else {
            return term; // errors propagate untouched
        };
        let declared = ["success", "operation", echo_name, "returned"];
        if names.len() != declared.len() {
            let msg = format!(
                "{operation}'s receipt heading is (success, operation, {echo_name}, \
                 returned) — the binding list is exact-arity; glob access `(*)` \
                 dumps the payload instead (EFFECT-ALGEBRA F5)"
            );
            return error_term(
                &crate::diagnostic::EffectRun::ReceiptAccess { message: msg }.into(),
            );
        }
        // The response buffer becomes the `returned` payload.
        let payload = match self.eager_buffers.remove(&handle) {
            Some(buf) => {
                let cols: Vec<String> = buf
                    .dimensions
                    .iter()
                    .map(|d| String::from_utf8_lossy(&d.name).into_owned())
                    .collect();
                let objs: Vec<serde_json::Value> = buf
                    .rows
                    .iter()
                    .map(|row| {
                        let mut m = serde_json::Map::new();
                        for (c, cell) in cols.iter().zip(row) {
                            m.insert(
                                c.clone(),
                                match cell {
                                    Some(bytes) => serde_json::Value::String(
                                        String::from_utf8_lossy(bytes).into_owned(),
                                    ),
                                    None => serde_json::Value::Null,
                                },
                            );
                        }
                        serde_json::Value::Object(m)
                    })
                    .collect();
                serde_json::Value::Array(objs).to_string()
            }
            None => "[]".to_string(),
        };
        let _ = dimensions;
        // The receipt is COMPOSED here, not read from an engine: every
        // field is present by construction, so each is a cell carrying its
        // own bytes.
        let rows: Vec<Vec<delightql_protocol::Cell>> = if self.last_run_exited {
            Vec::new()
        } else {
            vec![vec![
                Some(b"1".to_vec()),
                Some(operation.as_bytes().to_vec()),
                Some(echo_value.to_string().into_bytes()),
                Some(payload.into_bytes()),
            ]]
        };
        let columns: Vec<String> = names.to_vec();
        self.eager_header(&columns, rows)
    }

    fn demand_namespace_main(&mut self, namespace: &str) -> ServerTerm {
        match effect_transformer::compile_namespace_main(self.system, namespace) {
            Ok(plan) => self.play_plan(&plan),
            Err(e) => error_term(&e),
        }
    }

    /// Play the compiled plan. Every plan-scratch shell replaces residue
    /// adjacent to its CREATE, before guards or exit checks can observe it,
    /// so repeated runs on one session start with empty scratch. Pinned by
    /// the CLI integration test
    /// `run_twice_on_one_session_gets_fresh_scratch`.
    fn play_plan(&mut self, plan: &CompiledPlan) -> ServerTerm {
        self.play_plan_with_catalog(plan, &crate::system::RealCreatedObjectCatalog)
    }

    /// Test seam for the post-run catalog boundary. Production uses the real
    /// catalog implementation; crate tests can inject a scripted failure
    /// without changing target execution or bootstrap state.
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
        let response = self.handle_plan(plan);
        if !matches!(response, ServerTerm::Error { .. }) {
            // Catalog registration is one plan-level reconciliation. Target
            // read-backs happen before the bootstrap savepoint, so a failure
            // cannot leave an earlier sibling registered. A skipped object is
            // represented as NotPresent; unsupported metadata is surfaced.
            if !plan.created_objects.is_empty() {
                match self
                    .system
                    .register_run_created_objects_with(&plan.created_objects, catalog)
                {
                    Ok(outcomes) => {
                        if let Some(reason) = outcomes.iter().find_map(|outcome| match outcome {
                            crate::external_effects::RegistrationOutcome::Unsupported {
                                reason,
                            } => Some(reason.clone()),
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
        }
        response
    }

    /// A successful plan may already have allocated a final Header handle
    /// before its post-run catalog reconciliation fails. Retire that unsent
    /// handle before returning the health error; non-final hook deliveries are
    /// intentionally not retractable once they have been emitted.
    fn fail_created_object_registration(
        &mut self,
        response: ServerTerm,
        message: String,
    ) -> ServerTerm {
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

    /// run!'s consult half: consult the file into its stem-derived
    /// namespace, or RE-consult when an earlier run! (or consult!)(*) already
    /// loaded that namespace — each run! re-reads the file (a script
    /// runner's contract), while run_namespace! deliberately does not.
    fn consult_for_run(&mut self, path: &str) -> Result<String, DelightQLError> {
        let namespace = namespace_from_path(path);
        match self.namespace_kind(&namespace)? {
            None => {
                crate::bin_cartridge::prelude::consult::execute_consult(
                    self.system,
                    path,
                    &namespace,
                    None,
                )?;
            }
            // reconsult_namespace itself gates by kind (lib/scratch reload;
            // data/system/grounded refuse with curated messages we surface
            // as-is).
            Some(_) => {
                self.system.reconsult_namespace(&namespace, Some(path))?;
            }
        }
        Ok(namespace)
    }

    /// The target namespace's catalog kind, `None` when it does not exist.
    #[cfg(not(target_arch = "wasm32"))]
    fn namespace_kind(&self, namespace: &str) -> Result<Option<String>, DelightQLError> {
        let conn = self.system.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            crate::diagnostic::Runtime::poisoned("Failed to acquire bootstrap lock", e)
        })?;
        let mut stmt = guard
            .prepare("SELECT COALESCE(kind, 'unknown') FROM namespace WHERE fq_name = ?1")
            .map_err(|e| {
                crate::diagnostic::Runtime::catalog("Failed to query namespace catalog", e)
            })?;
        let mut rows = stmt.query([namespace]).map_err(|e| {
            crate::diagnostic::Runtime::catalog("Failed to query namespace catalog", e)
        })?;
        match rows.next() {
            Ok(Some(row)) => Ok(Some(row.get(0).unwrap_or_else(|_| "unknown".to_string()))),
            Ok(None) => Ok(None),
            Err(e) => Err(crate::diagnostic::Runtime::catalog(
                "Failed to read namespace catalog",
                e,
            )),
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn namespace_kind(&self, _namespace: &str) -> Result<Option<String>, DelightQLError> {
        Ok(None) // no bootstrap catalog on wasm; consult decides
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_statement_run_namespace_classifies_both_forms() {
        for dql in [
            "run_namespace!(fx)(*)",
            "run_namespace!(fx)(*)",
            "run_namespace!(\"fx\")(*)",
        ] {
            match classify_effect_entry(read_goal(dql), true) {
                Ok(Classified::Effect(EffectEntry::RunNamespace { namespace, .. })) => {
                    assert_eq!(namespace, "fx")
                }
                _ => panic!("expected RunNamespace for {:?}", dql),
            }
        }
    }

    #[test]
    fn whole_statement_run_classifies_with_path() {
        match classify_effect_entry(read_goal("run!(\"ddl/script.dql\")(*)"), true) {
            Ok(Classified::Effect(EffectEntry::RunFile { path, .. })) => {
                assert_eq!(path, "ddl/script.dql")
            }
            _ => panic!("expected RunFile"),
        }
    }

    /// EVERY ad-hoc statement terminal the descriptor table declares reaches
    /// the statement road — the population is iterated, not listed, so
    /// declaring one more is covered here the moment it is declared.
    #[test]
    fn every_declared_adhoc_terminal_classifies_as_an_adhoc_body() {
        let terminals: Vec<&str> = crate::pipeline::asts::effects::DIRECTIVE_DESCRIPTORS
            .iter()
            .filter(|d| d.is_adhoc_statement_terminal())
            .map(|d| d.name)
            .collect();
        assert!(
            !terminals.is_empty(),
            "the policy must select someone, or this test proves nothing"
        );

        for name in terminals {
            let dql = match name {
                "abort" => "orders(*) |> abort!(\"test\")(*)".to_string(),
                "assert" => "has_rows(T(*))(*) : T(*)\n\
                             orders(*) |> assert!(has_rows(*), \"test\")(*)"
                    .to_string(),
                "exit" | "returning" | "stdout" => format!("orders(*) |> {name}!(*)"),
                "returning_other" => "orders(*) |> returning_other!(other(*))(*)".to_string(),
                _ => format!("orders(*) |> {name}!(target(*))(*)"),
            };
            assert!(
                matches!(
                    classify_effect_entry(read_goal(&dql), true),
                    Ok(Classified::Effect(EffectEntry::AdhocBody { .. }))
                ),
                "expected AdhocBody for {dql:?}"
            );
        }
    }

    #[test]
    fn a_pure_cte_does_not_hide_the_bodys_adhoc_terminal() {
        let dql = "adults(*) : users(*), age > 30\nadults(*) |> table!(a2)(*)";
        assert!(matches!(
            classify_effect_entry(read_goal(dql), true),
            Ok(Classified::Effect(EffectEntry::AdhocBody { .. }))
        ));
    }

    /// The realization is the whole answer: DDL realized as an ENTITY has a
    /// callable to invoke and is not a statement terminal, while a utility
    /// pipe terminal that writes no database still needs the piped relation
    /// and takes the statement road — a category or name subset would
    /// leave it unrouted.
    #[test]
    fn the_realization_alone_selects_the_statement_road() {
        use crate::pipeline::asts::effects::{descriptor, DirectiveCategory, DirectiveRealization};

        let imprint = descriptor("imprint").expect("imprint is declared");
        assert_eq!(imprint.category, DirectiveCategory::Ddl);
        assert_eq!(imprint.realization, DirectiveRealization::Entity);
        assert!(!imprint.is_adhoc_statement_terminal());

        let returning = descriptor("returning").expect("returning is declared");
        assert_eq!(returning.category, DirectiveCategory::Utility);
        assert_eq!(
            returning.realization,
            DirectiveRealization::SyntaxPipeTerminal
        );
        assert!(returning.is_adhoc_statement_terminal());
    }

    /// A directive on the evaluation spine under a PURE higher-order call
    /// routes the statement: the pipe landed the effect's released relation
    /// in the call, and the spine continues through that landed member. An
    /// effect in an authored argument of the same call is enclosed and does
    /// not route.
    #[test]
    fn a_landed_effect_under_a_pure_call_routes_and_an_enclosed_one_does_not() {
        let landed = "add_one(T(*))(*) : T(*) |> +(1 as extra)\n\
                      _(value @ 7) !> returning!(*) |> add_one(*)";
        assert!(matches!(
            classify_effect_entry(read_goal(landed), true),
            Ok(Classified::Effect(EffectEntry::AdhocBody { .. }))
        ));
        let landed_twice = "add_one(T(*))(*) : T(*) |> +(1 as extra)\n\
                            _(value @ 7) |> returning!(*) |> add_one(*) |> add_one(*)";
        assert!(matches!(
            classify_effect_entry(read_goal(landed_twice), true),
            Ok(Classified::Effect(EffectEntry::AdhocBody { .. }))
        ));
    }

    /// A MALFORMED ROW IS AN ERROR, NOT AN ABSENCE: a pure call carrying
    /// two landed relations refuses at classification through the same
    /// exhaustive judgment every consumer of the row makes, instead of being
    /// read as "no directive" and sent toward ordinary compilation.
    #[test]
    fn a_row_with_two_landed_relations_refuses_at_classification() {
        use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
        use crate::pipeline::asts::core::{FunctorCall, GroundForm, QueryLocals};
        let goal = read_goal("users(*) |> pair(*)");
        let GroundForm::Reference(Relation::FunctorCall { call: pure, alias }) =
            goal.query.body.head().form().clone()
        else {
            panic!("a pure call head")
        };
        let source = crate::pipeline::ast_unresolved::Chain::authored(GroundForm::Reference(
            match read_goal("users(*)").query.body.head().form() {
                GroundForm::Reference(relation) => relation.clone(),
                other => panic!("a read head, got {other:?}"),
            },
        ));
        let malformed = crate::pipeline::asts::core::SealedCall::authored(FunctorCall {
            callee: pure.call().callee.clone(),
            arguments: CallArguments::higher_order(vec![
                HoArgument::Landed(source.clone()),
                HoArgument::Landed(source),
            ]),
            marks: pure.call().marks.clone(),
        });
        let body = crate::pipeline::ast_unresolved::Chain::authored(GroundForm::Reference(
            Relation::FunctorCall {
                call: malformed,
                alias,
            },
        ));
        let statement = crate::pipeline::normalize::Goal {
            query: Query::binding(QueryLocals::none(), body),
            ..goal
        };
        assert!(
            classify_effect_entry(statement, true).is_err(),
            "two landed relations refuse instead of classifying as ordinary"
        );
    }

    /// THE FENCE AT A PURE CALL, at the constructible AST boundary: an
    /// effect standing in an authored relation argument of a pure call is
    /// enclosed and refuses as a pure position demanding an effect, while
    /// the same effect landed by the pipe is the spine's. The grammar
    /// refuses the enclosed spelling before this point; the fence guards
    /// the row a later phase could assemble.
    #[test]
    fn an_effect_in_an_authored_argument_is_fenced_and_a_landed_one_is_the_spine() {
        use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
        use crate::pipeline::asts::core::{FunctorCall, GroundForm};
        fn head_relation(dql: &str) -> Relation {
            match read_goal(dql).query.body.head().form() {
                GroundForm::Reference(relation) => relation.clone(),
                other => panic!("a call head, got {other:?}"),
            }
        }
        let Relation::FunctorCall { call: pure, .. } = head_relation("users(*) |> pair(*)") else {
            panic!("a pure call")
        };
        let effect = head_relation("users(*) |> other!(*)");
        let effect_chain =
            crate::pipeline::ast_unresolved::Chain::authored(GroundForm::Reference(effect));
        let enclosed = FunctorCall {
            callee: pure.call().callee.clone(),
            arguments: CallArguments::higher_order(vec![
                HoArgument::Landed(crate::pipeline::ast_unresolved::Chain::authored(
                    GroundForm::Reference(head_relation("users(*)")),
                )),
                HoArgument::Relation(effect_chain.clone()),
            ]),
            marks: pure.call().marks.clone(),
        };
        let refusal = crate::pipeline::asts::effects::refuse_enclosed_effects(&enclosed)
            .expect_err("an enclosed effect refuses");
        assert!(
            matches!(
                refusal,
                DelightQLError::Semantic(crate::diagnostic::Semantic::Effect(
                    crate::diagnostic::Effect::CompilePurity { .. }
                ))
            ),
            "a pure position demanded an effect: {refusal}"
        );
        let landed = FunctorCall {
            callee: pure.call().callee.clone(),
            arguments: CallArguments::higher_order(vec![HoArgument::Landed(effect_chain)]),
            marks: pure.call().marks.clone(),
        };
        crate::pipeline::asts::effects::refuse_enclosed_effects(&landed)
            .expect("the landed member is the spine's, not an enclosed position");
    }

    #[test]
    fn plain_queries_and_session_directives_stay_on_todays_path() {
        for dql in [
            "users(*)",
            "consult!(\"x.dql\", \"fx\")(*)",
            "mount!(\"db.sqlite\", \"main\")(*)",
            "users(*), region = \"EU\"",
            // imprint! keeps its own existing execution path
            "users(*) |> imprint!(\"lib::t\", \"main\")(*)",
        ] {
            assert!(
                matches!(
                    classify_effect_entry(read_goal(dql), true),
                    Ok(Classified::Ordinary(_))
                ),
                "expected today's path for {:?}",
                dql
            );
        }
    }

    #[test]
    fn query_local_danger_annotations_ride_the_typed_path() {
        let dql = "orders(*) |> insert!(t(*))(*) (~~danger://cardinality/cartesian ~~)";
        match classify_effect_entry(read_goal(dql), true) {
            Ok(Classified::Effect(EffectEntry::AdhocBody { danger_specs, .. })) => {
                assert_eq!(danger_specs.len(), 1);
            }
            _ => panic!("expected annotated AdhocBody"),
        }
    }

    #[test]
    fn spaced_directive_calls_classify() {
        // Grammar-legal whitespace between `!` and `(` reaches the classifier
        // like any other spelling; nothing textual stands in front of it
        // (effects ball main--25 pins this end to end).
        for dql in [
            "run_namespace! (fx)(*)",
            "run_namespace!  (fx)(*)",
            "run! (\"a.dql\")(*)",
        ] {
            assert!(
                matches!(
                    classify_effect_entry(read_goal(dql), true),
                    Ok(Classified::Effect(_))
                ),
                "expected classification for {:?}",
                dql
            );
        }
        // An ordinary statement is not the effect chain's business.
        assert!(matches!(
            classify_effect_entry(read_goal("users(*), a != b"), true),
            Ok(Classified::Ordinary(_))
        ));
    }

    /// One statement, read the way the relay reads it.
    fn read_goal(dql: &str) -> crate::pipeline::normalize::Goal {
        let tree = crate::pipeline::parse::prompt(dql).expect("the statement parses");
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let normalized = crate::pipeline::normalize::definition_file(&tree, registry.names())
            .expect("the statement normalizes");
        crate::pipeline::one_goal(normalized).expect("one statement, one goal")
    }

    #[test]
    fn namespace_from_path_uses_sanitized_stem() {
        assert_eq!(namespace_from_path("ddl/torture.dql"), "torture");
        assert_eq!(namespace_from_path("a/b/my-script.dql"), "my_script");
        assert_eq!(namespace_from_path(""), "script");
    }
}
